import streamlit as st
import xml.etree.ElementTree as ET
import pandas as pd
import re
from collections import defaultdict
import re
import os
from quality_dashboard import render_quality_dashboard
from sql_refiner import SQLRefiner

st.set_page_config(page_title="SSIS Metadata Extractor for Migration", layout="wide")

st.title("🔄 SSIS Package Metadata Extractor for Migration")
st.markdown("**Extract complete metadata from SSIS packages for migration purposes**")

class SSISMetadataExtractor:
    def __init__(self, xml_content):
        self.root = ET.fromstring(xml_content)
        self.namespaces = {
            'DTS': 'www.microsoft.com/SqlServer/Dts',
            'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'
        }
        self.variable_map = self._cache_variables()
        self.conn_map = self._cache_connections()

    def _cache_connections(self):
        """Cache connection strings for quick lookup by ID and Name"""
        c_map = {}
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for conn in self.root.findall('.//DTS:ConnectionManager', self.namespaces):
            conn_id = conn.get(f'{ns}DTSID')
            conn_name = conn.get(f'{ns}ObjectName')
            
            # Get connection string
            conn_string = ''
            conn_mgr = conn.find('.//DTS:ConnectionManager', self.namespaces)
            if conn_mgr is not None:
                conn_string = conn_mgr.get(f'{ns}ConnectionString', '')
                
            if conn_string:
                if conn_id:
                    c_map[conn_id] = conn_string
                if conn_name:
                    c_map[conn_name] = conn_string
                    c_map[f"Package.ConnectionManagers[{conn_name}]"] = conn_string
                    
        return c_map

    def _cache_variables(self):
        """Index variables for quick lookup"""
        v_map = {}
        ns = '{www.microsoft.com/SqlServer/Dts}'
        for var in self.root.findall('.//DTS:Variable', self.namespaces):
            name = var.get(f'{ns}ObjectName')
            val_elem = var.find('.//DTS:VariableValue', self.namespaces)
            val = val_elem.text if val_elem is not None else ''
            
            if name:
                v_map[name] = val
                v_map[f"User::{name}"] = val # Support qualified name
        return v_map

    def _clean_sql_comments(self, sql):
        """Robustly remove SQL comments (including nested block comments)"""
        out = []
        i = 0
        n = len(sql)
        depth = 0
        in_string = False
        in_line_comment = False
        
        while i < n:
            char = sql[i]
            
            # 1. Handle String State
            if in_string:
                out.append(char)
                if char == "'":
                    # Check for escaped quote ''
                    if i + 1 < n and sql[i+1] == "'":
                        out.append("'")
                        i += 1
                    else:
                        in_string = False
                i += 1
                continue
                
            # 2. Handle Line Comment State
            if in_line_comment:
                if char == '\n':
                    in_line_comment = False
                    out.append(char) # Keep newline
                i += 1
                continue
            
            # 3. Handle Block Comment Internal State (depth > 0)
            if depth > 0:
                # Check for nested block comment start
                if char == '/' and i + 1 < n and sql[i+1] == '*':
                    depth += 1
                    i += 2
                    continue
                # Check for block comment end
                if char == '*' and i + 1 < n and sql[i+1] == '/':
                    depth -= 1
                    i += 2
                    continue
                # Ignore other chars inside comment
                i += 1
                continue
                
            # 4. Normal State (depth == 0)
            
            # Start of Block Comment?
            if char == '/' and i + 1 < n and sql[i+1] == '*':
                depth += 1
                i += 2
                continue
                
            # Start of Line Comment?
            if char == '-' and i + 1 < n and sql[i+1] == '-':
                in_line_comment = True
                i += 2
                continue
                
            # Start of String?
            if char == "'":
                in_string = True
                out.append(char)
                i += 1
                continue
                
            # Normal char
            out.append(char)
            i += 1
            
        return "".join(out)
        
    def _extract_ctes_and_clean_sql(self, sql_clean):
        """Helper to strip DECLAREs and extract CTEs, returning (mappings, main_sql)"""
        subquery_mappings = {} 

        # PRE-PROCESSING: Remove DECLARE statements to expose WITH/SELECT
        while re.match(r'^\s*DECLARE\s+', sql_clean, re.IGNORECASE):
            end_match = re.search(r';', sql_clean)
            if end_match:
                sql_clean = sql_clean[end_match.end():].strip()
            else:
                 lines = sql_clean.split('\n', 1)
                 if len(lines) > 1: sql_clean = lines[1].strip()
                 else: break

        # PHASE 1: EXTRACT CTEs (WITH clause)
        with_match = re.match(r'^\s*WITH\s+', sql_clean, re.IGNORECASE)
        
        if with_match:
            cte_section_start = with_match.end()
            paren_depth = 0
            main_select_start = -1
            
            for i, char in enumerate(sql_clean[cte_section_start:], start=cte_section_start):
                if char == '(': paren_depth += 1
                elif char == ')': paren_depth -= 1
                elif paren_depth == 0:
                    if sql_clean[i:i+6] == 'SELECT':
                        main_select_start = i
                        break
            
            if main_select_start != -1:
                cte_section = sql_clean[cte_section_start:main_select_start].strip()
                cte_pattern = r'(\w+)\s+AS\s*\('
                cte_start_pos = 0
                
                while True:
                    cte_match = re.search(cte_pattern, cte_section[cte_start_pos:], re.IGNORECASE)
                    if not cte_match: break
                    
                    cte_alias = cte_match.group(1).upper()
                    open_paren_pos = cte_start_pos + cte_match.end() - 1
                    
                    paren_count = 1
                    close_paren_pos = -1
                    
                    for i, char in enumerate(cte_section[open_paren_pos + 1:], start=open_paren_pos + 1):
                        if char == '(': paren_count += 1
                        elif char == ')': paren_count -= 1
                        if paren_count == 0:
                            close_paren_pos = i
                            break
                    
                    if close_paren_pos != -1:
                        cte_sql = cte_section[open_paren_pos + 1:close_paren_pos]
                        subquery_mappings[cte_alias] = self.parse_sql_column_sources(cte_sql)
                        cte_start_pos = close_paren_pos + 1
                    else: break
                
                sql_clean = sql_clean[main_select_start:].strip()
                
        return subquery_mappings, sql_clean

    def extract_join_keys(self, sql_query):
        """Extract columns used in JOIN ON clauses"""
        if not sql_query or sql_query == 'N/A': return []
        
        # 1. Parse CTEs to understand aliases
        subquery_mappings, sql_clean = self._extract_ctes_and_clean_sql(self._clean_sql_comments(sql_query).upper().strip())
        
        # 1.5 Parse Table Aliases in Main Query
        table_aliases = {}
        # Pattern handles: FROM table alias, JOIN table AS alias
        table_pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w\.\[\]]+\]?\.)?(?:\[?[\w\.\[\]]+\]?\.)?(\[?[\w_]+\]?)(?:\s+(?:AS\s+)?(\w+))?'
        
        for match in re.finditer(table_pattern, sql_clean, re.IGNORECASE):
            table_name = match.group(1).strip('[]').upper()
            alias_group = match.group(2)
            alias = alias_group.upper() if alias_group else table_name
            
            if alias in ['LEFT', 'RIGHT', 'INNER', 'OUTER', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'BY', 'SELECT', 'FROM', 'WITH', 'OPTION']:
                alias = table_name
                
            table_aliases[alias] = table_name
            
        join_keys = []
        
        # 2. Extract ON clauses
        # Regex to find ON ... until next keyword
        on_pattern = r'\bON\b\s+(.*?)(?=\b(?:LEFT|RIGHT|INNER|OUTER|JOIN|WHERE|GROUP|ORDER|UNION|OPTION)\b|$)'
        
        for match in re.finditer(on_pattern, sql_clean, re.DOTALL | re.IGNORECASE):
            condition = match.group(1).strip()
            # print(f"DEBUG: Found ON clause: {condition}")
            
            # 3. Extract table.column references
            col_refs = re.findall(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', condition)
            
            for table_alias, col_name in col_refs:
                table_alias = table_alias.upper()
                col_name = col_name.upper()
                
                source_table = table_alias
                source_col = col_name
                
                # Resolve Table Alias -> Real Table Name (e.g. A -> T1)
                real_table_name = table_aliases.get(table_alias, table_alias)
                source_table = real_table_name # Default to real name
                
                # 4. Resolve Aliases using CTE mappings
                if real_table_name in subquery_mappings:
                    inner_map = subquery_mappings[real_table_name]
                    resolved = inner_map.get(col_name)
                    if resolved:
                        if isinstance(resolved, dict):
                            source_table = resolved.get('source_table', source_table)
                            source_col = resolved.get('source_column', source_col)
                        else:
                             # Should not happen with new structure but fallback
                             pass
                
                join_keys.append({
                    'Original Table Alias': table_alias,
                    'Original Column': col_name,
                    'Source Table': source_table,
                    'Source Column': source_col
                })
                
        return join_keys

    def parse_sql_column_sources(self, sql_query):
        """Parse SQL query to extract source table for each column with smart inference and recursion"""
        if not sql_query or sql_query == 'N/A':
            return {}
        
        column_to_table = {}
        
        try:
            # Clean up the SQL (Handle nested comments properly)
            sql_clean = self._clean_sql_comments(sql_query).upper().strip()
            
            # PHASE 0: CHECK FOR STORED PROCEDURES (EXEC)
            if sql_clean.startswith('EXEC'):
                # Simple extraction of procedure name (first token after EXEC)
                # Handles: EXEC dbo.proc, EXECUTE dbo.proc, EXEC @var
                parts = sql_clean.split()
                if len(parts) > 1:
                    proc_name = parts[1]
                    # Return wildcard mapping so all output columns map to this SP
                    return {'*': {'source_table': proc_name, 'expression': 'Stored Procedure Result'}}
            
            # PHASE 1: EXTRACT CTEs (WITH clause)
            # Use Helper Method
            subquery_mappings, sql_clean = self._extract_ctes_and_clean_sql(sql_clean)
            
            # PHASE 2: CHECK FOR UNION/UNION ALL
            # If query has UNION, split into branches and parse each separately
            # Then merge results (first SELECT defines the column schema)
            
            union_branches = []
            
            # Find UNION keywords at top level (not inside parentheses)
            paren_depth = 0
            union_positions = []
            
            i = 0
            while i < len(sql_clean):
                if sql_clean[i] == '(':
                    paren_depth += 1
                elif sql_clean[i] == ')':
                    paren_depth -= 1
                elif paren_depth == 0:
                    # Check for UNION at top level
                    if sql_clean[i:i+5] == 'UNION':
                        # Make sure it's a word boundary
                        if (i == 0 or not sql_clean[i-1].isalnum()) and \
                           (i+5 >= len(sql_clean) or not sql_clean[i+5].isalnum()):
                            union_positions.append(i)
                i += 1
            
            if union_positions:
                # Split query into branches
                start_pos = 0
                for union_pos in union_positions:
                    branch_sql = sql_clean[start_pos:union_pos].strip()
                    if branch_sql:
                        union_branches.append(branch_sql)
                    
                    # Skip past "UNION" or "UNION ALL"
                    next_start = union_pos + 5  # len("UNION")
                    if sql_clean[next_start:next_start+4].strip().upper() == 'ALL':
                        next_start += 4
                    start_pos = next_start
                
                # Add last branch
                last_branch = sql_clean[start_pos:].strip()
                if last_branch:
                    union_branches.append(last_branch)
                
                # Parse each branch recursively
                branch_results = []
                for branch_sql in union_branches:
                    branch_result = self.parse_sql_column_sources(branch_sql)
                    branch_results.append(branch_result)
                
                # Merge results: First SELECT defines the schema
                # All branches should have same columns (by SQL UNION rules)
                if branch_results:
                    merged_result = {}
                    first_branch = branch_results[0]
                    
                    # For each column in first branch
                    for col_alias, col_data in first_branch.items():
                        # Collect source tables from all branches for this column
                        all_sources = set()
                        all_source_cols = set()
                        
                        for branch_result in branch_results:
                            if col_alias in branch_result:
                                branch_col_data = branch_result[col_alias]
                                if isinstance(branch_col_data, dict):
                                    all_sources.add(branch_col_data.get('source_table', 'N/A'))
                                    all_source_cols.add(branch_col_data.get('source_column', col_alias))
                                else:
                                    all_sources.add(str(branch_col_data))
                        
                        # Merge into single result
                        if isinstance(col_data, dict):
                            merged_result[col_alias] = {
                                'source_table': ', '.join(sorted(all_sources)) if all_sources else 'N/A',
                                'source_column': ', '.join(sorted(all_source_cols)) if all_source_cols else col_alias,
                                'expression': col_data.get('expression', '')
                            }
                        else:
                            merged_result[col_alias] = ', '.join(sorted(all_sources)) if all_sources else 'N/A'
                    
                    return merged_result
            
            # PHASE 3: CHECK FOR SUBQUERIES (Derived Tables) in FROM and JOINs
            # Logic: Iterate through string to find ALL "(SELECT" blocks
            # Logic: Iterate through string to find ALL "(SELECT" blocks
            
            masked_sql = sql_clean
            
            # Find all start indices of "(SELECT" or "( SELECT"
            # Regex is tricky for finding all overlapping, but we process sequentially.
            # actually we need to loop until no more (SELECT found that hasn't been masked.
            
            while True:
                # Find first unmasked (SELECT
                # Look for ( followed by whitespace? followed by SELECT
                match = re.search(r'\(\s*SELECT', masked_sql, re.IGNORECASE)
                if not match:
                    break
                
                start_inner = match.start() + 1 # Skip '('
                
                # Context Check: Is this a Derived Table (FROM/JOIN) or Scalar Subquery (SELECT list)?
                prefix_segment = masked_sql[:match.start()].strip()
                is_derived_context = False
                if prefix_segment:
                     # Find last word
                     last_word_match = re.search(r'(\w+)\s*$', prefix_segment)
                     if last_word_match:
                         last_token = last_word_match.group(1).upper()
                         if last_token in ['FROM', 'JOIN', 'APPLY', 'UPDATE', 'INTO']:
                              is_derived_context = True
                
                # If parsed as UNION branch, the string starts with SELECT, so no FROM before it.
                # But subquery inside FROM clause: FROM (SELECT ...
                # So the check holds.
                
                # Balanced Parsing to find closing )
                paren_count = 1
                end_inner = -1
                
                for i, char in enumerate(masked_sql[start_inner:]):
                    if char == '(':
                        paren_count += 1
                    elif char == ')':
                        paren_count -= 1
                    
                    if paren_count == 0:
                        end_inner = start_inner + i
                        break
                
                if end_inner != -1:
                    # check if this is actually a subquery we want (has SELECT) - yes regex checked it
                    inner_sql = masked_sql[start_inner : end_inner]
                    
                    # Extract Alias after )
                    remainder = masked_sql[end_inner+1:]
                    # Alias is optional [AS] alias
                    alias_match = re.match(r'^\s*(?:AS\s+)?(\w+)', remainder, re.IGNORECASE)
                    
                    sub_alias = None
                    if alias_match and is_derived_context: # Only extract alias if it's a derived table
                        sub_alias = alias_match.group(1).upper()
                        # Ignore keyword aliases just in case
                        if sub_alias in ['ON', 'JOIN', 'LEFT', 'RIGHT', 'WHERE', 'ORDER', 'GROUP']:
                            sub_alias = None
                    
                    # Recursively Parse only if it's a Derived Table
                    if sub_alias:
                        # print(f"DEBUG: Found subquery alias {sub_alias}")
                        subquery_mappings[sub_alias] = self.parse_sql_column_sources(inner_sql)
                    
                    # MASK IT
                    # We replace providing the same length to preserve other indices? 
                    # No, we restart search on modified string.
                    # Replace (SELECT ... ) with (SUBQUERY_PROCESSED)
                    # We utilize the start/end indices. 
                    prefix = masked_sql[:match.start()]
                    suffix = masked_sql[end_inner+1:]
                    
                    # If it was a scalar subquery (not derived context), mask differently to avoid table scanner
                    if is_derived_context:
                        replacement = " (SUBQUERY_MASK) "
                    else:
                        replacement = " (SCALAR_MASK) "
                        
                    masked_sql = prefix + replacement + suffix
                else:
                    # Malformed? Mismatch parens. Break to avoid infinite loop
                    break

            # Now parse standard tables from the MASKED sql
            # The masked SQL contains "SUBQUERY_MASK" instead of (SELECT...)
            
            table_aliases = {}
            # Inject known subquery aliases as special tables
            for alias in subquery_mappings:
                # Use a unique name so unique_tables logic works and we can recover the alias
                table_aliases[alias] = f"SUBQUERY::{alias}"
            
            # Standard table pattern
            table_pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w\.\[\]]+\]?\.)?(?:\[?[\w\.\[\]]+\]?\.)?(\[?[\w_]+\]?)(?:\s+(?:AS\s+)?(\w+))?'
            
            for match in re.finditer(table_pattern, masked_sql):
                table_name = match.group(1).strip('[]')
                alias_group = match.group(2)
                alias = alias_group if alias_group else table_name
                
                if alias in ['LEFT', 'RIGHT', 'INNER', 'OUTER', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'BY', 'SELECT', 'FROM', 'SUBQUERY_MASK']:
                     alias = table_name
                
                if alias.upper() not in table_aliases:
                    table_aliases[alias.upper()] = table_name
            
            # Determine single table context
            unique_tables = sorted(list(set(table_aliases.values())))
            is_single_table = len(unique_tables) == 1
            
            default_table = unique_tables[0] if is_single_table else None
            
            # Extract SELECT clause robustly (handle nested FROM in subqueries)
            if 'select_clause' not in locals():
                select_clause = ""
                # Find start of SELECT
                match_sel = re.search(r'SELECT\s+', sql_clean, re.IGNORECASE)
                
                if not match_sel:
                    return subquery_mappings
                
                select_start = match_sel.end()
                
                # Find all FROM occurrences
                from_candidates = [m.start() for m in re.finditer(r'\bFROM\b', sql_clean, re.IGNORECASE)]
                
                found_from = False
                for idx in from_candidates:
                    if idx < select_start: continue
                    
                    # Check parens balance in the segment
                    segment = sql_clean[select_start:idx]
                    if segment.count('(') == segment.count(')'):
                         select_clause = segment
                         found_from = True
                         break
                
                if not found_from:
                     # No FROM or all FROMs are inside parens? Take reset
                     # Or maybe just SELECT 1
                     select_clause = sql_clean[select_start:]

            # Parse columns (handle commas inside parens)
            columns = []
            paren_depth = 0
            current_col = ""
            for char in select_clause:
                if char == '(': paren_depth += 1
                elif char == ')': paren_depth -= 1
                elif char == ',' and paren_depth == 0:
                    columns.append(current_col.strip())
                    current_col = ""
                    continue
                current_col += char
            if current_col.strip(): columns.append(current_col.strip())
            
            # Process each column
            for col in columns:
                col_alias = None
                source_tables = set()
                col_expr = col
                
                # Alias Extraction ( =, AS, Implicit) logic...
                if '=' in col and not col.startswith("'"):
                     parts = col.split('=', 1)
                     potential_alias = parts[0].strip()
                     if ' ' not in potential_alias and '(' not in potential_alias:
                         col_alias = potential_alias
                         col_expr = parts[1].strip()
                
                if not col_alias:
                    alias_match = re.search(r'(?:\s+AS\s+|\s+)((?:\[[^\]]+\])|(?:[\w]+))\s*$', col, re.IGNORECASE)
                    if alias_match:
                        found_alias = alias_match.group(1)
                        keywords = ['END', 'ZS', 'AS', 'AND', 'OR', 'IS', 'NULL', 'NOT']
                        if found_alias.upper() not in keywords:
                            col_alias = found_alias
                            col_expr = col[:alias_match.start()].strip()
                    
                    if not col_alias:
                         col_expr = col.strip()
                         if '.' in col_expr: col_alias = col_expr.split('.')[-1].strip('[]')
                         else: col_alias = col_expr.strip('[]')

                # RESOLVE SOURCE
                final_source_table = set()
                final_source_col = set()
                
                # 0. Check for Scalar Subquery in SELECT list
                # Pattern: (SELECT ... )
                col_expr_clean = col_expr.strip().upper()
                # Simple check: starts with ( and contains SELECT nearby
                if col_expr_clean.startswith('(') and 'SELECT' in col_expr_clean[:20]:
                    # Try to extract inner SQL
                    inner_content = col_expr.strip()
                    if inner_content.startswith('(') and inner_content.endswith(')'):
                         inner_content = inner_content[1:-1].strip()
                    
                    if inner_content.upper().startswith('SELECT'):
                        # Recursive Parse
                        # print(f"DEBUG: Found Scalar Subquery in {col_alias}: {inner_content[:30]}...")
                        sub_result = self.parse_sql_column_sources(inner_content)
                        
                        # Merge results - usually a scalar subquery returns 1 col, but could be wild
                        for sub_col, sub_meta in sub_result.items():
                             if isinstance(sub_meta, dict):
                                 t = sub_meta.get('source_table', 'N/A')
                                 c = sub_meta.get('source_column', sub_col)
                                 if t != 'N/A': final_source_table.add(t)
                                 if c != 'N/A': final_source_col.add(c)
                             else:
                                 final_source_table.add(str(sub_meta))
                
                # 1. References a subquery alias?
                # Look for alias.column where alias is in subquery_mappings
                
                # Helper: Extract ALL table.column references from expression
                def extract_column_refs(expr):
                    """Extract all table.column references from complex expressions"""
                    refs = []
                    # Pattern: word.word (table.column)
                    # Handles: table.col, ISNULL(table.col), CASE WHEN table.col...
                    pattern = r'\b([a-zA-Z_][\w]*)\s*\.\s*([a-zA-Z_][\w]*)\b'
                    for match in re.finditer(pattern, expr, re.IGNORECASE):
                        table_ref = match.group(1).upper()
                        col_ref = match.group(2).upper()
                        refs.append((table_ref, col_ref))
                    return refs
                
                # Extract all column references from the expression
                column_refs = extract_column_refs(col_expr)
                found_explicit_ref = False
                
                if column_refs:
                    # We have explicit table.column references
                    for table_ref, col_ref in column_refs:
                        # Check if this reference is a Subquery Alias
                        if table_ref in subquery_mappings:
                             found_explicit_ref = True
                             inner_map = subquery_mappings[table_ref]
                             resolved_data = inner_map.get(col_ref)
                             
                             if resolved_data:
                                 if isinstance(resolved_data, dict):
                                     final_source_table.add(resolved_data.get('source_table', 'Unknown'))
                                     final_source_col.add(resolved_data.get('source_column', col_ref))
                                 else:
                                     final_source_table.add(str(resolved_data))
                                     final_source_col.add(col_ref)
                             else:
                                 wildcard_data = inner_map.get('*')
                                 if wildcard_data: 
                                     if isinstance(wildcard_data, dict):
                                         final_source_table.add(wildcard_data.get('source_table', 'Unknown'))
                                         final_source_col.add(col_ref) 
                                     else:
                                         final_source_table.add(str(wildcard_data))
                                         final_source_col.add(col_ref)
                                 else: 
                                     final_source_table.add(f"Subquery({table_ref})")
                                     final_source_col.add(col_ref)
                        
                        # Check if standard table alias
                        elif table_ref in table_aliases:
                            found_explicit_ref = True
                            t_name = table_aliases[table_ref]
                            
                            # Check if the resolved table name is actually a CTE/Subquery
                            if t_name in subquery_mappings:
                                inner_map = subquery_mappings[t_name]
                                resolved_data = inner_map.get(col_ref)
                                
                                if resolved_data:
                                     if isinstance(resolved_data, dict):
                                         final_source_table.add(resolved_data.get('source_table', 'Unknown'))
                                         final_source_col.add(resolved_data.get('source_column', col_ref))
                                     else:
                                         final_source_table.add(str(resolved_data))
                                         final_source_col.add(col_ref)
                                else:
                                     # Wildcard fallback
                                     wildcard_data = inner_map.get('*')
                                     if wildcard_data:
                                          if isinstance(wildcard_data, dict):
                                              final_source_table.add(wildcard_data.get('source_table', 'Unknown'))
                                              final_source_col.add(col_ref)
                                          else:
                                              final_source_table.add(str(wildcard_data))
                                              final_source_col.add(col_ref)
                                     else:
                                          final_source_table.add(f"Subquery({t_name})")
                                          final_source_col.add(col_ref)

                            elif t_name.startswith("SUBQUERY::"):
                                 final_source_table.add("Subquery")
                                 final_source_col.add(col_ref)
                            else:
                                final_source_table.add(t_name)
                                final_source_col.add(col_ref)
                
                # 2. Smart Inference (if no specific alias ref found)
                if not found_explicit_ref:
                     # Helper to resolving from a potential subquery if ambiguous or default
                     def resolve_from_subquery(s_alias, c_expr):
                         inner_c = c_expr.strip('[]').upper()
                         if '.' in c_expr: inner_c = c_expr.split('.')[-1].strip('[]').upper()
                         
                         if s_alias in subquery_mappings:
                             i_map = subquery_mappings[s_alias]
                             return i_map.get(inner_c, i_map.get('*', None))
                         return None

                     # Standard inference logic
                     is_literal = (col_expr.startswith("'") and col_expr.endswith("'")) or \
                                  col_expr.replace('.','',1).isdigit() or \
                                  col_expr.upper() == 'NULL'
                                  
                     if not is_literal and not final_source_table:
                         if is_single_table:
                             # Check if Default Table is a Subquery wrapper
                             if default_table.startswith("SUBQUERY::"):
                                 real_alias = default_table.split("::")[1]
                                 if real_alias in subquery_mappings:
                                     res = resolve_from_subquery(real_alias, col_expr)
                                     if res and isinstance(res, dict):
                                         final_source_table.add(res.get('source_table', 'Unknown'))
                                         final_source_col.add(res.get('source_column', 'Unknown'))
                                     elif res:
                                          final_source_table.add(str(res))
                                          final_source_col.add(col_expr)
                                     else: 
                                         final_source_table.add(f"Subquery({real_alias})")
                                         final_source_col.add(col_expr)
                             elif default_table in subquery_mappings: # Fallback
                                 res = resolve_from_subquery(default_table, col_expr)
                                 if res and isinstance(res, dict):
                                     final_source_table.add(res.get('source_table', 'Unknown'))
                                     final_source_col.add(res.get('source_column', 'Unknown'))
                                 elif res:
                                     final_source_table.add(str(res))
                                     final_source_col.add(col_expr)
                                 else:
                                     final_source_table.add(f"Subquery({default_table})")
                                     final_source_col.add(col_expr)
                             else:
                                 final_source_table.add(default_table)
                                 # If single table, assume col_expr is the column name
                                 final_source_col.add(col_expr)
                         elif unique_tables:
                             # Ambiguous
                             for t in unique_tables:
                                 if t.startswith("SUBQUERY::"):
                                      real_alias = t.split("::")[1]
                                      if real_alias in subquery_mappings:
                                          res = resolve_from_subquery(real_alias, col_expr)
                                          if res and isinstance(res, dict):
                                              final_source_table.add(res.get('source_table', 'Unknown'))
                                              final_source_col.add(res.get('source_column', 'Unknown'))
                                          elif res:
                                              final_source_table.add(str(res))
                                              final_source_col.add(col_expr)
                                          else: 
                                              final_source_table.add(f"Subquery({real_alias})")
                                              final_source_col.add(col_expr)
                                 elif t in subquery_mappings:
                                      res = resolve_from_subquery(t, col_expr)
                                      if res and isinstance(res, dict):
                                          final_source_table.add(res.get('source_table', 'Unknown'))
                                          final_source_col.add(res.get('source_column', 'Unknown'))
                                      elif res:
                                          final_source_table.add(str(res))
                                          final_source_col.add(col_expr)
                                      else:
                                          final_source_table.add(f"Subquery({t})")
                                          final_source_col.add(col_expr)
                                 else:
                                      final_source_table.add(t)
                                      final_source_col.add(col_expr)

                # Register Rich Metadata
                col_alias_clean = col_alias.strip('[]').upper()
                
                # Format Result
                res_table = ', '.join(sorted(final_source_table)) if final_source_table else 'Expression/Literal'
                res_col = ', '.join(sorted(final_source_col)) if final_source_col else 'Calculated'
                
                # Cleanup res_col (remove table alias prefixes from column names for display)
                # e.g. "mc.Region" -> "Region" if table is known
                clean_cols = []
                for c in res_col.split(','):
                    c = c.strip()
                    if '.' in c: clean_cols.append(c.split('.')[-1])
                    else: clean_cols.append(c)
                res_col_clean = ', '.join(sorted(set(clean_cols)))
                
                column_to_table[col_alias_clean] = {
                    'source_table': res_table,
                    'source_column': res_col_clean,
                    'expression': col_expr
                }
            
            # Propagate wildcard (expand *)
            if '*' in columns or (len(columns) == 1 and columns[0] == '*'):
                 # If single table, try to expand from subquery schema
                 if is_single_table and default_table.startswith("SUBQUERY::"):
                      real_alias = default_table.split("::")[1]
                      if real_alias in subquery_mappings:
                           # Expand ALL columns from the subquery
                           sub_mapping = subquery_mappings[real_alias]
                           for sub_col, sub_data in sub_mapping.items():
                               # Skip the wildcard key itself if we are expanding explicit columns
                               if sub_col == '*' and len(sub_mapping) > 1:
                                   continue
                                   
                               column_to_table[sub_col] = sub_data
                 
                 elif is_single_table and default_table in subquery_mappings:
                      # Expand from CTE or standard subquery
                      sub_mapping = subquery_mappings[default_table]
                      for sub_col, sub_data in sub_mapping.items():
                           if sub_col == '*' and len(sub_mapping) > 1:
                               continue
                           column_to_table[sub_col] = sub_data
                 
                 else:
                      # Just mark * as from default table
                      if default_table:
                          column_to_table['*'] = {
                              'source_table': default_table,
                              'source_column': '*', 
                              'expression': 'SELECT *'
                          }


        except Exception as e:
            pass
        
        return column_to_table
    
    def get_package_info(self):
        """Extract basic package information"""
        ns = '{www.microsoft.com/SqlServer/Dts}'
        return {
            'Package Name': self.root.get(f'{ns}ObjectName', 'N/A'),
            'CreationDate': self.root.get(f'{ns}CreationDate', 'N/A'),
            'CreatorName': self.root.get(f'{ns}CreatorName', 'N/A'),
            'CreatorComputerName': self.root.get(f'{ns}CreatorComputerName', 'N/A'),
            'DTSID': self.root.get(f'{ns}DTSID', 'N/A'),
            'VersionBuild': self.root.get(f'{ns}VersionBuild', 'N/A'),
            'VersionMajor': self.root.get(f'{ns}VersionMajor', '0'),
            'VersionMinor': self.root.get(f'{ns}VersionMinor', '0'),
            # Legacy compatibility
            'Creator': self.root.get(f'{ns}CreatorName', 'N/A'),
            'Version Build': self.root.get(f'{ns}VersionBuild', 'N/A')
        }
    
    def get_connections(self):
        """Extract all connection managers"""
        connections = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for conn in self.root.findall('.//DTS:ConnectionManager', self.namespaces):
            conn_name = conn.get(f'{ns}ObjectName')
            conn_type = conn.get(f'{ns}CreationName')
            conn_id = conn.get(f'{ns}DTSID')
            
            # Get connection string
            conn_string = ''
            server = 'N/A'
            database = 'N/A'
            
            conn_mgr = conn.find('.//DTS:ConnectionManager', self.namespaces)
            if conn_mgr is not None:
                conn_string = conn_mgr.get(f'{ns}ConnectionString', '')
                
                # Parse connection string
                if conn_string:
                    for part in conn_string.split(';'):
                        if '=' in part:
                            key, value = part.split('=', 1)
                            if key.strip() == 'Data Source':
                                server = value.strip()
                            elif key.strip() == 'Initial Catalog':
                                database = value.strip()
            
            connections.append({
                'Connection ID': conn_id,
                'Connection Name': conn_name,
                'Type': conn_type,
                'Server': server,
                'Database': database,
                'Full Connection String': conn_string
            })
        
        return connections
    
    def get_variables(self):
        """Extract all package variables"""
        variables = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for var in self.root.findall('.//DTS:Variable', self.namespaces):
            var_name = var.get(f'{ns}ObjectName')
            var_namespace = var.get(f'{ns}Namespace', 'User')
            var_expression = var.get(f'{ns}Expression', '')
            
            var_value_elem = var.find('.//DTS:VariableValue', self.namespaces)
            var_value = var_value_elem.text if var_value_elem is not None else ''
            
            variables.append({
                'Namespace': var_namespace,
                'Variable Name': var_name,
                'Expression': var_expression,
                'Value': str(var_value)
            })
        
        return variables
    
    def get_executables(self):
        """Extract all executables (tasks)"""
        executables = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        
        for exe in self.root.findall('.//DTS:Executable', self.namespaces):
            exe_type = exe.get(f'{ns}ExecutableType', '')
            exe_name = exe.get(f'{ns}ObjectName', 'N/A')
            exe_desc = exe.get(f'{ns}Description', '')
            
            # Check if it's SQL Task
            sql_statement = 'N/A'
            if 'ExecuteSQLTask' in exe_type:
                sql_task = exe.find('.//SQLTask:SqlTaskData', self.namespaces)
                if sql_task is not None:
                    sql_source = sql_task.get('{www.microsoft.com/sqlserver/dts/tasks/sqltask}SqlStatementSource', '')
                    sql_statement = sql_source if sql_source else 'Variable/Expression'
            
            executables.append({
                'Task Name': exe_name,
                'Type': exe_type,
                'Description': exe_desc,
                'SQL Statement': sql_statement
            })
        
        return executables
    
    def _get_dataflow_task_name(self, component):
        """Helper to get the parent Data Flow Task name for a component"""
        # Traverse up to find the parent pipeline/dataflow element
        # Strategy: Find the nearest ancestor 'pipeline' element, then find its parent Executable
        
        # First, find all Data Flow Tasks
        ns = '{www.microsoft.com/SqlServer/Dts}'
        dataflow_tasks = {}
        
        for exe in self.root.findall('.//DTS:Executable', self.namespaces):
            exe_type = exe.get(f'{ns}ExecutableType', '')
            if 'Pipeline' in exe_type or 'DTS.Pipeline' in exe_type:
                task_name = exe.get(f'{ns}ObjectName', 'N/A')
                task_id = exe.get(f'{ns}DTSID', '')
                
                # Find the pipeline element within this executable
                pipeline = exe.find('.//pipeline', {})
                if pipeline is not None:
                    dataflow_tasks[id(pipeline)] = task_name
        
        # Now find which pipeline this component belongs to
        # Walk up from component to find pipeline ancestor
        current = component
        while current is not None:
            if current.tag == 'pipeline':
                return dataflow_tasks.get(id(current), 'Unknown Data Flow')
            current = self._get_parent(current)
        
        return 'Unknown Data Flow'
    
    def _get_parent(self, element):
        """Helper to get parent element (ElementTree doesn't have built-in parent)"""
        for parent in self.root.iter():
            for child in parent:
                if child == element:
                    return parent
        return None
    
    def get_dataflow_sources(self):
        """Extract all data sources from data flow tasks (Sources + Lookups)"""
        sources = []
        
        # Find all components with Source or Lookup in class ID
        for component in self.root.findall('.//component', {}):
            comp_class = component.get('componentClassID', '')
            
            # Treat Lookup as a Source (Reference Table)
            if 'Source' in comp_class or 'Lookup' in comp_class:
                comp_name = component.get('name', 'N/A')
                comp_desc = component.get('description', '')
                
                # Get connection
                connection_name = 'N/A'
                conn_elem = component.find('.//connection', {})
                if conn_elem is not None:
                    conn_ref = conn_elem.get('connectionManagerRefId', '')
                    if 'ConnectionManagers[' in conn_ref:
                        connection_name = conn_ref.split('[')[1].split(']')[0]
                
                # Get Properties (SQL, AccessMode, etc)
                sql_command = None
                table_name = None
                access_mode = 0
                sql_var = None
                
                for prop in component.findall('.//property', {}):
                    prop_name = prop.get('name', '')
                    if prop_name == 'SqlCommand':
                        sql_command = prop.text
                    elif prop_name == 'OpenRowset':
                        table_name = prop.text
                    elif prop_name == 'AccessMode':
                        try:
                            access_mode = int(prop.text)
                        except:
                            pass
                    elif prop_name == 'SqlCommandVariable':
                        sql_var = prop.text
                
                # Handle SQL from Variable (AccessMode 3 usually)
                if access_mode == 3 and sql_var:
                    # Try to resolve variable
                    resolved_sql = self.variable_map.get(sql_var)
                    if not resolved_sql and '::' in sql_var:
                        # Try without namespace
                        resolved_sql = self.variable_map.get(sql_var.split('::')[-1])
                    
                    if resolved_sql:
                        sql_command = resolved_sql
                        comp_desc += f" (From Variable: {sql_var})"

                # Parse SQL to get column-to-table mapping
                column_to_table_map = {}
                if sql_command:
                    column_to_table_map = self.parse_sql_column_sources(sql_command)
                
                # Get output columns
                output_columns = []
                for output in component.findall('.//output', {}):
                    output_name = output.get('name', '')
                    if 'Error' not in output_name:  # Skip error outputs
                        # Cache external metadata columns for name resolution
                        ext_meta_map = {} # id -> name
                        for ext in output.findall('.//externalMetadataColumn', {}):
                            ext_id = ext.get('refId') # Usually refId is used for linkage
                            # In Source components, outputColumn refers to externalMetadataColumnId matching refId?
                            # Actually usually matching 'id' of external col?
                            # Let's map both id and refId to name just in case
                            if ext.get('id'): ext_meta_map[ext.get('id')] = ext.get('name')
                            if ext.get('refId'): ext_meta_map[ext.get('refId')] = ext.get('name')

                        for col in output.findall('.//outputColumn', {}):
                            col_name = col.get('name', '')
                            
                            # Resolve REAL column name using External Metadata
                            # (Important if aliases are used in the component)
                            ext_ref = col.get('externalMetadataColumnId')
                            lookup_col_name = col_name
                            if ext_ref and ext_ref in ext_meta_map:
                                lookup_col_name = ext_meta_map[ext_ref]
                            
                            col_type = col.get('dataType', '')
                            col_length = col.get('length', '')
                            col_precision = col.get('precision', '')
                            col_scale = col.get('scale', '')
                            
                            col_def = f"{col_type}"
                            if col_length:
                                col_def += f"({col_length})"
                            elif col_precision:
                                col_def += f"({col_precision}"
                                if col_scale:
                                    col_def += f",{col_scale}"
                                col_def += ")"
                            
                            # Determine source table for this column
                            source_table = 'N/A'
                            source_col_original = lookup_col_name # Default to lookup name
                            expression = ''
                            
                            if column_to_table_map:
                                # Look up column in the mapping
                                mapped_data = column_to_table_map.get(lookup_col_name.upper())

                                if not mapped_data:
                                     mapped_data = column_to_table_map.get('*')
                                
                                if mapped_data:
                                    if isinstance(mapped_data, dict):
                                        source_table = mapped_data.get('source_table', 'N/A')
                                        source_col_original = mapped_data.get('source_column', lookup_col_name)
                                        expression = mapped_data.get('expression', '')
                                    else:
                                        source_table = str(mapped_data)
                                        source_col_original = 'N/A' 
                                        expression = ''
                                
                                # Fallback: Check for wildcard '*' mapping
                                if source_table == 'N/A' and '*' in column_to_table_map:
                                    wildcard_data = column_to_table_map['*']
                                    if isinstance(wildcard_data, dict):
                                        source_table = wildcard_data.get('source_table', 'N/A')
                                    else:
                                        source_table = str(wildcard_data)
                                    
                            elif table_name:
                                # If no SQL query, use the table name from OpenRowset
                                source_table = table_name
                            
                            elif 'FlatFileSource' in comp_class or 'ExcelSource' in comp_class:
                                source_table = connection_name
                                expression = 'File Read'

                            elif 'Lookup' in comp_class and not sql_command:
                                # Lookups might use Table Name property too?
                                # Usually 'SqlCommand' is set for query mode. 
                                # If 'NoCache' or 'FullCache' with table, might use OpenRowset too?
                                # Let's assume if table_name found it works.
                                pass

                            output_columns.append({
                                'Column Alias': col_name,
                                'Original Column': source_col_original,
                                'Source Table': source_table,
                                'Expression/Logic': expression,
                                'Data Type': col_def
                            })
                
                source_info = {
                    'Data Flow Task': self._get_dataflow_task_name(component),
                    'Component Name': comp_name,
                    'Component Type': comp_class,
                    'Connection': connection_name,
                    'Table/View': table_name if table_name else 'N/A',
                    'SQL Query': sql_command if sql_command else 'N/A',
                    'Description': comp_desc,
                    'Output Columns': output_columns
                }
                
                sources.append(source_info)
        
        return sources
    
    def get_dataflow_destinations(self):
        """Extract all destinations from data flow tasks"""
        destinations = []
        
        for component in self.root.findall('.//component', {}):
            comp_class = component.get('componentClassID', '')
            
            if 'Destination' in comp_class:
                comp_name = component.get('name', 'N/A')
                comp_desc = component.get('description', '')
                
                # Get connection
                connection_name = 'N/A'
                conn_elem = component.find('.//connection', {})
                if conn_elem is not None:
                    conn_ref = conn_elem.get('connectionManagerRefId', '')
                    if 'ConnectionManagers[' in conn_ref:
                        connection_name = conn_ref.split('[')[1].split(']')[0]
                
                # Get table name or SQL command
                table_name = 'N/A'
                sql_command = ''
                
                for prop in component.findall('.//property', {}):
                    prop_name = prop.get('name', '')
                    if prop_name == 'OpenRowset':
                        table_name = prop.text
                    elif prop_name == 'SqlCommand':
                        sql_command = prop.text
                
                # Fallback: If no table/SQL, check connection string (File Path)
                if table_name == 'N/A' and not sql_command:
                    conn_elem = component.find('.//connection', {})
                    if conn_elem is not None:
                        conn_ref = conn_elem.get('connectionManagerRefId', '')
                        if conn_ref and conn_ref in self.conn_map:
                             table_name = self.conn_map[conn_ref]
                
                # Get input columns (yang masuk ke destination)
                input_columns = []
                for input_elem in component.findall('.//input', {}):
                    input_name = input_elem.get('name', '')
                    if 'Error' not in input_name:
                        for col in input_elem.findall('.//inputColumn', {}):
                            col_name = col.get('cachedName', col.get('name', ''))
                            col_type = col.get('cachedDataType', '')
                            col_length = col.get('cachedLength', '')
                            
                            # Get external metadata (target column)
                            ext_meta_id = col.get('externalMetadataColumnId', '')
                            target_col_name = col_name  # Default sama
                            
                            if ext_meta_id:
                                # Cari external metadata column
                                for ext_col in input_elem.findall('.//externalMetadataColumn', {}):
                                    if ext_col.get('refId', '') == ext_meta_id:
                                        target_col_name = ext_col.get('name', col_name)
                                        break
                            
                            col_def = f"{col_type}"
                            if col_length:
                                col_def += f"({col_length})"
                            
                            input_columns.append({
                                'Source Column': col_name,
                                'Target Column': target_col_name,
                                'Data Type': col_def,
                                'Destination': comp_name
                            })
                
                dest_info = {
                    'Data Flow Task': self._get_dataflow_task_name(component),
                    'Component Name': comp_name,
                    'Component Type': comp_class,
                    'Connection': connection_name,
                    'Target Table': table_name,
                    'SQL Query': sql_command,
                    'Description': comp_desc,
                    'Input Columns': input_columns
                }
                
                destinations.append(dest_info)
        
        return destinations
    
    def get_transformations(self):
        """Extract all transformations"""
        transformations = []
        
        transform_classes = [
            'Microsoft.DerivedColumn',
            'Microsoft.MergeJoin',
            'Microsoft.Sort',
            'Microsoft.Lookup',
            'Microsoft.ConditionalSplit',
            'Microsoft.UnionAll',
            'Microsoft.DataConversion',
            'Microsoft.Aggregate'
        ]
        
        for component in self.root.findall('.//component', {}):
            comp_class = component.get('componentClassID', '')
            
            if any(tc in comp_class for tc in transform_classes):
                comp_name = component.get('name', 'N/A')
                comp_desc = component.get('description', '')
                
                details = {}
                
                # Derived Column - extract expressions
                if 'DerivedColumn' in comp_class:
                    expressions = []
                    for output in component.findall('.//output', {}):
                        for col in output.findall('.//outputColumn', {}):
                            col_name = col.get('name', '')
                            for prop in col.findall('.//property', {}):
                                if prop.get('name') == 'FriendlyExpression':
                                    expr = prop.text or ''
                                    expressions.append(f"{col_name} = {expr}")
                    details['Expressions'] = expressions
                
                # Merge Join - extract join type and keys
                elif 'MergeJoin' in comp_class:
                    for prop in component.findall('.//property', {}):
                        prop_name = prop.get('name', '')
                        if prop_name == 'JoinType':
                            join_type_map = {0: 'FULL', 1: 'LEFT', 2: 'INNER'}
                            details['Join Type'] = join_type_map.get(int(prop.text or 1), 'INNER')
                        elif prop_name == 'NumKeyColumns':
                            details['Key Columns'] = prop.text
                
                # Sort - extract sort columns
                elif 'Sort' in comp_class:
                    sort_cols = []
                    for input_elem in component.findall('.//input', {}):
                        for col in input_elem.findall('.//inputColumn', {}):
                            sort_pos = col.find('.//property[@name="NewSortKeyPosition"]', {})
                            if sort_pos is not None and sort_pos.text != '0':
                                col_name = col.get('cachedName', col.get('name', ''))
                                sort_cols.append(col_name)
                    details['Sort Columns'] = sort_cols
                
                transformations.append({
                    'Component Name': comp_name,
                    'Type': comp_class.replace('Microsoft.', ''),
                    'Description': comp_desc,
                    'Details': str(details)
                })
        
        return transformations

    def _get_dataflow_tasks(self):
        """Helper to find all Data Flow Task executables"""
        dfts = []
        ns = '{www.microsoft.com/SqlServer/Dts}'
        for exe in self.root.findall('.//DTS:Executable', self.namespaces):
            exe_type = exe.get(f'{ns}ExecutableType', '')
            if 'Pipeline' in exe_type or 'DTS.Pipeline' in exe_type:
                dfts.append(exe)
        return dfts

    def _trace_column_lineage_topology(self):
        """Holy Grail: Topological Tracing using LineageIDs (Multi-Source Capable)"""
        lineage_results = []
        
        # Process each Data Flow Task separately (LineageIDs are scoped to DFT)
        for dft in self._get_dataflow_tasks():
            pipeline = dft.find('.//pipeline', {})
            if pipeline is None: continue
            
            dft_name = dft.get('{www.microsoft.com/SqlServer/Dts}ObjectName', 'Unknown')
            
            # 1. Map Components and Paths
            # Use refId preferably (newer SSIS) or id (older)
            components = {}
            for c in pipeline.findall('.//component', {}):
                cid = c.get('refId') or c.get('id')
                if cid: components[cid] = c

            paths = pipeline.findall('.//path', {})
            
            # Graph: ComponentID -> [Downstream ComponentIDs]
            # Path maps OutputID (Start) -> InputID (End)
            # We need to map InputID -> ComponentID to traverse
            
            input_to_comp = {}
            for cid, comp in components.items():
                for inp in comp.findall('.//input', {}):
                    lid = inp.get('refId') or inp.get('id')
                    if lid: input_to_comp[lid] = cid
            
            adj_list = defaultdict(list)
            in_degree = defaultdict(int)
            
            # Initialize in-degree for all components
            for cid in components: in_degree[cid] = 0
                
            for path in paths:
                start_id = path.get('startId') # Output ID
                end_id = path.get('endId')     # Input ID
                
                # Find which component owns start_id? (Not strictly needed for adj)
                # Find which component owns end_id
                target_comp_id = input_to_comp.get(end_id)
                
                # We need source component ID.
                # OutputID -> ComponentID
                src_comp_id = None
                for cid, comp in components.items():
                    for out in comp.findall('.//output', {}):
                        oid = out.get('refId') or out.get('id')
                        if oid == start_id:
                            src_comp_id = cid
                            break
                    if src_comp_id: break
                
                if src_comp_id and target_comp_id:
                    adj_list[src_comp_id].append(target_comp_id)
                    in_degree[target_comp_id] += 1
            
            # 2. Lineage Map: LineageID -> LIST of SourceInfo Dicts
            # SourceInfo: {SourceComponent, SourceTable, OriginalColumn, Expression, ...}
            # List structure supports one column derived from multiple sources (1-to-Many Lineage)
            lineage_id_map = defaultdict(list)
            
            # 3. Topological Traversal (Queue based)
            queue = [cid for cid, deg in in_degree.items() if deg == 0]
            
            # Pre-calculate Source SQL info to avoid re-parsing
            # We can use our existing methods, filtering by component name or ID
            source_configs = self.get_dataflow_sources()
            source_config_map = {s['Component Name']: s for s in source_configs if s.get('Data Flow Task') == dft_name}
            
            processed_count = 0
            while queue:
                cid = queue.pop(0)
                comp = components[cid]
                processed_count += 1
                
                comp_class = comp.get('componentClassID', '')
                comp_name = comp.get('name', '')
                
                # --- PROCESS COMPONENT ---
                
                # A. Source Component / Lookup (Generator)
                if 'Source' in comp_class or 'Lookup' in comp_class:
                    if comp_name in source_config_map:
                        src_config = source_config_map[comp_name]
                        
                        # Map Outputs based on Alias (Name) match
                        # We need to find the LineageID for each output column
                        for output in comp.findall('.//output', {}):
                            for col in output.findall('.//outputColumn', {}):
                                lid = col.get('lineageId')
                                name = col.get('name')
                                
                                # Find matching config
                                col_config = next((c for c in src_config['Output Columns'] if c['Column Alias'] == name), None)
                                
                                if col_config and lid:
                                    # Initialize as LIST with one source
                                    lineage_id_map[lid] = [{
                                        'Source Component': comp_name,
                                        'Source Table': col_config['Source Table'],
                                        'Original Column': col_config['Original Column'],
                                        'Expression/Logic': col_config['Expression/Logic'],
                                        'Source Type': col_config['Data Type']
                                    }]
                
                # B. Transformations (Pass-through vs Async)
                else:
                    # Logic: 
                    # 1. Identify Input Lines (Upstream Sources)
                    # 2. Identify Output Columns
                    #    - If Synchronous (share LineageID with Input), inherit Source info.
                    #    - If Asynchronous (New LineageID), try to map input -> output.
                    
                    # Map: InputLineageID -> List[UpstreamInfo]
                    # We can access lineage_id_map directly.
                    
                    # Store Input Columns metadata for Expression lookup
                    # Map: Name -> LineageID
                    input_name_map = {}
                    for inp in comp.findall('.//input', {}):
                        for col in inp.findall('.//inputColumn', {}):
                            lid = col.get('lineageId')
                            # Prefer 'name' (Source Name) or 'cachedName' (Input Name)?
                            # SSIS Expressions usually reference the Input Column Name.
                            cname = col.get('name')
                            if cname: input_name_map[cname] = lid
                            cached_name = col.get('cachedName')
                            if cached_name: input_name_map[cached_name] = lid

                    for output in comp.findall('.//output', {}):
                        sync_id = output.get('synchronousInputId') # If set, this is synchronous
                        
                        for out_col in output.findall('.//outputColumn', {}):
                            lid = out_col.get('lineageId')
                            if not lid: continue
                            
                            # Case 1: Existing LineageID (Pass-through) - ALREADY HANDLED by Python Ref logic?
                            # No, lineage_id_map persists. 
                            # If pass-through, no action needed unless we want to tag "Passed through X".
                            
                            # Case 2: New Lineage ID (Derived or Async)
                            if lid in lineage_id_map:
                                # Start with existing info (flow through)
                                # Logic to update expression?
                                pass
                            else:
                                # New ID. Need to derive source.
                                new_sources = []
                                
                                if 'UnionAll' in comp_class:
                                    # Union Logic (Match by Index)
                                    out_cols = list(output.findall('.//outputColumn', {}))
                                    try:
                                        idx = out_cols.index(out_col)
                                        used_source_keys = set()
                                        
                                        for inp in comp.findall('.//input', {}):
                                            in_cols = list(inp.findall('.//inputColumn', {}))
                                            if idx < len(in_cols):
                                                in_lid = in_cols[idx].get('lineageId')
                                                if in_lid in lineage_id_map:
                                                    upstream_list = lineage_id_map[in_lid]
                                                    for src in upstream_list:
                                                        # Deduplicate by Source Table + Col
                                                        key = (src['Source Table'], src['Original Column'])
                                                        if key not in used_source_keys:
                                                            used_source_keys.add(key)
                                                            new_src = src.copy()
                                                            new_src['Expression/Logic'] += f" -> Union({comp_name})"
                                                            new_sources.append(new_src)
                                    except:
                                        pass
                                


                                elif 'DataConvert' in comp_class:
                                    # Data Conversion: Map Output -> Input via SourceInputColumnLineageID
                                    source_lid_prop = None
                                    for prop in out_col.findall('.//property', {}):
                                        if prop.get('name') == 'SourceInputColumnLineageID':
                                            source_lid_prop = prop.text
                                            if source_lid_prop:
                                                 # Strip #{ } wrapper if present
                                                 source_lid_prop = source_lid_prop.strip().replace('#{', '').replace('}', '')
                                                 # print(f"DEBUG: DataConv Key: {source_lid_prop} Found? {source_lid_prop in lineage_id_map}")
                                            break
                                    
                                    if source_lid_prop and source_lid_prop in lineage_id_map:
                                        upstream_list = lineage_id_map[source_lid_prop]
                                        for src in upstream_list:
                                            new_src = src.copy()
                                            new_src['Expression/Logic'] += f" -> Conv({out_col.get('dataType')})"
                                            new_sources.append(new_src)

                                elif 'MergeJoin' in comp_class or 'Sort' in comp_class or 'Aggregate' in comp_class:
                                    # Async Pass-Through by Name
                                    name = out_col.get('name')
                                    src_lid = input_name_map.get(name)
                                    
                                    if src_lid and src_lid in lineage_id_map:
                                        upstream_list = lineage_id_map[src_lid]
                                        for src in upstream_list:
                                            new_src = src.copy()
                                            # We generally just pass through, but can tag if needed
                                            # new_src['Expression/Logic'] += f" -> {comp_name}"
                                            new_sources.append(new_src)
                                    else:
                                        # Fallback
                                        new_sources.append({
                                            'Source Component': comp_name,
                                            'Source Table': 'Transformation',
                                            'Original Column': name,
                                            'Expression/Logic': f'Async ({comp_name})',
                                            'Source Type': out_col.get('dataType')
                                        })

                                elif 'DerivedColumn' in comp_class:
                                    # Expression Parsing Logic
                                    expr = ''
                                    for prop in out_col.findall('.//property', {}):
                                        if prop.get('name') == 'FriendlyExpression':
                                            expr = prop.text
                                    
                                    if expr:
                                        # Parse dependencies: [ColName]
                                        deps = re.findall(r'\[(.*?)\]', expr)
                                        # Filter out variables (User::...)
                                        col_deps = [d for d in deps if '::' not in d]
                                        
                                        if not col_deps and deps:
                                            # Depends only on variables?
                                            new_sources.append({
                                                'Source Component': comp_name,
                                                'Source Table': 'Variable/Expression',
                                                'Original Column': 'Expression',
                                                'Expression/Logic': expr,
                                                'Source Type': 'Derived'
                                            })
                                        
                                        used_source_keys = set()
                                        for d in col_deps:
                                            # Find input LineageID
                                            in_lid = input_name_map.get(d)
                                            if in_lid and in_lid in lineage_id_map:
                                                upstream_list = lineage_id_map[in_lid]
                                                for src in upstream_list:
                                                    key = (src['Source Table'], src['Original Column'])
                                                    # Allow multiple columns if different tables?
                                                    # Actually, allows same table different cols (ColA + ColB).
                                                    # Key should include d (the input column name used).
                                                    # Let's just append everything.
                                                    new_src = src.copy()
                                                    new_src['Expression/Logic'] = f"{src['Expression/Logic']} -> Derived({d})"
                                                    new_sources.append(new_src)
                                    
                                    if not new_sources:
                                        # Fallback if no deps found or parse failed
                                        new_sources.append({
                                            'Source Component': comp_name,
                                            'Source Table': 'Derived / Transformation',
                                            'Original Column': out_col.get('name'),
                                            'Expression/Logic': expr or 'Derived',
                                            'Source Type': out_col.get('dataType')
                                        })
                                
                                else:
                                    # Unknown origin (Async transform?)
                                    new_sources.append({
                                        'Source Component': comp_name,
                                        'Source Table': 'Transformation',
                                        'Original Column': out_col.get('name'),
                                        'Expression/Logic': 'Unknown Logic',
                                        'Source Type': out_col.get('dataType')
                                    })
                                
                                # Assign list
                                lineage_id_map[lid] = new_sources

                # C. Destination Component (Consumer)
                if 'Destination' in comp_class:
                     # Get Target Table info
                     target_table = 'N/A'
                     for prop in comp.findall('.//property', {}):
                         if prop.get('name') == 'OpenRowset': target_table = prop.text
                     
                     if target_table == 'N/A':
                         conn_elem = comp.find('.//connection', {})
                         if conn_elem is not None:
                             cm_ref = conn_elem.get('connectionManagerRefId')
                             if cm_ref and cm_ref in self.conn_map:
                                 target_table = self.conn_map[cm_ref]
                     
                     # Map Inputs
                     for inp in comp.findall('.//input', {}):
                         for in_col in inp.findall('.//inputColumn', {}):
                             lid = in_col.get('lineageId')
                             target_col = in_col.get('cachedName', in_col.get('name')) # Destination Col Name
                             
                             # Resolve External Metadata if available (to get real Target Column)
                             ext_id = in_col.get('externalMetadataColumnId')
                             if ext_id:
                                 for ext in inp.findall('.//externalMetadataColumn', {}):
                                     if ext.get('refId') == ext_id:
                                         target_col = ext.get('name')
                                         break
                             
                             if lid in lineage_id_map:
                                 src_infos = lineage_id_map[lid] # LIST of sources
                                 for src_info in src_infos:
                                     lineage_results.append({
                                         'Source Component': src_info['Source Component'],
                                         'Source Table': src_info['Source Table'],
                                         'Original Column': src_info['Original Column'],
                                         'Expression/Logic': src_info.get('Expression/Logic', ''),
                                         'Source Column': src_info['Original Column'], # Tracked origin name
                                         'Source Type': src_info.get('Source Type', ''),
                                         'Destination Component': comp_name,
                                         'Destination Table': target_table,
                                         'Destination Column': target_col,
                                         'Destination Type': in_col.get('cachedDataType', '')
                                     })
            
                # Push downstream
                for neighbor in adj_list[cid]:
                    in_degree[neighbor] -= 1
                    if in_degree[neighbor] == 0:
                        queue.append(neighbor)
                        
        return lineage_results

    def get_column_lineage(self):
        """Extract complete column lineage using Topological LineageID Tracing"""
        try:
            return self._trace_column_lineage_topology()
        except Exception as e:
            # Fallback to legacy method if graph fails? 
            # Or just return empty and log?
            print(f"Graph Lineage Failed: {e}")
            return []

    def refine_package_sql(self):
        """
        Refines SQL commands in the package using SQLRefiner.
        Returns:
            list: List of changes (dict with Component, OldSQL, NewSQL)
        """
        changes = []
        refiner = SQLRefiner()
        
        # Iterate over all components (Data Flow Components)
        # We need to look for 'SqlCommand' property in components
        for pipeline in self.root.findall('.//DTS:Executable', self.namespaces):
            # Check if it's a Data Flow Task
            if 'Pipeline' in pipeline.get(f'{{{self.namespaces["DTS"]}}}CreationName', ''):
                obj_data = pipeline.find('.//DTS:ObjectData', self.namespaces)
                if obj_data:
                    pipeline_xml = obj_data.find('.//pipeline', self.namespaces) # Note: pipeline has no namespace prefix usually or different one?
                    # Actually valid pipeline XML inside ObjectData uses generic 'pipeline' tag or defaults.
                    # Let's try finding all components recursively from root might be easier if we just want to patch properties.
                    pass 

        # Global search for properties with Name='SqlCommand'
        # This is safer as it covers all components
        for prop in self.root.findall('.//property[@name="SqlCommand"]', {}): # property tag might not have namespace
             # Wait, elementtree findall with empty namespace dict might miss if they have namespace.
             # In .dtsx, <property> is usually under <component>.
             # Let's verify the XML structure from previous file reads.
             # Usually: <component ...> <properties> <property name="SqlCommand">...</property>
             pass

        # Let's use the iterator we used in get_dataflow_sources but for patching
        # Re-implementing simplified traversal
        dataflow_tasks = self._get_dataflow_tasks()
        
        for task in dataflow_tasks:
            obj_data = task.find('.//DTS:ObjectData', self.namespaces)
            if not obj_data: continue
            
            pipeline_inner = obj_data.find('.//pipeline') # Usually no namespace for inner pipeline
            if pipeline_inner is None: continue
            
            for component in pipeline_inner.findall('.//component'):
                comp_name = component.get('name')
                
                # specific check for SqlCommand property
                for prop in component.findall('.//property'):
                    if prop.get('name') == 'SqlCommand':
                        original_sql = prop.text
                        if original_sql:
                            refined_sql = refiner.refine(original_sql)
                            
                            # Normalize line endings for comparison
                            if original_sql.strip() != refined_sql.strip():
                                prop.text = refined_sql
                                changes.append({
                                    "component": comp_name,
                                    "old": original_sql,
                                    "new": refined_sql
                                })
        
        return changes

# File uploader
def render_package_details(extractor, file_path=None):
    """Render details for a single package using the extractor instance"""
    # ... (existing extractions) ...
    package_info = extractor.get_package_info()
    connections = extractor.get_connections()
    variables = extractor.get_variables()
    executables = extractor.get_executables()
    sources = extractor.get_dataflow_sources()
    destinations = extractor.get_dataflow_destinations()
    transformations = extractor.get_transformations()
    lineage = extractor.get_column_lineage()
    
    # Package Info (Updated Design)
    # Package Info
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Package Name", package_info['Package Name'])
    with col2:
        st.metric("Creator", package_info['Creator'])
    with col3:
        st.metric("Version", package_info['Version Build'])
    
    with st.expander("📋 Full Package Details"):
        st.json(package_info)
    
    st.divider()
    
    # Create tabs
    # Create tabs
    tabs = st.tabs([
        "🔌 Connections",
        "📥 Sources", 
        "📤 Destinations",
        "🔄 Transformations",
        "🔗 Column Lineage",
        "🛡️ Quality & Stats",
        "⚙️ Variables",
        "📋 Tasks",
        "🛠️ SQL Refiner",
        "💾 Export"
    ])
    tab1, tab2, tab3, tab4, tab5, tab_qual, tab6, tab7, tab_refine, tab8 = tabs
    
    with tab1:
        st.subheader("Connection Managers")
        if connections:
            df_conn = pd.DataFrame(connections)
            st.dataframe(df_conn, use_container_width=True, height=400)
            
            st.download_button(
                "📥 Download Connections CSV",
                df_conn.to_csv(index=False),
                file_name="ssis_connections.csv",
                mime="text/csv"
            )
        else:
            st.info("No connections found")
    
    with tab2:
        st.subheader("Data Sources")
        if sources:
            # Group sources by Data Flow Task
            from collections import defaultdict
            sources_by_flow = defaultdict(list)
            for source in sources:
                flow_name = source.get('Data Flow Task', 'Unknown')
                sources_by_flow[flow_name].append(source)
            
            # Display each data flow separately
            for flow_name, flow_sources in sources_by_flow.items():
                st.markdown(f"### 🔄 Data Flow: `{flow_name}`")
                st.caption(f"{len(flow_sources)} source component(s)")
                
                for idx, source in enumerate(flow_sources, 1):
                    with st.expander(f"📥 {source['Component Name']}", expanded=(idx==1 and len(sources_by_flow)==1)):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Connection:** {source['Connection']}")
                            st.write(f"**Table/View:** {source['Table/View']}")
                            st.write(f"**Type:** {source['Component Type']}")
                        with col2:
                            st.write(f"**Column Count:** {len(source['Output Columns'])}")
                        
                        if source['SQL Query'] != 'N/A':
                            st.write("**SQL Query:**")
                            st.code(source['SQL Query'], language='sql')
                        
                        if source['Output Columns']:
                            st.write("**Output Columns:**")
                            df_cols = pd.DataFrame(source['Output Columns'])
                            st.dataframe(df_cols, use_container_width=True)
                
                st.divider()
        else:
            st.info("No data sources found")
    
    with tab3:
        st.subheader("Destinations")
        if destinations:
            # Group destinations by Data Flow Task
            from collections import defaultdict
            dests_by_flow = defaultdict(list)
            for dest in destinations:
                flow_name = dest.get('Data Flow Task', 'Unknown')
                dests_by_flow[flow_name].append(dest)
            
            # Create a lookup for source tables using lineage
            dest_col_source_map = {}
            for item in lineage:
                key = (item['Destination Component'], item['Destination Column'])
                dest_col_source_map[key] = {
                    'Source Table': item['Source Table'],
                    'Original Column': item.get('Original Column', 'N/A'),
                    'Expression/Logic': item.get('Expression/Logic', '')
                }
                
            # Create a lookup for SQL Query by Data Flow Task (from Sources)
            dft_sql_map = {}
            if sources:
                for src in sources:
                    dft = src.get('Data Flow Task')
                    sql = src.get('SQL Query')
                    if dft and sql and sql != 'N/A':
                        dft_sql_map[dft] = sql
            
            # Display each data flow separately
            for flow_name, flow_dests in dests_by_flow.items():
                st.markdown(f"### 🔄 Data Flow: `{flow_name}`")
                st.caption(f"{len(flow_dests)} destination component(s)")
                
                for idx, dest in enumerate(flow_dests, 1):
                    with st.expander(f"📤 {dest['Component Name']}", expanded=(idx==1 and len(dests_by_flow)==1)):
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Connection:** {dest['Connection']}")
                            st.write(f"**Target Table:** {dest['Target Table']}")
                        with col2:
                            st.write(f"**Type:** {dest['Component Type']}")
                            st.write(f"**Column Count:** {len(dest['Input Columns'])}")
                        
                        if dest.get('SQL Query') and dest['SQL Query'] != 'N/A':
                            st.write("**SQL Query:**")
                            st.code(dest['SQL Query'], language='sql')
                        
                        # Extract and Display JOIN Keys (from Source, linked by Flow Name)
                        source_sql = dft_sql_map.get(flow_name)
                        if source_sql:
                             # Only show if not already shown via dest['SQL Query'] checking (deduplication)
                             # or if dest['SQL Query'] was missing. 
                             # Simpler: Just show Join Keys if we have source SQL.
                             try:
                                 join_keys = extractor.extract_join_keys(source_sql)
                                 if join_keys:
                                     with st.expander("🧩 Join Logic & Keys", expanded=False):
                                         df_joins = pd.DataFrame(join_keys)
                                         cols = ['Original Table Alias', 'Original Column', 'Source Table', 'Source Column']
                                         st.dataframe(df_joins[cols], use_container_width=True)
                             except Exception as e:
                                 pass # Silent fail if extraction issues
                                 
                        if dest['Input Columns']:
                            st.write("**Column Mappings:**")
                            
                            # Enrich input columns with source table info
                            enriched_cols = []
                            for col in dest['Input Columns']:
                                col_copy = col.copy()
                                key = (dest['Component Name'], col['Target Column'])
                                source_info = dest_col_source_map.get(key, {})
                                
                                col_copy['Source Table'] = source_info.get('Source Table', 'N/A')
                                col_copy['Original Column'] = source_info.get('Original Column', 'N/A')
                                col_copy['Expression/Logic'] = source_info.get('Expression/Logic', '')
                                
                                enriched_cols.append(col_copy)
                            
                            df_cols = pd.DataFrame(enriched_cols)
                            # Reorder columns to show Source Table prominently
                            cols_order = ['Source Column', 'Original Column', 'Source Table', 'Expression/Logic', 'Target Column', 'Data Type', 'Destination']
                            # Keep only columns that exist (in case structure changes)
                            final_cols = [c for c in cols_order if c in df_cols.columns]
                            st.dataframe(df_cols[final_cols], use_container_width=True)
                
                st.divider()
        else:
            st.info("No destinations found")
    
    with tab4:
        st.subheader("Transformations")
        if transformations:
            df_trans = pd.DataFrame(transformations)
            st.dataframe(df_trans, use_container_width=True, height=400)
            
            st.download_button(
                "📥 Download Transformations CSV",
                df_trans.to_csv(index=False),
                file_name="ssis_transformations.csv",
                mime="text/csv"
            )
        else:
            st.info("No transformations found")
    
    with tab5:
        st.subheader("🔗 Column Lineage (Source → Destination)")
        if lineage:
            df_lineage = pd.DataFrame(lineage)
            
            # --- Filtering ---
            with st.expander("🔎 Advanced Search & Filter", expanded=True):
                c1, c2 = st.columns(2)
                with c1:
                    filter_source = st.multiselect("Filter Source Table", options=sorted(df_lineage['Source Table'].astype(str).unique()))
                with c2:
                    filter_dest = st.multiselect("Filter Dest Table", options=sorted(df_lineage['Destination Table'].astype(str).unique()))
                
                search_term = st.text_input("Search Column Name (Source or Destination)", "")
            
            if filter_source:
                df_lineage = df_lineage[df_lineage['Source Table'].isin(filter_source)]
            if filter_dest:
                df_lineage = df_lineage[df_lineage['Destination Table'].isin(filter_dest)]
            if search_term:
                # Search in Source Column AND Destination Column
                mask = df_lineage['Source Column'].astype(str).str.contains(search_term, case=False, na=False) | \
                       df_lineage['Destination Column'].astype(str).str.contains(search_term, case=False, na=False)
                df_lineage = df_lineage[mask]
            
            st.dataframe(df_lineage, use_container_width=True, height=500)
            
            st.download_button(
                "📥 Download Column Lineage CSV",
                df_lineage.to_csv(index=False),
                file_name="ssis_column_lineage.csv",
                mime="text/csv"
            )
            
            # Group by destination table
            st.subheader("📊 Lineage by Destination Table")
            for dest_table in df_lineage['Destination Table'].unique():
                with st.expander(f"🎯 {dest_table}"):
                    df_table = df_lineage[df_lineage['Destination Table'] == dest_table]
                    st.dataframe(df_table, use_container_width=True)
        else:
            st.info("No column lineage found")
    
    with tab_qual:
        render_quality_dashboard(lineage)

    with tab6:
        st.subheader("Variables")
        if variables:
            df_vars = pd.DataFrame(variables)
            st.dataframe(df_vars, use_container_width=True, height=400)
            
            st.download_button(
                "📥 Download Variables CSV",
                df_vars.to_csv(index=False),
                file_name="ssis_variables.csv",
                mime="text/csv"
            )
        else:
            st.info("No variables found")
    
    with tab7:
        st.subheader("Tasks/Executables")
        if executables:
            df_exe = pd.DataFrame(executables)
            st.dataframe(df_exe, use_container_width=True, height=400)
            
            st.download_button(
                "📥 Download Tasks CSV",
                df_exe.to_csv(index=False),
                file_name="ssis_tasks.csv",
                mime="text/csv"
            )
        else:
            st.info("No tasks found")
    
    with tab_refine:
        st.subheader("🛠️ SQL Refiner & Standardization")
        st.info("Scan package for messy SQL (inconsistent keywords, aliases, etc.) and standardize it.")
        
        # Check session state for changes specific to this package
        changes_key = f'refine_changes_{package_info["Package Name"]}'
        
        c1, c2 = st.columns([1, 2])
        with c1:
            if st.button("Scan & Refine SQL Scripts", key=f"btn_refine_{package_info['Package Name']}"):
                changes = extractor.refine_package_sql()
                st.session_state[changes_key] = changes
                if not changes:
                    st.success("✅ All SQL scripts look standard!")
        
        if changes_key in st.session_state:
            changes = st.session_state[changes_key]
            
            if changes:
                st.warning(f"Found {len(changes)} components with potential improvements.")
                
                for idx, change in enumerate(changes):
                    with st.expander(f"📝 {change['component']}"):
                        col_a, col_b = st.columns(2)
                        with col_a:
                            st.caption("Original")
                            st.code(change['old'], language='sql')
                        with col_b:
                            st.caption("Refined")
                            st.code(change['new'], language='sql')
                
                st.divider()
                
                # Save Action
                if file_path:
                    if st.button("💾 Save Refined Package", key=f"btn_save_{package_info['Package Name']}"):
                        try:
                            # Re-register namespace to avoid ns0 prefixes
                            ET.register_namespace('', "www.microsoft.com/SqlServer/Dts")
                            ET.register_namespace('DTS', "www.microsoft.com/SqlServer/Dts")
                            
                            tree = ET.ElementTree(extractor.root)
                            tree.write(file_path, encoding='utf-8', xml_declaration=True)
                            
                            st.success(f"Successfully saved refined package to {file_path}")
                            st.balloons()
                            
                            # Clear state
                            del st.session_state[changes_key]
                            
                            # Optional: Trigger reload?
                            # st.experimental_rerun()
                            
                        except Exception as e:
                            st.error(f"Failed to save: {e}")
                else:
                    st.warning("Cannot save directly (File uploaded). Download the refined version below.")
                    rough_string = ET.tostring(extractor.root, encoding='utf-8')
                    st.download_button(
                        "📥 Download Refined .dtsx", 
                        rough_string, 
                        file_name=f"refined_{package_info['Package Name']}.dtsx",
                        mime="text/xml"
                    )

    with tab8:
        st.subheader("💾 Export Complete Metadata")
        
        # Create comprehensive report
        report = f"""
# SSIS Package Metadata Report
Package: {package_info['Package Name']}
Date Extracted: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}

## Summary
- Connections: {len(connections)}
- Data Sources: {len(sources)}
- Destinations: {len(destinations)}
- Transformations: {len(transformations)}
- Variables: {len(variables)}
- Tasks: {len(executables)}
- Column Mappings: {len(lineage)}

## Package Details
{pd.DataFrame([package_info]).to_markdown()}

## Connections
{pd.DataFrame(connections).to_markdown() if connections else 'None'}

## Data Sources
"""
        
        for source in sources:
            report += f"\n### {source['Component Name']}\n"
            report += f"- Connection: {source['Connection']}\n"
            report += f"- Table/View: {source['Table/View']}\n"
            if source['SQL Query'] != 'N/A':
                report += f"- SQL: ```sql\n{source['SQL Query']}\n```\n"
            if source['Output Columns']:
                report += f"\nColumns:\n{pd.DataFrame(source['Output Columns']).to_markdown()}\n"
        
        report += "\n## Destinations\n"
        for dest in destinations:
            report += f"\n### {dest['Component Name']}\n"
            report += f"- Connection: {dest['Connection']}\n"
            report += f"- Target Table: {dest['Target Table']}\n"
            if dest['Input Columns']:
                report += f"\nColumns:\n{pd.DataFrame(dest['Input Columns']).to_markdown()}\n"
        
        if lineage:
            report += "\n## Column Lineage\n"
            report += pd.DataFrame(lineage).to_markdown()
        
        st.download_button(
            "📥 Download Complete Report (Markdown)",
            report,
            file_name=f"ssis_metadata_{package_info['Package Name']}.md",
            mime="text/markdown"
        )

# ==========================================
# Main Application Logic
# ==========================================

# Sidebar
st.sidebar.header("Input Settings")
source_mode = st.sidebar.radio("Select Input Mode", ["Upload Files", "Scan Local Folder"])

packages_to_process = [] # List of (filename, content) keys

if source_mode == "Upload Files":
    uploaded_files = st.sidebar.file_uploader("Upload SSIS Packages (.dtsx)", type=['dtsx', 'xml'], accept_multiple_files=True)
    if uploaded_files:
        for f in uploaded_files:
            packages_to_process.append((f.name, f.read().decode('utf-8'), None))

elif source_mode == "Scan Local Folder":
    st.sidebar.info("Enter absolute path to folder containing .dtsx files")
    folder_path = st.sidebar.text_input("Folder Path")
    
    if folder_path:
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            files = [f for f in os.listdir(folder_path) if f.lower().endswith(('.dtsx', '.xml'))]
            st.sidebar.success(f"Found {len(files)} package files.")
            
            # Persist load state
            if st.sidebar.button("Load Files"):
                st.session_state['loaded_folder'] = folder_path
            
            # Auto-load if path matches stored state
            if st.session_state.get('loaded_folder') == folder_path:
                for f in files:
                    full_path = os.path.join(folder_path, f)
                    try:
                        with open(full_path, 'r', encoding='utf-8') as file:
                            packages_to_process.append((f, file.read(), full_path))
                    except Exception as e:
                        st.sidebar.error(f"Error reading {f}: {e}")
        else:
            st.sidebar.warning("Folder path does not exist or is not a directory.")

# Processing and Display
if packages_to_process:
    st.success(f"✅ Loaded {len(packages_to_process)} packages. Processing...")
    
    processed_packages = []
    
    try:
        with st.spinner("Extracting metadata..."):
            for fname, content, full_path in packages_to_process:
                extractor = SSISMetadataExtractor(content)
                info = extractor.get_package_info()
                processed_packages.append({
                    'filename': fname,
                    'extractor': extractor,
                    'info': info,
                    'full_path': full_path
                })
        
        # Display Overview
        st.divider()
        st.header("📊 Global Overview")
        m1, m2 = st.columns(2)
        m1.metric("Total Packages Loaded", len(processed_packages))
        m2.metric("Total Processed", len(processed_packages))
        
        # Package Selector
        st.subheader("🔍 Inspect Package")
        
        pkg_options = {f"{p['info']['Package Name']} ({p['filename']})": i for i, p in enumerate(processed_packages)}
        selected_pkg_name = st.selectbox("Select Package", list(pkg_options.keys()))
        
        if selected_pkg_name:
            idx = pkg_options[selected_pkg_name]
            selected_pkg = processed_packages[idx]
            
            st.divider()
            st.markdown(f"### Currently Viewing: **{selected_pkg['info']['Package Name']}**")
            render_package_details(selected_pkg['extractor'], selected_pkg['full_path'])
            
    except Exception as e:
        st.error(f"❌ Error during processing: {str(e)}")
        st.exception(e)

else:
    st.info("👈 Select an input mode in the sidebar to get started.")
    
    st.markdown("""
    ## 🎯 Features for Migration
    
    ### What This Tool Extracts:
    
    1. **📦 Package Information**
       - Package name, creator, creation date
    
    2. **🔌 Connection Managers**
       - Connection names, types, connection strings
    
    3. **📥 Data Sources & 📤 Destinations**
       - Tables, Views, SQL Queries
       - **Auto-detected Source Tables** (even for derived columns)
    
    4. **🔗 Complete Column Lineage**
       - End-to-end tracking from Source to Destination
       - Handling of Transformations (Derived Column, Merge Join, etc.)
    
    5. **💾 Flexible Input**
       - Upload multiple .dtsx files
       - Scan local folders for bulk processing
    """)