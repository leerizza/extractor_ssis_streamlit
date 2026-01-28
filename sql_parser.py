import re
from collections import defaultdict

class SQLParser:
    """
    Standalone SQL Parser for extracting column lineage and statement metadata.
    Refactored from SSISMetadataExtractor for reusability.
    """
    def __init__(self, variable_resolver=None):
        """
        Args:
            variable_resolver (callable, optional): Function to resolve SSIS variables (e.g. @[User::Var]) 
                                                   to their values. Should return the resolved string.
        """
        self.variable_resolver = variable_resolver

    def _resolve_sql_variables(self, sql_query):
        """Wrapper to call the injected resolver if present"""
        if self.variable_resolver:
            return self.variable_resolver(sql_query)
        return sql_query

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

    def parse_sql_column_sources(self, sql_query):
        """Parse SQL query to extract source table for each column with smart inference and recursion"""
        if not sql_query or sql_query == 'N/A':
            return {}
            
        # 0. RESOLVE VARIABLES
        sql_query = self._resolve_sql_variables(sql_query)
            
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
            # Use Helper Method
            masked_sql = self._extract_derived_tables(sql_clean, subquery_mappings)
            
            # Now parse standard tables from the MASKED sql
            table_aliases = {}
            for alias in subquery_mappings:
                table_aliases[alias] = f"SUBQUERY::{alias}"
            
            table_pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w\.\[\]]+\]?\.)?(?:\[?[\w\.\[\]]+\]?\.)?(\[?[\w_]+\]?)(?:\s+(?:AS\s+)?(\w+))?'
            
            for match in re.finditer(table_pattern, masked_sql):
                table_name = match.group(1).strip('[]')
                alias_group = match.group(2)
                alias = alias_group if alias_group else table_name
                
                if alias in ['LEFT', 'RIGHT', 'INNER', 'OUTER', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'BY', 'SELECT', 'FROM', 'SUBQUERY_MASK']:
                     alias = table_name
                
                if alias.upper() not in table_aliases:
                    table_aliases[alias.upper()] = table_name
            
            unique_tables = sorted(list(set(table_aliases.values())))
            is_single_table = len(unique_tables) == 1
            default_table = unique_tables[0] if is_single_table else None
            
            if 'select_clause' not in locals():
                select_clause = ""
                match_sel = re.search(r'SELECT\s+', sql_clean, re.IGNORECASE)
                
                if not match_sel:
                    return subquery_mappings
                
                select_start = match_sel.end()
                from_candidates = [m.start() for m in re.finditer(r'\bFROM\b', sql_clean, re.IGNORECASE)]
                
                found_from = False
                for idx in from_candidates:
                    if idx < select_start: continue
                    segment = sql_clean[select_start:idx]
                    if segment.count('(') == segment.count(')'):
                         select_clause = segment
                         found_from = True
                         break
                
                if not found_from:
                     select_clause = sql_clean[select_start:]

            # Parse columns
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
            
            for col in columns:
                col_alias = None
                final_source_table = set()
                final_source_col = set()
                col_expr = col
                
                if '=' in col and not col.startswith("'"):
                     parts = col.split('=', 1)
                     potential_alias = parts[0].strip()
                     if ' ' not in potential_alias and '(' not in potential_alias:
                         col_alias = potential_alias
                         col_expr = parts[1].strip()
                
                if not col_alias:
                    alias_match = re.search(r'(?:\s+AS\s+|\s+|(?<=\)))((?:\[[^\]]+\])|(?:[\w]+))\s*$', col, re.IGNORECASE)
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
                col_expr_clean = col_expr.strip().upper()
                if col_expr_clean.startswith('(') and 'SELECT' in col_expr_clean[:20]:
                    inner_content = col_expr.strip()
                    if inner_content.startswith('(') and inner_content.endswith(')'):
                         inner_content = inner_content[1:-1].strip()
                    
                    if inner_content.upper().startswith('SELECT'):
                        sub_result = self.parse_sql_column_sources(inner_content)
                        for sub_col, sub_meta in sub_result.items():
                             if isinstance(sub_meta, dict):
                                 t = sub_meta.get('source_table', 'N/A')
                                 c = sub_meta.get('source_column', sub_col)
                                 if t != 'N/A': final_source_table.add(t)
                                 if c != 'N/A': final_source_col.add(c)
                             else:
                                 final_source_table.add(str(sub_meta))
                
                def extract_column_refs(expr):
                    refs = []
                    masked_expr = re.sub(r"'[^']*'", "'STR'", expr)
                    pattern_dot = r'\b([a-zA-Z_][\w]*)\s*\.\s*([a-zA-Z_][\w]*)\b'
                    for match in re.finditer(pattern_dot, masked_expr, re.IGNORECASE):
                        table_ref = match.group(1).upper()
                        col_ref = match.group(2).upper()
                        refs.append((table_ref, col_ref))
                    
                    keywords = {'SELECT', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN', 'EXISTS', 'CAST', 'CONVERT', 'COALESCE', 'ISNULL', 'ABS', 'ROUND', 'FLOOR', 'CEILING', 'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'LEFT', 'RIGHT', 'SUBSTRING', 'LEN', 'TRIM', 'LTRIM', 'RTRIM', 'DATEADD', 'DATEDIFF', 'DATEPART', 'GETDATE', 'YEAR', 'MONTH', 'DAY', 'AS', 'ON', 'JOIN', 'INNER', 'OUTER', 'CROSS', 'APPLY', 'TOP', 'DISTINCT', 'INT', 'VARCHAR', 'CHAR', 'DATE', 'DATETIME', 'BIT', 'DECIMAL', 'NUMERIC', 'FLOAT', 'TRUE', 'FALSE', 'UNKNOWN', 'STR'}
                    pattern_word = r'\b([a-zA-Z_][\w]*)\b'
                    for match in re.finditer(pattern_word, masked_expr, re.IGNORECASE):
                        word = match.group(1).upper()
                        if word in keywords: continue
                        
                        is_covered = False
                        start, end = match.span()
                        if start > 0 and masked_expr[start-1] == '.': is_covered = True
                        if end < len(masked_expr) and masked_expr[end] == '.': is_covered = True 
                        
                        if not is_covered:
                            refs.append((None, word))
                    return refs
                
                column_refs = extract_column_refs(col_expr)
                found_explicit_ref = False
                
                if column_refs:
                    for table_ref, col_ref in column_refs:
                        if table_ref is None:
                            found_explicit_ref = True
                            if is_single_table and default_table:
                                t_name = default_table
                                if t_name.startswith("SUBQUERY::"):
                                    real_alias = t_name.split("::")[1]
                                    if real_alias in subquery_mappings:
                                         inner_map = subquery_mappings[real_alias]
                                         res = inner_map.get(col_ref, inner_map.get('*'))
                                         if res and isinstance(res, dict):
                                             final_source_table.add(res.get('source_table', 'Unknown'))
                                             final_source_col.add(res.get('source_column', col_ref))
                                         elif res:
                                             final_source_table.add(str(res))
                                             final_source_col.add(col_ref)
                                         else:
                                             final_source_table.add(f"Subquery({real_alias})")
                                             final_source_col.add(col_ref)
                                elif t_name in subquery_mappings:
                                     inner_map = subquery_mappings[t_name]
                                     res = inner_map.get(col_ref, inner_map.get('*'))
                                     if res and isinstance(res, dict):
                                         final_source_table.add(res.get('source_table', 'Unknown'))
                                         final_source_col.add(res.get('source_column', col_ref))
                                     elif res:
                                         final_source_table.add(str(res))
                                         final_source_col.add(col_ref)
                                     else:
                                         final_source_table.add(f"Subquery({t_name})")
                                         final_source_col.add(col_ref)
                                else:
                                     final_source_table.add(t_name)
                                     final_source_col.add(col_ref)
                            else:
                                final_source_table.add("Ambiguous/Multiple")
                                final_source_col.add(col_ref)
                            continue

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
                        
                        elif table_ref in table_aliases:
                            found_explicit_ref = True
                            t_name = table_aliases[table_ref]
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

                if not found_explicit_ref:
                     def resolve_from_subquery(s_alias, c_expr):
                         inner_c = c_expr.strip('[]').upper()
                         if '.' in c_expr: inner_c = c_expr.split('.')[-1].strip('[]').upper()
                         if s_alias in subquery_mappings:
                             i_map = subquery_mappings[s_alias]
                             return i_map.get(inner_c, i_map.get('*', None))
                         return None
                     
                     is_literal = (col_expr.startswith("'") and col_expr.endswith("'")) or \
                                  col_expr.replace('.','',1).isdigit() or \
                                  col_expr.upper() == 'NULL'
                                  
                     if not is_literal and not final_source_table:
                         if is_single_table:
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
                             elif default_table in subquery_mappings:
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
                                 final_source_col.add(col_expr)
                         elif unique_tables:
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

                col_alias_clean = col_alias.strip('[]').upper()
                res_table = ', '.join(sorted(final_source_table)) if final_source_table else 'Expression/Literal'
                res_col = ', '.join(sorted(final_source_col)) if final_source_col else 'Calculated'
                
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
            
            if '*' in columns or (len(columns) == 1 and columns[0] == '*'):
                 if is_single_table and default_table.startswith("SUBQUERY::"):
                      real_alias = default_table.split("::")[1]
                      if real_alias in subquery_mappings:
                           sub_mapping = subquery_mappings[real_alias]
                           for sub_col, sub_data in sub_mapping.items():
                               if sub_col == '*' and len(sub_mapping) > 1: continue
                               column_to_table[sub_col] = sub_data
                 elif is_single_table and default_table in subquery_mappings:
                      sub_mapping = subquery_mappings[default_table]
                      for sub_col, sub_data in sub_mapping.items():
                           if sub_col == '*' and len(sub_mapping) > 1: continue
                           column_to_table[sub_col] = sub_data
                 else:
                      if default_table:
                          column_to_table['*'] = {
                              'source_table': default_table,
                              'source_column': '*', 
                              'expression': 'SELECT *'
                          }

        except Exception as e:
            # print(f"Error parsing SQL: {e}")
            pass
        
        return column_to_table

    def _extract_derived_tables(self, sql_clean, subquery_mappings):
        """Helper to extract derived tables from SQL and populate mappings recursively"""
        masked_sql = sql_clean
        
        # Logic: Iterate through string to find ALL "(SELECT" blocks
        while True:
            # Find first unmasked (SELECT
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
                inner_sql = masked_sql[start_inner : end_inner]
                
                # Extract Alias after )
                remainder = masked_sql[end_inner+1:]
                alias_match = re.match(r'^\s*(?:AS\s+)?(\w+)', remainder, re.IGNORECASE)
                
                sub_alias = None
                if alias_match and is_derived_context: # Only extract alias if it's a derived table
                    sub_alias = alias_match.group(1).upper()
                    if sub_alias in ['ON', 'JOIN', 'LEFT', 'RIGHT', 'WHERE', 'ORDER', 'GROUP']:
                        sub_alias = None
                
                # Recursively Parse only if it's a Derived Table
                if sub_alias:
                    subquery_mappings[sub_alias] = self.parse_sql_column_sources(inner_sql)
                
                # MASK IT
                prefix = masked_sql[:match.start()]
                suffix = masked_sql[end_inner+1:]
                
                if is_derived_context:
                    replacement = " (SUBQUERY_MASK) "
                else:
                    replacement = " (SCALAR_MASK) "
                    
                masked_sql = prefix + replacement + suffix
            else:
                break
        
        return masked_sql

    def extract_statement_metadata(self, sql_stmt):
        """
        Analyze a single SQL statement to exact metadata including:
        - Operation Type (INSERT, UPDATE, SELECT INTO)
        - Destination Table
        - Column Lineage (TargetCol -> SourceCol details)
        """
        stmt = sql_stmt.strip()
        if not stmt: return None

        op_type = "UNKNOWN"
        dest_table = "N/A"
        sources = set()
        columns_lineage = {} # Target -> Source info

        clean_stmt = self._clean_sql_comments(stmt)
        
        # 1. Determine Operation
        if re.search(r'\bINSERT\s+INTO\b', clean_stmt, re.IGNORECASE):
            op_type = "INSERT"
            match = re.search(r'INSERT\s+INTO\s+([\[\]\w\.]+)', clean_stmt, re.IGNORECASE)
            if match: dest_table = match.group(1)
            
            # Logic for INSERT:
            # Pattern 1: INSERT INTO Tbl (Cols) SELECT ...
            # Pattern 2: INSERT INTO Tbl SELECT ... (No cols specified, harder to map)
            
            # Extract Target Columns
            # Find (...) after table name before SELECT/VALUES
            
            # Note: We need to parse valid regex logic for this.
            # Simplified assumption: We look for the SELECT part.
            
            select_match = re.search(r'\bSELECT\b', clean_stmt, re.IGNORECASE)
            if select_match:
                select_part = clean_stmt[select_match.start():]
                # Parse the SELECT part using existing logic
                source_columns = self.parse_sql_column_sources(select_part)
                
                # If target cols are explicit, map them
                cols_match = re.search(r'INSERT\s+INTO\s+[\[\]\w\.]+\s*\(([^)]+)\)', clean_stmt, re.IGNORECASE)
                if cols_match:
                    target_cols = [c.strip().strip('[]') for c in cols_match.group(1).split(',')]
                    
                    # Map source_columns (ordered? No, parser returns dict with aliases)
                    # We can't rely on dict order in Python < 3.7 explicitly but usually okay.
                    # Best effort: If source_columns keys match target_cols length?
                    # The parser keys are ALIASES. In INSERT SELECT, aliases don't matter much for mapping usually, 
                    # but they might match target cols?
                    
                    # Actually, we need positional mapping if INSERT INTO (A,B) SELECT X,Y
                    # Our parser returns a dict. We might need a list-returning version or just use values.
                    
                    # For now, let's just expose the SOURCE columns found.
                    columns_lineage = source_columns
                else:
                    columns_lineage = source_columns

        elif re.search(r'\bINTO\b', clean_stmt, re.IGNORECASE) and re.search(r'\bFROM\b', clean_stmt, re.IGNORECASE):
            # SELECT INTO
            into_match = re.search(r'\bINTO\s+([\[\]\w\.]+)', clean_stmt, re.IGNORECASE)
            if into_match:
                 op_type = "SELECT INTO"
                 dest_table = into_match.group(1)
                 
                 # The Query IS the logic. We just parse the whole thing.
                 # SELECT X AS A, Y AS B INTO Tbl FROM ...
                 # The columns will include 'INTO Tbl' if we are not careful? 
                 # SSIS parser handles SELECT list roughly. 
                 
                 # Remove INTO clause for parsing?
                 # SELECT A, B INTO T FROM S -> SELECT A, B FROM S
                 query_for_parse = re.sub(r'\bINTO\s+[\[\]\w\.]+', '', clean_stmt, flags=re.IGNORECASE)
                 columns_lineage = self.parse_sql_column_sources(query_for_parse)

        elif re.search(r'\bUPDATE\b', clean_stmt, re.IGNORECASE):
            op_type = "UPDATE"
            match = re.search(r'UPDATE\s+([\[\]\w\.]+)', clean_stmt, re.IGNORECASE)
            if match: dest_table = match.group(1)
            
            # Logic for UPDATE:
            # UPDATE T SET Col1 = Expr1, Col2 = Expr2 FROM ...
            # We need to extract the SET clause.
            
            set_match = re.search(r'\bSET\b\s+(.*?)(\bFROM\b|\bWHERE\b|$)', clean_stmt, re.IGNORECASE | re.DOTALL)
            if set_match:
                set_clause = set_match.group(1)
                
                # Split by comma (careful with parens)
                assignments = []
                paren_d = 0
                curr = ""
                for c in set_clause:
                    if c == '(': paren_d+=1
                    elif c == ')': paren_d-=1
                    elif c == ',' and paren_d==0:
                        assignments.append(curr.strip())
                        curr = ""
                        continue
                    curr += c
                if curr.strip(): assignments.append(curr.strip())
                
                # Construct a fake SELECT to use the parser
                # SET Col1 = Expr1  --> SELECT Expr1 AS Col1
                fake_select_items = []
                for assign in assignments:
                    if '=' in assign:
                        parts = assign.split('=', 1)
                        target = parts[0].strip()
                        expr = parts[1].strip()
                        fake_select_items.append(f"{expr} AS {target}")
                
                if fake_select_items:
                    fake_query = "SELECT " + ", ".join(fake_select_items)
                    
                    # Append FROM/WHERE if present to allow resolution
                    rest_match = re.search(r'(\bFROM\b.*)', clean_stmt, re.IGNORECASE | re.DOTALL)
                    if rest_match:
                        fake_query += " " + rest_match.group(1)
                    
                    columns_lineage = self.parse_sql_column_sources(fake_query)

        # Fallback: Try parsing as plain SELECT (common for OLE DB Source)
        if op_type == 'UNKNOWN':
            # This handles SELECT, WITH ... SELECT, etc.
            # parse_sql_column_sources is robust enough to handle CTEs and complex SELECTs
            results = self.parse_sql_column_sources(clean_stmt)
            if results:
                op_type = 'SELECT'
                columns_lineage = results

        # Extract Raw Sources
        path_pattern = r'(?:FROM|JOIN)\s+([\[\]\w\.]+)'
        for m in re.finditer(path_pattern, clean_stmt, re.IGNORECASE):
            tbl = m.group(1)
            if tbl.upper() not in ['SELECT', 'WHERE', 'GROUP', 'ORDER', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'APPLY']:
                sources.add(tbl)

        return {
            'Operation': op_type,
            'Destination': dest_table,
            'Sources': list(sources),
            'Columns': columns_lineage,
            'Join Keys': self.extract_join_keys(clean_stmt)
        }

    def extract_join_keys(self, sql_query):
        """Extract columns used in JOIN ON clauses"""
        if not sql_query or sql_query == 'N/A': return []
        
        # 0. Resolve Variables
        sql_query = self._resolve_sql_variables(sql_query)
        
        # 1. Parse CTEs to understand aliases
        subquery_mappings, sql_clean_initial = self._extract_ctes_and_clean_sql(self._clean_sql_comments(sql_query).upper().strip())
        
        # 1.5 Parse Derived Tables in Main Query (RECURSIVE)
        # This populates subquery_mappings with ANY derived table in FROM/JOIN
        # AND returns a masked SQL where those derived tables are replaced by (SUBQUERY_MASK)
        # This PREVENTS the ON-clause regex from matching inside the subquery!
        sql_masked = self._extract_derived_tables(sql_clean_initial, subquery_mappings)
        
        # 2. Parse Table Aliases in Main Query (using MASKED SQL)
        table_aliases = {}
        # Pattern handles: FROM table alias, JOIN table AS alias
        table_pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w\.\[\]]+\]?\.)?(?:\[?[\w\.\[\]]+\]?\.)?(\[?[\w_]+\]?)(?:\s+(?:AS\s+)?(\w+))?'
        
        for match in re.finditer(table_pattern, sql_masked, re.IGNORECASE):
            table_name = match.group(1).strip('[]').upper()
            alias_group = match.group(2)
            alias = alias_group.upper() if alias_group else table_name
            
            if alias in ['LEFT', 'RIGHT', 'INNER', 'OUTER', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'BY', 'SELECT', 'FROM', 'WITH', 'OPTION', 'SUBQUERY_MASK']:
                alias = table_name
                
            table_aliases[alias] = table_name
            
        join_keys = []
        
        # 3. Extract ON clauses (using MASKED SQL)
        # Regex to find ON ... until next keyword
        on_pattern = r'\bON\b\s+(.*?)(?=\b(?:LEFT|RIGHT|INNER|OUTER|JOIN|WHERE|GROUP|ORDER|UNION|OPTION)\b|$)'
        
        for match in re.finditer(on_pattern, sql_masked, re.DOTALL | re.IGNORECASE):
            condition = match.group(1).strip()
            
            # 4. Extract table.column references
            col_refs = re.findall(r'([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', condition)
            
            for table_alias, col_name in col_refs:
                table_alias = table_alias.upper()
                col_name = col_name.upper()
                
                source_table = table_alias
                source_col = col_name
                
                # Resolve Table Alias -> Real Table Name (e.g. A -> T1)
                real_table_name = table_aliases.get(table_alias, table_alias)
                source_table = real_table_name # Default to real name
                
                # 5. Resolve Aliases using CTE/Derived Table mappings
                if real_table_name in subquery_mappings:
                    inner_map = subquery_mappings[real_table_name]
                    resolved = inner_map.get(col_name)
                    if not resolved:
                         resolved = inner_map.get('*') # Fallback to wildcard if present

                    if resolved:
                        if isinstance(resolved, dict):
                            source_table = resolved.get('source_table', source_table)
                            source_col = resolved.get('source_column', source_col)
                        else:
                             # If resolved is a string (simple mapping)
                             # It might be "Table.Col" or just "Table" or "Col"
                             # Standardize this? parse_sql_column_sources returns simple str if unique?
                             # In parse_sql_column_sources Phase 2 merge:
                             # merged_result[col_alias] = { 'source_table': ..., 'source_column': ... }
                             # But some paths return strings. Let's handle string.
                             pass 
                
                    # Special check: If real_table_name starts with SUBQUERY:: (Legacy/Fallback)
                    # The _extract_derived_tables logic populates mappings by ALIAS directly.
                    # so real_table_name should match the key in subquery_mappings.
                
                join_keys.append({
                    'Original Table Alias': table_alias,
                    'Original Column': col_name,
                    'Source Table': source_table,
                    'Source Column': source_col
                })
                
        return join_keys
