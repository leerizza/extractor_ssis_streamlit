import re
from collections import defaultdict
from typing import Dict, List, Tuple, Any, Optional, Set

class EnhancedSQLParser:
    """
    Enhanced SQL Parser with deep lineage tracking capabilities.
    
    Features:
    - Deep nested subquery/CTE resolution
    - Expression decomposition (CASE, CONVERT, CAST, COALESCE, etc.)
    - Join key tracking with full provenance
    - Column dependency graph
    - Function argument tracking
    """
    
    def __init__(self, variable_resolver=None, debug=False):
        self.variable_resolver = variable_resolver
        self.debug = debug
        self._parse_cache = {}  # Cache untuk performa
        
    def _log(self, msg):
        """Debug logging"""
        if self.debug:
            print(f"[PARSER] {msg}")
    
    def _resolve_variables(self, sql):
        """Resolve SSIS variables like @[User::VarName]"""
        if self.variable_resolver and '@[' in sql:
            return self.variable_resolver(sql)
        return sql
    
    def _clean_sql_comments(self, sql: str) -> str:
        """Remove SQL comments with proper nesting support"""
        result = []
        i = 0
        n = len(sql)
        depth = 0
        in_string = False
        in_line_comment = False
        string_char = None
        
        while i < n:
            char = sql[i]
            
            # Handle strings
            if in_string:
                result.append(char)
                if char == string_char:
                    if i + 1 < n and sql[i + 1] == string_char:  # Escaped quote
                        result.append(sql[i + 1])
                        i += 2
                        continue
                    else:
                        in_string = False
                i += 1
                continue
            
            # Handle line comments
            if in_line_comment:
                if char == '\n':
                    in_line_comment = False
                    result.append(char)
                i += 1
                continue
            
            # Handle block comments
            if depth > 0:
                if char == '/' and i + 1 < n and sql[i + 1] == '*':
                    depth += 1
                    i += 2
                    continue
                if char == '*' and i + 1 < n and sql[i + 1] == '/':
                    depth -= 1
                    i += 2
                    continue
                i += 1
                continue
            
            # Start of block comment
            if char == '/' and i + 1 < n and sql[i + 1] == '*':
                depth += 1
                i += 2
                continue
            
            # Start of line comment
            if char == '-' and i + 1 < n and sql[i + 1] == '-':
                in_line_comment = True
                i += 2
                continue
            
            # Start of string
            if char in ("'", '"'):
                in_string = True
                string_char = char
                result.append(char)
                i += 1
                continue
            
            result.append(char)
            i += 1
        
        return ''.join(result)
    
    def _extract_balanced_parens(self, sql: str, start_pos: int) -> Tuple[str, int]:
        """Extract content within balanced parentheses"""
        if start_pos >= len(sql) or sql[start_pos] != '(':
            return '', start_pos
        
        depth = 0
        i = start_pos
        in_string = False
        string_char = None
        
        while i < len(sql):
            char = sql[i]
            
            if in_string:
                if char == string_char:
                    if i + 1 < len(sql) and sql[i + 1] == string_char:
                        i += 2
                        continue
                    in_string = False
                i += 1
                continue
            
            if char in ("'", '"'):
                in_string = True
                string_char = char
                i += 1
                continue
            
            if char == '(':
                depth += 1
            elif char == ')':
                depth -= 1
                if depth == 0:
                    return sql[start_pos + 1:i], i
            
            i += 1
        
        return sql[start_pos + 1:], i
    
    def _tokenize_select_list(self, select_clause: str) -> List[str]:
        """Split SELECT list by commas, respecting parentheses and strings"""
        tokens = []
        current = []
        depth = 0
        in_string = False
        string_char = None
        
        i = 0
        while i < len(select_clause):
            char = select_clause[i]
            
            if in_string:
                current.append(char)
                if char == string_char:
                    if i + 1 < len(select_clause) and select_clause[i + 1] == string_char:
                        current.append(select_clause[i + 1])
                        i += 2
                        continue
                    in_string = False
                i += 1
                continue
            
            if char in ("'", '"'):
                in_string = True
                string_char = char
                current.append(char)
                i += 1
                continue
            
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                tokens.append(''.join(current).strip())
                current = []
                i += 1
                continue
            else:
                current.append(char)
            
            i += 1
        
        if current:
            tokens.append(''.join(current).strip())
        
        return tokens
    
    def decompose_expression(self, expr: str, context_tables: Dict[str, str]) -> Dict[str, Any]:
        """
        Decompose complex expressions into their components.
        
        Returns:
        {
            'type': 'CASE' | 'FUNCTION' | 'COLUMN' | 'LITERAL' | 'ARITHMETIC',
            'dependencies': [list of column references],
            'logic': 'human-readable description',
            'source_tables': set of tables involved
        }
        """
        expr = expr.strip().upper()
        result = {
            'type': 'UNKNOWN',
            'dependencies': [],
            'logic': expr,
            'source_tables': set(),
            'source_columns': set()
        }
        
        # 1. CASE WHEN
        if expr.startswith('CASE'):
            result['type'] = 'CASE'
            dependencies = self._extract_case_dependencies(expr, context_tables)
            result.update(dependencies)
            return result
        
        # 2. Functions (CONVERT, CAST, COALESCE, ISNULL, etc.)
        func_patterns = [
            (r'CONVERT\s*\(\s*([^,]+)\s*,\s*(.+)\s*\)', 'CONVERT'),
            (r'CAST\s*\(\s*(.+?)\s+AS\s+([^)]+)\)', 'CAST'),
            (r'COALESCE\s*\((.+)\)', 'COALESCE'),
            (r'ISNULL\s*\((.+?),(.+?)\)', 'ISNULL'),
            (r'NULLIF\s*\((.+?),(.+?)\)', 'NULLIF'),
            (r'SUBSTRING\s*\((.+?),(.+?),(.+?)\)', 'SUBSTRING'),
            (r'DATEADD\s*\((.+?),(.+?),(.+?)\)', 'DATEADD'),
            (r'DATEDIFF\s*\((.+?),(.+?),(.+?)\)', 'DATEDIFF'),
        ]
        
        for pattern, func_name in func_patterns:
            match = re.search(pattern, expr, re.DOTALL)
            if match:
                result['type'] = 'FUNCTION'
                result['function_name'] = func_name
                
                # Extract arguments and recurse
                args = match.groups()
                all_deps = []
                for arg in args:
                    arg_result = self.decompose_expression(arg, context_tables)
                    all_deps.extend(arg_result['dependencies'])
                    result['source_tables'].update(arg_result['source_tables'])
                    result['source_columns'].update(arg_result['source_columns'])
                
                result['dependencies'] = all_deps
                result['logic'] = f"{func_name}({', '.join([self._simplify_expr(a) for a in args])})"
                return result
        
        # 3. Arithmetic/String operations
        if any(op in expr for op in ['+', '-', '*', '/', '||', 'CONCAT']):
            result['type'] = 'ARITHMETIC'
            # Extract all column references
            deps = self._extract_column_refs(expr, context_tables)
            result['dependencies'] = deps
            for table_ref, col_ref in deps:
                if table_ref:
                    result['source_tables'].add(context_tables.get(table_ref, table_ref))
                result['source_columns'].add(col_ref)
            return result
        
        # 4. Simple column reference
        # Support brackets: [Table].[Column] or Table.Column
        col_match = re.match(r'^((?:\[[^\]]+\])|(?:[\w]+))\s*\.\s*((?:\[[^\]]+\])|(?:[\w]+))$', expr)
        if col_match:
            result['type'] = 'COLUMN'
            table_alias = col_match.group(1).strip('[]').upper()
            col_name = col_match.group(2).strip('[]').upper()
            result['dependencies'] = [(table_alias, col_name)]
            result['source_tables'].add(context_tables.get(table_alias, table_alias))
            result['source_columns'].add(col_name)
            result['logic'] = f"{table_alias}.{col_name}"
            return result
        
        # 5. Literal
        if (expr.startswith("'") and expr.endswith("'")) or expr.replace('.', '', 1).isdigit() or expr == 'NULL':
            result['type'] = 'LITERAL'
            result['logic'] = expr
            return result
        
        # 6. Unqualified column (no table prefix)
        if re.match(r'^((?:\[[^\]]+\])|(?:[\w]+))$', expr):
            clean_col = expr.strip('[]').upper()
            result['type'] = 'COLUMN'
            result['dependencies'] = [(None, clean_col)]
            result['source_columns'].add(clean_col)
            result['logic'] = clean_col
            return result
        
        # Fallback: Try to extract any column references
        deps = self._extract_column_refs(expr, context_tables)
        if deps:
            result['dependencies'] = deps
            for table_ref, col_ref in deps:
                if table_ref:
                    result['source_tables'].add(context_tables.get(table_ref, table_ref))
                result['source_columns'].add(col_ref)
        
        return result
    
    def _extract_case_dependencies(self, case_expr: str, context_tables: Dict[str, str]) -> Dict:
        """Extract all column dependencies from CASE expression"""
        dependencies = []
        source_tables = set()
        source_columns = set()
        
        # Extract WHEN conditions and THEN/ELSE values
        # Pattern: WHEN condition THEN value
        when_pattern = r'WHEN\s+(.+?)\s+THEN\s+(.+?)(?=\s+WHEN|\s+ELSE|\s+END|$)'
        
        for match in re.finditer(when_pattern, case_expr, re.DOTALL):
            condition = match.group(1)
            value = match.group(2)
            
            # Recurse on condition
            cond_deps = self.decompose_expression(condition, context_tables)
            dependencies.extend(cond_deps['dependencies'])
            source_tables.update(cond_deps['source_tables'])
            source_columns.update(cond_deps['source_columns'])
            
            # Recurse on value
            val_deps = self.decompose_expression(value, context_tables)
            dependencies.extend(val_deps['dependencies'])
            source_tables.update(val_deps['source_tables'])
            source_columns.update(val_deps['source_columns'])
        
        # Extract ELSE
        else_match = re.search(r'ELSE\s+(.+?)\s+END', case_expr, re.DOTALL)
        if else_match:
            else_val = else_match.group(1)
            else_deps = self.decompose_expression(else_val, context_tables)
            dependencies.extend(else_deps['dependencies'])
            source_tables.update(else_deps['source_tables'])
            source_columns.update(else_deps['source_columns'])
        
        # Build human-readable logic
        logic_parts = []
        for match in re.finditer(when_pattern, case_expr, re.DOTALL):
            cond = self._simplify_expr(match.group(1))
            val = self._simplify_expr(match.group(2))
            logic_parts.append(f"WHEN {cond} THEN {val}")
        
        if else_match:
            logic_parts.append(f"ELSE {self._simplify_expr(else_match.group(1))}")
        
        return {
            'dependencies': dependencies,
            'source_tables': source_tables,
            'source_columns': source_columns,
            'logic': f"CASE {' '.join(logic_parts)} END"
        }
    
    def _simplify_expr(self, expr: str) -> str:
        """Simplify expression for display"""
        expr = expr.strip()
        if len(expr) > 50:
            return expr[:47] + "..."
        return expr
    
    def _extract_column_refs(self, expr: str, context_tables: Dict[str, str]) -> List[Tuple[Optional[str], str]]:
        """Extract all column references from expression"""
        refs = []
        
        # Mask literals
        masked = re.sub(r"'[^']*'", "'LITERAL'", expr)
        masked = re.sub(r'\d+', 'NUM', masked)
        
        # Pattern 1: table.column (with optional brackets)
        # [Table].[Col], Table.[Col], [Table].Col, Table.Col
        # Apply \b only to unbracketed words
        pattern_dot = r'(?:\[[^\]]+\]|\b[A-Z_][\w]*)\s*\.\s*(?:\[[^\]]+\]|[A-Z_][\w]*\b)'
        
        for match in re.finditer(pattern_dot, masked, re.IGNORECASE):
            # We need to re-parse groups as the pattern logic changed slightly
            full_match = match.group(0)
            parts = full_match.split('.', 1)
            table_ref = parts[0].strip().strip('[]').upper()
            col_ref = parts[1].strip().strip('[]').upper()
            refs.append((table_ref, col_ref))
        
        # Pattern 2: unqualified columns (with optional brackets)
        keywords = {
            'SELECT', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
            'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN', 'EXISTS',
            'CAST', 'CONVERT', 'COALESCE', 'ISNULL', 'SUM', 'COUNT', 'AVG', 'MIN', 'MAX',
            'LEFT', 'RIGHT', 'SUBSTRING', 'DATEADD', 'DATEDIFF', 'AS', 'ON', 'JOIN',
            'INNER', 'OUTER', 'CROSS', 'APPLY', 'TOP', 'DISTINCT', 'GROUP', 'ORDER', 'BY',
            'INT', 'VARCHAR', 'CHAR', 'DATE', 'DATETIME', 'DECIMAL', 'NUMERIC', 'FLOAT',
            'TRUE', 'FALSE', 'LITERAL', 'NUM', 'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM',
            'REPLACE', 'LEN', 'DATALENGTH', 'ROUND', 'FLOOR', 'CEILING', 'ABS', 'FORMAT',
            'YEAR', 'MONTH', 'DAY', 'GETDATE', 'GETUTCDATE', 'IIF', 'CHOOSE', 'NULLIF'
        }
        
        # Match words (with \b) OR [words] (without \b constraint)
        # Use alternation properly: (\[[^\]]+\])|(\b[A-Z_][\w]*\b)
        pattern_word = r'(?:(\[[^\]]+\])|(\b[A-Z_][\w]*\b))'
        
        for match in re.finditer(pattern_word, masked, re.IGNORECASE):
            # Check which group matched
            if match.group(1):
                raw_word = match.group(1) # Bracketed
            else:
                raw_word = match.group(2) # Unbracketed
            
            word = raw_word.strip('[]').upper()
            
            if word in keywords:
                continue
            
            # Check if it's part of table.column (already captured)
            # This check needs to be careful about brackets
            start, end = match.span()
            
            # Look behind for dot
            prefix = masked[:start].rstrip()
            if prefix.endswith('.'):
                continue
                
            # Look ahead for dot
            suffix = masked[end:].lstrip()
            if suffix.startswith('.'):
                continue
            
            # if self.debug: print(f"DEBUG: Found unqualified ref: {word}") 
            refs.append((None, word))
        
        return refs
    
    def _extract_ctes(self, sql: str) -> Tuple[Dict[str, Dict], str]:
        """Extract CTEs and return mappings + remaining SQL"""
        sql = sql.strip()
        cte_mappings = {}
        
        # Remove DECLARE statements
        while re.match(r'^\s*DECLARE\s+', sql, re.IGNORECASE):
            semicolon_match = re.search(r';', sql)
            if semicolon_match:
                sql = sql[semicolon_match.end():].strip()
            else:
                lines = sql.split('\n', 1)
                sql = lines[1].strip() if len(lines) > 1 else ''
        
        # Check for WITH clause
        with_match = re.match(r'^\s*WITH\s+', sql, re.IGNORECASE)
        if not with_match:
            return cte_mappings, sql
        
        # Find where CTEs end (start of main SELECT)
        cte_start = with_match.end()
        depth = 0
        main_select_pos = -1
        
        i = cte_start
        while i < len(sql):
            if sql[i] == '(':
                depth += 1
            elif sql[i] == ')':
                depth -= 1
            elif depth == 0 and sql[i:i+6].upper() == 'SELECT':
                main_select_pos = i
                break
            i += 1
        
        if main_select_pos == -1:
            return cte_mappings, sql
        
        # Parse each CTE
        cte_section = sql[cte_start:main_select_pos]
        remaining_sql = sql[main_select_pos:]
        
        # Pattern: CTE_NAME AS (SELECT ...)
        pos = 0
        while pos < len(cte_section):
            # Find CTE name
            name_match = re.search(r'(\w+)\s+AS\s*\(', cte_section[pos:], re.IGNORECASE)
            if not name_match:
                break
            
            cte_name = name_match.group(1).upper()
            paren_start = pos + name_match.end() - 1
            
            # Extract CTE content
            cte_content, paren_end = self._extract_balanced_parens(cte_section, paren_start)
            
            if cte_content:
                self._log(f"Parsing CTE: {cte_name}")
                cte_mappings[cte_name] = self.parse_sql_deep(cte_content)
            
            pos = paren_end + 1
        
        return cte_mappings, remaining_sql
    
    def _extract_derived_tables(self, sql: str) -> Tuple[Dict[str, Dict], str]:
        """Extract derived tables from FROM/JOIN and return mappings + masked SQL"""
        derived_mappings = {}
        masked_sql = sql
        
        iteration = 0
        max_iterations = 20  # Prevent infinite loops
        
        while iteration < max_iterations:
            iteration += 1
            
            # Find (SELECT ...) in FROM/JOIN context
            match = re.search(r'\(\s*SELECT', masked_sql, re.IGNORECASE)
            if not match:
                break
            
            # Check context
            prefix = masked_sql[:match.start()].strip()
            is_derived = False
            
            if prefix:
                last_word_match = re.search(r'(\w+)\s*$', prefix)
                if last_word_match:
                    last_token = last_word_match.group(1).upper()
                    if last_token in ['FROM', 'JOIN', 'APPLY', 'UPDATE', 'INTO']:
                        is_derived = True
            
            # Extract content
            inner_sql, end_pos = self._extract_balanced_parens(masked_sql, match.start())
            
            if not inner_sql:
                break
            
            # Extract alias
            remainder = masked_sql[end_pos + 1:] if end_pos + 1 < len(masked_sql) else ''
            alias_match = re.match(r'^\s*(?:AS\s+)?(\w+)', remainder, re.IGNORECASE)
            
            derived_alias = None
            if alias_match and is_derived:
                derived_alias = alias_match.group(1).upper()
                if derived_alias in ['ON', 'JOIN', 'LEFT', 'RIGHT', 'WHERE', 'ORDER', 'GROUP']:
                    derived_alias = None
            
            # Parse if it's a derived table
            if derived_alias:
                self._log(f"Parsing derived table: {derived_alias}")
                derived_mappings[derived_alias] = self.parse_sql_deep(inner_sql)
            
            # Mask it
            prefix = masked_sql[:match.start()]
            suffix = masked_sql[end_pos + 1:]
            
            if is_derived:
                masked_sql = prefix + " (DERIVED_TABLE_MASK) " + suffix
            else:
                masked_sql = prefix + " (SCALAR_SUBQUERY_MASK) " + suffix
        
        return derived_mappings, masked_sql
    
    def parse_sql_deep(self, sql_query: str) -> Dict[str, Any]:
        """
        Deep SQL parsing with full lineage resolution.
        
        Returns dict of column mappings:
        {
            'COLUMN_ALIAS': {
                'source_table': 'actual_table_name',
                'source_column': 'actual_column_name',
                'expression': 'original expression',
                'expression_type': 'CASE' | 'FUNCTION' | 'COLUMN' | etc.,
                'dependencies': [(table, col), ...],
                'logic_breakdown': 'human-readable explanation'
            }
        }
        """
        if not sql_query or sql_query == 'N/A':
            return {}
        
        # Check cache
        cache_key = sql_query[:200]  # Use first 200 chars as key
        if cache_key in self._parse_cache:
            return self._parse_cache[cache_key]
        
        self._log(f"Parsing SQL: {sql_query[:100]}...")
        
        sql_query = self._resolve_variables(sql_query)
        sql_clean = self._clean_sql_comments(sql_query).upper().strip()
        
        # Handle EXEC (stored procedures)
        if sql_clean.startswith('EXEC'):
            parts = sql_clean.split()
            proc_name = parts[1] if len(parts) > 1 else 'UNKNOWN_PROC'
            return {
                '*': {
                    'source_table': proc_name,
                    'source_column': '*',
                    'expression': 'Stored Procedure Result',
                    'expression_type': 'PROCEDURE',
                    'dependencies': [],
                    'logic_breakdown': f'Result from {proc_name}'
                }
            }
        
        # Extract CTEs
        cte_mappings, sql_clean = self._extract_ctes(sql_clean)
        
        # Handle UNION
        if self._contains_union(sql_clean):
            return self._parse_union(sql_clean, cte_mappings)
        
        # Extract derived tables
        derived_mappings, sql_masked = self._extract_derived_tables(sql_clean)
        
        # Merge mappings
        all_subqueries = {**cte_mappings, **derived_mappings}
        
        # Parse table aliases from masked SQL
        table_aliases = self._parse_table_aliases(sql_masked, all_subqueries)
        
        # Extract SELECT clause
        select_clause = self._extract_select_clause(sql_masked)
        if not select_clause:
            return {}
        
        # Parse columns
        column_mappings = {}
        column_tokens = self._tokenize_select_list(select_clause)
        
        for token in column_tokens:
            col_result = self._parse_column_token(
                token, 
                table_aliases, 
                all_subqueries
            )
            if col_result:
                alias, mapping = col_result
                column_mappings[alias] = mapping
        
        # Handle SELECT *
        if any(t.strip() == '*' for t in column_tokens):
            star_mappings = self._expand_select_star(table_aliases, all_subqueries)
            column_mappings.update(star_mappings)
        
        # Cache result
        self._parse_cache[cache_key] = column_mappings
        
        return column_mappings
    
    def _contains_union(self, sql: str) -> bool:
        """Check if SQL contains top-level UNION"""
        depth = 0
        i = 0
        while i < len(sql):
            if sql[i] == '(':
                depth += 1
            elif sql[i] == ')':
                depth -= 1
            elif depth == 0 and sql[i:i+5] == 'UNION':
                # Check word boundary
                if (i == 0 or not sql[i-1].isalnum()) and \
                   (i+5 >= len(sql) or not sql[i+5].isalnum()):
                    return True
            i += 1
        return False
    
    def _parse_union(self, sql: str, cte_mappings: Dict) -> Dict:
        """Parse UNION query"""
        # Split by UNION
        branches = []
        depth = 0
        start = 0
        i = 0
        
        while i < len(sql):
            if sql[i] == '(':
                depth += 1
            elif sql[i] == ')':
                depth -= 1
            elif depth == 0 and sql[i:i+5] == 'UNION':
                if (i == 0 or not sql[i-1].isalnum()) and \
                   (i+5 >= len(sql) or not sql[i+5].isalnum()):
                    branches.append(sql[start:i].strip())
                    # Skip UNION [ALL]
                    next_pos = i + 5
                    if sql[next_pos:next_pos+4].strip().upper() == 'ALL':
                        next_pos += 4
                    start = next_pos
            i += 1
        
        # Add last branch
        if start < len(sql):
            branches.append(sql[start:].strip())
        
        # Parse each branch
        branch_results = []
        for branch in branches:
            result = self.parse_sql_deep(branch)
            branch_results.append(result)
        
        # Merge results
        if not branch_results:
            return {}
        
        merged = {}
        first_branch = branch_results[0]
        
        for col_alias, col_data in first_branch.items():
            all_sources = []
            all_source_cols = []
            
            for branch_result in branch_results:
                if col_alias in branch_result:
                    branch_data = branch_result[col_alias]
                    if isinstance(branch_data, dict):
                        all_sources.append(branch_data.get('source_table', 'N/A'))
                        all_source_cols.append(branch_data.get('source_column', col_alias))
            
            merged[col_alias] = {
                'source_table': ' UNION '.join(filter(lambda x: x != 'N/A', all_sources)),
                'source_column': ' UNION '.join(set(all_source_cols)),
                'expression': col_data.get('expression', ''),
                'expression_type': 'UNION',
                'dependencies': col_data.get('dependencies', []),
                'logic_breakdown': f'UNION of {len(branch_results)} branches'
            }
        
        return merged
    
    def _parse_table_aliases(self, sql: str, subquery_mappings: Dict) -> Dict[str, str]:
        """Parse table aliases from FROM/JOIN clauses"""
        table_aliases = {}
        
        # Add subqueries
        for alias in subquery_mappings:
            table_aliases[alias] = f"SUBQUERY::{alias}"
        
        # Pattern: FROM/JOIN table [AS] alias
        pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w\.\[\]]+\]?\.)?(?:\[?[\w\.\[\]]+\]?\.)?(\[?[\w_]+\]?)(?:\s+(?:AS\s+)?(\w+))?'
        
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            table_name = match.group(1).strip('[]').upper()
            alias_group = match.group(2)
            alias = alias_group.upper() if alias_group else table_name
            
            # Skip keywords
            if alias in ['LEFT', 'RIGHT', 'INNER', 'OUTER', 'JOIN', 'ON', 'WHERE', 
                         'GROUP', 'ORDER', 'BY', 'SELECT', 'FROM', 'DERIVED_TABLE_MASK', 'SCALAR_SUBQUERY_MASK']:
                alias = table_name
            
            if alias not in table_aliases:
                table_aliases[alias] = table_name
        
        return table_aliases
    
    def _extract_select_clause(self, sql: str) -> str:
        """Extract SELECT clause between SELECT and FROM"""
        match = re.search(r'SELECT\s+', sql, re.IGNORECASE)
        if not match:
            return ''
        
        select_start = match.end()
        
        # Find FROM at same depth
        depth = 0
        i = select_start
        while i < len(sql):
            if sql[i] == '(':
                depth += 1
            elif sql[i] == ')':
                depth -= 1
            elif depth == 0 and sql[i:i+4].upper() == 'FROM':
                if (i == 0 or not sql[i-1].isalnum()):
                    return sql[select_start:i].strip()
            i += 1
        
        return sql[select_start:].strip()
    
    def _parse_column_token(
        self, 
        token: str, 
        table_aliases: Dict[str, str],
        subquery_mappings: Dict[str, Dict]
    ) -> Optional[Tuple[str, Dict]]:
        """Parse a single column token from SELECT list"""
        
        if not token or token.strip() == '*':
            return None
        
        original_token = token
        token = token.strip().upper()
        
        # Extract alias
        col_alias = None
        col_expr = token
        
        # Pattern 1: alias = expression (T-SQL style)
        if '=' in token and not token.startswith("'"):
            parts = token.split('=', 1)
            potential_alias = parts[0].strip()
            if ' ' not in potential_alias and '(' not in potential_alias:
                col_alias = potential_alias
                col_expr = parts[1].strip()
        
        # Pattern 2: expression AS alias
        if not col_alias:
            alias_match = re.search(
                r'(?:\s+AS\s+|\s+)((?:\[[^\]]+\])|(?:[\w]+))\s*$', 
                token, 
                re.IGNORECASE
            )
            if alias_match:
                found_alias = alias_match.group(1)
                keywords = ['END', 'AS', 'AND', 'OR', 'IS', 'NULL', 'NOT']
                if found_alias.upper() not in keywords:
                    col_alias = found_alias
                    col_expr = token[:alias_match.start()].strip()
        
        # Default alias
        if not col_alias:
            if '.' in col_expr:
                col_alias = col_expr.split('.')[-1].strip('[]')
            else:
                col_alias = col_expr.strip('[]')
        
        # Decompose expression
        expr_analysis = self.decompose_expression(col_expr, table_aliases)
        
        # Resolve dependencies
        source_tables = set()
        source_columns = set()
        
        for table_ref, col_ref in expr_analysis['dependencies']:
            if table_ref is None:
                # Unqualified column - need to resolve
                resolved = self._resolve_unqualified_column(
                    col_ref, 
                    table_aliases, 
                    subquery_mappings
                )
                if resolved:
                    source_tables.add(resolved['source_table'])
                    source_columns.add(resolved['source_column'])
            else:
                # Qualified column
                resolved = self._resolve_qualified_column(
                    table_ref, 
                    col_ref, 
                    table_aliases, 
                    subquery_mappings
                )
                if resolved:
                    source_tables.add(resolved['source_table'])
                    source_columns.add(resolved['source_column'])
        
        # Build result
        is_literal = expr_analysis['type'] == 'LITERAL'
        res_table = 'Static Value' if is_literal else 'Calculation'
        
        # Override source tables if found
        if source_tables:
             res_table = ', '.join(sorted(source_tables))
        
        res_col = ', '.join(sorted(source_columns))
        if not res_col:
             if res_table == 'Static Value':
                 clean_val = original_token.strip()
                 res_col = clean_val if len(clean_val) < 50 else clean_val[:47] + "..."
             else:
                 clean_val = original_token.strip()
                 res_col = clean_val if len(clean_val) < 50 else clean_val[:47] + "..."
        
        result = {
            'source_table': res_table,
            'source_column': res_col,
            'expression': original_token,
            'expression_type': expr_analysis['type'],
            'dependencies': expr_analysis['dependencies'],
            'logic_breakdown': expr_analysis['logic']
        }
        
        return col_alias.strip('[]').upper(), result
    
    def _resolve_unqualified_column(
        self, 
        col_name: str, 
        table_aliases: Dict[str, str],
        subquery_mappings: Dict[str, Dict]
    ) -> Optional[Dict]:
        """Resolve unqualified column reference"""
        
        # If single table, use it
        real_tables = [t for t in table_aliases.values() if not t.startswith('SUBQUERY::')]
        
        if len(real_tables) == 1:
            table_name = real_tables[0]
            
            # Check if it's a subquery
            if table_name in subquery_mappings:
                sub_mapping = subquery_mappings[table_name]
                if col_name in sub_mapping:
                    return sub_mapping[col_name]
                elif '*' in sub_mapping:
                    return sub_mapping['*']
            
            return {
                'source_table': table_name,
                'source_column': col_name
            }
        
        # Try each table alias
        for alias, table_name in table_aliases.items():
            if table_name.startswith('SUBQUERY::'):
                real_alias = table_name.split('::')[1]
                if real_alias in subquery_mappings:
                    sub_mapping = subquery_mappings[real_alias]
                    if col_name in sub_mapping:
                        return sub_mapping[col_name]
            elif table_name in subquery_mappings:
                sub_mapping = subquery_mappings[table_name]
                if col_name in sub_mapping:
                    return sub_mapping[col_name]
        
        # Heuristic: Default to Primary Table (First real table found)
        # This resolves "Ambiguous" mappings in complex joins where prefixes are omitted
        if real_tables:
            primary_table = real_tables[0]
            return {
                'source_table': primary_table,
                'source_column': col_name,
                'logic_breakdown': f'Assumed from Primary Table ({primary_table})'
            }
        
        return {
            'source_table': 'Ambiguous',
            'source_column': col_name
        }
    
    def _resolve_qualified_column(
        self, 
        table_ref: str, 
        col_name: str,
        table_aliases: Dict[str, str],
        subquery_mappings: Dict[str, Dict]
    ) -> Optional[Dict]:
        """Resolve qualified column reference (table.column)"""
        
        # Check if table_ref is in subqueries
        if table_ref in subquery_mappings:
            sub_mapping = subquery_mappings[table_ref]
            if col_name in sub_mapping:
                return sub_mapping[col_name]
            elif '*' in sub_mapping:
                wildcard = sub_mapping['*']
                if isinstance(wildcard, dict):
                    return {
                        'source_table': wildcard.get('source_table', 'Unknown'),
                        'source_column': col_name
                    }
        
        # Check if it's an alias
        if table_ref in table_aliases:
            table_name = table_aliases[table_ref]
            
            if table_name.startswith('SUBQUERY::'):
                real_alias = table_name.split('::')[1]
                if real_alias in subquery_mappings:
                    sub_mapping = subquery_mappings[real_alias]
                    if col_name in sub_mapping:
                        return sub_mapping[col_name]
                    elif '*' in sub_mapping:
                        wildcard = sub_mapping['*']
                        if isinstance(wildcard, dict):
                            return {
                                'source_table': wildcard.get('source_table', 'Unknown'),
                                'source_column': col_name
                            }
            elif table_name in subquery_mappings:
                sub_mapping = subquery_mappings[table_name]
                if col_name in sub_mapping:
                    return sub_mapping[col_name]
            
            return {
                'source_table': table_name,
                'source_column': col_name
            }
        
        # Direct table reference
        return {
            'source_table': table_ref,
            'source_column': col_name
        }
    
    def _expand_select_star(
        self, 
        table_aliases: Dict[str, str],
        subquery_mappings: Dict[str, Dict]
    ) -> Dict:
        """Expand SELECT * to individual columns if possible"""
        result = {}
        
        candidates = list(table_aliases.values())
        
        # If single table (real or subquery)
        # Note: If multiple tables, we skip expansion to avoid ambiguity in simplistic parser
        if len(candidates) == 1:
            table_name = candidates[0]
            
            # Handle Subquery alias
            if table_name.startswith('SUBQUERY::'):
                 real_alias = table_name.split('::')[1]
                 if real_alias in subquery_mappings:
                       sub_mapping = subquery_mappings[real_alias]
                       for col, data in sub_mapping.items():
                           if col != '*':
                               result[col] = data
            
            # Handle Real table if in subquery_mappings
            elif table_name in subquery_mappings:
                sub_mapping = subquery_mappings[table_name]
                for col, data in sub_mapping.items():
                    if col != '*':
                        result[col] = data
            
            if not result and not table_name.startswith('SUBQUERY::'): 
                # No expansion possible, return wildcard for REAL table
                result['*'] = {
                    'source_table': table_name,
                    'source_column': '*',
                    'expression': 'SELECT *',
                    'expression_type': 'WILDCARD',
                    'dependencies': [],
                    'logic_breakdown': f'All columns from {table_name}'
                }
        
        return result
    
    def extract_join_conditions(self, sql_query: str) -> List[Dict]:
        """
        Extract JOIN conditions with full resolution.
        
        Returns:
        [
            {
                'left_table': 'actual_table_name',
                'left_column': 'col_name',
                'right_table': 'actual_table_name',
                'right_column': 'col_name',
                'join_type': 'INNER' | 'LEFT' | 'RIGHT' | 'FULL',
                'condition': 'original condition'
            }
        ]
        """
        if not sql_query or sql_query == 'N/A':
            return []
        
        sql_query = self._resolve_variables(sql_query)
        sql_clean = self._clean_sql_comments(sql_query).upper().strip()
        
        # Extract CTEs and derived tables
        cte_mappings, sql_clean = self._extract_ctes(sql_clean)
        derived_mappings, sql_masked = self._extract_derived_tables(sql_clean)
        all_subqueries = {**cte_mappings, **derived_mappings}
        
        # Parse table aliases
        table_aliases = self._parse_table_aliases(sql_masked, all_subqueries)
        
        join_conditions = []
        
        # Pattern: JOIN ... ON ...
        join_pattern = r'(LEFT|RIGHT|INNER|FULL|CROSS)?\s*(OUTER\s+)?JOIN\s+.*?\s+ON\s+(.*?)(?=\s+(?:LEFT|RIGHT|INNER|FULL|CROSS|WHERE|GROUP|ORDER|UNION|$))'
        
        for match in re.finditer(join_pattern, sql_masked, re.DOTALL | re.IGNORECASE):
            join_type = (match.group(1) or 'INNER').upper()
            condition = match.group(3).strip()
            
            # Extract column pairs from condition
            # Pattern: table.column = table.column
            # Enhanced to support ISNULL(table.col, val) = ISNULL(table.col, val)
            col_pairs = re.findall(
                r'(?:(?:ISNULL|COALESCE)\s*\(\s*)?([A-Z_][\w]*\.[A-Z_][\w]*)(?:.*?\))?\s*=\s*(?:(?:ISNULL|COALESCE)\s*\(\s*)?([A-Z_][\w]*\.[A-Z_][\w]*)(?:.*?\))?',
                condition,
                re.IGNORECASE
            )
            
            for left_full, right_full in col_pairs:
                # Extract table/col from the full match (which might be table.col)
                # Wait, the regex groups 1 and 2 capture the table.col part directly!
                # Group 1: ([A-Z_][\w]*\.[A-Z_][\w]*)
                
                parts_left = left_full.split('.', 1)
                left_table_ref = parts_left[0]
                left_col = parts_left[1]
                
                parts_right = right_full.split('.', 1)
                right_table_ref = parts_right[0]
                right_col = parts_right[1]
                
                # Resolve table references
                left_resolved = self._resolve_qualified_column(
                    left_table_ref, left_col, table_aliases, all_subqueries
                )
                right_resolved = self._resolve_qualified_column(
                    right_table_ref, right_col, table_aliases, all_subqueries
                )
                
                join_conditions.append({
                    'left_table_alias': left_table_ref,
                    'left_table': left_resolved['source_table'] if left_resolved else left_table_ref,
                    'left_column': left_resolved['source_column'] if left_resolved else left_col,
                    'right_table_alias': right_table_ref,
                    'right_table': right_resolved['source_table'] if right_resolved else right_table_ref,
                    'right_column': right_resolved['source_column'] if right_resolved else right_col,
                    'join_type': join_type,
                    'condition': condition
                })
        
        return join_conditions


# Backward compatibility wrapper
class SQLParser(EnhancedSQLParser):
    """Wrapper for backward compatibility"""
    
    def parse_sql_column_sources(self, sql_query: str) -> Dict:
        """Legacy method - convert new format to old"""
        new_result = self.parse_sql_deep(sql_query)
        
        # Convert to old format
        old_format = {}
        for col_alias, col_data in new_result.items():
            old_format[col_alias] = {
                'source_table': col_data['source_table'],
                'source_column': col_data['source_column'],
                'expression': col_data.get('expression', '')
            }
        
        return old_format
    
    def extract_join_keys(self, sql_query: str) -> List[Dict]:
        """Legacy method - adapt new join extraction"""
        join_conditions = self.extract_join_conditions(sql_query)
        
        # Convert to old format
        old_format = []
        for cond in join_conditions:
            old_format.append({
                'Original Table Alias': cond['left_table_alias'],
                'Original Column': cond['left_column'],
                'Source Table': cond['left_table'],
                'Source Column': cond['left_column']
            })
            old_format.append({
                'Original Table Alias': cond['right_table_alias'],
                'Original Column': cond['right_column'],
                'Source Table': cond['right_table'],
                'Source Column': cond['right_column']
            })
        
        return old_format
    
    def extract_statement_metadata(self, sql_stmt: str) -> Dict:
        """Enhanced statement metadata extraction"""
        stmt = sql_stmt.strip()
        if not stmt:
            return None
        
        clean_stmt = self._clean_sql_comments(stmt)
        
        op_type = "UNKNOWN"
        dest_table = "N/A"
        sources = set()
        columns_lineage = {}
        
        # Determine operation type
        if re.search(r'\bINSERT\b\s+(?:INTO\b)?', clean_stmt, re.IGNORECASE):
            op_type = "INSERT"
            match = re.search(r'INSERT\s+(?:INTO\s+)?([\[\]\w\.]+)', clean_stmt, re.IGNORECASE)
            if match:
                dest_table = match.group(1)
            
            select_match = re.search(r'\bSELECT\b', clean_stmt, re.IGNORECASE)
            if select_match:
                select_part = clean_stmt[select_match.start():]
                columns_lineage = self.parse_sql_deep(select_part)
        
        elif re.search(r'\bINTO\b.*\bFROM\b', clean_stmt, re.IGNORECASE | re.DOTALL):
            op_type = "SELECT INTO"
            into_match = re.search(r'\bINTO\s+([\[\]\w\.]+)', clean_stmt, re.IGNORECASE)
            if into_match:
                dest_table = into_match.group(1)
            
            query_for_parse = re.sub(r'\bINTO\s+[\[\]\w\.]+', '', clean_stmt, flags=re.IGNORECASE)
            columns_lineage = self.parse_sql_deep(query_for_parse)
        
        elif re.search(r'\bCREATE\s+(?:OR\s+ALTER\s+)?VIEW\b', clean_stmt, re.IGNORECASE):
            op_type = "CREATE VIEW"
            # Extract View Name
            view_match = re.search(r'\bVIEW\s+([\[\]\w\.]+)', clean_stmt, re.IGNORECASE)
            if view_match:
                dest_table = view_match.group(1)
            
            # Extract Definition (after AS)
            as_match = re.search(r'\bAS\b\s+(SELECT\b.*)', clean_stmt, re.IGNORECASE | re.DOTALL)
            if as_match:
                view_def = as_match.group(1)
                columns_lineage = self.parse_sql_deep(view_def)
        
        elif re.search(r'\bUPDATE\b', clean_stmt, re.IGNORECASE):
            op_type = "UPDATE"
            match = re.search(r'UPDATE\s+([\[\]\w\.]+)', clean_stmt, re.IGNORECASE)
            if match:
                dest_table = match.group(1)
            
            set_match = re.search(r'\bSET\b\s+(.*?)(\bFROM\b|\bWHERE\b|$)', clean_stmt, re.IGNORECASE | re.DOTALL)
            if set_match:
                set_clause = set_match.group(1)
                
                # Parse assignments
                assignments = []
                depth = 0
                current = ""
                for c in set_clause:
                    if c == '(':
                        depth += 1
                    elif c == ')':
                        depth -= 1
                    elif c == ',' and depth == 0:
                        assignments.append(current.strip())
                        current = ""
                        continue
                    current += c
                if current.strip():
                    assignments.append(current.strip())
                
                # Build fake SELECT
                fake_items = []
                for assign in assignments:
                    if '=' in assign:
                        parts = assign.split('=', 1)
                        target = parts[0].strip()
                        expr = parts[1].strip()
                        fake_items.append(f"{expr} AS {target}")
                
                if fake_items:
                    fake_query = "SELECT " + ", ".join(fake_items)
                    rest_match = re.search(r'(\bFROM\b.*)', clean_stmt, re.IGNORECASE | re.DOTALL)
                    if rest_match:
                        fake_query += " " + rest_match.group(1)
                    
                    columns_lineage = self.parse_sql_deep(fake_query)
        
        else:
            # Try as SELECT
            result = self.parse_sql_deep(clean_stmt)
            if result:
                op_type = 'SELECT'
                columns_lineage = result
        
        # Extract source tables
        table_pattern = r'(?:FROM|JOIN)\s+([\[\]\w\.]+)'
        for m in re.finditer(table_pattern, clean_stmt, re.IGNORECASE):
            tbl = m.group(1)
            if tbl.upper() not in ['SELECT', 'WHERE', 'GROUP', 'ORDER', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'CROSS', 'APPLY']:
                sources.add(tbl)
        
        return {
            'Operation': op_type,
            'Destination': dest_table,
            'Sources': list(sources),
            'Columns': columns_lineage,
            'Join Keys': self.extract_join_conditions(clean_stmt)
        }