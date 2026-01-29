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
        return self._split_arguments(select_clause)
    
    def _split_arguments(self, args_str: str) -> List[str]:
        """Split arguments by comma, respecting parentheses and quotes"""
        args = []
        current = []
        depth = 0
        in_string = False
        string_char = None
        
        for char in args_str:
            if in_string:
                current.append(char)
                if char == string_char:
                    in_string = False
                continue
            
            if char in ("'", '"'):
                in_string = True
                string_char = char
                current.append(char)
                continue
                
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                args.append("".join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            args.append("".join(current).strip())
        
        return [a for a in args if a]

    def decompose_expression(self, expr: str, context_tables: Dict[str, str]) -> Dict[str, Any]:
        """
        Decompose complex expressions into their components.
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
        
        # 2. Generic Function Handler (Matches ANY function usage: NAME(...))
        # Use regex to find the Function Name and the Content inside the OUTERMOST parens
        func_match = re.match(r'^(\w+)\s*\((.+)\)$', expr, re.DOTALL)
        if func_match:
            func_name = func_match.group(1).upper()
            content = func_match.group(2)
            
            # Validation: Ensure it's a valid function call by checking balanced parens on the content
            # If the content contains an unbalanced closing paren towards the end, regex might have consumed too much?
            # actually re.match(..., .+) is greedy. It will take until the last ).
            # If we have "FUNC(A), B", regex '^(\w+)\s*\((.+)\)$' will NOT match because of ", B".
            # It only matches if the WHOLE string is a function call.
            # Since we assume 'expr' is a single column expression (already split by comma), this is safe.
            
            if func_name not in ['AND', 'OR', 'NOT', 'IN', 'EXISTS', 'SELECT', 'FROM', 'WHERE']:
                result['type'] = 'FUNCTION'
                result['function_name'] = func_name
                
                # Split arguments
                args = self._split_arguments(content)
                
                all_deps = []
                simplified_args = []
                
                for arg in args:
                    arg_result = self.decompose_expression(arg, context_tables)
                    all_deps.extend(arg_result['dependencies'])
                    result['source_tables'].update(arg_result['source_tables'])
                    result['source_columns'].update(arg_result['source_columns'])
                    simplified_args.append(self._simplify_expr(arg))
                
                result['dependencies'] = all_deps
                result['logic'] = f"{func_name}({', '.join(simplified_args)})"
                return result

        # 3. Arithmetic/String operations
        if any(op in expr for op in ['+', '-', '*', '/', '||', 'CONCAT']):
            result['type'] = 'ARITHMETIC'
            deps = self._extract_column_refs(expr, context_tables)
            result['dependencies'] = deps
            for table_ref, col_ref in deps:
                if table_ref:
                    result['source_tables'].add(context_tables.get(table_ref, table_ref))
                result['source_columns'].add(col_ref)
            return result
        
        # 4. Simple column reference
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
        
        # 6. Unqualified column
        if re.match(r'^((?:\[[^\]]+\])|(?:[\w]+))$', expr):
            clean_col = expr.strip('[]').upper()
            result['type'] = 'COLUMN'
            result['dependencies'] = [(None, clean_col)]
            result['source_columns'].add(clean_col)
            result['logic'] = clean_col
            return result
        
        return result
    
    def _extract_case_dependencies(self, case_expr: str, context_tables: Dict[str, str]) -> Dict:
        """Extract all column dependencies from CASE expression"""
        dependencies = []
        source_tables = set()
        source_columns = set()
        when_pattern = r'WHEN\s+(.+?)\s+THEN\s+(.+?)(?=\s+WHEN|\s+ELSE|\s+END|$)'
        
        for match in re.finditer(when_pattern, case_expr, re.DOTALL):
            cond_deps = self.decompose_expression(match.group(1), context_tables)
            dependencies.extend(cond_deps['dependencies'])
            source_tables.update(cond_deps['source_tables'])
            source_columns.update(cond_deps['source_columns'])
            val_deps = self.decompose_expression(match.group(2), context_tables)
            dependencies.extend(val_deps['dependencies'])
            source_tables.update(val_deps['source_tables'])
            source_columns.update(val_deps['source_columns'])
        
        else_match = re.search(r'ELSE\s+(.+?)\s+END', case_expr, re.DOTALL)
        if else_match:
            else_deps = self.decompose_expression(else_match.group(1), context_tables)
            dependencies.extend(else_deps['dependencies'])
            source_tables.update(else_deps['source_tables'])
            source_columns.update(else_deps['source_columns'])
        
        return {
            'dependencies': dependencies,
            'source_tables': source_tables,
            'source_columns': source_columns,
            'logic': 'CASE ... END'
        }
    
    def _simplify_expr(self, expr: str) -> str:
        expr = expr.strip()
        if len(expr) > 50: return expr[:47] + "..."
        return expr
    
    def _extract_column_refs(self, expr: str, context_tables: Dict[str, str]) -> List[Tuple[Optional[str], str]]:
        refs = []
        masked = re.sub(r"'[^']*'", "'LITERAL'", expr)
        masked = re.sub(r'\d+', 'NUM', masked)
        pattern_dot = r'(?:\[[^\]]+\]|\b[A-Z_][\w]*)\s*\.\s*(?:\[[^\]]+\]|[A-Z_][\w]*\b)'
        for match in re.finditer(pattern_dot, masked, re.IGNORECASE):
            full_match = match.group(0)
            parts = full_match.split('.', 1)
            table_ref = parts[0].strip().strip('[]').upper()
            col_ref = parts[1].strip().strip('[]').upper()
            refs.append((table_ref, col_ref))
        
        pattern_word = r'(?:(\[[^\]]+\])|(\b[A-Z_][\w]*\b))'
        keywords = {'SELECT', 'FROM', 'WHERE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END', 'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN', 'EXISTS', 'CAST', 'CONVERT', 'COALESCE', 'ISNULL', 'SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'AS', 'ON', 'JOIN', 'INNER', 'OUTER', 'CROSS', 'APPLY', 'TOP', 'DISTINCT', 'GROUP', 'ORDER', 'BY', 'LITERAL', 'NUM'}
        
        for match in re.finditer(pattern_word, masked, re.IGNORECASE):
            raw_word = match.group(1) if match.group(1) else match.group(2)
            word = raw_word.strip('[]').upper()
            if word in keywords: continue
            start, end = match.span()
            if masked[:start].rstrip().endswith('.'): continue
            if masked[end:].lstrip().startswith('.'): continue
            refs.append((None, word))
        return refs
    
    def _extract_ctes(self, sql: str) -> Tuple[Dict[str, Dict], str]:
        sql = sql.strip()
        cte_mappings = {}
        while re.match(r'^\s*DECLARE\s+', sql, re.IGNORECASE):
            semicolon_match = re.search(r';', sql)
            if semicolon_match: sql = sql[semicolon_match.end():].strip()
            else:
                lines = sql.split('\n', 1)
                sql = lines[1].strip() if len(lines) > 1 else ''
        with_match = re.match(r'^\s*WITH\s+', sql, re.IGNORECASE)
        if not with_match: return cte_mappings, sql
        cte_start = with_match.end()
        depth = 0
        main_select_pos = -1
        i = cte_start
        while i < len(sql):
            if sql[i] == '(': depth += 1
            elif sql[i] == ')': depth -= 1
            elif depth == 0 and sql[i:i+6].upper() == 'SELECT':
                main_select_pos = i
                break
            i += 1
        if main_select_pos == -1: return cte_mappings, sql
        cte_section = sql[cte_start:main_select_pos]
        remaining_sql = sql[main_select_pos:]
        pos = 0
        while pos < len(cte_section):
            name_match = re.search(r'(\w+)\s+AS\s*\(', cte_section[pos:], re.IGNORECASE)
            if not name_match: break
            cte_name = name_match.group(1).upper()
            paren_start = pos + name_match.end() - 1
            cte_content, paren_end = self._extract_balanced_parens(cte_section, paren_start)
            if cte_content: cte_mappings[cte_name] = self.parse_sql_deep(cte_content)
            pos = paren_end + 1
        return cte_mappings, remaining_sql
    
    def _extract_derived_tables(self, sql: str) -> Tuple[Dict[str, Dict], str]:
        derived_mappings = {}
        masked_sql = sql
        iteration = 0
        while iteration < 20:
            iteration += 1
            match = re.search(r'\(\s*SELECT', masked_sql, re.IGNORECASE)
            if not match: break
            prefix = masked_sql[:match.start()].strip()
            is_derived = False
            if prefix:
                last_word_match = re.search(r'(\w+)\s*$', prefix)
                if last_word_match:
                    last_token = last_word_match.group(1).upper()
                    if last_token in ['FROM', 'JOIN', 'APPLY', 'UPDATE', 'INTO']: is_derived = True
            inner_sql, end_pos = self._extract_balanced_parens(masked_sql, match.start())
            if not inner_sql: break
            remainder = masked_sql[end_pos + 1:]
            alias_match = re.match(r'^\s*(?:AS\s+)?(\w+)', remainder, re.IGNORECASE)
            derived_alias = alias_match.group(1).upper() if alias_match and is_derived else None
            if derived_alias in ['ON', 'JOIN', 'LEFT', 'RIGHT', 'WHERE', 'ORDER', 'GROUP']: derived_alias = None
            if derived_alias: derived_mappings[derived_alias] = self.parse_sql_deep(inner_sql)
            prefix = masked_sql[:match.start()]
            suffix = masked_sql[end_pos + 1:]
            masked_sql = prefix + (" (DERIVED_TABLE_MASK) " if is_derived else " (SCALAR_SUBQUERY_MASK) ") + suffix
        return derived_mappings, masked_sql
    
    def parse_sql_deep(self, sql_query: str) -> Dict[str, Any]:
        if not sql_query or sql_query == 'N/A': return {}
        cache_key = sql_query[:200]
        if cache_key in self._parse_cache: return self._parse_cache[cache_key]
        sql_query = self._resolve_variables(sql_query)
        sql_clean = self._clean_sql_comments(sql_query).upper().strip()
        if sql_clean.startswith('EXEC'):
            parts = sql_clean.split()
            proc_name = parts[1] if len(parts) > 1 else 'UNKNOWN_PROC'
            return {'*': {'source_table': proc_name, 'source_column': '*', 'expression': 'Stored Procedure Result', 'expression_type': 'PROCEDURE', 'dependencies': [], 'logic_breakdown': f'Result from {proc_name}'}}
        cte_mappings, sql_clean = self._extract_ctes(sql_clean)
        derived_mappings, sql_masked = self._extract_derived_tables(sql_clean)
        all_subqueries = {**cte_mappings, **derived_mappings}
        table_aliases = self._parse_table_aliases(sql_masked, all_subqueries)
        select_clause = self._extract_select_clause(sql_masked)
        if not select_clause: return {}
        column_mappings = {}
        column_tokens = self._tokenize_select_list(select_clause)
        for token in column_tokens:
            col_result = self._parse_column_token(token, table_aliases, all_subqueries)
            if col_result:
                alias, mapping = col_result
                column_mappings[alias] = mapping
        if any(t.strip() == '*' for t in column_tokens):
            star_mappings = self._expand_select_star(table_aliases, all_subqueries)
            column_mappings.update(star_mappings)
        self._parse_cache[cache_key] = column_mappings
        return column_mappings
    
    def _parse_table_aliases(self, sql: str, subquery_mappings: Dict) -> Dict[str, str]:
        table_aliases = {alias: f"SUBQUERY::{alias}" for alias in subquery_mappings}
        pattern = r'(?:FROM|JOIN)\s+(?:\[?[\w\.\[\]]+\]?\.)?(?:\[?[\w\.\[\]]+\]?\.)?(\[?[\w_]+\]?)(?:\s+(?:AS\s+)?(\w+))?'
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            table_name = match.group(1).strip('[]').upper()
            alias_group = match.group(2)
            alias = alias_group.upper() if alias_group else table_name
            if alias in ['LEFT', 'RIGHT', 'INNER', 'OUTER', 'JOIN', 'ON', 'WHERE', 'GROUP', 'ORDER', 'BY', 'SELECT', 'FROM', 'DERIVED_TABLE_MASK', 'SCALAR_SUBQUERY_MASK']: alias = table_name
            if alias not in table_aliases: table_aliases[alias] = table_name
        return table_aliases
    
    def _extract_select_clause(self, sql: str) -> str:
        """Extract the content between SELECT and FROM (or end of string)"""
        # Iterate through all SELECTs to find the "real" one (skipping variable assignments)
        for match in re.finditer(r'SELECT\s+', sql, re.IGNORECASE):
            select_start = match.end()
            
            # Check if this is a variable assignment (e.g. SELECT @var = ...)
            # T-SQL assignment via SELECT always starts with @variable =
            post_select = sql[select_start:]
            # Look for @var = or @var= (ignoring comments handled by clean, but here raw text might have spaces)
            # Use strict regex for variable assignment
            if re.match(r'\s*@[\w@#$]+\s*=', post_select, re.IGNORECASE):
                continue

            # Found valid SELECT (or at least one that isn't obviously an assignment)
            depth = 0
            i = select_start
            while i < len(sql):
                if sql[i] == '(':
                    depth += 1
                elif sql[i] == ')':
                    depth -= 1
                elif depth == 0 and i + 4 <= len(sql) and sql[i:i+4].upper() == 'FROM':
                    # Check partial world match for FROM
                    prev_char = sql[i-1] if i > 0 else ' '
                    next_char = sql[i+4] if i+4 < len(sql) else ' '
                    is_prev_valid = not (prev_char.isalnum() or prev_char == '_')
                    is_next_valid = not (next_char.isalnum() or next_char == '_')
                    
                    if is_prev_valid and is_next_valid:
                        clause = sql[select_start:i].strip()
                        # Remove DISTINCT or TOP
                        clause = re.sub(r'^(?:DISTINCT|TOP\s+\d+|TOP\s+\(\d+\))\s+', '', clause, flags=re.IGNORECASE)
                        return clause
                i += 1
            
            # If no FROM, return rest of string?
            # Issue: If we have multiple statements 'SELECT A; SELECT B', and we prefer the last one?
            # Or if 'Select @var=1' (no FROM). 
            # If we fell through here (no FROM), it means we reached end of string.
            # So this is the last statement.
            clause = sql[select_start:].strip()
            return re.sub(r'^(?:DISTINCT|TOP\s+\d+|TOP\s+\(\d+\))\s+', '', clause, flags=re.IGNORECASE)
            
        return ''
    
    def _parse_column_token(self, token: str, table_aliases: Dict[str, str], subquery_mappings: Dict[str, Dict]) -> Optional[Tuple[str, Dict]]:
        if not token or token.strip() == '*': return None
        original_token = token
        token = token.strip().upper()
        col_alias = None
        col_expr = token
        if '=' in token and not token.startswith("'"):
            parts = token.split('=', 1)
            if ' ' not in parts[0].strip() and '(' not in parts[0].strip():
                col_alias = parts[0].strip()
                col_expr = parts[1].strip()
        
        # Pattern 2: expression AS alias
        if not col_alias:
            # Enhanced regex: allow alias after ) even without space
            alias_match = re.search(r'(?:\s+AS\s+|\s+|\))((?:\[[^\]]+\])|(?:[\w]+))\s*$', token, re.IGNORECASE)
            if alias_match:
                if alias_match.group(0).startswith(')'):
                    # Alias follows )
                    col_alias = alias_match.group(1)
                    col_expr = token[:alias_match.start() + 1].strip()
                elif alias_match.group(1).upper() not in ['END', 'AS', 'AND', 'OR', 'IS', 'NULL', 'NOT']:
                    col_alias = alias_match.group(1)
                    col_expr = token[:alias_match.start()].strip()
        if not col_alias: col_alias = col_expr.split('.')[-1].strip('[]') if '.' in col_expr else col_expr.strip('[]')
        expr_analysis = self.decompose_expression(col_expr, table_aliases)
        source_tables, source_columns = set(), set()
        for t_ref, c_ref in expr_analysis['dependencies']:
            resolved = self._resolve_qualified_column(t_ref, c_ref, table_aliases, subquery_mappings) if t_ref else self._resolve_unqualified_column(c_ref, table_aliases, subquery_mappings)
            if resolved:
                source_tables.add(resolved['source_table'])
                source_columns.add(resolved['source_column'])
        is_lit = expr_analysis['type'] == 'LITERAL'
        res_table = ', '.join(sorted(source_tables)) if source_tables else ('Static Value' if is_lit else 'Calculation')
        res_col = ', '.join(sorted(source_columns)) if source_columns else original_token.strip()[:50]
        return col_alias.strip('[]').upper(), {'source_table': res_table, 'source_column': res_col, 'expression': col_expr, 'expression_type': expr_analysis['type'], 'dependencies': expr_analysis['dependencies'], 'logic_breakdown': expr_analysis['logic']}
    
    def _resolve_unqualified_column(self, col_name: str, table_aliases: Dict[str, str], subquery_mappings: Dict[str, Dict]) -> Optional[Dict]:
        real_tables = [t for t in table_aliases.values() if not t.startswith('SUBQUERY::')]
        if len(real_tables) == 1:
            table_name = real_tables[0]
            if table_name in subquery_mappings:
                sub_mapping = subquery_mappings[table_name]
                if col_name in sub_mapping: return sub_mapping[col_name]
                if '*' in sub_mapping: return sub_mapping['*']
            return {'source_table': table_name, 'source_column': col_name}
        for alias, table_name in table_aliases.items():
            check_alias = table_name.split('::')[1] if table_name.startswith('SUBQUERY::') else table_name
            if check_alias in subquery_mappings:
                sub_mapping = subquery_mappings[check_alias]
                if col_name in sub_mapping: return sub_mapping[col_name]
        if real_tables:
            return {'source_table': real_tables[0], 'source_column': col_name, 'logic_breakdown': f'Assumed from {real_tables[0]}'}
        return {'source_table': 'Ambiguous', 'source_column': col_name}
    
    def _resolve_qualified_column(self, table_ref: str, col_name: str, table_aliases: Dict[str, str], subquery_mappings: Dict[str, Dict]) -> Optional[Dict]:
        if table_ref in subquery_mappings:
            sub_mapping = subquery_mappings[table_ref]
            if col_name in sub_mapping: return sub_mapping[col_name]
            if '*' in sub_mapping and isinstance(sub_mapping['*'], dict): return {'source_table': sub_mapping['*'].get('source_table', 'Unknown'), 'source_column': col_name}
        if table_ref in table_aliases:
            table_name = table_aliases[table_ref]
            check_alias = table_name.split('::')[1] if table_name.startswith('SUBQUERY::') else table_name
            if check_alias in subquery_mappings:
                sub_mapping = subquery_mappings[check_alias]
                if col_name in sub_mapping: return sub_mapping[col_name]
            return {'source_table': table_name.replace('SUBQUERY::', ''), 'source_column': col_name}
        return {'source_table': table_ref, 'source_column': col_name}
    
    def _expand_select_star(self, table_aliases: Dict[str, str], subquery_mappings: Dict[str, Dict]) -> Dict:
        result = {}
        for table_name in table_aliases.values():
            real_alias = table_name.split('::')[1] if table_name.startswith('SUBQUERY::') else table_name
            if real_alias in subquery_mappings:
                for col, data in subquery_mappings[real_alias].items():
                    if col != '*': result[col] = data
        if not result and table_aliases:
            primary = list(table_aliases.values())[0]
            result['*'] = {'source_table': primary, 'source_column': '*', 'expression': 'SELECT *', 'expression_type': 'WILDCARD', 'dependencies': [], 'logic_breakdown': f'All from {primary}'}
        return result

    def extract_join_conditions(self, sql_query: str) -> List[Dict]:
        if not sql_query or sql_query == 'N/A': return []
        sql_clean = self._clean_sql_comments(self._resolve_variables(sql_query)).upper().strip()
        cte_mappings, sql_clean = self._extract_ctes(sql_clean)
        derived_mappings, sql_masked = self._extract_derived_tables(sql_clean)
        all_subqueries = {**cte_mappings, **derived_mappings}
        table_aliases = self._parse_table_aliases(sql_masked, all_subqueries)
        join_conditions = []
        join_pattern = r'(LEFT|RIGHT|INNER|FULL|CROSS)?\s*(OUTER\s+)?JOIN\s+.*?\s+ON\s+(.*?)(?=\s+(?:LEFT|RIGHT|INNER|FULL|CROSS|WHERE|GROUP|ORDER|UNION|$))'
        for match in re.finditer(join_pattern, sql_masked, re.DOTALL | re.IGNORECASE):
            type_ = (match.group(1) or 'INNER').upper()
            cond = match.group(3).strip()
            col_pairs = re.findall(r'([A-Z_][\w]*\.[A-Z_][\w]*)\s*=\s*([A-Z_][\w]*\.[A-Z_][\w]*)', cond, re.IGNORECASE)
            for left, right in col_pairs:
                l_parts, r_parts = left.split('.'), right.split('.')
                l_res = self._resolve_qualified_column(l_parts[0], l_parts[1], table_aliases, all_subqueries)
                r_res = self._resolve_qualified_column(r_parts[0], r_parts[1], table_aliases, all_subqueries)
                join_conditions.append({'left_table_alias': l_parts[0], 'left_table': l_res['source_table'], 'left_column': l_res['source_column'], 'right_table_alias': r_parts[0], 'right_table': r_res['source_table'], 'right_column': r_res['source_column'], 'join_type': type_, 'condition': cond})
        return join_conditions

class SQLParser(EnhancedSQLParser):
    def parse_sql_column_sources(self, sql_query: str) -> Dict:
        new_result = self.parse_sql_deep(sql_query)
        return {alias: {'source_table': data['source_table'], 'source_column': data['source_column'], 'expression': data.get('expression', '')} for alias, data in new_result.items()}
    
    def extract_join_keys(self, sql_query: str) -> List[Dict]:
        conditions = self.extract_join_conditions(sql_query)
        res = []
        for c in conditions:
            res.append({'Original Table Alias': c['left_table_alias'], 'Original Column': c['left_column'], 'Source Table': c['left_table'], 'Source Column': c['left_column']})
            res.append({'Original Table Alias': c['right_table_alias'], 'Original Column': c['right_column'], 'Source Table': c['right_table'], 'Source Column': c['right_column']})
        return res
    
    def extract_statement_metadata(self, sql_stmt: str) -> Dict:
        stmt = sql_stmt.strip()
        if not stmt: return None
        clean = self._clean_sql_comments(stmt)
        op, dest, cols = "UNKNOWN", "N/A", {}
        
        # 1. DDL Statements (CREATE/ALTER VIEW/PROC)
        ddl_match = re.search(r'\b(CREATE|ALTER)\s+(VIEW|PROCEDURE|PROC)\s+([\[\]\w\.]+)', clean, re.IGNORECASE)
        if ddl_match:
            op = f"{ddl_match.group(1).upper()} {ddl_match.group(2).upper()}"
            dest = ddl_match.group(3).replace('[', '').replace(']', '')
            
            # Find the body after AS
            as_match = re.search(r'\bAS\b', clean, re.IGNORECASE)
            if as_match:
                body = clean[as_match.end():].strip()
                cols = self.parse_sql_deep(body)
        
        # 2. INSERT Statements
        elif re.search(r'\bINSERT\b', clean, re.IGNORECASE):
            op = "INSERT"
            m = re.search(r'INSERT\s+(?:INTO\s+)?([\[\]\w\.]+)', clean, re.IGNORECASE)
            dest = m.group(1) if m else "N/A"
            sm = re.search(r'\bSELECT\b', clean, re.IGNORECASE)
            if sm: cols = self.parse_sql_deep(clean[sm.start():])
            
        # 3. SELECT INTO Statements
        elif re.search(r'\bINTO\b.*\bFROM\b', clean, re.IGNORECASE | re.DOTALL):
            op = "SELECT INTO"
            m = re.search(r'\bINTO\s+([\[\]\w\.]+)', clean, re.IGNORECASE)
            dest = m.group(1) if m else "N/A"
            cols = self.parse_sql_deep(re.sub(r'\bINTO\s+[\(\[\]\w\.]+', '', clean, flags=re.IGNORECASE))
            
        # 4. Pure SELECT
        else:
            res = self.parse_sql_deep(clean)
            if res: 
                op, cols = 'SELECT', res
        
        return {
            'Operation': op, 
            'Destination': dest, 
            'Sources': list(set(re.findall(r'(?:FROM|JOIN)\s+([\[\]\w\.]+)', clean, re.IGNORECASE))), 
            'Columns': cols, 
            'Join Keys': self.extract_join_conditions(clean)
        }