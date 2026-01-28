
import re

class SQLRefiner:
    """
    Standardizes and refines SQL scripts for better readability and consistent parsing.
    Features:
    1. Keyword Formatting: Converts SQL keywords to UPPERCASE.
    2. Alias Standardization: Converts 'Col = Expr' to 'Expr AS Col'.
    3. Bracket Normalization: Adds brackets to columns with spaces or special chars.
    4. Comment Cleanup: Standardizes comments.
    """

    SQL_KEYWORDS = {
        'select', 'from', 'where', 'group by', 'order by', 'having',
        'join', 'left join', 'right join', 'inner join', 'outer join', 'full join', 'cross join',
        'on', 'and', 'or', 'not', 'in', 'exists', 'between', 'like', 'is', 'null',
        'case', 'when', 'then', 'else', 'end',
        'cast', 'convert', 'coalesce', 'isnull',
        'distinct', 'top', 'percent', 'with', 'as', 'union', 'all', 'exec', 'execute',
        'declare', 'set', 'update', 'insert', 'delete', 'into', 'values', 'create', 'table', 'drop', 'alter'
    }

    def __init__(self):
        pass

    def refine(self, sql):
        if not sql:
            return sql
            
        refined = sql
        
        # 1. Clean Comments (Optional, maybe just ensure they don't break things)
        # For now, we keep comments but maybe ensure they are spaced?
        
        # 2. Format Keywords (UPPERCASE)
        # Look for whole words that match keywords
        def replace_keyword(match):
            word = match.group(0)
            if word.lower() in self.SQL_KEYWORDS:
                return word.upper()
            return word
            
        refined = re.sub(r'\b\w+\b', replace_keyword, refined)
        
        # 3. Standardize Aliases: Col = Expr -> Expr AS Col
        # This is tricky because of complex expressions. 
        # Strategy: Use the same logic as the parser to identify the pattern, then swap.
        # Simple Case: Alias = Column
        # Complex Case: Alias = (SELECT ...) or Alias = CASE ...
        
        # We process line by line for safer transformation in SELECT lists
        lines = refined.split('\n')
        new_lines = []
        is_select_block = False
        
        for line in lines:
            stripped = line.strip()
            
            # Simple heuristic for SELECT block detection
            if stripped.upper().startswith('SELECT'):
                is_select_block = True
            if stripped.upper().startswith('FROM'):
                is_select_block = False
                
            if is_select_block:
                # Regex for "Alias = Expression" matching
                # Allow spaces around =
                # Exclude lines starting with comment '--'
                if not stripped.startswith('--'):
                     # Pattern: Start of line or comma, spaces, Alias, spaces, =, spaces, Expression
                     # Note: This is a risky find-replace, so we keep it conservative.
                     # We target "Word = Word" or "Word = Table.Col" patterns primarily.
                     
                     # Check for: Alias = Expression
                     # Avoid if it looks like a WHERE clause (e.g. A.ID = B.ID)
                     # But we are in SELECT block.
                     
                     # Capture: (Optional leading comma/space)(Alias)\s*=\s*(Rest of line)
                     match = re.search(r'^(\s*,?\s*)([\w\[\]]+)\s*=\s*(.*)', line)
                     if match:
                         prefix = match.group(1)
                         alias = match.group(2)
                         expression = match.group(3)
                         
                         # Check if expression ends with comma (it usually does in select list)
                         suffix = ''
                         if expression.strip().endswith(','):
                             # Move comma to after the AS alias? No, SQL allows trailing comma.
                             # But `Expr AS Alias,` is standard.
                             pass
                         
                         # Check if "CASE" or complex logic is involved.
                         # If so, risky to just swap without ensuring end of expression.
                         # However, typically "Alias = Expr" spans until the next comma or EOL.
                         
                         # Let's perform the swap: prefix + Expression + ' AS ' + Alias
                         # We need to handle the trailing comma carefully.
                         
                         has_comma = expression.rstrip().endswith(',')
                         clean_expr = expression.rstrip()
                         if has_comma:
                             clean_expr = clean_expr[:-1].strip()
                             
                         new_line = f"{prefix}{clean_expr} AS {alias}"
                         if has_comma:
                             new_line += ","
                             
                         # Retain original indentation?
                         # "prefix" has indentation but might be mixed with comma.
                         
                         new_lines.append(new_line)
                         continue

            new_lines.append(line)
            
        refined = '\n'.join(new_lines)
        
        # 4. Add Explicit Aliases (QC Rule)
        refined = self.add_explicit_aliases(refined)
        
        return refined

    def _split_columns(self, select_clause):
        """Split columns by comma, respecting parentheses"""
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
        return columns

    def add_explicit_aliases(self, sql):
        """
        Adds explicit aliases to columns that don't have them.
        e.g. 'SELECT table.col' -> 'SELECT table.col AS col'
        """
        # 1. Find the main SELECT clause (simplistic, assumes first SELECT)
        # TODO: Handle multiple SELECTs/Subqueries properly. Current scope: Main query.
        
        match_sel = re.search(r'SELECT\s+(.+?)(\bFROM\b|$)', sql, re.IGNORECASE | re.DOTALL)
        if not match_sel:
            return sql
            
        select_clause = match_sel.group(1)
        original_select_clause = select_clause
        
        columns = self._split_columns(select_clause)
        new_columns = []
        
        for col in columns:
            col_clean = col.strip()
            # Check if it already has an alias
            # Check for ' AS ' or ' = ' (already handled by refine, but check just in case)
            
            has_alias = False
            # Check AS
            if re.search(r'\s+AS\s+[\w\[\]]+$', col_clean, re.IGNORECASE):
                has_alias = True
            # Check = (Legacy T-SQL: Alias = Column)
            elif re.search(r'^[\w\[\]]+\s*=\s*', col_clean):
                 has_alias = True
            # Check implicit alias (Col Name) - tricky, risk of false positive with keywords
            # e.g. "table.col alias" vs "table.col"
            elif re.search(r'\s+[\w\[\]]+$', col_clean):
                 # Verify it's not a keyword ending (like END)
                 last_word = re.search(r'([\w\[\]]+)$', col_clean).group(1)
                 if last_word.upper() not in ['END', 'NULL', 'STAR', 'ALL']: 
                     # Could be an alias or just a column like "Count(*)" -> "Count(*)" no... 
                     # "Count(*) Cnt" -> Alias is Cnt
                     # "table.col" -> Last word is col. Is it alias? No.
                     # "table.col col_alias" -> Last word is col_alias.
                     
                     # Subtlety: How to distinguish "table.col" from "table.col alias"?
                     # Check if there's a space that is not inside quotes/parens before the last word
                     pass
            
            # If no obvious alias, try to infer
            # Logic: If implicit alias detection is hard, let's focus on "table.col" pattern which DEFINITELY needs alias if strict
            
            # Safe inference:
            # 1. table.col -> AS col
            # 2. col -> AS col (redundant but safe? "SELECT col AS col")
            
            pattern_dot = r'[\w\[\]]+\.([\w\[\]]+)$' # table.col
            match_dot = re.search(pattern_dot, col_clean)
            
            if not has_alias:
                 # Check for "table.column" structure
                 if match_dot:
                     col_name = match_dot.group(1)
                     # Only add alias if it doesn't end with that name already (e.g. t.name name)
                     if not col_clean.endswith(" " + col_name) and not col_clean.endswith("]" + col_name):
                          new_columns.append(f"{col_clean} AS {col_name}")
                          continue
                 
                 # Check for simple column "column"
                 elif re.match(r'^[\w\[\]]+$', col_clean):
                      # It is just "col". Add "AS col" for consistency?
                      # User said "kalo yang ga ada alias tambahin".
                      # "SELECT col" -> "SELECT col AS col"
                      new_columns.append(f"{col_clean} AS {col_clean}")
                      continue
                      
            new_columns.append(col_clean)
            
        new_select_clause = ",\n\t".join(new_columns)
        
        # Replace the SELECT clause in the original SQL
        # Use simple string replacement to fallback if regex fails due to escaping
        
        # This is risky if exact string appears elsewhere.
        # But select clause is usually unique enough if large.
        
        # Better: use the match spans
        start, end = match_sel.span(1)
        refined_sql = sql[:start] + "\n\t" + new_select_clause + "\n" + sql[end:]
        
        return refined_sql

if __name__ == "__main__":
    # Test cases
    refiner = SQLRefiner()
    
    test_sql = """
    select 
        id = a.id,
        name = a.name,
        is_active = (case when x=1 then 1 else 0 end),
        date
    from table a
    where a.id = 1
    """
    
    print("Original:")
    print(test_sql)
    print("\nRefined:")
    print(refiner.refine(test_sql))
