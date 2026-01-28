import os

file_path = r'c:\Users\207746\Documents\test_extract\app.py'

with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()

valid_lines = []
exception_line_idx = -1

# 1. Find the cut point (st.exception(e))
for i, line in enumerate(lines):
    if "st.exception(e)" in line:
        exception_line_idx = i
        break

if exception_line_idx == -1:
    print("Could not find st.exception(e). Aborting.")
    exit(1)

# Keep up to the exception line (inclusive)
valid_lines = lines[:exception_line_idx+1]

# 2. Find the correct else: block (Indent 0)
rest_of_file = []
found_else = False

# We start scanning from the cut point
for i in range(exception_line_idx + 1, len(lines)):
    line = lines[i]
    # Check for 'else:' at the very beginning of the line (no indentation)
    if line.startswith("else:") and line.strip() == "else:":
        # Verify it's the correct else block by checking next line
        if i+1 < len(lines) and "st.info" in lines[i+1]:
             print(f"Found correct 'else:' at line {i+1}")
             rest_of_file = lines[i:]
             found_else = True
             break
        else:
             print(f"Found 'else:' at line {i+1} but it doesn't look like the main one.")

if found_else:
    new_content = "".join(valid_lines + rest_of_file)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Successfully repaired app.py (v2)")
else:
    print("Could not find the main 'else:' block. Aborting.")
