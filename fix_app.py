import os

file_path = r'c:\Users\207746\Documents\test_extract\app.py'

# Read valid content up to line ~1671
valid_lines = []
with open(file_path, 'r', encoding='utf-8') as f:
    lines = f.readlines()
    
    found_garbage_start = False
    for i, line in enumerate(lines):
        # The garbage starts exactly with "    with col2:" at the top level indentation (or within except block which is indentation 4)
        # But wait, looking at my view output (Step 294), "with col2:" is at line 1672.
        # It follows "        st.exception(e)".
        
        # Let's target the exact line 1672 if possible or just search for the start of the garbage sequence.
        if "with col2:" in line and "st.metric" not in line: # Simple heuristic
             # Double check context? 
             # The line before was st.exception(e) (indent 8 spaces)
             # This line is indent 4 spaces?
             # Let's just cut off everything after line 1671.
             if i == 1671: # 0-indexed 1671 is line 1672
                 print(f"Found cut point at line {i+1}: {line.strip()}")
                 # Verify it is indeed "with col2:"
                 if "with col2:" in line:
                    break
    
    valid_lines = lines[:1672] # Keep up to line 1672 (which is the start of garbage, so wait, slice is exclusive)
    # So lines[0...1671] are kept. Line 1671 is index 1671.
    # Wait, lines are 0-indexed lists.
    # Line 1672 in editor is index 1671.
    # I want to KEEP lines 0 to 1671 (inclusive of 1671). ST.exception(e) is at line 1671 (index 1670).
    # So valid_lines = lines[:1672]
    
    # Now append the rest of the file that was valid (The "else:" block)
    # The garbage ends and "else:" starts later.
    # I need to find where "else:" block starts.
    # It was around line 2238 in previous views.
    # I will search for "else:" starting from column 0 (or indent 0)
    
    rest_of_file = []
    found_else = False
    for i in range(1672, len(lines)):
        line = lines[i]
        if line.startswith("else:") or line.strip() == "else:":
            found_else = True
            rest_of_file = lines[i:]
            print(f"Found 'else:' block at line {i+1}")
            break
            
if valid_lines and found_else:
    new_content = "".join(valid_lines + rest_of_file)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)
    print("Successfully repaired app.py")
else:
    print("Could not identify cut points safely. Aborting.")
