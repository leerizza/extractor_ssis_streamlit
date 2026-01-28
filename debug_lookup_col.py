import xml.etree.ElementTree as ET

file_path = r'c:\Users\207746\Documents\test_extract\LoadFact.dtsx'
tree = ET.parse(file_path)
root = tree.getroot()

comp_name = "Lookup ParamID Manufacturing Year"
target_col = "ID_Year"

for comp in root.findall('.//component'):
    if comp.get('name') == comp_name:
        for out in comp.findall('.//output'):
            for col in out.findall('.//outputColumn'):
                if col.get('name') == target_col:
                    print(f"Found Column: {target_col}")
                    print("Attributes:")
                    for k,v in col.items():
                        print(f"  {k}: {v}")
                    print("Properties:")
                    for prop in col.findall('properties/property'):
                        print(f"  {prop.get('name')}: {prop.text}")
                    exit()
print("Column not found.")
