import xml.etree.ElementTree as ET

file_path = r'c:\Users\207746\Documents\test_extract\LoadFact.dtsx'
tree = ET.parse(file_path)
root = tree.getroot()

# Find the Lookup component
lookup_name = "Lookup ParamID Manufacturing Year"
comp = None
for c in root.findall('.//component'):
    if c.get('name') == lookup_name:
        comp = c
        break

if not comp:
    print(f"Component '{lookup_name}' not found.")
    exit()

print(f"Component: {comp.get('name')} ({comp.get('componentClassID')})")

print("\nProperties:")
for prop in comp.findall('properties/property'):
    name = prop.get('name')
    val = prop.text
    print(f"  {name}: {val}")
    
print("\nConnections:")
for conn in comp.findall('connections/connection'):
    print(f"  {conn.get('name')}: {conn.get('connectionManagerID')}")

print("\nOutputs:")
for out in comp.findall('outputs/output'):
    print(f"  Output: {out.get('name')}")
    for col in out.findall('outputColumns/outputColumn'):
        print(f"    Col: {col.get('name')} (LineageID: {col.get('lineageId')})")
