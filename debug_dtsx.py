import xml.etree.ElementTree as ET
import re

file_path = r'c:\Users\207746\Documents\test_extract\LoadFact.dtsx'
try:
    tree = ET.parse(file_path)
    root = tree.getroot()
except Exception as e:
    print(f"Error parsing: {e}")
    exit(1)

ns = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
# SSIS 2008 uses different namespaces usually, but let's try generic or handle local name.

# Find destination component
# Destination tables are usually in "properties" keys like "OpenRowset" or "TableName"
# or in the name of the component.

dest_comp = None
for comp in root.findall('.//component'): # pipeline component
    # Check properties for TableName = Fact_Num_Of_Citizen
    name = comp.get('name')
    properties = comp.findall('properties/property')
    for prop in properties:
        val = prop.text
        if val and 'Fact_Num_Of_Citizen' in val:
            print(f"Found Destination Component: {name}")
            dest_comp = comp
            break
    if dest_comp: break

if not dest_comp:
    print("Could not find component with 'Fact_Num_Of_Citizen'")
    exit(1)

# Find Input Columns for this component
input_cols = dest_comp.findall(".//input/inputColumns/inputColumn")
print(f"Found {len(input_cols)} input columns in destination.")

lineage_map = {}
for col in input_cols:
    lid = col.get('lineageId')
    name = col.get('cachedName') # or name
    # The 'name' attribute in inputColumn is usually the name of the INPUT column (which might map to dest column)
    # The 'externalMetadataColumnId' maps to the external table column.
    
    # We are interested in 'SK_Guarantor' and 'SK_Region'
    if name in ['SK_Guarantor', 'SK_Region']:
        print(f"Tracing Input Column: {name}, Source LineageID: {lid}")
        lineage_map[name] = lid

# Now search for who produces these LineageIDs
# They must be Output Columns of some component.
found_sources = {}
for comp in root.findall('.//component'):
    outputs = comp.findall('outputs/output/outputColumns/outputColumn')
    for out in outputs:
        lid = out.get('lineageId')
        name = out.get('name')
        
        # Check if this lid matches any we are looking for
        for target_name, target_lid in lineage_map.items():
            if lid == target_lid:
                print(f"Found Source for {target_name}: Component '{comp.get('name')}', Output Column '{name}'")
                found_sources[target_name] = comp

# Check the properties of the source component (SQL Query?)
if not found_sources:
    print("Could not find sources for the columns.")
else:
    source_comp = list(found_sources.values())[0] # Assume same source
    print(f"Source Component Name: {source_comp.get('name')}")
    
    # Extract SQL
    sql_query = None
    properties = source_comp.findall('properties/property')
    for prop in properties:
        pname = prop.get('name')
        if pname in ['SqlCommand', 'SqlStatementSource']:
            sql_query = prop.text
            print(f"SQL Query found (Name={pname})")
            break
    
    if sql_query:
        print("\n--- SQL QUERY ---")
        print(sql_query[:500] + "...")
        print("\n-----------------")
        
        # Now try to verify if 'SK_Guarantor' is in the text of the query
        if 'SK_Guarantor' in sql_query:
            print("SK_Guarantor IS present in the SQL text.")
        else:
            print("SK_Guarantor is NOT present in the SQL text explicitly.")
            
    else:
        print("No SQL Command found in source component properties.")
