from app import SSISMetadataExtractor
import json

file_path = r'c:\Users\207746\Documents\test_extract\LoadFact.dtsx'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

extractor = SSISMetadataExtractor(content)
sources = extractor.get_dataflow_sources()

target_name = "Lookup ParamID Manufacturing Year"
found = None
for s in sources:
    if s['Component Name'] == target_name:
        found = s
        break

if found:
    print(f"Found Component: {found['Component Name']}")
    print(f"DFT: {found['Data Flow Task']}")
    print(f"SQL: {found['SQL Query']}")
    print("Output Columns:")
    for col in found['Output Columns']:
        print(f"  Alias: {col['Column Alias']}, SourceTable: {col['Source Table']}, SourceCol: {col['Original Column']}")
else:
    print("Component not found in get_dataflow_sources()")
