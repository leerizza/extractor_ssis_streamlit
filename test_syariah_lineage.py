import sys
import os
import pandas as pd
from app import SSISMetadataExtractor

file_path = "Report Application Oto DF Car Syariah.dtsx"
print(f"Testing Lineage on: {file_path}")

try:
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    extractor = SSISMetadataExtractor(content)
    lineage = extractor.get_column_lineage()

    print(f"Total Lineage Rows: {len(lineage)}")
    if lineage:
        df = pd.DataFrame(lineage)
        # Show rows where Source Table is 'Transformation', 'N/A', 'Unknown', 'Unknown Table'
        missing = df[df['Source Table'].isin(['Transformation', 'N/A', 'Unknown', 'Unknown Table', 'Variable/Expression'])]
        print(f"Missing Source Count: {len(missing)}")
        
        if not missing.empty:
            print("\nExamples of Missing Lineage:")
            # Group by Component to see common culprits
            print(missing.groupby('Source Component').size().to_markdown())
            
            # Print ClassID of 'Bravo'
            found = False
            for comp in extractor.root.findall('.//component', {}):
                if comp.get('name') == 'Bravo':
                     found = True
                     print(f"\n[DEBUG] Component 'Bravo':")
                     print(f"  ClassID: {comp.get('componentClassID')}")
                     for prop in comp.findall('.//property', {}):
                         name = prop.get('name')
                         if name in ['SqlCommand', 'OpenRowset', 'AccessMode', 'SqlCommandVariable', 'ParameterMapping']:
                             print(f"  Property {name}: {prop.text}")
                     break
            if not found:
                 print("\n[DEBUG] Component 'Bravo' NOT FOUND via iteration.")

            print("\nDetail Samples:")
            print(missing[['Source Component', 'Original Column', 'Destination Column', 'Expression/Logic']].head(20).to_markdown())
        
        # Also print components used
        print(f"\nUnique Source Components linked: {df['Source Component'].unique()}")

except Exception as e:
    print(f"Error: {e}")
