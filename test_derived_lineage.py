import sys
import os
import pandas as pd
from app import SSISMetadataExtractor

file_path = "RO_CRE_Phase1.dtsx"
print(f"Testing Derived Lineage on: {file_path}")

try:
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    extractor = SSISMetadataExtractor(content)
    lineage = extractor.get_column_lineage()

    print(f"Total Lineage Rows: {len(lineage)}")
    if lineage:
        df = pd.DataFrame(lineage)
        
        # Check for Derived entries
        derived_entries = df[df['Expression/Logic'].str.contains('Derived', na=False, case=False)]
        
        print(f"\nDerived Column Entries: {len(derived_entries)}")
        if not derived_entries.empty:
            print(derived_entries[['Source Table', 'Original Column', 'Expression/Logic', 'Destination Column']].head(20).to_markdown())
        else:
            print("No 'Derived' logic found in lineage results.")
            # Print general head
            cols_to_show = ['Source Component', 'Source Column', 'Source Table', 'Destination Component', 'Destination Column']
            cols = [c for c in cols_to_show if c in df.columns]
            print(df[cols].head(10).to_markdown())

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
