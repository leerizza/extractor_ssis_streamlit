import sys
import os
import pandas as pd
from app import SSISMetadataExtractor

file_path = "Report_Stock_Inv.dtsx"
if not os.path.exists(file_path):
    # Try the other one
    file_path = "Report Application Oto DF Car Syariah.dtsx"

print(f"Testing Lineage Graph on: {file_path}")

try:
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    extractor = SSISMetadataExtractor(content)
    lineage = extractor.get_column_lineage()

    print(f"Total Lineage Rows: {len(lineage)}")
    if lineage:
        df = pd.DataFrame(lineage)
        cols_to_show = ['Source Component', 'Source Column', 'Source Table', 'Destination Component', 'Destination Column']
        # Filter columns that exist
        cols = [c for c in cols_to_show if c in df.columns]
        print(df[cols].head(10).to_markdown())
        
        # Check for specific success criteria (e.g. traced LineageID)
        # We can't easily check internal Map...
        # But if we see 'Source Table' populated, it worked.
        
        traced_count = len(df[df['Source Table'] != 'N/A'])
        print(f"\nTraced Columns: {traced_count} / {len(lineage)}")
        
    else:
        print("No lineage found.")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
