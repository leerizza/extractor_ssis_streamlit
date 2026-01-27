import sys
import os
import pandas as pd
from app import SSISMetadataExtractor

file_path = "Load DGO.dtsx"
print(f"Testing Lineage on: {file_path}")

try:
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    extractor = SSISMetadataExtractor(content)
    lineage = extractor.get_column_lineage()

    print(f"Total Lineage Rows: {len(lineage)}")
    if lineage:
        df = pd.DataFrame(lineage)
        # Show rows where Source Table is 'Transformation' or 'N/A' or 'Unknown'
        missing = df[df['Source Table'].isin(['Transformation', 'N/A', 'Unknown', 'Unknown Table'])]
        print(f"Missing Source Count: {len(missing)}")
        
        if not missing.empty:
            print("\nExamples of Missing Lineage:")
            print(missing[['Source Component', 'Original Column', 'Destination Column', 'Expression/Logic']].head(20).to_markdown())
        
        # Also print components used
        print(f"\nUnique Source Components linked: {df['Source Component'].unique()}")

except Exception as e:
    print(f"Error: {e}")
