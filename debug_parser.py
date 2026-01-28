from sql_parser import SQLParser

sql_query = """
SELECT 20171201 SK_Time, 
	   rc.SK_RegionCitizen,
	   isnull(c.SK_Region,1)SK_Region,
	   a.Productive,
	   a.JumlahPenduduk,
	   JumlahUsiaProduktif
FROM something
"""

parser = SQLParser()
# Assuming extract_statement_metadata or similar is the entry point
try:
    meta = parser.extract_statement_metadata(sql_query)
    print("Parsed Metadata:")
    print(meta)
    
    cols = meta.get('Columns', {})
    print("\nExtracted Columns:")
    for col, source in cols.items():
        print(f"  {col}: {source}")
        
    if 'SK_Region' in cols:
        print("\nSUCCESS: SK_Region found.")
    else:
        print("\nFAILURE: SK_Region NOT found.")
except Exception as e:
    print(f"Error: {e}")
