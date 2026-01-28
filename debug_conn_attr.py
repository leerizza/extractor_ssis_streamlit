import xml.etree.ElementTree as ET

file_path = r'c:\Users\207746\Documents\test_extract\DatamartCentro.dtsx'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

root = ET.fromstring(content)
ns_dict = {'DTS': 'www.microsoft.com/SqlServer/Dts'}
ns_url = '{www.microsoft.com/SqlServer/Dts}'

print("Scanning Connections...")
connections = root.findall('./DTS:ConnectionManagers/DTS:ConnectionManager', ns_dict)
for conn in connections:
    name = conn.get(f'{ns_url}ObjectName')
    print(f"\nOuter Name: {name}")
    
    inner = conn.find('.//DTS:ConnectionManager', ns_dict)
    if inner is not None:
        print("  Found Inner ConnectionManager")
        # Try getting ConnectionString with Namespace
        cs = inner.get(f'{ns_url}ConnectionString')
        print(f"  CS (with ns): {cs}")
        
        # Try without Namespace (unlikely but possible if ET is weird)
        cs_no_ns = inner.get('ConnectionString')
        print(f"  CS (no ns): {cs_no_ns}")
        
        # Debug all attributes
        print("  Attributes:")
        for k, v in inner.items():
            print(f"    {k}: {v[:20]}...")
    else:
        print("  No Inner ConnectionManager found")
