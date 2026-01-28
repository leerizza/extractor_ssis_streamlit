import xml.etree.ElementTree as ET

file_path = r'c:\Users\207746\Documents\test_extract\DatamartCentro.dtsx'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

root = ET.fromstring(content)
ns = {'DTS': 'www.microsoft.com/SqlServer/Dts'}

print(f"Root Tag: {root.tag}")

# Try finding ConnectionManagers
cms = root.find('DTS:ConnectionManagers', ns) # Direct child
if cms:
    print("Found DTS:ConnectionManagers (Direct Child)")
    count = 0
    for cm in cms.findall('DTS:ConnectionManager', ns):
        name = cm.get(f"{{{ns['DTS']}}}ObjectName")
        print(f"  CM: {name}")
        count += 1
    print(f"Total direct generic find: {count}")

# Try the XPath I implemented
xpath1 = root.findall('./DTS:ConnectionManagers/DTS:ConnectionManager', ns)
print(f"\nXPath './DTS:ConnectionManagers/DTS:ConnectionManager': Found {len(xpath1)}")
for x in xpath1:
     name = x.get(f"{{{ns['DTS']}}}ObjectName")
     print(f"  StartMatch: {name}")

# Try without ./
xpath2 = root.findall('DTS:ConnectionManagers/DTS:ConnectionManager', ns)
print(f"\nXPath 'DTS:ConnectionManagers/DTS:ConnectionManager': Found {len(xpath2)}")

# Try recursive .//
xpath3 = root.findall('.//DTS:ConnectionManager', ns)
print(f"\nXPath './/DTS:ConnectionManager': Found {len(xpath3)}")
for x in xpath3:
     name = x.get(f"{{{ns['DTS']}}}ObjectName")
     creation_name = x.get(f"{{{ns['DTS']}}}CreationName")
     print(f"  Recursive: ObjectName={name}, CreationName={creation_name}")
