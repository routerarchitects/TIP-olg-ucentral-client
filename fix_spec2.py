import os
import re

file_path = "/home/carlos/routerarchitects_github/TIP-olg-ucentral-client/SPEC.md"
with open(file_path, "r") as f:
    content = f.read()

# Delete ProtocolState entirely
content = re.sub(r'\s*type ProtocolState string\n(\s+\w+\s+ProtocolState\s+=\s+"[^"]+"\n)+', '\n', content)

# Delete Protocol ProtocolState
content = re.sub(r'\s+Protocol\s+ProtocolState', '', content)

with open(file_path, "w") as f:
    f.write(content)
print("Done")
