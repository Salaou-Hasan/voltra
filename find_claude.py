import os
import sys

target = "claude_desktop_config.json"
found = []

print(f"Scanning entire system for '{target}'...\n")

for drive in ["C:\\", "D:\\", "E:\\", "F:\\"]:
    if not os.path.exists(drive):
        continue
    print(f"Scanning {drive}...")
    for root, dirs, files in os.walk(drive, topdown=True, onerror=None):
        # Skip junk folders to go faster
        dirs[:] = [d for d in dirs if d not in {
            "Windows", "System32", "SysWOW64", "WinSxS",
            "$Recycle.Bin", "ProgramData\\Microsoft",
            "node_modules", ".git"
        }]
        for file in files:
            if file == target:
                full_path = os.path.join(root, file)
                found.append(full_path)
                print(f"  FOUND: {full_path}")

print("\n--- Results ---")
if found:
    for p in found:
        print(p)
else:
    print("Not found anywhere. Claude Desktop may not have been launched yet.")
    print("Try launching Claude Desktop once — it creates the file on first run.")