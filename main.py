import os
import sys
from pathlib import Path

# Common filenames for Claude desktop config
TARGET_NAMES = {
    "claude_desktop_config.json",
    "claude.json",
    "config.json",  # broader fallback
}

# Directories to skip (system/protected/irrelevant)
SKIP_DIRS = {
    "$Recycle.Bin", "System Volume Information",  # Windows
    "proc", "sys", "dev",                         # Linux
    ".git", "node_modules", "__pycache__",
}

def get_root_paths():
    """Return all root paths to scan depending on OS."""
    if sys.platform == "win32":
        import string
        drives = [
            f"{d}:\\" for d in string.ascii_uppercase
            if os.path.exists(f"{d}:\\")
        ]
        return drives
    else:
        return ["/"]  # macOS and Linux

def scan(roots):
    found = []
    scanned = 0
    skipped = 0

    for root in roots:
        print(f"\n[+] Scanning root: {root}")
        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            # Skip protected/irrelevant directories in-place
            dirnames[:] = [
                d for d in dirnames
                if d not in SKIP_DIRS and not d.startswith(".")
            ]

            scanned += 1
            if scanned % 5000 == 0:
                print(f"    ... {scanned} directories scanned, {len(found)} found so far")

            for filename in filenames:
                if filename.lower() in TARGET_NAMES:
                    full_path = os.path.join(dirpath, filename)
                    # Prioritize actual Claude config files
                    if "claude" in full_path.lower() or filename != "config.json":
                        found.append(full_path)
                        print(f"  [FOUND] {full_path}")

    return found, scanned

def main():
    print("=" * 60)
    print("  Claude Desktop Config Scanner")
    print("=" * 60)

    # Also check known default locations first (fast path)
    known_paths = []
    home = Path.home()

    if sys.platform == "win32":
        appdata = os.environ.get("APPDATA", "")
        known_paths = [
            Path(appdata) / "Claude" / "claude_desktop_config.json",
            home / "AppData" / "Roaming" / "Claude" / "claude_desktop_config.json",
        ]
    elif sys.platform == "darwin":
        known_paths = [
            home / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json",
        ]
    else:  # Linux
        known_paths = [
            home / ".config" / "Claude" / "claude_desktop_config.json",
            home / ".config" / "claude" / "claude_desktop_config.json",
        ]

    print("\n[*] Checking known default locations first...")
    quick_hits = []
    for p in known_paths:
        if p.exists():
            print(f"  [FOUND] {p}")
            quick_hits.append(str(p))
        else:
            print(f"  [MISS]  {p}")

    print("\n[*] Starting full device scan (this may take a few minutes)...")

    try:
        roots = get_root_paths()
        found, scanned = scan(roots)
    except KeyboardInterrupt:
        print("\n[!] Scan interrupted by user.")
        found, scanned = [], 0

    # Merge and deduplicate
    all_found = list(dict.fromkeys(quick_hits + found))

    print("\n" + "=" * 60)
    print(f"  Scan complete. {scanned} directories scanned.")
    print(f"  Total matches found: {len(all_found)}")
    print("=" * 60)

    if all_found:
        print("\nAll found paths:")
        for i, path in enumerate(all_found, 1):
            print(f"  {i}. {path}")
    else:
        print("\nNo Claude config file found on this device.")
        print("Tip: Make sure Claude Desktop is installed.")

if __name__ == "__main__":
    main()