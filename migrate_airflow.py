#!/usr/bin/env python3
"""
Airflow 3.0 migration helper:
Deletes entire lines containing provide_context=True / true / 1
(with optional trailing comma and/or comment)
"""

from pathlib import Path
import re

# Improved pattern: allows optional trailing comma
LINE_ONLY_PATTERN = re.compile(
    r'^\s*provide_context\s*=\s*(?:True|true|1)\s*,?\s*(?:#.*)?$',
    re.IGNORECASE
)

def fix_file(filepath: Path) -> bool:
    original_lines = filepath.read_text(encoding="utf-8").splitlines(keepends=True)
    new_lines = []
    changed = False

    for line in original_lines:
        stripped_for_match = line.rstrip()   # remove trailing newline/spaces only for matching
        if LINE_ONLY_PATTERN.match(stripped_for_match):
            changed = True
            continue  # skip (delete) this line
        new_lines.append(line)

    if not changed:
        return False

    new_content = ''.join(new_lines)
    filepath.write_text(new_content, encoding="utf-8")
    return True


def main():
    print("Airflow 3.0 – Deleting full lines with provide_context=True (comma-aware)")
    print("Scanning .py files...\n")

    python_files = sorted(p for p in Path(".").rglob("*.py") if p.is_file())

    changed_files = []

    for path in python_files:
        name_lower = path.name.lower()
        if "migrate" in name_lower or "fix" in name_lower or "remove" in name_lower:
            print(f"  [SKIP] {path}")
            continue

        print(f"Checking: {path}")

        if fix_file(path):
            changed_files.append(path)
            print(f"  [MODIFIED] {path}")

    print("\n" + "═" * 70)
    if changed_files:
        print(f"Modified {len(changed_files)} file(s)")
        print("Review changes with: git diff")
    else:
        print("No matching lines found")


if __name__ == "__main__":
    main()