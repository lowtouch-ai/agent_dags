#!/usr/bin/env python3
"""
Airflow 3.0 migration helper:
- Deletes entire lines that contain only provide_context=True (with possible indentation + comment)
- Does NOT touch lines that have other content
- Preserves all your original spacing and formatting
"""

from pathlib import Path
import re

# Pattern for lines that are JUST provide_context = True (or true/1) ± comment
LINE_ONLY_PATTERN = re.compile(
    r'^\s*provide_context\s*=\s*(?:True|true|1)\s*(?:#.*)?$',
    re.IGNORECASE
)

def fix_file(filepath: Path) -> bool:
    original_lines = filepath.read_text(encoding="utf-8").splitlines(keepends=True)
    new_lines = []

    changed = False

    for line in original_lines:
        stripped = line.strip()
        if LINE_ONLY_PATTERN.match(stripped):
            changed = True
            # Skip this line completely
            continue
        new_lines.append(line)

    if not changed:
        return False

    new_content = ''.join(new_lines)
    filepath.write_text(new_content, encoding="utf-8")
    return True


def main():
    print("Airflow 3.0 – Deleting full lines containing only provide_context=True")
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
            print(f"  [MODIFIED] {path}  (removed one or more full lines)")

    print("\n" + "═" * 70)
    if changed_files:
        print(f"Modified {len(changed_files)} file(s):")
        for p in changed_files:
            print(f"  • {p}")
    else:
        print("No matching lines found → nothing changed")

    print("\nReview:")
    print("  git diff")
    print("Then commit and let Airflow re-parse the DAGs.")


if __name__ == "__main__":
    main()