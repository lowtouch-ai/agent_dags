#!/usr/bin/env python3
"""
Airflow 3.0 migration helper:
- Replaces 'from airflow.operators.empty import EmptyOperator' → 'from airflow.operators.empty import EmptyOperator'
- Also handles 'from airflow.operators.empty import EmptyOperator...' (older style)
- Replaces all occurrences of 'EmptyOperator(' → 'EmptyOperator(' in the code
- Skips this script file itself
"""

import re
from pathlib import Path

def fix_file(filepath: Path) -> bool:
    """Process one file. Returns True if any change was made."""
    original_content = filepath.read_text(encoding="utf-8")
    content = original_content

    # Step 1: Fix import statements
    # Catch common variations
    content = re.sub(
        r"(from\s+airflow\.operators\.)(dummy|dummy_operator)(\s+import\s+(DummyOperator|EmptyOperator|\*)?)",
        r"from airflow.operators.empty import EmptyOperator",
        content,
        flags=re.IGNORECASE | re.MULTILINE
    )

    # Also catch wildcard or aliased imports (rare, but safe)
    content = re.sub(
        r"from\s+airflow\.operators\.dummy\s+import\s+.*DummyOperator.*",
        r"from airflow.operators.empty import EmptyOperator",
        content,
        flags=re.MULTILINE
    )

    # Step 2: Replace class name usages (EmptyOperator( → EmptyOperator()
    # This is simple string replace – safe in most cases since it's followed by (
    content = content.replace("EmptyOperator(", "EmptyOperator(")

    # Optional: also replace standalone DummyOperator if used in type hints / variables (less common)
    # content = content.replace("DummyOperator", "EmptyOperator")

    if content == original_content:
        return False

    filepath.write_text(content, encoding="utf-8")
    return True


def main():
    print("Airflow 3.0: Replacing DummyOperator → EmptyOperator")
    print("Scanning current directory + subdirectories for .py files...\n")

    changed_files = []

    for filepath in sorted(Path(".").rglob("*.py")):
        if filepath.is_file() and "fix_dummy_to_empty" in filepath.name.lower():
            print(f"  [SKIP] This script: {filepath}")
            continue

        print(f"Checking → {filepath}")

        if fix_file(filepath):
            changed_files.append(filepath)
            print(f"  [MODIFIED] {filepath}")

    print("\n" + "═" * 70)
    if changed_files:
        print(f"Modified {len(changed_files)} file(s):")
        for f in changed_files:
            print(f"  • {f}")
        print("\nReview changes:")
        print("  git diff")
        print("Then:")
        print("  git commit -m 'Migrate DummyOperator to EmptyOperator for Airflow 3 compatibility'")
    else:
        print("No DummyOperator imports or usages found → nothing changed.")

    print("\nAfter commit:")
    print("• Restart Airflow scheduler / webserver or wait for DAG folder re-parse")
    print("• Test DAG parsing → ready for next error?")


if __name__ == "__main__":
    main()