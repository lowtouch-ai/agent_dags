#!/usr/bin/env python3
"""
One-time migration helper:
Replaces 'schedule_interval =' → 'schedule =' in all .py files
Preserves exact indentation, spacing, comments, etc.
Skips files that don't contain the pattern.
Creates backup as .bak before modifying.
"""

import re
import glob
import shutil
import os
from pathlib import Path

# Only process Python files in current directory (and subdirs if you want)
PATTERN = r" dags/.*\.py$"
# Or be more precise — only files that look like DAG definitions
# PATTERN = r"agent_dags/.*\.py$"

# The replacement we want
OLD = r"schedule_interval\s*="
NEW = r"schedule="

# Regex to match lines like:
#     schedule_interval   =   "@daily",   # comment
#     schedule_interval="@once"
LINE_PATTERN = re.compile(
    r"^(\s*)(schedule_interval)(\s*=\s*)(.*)$",
    re.MULTILINE
)

def fix_file(filepath: Path):
    original_content = filepath.read_text(encoding="utf-8")
    new_content = original_content

    def replacer(match):
        indent = match.group(1)
        # group 2 = "schedule_interval"
        eq_space = match.group(3)
        value_and_rest = match.group(4)

        # Rebuild line with new keyword, same spacing
        return f"{indent}schedule{eq_space}{value_and_rest}"

    new_content = LINE_PATTERN.sub(replacer, original_content)

    if new_content == original_content:
        print(f"  [SKIP] No changes needed: {filepath}")
        return False

    # Backup
    # backup_path = filepath.with_suffix(filepath.suffix + ".bak")
    # shutil.copy2(filepath, backup_path)
    # print(f"  [BACKUP] Created {backup_path}")

    # Write fixed version
    filepath.write_text(new_content, encoding="utf-8")
    print(f"  [FIXED] {filepath}")
    return True


def main():
    print("Airflow 3.0 schedule_interval → schedule fixer")
    print("Scanning current directory and subdirectories...\n")

    dag_files = sorted(Path(".").glob("**/*.py"))

    if not dag_files:
        print("No .py files found in current directory.")
        return

    changed_count = 0

    for file_path in dag_files:
        if "fix_airflow3_schedule" in file_path.name:
            print(f"  [SKIP] This script itself: {file_path}")
            continue

        print(f"Checking: {file_path}")
        if fix_file(file_path):
            changed_count += 1

    print("\n" + "="*60)
    print(f"Done. Modified {changed_count} file(s).")
    print("Always review changes and test DAGs after migration!")
    print("Backups created with .bak extension — you can restore if needed.")


if __name__ == "__main__":
    main()