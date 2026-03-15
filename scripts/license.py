#!/usr/bin/env python3
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
import re

# --- CONFIGURATION ---
AUTHOR = "Roland Uuesoo"
YEAR = "2026"
# Only process these top-level directories
INCLUDE_DIRS = {"examples", "scripts", "tools", "src"}

HEADER_TEMPLATE = f"""# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) {YEAR} {AUTHOR}
"""

MPL_REGEX = re.compile(
    r"# This Source Code Form is subject to the terms of the Mozilla Public.*?\n# Copyright \(c\) \d{4} .*?\n",
    re.DOTALL
)


def update_file_header(file_path: Path):
    """Reads, cleans, and prepends the license header to a file."""
    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"❌ Failed to read {file_path}: {e}")
        return

    shebang = ""
    if content.startswith("#!"):
        parts = content.split("\n", 1)
        shebang = parts[0] + "\n"
        content = parts[1] if len(parts) > 1 else ""

    content = MPL_REGEX.sub("", content).lstrip()
    new_content = f"{shebang}{HEADER_TEMPLATE}\n{content}"

    try:
        file_path.write_text(new_content, encoding='utf-8')
        print(f"✅ Updated: {file_path}")
    except Exception as e:
        print(f"❌ Failed to write {file_path}: {e}")


def process_project(root_dir: Path):
    """Iterates only through allowed directories."""
    for folder_name in INCLUDE_DIRS:
        target_path = root_dir / folder_name

        if not target_path.exists():
            print(f"ℹ️  Skipping {folder_name} (folder does not exist)")
            continue

        print(f"📂 Processing directory: {folder_name}")
        for py_file in target_path.rglob("*.py"):
            # Ensure we don't modify the running script if it's in /scripts
            if py_file.resolve() == Path(__file__).resolve():
                continue
            update_file_header(py_file)


if __name__ == "__main__":
    # Assuming the script is in /scripts, project root is one level up
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent

    print(f"🚀 Running selective license update on: {project_root}")
    process_project(project_root)
