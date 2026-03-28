# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def print_used_modules():
    import sys

    # Extract only the root part of the module name (e.g., 'PySide6' from 'PySide6.QtWidgets')
    top_level = sorted({m.split(".")[0] for m in sys.modules.keys()})

    print(f"--- Unique Top-Level Packages ({len(top_level)}) ---")
    for pkg in top_level:
        # Optional: ignore internal Python 'underscore' modules for clarity
        if not pkg.startswith("_"):
            print(pkg)
