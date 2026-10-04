# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Imports every blinkview module in the current interpreter; exits non-zero on any failure.

Run against an installed wheel on each supported Python - development happens on 3.14, whose lazy
annotations (PEP 649) hide e.g. a class-level annotation naming a TYPE_CHECKING-only import, which
is a NameError at import time on 3.10-3.13.

    python scripts/import_smoke.py
"""

import importlib
import os
import pkgutil
import sys
import traceback

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ.setdefault("QT_API", "pyside6")

# Modules that are entry points or need hardware/OS state just to import.
SKIP = {"blinkview.__main__"}


def main() -> int:
    import blinkview

    failures = []
    count = 0
    for mod in pkgutil.walk_packages(blinkview.__path__, "blinkview."):
        if mod.name in SKIP:
            continue
        count += 1
        try:
            importlib.import_module(mod.name)
        except Exception as e:
            frame = traceback.extract_tb(e.__traceback__)[-1]
            failures.append(f"{mod.name}: {type(e).__name__}: {e} ({frame.filename}:{frame.lineno})")

    print(f"Python {sys.version.split()[0]}: imported {count - len(failures)}/{count} blinkview modules")
    for f in failures:
        print(f"  FAIL {f}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
