# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_fresh_interpreter_can_import_device_identity_first():
    """Regression test for a circular import between core.device_identity and
    core.id_registry.registry: device_identity.py imports core.id_registry.tables (a submodule),
    which forces core.id_registry's __init__.py to run first, which imports registry.py, which
    imported DeviceIdentity/ModuleIdentity back from core.device_identity at module level - a
    module that, in this ordering, is still mid-import and hasn't defined those names yet.

    Previously this only worked by accident: tests/conftest.py pre-imported blinkview.core.registry
    before any test module could import blinkview.core.device_identity as its first touch of the
    cluster (e.g. via a blinkview.ui.widgets.* import), and the real app entry point
    (blinkview.ui.main_window) happened to import core.registry first for the same reason. Any
    caller that imported core.device_identity (or a parser module that imports it) before anything
    imported core.id_registry would hit ImportError.

    The fix moved the DeviceIdentity/ModuleIdentity import in registry.py under TYPE_CHECKING with
    local imports at the 3 call sites that need the real classes at runtime, so the cycle can
    resolve regardless of which side is imported first. This can only be caught in a fresh
    subprocess - an in-process test would find core.id_registry already in sys.modules from other
    test modules importing it first, masking the bug entirely.
    """
    script = textwrap.dedent(
        """
        import blinkview.core.device_identity
        print("DEVICE_IDENTITY_FIRST_OK")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )

    assert result.returncode == 0, f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    assert "DEVICE_IDENTITY_FIRST_OK" in result.stdout
