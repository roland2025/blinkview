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


def test_fresh_interpreter_registers_every_factory_category(tmp_path):
    """Regression test for a real bug found running the actual app (not caught by the rest of
    the suite): an "optimize imports" pass moved `from blinkview.core.central_storage import
    CentralStorage` in core/registry.py under `if TYPE_CHECKING:` because it's only referenced in
    a string-quoted annotation. That's correct for the annotation, but central_storage.py also
    defines CentralFactory, decorated with @register_factory_category(FactoryCategory.CENTRAL) -
    a TYPE_CHECKING import never executes at runtime, so the decorator never fired and
    'central' silently dropped out of the FactoryRegistry. Registry() then raised KeyError on
    `factories.get_produced_type(FactoryCategory.CENTRAL)` the moment a real user ran `blink gui`.

    Every other test in this suite already imports blinkview.core.central_storage indirectly
    (e.g. via tests/fakes/real_registry.py or other test modules importing Registry/CentralStorage
    first), which populates the global _FACTORY_REGISTRATIONS list before any assertion here would
    run - so an in-process test can't detect this class of bug at all. Only a fresh subprocess,
    with nothing on the import graph except what core/registry.py itself pulls in, reproduces
    what a user actually hits."""
    config_path = tmp_path / "cfg.json"
    script = textwrap.dedent(
        f"""
        from blinkview.core.registry import Registry
        from blinkview.core.constants import FactoryCategory

        reg = Registry(session_name="fresh_proc_check", log_dir={str(tmp_path)!r}, config_path={str(config_path)!r})

        categories = [
            v for k, v in vars(FactoryCategory).items() if isinstance(v, str) and not k.startswith("_")
        ]
        missing = [c for c in categories if reg.system_ctx.factories.get_factory(c) is None]
        assert not missing, f"Factory categories never registered: {{missing}}"
        print("FACTORY_CATEGORIES_OK")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
    )

    assert result.returncode == 0, f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
    assert "FACTORY_CATEGORIES_OK" in result.stdout
