# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Regression: SearchableLogArea's deferred pin-to-bottom (scroll_to_end) fired valueChanged,
which LogViewerWidget._on_scroll_value_changed took for a user scroll away from the tail - the
value can land off the bottom when long lines toggle the horizontal scrollbar mid-setValue. It
froze into history (setPlainText) re-entrantly inside QScrollBar.setValue, Qt carried on with the
discarded layout, and the process died with a native access violation. Before crashing, the same
mechanism made the view thrash live <-> history every tick (the "unfiltered flash" seen when
opening a saved log viewer). Found in the blinkview-python-demo client_server profile.

Run in a subprocess: the failure mode is a native crash, which would take pytest down with it."""

import os
import subprocess
import sys
import textwrap

SCRIPT = textwrap.dedent(
    """
    import pathlib, sys, tempfile
    from qtpy.QtWidgets import QApplication
    app = QApplication([])
    from blinkview.core.numpy_batch_manager import PooledLogBatch
    from blinkview.ui.widgets.log_viewer import LogViewerWidget
    from tests.fakes.real_registry import make_real_gui_context, make_real_registry

    reg = make_real_registry(pathlib.Path(tempfile.mkdtemp()), "pin_scroll_crash")
    ctx = make_real_gui_context(reg)
    dev = reg.id_registry.get_device("dev")
    mod = dev.get_module("m")

    # Long lines first (like the system config dumps at startup), then short ones - scrolling
    # the long lines out of view drops the horizontal scrollbar during the pin's setValue.
    msgs = ["config " + "x" * 400] * 100 + [f"short {i}" for i in range(27)]
    src = reg.system_ctx.array_pool.create(
        PooledLogBatch, len(msgs), 1 << 16, has_levels=True, has_modules=True, has_devices=True
    )
    base = reg.now_ns()
    with src:
        for i, m in enumerate(msgs):
            src.insert_any(base + i, base + i, m.encode(), level=0, module=mod.id, device=dev.id)
        reg.central.log_pool.batch_append(src)

    w = LogViewerWidget(ctx)
    w.resize(700, 400)
    w.show()

    rebuilds = []
    original = w.text_area.setPlainText
    w.text_area.setPlainText = lambda text: (rebuilds.append(1), original(text))[1]

    modes = []
    for _ in range(40):
        w.prev_apply = 0
        w.apply_updates()
        for _ in range(3):
            app.processEvents()
        modes.append(w.view_mode.name)

    print("REBUILDS", len(rebuilds))
    print("MODES", ",".join(sorted(set(modes))))
    reg.stop()
    """
)


def test_pin_scroll_does_not_freeze_or_crash():
    env = os.environ.copy()
    env.setdefault("QT_QPA_PLATFORM", "offscreen")
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )

    assert result.returncode == 0, f"subprocess died (native crash?):\n{result.stderr[-3000:]}"
    assert "REBUILDS 0" in result.stdout, result.stdout
    assert "MODES LIVE" in result.stdout, result.stdout
