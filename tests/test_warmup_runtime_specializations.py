# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

SCRIPT = textwrap.dedent(
    """
    import json, sys
    from pathlib import Path

    import blinkview.ui.main_window  # noqa: F401  registers every warmup callback
    from blinkview.core.module_snapshot import LatestModuleValueTracker
    from blinkview.core.numpy_batch_manager import PooledLogBatch
    from blinkview.core.numpy_log import allocate_telemetry_workspace, fetch_telemetry_window
    from blinkview.core.warmup import NumbaWarmupHelper
    from blinkview.core.warmup_registry import _WARMUP_CALLBACKS
    from blinkview.ops.module_snapshot import nb_update_master_arrays_reverse
    from blinkview.ops.telemetry import (
        nb_extract_telemetry_segment_window_backward,
        nb_extract_telemetry_segment_window_forward,
    )
    from tests.fakes.real_registry import make_real_registry

    KERNELS = [
        nb_update_master_arrays_reverse,
        nb_extract_telemetry_segment_window_backward,
        nb_extract_telemetry_segment_window_forward,
    ]

    def counts():
        return {k.__name__: len(k.signatures) for k in KERNELS}

    reg = make_real_registry(Path(sys.argv[1]), "warmup_specializations")
    helper = NumbaWarmupHelper(reg.system_ctx)
    try:
        for _p, cb in sorted(_WARMUP_CALLBACKS, key=lambda i: i[0], reverse=True):
            cb(helper)
        after_warmup = counts()

        mod = helper.floats_mod
        base = helper.time_ns()
        src = helper.array_pool.create(PooledLogBatch, 10, 4096, has_levels=True, has_modules=True, has_devices=True)
        with src:
            for i in range(10):
                ts = base + i * 1_000_000
                src.insert_any(ts, ts, b"1.0 2.0", level=0, module=mod.id, device=mod.device.id)
            helper.log_pool.batch_append(src)

        # Module value tracker: first tick (SEQ_TYPE watermark), then a plain-int watermark as
        # produced by a cold segment's header-derived last_sequence_id.
        tracker = LatestModuleValueTracker(
            helper.log_pool, helper.registry.modules_table, helper.array_pool, helper.time_ns
        )
        tracker.update()
        tracker.last_known_seq = int(tracker.last_known_seq) - 1
        tracker.update()

        # Plotter REPLAY window fetch, as TelemetryPlotter calls it (plus_one=True), anchored
        # inside, before and after the data.
        tf = allocate_telemetry_workspace(2)
        for anchor in (base + 5_000_000, base - 10_000_000_000, base + 10_000_000_000):
            with fetch_telemetry_window(
                helper.array_pool, helper.log_pool, mod.id, num_channels=2, temp_floats=tf,
                anchor_ts_ns=anchor, before_span_ns=30_000_000_000, after_span_ns=30_000_000_000,
                before_cap=500, after_cap=500, plus_one=True,
            ):
                pass

        print("RESULT " + json.dumps({"warmup": after_warmup, "runtime": counts()}))
    finally:
        helper.log_pool.release_all()
        reg.stop()
    """
)


def test_runtime_paths_hit_no_specialization_warmup_missed(tmp_path):
    """Kernels first used from the GUI thread after warmup must already have every specialization
    the app's call paths produce - otherwise the first real use loads/compiles on the GUI thread
    ("[cache] index loaded ..." + UI-monitor lag). Caught two gaps:
    - nb_extract_telemetry_segment_window_forward was never called by TelemetryPlotter.warmup (its
      data all sat before the anchor, and without plus_one the after-loop skipped every segment).
    - nb_update_master_arrays_reverse got a second (int64) specialization once last_known_seq came
      from a cold segment's plain-int header instead of a hot segment's uint64.
    Fresh subprocess: dispatcher signatures and _WARMUP_CALLBACKS are process-global, so an
    in-process run would see whatever earlier tests compiled/cleared."""
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT, str(tmp_path)],
        capture_output=True,
        text=True,
        cwd=str(REPO_ROOT),
        timeout=600,
    )
    assert result.returncode == 0, f"stdout:\n{result.stdout[-4000:]}\n\nstderr:\n{result.stderr[-4000:]}"

    line = next(ln for ln in result.stdout.splitlines() if ln.startswith("RESULT "))
    data = json.loads(line[len("RESULT ") :])

    assert all(n >= 1 for n in data["warmup"].values()), data
    assert data["runtime"] == data["warmup"], data
