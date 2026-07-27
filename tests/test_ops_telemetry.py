# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.dtypes import TS_UNSPECIFIED
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import fetch_telemetry_window
from blinkview.core.types.telemetry import TsWindowBundle
from blinkview.ops.telemetry import (
    nb_extract_telemetry_segment_window_backward,
    nb_extract_telemetry_segment_window_forward,
)
from tests.fakes.log_bundle import make_log_bundle
from tests.fakes.real_log_pool import make_real_log_pool

MODULE_A = 1
MODULE_B = 2

# Permissive (admit-everything) effective_mask for tests that don't care about level filtering -
# LogLevel.ALL == 0, the lowest threshold, so `levels[i] < mask[modules[i]]` is never true.
PERMISSIVE_MASK = np.zeros(10, dtype=dtypes.LEVEL_TYPE)


def make_telemetry_bundle(timestamps, modules, values, levels=None):
    """Builds a LogBundle whose message bytes are telemetry-parseable floats (one value per
    row here - nb_extract_floats_from_bytes handles multi-channel too, but one channel is
    enough to exercise the window-bounding logic these kernels add)."""
    messages = [str(v) for v in values]
    return make_log_bundle(
        timestamps,
        devices=[0] * len(timestamps),
        levels=levels if levels is not None else [0] * len(timestamps),
        modules=modules,
        sequences=list(range(1, len(timestamps) + 1)),
        messages=messages,
        rx_timestamps=timestamps,
        has_pids=False,
        has_tids=False,
    )


def test_backward_kernel_respects_start_and_end_ts_bounds():
    bundle = make_telemetry_bundle(
        timestamps=[10, 20, 30, 40, 50],
        modules=[MODULE_A] * 5,
        values=[1.0, 2.0, 3.0, 4.0, 5.0],
    )
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)
    out_times = np.zeros(5, dtype=dtypes.PLOT_TS_TYPE)
    out_times_int64 = np.zeros(5, dtype=np.int64)
    out_values = np.zeros((5, 1), dtype=dtypes.PLOT_VAL_TYPE)

    # window [15, 40] -> rows with ts 20/30/40 (values 2,3,4)
    window = TsWindowBundle(start_ts=dtypes.TS_TYPE(15), end_ts=dtypes.TS_TYPE(40))
    write_idx, _ = nb_extract_telemetry_segment_window_backward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 5, PERMISSIVE_MASK, False, 0
    )

    assert write_idx == 2  # 3 rows written backward from index 5 -> occupy [2, 5)
    assert list(out_times_int64[2:5]) == [20, 30, 40]
    assert list(out_values[2:5, 0]) == [2.0, 3.0, 4.0]


def test_forward_kernel_respects_start_and_end_ts_bounds():
    bundle = make_telemetry_bundle(
        timestamps=[10, 20, 30, 40, 50],
        modules=[MODULE_A] * 5,
        values=[1.0, 2.0, 3.0, 4.0, 5.0],
    )
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)
    out_times = np.zeros(5, dtype=dtypes.PLOT_TS_TYPE)
    out_times_int64 = np.zeros(5, dtype=np.int64)
    out_values = np.zeros((5, 1), dtype=dtypes.PLOT_VAL_TYPE)

    # window [20, 45] -> rows with ts 20/30/40 (values 2,3,4)
    window = TsWindowBundle(start_ts=dtypes.TS_TYPE(20), end_ts=dtypes.TS_TYPE(45))
    write_idx, _ = nb_extract_telemetry_segment_window_forward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 0, PERMISSIVE_MASK, False, 0
    )

    assert write_idx == 3
    assert list(out_times_int64[:3]) == [20, 30, 40]
    assert list(out_values[:3, 0]) == [2.0, 3.0, 4.0]


def test_both_kernels_filter_by_target_module():
    bundle = make_telemetry_bundle(
        timestamps=[10, 20, 30, 40],
        modules=[MODULE_A, MODULE_B, MODULE_A, MODULE_B],
        values=[1.0, 2.0, 3.0, 4.0],
    )
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)
    out_times_int64 = np.zeros(4, dtype=np.int64)
    out_times = np.zeros(4, dtype=dtypes.PLOT_TS_TYPE)
    out_values = np.zeros((4, 1), dtype=dtypes.PLOT_VAL_TYPE)
    window = TsWindowBundle(start_ts=TS_UNSPECIFIED, end_ts=TS_UNSPECIFIED)

    write_idx, _ = nb_extract_telemetry_segment_window_forward(
        bundle, MODULE_B, window, 1, out_times, out_times_int64, out_values, temp_floats, 0, PERMISSIVE_MASK, False, 0
    )

    assert write_idx == 2
    assert list(out_times_int64[:2]) == [20, 40]
    assert list(out_values[:2, 0]) == [2.0, 4.0]


def test_unspecified_bounds_extract_everything():
    bundle = make_telemetry_bundle(timestamps=[10, 20, 30], modules=[MODULE_A] * 3, values=[1.0, 2.0, 3.0])
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)
    out_times_int64 = np.zeros(3, dtype=np.int64)
    out_times = np.zeros(3, dtype=dtypes.PLOT_TS_TYPE)
    out_values = np.zeros((3, 1), dtype=dtypes.PLOT_VAL_TYPE)
    window = TsWindowBundle(start_ts=TS_UNSPECIFIED, end_ts=TS_UNSPECIFIED)

    write_idx, _ = nb_extract_telemetry_segment_window_forward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 0, PERMISSIVE_MASK, False, 0
    )

    assert write_idx == 3
    assert list(out_times_int64) == [10, 20, 30]


def test_forward_kernel_excludes_rows_below_effective_mask_threshold():
    """A row matching target_module but below the mask's level threshold must be excluded, same
    row-inclusion test ops/segments.py's segment_filter uses (levels[i] >= mask[modules[i]])."""
    bundle = make_telemetry_bundle(
        timestamps=[10, 20, 30, 40],
        modules=[MODULE_A] * 4,
        values=[1.0, 2.0, 3.0, 4.0],
        levels=[0, 3, 0, 3],  # rows at ts=20/40 are the only ones at/above a threshold of 3
    )
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)
    out_times_int64 = np.zeros(4, dtype=np.int64)
    out_times = np.zeros(4, dtype=dtypes.PLOT_TS_TYPE)
    out_values = np.zeros((4, 1), dtype=dtypes.PLOT_VAL_TYPE)
    window = TsWindowBundle(start_ts=TS_UNSPECIFIED, end_ts=TS_UNSPECIFIED)
    mask = np.zeros(10, dtype=dtypes.LEVEL_TYPE)
    mask[MODULE_A] = 3

    write_idx, _ = nb_extract_telemetry_segment_window_forward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 0, mask, False, 0
    )

    assert write_idx == 2
    assert list(out_times_int64[:2]) == [20, 40]
    assert list(out_values[:2, 0]) == [2.0, 4.0]


def test_backward_kernel_excludes_rows_below_effective_mask_threshold():
    bundle = make_telemetry_bundle(
        timestamps=[10, 20, 30, 40],
        modules=[MODULE_A] * 4,
        values=[1.0, 2.0, 3.0, 4.0],
        levels=[0, 3, 0, 3],
    )
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)
    out_times_int64 = np.zeros(4, dtype=np.int64)
    out_times = np.zeros(4, dtype=dtypes.PLOT_TS_TYPE)
    out_values = np.zeros((4, 1), dtype=dtypes.PLOT_VAL_TYPE)
    window = TsWindowBundle(start_ts=TS_UNSPECIFIED, end_ts=TS_UNSPECIFIED)
    mask = np.zeros(10, dtype=dtypes.LEVEL_TYPE)
    mask[MODULE_A] = 3

    write_idx, _ = nb_extract_telemetry_segment_window_backward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 4, mask, False, 0
    )

    assert write_idx == 2
    assert list(out_times_int64[2:4]) == [20, 40]
    assert list(out_values[2:4, 0]) == [2.0, 4.0]


def test_fetch_telemetry_window_permissive_default_mask_reproduces_unfiltered_output():
    """Regression guard for the "zero behavior change until a real filter is wired in" claim:
    not passing effective_mask at all must reproduce exactly what a caller got before this
    parameter existed."""
    array_pool, log_pool = make_real_log_pool()

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000
    values = [1.0, 2.0, 3.0]
    timestamps = [base + i * 10 for i in range(len(values))]
    with src:
        for ts, val in zip(timestamps, values):
            # A high level (well above any real threshold) must still pass through, since the
            # default mask is fully permissive (LogLevel.ALL == 0).
            src.insert_any(ts, ts, str(val).encode("ascii"), level=7, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=temp_floats,
        anchor_ts_ns=timestamps[1],
        before_span_ns=1000,
        after_span_ns=1000,
        before_cap=10,
        after_cap=10,
    ) as batch:
        got_values = list(batch.values[:, 0])

    assert got_values == values


def test_fetch_telemetry_window_end_to_end_against_a_real_pool():
    """Exercises fetch_telemetry_window through a real CircularLogPool, verifying the
    before/after halves land pre-concatenated and ascending, and that the anchor row itself
    (ts == anchor_ts_ns) is included exactly once, in the after half."""
    array_pool, log_pool = make_real_log_pool()

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000  # arbitrary epoch-ns anchor, ns-scale spacing
    values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0]
    timestamps = [base + i * 10 for i in range(len(values))]
    with src:
        for ts, val in zip(timestamps, values):
            src.insert_any(ts, ts, str(val).encode("ascii"), level=0, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    anchor_ts_ns = timestamps[3]  # value 4.0's own timestamp
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=temp_floats,
        anchor_ts_ns=anchor_ts_ns,
        before_span_ns=1000,
        after_span_ns=1000,
        before_cap=10,
        after_cap=10,
    ) as batch:
        got_values = list(batch.values[:, 0])
        got_times = list(batch.times_int64)

    assert got_values == values  # anchor row included exactly once, everything ascending
    assert got_times == timestamps
    assert got_times == sorted(got_times)


def test_fetch_telemetry_window_caps_each_side_independently():
    array_pool, log_pool = make_real_log_pool()

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000
    values = list(range(10))
    timestamps = [base + i * 10 for i in range(len(values))]
    with src:
        for ts, val in zip(timestamps, values):
            src.insert_any(ts, ts, str(float(val)).encode("ascii"), level=0, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    anchor_ts_ns = timestamps[5]
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=temp_floats,
        anchor_ts_ns=anchor_ts_ns,
        before_span_ns=10_000,
        after_span_ns=10_000,
        before_cap=2,
        after_cap=1,
    ) as batch:
        got_values = list(batch.values[:, 0])

    # 2 newest-before + 1 oldest-after (anchor itself), independently capped per side
    assert got_values == [3.0, 4.0, 5.0]


def test_fetch_telemetry_window_empty_when_anchor_predates_all_data():
    array_pool, log_pool = make_real_log_pool()

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000
    with src:
        src.insert_any(base, base, b"1.0", level=0, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=temp_floats,
        anchor_ts_ns=base - 1_000_000,  # long before any data exists
        before_span_ns=100,
        after_span_ns=100,
        before_cap=10,
        after_cap=10,
    ) as batch:
        assert batch.times.size == 0


def test_plus_one_fetches_the_nearest_sample_outside_a_sparse_window():
    """plus_one=True must find the single nearest sample just outside each boundary even when
    it's far outside the window (sparse data) - unlike a fixed-time padding on before_span_ns/
    after_span_ns, which would miss it if the gap between samples exceeds the padding. Samples
    are 1000ns apart; the window itself only spans +/-50ns around the anchor, so plain windowed
    extraction (plus_one=False) would return only the anchor row."""
    array_pool, log_pool = make_real_log_pool()

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    timestamps = [base + i * 1000 for i in range(len(values))]
    with src:
        for ts, val in zip(timestamps, values):
            src.insert_any(ts, ts, str(val).encode("ascii"), level=0, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    anchor_ts_ns = timestamps[2]  # value 3.0
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=temp_floats,
        anchor_ts_ns=anchor_ts_ns,
        before_span_ns=50,
        after_span_ns=50,
        before_cap=10,
        after_cap=10,
        plus_one=True,
    ) as batch:
        got_values = list(batch.values[:, 0])
        got_times = list(batch.times_int64)

    # Strict window would be just [3.0] (the anchor) - plus_one adds the nearest neighbor on
    # each side (2.0 before, 4.0 after), still ascending, no double-counting the anchor.
    assert got_values == [2.0, 3.0, 4.0]
    assert got_times == timestamps[1:4]
    assert got_times == sorted(got_times)


def test_plus_one_omits_an_edge_neighbor_that_does_not_exist():
    """plus_one=True must not fabricate data or crash when there's genuinely nothing past a
    boundary - here, nothing exists before the very first sample, so only the after-edge
    neighbor should be added."""
    array_pool, log_pool = make_real_log_pool()

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000
    values = [1.0, 2.0, 3.0]
    timestamps = [base + i * 1000 for i in range(len(values))]
    with src:
        for ts, val in zip(timestamps, values):
            src.insert_any(ts, ts, str(val).encode("ascii"), level=0, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    anchor_ts_ns = timestamps[0]  # value 1.0 - nothing exists before it
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=temp_floats,
        anchor_ts_ns=anchor_ts_ns,
        before_span_ns=50,
        after_span_ns=50,
        before_cap=10,
        after_cap=10,
        plus_one=True,
    ) as batch:
        got_values = list(batch.values[:, 0])

    # No before-edge neighbor exists (nothing predates the anchor) - just the anchor itself plus
    # the after-edge neighbor (2.0, the nearest sample past the 50ns after_span boundary).
    assert got_values == [1.0, 2.0]


def test_plus_one_defaults_to_false_preserving_the_original_strict_window():
    """Existing callers that don't pass plus_one must see byte-identical behavior to before this
    parameter existed - a regression guard for TestFetchTelemetryWindowCapsEachSideIndependently-
    style exact-count assertions elsewhere in this file."""
    array_pool, log_pool = make_real_log_pool()

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    timestamps = [base + i * 1000 for i in range(len(values))]
    with src:
        for ts, val in zip(timestamps, values):
            src.insert_any(ts, ts, str(val).encode("ascii"), level=0, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    anchor_ts_ns = timestamps[2]
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=temp_floats,
        anchor_ts_ns=anchor_ts_ns,
        before_span_ns=50,
        after_span_ns=50,
        before_cap=10,
        after_cap=10,
    ) as batch:
        got_values = list(batch.values[:, 0])

    assert got_values == [3.0]
