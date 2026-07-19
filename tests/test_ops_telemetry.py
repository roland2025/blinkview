# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import TS_UNSPECIFIED
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import CircularLogPool, fetch_telemetry_window
from blinkview.core.types.telemetry import TsWindowBundle
from blinkview.ops.telemetry import (
    nb_extract_telemetry_segment_window_backward,
    nb_extract_telemetry_segment_window_forward,
)
from tests.test_ops_segments import make_bundle

MODULE_A = 1
MODULE_B = 2


def make_telemetry_bundle(timestamps, modules, values):
    """Builds a LogBundle whose message bytes are telemetry-parseable floats (one value per
    row here - nb_extract_floats_from_bytes handles multi-channel too, but one channel is
    enough to exercise the window-bounding logic these kernels add)."""
    messages = [str(v) for v in values]
    return make_bundle(
        timestamps=timestamps,
        rx_timestamps=timestamps,
        devices=[0] * len(timestamps),
        levels=[0] * len(timestamps),
        modules=modules,
        sequences=list(range(1, len(timestamps) + 1)),
        messages=messages,
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
    write_idx = nb_extract_telemetry_segment_window_backward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 5
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
    write_idx = nb_extract_telemetry_segment_window_forward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 0
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

    write_idx = nb_extract_telemetry_segment_window_forward(
        bundle, MODULE_B, window, 1, out_times, out_times_int64, out_values, temp_floats, 0
    )

    assert write_idx == 2
    assert list(out_times_int64[:2]) == [20, 40]
    assert list(out_values[:2, 0]) == [2.0, 4.0]


def test_unspecified_bounds_extract_everything():
    bundle = make_telemetry_bundle(
        timestamps=[10, 20, 30], modules=[MODULE_A] * 3, values=[1.0, 2.0, 3.0]
    )
    temp_floats = np.empty(1, dtype=dtypes.PLOT_VAL_TYPE)
    out_times_int64 = np.zeros(3, dtype=np.int64)
    out_times = np.zeros(3, dtype=dtypes.PLOT_TS_TYPE)
    out_values = np.zeros((3, 1), dtype=dtypes.PLOT_VAL_TYPE)
    window = TsWindowBundle(start_ts=TS_UNSPECIFIED, end_ts=TS_UNSPECIFIED)

    write_idx = nb_extract_telemetry_segment_window_forward(
        bundle, MODULE_A, window, 1, out_times, out_times_int64, out_values, temp_floats, 0
    )

    assert write_idx == 3
    assert list(out_times_int64) == [10, 20, 30]


def test_fetch_telemetry_window_end_to_end_against_a_real_pool():
    """Exercises fetch_telemetry_window through a real CircularLogPool, verifying the
    before/after halves land pre-concatenated and ascending, and that the anchor row itself
    (ts == anchor_ts_ns) is included exactly once, in the after half."""
    array_pool = NumpyArrayPool(max_bytes=4 * 1024 * 1024)
    log_pool = CircularLogPool(array_pool, max_pieces=4, final_buffer_bytes=64 * 1024)

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
    array_pool = NumpyArrayPool(max_bytes=4 * 1024 * 1024)
    log_pool = CircularLogPool(array_pool, max_pieces=4, final_buffer_bytes=64 * 1024)

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
    array_pool = NumpyArrayPool(max_bytes=4 * 1024 * 1024)
    log_pool = CircularLogPool(array_pool, max_pieces=4, final_buffer_bytes=64 * 1024)

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
        assert batch.values.size == 0
