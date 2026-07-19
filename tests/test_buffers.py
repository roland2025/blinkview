# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.buffers import ReplayWindowBuffer
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import CircularLogPool, fetch_telemetry_window
from blinkview.core.types.log_batch import TelemetryBatch
from blinkview.ops.telemetry import (
    PLOT_INTERPOLATION_MODE_LINEAR,
    nb_slice_and_downsample,
)

MODULE_A = 1


def test_update_flat_overwrites_and_truncates_to_capacity():
    buf = ReplayWindowBuffer(capacity=3, num_channels=1)

    batch = TelemetryBatch(
        times=np.array([1.0, 2.0, 3.0, 4.0], dtype=dtypes.PLOT_TS_TYPE),
        times_int64=np.array([10, 20, 30, 40], dtype=np.int64),
        values=np.array([[1.0], [2.0], [3.0], [4.0]], dtype=dtypes.PLOT_VAL_TYPE),
        watermark=0,
    )
    updated = buf.update(batch)

    assert updated is True
    assert buf.size == 3  # truncated to capacity
    assert list(buf.x_data_int64[:3]) == [10, 20, 30]
    assert list(buf.y_data[:3, 0]) == [1.0, 2.0, 3.0]


def test_update_with_empty_batch_reports_no_update():
    buf = ReplayWindowBuffer(capacity=5, num_channels=1)
    empty = TelemetryBatch(
        times=np.zeros(0, dtype=dtypes.PLOT_TS_TYPE),
        times_int64=np.zeros(0, dtype=np.int64),
        values=np.zeros((0, 1), dtype=dtypes.PLOT_VAL_TYPE),
        watermark=0,
    )
    assert buf.update(empty) is False
    assert buf.size == 0


def test_bundle_contract_matches_data_start_zero():
    buf = ReplayWindowBuffer(capacity=5, num_channels=2)
    batch = TelemetryBatch(
        times=np.array([1.0, 2.0], dtype=dtypes.PLOT_TS_TYPE),
        times_int64=np.array([10, 20], dtype=np.int64),
        values=np.array([[1.0, 10.0], [2.0, 20.0]], dtype=dtypes.PLOT_VAL_TYPE),
        watermark=0,
    )
    buf.update(batch)
    bundle = buf.bundle()

    assert bundle.data_start == 0
    assert bundle.data_size == 2
    assert bundle.x_data is buf.x_data
    assert bundle.y_data is buf.y_data


def test_ensure_capacity_grows_and_resets_size_but_never_shrinks():
    buf = ReplayWindowBuffer(capacity=2, num_channels=1)
    batch = TelemetryBatch(
        times=np.array([1.0, 2.0], dtype=dtypes.PLOT_TS_TYPE),
        times_int64=np.array([10, 20], dtype=np.int64),
        values=np.array([[1.0], [2.0]], dtype=dtypes.PLOT_VAL_TYPE),
        watermark=0,
    )
    buf.update(batch)
    assert buf.size == 2

    buf.ensure_capacity(10)
    assert buf.capacity == 10
    assert buf.size == 0  # stale smaller-window data discarded
    assert buf.x_data.shape == (10,)

    buf.ensure_capacity(5)  # smaller than current - no-op
    assert buf.capacity == 10


def test_replay_window_buffer_renders_through_the_unmodified_downsample_kernel():
    """End-to-end: a real fetch_telemetry_window() result flows into ReplayWindowBuffer.update()
    and out through nb_slice_and_downsample unchanged - confirms the rendering path
    (_update_plots/_update_overview in plotter.py) needs zero changes to consume either buffer
    type, since both expose the identical TelemetryBufferBundle contract."""
    array_pool = NumpyArrayPool(max_bytes=4 * 1024 * 1024)
    log_pool = CircularLogPool(array_pool, max_pieces=4, final_buffer_bytes=64 * 1024)

    src = array_pool.create(PooledLogBatch, 20, 4096, has_levels=True, has_modules=True, has_devices=True)
    base = 1_000_000_000_000
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    timestamps = [base + i * 1_000_000_000 for i in range(len(values))]  # 1s apart
    with src:
        for ts, val in zip(timestamps, values):
            src.insert_any(ts, ts, str(val).encode("ascii"), level=0, module=MODULE_A, device=0)
        log_pool.batch_append(src)

    buf = ReplayWindowBuffer(capacity=10, num_channels=1)
    anchor_ts_ns = timestamps[2]

    with fetch_telemetry_window(
        array_pool,
        log_pool,
        MODULE_A,
        num_channels=1,
        temp_floats=buf.temp_floats,
        anchor_ts_ns=anchor_ts_ns,
        before_span_ns=10_000_000_000,
        after_span_ns=10_000_000_000,
        before_cap=5,
        after_cap=5,
    ) as batch:
        buf.update(batch)

    bundle = buf.bundle()
    assert bundle.data_size == 5

    out_x = np.zeros(32, dtype=dtypes.PLOT_TS_TYPE)
    out_y = np.zeros(32, dtype=dtypes.PLOT_VAL_TYPE)
    n, y_min, y_max = nb_slice_and_downsample(
        bundle, 0, out_x, out_y, timestamps[0] / 1e9, timestamps[-1] / 1e9, 16, PLOT_INTERPOLATION_MODE_LINEAR
    )

    assert n == 5
    assert list(out_y[:n]) == values
    assert y_min == 1.0
    assert y_max == 5.0
