# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.buffers import ModuleBuffer
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import (
    allocate_discovery_workspace,
    allocate_telemetry_workspace,
    fetch_telemetry_arrays,
    get_telemetry_anchor,
)
from blinkview.core.types.formatting import FormattingConfig
from blinkview.ops.formatting import estimate_log_batch_size, format_log_batch
from blinkview.ops.segments import filter_segment
from blinkview.ops.telemetry import minmax_downsample_inplace, slice_and_downsample_linear
from blinkview.utils.log_level import LogLevel


class NumbaWarmupHelper:
    """
    Encapsulates a dummy environment to trigger Numba JIT compilation
    for logging, telemetry, and registry kernels.
    """

    def __init__(self, array_pool, time_ns_func):
        self.array_pool = array_pool
        self.time_ns = time_ns_func

        from blinkview.core.id_registry import IDRegistry
        from blinkview.core.module_snapshot import LatestModuleValueTracker
        from blinkview.core.numpy_log import CircularLogPool

        # 1. Initialize dummy infrastructure
        self.registry = IDRegistry(self.array_pool)
        self.log_pool = CircularLogPool(self.array_pool, 4, 1)

        self.tracker = LatestModuleValueTracker(
            self.log_pool, self.registry.modules_table, self.array_pool, self.time_ns
        )

        # 2. Pre-resolve modules to ensure ID system kernels are warm
        self.warmup_mod = self.registry.resolve_module("numba.warmup")
        self.floats_mod = self.registry.resolve_module("tool.floats")

        # Default config for formatting kernels
        self.format_cfg = FormattingConfig(True, True, True, True)

    def get_pooled_log_batch(self, capacity=256):
        """Helper to acquire a pooled batch for data insertion."""
        return self.array_pool.create(
            PooledLogBatch,
            capacity,
            (capacity * 64) // 1024,
            has_levels=True,
            has_modules=True,
            has_devices=True,
        )

    def exercise_logging_kernels(self):
        """Triggers compilation for Batch Append and Log Filtering/Formatting."""
        log_level = LogLevel.INFO.value

        with self.get_pooled_log_batch(1024) as batch:
            # Trigger string/float parsing kernels
            #
            # create 1000 items
            for i in range(1000):
                time_now = self.time_ns()
                batch.insert(
                    time_now + i,
                    b"ADC: -1.234, 5.678 ; 100 -0.001",
                    log_level,
                    self.floats_mod.id,
                    self.floats_mod.device.id,
                )
            batch.insert(self.time_ns(), b"System Hot", log_level, self.warmup_mod.id, self.warmup_mod.device.id)
            print("Batch Append and Log Filtering/Formatting.")
            # Trigger: Batch Append Logic
            self.log_pool.batch_append(batch)

    def exercise_tracker_kernels(self):
        """Triggers compilation for Module Snapshot tracking and state copying."""
        # Trigger: _copy_snapshot_state and _update_master_arrays_reverse
        # This requires data to be in the pool (provided by exercise_logging_kernels)
        self.tracker.update()

        # Optionally exercise the string decoding/iterator logic
        with self.tracker.get_snapshot() as snap:
            for _ in snap:
                break

    def exercise_formatting_kernels(self):
        # Trigger: Filtering and Formatting Logic
        tm_arr = np.array([self.floats_mod.id, self.warmup_mod.id], dtype=dtypes.ID_TYPE)
        s_seq = dtypes.SEQ_TYPE(0)  # uint64
        t_lvl = dtypes.LEVEL_UNSPECIFIED  # uint8
        t_mod = dtypes.ID_UNSPECIFIED  # uint32
        t_dev = dtypes.ID_UNSPECIFIED  # uint32

        with self.log_pool.get_snapshot() as segments:
            for segment in segments:
                # print(
                #     f"warmup_filter_segment("
                #     f"start_seq={type(s_seq)}({s_seq}), "
                #     f"tm_arr={tm_arr.dtype}, "  # This is usually the culprit
                #     f"target_level={type(t_lvl)}({t_lvl}), "
                #     f"target_module={type(t_mod)}({t_mod}), "
                #     f"target_device={type(t_dev)}({t_dev}))"
                # )
                indices = filter_segment(
                    segment.bundle(),
                    target_modules_arr=tm_arr,
                    start_seq=s_seq,
                    target_level=t_lvl,
                    target_module=t_mod,
                    target_device=t_dev,
                )

                if indices.size > 0:
                    # Trigger: Size Estimation Kernel
                    req_bytes = estimate_log_batch_size(
                        indices, segment.bundle(), self.registry.bundle(), self.format_cfg
                    )
                    # Trigger: Formatting Kernel
                    with self.array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                        format_log_batch(
                            handle.array, indices, segment.bundle(), self.registry.bundle(), self.format_cfg, 0
                        )

    def exercise_telemetry_kernels(self):
        """Triggers compilation for telemetry discovery and extraction."""
        discovery_ws = allocate_discovery_workspace()

        # Trigger: Anchor discovery
        start_seq, num_channels = get_telemetry_anchor(self.log_pool, self.floats_mod.id, SEQ_NONE, discovery_ws)
        warmup_channels = num_channels if num_channels > 0 else 1
        temp_floats = allocate_telemetry_workspace(warmup_channels)
        module_buffer = ModuleBuffer(max_points=1024, num_channels=warmup_channels)

        if warmup_channels > 0:
            # Trigger: Extraction and Byte-to-Float kernels
            with fetch_telemetry_arrays(
                self.array_pool, self.log_pool, self.floats_mod.id, start_seq, warmup_channels, temp_floats
            ) as batch:
                module_buffer.update(batch)

        buf_bundle = module_buffer.bundle()

        t_now = self.time_ns() / 1e9
        t_min, t_max = t_now - 60, t_now
        num_bins = 200  # Small bin count is fine for warmup

        # Pre-allocate scratchpads for output
        # Note: size is usually num_bins * 2 to account for min-max pairs or extra points
        out_x = np.zeros(num_bins * 8, dtype=dtypes.PLOT_TS_TYPE)
        out_y = np.zeros(num_bins * 8, dtype=dtypes.PLOT_VAL_TYPE)

        # Exercise Main Plot Downsampler (Linear)
        # This now uses the clean, refactored signature
        _ = slice_and_downsample_linear(
            buf_bundle,
            col_idx=0,
            out_x=out_x,
            out_y=out_y,
            t_min_s=t_min,
            t_max_s=t_max,
            num_bins=num_bins,
        )

        _ = minmax_downsample_inplace(
            x_plot=module_buffer.x_data,
            x_ts=module_buffer.x_data_int64,
            y_2d=module_buffer.y_data,
            col_idx=0,
            start_idx=0,
            count=module_buffer.size,
            out_x=out_x,
            out_y=out_y,
            num_bins=num_bins,
        )

    def run_all(self):
        """Execute the full warmup suite."""
        try:
            print("exercise_logging_kernels...")
            self.exercise_logging_kernels()

            print("exercise_tracker_kernels...")
            self.exercise_tracker_kernels()

            print("exercise_formatting_kernels...")
            self.exercise_formatting_kernels()

            print("exercise_telemetry_kernels...")
            self.exercise_telemetry_kernels()
        finally:
            # Clean up dummy data
            self.log_pool.release_all()
