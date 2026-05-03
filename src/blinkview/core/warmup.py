# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import numpy as np

from blinkview.core import dtypes
from blinkview.core.buffers import ModuleBuffer
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.logger import PrintLogger, SystemLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import (
    allocate_discovery_workspace,
    allocate_telemetry_workspace,
    fetch_telemetry_arrays,
    get_telemetry_anchor,
)
from blinkview.core.system_context import SystemContext
from blinkview.core.types.formatting import FormattingConfig
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import create_default_sync
from blinkview.ops.dispatch import process_batch_kernel
from blinkview.ops.formatting import estimate_log_batch_size, format_log_batch
from blinkview.ops.segments import filter_segment, nb_find_next_module_index, nb_find_next_module_match
from blinkview.ops.telemetry import minmax_downsample_inplace, slice_and_downsample_linear
from blinkview.ops.timestamps import nb_project_synced_ns
from blinkview.parsers.frame_decoders import FrameDecoder
from blinkview.parsers.frame_parsers import GenericFrameParser
from blinkview.parsers.state import FrameState
from blinkview.storage.file_logger import BinaryBatchProcessor, LogRowBatchProcessor
from blinkview.utils.log_level import LogLevel


class NumbaWarmupHelper:
    """
    Encapsulates a dummy environment to trigger Numba JIT compilation
    for logging, telemetry, and registry kernels.
    """

    def __init__(self, shared: SystemContext):
        self.array_pool = shared.array_pool
        self.time_ns = shared.time_ns

        self.logger = PrintLogger("warmup")

        from blinkview.core.id_registry import IDRegistry
        from blinkview.core.module_snapshot import LatestModuleValueTracker
        from blinkview.core.numpy_log import CircularLogPool

        # 1. Initialize dummy infrastructure
        self.registry = IDRegistry(self.array_pool)
        self.log_pool = CircularLogPool(self.array_pool, 4, 1024 * 16)

        self.tracker = LatestModuleValueTracker(
            self.log_pool, self.registry.modules_table, self.array_pool, self.time_ns
        )

        # 2. Pre-resolve modules to ensure ID system kernels are warm
        self.warmup_mod = self.registry.resolve_module("numba.warmup")
        self.floats_mod = self.registry.resolve_module("tool.floats")

        # Default config for formatting kernels
        self.format_cfg = FormattingConfig(True, True, True, True)

        self.shared = SystemContext(
            time_ns=self.time_ns,
            registry=None,
            id_registry=self.registry,
            factories=shared.factories,
            tasks=shared.tasks,
            settings=shared.settings,
            pool=shared.pool,
            array_pool=shared.array_pool,
        )

        self._frame_codec: FrameDecoder = None
        self._frame_parser: GenericFrameParser = None

    def get_pooled_log_batch(self, capacity=256):
        """Helper to acquire a pooled batch for data insertion."""
        return self.array_pool.create(
            PooledLogBatch,
            capacity,
            capacity * 64,
            has_levels=True,
            has_modules=True,
            has_devices=True,
        )

    def exercise_logging_kernels(self):
        """Triggers compilation for Batch Append and Log Filtering/Formatting."""
        print("exercise_logging_kernels...")
        log_level = LogLevel.INFO.value

        with self.get_pooled_log_batch(1024) as batch:
            # Trigger string/float parsing kernels
            #
            # create 1000 items
            for i in range(1000):
                time_now = self.time_ns()
                batch.insert(
                    time_now + i,
                    time_now + i,
                    b"ADC: -1.234, 5.678 ; 100 -0.001",
                    log_level,
                    self.floats_mod.id,
                    self.floats_mod.device.id,
                )
            batch.insert(
                self.time_ns(), self.time_ns(), b"System Hot", log_level, self.warmup_mod.id, self.warmup_mod.device.id
            )
            print("Batch Append and Log Filtering/Formatting.")
            # Trigger: Batch Append Logic
            self.log_pool.batch_append(batch)

    def exercise_tracker_kernels(self):
        """Triggers compilation for Module Snapshot tracking and state copying."""

        print("exercise_tracker_kernels...")

        # Trigger: _copy_snapshot_state and _update_master_arrays_reverse
        # This requires data to be in the pool (provided by exercise_logging_kernels)
        self.tracker.update()

        # Optionally exercise the string decoding/iterator logic
        with self.tracker.get_snapshot() as snap:
            for _ in snap:
                break

    def exercise_formatting_kernels(self):
        # Trigger: Filtering and Formatting Logic

        print("exercise_formatting_kernels...")

        tm_arr = np.array([self.floats_mod.id, self.warmup_mod.id], dtype=dtypes.ID_TYPE)
        s_seq = dtypes.SEQ_TYPE(0)  # uint64
        t_lvl = dtypes.LEVEL_UNSPECIFIED  # uint8
        t_dev = dtypes.ID_UNSPECIFIED  # uint32

        filter_mask = np.full(1, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)
        filter_enabled = False

        with self.log_pool.get_snapshot() as segments, self.log_pool.acquire_indices_buffer() as indices:
            for segment in segments:
                # print(
                #     f"warmup_filter_segment("
                #     f"bundle={type(segment.bundle)}, "
                #     f"tm_arr={tm_arr.dtype}, "
                #     f"indices={type(indices.array)}, "
                #     f"filter_mask={type(filter_mask)}, "
                #     f"filter_enabled={type(filter_enabled)}, "
                #     f"s_seq={type(s_seq)}, "
                #     f"t_lvl={type(t_lvl)}, "
                #     f"t_dev={type(t_dev)}, "
                # )
                match_count = filter_segment(
                    segment.bundle,
                    target_modules_arr=tm_arr,
                    out_indices=indices.array,
                    module_filter_mask=filter_mask,
                    filter_enabled=filter_enabled,
                    start_seq=s_seq,
                    target_level=dtypes.LEVEL_TYPE(t_lvl),
                    target_device=dtypes.ID_TYPE(t_dev),
                )

                if match_count > 0:
                    # Trigger: Size Estimation Kernel
                    req_bytes = estimate_log_batch_size(
                        indices.array, match_count, segment.bundle, self.registry.bundle(), self.format_cfg
                    )
                    # Trigger: Formatting Kernel
                    with self.array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                        format_log_batch(
                            handle.array,
                            indices.array,
                            match_count,
                            segment.bundle,
                            self.registry.bundle(),
                            self.format_cfg,
                            0,
                        )

    def exercise_telemetry_kernels(self):
        """Triggers compilation for telemetry discovery and extraction."""

        print("exercise_telemetry_kernels...")

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

    def exercise_timesync_kernels(self):
        """Triggers compilation for the TimeSyncEngine, Projection, and String Parsing."""
        from blinkview.core.types.parsing import SyncState

        from ..core.time_sync_engine import TimeSyncEngine

        print("exercise_timesync_kernels...")

        # 1. Setup Mock State
        now_ns = self.time_ns()
        # Initialize a real SyncState object
        sync_state = create_default_sync(now_ns, start_enabled=True)
        engine = TimeSyncEngine(sync_state)

        # 2. Exercise: nb_sync_kernel (via Engine.feed)
        # We simulate a few pings to exercise the statistical window and drift math
        mock_pc_tx = now_ns
        mock_phone_mono = 1_000_000_000  # 1 second uptime
        mock_pc_rx = now_ns + 30_000_000  # 30ms RTT

        # Ping 1: Initial anchor
        engine.feed(mock_pc_tx, mock_phone_mono, mock_phone_mono, mock_pc_rx)

        # Ping 2: Jitter check and drift accumulation
        engine.feed(
            mock_pc_tx + 1_000_000_000,
            mock_phone_mono + 1_000_000_000,
            mock_phone_mono + 1_000_000_000,
            mock_pc_rx + 1_000_000_000,
        )

        # Trigger: soft_reset (exercises scalar clearing)
        engine.soft_reset()

        # 4. Exercise: nb_project_synced_ns
        # Tests the linear projection and 64-bit overflow safety guards

        with self.log_pool.get_snapshot() as segments:
            for segment in segments:
                b = segment.bundle
                nb_find_next_module_match(b, dtypes.ID_TYPE(self.warmup_mod.id), SEQ_NONE)

                nb_find_next_module_index(b, dtypes.ID_TYPE(self.warmup_mod.id), dtypes.SEQ_TYPE(SEQ_NONE))
                break

        # print(f"DEBUG: {nb_find_next_module_match.__name__} signatures:")
        # for sig in nb_find_next_module_match.signatures:
        #     print(f"  - {sig}")

        print("TimeSync kernels warmed.")

    def exercise_parsing_pipeline_config(self, frame_config, parser_config):
        print("exercise_parsing_pipeline...")
        try:
            factory_build = self.shared.factories.build

            device_id = self.warmup_mod.device

            time_ns = self.shared.time_ns

            pool = self.shared.array_pool
            pool_create = pool.create

            frame_ctx = SimpleNamespace(
                get_logger=self.logger.child_creator("decoder"),
                device_id=device_id,
            )
            self._frame_codec = factory_build(
                "frame_decoder", frame_config, system_ctx=self.shared, local_ctx=frame_ctx
            )

            parser_ctx = SimpleNamespace(
                get_logger=self.logger.child_creator("parser"),
                device_id=device_id,
                sync_state=create_default_sync(self.shared.time_ns()),
            )
            self._frame_parser = factory_build(
                "frame_parser", parser_config, system_ctx=self.shared, local_ctx=parser_ctx
            )

            codec = self._frame_codec

            f_config = codec.bundle()
            frame_state = FrameState(pool, codec.frame_length_maximum)
            f_state = frame_state.bundle

            o_config = OutputConfig(compact_buffer=True)

            parser = self._frame_parser
            parser_bundle = parser.bundle()

            def batch_acquire():
                return pool_create(
                    PooledLogBatch,
                    4096,
                    codec.frame_length_maximum,
                    has_levels=True,
                    has_modules=True,
                    has_devices=True,
                )

            def batch_acquire_input():
                return pool_create(PooledLogBatch, 128, 4)  # 128 items, 4 kB buffer for the dummy batch

            # 1. Create a dummy input batch
            # We need a small buffer and at least one 'row' entry
            with (
                batch_acquire_input() as dummy_in,
                batch_acquire() as dummy_out,
            ):
                dummy_in: PooledLogBatch
                dummy_in.insert(time_ns(), time_ns(), b"       0.00 V     -0.010 mA \n")
                dummy_in.insert(time_ns(), time_ns(), b"N1 main reg input          0.00 V     -0.010 mA \n")
                dummy_in.insert(time_ns(), time_ns(), b"N2 ASI switch")
                dummy_in.insert(time_ns(), time_ns(), b"              0.00 V")
                dummy_in.insert(time_ns(), time_ns(), b"     -0.014 mA \n")
                dummy_in.insert(time_ns(), time_ns(), b"N3 charger input        ")
                dummy_in.append(b" ")

                dummy_in.append_any(b" ")
                # 3. Trigger the kernel
                # This will block the thread while LLVM does its work
                _ = process_batch_kernel(
                    f_config,
                    f_state,
                    dummy_in.bundle,
                    parser_bundle,
                    o_config,
                    dummy_out.bundle,
                )

                bin_processor = BinaryBatchProcessor()
                bin_processor.shared = self.shared
                bin_processor.process(dummy_in)

                txt_processor = LogRowBatchProcessor()
                txt_processor.shared = self.shared
                txt_processor.process(dummy_out)
        finally:
            self._frame_codec = None
            self._frame_parser = None

    def exercise_parsing_pipeline(self):
        print("exercise_parsing_pipeline...")

        steps = []
        all_steps = [
            {"type": "skip_words", "count": 1},
            {"type": "log_level_default"},
            {"type": "module_name_normalizer", "max_depth": 8, "max_length": 64},
            {"type": "skip_words", "count": 1},
            {"type": "skip_words", "count": 1},
        ]

        for step in all_steps:
            decoder_config = {
                "type": "line_decoder",
                "frame_errors_hidden": False,
                "filter_trim_r": True,
                "filter_printable": True,
                "filter_ansi": True,
                "frame_length_dynamic": True,
                "frame_length": 0,
                "frame_length_minimum": 8,
                "frame_length_maximum": 1024,
            }

            parser_config = {
                "type": "default",
                "parser_errors_hidden": False,
                "steps": steps,
                "filter_squash_spaces": False,
            }

            self.exercise_parsing_pipeline_config(decoder_config, parser_config)
            steps.append(step)

    def run_all(self):
        """Execute the full warmup suite."""
        try:
            self.exercise_parsing_pipeline()

            self.exercise_logging_kernels()

            self.exercise_tracker_kernels()

            self.exercise_formatting_kernels()

            self.exercise_telemetry_kernels()

            self.exercise_timesync_kernels()
        finally:
            # Clean up dummy data
            self.log_pool.release_all()
