# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.id_registry import IDRegistry

from ..ops.formatting import format_log_batch
from ..utils.log_level import LogLevel
from ..utils.throughput import Speedometer
from . import dtypes
from .base_daemon import BaseDaemon
from .batch_queue import BatchQueue
from .configurable import configuration_factory, configuration_property, override_property
from .factory import BaseFactory
from .limits import CENTRAL_STORAGE_MAXLEN
from .numpy_batch_manager import PooledLogBatch
from .numpy_log import (
    CircularLogPool,
    fetch_telemetry_arrays,
    filter_segment,
    peek_channel_count,
)
from .types.formatting import FormattingConfig


@configuration_factory("central")
@override_property("enabled", default=True, hidden=True)
class BaseCentralStorage(BaseDaemon):
    def __init__(self):
        super().__init__()


class CentralFactory(BaseFactory[BaseCentralStorage]):
    pass


@CentralFactory.register("default")
@configuration_property(
    "maxlen",
    type="integer",
    default=CENTRAL_STORAGE_MAXLEN,
    description="Maximum number of log entries to keep in memory",
    ui_order=10,
)
@override_property(
    "logging", hidden=False, required=True, default={"enabled": True, "processor": {"type": "log_row"}}, ui_order=20
)
class CentralStorage(BaseCentralStorage):
    maxlen: int

    def __init__(self):
        super().__init__()
        self.name = "central"

        self.input_queue = BatchQueue()  # messages that have not yet been pushed to subscribers

        self.put = self.input_queue.put

        self.log_pool = None

        self.numba_needs_compile = True

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        if changed:
            buffer_size_mb = 1
            avg_msg_len = 32
            segments = buffer_size_mb * 1024 * 1024 // avg_msg_len
            if self.log_pool is None:
                self.log_pool = CircularLogPool(self.shared.array_pool, 10, segments, buffer_size_mb)

        self.numba_needs_compile = True
        return changed

    def warm_up_numba(self):
        self.logger.info("Pre-compiling Numba kernels (this may take a few seconds)...")

        time_ns = self.shared.time_ns

        dummy_registry = IDRegistry(self.shared.array_pool)

        buffer_size_mb = 1
        avg_msg_len = 32
        segments = buffer_size_mb * 1024 * 1024 // avg_msg_len
        dummy_log_pool = CircularLogPool(self.shared.array_pool, 10, segments, buffer_size_mb)

        def dummy_batch_acquire():

            pool = self.shared.array_pool
            pool_create = pool.create
            return pool_create(
                PooledLogBatch,
                256,
                4,
                has_levels=True,
                has_modules=True,
                has_devices=True,
            )

        dummy_floats_module = dummy_registry.resolve_module("tool.floats")

        dummy_module = dummy_registry.resolve_module("numba.warmup")

        # 1. Create a dummy input batch
        # We need a small buffer and at least one 'row' entry
        with dummy_batch_acquire() as dummy_in:
            # 2. Fill it with some dummy data that matches the expected formatf
            dummy_log_level = LogLevel.INFO.value
            for i in range(5):
                # This triggers the float parser: "ADC: 1.234, 5.678, 9.012"
                dummy_in.insert(
                    time_ns(),
                    b"ADC: -1.234, 5.678 ; 100  -0.001 ",
                    dummy_log_level,
                    dummy_floats_module.id,
                    dummy_floats_module.device.id,
                )
            # 2. Add Standard Text Data
            dummy_in.insert(
                time_ns(), b"System Boot Complete", dummy_log_level, dummy_module.id, dummy_module.device.id
            )

            # 3. Compile: Batch Append Logic
            dummy_log_pool.batch_append(dummy_in)

        # 4. Compile: Telemetry Extraction Logic
        # This triggers extract_telemetry_segment_numba and extract_floats_from_bytes

        num_channels = peek_channel_count(dummy_log_pool, dummy_floats_module.id, -1)
        print(f"[numba warmup] peeked {num_channels} channels for module {dummy_floats_module.id}")
        list(
            fetch_telemetry_arrays(
                dummy_log_pool, target_module_int=dummy_floats_module.id, start_seq=-1, num_channels=num_channels
            )
        )

        # 5. Compile: Channel Peeking Logic

        format_cfg = FormattingConfig(True, True, True, True)

        # 6. Compile: Log Filtering & Querying Logic
        # This triggers filter_segment
        # list(query_pool(self.shared.id_registry, self.log_pool, target_modules=[test_mod_id], start_seq=-1))
        tm_arr = np.array([dummy_floats_module.id, dummy_module.id], dtype=dtypes.ID_TYPE)
        for segment in dummy_log_pool.get_ordered_segments():
            indices = filter_segment(
                segment.bundle(),
                target_modules_arr=tm_arr,
            )

            # 7. Compile: Log Formatting (The heaviest kernel)
            # We need the Numba params from the ID registry to exercise this
            format_log_batch(
                indices,  # Dummy indices
                segment.bundle(),
                dummy_registry.bundle(),
                format_cfg,
                0,
            )

        dummy_log_pool.release_all()

        self.logger.info("Numba environment fully hot. All kernels cached.")

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        get = self.input_queue.get
        timeout_sec = 0.2

        time_ns = self.shared.time_ns

        # --- [START] WARM UP THE NUMBA KERNEL ---
        if self.numba_needs_compile:
            try:
                self.warm_up_numba()

            except Exception as e:
                self.logger.exception("Exception during kernel compilation", e)
            self.numba_needs_compile = False
        # --- [END] WARM UP ---

        speedometer = Speedometer(logger=self.logger.child("stats"))

        while not stop_is_set():
            # we need to push messages to subscribers here, but for now we just keep them in the log

            try:
                batch = get(timeout=timeout_sec)
                if batch is None:
                    continue

                with batch:
                    # print(f"[CENTRAL] Received batch of {len(entry)} entries.")
                    # print(f"[Central] batch={batch}")
                    # print(f"[central] batch={batch}")

                    speedometer.batch(batch)

                    self.log_pool.batch_append(batch)

                    self.distribute(batch)

            except Exception as e:
                self.logger.exception(f"fcked", e)
