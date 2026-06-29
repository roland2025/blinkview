# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time

from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.parser import BaseParser, ParserFactory


@ParserFactory.register("module_gen")
@configuration_property(
    "modules_per_second",
    default=200,
    type="integer",
    description="Number of unique modules to generate and log to per second",
)
class ModuleGenParser(BaseParser):
    def __init__(self):
        super().__init__()

        self.module_counter = 0

    def run(self):
        # Localize built-ins and shared functions for high-speed hot paths
        _time_ns = self.shared.time_ns
        _len = len
        _distribute = self.distribute
        _sleep = time.sleep

        max_batch = self.max_batch
        modules_per_sec = self.modules_per_second

        # Calculate pacing intervals
        ns_per_module = int(1e9 / modules_per_sec) if modules_per_sec > 0 else 1e9

        device_identity: DeviceIdentity = self.local.device_id
        device_identity_id = device_identity.id
        _get_module = device_identity.get_module

        pool_create = self.shared.array_pool.create

        # 100ms flush interval in nanoseconds
        _FLUSH_INTERVAL_NS = 100_000_000
        last_flush_ns = _time_ns()

        def batch_acquire():
            return pool_create(
                PooledLogBatch, max_batch, max_batch * 128, has_levels=True, has_modules=True, has_devices=True
            )

        parsed_batch = batch_acquire()

        def flush():
            nonlocal parsed_batch, last_flush_ns
            if parsed_batch and parsed_batch.size > 0:
                with parsed_batch:
                    _distribute(parsed_batch)
                parsed_batch = batch_acquire()
            last_flush_ns = _time_ns()

        next_generation_ns = _time_ns()
        stop_is_set = self._stop_event.is_set

        while not stop_is_set():
            now_ns = _time_ns()

            # 100ms auto-flush check
            if parsed_batch.size > 0 and (now_ns - last_flush_ns) >= _FLUSH_INTERVAL_NS:
                flush()
                now_ns = _time_ns()

            # Pacing catching up loop
            while now_ns >= next_generation_ns and not stop_is_set():
                module_counter = self.module_counter = self.module_counter + 1

                # Create a fake dynamic module string and grab/register its ID
                mod_name = f"mod_{module_counter}"
                try:
                    target_mod_id = _get_module(mod_name).id
                except Exception:
                    # Fallback to a default module ID if registry fails dynamically
                    target_mod_id = 0

                # Generate the dynamic log line appending the counter
                # dynamic_log_line = f"status=ok emulation=true counter={module_counter}".encode()
                dynamic_log_line = f"status=ok emulation=true".encode()
                log_line_len = _len(dynamic_log_line)

                # Capacity management before writing (using the dynamic length)
                if (
                    parsed_batch.size >= parsed_batch.capacity
                    or parsed_batch.msg_cursor + log_line_len > parsed_batch.buffer_capacity()
                ):
                    flush()

                # Direct zero-allocation byte insertion
                parsed_batch.insert(
                    ts_ns=now_ns,
                    rx_ts_ns=now_ns,
                    msg_bytes=dynamic_log_line,
                    level=1,  # Info level default
                    module=target_mod_id,
                    device=device_identity_id,
                )

                if parsed_batch.size >= max_batch:
                    flush()

                # Increment schedule window
                next_generation_ns += ns_per_module

            # Prevent CPU pinning if we are ahead of schedule
            now_ns = _time_ns()
            if next_generation_ns > now_ns:
                sleep_secs = (next_generation_ns - now_ns) / 1e9

                # If we have data, do not sleep past the 100ms flush deadline
                if parsed_batch.size > 0:
                    time_until_flush_secs = (_FLUSH_INTERVAL_NS - (now_ns - last_flush_ns)) / 1e9
                    if time_until_flush_secs < 0:
                        time_until_flush_secs = 0
                    sleep_secs = min(sleep_secs, time_until_flush_secs)

                # Only sleep if it's a meaningful slice of time to avoid jitter
                if sleep_secs > 0.0005:
                    _sleep(sleep_secs)
