# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter

from blinkview.core.configurable import override_property
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.log_row import LogRow
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.parser import BaseParser, ParserFactory


@ParserFactory.register("key_value_equal")
@override_property(
    "sources_",
    items={"type": "string", "_reference": "/targets"},
)
class KeyValueEqualParser(BaseParser):
    def __init__(self):
        super().__init__()

    def run(self):
        # Localize built-ins and shared functions for high-speed hot paths
        _EQ_INT = 61  # ASCII decimal for '='
        _EQ = b"="
        _SPACE = b" "
        _STRIP = b",;"

        _time_ns = self.shared.time_ns
        _len = len
        _getattr = getattr
        _get = self.input_queue.get
        _distribute = self.distribute
        _module_from_int = self.shared.id_registry.module_from_int

        max_batch = self.max_batch
        max_timeout = self.delay / 1000.0
        max_timeout_ns = int(max_timeout * 1e9)  #

        device_identity: DeviceIdentity = self.local.device_id
        device_identity_id = device_identity.id
        system_identity_id = self.shared.id_registry.get_device("SYSTEM").id
        _get_module = device_identity.get_module

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            #
            return pool_create(
                PooledLogBatch, max_batch, max_batch * 128, has_levels=True, has_modules=True, has_devices=True
            )

        parsed_batch = batch_acquire()
        last_flush_ns = _time_ns()
        module_cache = {}

        def flush():
            nonlocal parsed_batch, last_flush_ns
            if parsed_batch and parsed_batch.size > 0:
                with parsed_batch:
                    _distribute(parsed_batch)  #
                parsed_batch = batch_acquire()
                last_flush_ns = _time_ns()

        stop_is_set = self._stop_event.is_set
        while not stop_is_set():
            now_ns = _time_ns()

            # --- Dynamic Timeout Logic ---
            # If buffer is empty, wait up to 120s for new data.
            # If data is pending, wait only until the configured flush deadline.
            if parsed_batch.size > 0:
                elapsed_ns = now_ns - last_flush_ns
                current_timeout = max(0.0, max_timeout - (elapsed_ns / 1e9))
            else:
                current_timeout = 120.0

            batch = _get(timeout=current_timeout)

            if not batch:
                # Flush pending dribbles if we timed out
                if parsed_batch.size > 0:
                    flush()
                continue

            with batch:
                for ts_ns, msg_view, level, module_id, device_id, *_ in batch:
                    if device_id == device_identity_id or device_id == system_identity_id:
                        continue

                    # THE ZERO-COPY FILTER: Scan the NumPy view for the integer 61
                    # This drops non-matching logs without allocating a single byte of memory
                    if _EQ_INT not in msg_view:
                        continue

                    # We found an '=', NOW we extract the bytes to use the fast .split() methods
                    msg_bytes = msg_view.tobytes()

                    # Byte-level splitting using localized constants
                    for key_value in msg_bytes.split(_SPACE):
                        if _EQ in key_value:
                            if _len(parts := key_value.split(_EQ, 1)) == 2:
                                k_bytes, v_bytes = parts

                                # Strip localized trailing characters
                                if k_bytes and (v_bytes := v_bytes.rstrip(_STRIP)):
                                    try:
                                        cache_key = (module_id, k_bytes)
                                        if cache_key not in module_cache:
                                            # Only decode the Key for registry lookup
                                            parent_mod = _module_from_int(module_id)
                                            key_str = k_bytes.decode("ascii", errors="ignore")
                                            module_cache[cache_key] = _get_module(f"{parent_mod.name}.{key_str}").id

                                        target_mod_id = module_cache[cache_key]

                                        # Capacity management
                                        if (
                                            parsed_batch.size >= parsed_batch.capacity
                                            or parsed_batch.msg_cursor + _len(v_bytes) > parsed_batch.buffer_capacity()
                                        ):
                                            flush()

                                        # Direct byte insertion
                                        parsed_batch.insert(
                                            ts_ns=ts_ns,
                                            rx_ts_ns=ts_ns,
                                            msg_bytes=v_bytes,
                                            level=level if level is not None else 0,
                                            module=target_mod_id,
                                            device=device_identity_id,
                                        )
                                    except Exception:
                                        pass

                    if parsed_batch.size >= max_batch:
                        flush()

            # Final age check after processing the batch
            if parsed_batch.size > 0 and (_time_ns() - last_flush_ns >= max_timeout_ns):
                flush()

        # Final cleanup on thread exit
        if parsed_batch:
            if parsed_batch.size > 0:
                flush()
            else:
                parsed_batch.release()
