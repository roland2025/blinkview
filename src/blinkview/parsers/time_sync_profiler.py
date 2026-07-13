# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep

import numpy as np

from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ops.segments import nb_find_next_module_index
from blinkview.ops.strings import nb_find_and_parse_int
from blinkview.parsers.parser import BaseParser, ParserFactory


def nb_bundle_parse_states(bundle, index, key_seq, key_target, key_pin):
    """
    Extracts seq, target_state, and pin_state in a single LLVM pass.
    """
    offset = bundle.offsets[index]
    length = bundle.lengths[index]
    buffer = bundle.buffer

    has_seq, seq = nb_find_and_parse_int(buffer, offset, length, key_seq)

    has_target, target = False, 0
    has_pin, pin = False, 0

    if has_seq:
        has_target, target = nb_find_and_parse_int(buffer, offset, length, key_target)
        # Optimization: if it's a target_state payload, it's unlikely to also have pin_state
        if not has_target:
            has_pin, pin = nb_find_and_parse_int(buffer, offset, length, key_pin)

    return has_seq, seq, has_target, target, has_pin, pin


@ParserFactory.register("time_sync_profiler")
@override_property(
    "sources_",
    items={"type": "string", "_reference": "/pipelines"},
)
@configuration_property("server", type="string", required=True, _reference="/pipelines", default="")
@configuration_property(
    "time_source_boot",
    type="boolean",
    required=True,
    ui_order=40,
)
class TimeSyncProfiler(BaseParser):
    def __init__(self):
        super().__init__()

    def run(self):
        # --- Localize Framework Plumbing ---
        logger = self.logger
        _get = self.input_queue.get
        _time_ns = self.shared.time_ns
        _distribute = self.distribute
        registry = self.shared.registry
        pool_create = self.shared.array_pool.create

        # Base timeout configuration (strictly capped at 50ms)
        requested_delay = getattr(self, "delay", 50)  # Default to 50ms if unset
        actual_delay = min(requested_delay, 50)  # Enforce the 50ms ceiling

        max_timeout = actual_delay / 1000.0
        max_timeout_ns = int(max_timeout * 1e9)

        # --- Device Identification ---
        pipeline_server = registry.get_reference_target(self.server)
        server_device_id = pipeline_server.local.device_id
        server_device_id_int = server_device_id.id

        logger.info(f"server: {server_device_id} ({server_device_id_int})")

        pipeline_sources = [registry.get_reference_target(src) for src in self.sources_]

        if pipeline_server in pipeline_sources:
            pipeline_sources.remove(pipeline_server)
        source_devices = [src.local.device_id for src in pipeline_sources]

        source_device_ids = {dev.id for dev in source_devices}
        logger.info(f"sources: {source_devices}")

        consumer_loggers = {dev.id: logger.child(str(dev.name)) for dev in source_devices}

        # --- Cache Modules ---
        main_module_ids = {}
        for dev in [server_device_id] + source_devices:
            try:
                main_module_ids[dev.id] = dev.get_module("main").id
            except Exception as e:
                logger.warning(f"Could not cache 'main' module for device {dev.id}: {e}")

        local_device_id_int = self.local.device_id.id

        # The module ID to tag our outgoing metric batches with
        out_module_ids = {}
        for dev in source_devices:
            consumer_name = str(dev.name)
            try:
                out_module_ids[dev.id] = self.local.device_id.get_module(consumer_name).id
            except Exception as e:
                logger.warning(f"Could not cache output module '{consumer_name}' on local device: {e}")

        def get_pipeline_source(_pipe):
            try:
                with _pipe._subscribers_lock:
                    if _pipe._subscriptions:
                        return _pipe._subscriptions[0]
            except Exception:
                pass
            return None

        # --- Wait for Devices to Connect ---
        connection_timeout = 5.0
        poll_interval = 0.1
        start_time = _time_ns()
        timeout_ns = int(connection_timeout * 1e9)

        logger.info("Waiting up to 5 seconds for all pipeline devices to connect...")

        while True:
            # 1. Evaluate Server Connection
            server_connected = True
            if hasattr(pipeline_server, "is_connected"):
                server_connected = pipeline_server.is_connected()
            else:
                # Fall back to checking the target's underlying source subscription
                resolved_server = get_pipeline_source(pipeline_server)
                if resolved_server and hasattr(resolved_server, "is_connected"):
                    server_connected = resolved_server.is_connected()

            # 2. Evaluate Sources Connections
            sources_connected = True
            for src in pipeline_sources:
                src_connected = True
                if hasattr(src, "is_connected"):
                    src_connected = src.is_connected()
                else:
                    # Try to extract the true underlying subscriber from the pipeline
                    resolved_src = get_pipeline_source(src)
                    if resolved_src and hasattr(resolved_src, "is_connected"):
                        src_connected = resolved_src.is_connected()

                if not src_connected:
                    sources_connected = False
                    break  # Short-circuit current iteration loop if any source is down

            # 3. Check loop breaking conditions
            if server_connected and sources_connected:
                logger.info("All pipeline devices successfully connected.")
                break

            if (_time_ns() - start_time) > timeout_ns:
                logger.warning("Timed out waiting for some devices to connect. Proceeding anyway.")
                break

            sleep(poll_interval)

        # --- Helpers ---
        def send_cmd(pipeline, cmd_str):
            try:
                pipe_src = get_pipeline_source(pipeline)
                if pipe_src is not None:
                    pipe_src.send_data(f"{cmd_str}\n")
                    logger.debug(f"Sent command to {pipeline.local.device_id}: {cmd_str}  (({pipe_src}))")
            except Exception as e:
                logger.error(f"Failed to send command '{cmd_str}' to {pipeline.local.device_id}: {e}")

        # --- 1. Initialization Commands ---
        send_cmd(pipeline_server, "sync source")
        send_cmd(pipeline_server, "sync seq 0")

        for pipeline in pipeline_sources:
            send_cmd(pipeline, "sync consumer")
            send_cmd(pipeline, "sync seq 0")

        sleep(0.1)

        # --- Output Batch Management ---
        def batch_acquire():
            return pool_create(
                PooledLogBatch,
                1024,  # capacity
                4096,  # buffer limit
                has_levels=True,
                has_modules=True,
                has_devices=True,
            )

        batch_out = batch_acquire()
        batch_out_time = _time_ns()
        batch_out_capacity = batch_out.capacity
        batch_out_buf_limit = batch_out.buffer_capacity() * 0.9

        def flush():
            nonlocal batch_out, batch_out_time, batch_out_capacity, batch_out_buf_limit
            if batch_out and batch_out.size > 0:
                with batch_out:
                    _distribute(batch_out)
            batch_out = batch_acquire()
            batch_out_time = _time_ns()
            batch_out_capacity = batch_out.capacity
            batch_out_buf_limit = batch_out.buffer_capacity() * 0.9

        # --- State Tracking ---
        server_seq_times = {}
        consumer_seq_times = {dev_id: {} for dev_id in source_device_ids}
        last_cleanup_time = _time_ns()

        stop_is_set = self._stop_event.is_set

        # Pre-allocate Byte Keys for Numba Zero-Allocation Lookups
        KEY_SEQ = np.frombuffer(b"seq", dtype=np.uint8)
        KEY_TARGET_STATE = np.frombuffer(b"target_state", dtype=np.uint8)
        KEY_PIN_STATE = np.frombuffer(b"pin_state", dtype=np.uint8)

        # --- Hot Loop ---
        while not stop_is_set():
            now = _time_ns()

            # --- Dynamic Polling Timeout ---
            if batch_out.size > 0:
                elapsed_ns = now - batch_out_time
                current_timeout = max(0.0, max_timeout - (elapsed_ns / 1e9))
            else:
                current_timeout = max_timeout

            # --- 2. Process Incoming Rx Batches ---
            batch_in: PooledLogBatch = _get(timeout=current_timeout)

            if batch_in is None:
                if batch_out.size > 0:
                    flush()
                continue

            with batch_in:
                b = batch_in.bundle

                if batch_out.size == 0:
                    batch_out_time = _time_ns()

                device_id_int = batch_in.get_device()

                if device_id_int not in main_module_ids:
                    continue

                main_module_id = main_module_ids[device_id_int]
                cursor = 0

                # Memoryviews for fast 1D array indexing in pure Python
                timestamps = memoryview(b.timestamps)
                rx_timestamps = memoryview(b.rx_timestamps)
                levels = memoryview(b.levels)

                try:
                    while True:
                        found, idx = nb_find_next_module_index(
                            b, dtypes.ID_TYPE(main_module_id), dtypes.SEQ_TYPE(cursor)
                        )

                        if not found:
                            break

                        # Update: Capture target_val and pin_val instead of discarding them
                        has_seq, seq, has_target, target_val, has_pin, pin_val = nb_bundle_parse_states(
                            b, idx, KEY_SEQ, KEY_TARGET_STATE, KEY_PIN_STATE
                        )

                        if has_seq:
                            ts = timestamps[idx]
                            rx_ts = rx_timestamps[idx]
                            orig_level = levels[idx]

                            # --- Handle Server Output ---
                            if device_id_int == server_device_id_int and has_target:
                                # Store both timestamp and the target state value
                                server_seq_times[seq] = (ts, target_val)

                                if len(server_seq_times) > 50:
                                    del server_seq_times[next(iter(server_seq_times))]

                                # Safely cross-match if a consumer packet arrived earlier
                                for dev_id in source_device_ids:
                                    if seq in consumer_seq_times[dev_id]:
                                        t_target, stored_target_val = server_seq_times[seq]
                                        t_pin, stored_pin_val = consumer_seq_times[dev_id][seq]

                                        del consumer_seq_times[dev_id][seq]

                                        # Desync Check: Ensure the target state matches the pin state
                                        if stored_target_val != stored_pin_val:
                                            logger.warning(
                                                f"Desync detected on device {dev_id} at seq {seq}! Expected target state {stored_target_val}, got pin state {stored_pin_val}."
                                            )
                                            continue

                                        diff_ns = t_pin - t_target
                                        diff_ms = diff_ns / 1_000_000.0

                                        # --- Distribute Result Downstream ---
                                        if dev_id in out_module_ids:
                                            try:
                                                batch_out.insert(
                                                    ts_ns=t_target,
                                                    rx_ts_ns=rx_ts,
                                                    msg_bytes=f"{diff_ms:.3f}".encode("ascii"),
                                                    level=orig_level,
                                                    module=out_module_ids[dev_id],
                                                    device=local_device_id_int,
                                                )
                                            except Exception:
                                                pass

                            # --- Handle Consumer Input & Diff Calculation ---
                            elif device_id_int in source_device_ids and has_pin:
                                # Safely isolate by ensuring device_id_int belongs to the consumers
                                if seq not in consumer_seq_times[device_id_int]:
                                    # Store both timestamp and the pin state value
                                    consumer_seq_times[device_id_int][seq] = (ts, pin_val)

                                if len(consumer_seq_times[device_id_int]) > 50:
                                    del consumer_seq_times[device_id_int][next(iter(consumer_seq_times[device_id_int]))]

                                if seq in server_seq_times:
                                    t_target, stored_target_val = server_seq_times[seq]
                                    t_pin, stored_pin_val = consumer_seq_times[device_id_int][seq]

                                    del consumer_seq_times[device_id_int][seq]

                                    # Desync Check: Ensure the target state matches the pin state
                                    if stored_target_val != stored_pin_val:
                                        logger.warning(
                                            f"Desync detected on device {device_id_int} at seq {seq}! Expected target state {stored_target_val}, got pin state {stored_pin_val}."
                                        )
                                        continue

                                    diff_ns = t_pin - t_target
                                    diff_ms = diff_ns / 1_000_000.0

                                    # --- Distribute Result Downstream ---
                                    if device_id_int in out_module_ids:
                                        try:
                                            batch_out.insert(
                                                ts_ns=t_target,
                                                rx_ts_ns=rx_ts,
                                                msg_bytes=f"{diff_ms:.3f}".encode("ascii"),
                                                level=orig_level,
                                                module=out_module_ids[device_id_int],
                                                device=local_device_id_int,
                                            )
                                        except Exception:
                                            pass

                        cursor = idx + 1
                finally:
                    timestamps.release()
                    rx_timestamps.release()
                    levels.release()
                    timestamps = rx_timestamps = levels = None
            # Flush by age
            if batch_out.size > 0 and (_time_ns() - batch_out_time >= max_timeout_ns):
                flush()

        # --- Drain on Shutdown ---
        if batch_out:
            if batch_out.size > 0:
                flush()
            else:
                batch_out.release()
