# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep, time

import numpy as np

from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.time_sync_engine import TimeSyncEngine
from blinkview.ops.segments import nb_find_next_module_index
from blinkview.parsers.parser import BaseParser, ParserFactory


@ParserFactory.register("serial_time_syncer")
@override_property(
    "sources_",
    items={"type": "string", "_reference": "/targets"},
)
@configuration_property(
    "time_source_boot",
    type="boolean",
    required=True,
    ui_order=40,
)
class SerialTimeSyncer(BaseParser):
    def __init__(self):
        super().__init__()

        self.sources = []

        self._syncer_engine = None
        self._syncer_task_id = None

    def run(self):
        logger = self.logger

        get = self.input_queue.get
        time_ns = self.shared.time_ns

        pong_prefix = np.frombuffer(b"pong", dtype=dtypes.BYTE)

        def parse_kv(payload_bytes):
            res = {}
            for item in bytes(payload_bytes).split():
                if b"=" in item:
                    k, v = item.split(b"=", 1)
                    try:
                        res[k.decode("ascii")] = int(v)
                    except ValueError:
                        pass
            return res

        try:
            time_source_boot = getattr(self, "time_source_boot", False)
            logger.debug(f"time_source_boot={time_source_boot}")

            with self._subscribers_lock:
                source = self._subscriptions[0]
                sync_state = source.sync_state

            with source._subscribers_lock:
                command_target = source._subscriptions[0]

            if self._syncer_engine is None:
                self.logger.info("Initializing new TimeSyncEngine")
                self._syncer_engine = TimeSyncEngine(sync_state, time_source_boot, self.logger)
            else:
                self.logger.info("Warm-starting existing TimeSyncEngine")
                self._syncer_engine.soft_reset()

            logger.debug(f"sync_state={sync_state}")

            module_pong = source.local.device_id.get_module("blinksync")
            module_pong_id = module_pong.id
            logger.debug(f"module_pong={module_pong} id={module_pong_id}")

            # Simplified Ping-Pong State Machine
            seq_num = 0
            pending_seq = -1
            pending_tx_ns = 0

            last_ping_ns = 0
            last_pong_ns = 0
            accepted_pings = 0
            PONG_TIMEOUT_NS = 2_000_000_000

            stop_is_set = self._stop_event.is_set
            while not stop_is_set():
                now = time_ns()
                source_enabled = source.enabled and command_target.enabled

                # --- 1. Manage Ping Transmissions ---
                in_flight = pending_seq != -1
                time_since_last_pong = now - last_pong_ns
                time_since_last_ping = now - last_ping_ns

                idle_interval_ns = 100_000_000 if accepted_pings < 50 else 1_000_000_000
                next_ping_due_ns = 0
                if source_enabled:
                    send_ping = False

                    if not in_flight:
                        next_ping_due_ns = last_pong_ns + idle_interval_ns
                        if now >= next_ping_due_ns:
                            send_ping = True
                    else:
                        next_ping_due_ns = last_ping_ns + PONG_TIMEOUT_NS
                        if now >= next_ping_due_ns:
                            logger.warning(f"Sequence {pending_seq} timed out. Retrying...")
                            pending_seq = -1  # Clear the stalled sequence
                            send_ping = True

                    if send_ping:
                        seq_num += 1
                        now = time_ns()  # Refresh time just before sending

                        last_ping_ns = now
                        pending_seq = seq_num
                        pending_tx_ns = now

                        cmd = f"blinksync ping {now} {seq_num}\n"
                        # logger.debug(f"ping seq_num={seq_num}  now={now}")
                        try:
                            command_target.send_data(cmd)
                        except Exception as e:
                            logger.exception("Failed to send ping data", e)
                            pending_seq = -1  # Abort flight immediately if send fails

                        next_ping_due_ns = last_ping_ns + PONG_TIMEOUT_NS

                    # --- 2. Calculate Dynamic Timeout ---
                    now = time_ns()
                    wait_ns = next_ping_due_ns - now
                    wait_sec = max(0.0, min(0.1, wait_ns / 1_000_000_000.0))
                else:
                    wait_sec = 0.2

                # --- 3. Process Incoming Rx Batches ---
                # logger.debug(f"get timeout={wait_sec}")
                batch_in = get(timeout=wait_sec)
                # logger.debug(f"got batch_in={batch_in}")
                if batch_in is None:
                    continue

                with batch_in:
                    b = batch_in.bundle
                    cursor = 0

                    while source_enabled:
                        found, idx = nb_find_next_module_index(
                            b, dtypes.ID_TYPE(module_pong_id), dtypes.SEQ_TYPE(cursor)
                        )

                        # logger.debug(f"idx={idx}")

                        if not found:
                            break  # Break out of the loop if no more matches

                        payload = b.buffer[b.offsets[idx] : b.offsets[idx] + b.lengths[idx]]

                        # Check for matching 'pong' prefix
                        if payload.shape[0] >= 4 and np.array_equal(payload[:4], pong_prefix):
                            # logger.debug(f"pong={payload.tobytes()}")
                            data = parse_kv(payload)
                            received_seq = data.get("seq", -1)

                            # Only process if this pong matches the ONE active ping we care about
                            if received_seq == pending_seq and pending_seq != -1:
                                # Use hardware/buffer timestamp for sync engine accuracy
                                pc_rx = b.rx_timestamps[idx]
                                last_pong_ns = last_ping_ns

                                # Feed the sync engine with tx/rx pairs
                                accepted = self._syncer_engine.feed(
                                    pc_tx=pending_tx_ns,
                                    phone_mono=data.get("mono", 0),
                                    phone_boot=data.get("boot", 0),
                                    pc_rx=pc_rx,
                                )

                                if accepted:
                                    accepted_pings += 1

                                # Clear flight status to allow next scheduled ping
                                pending_seq = -1

                        # Advance the cursor so the next search starts AFTER this match
                        cursor = idx + 1

        except Exception as e:
            logger.exception("Failure in SerialTimeSyncerParser run loop", e)
