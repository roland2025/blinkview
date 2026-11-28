# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property
from blinkview.core.time_sync_engine import ASYMMETRY_RATIO_PROPERTY, TimeSyncEngine
from blinkview.ops.blinksync import KEY_BOOT, KEY_MONO, KEY_SEQ, PONG_PREFIX, nb_find_and_parse_next_pong
from blinkview.subscribers.subscriber import BaseSubscriber, TimeSyncerFactory

SEQ_FIRST = 100_000  # ping seqs stay 6 digits: 100000..999999
SEQ_WRAP = 1_000_000


@TimeSyncerFactory.register("serial_time_syncer")
@configuration_property("time_source_boot", type="boolean", required=True, ui_order=40, default=False)
@configuration_property("asymmetry_ratio", **ASYMMETRY_RATIO_PROPERTY)
class SerialTimeSyncerSubscriber(BaseSubscriber):
    def __init__(self):
        super().__init__()

        self.sources = []

        self._syncer_engine = None
        self._syncer_task_id = None
        self._seq = None  # last ping seq sent; kept across warm restarts of run()

    def run(self):
        logger = self.logger

        get = self.input_queue.get
        time_ns = self.shared.time_ns

        try:
            logger.debug("asymmetry_ratio=%s", self.asymmetry_ratio)

            time_source_boot = self.time_source_boot
            logger.debug("time_source_boot=%s", time_source_boot)

            with self._subscribers_lock:
                source = self._subscriptions[0]
                sync_state = source.sync_state

            # The reader the pings go out through: given when the parser auto-loaded this syncer
            # for its source, else the parser's first upstream subscription
            command_target = getattr(self.local, "source", None)
            if command_target is None:
                with source._subscribers_lock:
                    command_target = source._subscriptions[0]

            if self._syncer_engine is None:
                self.logger.info("Initializing new TimeSyncEngine")
                self._syncer_engine = TimeSyncEngine(sync_state, time_source_boot, self.logger)
            else:
                self.logger.info("Warm-starting existing TimeSyncEngine")
                self._syncer_engine.soft_reset()

            self._syncer_engine.set_asymmetry(self.asymmetry_ratio)

            logger.debug("sync_state=%s", sync_state)

            module_pong = source.local.device_id.get_module("bsync")
            module_pong_id = module_pong.id
            logger.debug("module_pong=%s id=%s", module_pong, module_pong_id)

            # Simplified Ping-Pong State Machine
            # A restarted syncer must not count from 1 again: the pong to the previous run's last ping can still
            # be on its way and would match the new "bsync p 1" (on the rig a time_sync toggle restarted it twice
            # within 2 ms; the stale pong read as a 0.07 ms RTT). A new syncer starts at a 6-digit value from the
            # clock, which also keeps the pong the same length for days (its length is return-path time).
            if self._seq is None:
                self._seq = SEQ_FIRST + (time_ns() // 1_000_000) % (SEQ_WRAP - SEQ_FIRST)
            seq_num = self._seq
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

                idle_interval_ns = 100_300_000 if accepted_pings < 50 else 1_000_300_000
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
                            logger.warning("Sequence %s timed out. Retrying...", pending_seq)
                            pending_seq = -1  # Clear the stalled sequence
                            send_ping = True

                    if send_ping:
                        seq_num = seq_num + 1 if seq_num + 1 < SEQ_WRAP else SEQ_FIRST
                        self._seq = seq_num
                        now = time_ns()  # Refresh time just before sending

                        last_ping_ns = now
                        pending_seq = seq_num
                        pending_tx_ns = now

                        cmd = f"bsync p {seq_num}\n"
                        try:
                            command_target.send_data(cmd)
                        except Exception as e:
                            logger.exception("Failed to send ping data", exc=e)
                            pending_seq = -1  # Abort flight immediately if send fails

                        next_ping_due_ns = last_ping_ns + PONG_TIMEOUT_NS

                    # --- 2. Calculate Dynamic Timeout ---
                    now = time_ns()
                    wait_ns = next_ping_due_ns - now
                    wait_sec = max(0.0, min(0.1, wait_ns / 1_000_000_000.0))
                else:
                    wait_sec = 0.2

                # --- 3. Process Incoming Rx Batches ---
                batch_in = get(timeout=wait_sec)

                if batch_in is None:
                    continue

                with batch_in:
                    b = batch_in.bundle
                    cursor = 0

                    # Ensure types match the Numba signature exactly
                    numba_module_id = dtypes.ID_TYPE(module_pong_id)

                    while source_enabled:
                        numba_cursor = dtypes.SEQ_TYPE(cursor)

                        # One single C-level execution
                        found, is_pong, cursor, received_seq, phone_mono, phone_boot, pc_rx = (
                            nb_find_and_parse_next_pong(
                                b, numba_module_id, numba_cursor, PONG_PREFIX, KEY_SEQ, KEY_MONO, KEY_BOOT
                            )
                        )

                        if not found:
                            break  # Break out of the loop if no more matches

                        if is_pong:
                            # Only process if this pong matches the ONE active ping we care about
                            if received_seq == pending_seq and pending_seq != -1:
                                last_pong_ns = last_ping_ns

                                # Feed the sync engine with tx/rx pairs
                                accepted = self._syncer_engine.feed(
                                    pc_tx=pending_tx_ns,
                                    phone_mono=phone_mono,
                                    phone_boot=phone_boot,
                                    pc_rx=pc_rx,
                                )

                                if accepted:
                                    accepted_pings += 1

                                # Clear flight status to allow next scheduled ping
                                pending_seq = -1

        except Exception as e:
            logger.exception("Failure in SerialTimeSyncerSubscriber run loop", exc=e)
