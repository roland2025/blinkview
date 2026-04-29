# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.time_sync_engine import TimeSyncEngine
from blinkview.ops.segments import nb_find_next_module_index, nb_find_next_module_match
from blinkview.parsers.parser import BaseParser, ParserFactory
from blinkview.subscribers.subscriber import BaseSubscriber, TimeSyncerFactory


@ParserFactory.register("adb_time_syncer")
@override_property(
    "sources_",
    items={"type": "string", "_reference": "/targets"},
)
@configuration_property(
    "time_source_boot",
    type="boolean",
    default=True,
    hidden=True,
    required=True,
    ui_order=40,
)
class AdbTimeSyncerParser(BaseParser):
    def __init__(self):
        super().__init__()

        self.sources = []

        self._syncer_engine = None
        self._syncer_task_id = None

        self.command_target = None

    def ensure_blinksync_running(self, timeout_s=3.0, interval_s=0.05):
        """
        Checks if blinksync is running. If not, starts it and polls
        until the PID appears or we hit the timeout.
        """
        from time import sleep  # Assuming this is or will be imported at the top

        pkg = "ee.incubator.blinksync"

        time_ns = self.shared.time_ns
        target = self.command_target

        # 1. Quick check using walrus operator
        if pid := target.get_pid_from_name(pkg):
            self.logger.info(f"Blinksync already active (PID: {pid})")
            return True

        # 2. Trigger the launch
        self.logger.info("Blinksync not detected. Triggering launch...")
        target.query(f"am start -n {pkg}/.MainActivity")

        # 3. Active Wait (Polling)
        start_time_ns = time_ns()
        timeout_ns = int(timeout_s * 1_000_000_000)
        deadline_ns = start_time_ns + timeout_ns

        while time_ns() < deadline_ns:
            if new_pid := target.get_pid_from_name(pkg):
                elapsed_ms = (time_ns() - start_time_ns) / 1_000_000.0
                self.logger.info(f"Blinksync started in {elapsed_ms:.0f}ms (PID: {new_pid})")
                return True

            sleep(interval_s)

        self.logger.error(f"Blinksync failed to spawn after {timeout_s}s. Check device state.")
        return False

    def run(self):
        logger = self.logger

        get = self.input_queue.get
        time_ns = self.shared.time_ns

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
                self.command_target = command_target

            if self._syncer_engine is None:
                self.logger.info("Initializing new TimeSyncEngine")
                self._syncer_engine = TimeSyncEngine(sync_state, time_source_boot, self.logger)
            else:
                self.logger.info("Warm-starting existing TimeSyncEngine")
                self._syncer_engine.soft_reset()

            logger.debug(f"sync_state={sync_state}")

            module_pong = source.local.device_id.get_module("blinksync.pong")
            if not module_pong:
                logger.error("Could not find 'blinksync.pong' module on target.")
                return

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

            blinksync_verified = False
            last_verify_attempt_ns = 0
            VERIFY_COOLDOWN_NS = 1_000_000_000

            stop_is_set = self._stop_event.is_set
            while not stop_is_set():
                now = time_ns()
                source_enabled = source.enabled and command_target.is_connected()

                # --- 1. Manage Ping Transmissions ---
                in_flight = pending_seq != -1

                # Two-Stage Backoff: Warm-up quickly, then idle back
                idle_interval_ns = 200_000_000 if accepted_pings < 50 else 1_000_000_000

                next_ping_due_ns = 0
                if source_enabled:
                    send_ping = False

                    if not blinksync_verified:
                        if now >= last_verify_attempt_ns + VERIFY_COOLDOWN_NS:
                            blinksync_verified = self.ensure_blinksync_running()
                            last_verify_attempt_ns = time_ns()
                            now = time_ns()  # Refresh time after potentially blocking in check

                        if not blinksync_verified:
                            # Schedule the next check without sleeping so batches can still clear
                            next_ping_due_ns = last_verify_attempt_ns + VERIFY_COOLDOWN_NS

                    if blinksync_verified:
                        if not in_flight:
                            next_ping_due_ns = last_pong_ns + idle_interval_ns
                            if now >= next_ping_due_ns:
                                send_ping = True
                        else:
                            next_ping_due_ns = last_ping_ns + PONG_TIMEOUT_NS
                            if now >= next_ping_due_ns:
                                logger.warning(f"Sequence {pending_seq} timed out. Retrying...")
                                pending_seq = -1  # Clear the stalled sequence
                                blinksync_verified = False  # Force a health check on the next loop
                                send_ping = True

                        if send_ping:
                            seq_num += 1
                            now = time_ns()

                            last_ping_ns = now
                            pending_seq = seq_num
                            pending_tx_ns = now

                            cmd = f"am broadcast -a ee.incubator.blinksync.ping --es seq {seq_num} --es pc_tx {now}"

                            try:
                                command_target.send_data(cmd)
                            except Exception as e:
                                logger.exception("Failed to send ping data over ADB", e)
                                pending_seq = -1  # Abort flight immediately if send fails
                                blinksync_verified = False

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

                    search_pongs = source_enabled and blinksync_verified

                    # Skip searching the bundle entirely if the app isn't verified running
                    while search_pongs:
                        found, idx = nb_find_next_module_index(
                            b, dtypes.ID_TYPE(module_pong_id), dtypes.SEQ_TYPE(cursor)
                        )

                        if not found:
                            break

                        payload = b.buffer[b.offsets[idx] : b.offsets[idx] + b.lengths[idx]]
                        data = parse_kv(payload)
                        received_seq = data.get("seq", -1)

                        # Only process if this pong matches the ONE packet we actually care about
                        if received_seq == pending_seq and pending_seq != -1:
                            pc_rx = b.rx_timestamps[idx]
                            last_pong_ns = last_ping_ns

                            accepted = self._syncer_engine.feed(
                                pc_tx=data.get("tx", pending_tx_ns),
                                phone_mono=data.get("mono", 0),
                                phone_boot=data.get("boot", 0),
                                pc_rx=pc_rx,
                            )

                            if accepted:
                                accepted_pings += 1

                            # Clear flight status to allow next scheduled ping,
                            # regardless of engine acceptance.
                            pending_seq = -1

                        cursor = idx + 1

        except Exception as e:
            logger.exception("Failure in AdbTimeSyncerParser run loop", e)


@TimeSyncerFactory.register("adb_time_syncer")
@configuration_property(
    "time_source_boot",
    type="boolean",
    default=True,
    hidden=True,
    required=True,
    # ui_order=40,
)
@configuration_property(
    "blinksync_app",
    title="BlinkSync app",
    type="boolean",
    default=False,
    required=True,
    # ui_order=40,
)
class AdbTimeSyncerSubscriber(BaseSubscriber):
    def __init__(self):
        super().__init__()

        self.sources = []

        self._syncer_engine = None
        self._syncer_task_id = None

        self.command_target = None

    def ensure_blinksync_running(self, timeout_s=3.0, interval_s=0.05):
        """
        Checks if blinksync is running. If not, starts it and polls
        until the PID appears or we hit the timeout.
        """
        from time import sleep  # Assuming this is or will be imported at the top

        pkg = "ee.incubator.blinksync"

        time_ns = self.shared.time_ns
        target = self.command_target

        # 1. Quick check using walrus operator
        if pid := target.get_pid_from_name(pkg):
            self.logger.info(f"Blinksync already active (PID: {pid})")
            return True

        # 2. Trigger the launch
        self.logger.info("Blinksync not detected. Triggering launch...")
        target.query(f"am start -n {pkg}/.MainActivity")

        # 3. Active Wait (Polling)
        start_time_ns = time_ns()
        timeout_ns = int(timeout_s * 1_000_000_000)
        deadline_ns = start_time_ns + timeout_ns

        while time_ns() < deadline_ns:
            if new_pid := target.get_pid_from_name(pkg):
                elapsed_ms = (time_ns() - start_time_ns) / 1_000_000.0
                self.logger.info(f"Blinksync started in {elapsed_ms:.0f}ms (PID: {new_pid})")
                return True

            sleep(interval_s)

        self.logger.error(f"Blinksync failed to spawn after {timeout_s}s. Check device state.")
        return False

    def run(self):
        logger = self.logger

        get = self.input_queue.get
        time_ns = self.shared.time_ns

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

            source = self.local.parser
            sync_state = source.sync_state

            with source._subscribers_lock:
                command_target = source._subscriptions[0]
                self.command_target = command_target

            if self._syncer_engine is None:
                self.logger.info("Initializing new TimeSyncEngine")
                self._syncer_engine = TimeSyncEngine(sync_state, time_source_boot, self.logger)
            else:
                self.logger.info("Warm-starting existing TimeSyncEngine")
                self._syncer_engine.soft_reset()

            logger.debug(f"sync_state={sync_state}")

            module_pong = source.local.device_id.get_module("blinksync.pong")
            if not module_pong:
                logger.error("Could not find 'blinksync.pong' module on target.")
                return

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

            blinksync_verified = False
            last_verify_attempt_ns = 0
            VERIFY_COOLDOWN_NS = 1_000_000_000

            stop_is_set = self._stop_event.is_set
            while not stop_is_set():
                now = time_ns()
                source_enabled = source.enabled and command_target.is_connected()

                # --- 1. Manage Ping Transmissions ---
                in_flight = pending_seq != -1

                # Two-Stage Backoff: Warm-up quickly, then idle back
                idle_interval_ns = 200_000_000 if accepted_pings < 50 else 1_000_000_000

                next_ping_due_ns = 0
                if source_enabled:
                    send_ping = False

                    if not blinksync_verified:
                        if now >= last_verify_attempt_ns + VERIFY_COOLDOWN_NS:
                            blinksync_verified = self.ensure_blinksync_running()
                            last_verify_attempt_ns = time_ns()
                            now = time_ns()  # Refresh time after potentially blocking in check

                        if not blinksync_verified:
                            # Schedule the next check without sleeping so batches can still clear
                            next_ping_due_ns = last_verify_attempt_ns + VERIFY_COOLDOWN_NS

                    if blinksync_verified:
                        if not in_flight:
                            next_ping_due_ns = last_pong_ns + idle_interval_ns
                            if now >= next_ping_due_ns:
                                send_ping = True
                        else:
                            next_ping_due_ns = last_ping_ns + PONG_TIMEOUT_NS
                            if now >= next_ping_due_ns:
                                logger.warning(f"Sequence {pending_seq} timed out. Retrying...")
                                pending_seq = -1  # Clear the stalled sequence
                                blinksync_verified = False  # Force a health check on the next loop
                                send_ping = True

                        if send_ping:
                            seq_num += 1
                            now = time_ns()

                            last_ping_ns = now
                            pending_seq = seq_num
                            pending_tx_ns = now

                            cmd = f"am broadcast -a ee.incubator.blinksync.ping --es seq {seq_num} --es pc_tx {now}"

                            try:
                                command_target.send_data(cmd)
                            except Exception as e:
                                logger.exception("Failed to send ping data over ADB", e)
                                pending_seq = -1  # Abort flight immediately if send fails
                                blinksync_verified = False

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

                    search_pongs = source_enabled and blinksync_verified

                    # Skip searching the bundle entirely if the app isn't verified running
                    while search_pongs:
                        found, idx = nb_find_next_module_index(
                            b, dtypes.ID_TYPE(module_pong_id), dtypes.SEQ_TYPE(cursor)
                        )

                        if not found:
                            break

                        payload = b.buffer[b.offsets[idx] : b.offsets[idx] + b.lengths[idx]]
                        data = parse_kv(payload)
                        received_seq = data.get("seq", -1)

                        # Only process if this pong matches the ONE packet we actually care about
                        if received_seq == pending_seq and pending_seq != -1:
                            pc_rx = b.rx_timestamps[idx]
                            last_pong_ns = last_ping_ns

                            accepted = self._syncer_engine.feed(
                                pc_tx=data.get("tx", pending_tx_ns),
                                phone_mono=data.get("mono", 0),
                                phone_boot=data.get("boot", 0),
                                pc_rx=pc_rx,
                            )

                            if accepted:
                                accepted_pings += 1

                            # Clear flight status to allow next scheduled ping,
                            # regardless of engine acceptance.
                            pending_seq = -1

                        cursor = idx + 1

        except Exception as e:
            logger.exception("Failure in AdbTimeSyncerParser run loop", e)
