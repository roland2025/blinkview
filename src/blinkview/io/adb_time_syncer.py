# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.ops.segments import nb_find_next_module_match


class AdbTimeSyncer:
    __slots__ = (
        "reader",
        "engine",
        "time_ns",
        "log_pool",
        "target_parser",
        "hb_id",
        "hb_cursor",
        "pong_id",
        "pong_cursor",
        "last_ping_ns",
        "accepted_pings",
        "seq_num",
        "logger",
    )

    def __init__(self, reader, engine, logger, time_ns, log_pool, target_parser):
        self.reader = reader
        self.engine = engine
        self.time_ns = time_ns
        self.log_pool = log_pool
        self.target_parser = target_parser
        self.logger = logger

        # Cursors
        self.hb_cursor = self.pong_cursor = SEQ_NONE
        self.hb_id = self.pong_id = None

        # Timing
        self.last_ping_ns = 0
        self.accepted_pings = 0
        self.seq_num = 0

    def tick(self, now_ns):
        self.seq_num += 1
        self.last_ping_ns = now_ns
        cmd = f"am broadcast -a ee.incubator.blinksync.ping --es seq {self.seq_num} --es pc_tx {now_ns}"
        self.reader.send_data(cmd)

    def _parse_kv(self, payload):
        res = {}
        # Splitting bytes without args perfectly handles spaces, \t, \n, and \r
        for item in bytes(payload).split():
            if b"=" in item:
                k, v = item.split(b"=", 1)
                try:
                    res[k.decode("ascii")] = int(v)
                except ValueError:
                    pass
        return res

    def handler(self):
        try:
            now = self.time_ns()

            if self.hb_id is None or self.pong_id is None:
                hb = self.target_parser.local.device_id.get_module("blinksync.hb")
                pong = self.target_parser.local.device_id.get_module("blinksync.pong")
                if not (hb and pong):
                    return
                self.hb_id, self.pong_id = hb.id, pong.id

            if self.hb_cursor == SEQ_NONE:
                with self.log_pool.get_snapshot() as segments:
                    if segments:
                        tail = segments[-1].last_sequence_id
                        self.hb_cursor = self.pong_cursor = tail
                return

            retry_needed = False

            with self.log_pool.get_snapshot() as segments:
                for segment in segments:
                    if segment.last_sequence_id <= self.pong_cursor:
                        continue

                    b = segment.bundle
                    while True:
                        found_seq, idx = nb_find_next_module_match(
                            b, dtypes.ID_TYPE(self.pong_id), dtypes.SEQ_TYPE(self.pong_cursor)
                        )
                        if not found_seq:
                            self.pong_cursor = segment.last_sequence_id
                            break

                        # print(f"DEBUG: {nb_find_next_module_match.__name__} signatures:")
                        # for sig in nb_find_next_module_match.signatures:
                        #     print(f"  - {sig}")

                        payload = b.buffer[b.offsets[idx] : b.offsets[idx] + b.lengths[idx]]
                        data = self._parse_kv(payload)

                        received_seq = data.get("seq", -1)

                        if received_seq > self.seq_num:
                            # We just ignore it and move the cursor forward
                            self.pong_cursor = found_seq
                            continue

                        if received_seq != -1 and received_seq < self.seq_num - 5:
                            self.pong_cursor = found_seq
                            continue

                        # Note: Android logcat uses CLOCK_MONOTONIC (pauses in sleep).
                        # We use phone_mono to align the logcat phase.
                        # We use phone_boot (CLOCK_BOOTTIME) to calculate absolute hardware drift,
                        # ensuring crystal frequency math remains accurate even if the device slept.
                        accepted = self.engine.feed(
                            pc_tx=data.get("tx", 0),
                            phone_mono=data.get("mono", 0),
                            phone_boot=data.get("boot", 0),
                            pc_rx=b.rx_timestamps[idx],
                        )

                        if accepted:
                            self.accepted_pings += 1
                        else:
                            retry_needed = True

                        self.pong_cursor = found_seq

            # Two-Stage Backoff: Warm-up quickly, then idle back to avoid ADB DDoS
            if self.accepted_pings < 50 or retry_needed:
                target_interval_ns = 333_000_000  # 0.5s
            else:
                target_interval_ns = 1_000_000_000  # 1.0s

            if (now - self.last_ping_ns) >= target_interval_ns:
                self.tick(now)

        except Exception as e:
            self.logger.exception("Syncer handler failure", e)
