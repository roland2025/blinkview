# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.unified_log_scan import nb_push_unified_log_rows, nb_scan_unified_log_lines
from blinkview.utils.log_level import LogLevel


def make_out_bundle(capacity, max_msg_bytes=64):
    """Minimal LogBundle for testing nb_push_unified_log_rows's writes."""
    return LogBundle(
        timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        rx_timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        offsets=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        buffer=np.zeros(capacity * max_msg_bytes, dtype=dtypes.BYTE),
        levels=np.zeros(capacity, dtype=dtypes.LEVEL_TYPE),
        modules=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        devices=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        sequences=np.zeros(capacity, dtype=dtypes.SEQ_TYPE),
        pids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        tids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        ext_u32_1=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u32_2=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u64_1=np.zeros(capacity, dtype=dtypes.UINT64),
        size=np.array([0], dtype=np.int64),
        msg_cursor=np.array([0], dtype=np.int64),
        capacity=capacity,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def make_buf(text: str) -> np.ndarray:
    return np.frombuffer(text.encode("utf-8"), dtype=np.uint8)


class ScanOutputs:
    """Preallocated output arrays for nb_scan_unified_log_lines, sized for test use."""

    def __init__(self, max_rows=16, max_malformed=16):
        self.max_rows = max_rows
        self.max_malformed = max_malformed
        self.ts_ns = np.empty(max_rows, dtype=np.int64)
        self.level = np.empty(max_rows, dtype=np.int64)
        self.dev_off = np.empty(max_rows, dtype=np.int64)
        self.dev_len = np.empty(max_rows, dtype=np.int64)
        self.mod_off = np.empty(max_rows, dtype=np.int64)
        self.mod_len = np.empty(max_rows, dtype=np.int64)
        self.msg_off = np.empty(max_rows, dtype=np.int64)
        self.msg_len = np.empty(max_rows, dtype=np.int64)
        self.malformed_off = np.empty(max_malformed, dtype=np.int64)
        self.malformed_len = np.empty(max_malformed, dtype=np.int64)

    def scan(self, buf, start_cursor=0, max_rows=None):
        return nb_scan_unified_log_lines(
            buf,
            start_cursor,
            max_rows if max_rows is not None else self.max_rows,
            self.ts_ns,
            self.level,
            self.dev_off,
            self.dev_len,
            self.mod_off,
            self.mod_len,
            self.msg_off,
            self.msg_len,
            self.malformed_off,
            self.malformed_len,
            self.max_malformed,
        )

    def field_str(self, buf, off_arr, len_arr, row):
        off, length = int(off_arr[row]), int(len_arr[row])
        return bytes(buf[off : off + length]).decode("utf-8")


class TestScanUnifiedLogLines:
    def test_parses_well_formed_line(self):
        buf = make_buf("2026-01-01T00:00:00.000000Z I nrf52 log: hello world\n")
        out = ScanOutputs()

        rows_found, next_cursor, malformed_found, overflow = out.scan(buf)

        assert rows_found == 1
        assert next_cursor == len(buf)
        assert malformed_found == 0
        assert overflow == 0
        assert out.ts_ns[0] == 1_767_225_600_000_000_000
        assert out.level[0] == LogLevel.INFO.value
        assert out.field_str(buf, out.dev_off, out.dev_len, 0) == "nrf52"
        assert out.field_str(buf, out.mod_off, out.mod_len, 0) == "log"
        assert out.field_str(buf, out.msg_off, out.msg_len, 0) == "hello world"

    def test_message_containing_colon_space_is_kept_whole(self):
        buf = make_buf("2026-01-01T00:00:00.000000Z I dev log: key: value\n")
        out = ScanOutputs()

        rows_found, _next_cursor, _malformed_found, _overflow = out.scan(buf)

        assert rows_found == 1
        assert out.field_str(buf, out.mod_off, out.mod_len, 0) == "log"
        assert out.field_str(buf, out.msg_off, out.msg_len, 0) == "key: value"

    def test_blank_lines_are_skipped(self):
        buf = make_buf("\n2026-01-01T00:00:00.000000Z I dev log: only\n\n")
        out = ScanOutputs()

        rows_found, next_cursor, malformed_found, overflow = out.scan(buf)

        assert rows_found == 1
        assert next_cursor == len(buf)
        assert malformed_found == 0
        assert overflow == 0
        assert out.field_str(buf, out.msg_off, out.msg_len, 0) == "only"

    def test_malformed_line_recorded_and_skipped(self):
        buf = make_buf(
            "2026-01-01T00:00:00.000000Z I dev log: before\n"
            "this line does not match the grammar at all\n"
            "2026-01-01T00:00:01.000000Z I dev log: after\n"
        )
        out = ScanOutputs()

        rows_found, next_cursor, malformed_found, overflow = out.scan(buf)

        assert rows_found == 2
        assert next_cursor == len(buf)
        assert malformed_found == 1
        assert overflow == 0
        assert out.field_str(buf, out.msg_off, out.msg_len, 0) == "before"
        assert out.field_str(buf, out.msg_off, out.msg_len, 1) == "after"
        off, length = int(out.malformed_off[0]), int(out.malformed_len[0])
        assert bytes(buf[off : off + length]) == b"this line does not match the grammar at all"

    def test_malformed_overflow_beyond_cap_is_counted_not_recorded(self):
        lines = "\n".join("not a valid line" for _ in range(5)) + "\n"
        buf = make_buf(lines)
        out = ScanOutputs(max_malformed=2)

        rows_found, _next_cursor, malformed_found, overflow = out.scan(buf)

        assert rows_found == 0
        assert malformed_found == 2
        assert overflow == 3

    def test_stops_at_max_rows_and_resumes_from_next_cursor(self):
        lines = "".join(f"2026-01-01T00:00:0{i}.000000Z I dev log: row{i}\n" for i in range(3))
        buf = make_buf(lines)
        out = ScanOutputs()

        rows_found, next_cursor, _malformed_found, _overflow = out.scan(buf, max_rows=2)
        assert rows_found == 2
        assert out.field_str(buf, out.msg_off, out.msg_len, 0) == "row0"
        assert out.field_str(buf, out.msg_off, out.msg_len, 1) == "row1"
        assert next_cursor < len(buf)

        rows_found2, next_cursor2, _malformed_found2, _overflow2 = out.scan(buf, start_cursor=next_cursor, max_rows=2)
        assert rows_found2 == 1
        assert out.field_str(buf, out.msg_off, out.msg_len, 0) == "row2"
        assert next_cursor2 == len(buf)

    def test_all_seven_level_chars_resolve_correctly(self):
        levels = [("T", LogLevel.TRACE), ("D", LogLevel.DEBUG), ("I", LogLevel.INFO), ("W", LogLevel.WARN)]
        levels += [("E", LogLevel.ERROR), ("F", LogLevel.FATAL), ("C", LogLevel.CRITICAL)]
        lines = "".join(f"2026-01-01T00:00:00.000000Z {c} dev log: msg\n" for c, _ in levels)
        buf = make_buf(lines)
        out = ScanOutputs()

        rows_found, _next_cursor, _malformed_found, _overflow = out.scan(buf)

        assert rows_found == len(levels)
        for i, (_char, lvl) in enumerate(levels):
            assert out.level[i] == lvl.value

    def test_unrecognized_level_char_falls_back_to_info(self):
        buf = make_buf("2026-01-01T00:00:00.000000Z X dev log: msg\n")
        out = ScanOutputs()

        rows_found, _next_cursor, _malformed_found, _overflow = out.scan(buf)

        assert rows_found == 1
        assert out.level[0] == LogLevel.INFO.value


class TestPushUnifiedLogRows:
    def test_pushes_rows_into_bundle(self):
        buf = make_buf(
            "2026-01-01T00:00:00.000000Z I dev log: hello world\n2026-01-01T00:00:01.500000Z E dev log: boom\n"
        )
        out = ScanOutputs()
        rows_found, _next_cursor, _malformed_found, _overflow = out.scan(buf)

        device_id = np.array([5, 5], dtype=np.int64)
        module_id = np.array([9, 9], dtype=np.int64)
        bundle = make_out_bundle(10)

        pushed = nb_push_unified_log_rows(
            bundle, buf, out.ts_ns, out.level, device_id, module_id, out.msg_off, out.msg_len, rows_found
        )

        assert pushed == 2
        assert bundle.size[0] == 2
        assert bundle.timestamps[0] == 1_767_225_600_000_000_000
        assert bundle.timestamps[1] == 1_767_225_601_500_000_000
        assert bundle.levels[0] == LogLevel.INFO.value
        assert bundle.levels[1] == LogLevel.ERROR.value
        assert bundle.devices[0] == 5
        assert bundle.modules[0] == 9

        s0, l0 = int(bundle.offsets[0]), int(bundle.lengths[0])
        s1, l1 = int(bundle.offsets[1]), int(bundle.lengths[1])
        assert bytes(bundle.buffer[s0 : s0 + l0]) == b"hello world"
        assert bytes(bundle.buffer[s1 : s1 + l1]) == b"boom"

    def test_stops_early_when_bundle_row_capacity_full(self):
        buf = make_buf(
            "2026-01-01T00:00:00.000000Z I dev log: one\n"
            "2026-01-01T00:00:01.000000Z I dev log: two\n"
            "2026-01-01T00:00:02.000000Z I dev log: three\n"
        )
        out = ScanOutputs()
        rows_found, _next_cursor, _malformed_found, _overflow = out.scan(buf)
        assert rows_found == 3

        device_id = np.zeros(rows_found, dtype=np.int64)
        module_id = np.zeros(rows_found, dtype=np.int64)
        bundle = make_out_bundle(2)  # capacity smaller than rows_found

        pushed = nb_push_unified_log_rows(
            bundle, buf, out.ts_ns, out.level, device_id, module_id, out.msg_off, out.msg_len, rows_found
        )

        assert pushed == 2
        assert bundle.size[0] == 2

    def test_stops_early_when_message_buffer_capacity_full(self):
        buf = make_buf(
            "2026-01-01T00:00:00.000000Z I dev log: a longer first message here\n"
            "2026-01-01T00:00:01.000000Z I dev log: second\n"
        )
        out = ScanOutputs()
        rows_found, _next_cursor, _malformed_found, _overflow = out.scan(buf)
        assert rows_found == 2

        device_id = np.zeros(rows_found, dtype=np.int64)
        module_id = np.zeros(rows_found, dtype=np.int64)
        # Row capacity of 10 but a byte buffer only big enough for the first message.
        bundle = make_out_bundle(10, max_msg_bytes=3)

        pushed = nb_push_unified_log_rows(
            bundle, buf, out.ts_ns, out.level, device_id, module_id, out.msg_off, out.msg_len, rows_found
        )

        assert pushed == 1
        assert bundle.size[0] == 1
