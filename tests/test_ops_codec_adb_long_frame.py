# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Covers the codec_adb_long.py kernels that don't do raw big-constant arithmetic directly on
numpy uint8 scalars (see test_ops_zephyr_timestamp.py's docstring for that overflow class of
issue under BLINKVIEW_DISABLE_NUMBA=1). nb_parse_adb_pid_tid's own digit accumulation
(`pid = pid * 10 + ...`) has the same defect - see test_ops_codec_adb_long.py's pre-existing
failing test - so it isn't extended further here."""

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.types.frames import FrameConfig
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.modules import ModuleTrackerState
from blinkview.core.types.parsing import (
    EmptyUnifiedParserConfig,
    EmptyUnifiedParserState,
    TimeParserState,
    UnifiedParserConfig,
    UnifiedParserState,
    create_default_sync,
)
from blinkview.ops.codec_adb_long import (
    nb_decode_adb_long_frame,
    nb_is_adb_long_header_iso,
    nb_is_adb_long_header_monotonic,
    nb_parse_adb_level,
    nb_parse_adb_tag,
    nb_parse_adb_timestamp_monotonic,
    nb_parse_monotonic_to_ns,
)


def _buf(msg):
    return np.frombuffer(msg.encode("ascii"), dtype=dtypes.BYTE).copy()


def _out_bundle(capacity=1):
    return LogBundle(
        timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        rx_timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        offsets=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        buffer=np.zeros(capacity * 32, dtype=dtypes.BYTE),
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
        has_devices=False,
        has_sequences=False,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def _tracker(capacity=8, name_bytes=None):
    if name_bytes is None:
        name_bytes = np.zeros(256, dtype=dtypes.BYTE)
    return ModuleTrackerState(
        count=np.zeros(1, dtype=np.int64),
        bytes_cursor=np.zeros(1, dtype=np.int64),
        starts=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        hashes=np.zeros(capacity, dtype=dtypes.HASH_TYPE),
        name_bytes=name_bytes,
    )


class TestIsAdbLongHeaderIso:
    def test_valid_iso_header(self):
        assert nb_is_adb_long_header_iso(_buf("[ 2026-04-21 19:53:52."), 0, 22) is True

    def test_too_short_is_false(self):
        assert nb_is_adb_long_header_iso(_buf("[ 2026-04-21"), 0, 12) is False

    def test_wrong_punctuation_is_false(self):
        assert nb_is_adb_long_header_iso(_buf("[ 2026x04-21 19:53:52."), 0, 22) is False


class TestIsAdbLongHeaderMonotonic:
    def test_valid_monotonic_header(self):
        assert nb_is_adb_long_header_monotonic(_buf("[ 123.456]"), 0, 10) is True

    def test_too_short_is_false(self):
        assert nb_is_adb_long_header_monotonic(_buf("[ 1"), 0, 3) is False

    def test_missing_digit_after_bracket_space_is_false(self):
        assert nb_is_adb_long_header_monotonic(_buf("[ x123]"), 0, 7) is False

    def test_missing_space_is_false(self):
        assert nb_is_adb_long_header_monotonic(_buf("[123.456]"), 0, 9) is False


class TestParseMonotonicToNs:
    def test_parses_seconds_and_nanoseconds(self):
        result = nb_parse_monotonic_to_ns(_buf("5.500000000"), 0, 11)
        assert int(result) == 5_500_000_000

    def test_partial_fraction_digits(self):
        result = nb_parse_monotonic_to_ns(_buf("1.5"), 0, 3)
        assert int(result) == 1_500_000_000

    def test_zero(self):
        result = nb_parse_monotonic_to_ns(_buf("0.0"), 0, 3)
        assert int(result) == 0


class TestParseAdbTimestampMonotonic:
    def test_parses_and_advances_cursor(self):
        msg = "[ 5.500000000] rest"
        out_b = _out_bundle()
        out_b.rx_timestamps[0] = 123_456_789  # nonzero, so a real projection is observable

        # Uses a fresh, per-test sync state rather than EmptyUnifiedParserState's shared
        # UnusedSyncState singleton - nb_auto_sync_fallback mutates the sync arrays in place, and
        # the singleton is shared module-wide, so touching it here would leak init state into
        # every other test that relies on it starting fresh.
        state = UnifiedParserState(timestamp=TimeParserState(sync=create_default_sync(now_ns=0)))

        next_cursor = nb_parse_adb_timestamp_monotonic(
            _buf(msg), 0, len(msg), out_b, 0, state, EmptyUnifiedParserConfig
        )

        # First-ever call through the (disabled-by-default) sync state routes to
        # nb_auto_sync_fallback's init branch, which just anchors and echoes rx_ns back.
        assert int(out_b.timestamps[0]) == 123_456_789
        assert _buf(msg)[next_cursor : next_cursor + 4].tobytes() == b"rest"

    def test_missing_leading_bracket_returns_negative_one(self):
        msg = "5.5]"
        out_b = _out_bundle()

        result = nb_parse_adb_timestamp_monotonic(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_no_space_found_returns_negative_one(self):
        msg = "[5.5]"  # no space anywhere after index 2
        out_b = _out_bundle()

        result = nb_parse_adb_timestamp_monotonic(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1


class TestDecodeAdbLongFrame:
    def _cfg(self, filter_ansi=False, filter_printable=False):
        return FrameConfig(
            decode_id=0,
            delimiter=0,
            length_fixed=False,
            length_min=1,
            length_max=4096,
            length=0,
            filter_printable=filter_printable,
            filter_ansi=filter_ansi,
            filter_trim_r=True,
            report_error=False,
        )

    def test_incomplete_without_a_second_header(self):
        f_buf = _buf("[ 1.0] android.wifi I/Tag: hello")
        out_buf = np.zeros(128, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_adb_long_frame(f_buf, 0, len(f_buf), out_buf, 0, self._cfg(), 0)

        assert status == 1  # STATE_INCOMPLETE
        assert consumed == 0

    def test_complete_frame_copies_header_and_body(self):
        # The header must contain a literal " ]" (space + close-bracket) for
        # nb_decode_adb_long_frame to recognize where it ends - real ADB long frames put the
        # timestamp and tag inside the brackets, e.g. "[ 1.0 android.wifi ] I/Tag: hello".
        msg = "[ 1.0 android.wifi ] I/Tag: hello\n[ 2.0] next header"
        f_buf = _buf(msg)
        out_buf = np.zeros(128, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_adb_long_frame(f_buf, 0, len(f_buf), out_buf, 0, self._cfg(), 0)

        assert status == 0  # STATE_COMPLETE
        assert consumed > 0
        decoded = bytes(out_buf[:cursor]).decode()
        assert decoded.startswith("[ 1.0 android.wifi ]")
        assert "hello" in decoded

    def test_filter_printable_drops_non_printable_bytes(self):
        msg = "[ 1.0 tag ] I/Tag: he\x01llo\n[ 2.0] next"
        f_buf = _buf(msg)
        out_buf = np.zeros(128, dtype=dtypes.BYTE)

        status, cursor, consumed = nb_decode_adb_long_frame(
            f_buf, 0, len(f_buf), out_buf, 0, self._cfg(filter_printable=True), 0
        )

        decoded = bytes(out_buf[:cursor]).decode()
        assert "\x01" not in decoded
        assert "hello" in decoded


class TestParseAdbLevel:
    def _string_table_for_levels(self):
        table_obj = IndexedStringTable(initial_capacity=4)
        return table_obj.bundle()

    def test_unmatched_level_char_returns_negative_one(self):
        out_b = _out_bundle()
        config = UnifiedParserConfig(string_table=self._string_table_for_levels())

        result = nb_parse_adb_level(_buf("I/Tag"), 0, 5, out_b, 0, EmptyUnifiedParserState, config)

        assert result == -1

    def test_too_short_buffer_returns_negative_one(self):
        out_b = _out_bundle()
        config = UnifiedParserConfig(string_table=self._string_table_for_levels())

        result = nb_parse_adb_level(_buf("I"), 0, 1, out_b, 0, EmptyUnifiedParserState, config)

        assert result == -1


class TestParseAdbTag:
    def test_simple_tag_resolves_module(self):
        msg = "Wifi ]"
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(string_table=table)
        state = UnifiedParserState(modules=tracker)

        next_cursor = nb_parse_adb_tag(_buf(msg), 0, len(msg), out_b, 0, state, config)

        assert out_b.modules[0] != 0
        assert next_cursor == len(msg)

    def test_missing_closing_delimiter_returns_negative_one(self):
        msg = "Wifi no closer here"
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(string_table=table)
        state = UnifiedParserState(modules=tracker)

        result = nb_parse_adb_tag(_buf(msg), 0, len(msg), out_b, 0, state, config)

        assert result == -1

    def test_empty_tag_before_bracket_defaults_to_zero_module(self):
        msg = " ]"
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(string_table=table)
        state = UnifiedParserState(modules=tracker)

        nb_parse_adb_tag(_buf(msg), 0, len(msg), out_b, 0, state, config)

        assert out_b.modules[0] == 0

    def test_colon_metadata_tag_with_content_after(self):
        msg = "Wifi:5 ]"
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(string_table=table)
        state = UnifiedParserState(modules=tracker)

        result = nb_parse_adb_tag(_buf(msg), 0, len(msg), out_b, 0, state, config)

        assert out_b.modules[0] != 0
        assert result != -1
