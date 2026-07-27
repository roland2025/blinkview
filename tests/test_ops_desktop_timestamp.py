# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from datetime import datetime, timezone

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.parsing import (
    EmptyUnifiedParserConfig,
    EmptyUnifiedParserState,
    SyncState,
    TimeParserState,
    UnifiedParserConfig,
    UnifiedParserState,
)
from blinkview.ops.desktop_timestamp import nb_parse_iso8601_desktop, nb_parse_syslog_timestamp


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
        has_levels=False,
        has_modules=False,
        has_devices=False,
        has_sequences=False,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def _buf(msg):
    return np.frombuffer(msg.encode("ascii"), dtype=dtypes.BYTE)


def _identity_sync_state():
    """A formal (non-auto) sync with a 1:1, zero-offset drift, so `nb_project_synced_ns`
    passes the parsed raw_ns straight through - letting tests assert the exact parsed epoch
    value instead of the auto-sync fallback's first-packet-anchors-to-rx_ns behavior."""
    identity = np.array([1_000_000_000, 1_000_000_000], dtype=dtypes.INT64)
    zero = np.array([0, 0], dtype=dtypes.INT64)
    return SyncState(
        enabled=np.array([1], dtype=np.uint8),
        active_idx=np.array([0], dtype=dtypes.INT64),
        offset=zero,
        ref_time=zero,
        drift_m=identity,
        drift_d=identity,
    )


def _identity_state():
    return UnifiedParserState(timestamp=TimeParserState(sync=_identity_sync_state()))


class TestParseIso8601Desktop:
    def test_python_logging_format_with_comma_separator(self):
        msg = "2026-01-15 10:23:01,456 INFO myapp.module: message"
        out_b = _out_bundle()

        cursor = nb_parse_iso8601_desktop(_buf(msg), 0, len(msg), out_b, 0, _identity_state(), EmptyUnifiedParserConfig)

        assert cursor == msg.index("INFO")
        expected = int(datetime(2026, 1, 15, 10, 23, 1, 456000, tzinfo=timezone.utc).timestamp() * 1e9)
        assert out_b.timestamps[0] == expected

    def test_iso8601_format_with_dot_separator(self):
        msg = "2026-01-15 10:23:01.456 INFO myapp.module: message"
        out_b = _out_bundle()

        cursor = nb_parse_iso8601_desktop(_buf(msg), 0, len(msg), out_b, 0, _identity_state(), EmptyUnifiedParserConfig)

        assert cursor == msg.index("INFO")
        expected = int(datetime(2026, 1, 15, 10, 23, 1, 456000, tzinfo=timezone.utc).timestamp() * 1e9)
        assert out_b.timestamps[0] == expected

    def test_bad_dash_placement_returns_negative_one(self):
        msg = "2026x01-15 10:23:01.456 INFO"
        out_b = _out_bundle()

        result = nb_parse_iso8601_desktop(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_missing_fraction_separator_returns_negative_one(self):
        msg = "2026-01-15 10:23:01x456 INFO"
        out_b = _out_bundle()

        result = nb_parse_iso8601_desktop(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_too_short_buffer_returns_negative_one(self):
        msg = "2026-01-15 10:23:01."
        out_b = _out_bundle()

        result = nb_parse_iso8601_desktop(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1


class TestParseSyslogTimestamp:
    def test_zero_padded_day(self):
        msg = "Jan 02 15:04:05 myhost app: message"
        out_b = _out_bundle()
        config = UnifiedParserConfig(syslog_year=2026)

        cursor = nb_parse_syslog_timestamp(_buf(msg), 0, len(msg), out_b, 0, _identity_state(), config)

        assert cursor == msg.index("myhost")
        expected = int(datetime(2026, 1, 2, 15, 4, 5, tzinfo=timezone.utc).timestamp() * 1e9)
        assert out_b.timestamps[0] == expected

    def test_space_padded_day(self):
        msg = "Jan  2 15:04:05 myhost app: message"
        out_b = _out_bundle()
        config = UnifiedParserConfig(syslog_year=2026)

        cursor = nb_parse_syslog_timestamp(_buf(msg), 0, len(msg), out_b, 0, _identity_state(), config)

        assert cursor == msg.index("myhost")
        expected = int(datetime(2026, 1, 2, 15, 4, 5, tzinfo=timezone.utc).timestamp() * 1e9)
        assert out_b.timestamps[0] == expected

    def test_december(self):
        msg = "Dec 31 23:59:59 myhost app: message"
        out_b = _out_bundle()
        config = UnifiedParserConfig(syslog_year=2026)

        cursor = nb_parse_syslog_timestamp(_buf(msg), 0, len(msg), out_b, 0, _identity_state(), config)

        assert cursor == msg.index("myhost")
        expected = int(datetime(2026, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1e9)
        assert out_b.timestamps[0] == expected

    def test_unknown_month_returns_negative_one(self):
        msg = "Xxx  2 15:04:05 myhost"
        out_b = _out_bundle()

        result = nb_parse_syslog_timestamp(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_missing_colons_returns_negative_one(self):
        msg = "Jan  2 15x04x05 myhost"
        out_b = _out_bundle()

        result = nb_parse_syslog_timestamp(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_too_short_buffer_returns_negative_one(self):
        msg = "Jan  2 15:04"
        out_b = _out_bundle()

        result = nb_parse_syslog_timestamp(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1
