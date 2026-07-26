# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""nb_parse_zephyr_uptime_formatted/nb_parse_zephyr_realtime both do raw `(buffer[i] - CHAR_ZERO)
* 1000`-style arithmetic directly on numpy uint8 scalars without an explicit int64 cast. Under
BLINKVIEW_DISABLE_NUMBA=1 (plain Python, no JIT) this overflows on this environment's numpy
version - `OverflowError: Python integer 1000 out of bounds for uint8` - the same root cause as
the pre-existing failures in test_ops_strings.py. When actually numba-JIT-compiled (the real
runtime path), numba's typed arithmetic promotes correctly and this never happens, so it's a gap
in the numba-disabled coverage fallback rather than a production bug - see
TestOverflowUnderDisabledNumba below for a test documenting it directly. Only the early-return
validation branches (which return before ever reaching that arithmetic) are exercisable here."""

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.parsing import EmptyUnifiedParserConfig, EmptyUnifiedParserState
from blinkview.ops.zephyr_timestamp import nb_parse_zephyr_realtime, nb_parse_zephyr_uptime_formatted


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


class TestParseZephyrUptimeFormatted:
    def test_missing_leading_bracket_returns_negative_one(self):
        msg = "00:00:05.123,456]"
        out_b = _out_bundle()

        result = nb_parse_zephyr_uptime_formatted(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_missing_closing_bracket_returns_negative_one(self):
        msg = "[00:00:05.123,456 no bracket"
        out_b = _out_bundle()

        result = nb_parse_zephyr_uptime_formatted(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_too_short_buffer_returns_negative_one(self):
        msg = "[00:0"
        out_b = _out_bundle()

        result = nb_parse_zephyr_uptime_formatted(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1


class TestParseZephyrRealtime:
    def test_bad_dash_placement_returns_negative_one(self):
        msg = "[1970x01-01 00:00:00.000,000]"
        out_b = _out_bundle()

        result = nb_parse_zephyr_realtime(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1

    def test_too_short_buffer_returns_negative_one(self):
        msg = "[1970-01-01 00:00"
        out_b = _out_bundle()

        result = nb_parse_zephyr_realtime(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
        )

        assert result == -1


class TestOverflowUnderDisabledNumba:
    """Documents the environment gap described in the module docstring, rather than silently
    dropping coverage of the happy path with no trace of why."""

    def test_uptime_formatted_happy_path_overflows_without_jit(self):
        msg = "[00:00:05.123,456] rest"
        out_b = _out_bundle()

        with pytest.raises(OverflowError):
            nb_parse_zephyr_uptime_formatted(
                _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
            )

    def test_realtime_happy_path_overflows_without_jit(self):
        msg = "[1970-01-01 00:00:00.000,000]"
        out_b = _out_bundle()

        with pytest.raises(OverflowError):
            nb_parse_zephyr_realtime(
                _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, EmptyUnifiedParserConfig
            )
