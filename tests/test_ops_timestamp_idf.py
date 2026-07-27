# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.parsing import TS_PRECISION_S, EmptyUnifiedParserState, UnifiedParserConfig
from blinkview.ops.timestamp_idf import nb_parse_int_timestamp_idf_v1


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


def _unix_seconds_config():
    return UnifiedParserConfig(timestamp_precision=TS_PRECISION_S, timestamp_unix=True)


class TestParseIntTimestampIdfV1:
    def test_parses_parenthesized_seconds_and_skips_trailing_whitespace(self):
        msg = "(1234567890) rest"
        out_b = _out_bundle()

        next_cursor = nb_parse_int_timestamp_idf_v1(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, _unix_seconds_config()
        )

        assert int(out_b.timestamps[0]) == 1_234_567_890 * 1_000_000_000
        assert bytes(_buf(msg)[next_cursor:]) == b"rest"

    def test_missing_opening_paren_returns_negative_one(self):
        msg = "1234567890) rest"
        out_b = _out_bundle()

        result = nb_parse_int_timestamp_idf_v1(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, _unix_seconds_config()
        )

        assert result == -1

    def test_missing_closing_paren_returns_negative_one(self):
        msg = "(1234567890 rest"
        out_b = _out_bundle()

        result = nb_parse_int_timestamp_idf_v1(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, _unix_seconds_config()
        )

        assert result == -1

    def test_no_digits_inside_parens_returns_negative_one(self):
        msg = "() rest"
        out_b = _out_bundle()

        result = nb_parse_int_timestamp_idf_v1(
            _buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, _unix_seconds_config()
        )

        assert result == -1

    def test_empty_buffer_returns_negative_one(self):
        out_b = _out_bundle()

        result = nb_parse_int_timestamp_idf_v1(
            _buf(""), 0, 0, out_b, 0, EmptyUnifiedParserState, _unix_seconds_config()
        )

        assert result == -1
