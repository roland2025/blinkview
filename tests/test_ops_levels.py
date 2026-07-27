# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.parsing import EmptyUnifiedParserState, UnifiedParserConfig
from blinkview.ops.levels import nb_parse_log_level


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
        has_modules=False,
        has_devices=False,
        has_sequences=False,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def _levels_config():
    # Sequential identity_ids (0, 1, ...) matching real level-table usage - nb_parse_log_level
    # iterates range(count) treating that as a direct index into offsets/lens/values.
    table = IndexedStringTable(initial_capacity=4, use_hashes=False, values_dtype=np.uint8)
    table.register_name(0, "INFO", value=1)
    table.register_name(1, "WARN", value=2)
    return UnifiedParserConfig(string_table=table.bundle())


class TestParseLogLevel:
    def test_matches_first_registered_level_and_skips_trailing_whitespace(self):
        msg = "INFO hello"
        out_b = _out_bundle()
        config = _levels_config()

        next_cursor = nb_parse_log_level(_buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, config)

        assert out_b.levels[0] == 1
        assert bytes(_buf(msg)[next_cursor:]) == b"hello"

    def test_matches_second_registered_level(self):
        msg = "WARN uh-oh"
        out_b = _out_bundle()
        config = _levels_config()

        nb_parse_log_level(_buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, config)

        assert out_b.levels[0] == 2

    def test_unmatched_text_returns_negative_one(self):
        msg = "XXXX hello"
        out_b = _out_bundle()
        config = _levels_config()

        result = nb_parse_log_level(_buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, config)

        assert result == -1

    def test_empty_buffer_returns_negative_one(self):
        out_b = _out_bundle()
        config = _levels_config()

        result = nb_parse_log_level(_buf(""), 0, 0, out_b, 0, EmptyUnifiedParserState, config)

        assert result == -1

    def test_prefix_is_not_a_false_match(self):
        # "INFOMAN" must not match "INFO" - the trailing alnum char blocks the prefix match, and
        # since there's no third registered name it exhausts the table and returns -1.
        msg = "INFOMAN hello"
        out_b = _out_bundle()
        config = _levels_config()

        result = nb_parse_log_level(_buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, config)

        assert result == -1

    def test_exact_length_match_at_end_of_buffer_with_no_trailing_char(self):
        msg = "INFO"
        out_b = _out_bundle()
        config = _levels_config()

        next_cursor = nb_parse_log_level(_buf(msg), 0, len(msg), out_b, 0, EmptyUnifiedParserState, config)

        assert out_b.levels[0] == 1
        assert next_cursor == len(msg)
