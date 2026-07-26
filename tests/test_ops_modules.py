# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.modules import DynamicWidthConfig, MODULE_ID_FULL, ModuleTrackerState
from blinkview.core.types.parsing import UnifiedParserConfig
from blinkview.ops.modules import (
    nb_normalize_name_inplace,
    nb_parse_fixed_width_name,
    nb_parse_module_tags_statemachine,
)


def _buf(msg, pad_to=None):
    b = bytearray(msg.encode("ascii"))
    if pad_to is not None:
        b.extend(b"\x00" * (pad_to - len(b)))
    return np.frombuffer(bytes(b), dtype=dtypes.BYTE).copy()  # writable - normalize_name_inplace mutates in place


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
        has_modules=True,
        has_devices=False,
        has_sequences=False,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


class TestNormalizeNameInplace:
    def test_uppercase_becomes_lowercase(self):
        buf = _buf("WiFi")
        n = nb_normalize_name_inplace(buf, 0, 4)
        assert bytes(buf[:n]).decode() == "wifi"

    def test_dots_are_preserved_singly(self):
        buf = _buf("net.wifi")
        n = nb_normalize_name_inplace(buf, 0, 8)
        assert bytes(buf[:n]).decode() == "net.wifi"

    def test_duplicate_dots_are_squashed(self):
        buf = _buf("net..wifi")
        n = nb_normalize_name_inplace(buf, 0, 9)
        assert bytes(buf[:n]).decode() == "net.wifi"

    def test_other_characters_become_underscores(self):
        buf = _buf("net wifi")
        n = nb_normalize_name_inplace(buf, 0, 8)
        assert bytes(buf[:n]).decode() == "net_wifi"

    def test_duplicate_separators_are_squashed(self):
        buf = _buf("net   wifi")
        n = nb_normalize_name_inplace(buf, 0, 10)
        assert bytes(buf[:n]).decode() == "net_wifi"

    def test_leading_and_trailing_separators_are_stripped(self):
        buf = _buf("  wifi  ", pad_to=8)
        n = nb_normalize_name_inplace(buf, 0, 8)
        assert bytes(buf[:n]).decode() == "wifi"

    def test_digits_are_preserved(self):
        buf = _buf("mod123")
        n = nb_normalize_name_inplace(buf, 0, 6)
        assert bytes(buf[:n]).decode() == "mod123"

    def test_operates_at_an_offset_within_a_larger_buffer(self):
        buf = _buf("XXWiFiYY")
        n = nb_normalize_name_inplace(buf, 2, 4)
        assert bytes(buf[2 : 2 + n]).decode() == "wifi"
        assert bytes(buf[:2]).decode() == "XX"  # untouched prefix


class TestParseFixedWidthName:
    def test_parses_a_simple_name_padded_with_spaces(self):
        msg = "wifi    rest"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=DynamicWidthConfig(max_length=8), string_table=table)
        from blinkview.core.types.parsing import UnifiedParserState

        state = UnifiedParserState(modules=tracker)

        next_cursor = nb_parse_fixed_width_name(buf, 0, len(msg), out_b, 0, state, config)

        assert next_cursor == 8
        assert out_b.modules[0] != 0

    def test_zero_width_field_returns_start_cursor(self):
        msg = "x"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=DynamicWidthConfig(max_length=0), string_table=table)
        from blinkview.core.types.parsing import UnifiedParserState

        state = UnifiedParserState(modules=tracker)

        result = nb_parse_fixed_width_name(buf, 0, 1, out_b, 0, state, config)

        assert result == 0

    def test_all_whitespace_field_returns_negative_one(self):
        msg = "     "
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=DynamicWidthConfig(max_length=5), string_table=table)
        from blinkview.core.types.parsing import UnifiedParserState

        state = UnifiedParserState(modules=tracker)

        result = nb_parse_fixed_width_name(buf, 0, 5, out_b, 0, state, config)

        assert result == -1

    def test_tracker_capacity_exhausted_returns_negative_one(self):
        msg = "wifi"
        buf = _buf(msg)
        out_b = _out_bundle()
        tiny_name_bytes = np.zeros(2, dtype=dtypes.BYTE)  # too small to hold "wifi"
        tracker = _tracker(name_bytes=tiny_name_bytes)
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=DynamicWidthConfig(max_length=4), string_table=table)
        from blinkview.core.types.parsing import UnifiedParserState

        state = UnifiedParserState(modules=tracker)

        result = nb_parse_fixed_width_name(buf, 0, 4, out_b, 0, state, config)

        assert result == -1


class TestParseModuleTagsStatemachine:
    def _config(self, **overrides):
        defaults = dict(max_length=64, max_depth=4, enable_brackets=True, enable_dot_separator=True)
        defaults.update(overrides)
        return DynamicWidthConfig(**defaults)

    def _state(self, tracker):
        from blinkview.core.types.parsing import UnifiedParserState

        return UnifiedParserState(modules=tracker)

    def test_single_bracketed_tag(self):
        msg = "[wifi] rest of message"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=self._config(), string_table=table)

        next_cursor = nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        assert out_b.modules[0] != 0
        assert buf[next_cursor : next_cursor + 4].tobytes() == b"rest"

    def test_word_ending_in_colon(self):
        msg = "wifi: connected"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=self._config(), string_table=table)

        nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        assert out_b.modules[0] != 0

    def test_chained_bracketed_tags_are_joined_with_dots(self):
        msg = "[net][wifi] connected"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=self._config(), string_table=table)

        nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        mod_id = out_b.modules[0]
        assert mod_id != 0
        start, length = tracker.starts[0], tracker.lengths[0]
        assert bytes(tracker.name_bytes[start : start + length]).decode() == "net.wifi"

    def test_no_tag_found_returns_negative_one(self):
        msg = "no tags here"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=self._config(), string_table=table)

        result = nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        assert result == -1

    def test_unclosed_bracket_returns_negative_one(self):
        msg = "[wifi unclosed"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=self._config(), string_table=table)

        result = nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        assert result == -1

    def test_exceeding_max_depth_returns_negative_one(self):
        msg = "[a][b][c] rest"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        config = UnifiedParserConfig(module_config=self._config(max_depth=2), string_table=table)

        result = nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        assert result == -1

    def test_prefix_removal(self):
        msg = "Elixir.MyApp.Wifi: connected"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        prefix = np.frombuffer(b"Elixir.", dtype=dtypes.BYTE)
        config = UnifiedParserConfig(
            module_config=self._config(prefix_bytes=prefix, prefix_remove=True), string_table=table
        )

        nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        mod_id = out_b.modules[0]
        assert mod_id != 0
        start, length = tracker.starts[0], tracker.lengths[0]
        assert bytes(tracker.name_bytes[start : start + length]).decode() == "myapp.wifi"

    def test_prefix_match_required_but_missing_returns_negative_one(self):
        msg = "OtherPrefix.Wifi: connected"
        buf = _buf(msg)
        out_b = _out_bundle()
        tracker = _tracker()
        table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
        prefix = np.frombuffer(b"Elixir.", dtype=dtypes.BYTE)
        config = UnifiedParserConfig(
            module_config=self._config(prefix_bytes=prefix, prefix_match=True), string_table=table
        )

        result = nb_parse_module_tags_statemachine(buf, 0, len(msg), out_b, 0, self._state(tracker), config)

        assert result == -1
