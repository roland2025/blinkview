# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from blinkview.core.types.parsing import CodecID, EmptyUnifiedParserConfig, EmptyUnifiedParserState, ParserID
from blinkview.ops.codec_adb_long import nb_decode_adb_long_frame
from blinkview.parsers.adb_decoder import AdbDecoder, AdbLongTimestamp, AdbModuleName, AdbPidTid, LevelMap
from blinkview.utils.log_level import LogLevel


def configure(instance, **overrides):
    """Mirrors BaseFactory.build(): hydrate the schema defaults into the config, then apply -
    these classes' __init__ methods don't call super().__init__(), so plain construction never
    hydrates schema defaults on its own (same pattern as test_frame_decoders.py/
    test_frame_parsers.py)."""
    hydrated = instance.hydrate_config(overrides)
    instance.apply_config(hydrated)
    return instance


class TestAdbDecoder:
    def test_defaults_override_delimiter_and_max_length(self):
        decoder = configure(AdbDecoder())
        bundle = decoder.bundle()

        assert bundle.delimiter == 10  # CHAR_LF
        assert bundle.length_max == 32 * 1024

    def test_uses_the_adb_long_codec_id_and_kernel(self):
        decoder = AdbDecoder()
        assert decoder.codec_id == CodecID.ADB_LONG
        assert decoder.decode is nb_decode_adb_long_frame


class TestAdbModuleName:
    def test_bundle_returns_expected_parser_id_and_config(self, id_registry):
        parser = AdbModuleName()
        device = id_registry.get_device("adb_module_test")
        parser.local = SimpleNamespace(device_id=device)

        parser_id, state, config = parser.bundle()

        assert parser_id == ParserID.MOD_ADB_LONG
        assert state is parser.tracker_state
        assert config.module_config.max_length == 128
        assert config.module_config.max_depth == 3
        assert config.string_table is device.modules_table.bundle()


class TestAdbLongTimestamp:
    def test_bundle_returns_expected_parser_id_and_empty_config(self, id_registry):
        parser = AdbLongTimestamp()
        parser.local = SimpleNamespace(device_id=id_registry.get_device("adb_ts_test"), sync_state=None)
        configure(parser)

        parser_id, state, config = parser.bundle()

        assert parser_id == ParserID.TS_ADB_LONG
        assert state is parser.state
        assert config is EmptyUnifiedParserConfig


class TestAdbPidTid:
    def test_bundle_returns_expected_parser_id_and_empty_state_config(self):
        parser = AdbPidTid()

        parser_id, state, config = parser.bundle()

        assert parser_id == ParserID.PID_TID_ADB_LONG
        assert state is EmptyUnifiedParserState
        assert config is EmptyUnifiedParserConfig


class TestLevelMap:
    def test_bundle_returns_expected_parser_id(self):
        level_map = LevelMap()

        parser_id, state, config = level_map.bundle()

        assert parser_id == ParserID.LEVEL_MAP_ADB_LONG
        assert state is EmptyUnifiedParserState

        level_map.release()

    def test_registers_all_adb_level_codes_with_correct_values(self):
        level_map = LevelMap()
        table = level_map._table

        expected = {
            "V": LogLevel.TRACE.value,
            "D": LogLevel.DEBUG.value,
            "I": LogLevel.INFO.value,
            "W": LogLevel.WARN.value,
            "E": LogLevel.ERROR.value,
            "F": LogLevel.FATAL.value,
            "S": LogLevel.OFF.value,
        }

        for i, (code, value) in enumerate(expected.items()):
            assert table.get_string(i) == code
            assert table._values[i] == value

        level_map.release()

    def test_release_clears_table_and_bundle(self):
        level_map = LevelMap()

        level_map.release()

        assert level_map._table is None
        assert level_map._bundle is None

    def test_release_is_idempotent(self):
        level_map = LevelMap()
        level_map.release()
        level_map.release()  # must not raise
