# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from blinkview.core.types.parsing import CodecID, create_default_sync
from blinkview.ops.codec_adb_long import nb_decode_adb_long_frame
from blinkview.parsers.adb_decoder import AdbDecoder, AdbLongTimestamp, LevelMap
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

    def test_keeps_the_first_frame_by_default(self):
        """logcat output always starts on a line boundary, so there is nothing to resync past."""
        assert configure(AdbDecoder()).frame_resync_on_start is False

    def test_uses_the_adb_long_codec_id_and_kernel(self):
        decoder = AdbDecoder()
        assert decoder.codec_id == CodecID.ADB_LONG
        assert decoder.decode is nb_decode_adb_long_frame


class TestAdbLongTimestamp:
    def test_uses_the_device_sync_state(self, id_registry):
        sync = create_default_sync(0)
        parser = AdbLongTimestamp()
        parser.local = SimpleNamespace(device_id=id_registry.get_device("adb_ts_test"), sync_state=sync)
        configure(parser)

        assert parser.sync_state is sync


class TestLevelMap:
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

    def test_release_clears_table(self):
        level_map = LevelMap()

        level_map.release()

        assert level_map._table is None

    def test_release_is_idempotent(self):
        level_map = LevelMap()
        level_map.release()
        level_map.release()  # must not raise
