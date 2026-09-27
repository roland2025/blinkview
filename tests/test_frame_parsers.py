# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.empty import EMPTY_BYTES_RO
from blinkview.core.types.modules import MODULE_TEMP_ID_BASE
from blinkview.core.types.parsing import TS_PRECISION_MS, TS_PRECISION_S, UnusedSyncState, create_default_sync
from blinkview.parsers.frame_parsers import (
    Esp32V1IntegerTimestampParser,
    FixedWidthModuleNameParser,
    FrameSectionParserFactory,
    GenericFrameParser,
    IntegerTimestampParser,
    Iso8601DesktopTimestampParser,
    ModuleNameNormalizer,
    SkipWordsParser,
    SyslogTimestampParser,
    ZephyrRealTimeParser,
    ZephyrUptimeFormattedParser,
)
from blinkview.utils.log_level import LogLevel


def configure(instance, **overrides):
    """Mirrors BaseFactory.build(): hydrate the schema defaults into the config, then apply -
    these classes' __init__ methods don't call super().__init__(), so plain construction never
    hydrates schema defaults on its own."""
    hydrated = instance.hydrate_config(overrides)
    instance.apply_config(hydrated)
    return instance


def make_shared(id_registry):
    registry = FactoryRegistry()
    registry.register("frame_section_parser", FrameSectionParserFactory)
    return SimpleNamespace(factories=registry, id_registry=id_registry)


def make_generic_parser(id_registry, device_name="frame_parser_test", **overrides):
    device = id_registry.get_device(device_name)
    parser = GenericFrameParser()
    parser.shared = make_shared(id_registry)
    parser.local = SimpleNamespace(device_id=device, sync_state=create_default_sync(0))
    configure(parser, **overrides)
    return parser, device


class TestGenericFrameParserPipeline:
    def test_builds_pipeline_steps_via_the_real_factory(self, id_registry):
        parser, _device = make_generic_parser(id_registry, steps=[{"type": "skip_words", "count": 3}])

        assert len(parser.pipeline) == 1
        assert isinstance(parser.pipeline[0], SkipWordsParser)
        assert parser.pipeline[0].count == 3

    def test_bundle_config_resolves_module_and_device_ids(self, id_registry):
        parser, device = make_generic_parser(id_registry, filter_squash_spaces=True, parser_errors_hidden=True)

        config = parser.bundle()

        assert config.level_default == LogLevel.INFO.value
        assert config.level_error == LogLevel.ERROR.value
        assert config.module_log == device.get_module("log").id
        assert config.module_unknown == device.get_module("unknown").id
        assert config.device_id == device.id
        assert config.report_error is False  # parser_errors_hidden=True
        assert config.filter_squash_spaces is True

    def test_no_post_process_steps_defaults_to_the_noop(self, id_registry):
        parser, _device = make_generic_parser(id_registry, steps=[{"type": "skip_words", "count": 1}])

        parser.bundle()

        # Bound method objects aren't `is`-identical across separate attribute accesses even
        # when they wrap the same function+instance - compare with == instead.
        assert parser.post_process == parser.no_post_process
        assert parser.post_process("anything") is False

    def test_single_module_name_step_uses_its_post_process_directly(self, id_registry):
        parser, _device = make_generic_parser(id_registry, steps=[{"type": "module_name_fixed_width", "max_length": 8}])

        parser.bundle()

        assert parser.post_process == parser.pipeline[0].post_process

    def test_two_module_name_steps_combine_with_or(self, id_registry):
        parser, _device = make_generic_parser(
            id_registry,
            steps=[
                {"type": "module_name_fixed_width", "max_length": 8},
                {"type": "module_name_normalizer"},
            ],
        )
        parser.bundle()

        calls = []
        parser.pipeline[0].post_process = lambda batch: calls.append("a") or False
        parser.pipeline[1].post_process = lambda batch: calls.append("b") or True

        # Re-bundle so the combined pp_2 closure captures the replaced post_process functions.
        parser.bundle()
        result = parser.post_process("batch")

        assert calls == ["a", "b"]
        assert result is True


class TestModuleNameParserBasePostProcess:
    def test_resolves_temp_ids_and_resets_tracker_state(self, id_registry):
        parser = FixedWidthModuleNameParser()
        device = id_registry.get_device("post_process_test")
        parser.shared = make_shared(id_registry)
        parser.local = SimpleNamespace(device_id=device, sync_state=create_default_sync(0))
        configure(parser, max_length=8)

        state = parser.tracker_state
        name = b"wifi"
        state.name_bytes[: len(name)] = np.frombuffer(name, dtype=np.uint8)
        state.starts[0] = 0
        state.lengths[0] = len(name)
        state.count[0] = 1

        pool = NumpyArrayPool()
        batch = pool.create(PooledLogBatch, 4, 64, has_modules=True)
        batch.insert(100, 100, b"x", module=MODULE_TEMP_ID_BASE + 0)

        registry_count_before = id_registry.modules_table.count
        changed = parser.post_process(batch)

        expected_id = device.get_module("wifi").id
        assert int(batch.bundle.modules[0]) == expected_id
        assert state.count[0] == 0
        assert state.bytes_cursor[0] == 0
        assert changed == (id_registry.modules_table.count > registry_count_before)

        batch.release()

    def test_returns_false_and_is_a_noop_when_nothing_is_unresolved(self, id_registry):
        parser = FixedWidthModuleNameParser()
        device = id_registry.get_device("post_process_noop_test")
        parser.shared = make_shared(id_registry)
        parser.local = SimpleNamespace(device_id=device, sync_state=create_default_sync(0))
        configure(parser, max_length=8)

        pool = NumpyArrayPool()
        batch = pool.create(PooledLogBatch, 4, 64, has_modules=True)
        batch.insert(100, 100, b"x", module=0)

        assert parser.post_process(batch) is False

        batch.release()


class TestModuleNameNormalizerConfig:
    def test_empty_prefix_uses_the_shared_readonly_empty_buffer(self, id_registry):
        parser = ModuleNameNormalizer()
        device = id_registry.get_device("normalizer_test")
        parser.shared = make_shared(id_registry)
        parser.local = SimpleNamespace(device_id=device, sync_state=create_default_sync(0))
        configure(parser, prefix="")

        assert parser.module_config.prefix_bytes is EMPTY_BYTES_RO

    def test_configured_prefix_is_encoded_to_bytes(self, id_registry):
        parser = ModuleNameNormalizer()
        device = id_registry.get_device("normalizer_prefix_test")
        parser.shared = make_shared(id_registry)
        parser.local = SimpleNamespace(device_id=device, sync_state=create_default_sync(0))
        configure(parser, prefix="app_")

        assert bytes(parser.module_config.prefix_bytes) == b"app_"


class TestTimestampParsers:
    def _configured(self, cls, id_registry, **overrides):
        parser = cls()
        parser.local = SimpleNamespace(device_id=id_registry.get_device("ts_test"), sync_state=create_default_sync(0))
        configure(parser, **overrides)
        return parser

    def test_integer_timestamp_parser_config(self, id_registry):
        parser = self._configured(IntegerTimestampParser, id_registry, precision=TS_PRECISION_S, unix_timestamp=True)

        assert parser._precision == TS_PRECISION_S
        assert parser._unix is True

    def test_integer_timestamp_parser_defaults(self, id_registry):
        parser = self._configured(IntegerTimestampParser, id_registry)

        assert parser._precision == TS_PRECISION_MS
        assert parser._unix is False

    def test_esp32_v1_parser_never_treats_timestamps_as_unix(self, id_registry):
        parser = self._configured(
            Esp32V1IntegerTimestampParser, id_registry, precision=TS_PRECISION_S, unix_timestamp=True
        )

        assert parser._precision == TS_PRECISION_S
        assert parser._unix is False

    @pytest.mark.parametrize(
        "cls",
        [
            IntegerTimestampParser,
            Esp32V1IntegerTimestampParser,
            ZephyrUptimeFormattedParser,
            ZephyrRealTimeParser,
            Iso8601DesktopTimestampParser,
            SyslogTimestampParser,
        ],
    )
    def test_timestamp_parsers_use_the_device_sync_state(self, cls, id_registry):
        parser = self._configured(cls, id_registry)

        assert parser.sync_state is parser.local.sync_state

    def test_timestamp_parser_without_a_sync_state_falls_back_to_unused_sync(self, id_registry):
        parser = IntegerTimestampParser()
        parser.local = SimpleNamespace(device_id=id_registry.get_device("ts_no_sync_test"))
        configure(parser)

        assert parser.sync_state is UnusedSyncState

    def test_syslog_parser_defaults_to_current_year(self, id_registry):
        parser = self._configured(SyslogTimestampParser, id_registry)

        from datetime import datetime

        assert parser._year == datetime.now().year

    def test_syslog_parser_uses_configured_year(self, id_registry):
        parser = self._configured(SyslogTimestampParser, id_registry, year=1999)

        assert parser._year == 1999
