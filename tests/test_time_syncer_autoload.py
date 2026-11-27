# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""A pipeline runs a time syncer only when its `time_sync` setting is enabled. Without a `type`,
the syncer is the one its source declares (BaseReader.time_syncer_type), loaded when the pipeline
is bound to that source (BaseParser._load_time_syncer)."""

import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.time_sync_engine import TimeSyncEngine
from blinkview.core.types.parsing import create_default_sync
from blinkview.io.adb_reader import AdbReader
from blinkview.io.adb_time_syncer import AdbTimeSyncerSubscriber
from blinkview.io.BaseReader import BaseReader
from blinkview.io.serial_time_syncer import SerialTimeSyncerSubscriber
from blinkview.io.uart import UARTReader
from blinkview.ops.timesync import IDX_ASYM_RATIO
from blinkview.parsers.frame_decoders import FrameDecoderFactory
from blinkview.parsers.frame_parsers import FrameParserFactory, FrameSectionParserFactory
from blinkview.parsers.frame_pipeline_parser import FramePipelineParser
from blinkview.subscribers.subscriber import TimeSyncerFactory

AUTO = {"enabled": True}
BASE_CONFIG = {"frame_decoder": {"type": "line_decoder"}, "frame_parser": {"type": "default", "steps": []}}


class FakeReader(BaseDaemon):
    """Only what binding needs: subscribe() calls the pipeline's track_subscription()."""

    def __init__(self, time_syncer_type):
        super().__init__()
        self.time_syncer_type = time_syncer_type


def make_shared(id_registry):
    registry = FactoryRegistry()
    registry.register("frame_decoder", FrameDecoderFactory)
    registry.register("frame_parser", FrameParserFactory)
    registry.register("frame_section_parser", FrameSectionParserFactory)
    registry.register("time_sync", TimeSyncerFactory)
    return SimpleNamespace(
        array_pool=NumpyArrayPool(), time_ns=time.time_ns, factories=registry, id_registry=id_registry
    )


def make_pipeline(id_registry, time_sync=None):
    pipeline = FramePipelineParser()
    pipeline.logger = PrintLogger("test.time_syncer_autoload")
    pipeline.shared = make_shared(id_registry)
    pipeline.local = SimpleNamespace(device_id=id_registry.get_device("autoload_test"))
    config = dict(BASE_CONFIG)
    if time_sync is not None:
        config["time_sync"] = time_sync
    pipeline.apply_config(config)
    return pipeline


class TestReaderDeclarations:
    def test_adb_and_uart_readers_name_their_syncers(self):
        assert AdbReader.time_syncer_type == "adb_time_syncer"
        assert UARTReader.time_syncer_type == "serial_time_syncer"
        assert BaseReader.time_syncer_type is None


class TestEnabledWithoutType:
    @pytest.mark.parametrize(
        "syncer_type, syncer_cls",
        [("serial_time_syncer", SerialTimeSyncerSubscriber), ("adb_time_syncer", AdbTimeSyncerSubscriber)],
    )
    def test_binding_loads_the_sources_syncer(self, id_registry, syncer_type, syncer_cls):
        pipeline = make_pipeline(id_registry, time_sync=AUTO)
        assert pipeline.time_syncer is None  # not bound yet

        source = FakeReader(syncer_type)
        source.subscribe(pipeline)

        syncer = pipeline.time_syncer
        assert isinstance(syncer, syncer_cls)
        assert syncer.local.source is source
        assert syncer.local.parser is pipeline
        assert syncer in pipeline.subscribers
        assert syncer in pipeline._children
        assert syncer.enabled is True  # a disabled child would never start with the pipeline

    @pytest.mark.parametrize("syncer_type, asymmetry", [("serial_time_syncer", 500_000), ("adb_time_syncer", 930_000)])
    def test_syncer_gets_its_config_defaults(self, id_registry, syncer_type, asymmetry):
        pipeline = make_pipeline(id_registry, time_sync=AUTO)
        FakeReader(syncer_type).subscribe(pipeline)

        assert pipeline.time_syncer.asymmetry_ratio == asymmetry
        # Both anchor on the clock the rows are stamped with: logcat -v monotonic is the pong's m, not b
        assert pipeline.time_syncer.time_source_boot is False

    @pytest.mark.parametrize("syncer_type", ["serial_time_syncer", "adb_time_syncer"])
    def test_asymmetry_ratio_is_configurable(self, id_registry, syncer_type):
        pipeline = make_pipeline(id_registry, time_sync=AUTO)
        FakeReader(syncer_type).subscribe(pipeline)

        # a config change rebuilds the syncer for the source it is already bound to
        pipeline.apply_config({**BASE_CONFIG, "time_sync": {**AUTO, "asymmetry_ratio": 300_000}})
        assert pipeline.time_syncer.asymmetry_ratio == 300_000

    @pytest.mark.parametrize(
        "other_change",
        [{}, {"name": "renamed"}, {"frame_decoder": {"type": "line_decoder", "frame_length_minimum": 4}}],
        ids=["unchanged", "name", "frame_decoder"],
    )
    def test_config_without_a_time_sync_change_keeps_the_syncer(self, id_registry, other_change):
        """Regression: any /pipelines edit applies every pipeline's config, and each apply
        rebuilt the syncer, so every board lost its drift estimate on an unrelated edit."""
        pipeline = make_pipeline(id_registry, time_sync=AUTO)
        FakeReader("serial_time_syncer").subscribe(pipeline)
        syncer = pipeline.time_syncer

        pipeline.apply_config({**BASE_CONFIG, "time_sync": dict(AUTO), **other_change})

        assert pipeline.time_syncer is syncer
        assert syncer in pipeline.subscribers
        assert syncer in pipeline._children


class TestEngineAsymmetry:
    @pytest.mark.parametrize("value, stored", [(300_000, 300_000), (0, 1), (-5, 1), (2_000_000, 1_000_000)])
    def test_set_asymmetry_clamps_to_the_valid_range(self, value, stored):
        engine = TimeSyncEngine(create_default_sync(0))
        engine.set_asymmetry(value)
        assert int(engine.engine.scalars[IDX_ASYM_RATIO]) == stored

    def test_enabling_after_binding_loads_it(self, id_registry):
        pipeline = make_pipeline(id_registry)
        source = FakeReader("serial_time_syncer")
        source.subscribe(pipeline)
        assert pipeline.time_syncer is None

        pipeline.apply_config({**BASE_CONFIG, "time_sync": AUTO})

        assert isinstance(pipeline.time_syncer, SerialTimeSyncerSubscriber)
        assert pipeline.time_syncer.local.source is source

    def test_disabling_removes_it(self, id_registry):
        pipeline = make_pipeline(id_registry, time_sync=AUTO)
        FakeReader("serial_time_syncer").subscribe(pipeline)
        syncer = pipeline.time_syncer

        pipeline.apply_config({**BASE_CONFIG, "time_sync": {"enabled": False}})

        assert pipeline.time_syncer is None
        assert syncer not in pipeline.subscribers
        assert syncer not in pipeline._children

    def test_source_without_a_syncer(self, id_registry):
        pipeline = make_pipeline(id_registry, time_sync=AUTO)

        FakeReader(None).subscribe(pipeline)

        assert pipeline.time_syncer is None

    def test_first_source_with_a_syncer_keeps_it(self, id_registry):
        pipeline = make_pipeline(id_registry, time_sync=AUTO)
        FakeReader(None).subscribe(pipeline)
        first = FakeReader("serial_time_syncer")
        first.subscribe(pipeline)
        syncer = pipeline.time_syncer

        FakeReader("adb_time_syncer").subscribe(pipeline)

        assert pipeline.time_syncer is syncer
        assert syncer.local.source is first


class TestNotEnabled:
    @pytest.mark.parametrize("time_sync", [None, {}, {"enabled": False}, {"type": "serial_time_syncer"}])
    def test_no_syncer(self, id_registry, time_sync):
        pipeline = make_pipeline(id_registry, time_sync=time_sync)

        FakeReader("serial_time_syncer").subscribe(pipeline)

        assert pipeline.time_syncer is None


class TestExplicitType:
    def test_type_overrides_the_sources_syncer(self, id_registry):
        pipeline = make_pipeline(id_registry, time_sync={"enabled": True, "type": "serial_time_syncer"})
        explicit = pipeline.time_syncer
        assert isinstance(explicit, SerialTimeSyncerSubscriber)

        FakeReader("adb_time_syncer").subscribe(pipeline)

        assert pipeline.time_syncer is explicit

    def test_type_with_the_pipeline_already_bound_pings_through_that_source(self, id_registry):
        pipeline = make_pipeline(id_registry)
        source = FakeReader(None)
        source.subscribe(pipeline)

        pipeline.apply_config({**BASE_CONFIG, "time_sync": {"enabled": True, "type": "serial_time_syncer"}})

        assert pipeline.time_syncer.local.source is source
