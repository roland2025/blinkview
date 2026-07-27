# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from blinkview.core.logger import PrintLogger
from blinkview.core.pipeline_manager import PipelineManager


class FakeConfigSub:
    def __init__(self):
        self.subscribed = []
        self.unsubscribed = []

    def subscribe(self, path, item):
        self.subscribed.append((path, item))

    def unsubscribe(self, path, item):
        self.unsubscribed.append((path, item))


class FakeTarget:
    """Stand-in for a reorder/central/reference-target component."""

    def __init__(self, name="target"):
        self.name = name
        self.subscribed = []
        self.unsubscribed = []
        self.enabled = True

    def subscribe(self, ref):
        self.subscribed.append(ref)

    def unsubscribe(self, ref):
        self.unsubscribed.append(ref)


class FakeRegistry:
    def __init__(self):
        self.config = FakeConfigSub()
        self.reorder = None
        self.central = FakeTarget("central")
        self._targets = {}

    def logger_creator(self, category, name):
        return lambda: PrintLogger(f"test.{category}.{name}")

    def get_reference_target(self, ref_id):
        return self._targets.get(ref_id)


class FakePipeline:
    def __init__(self, config, reference_id=None):
        self.config = config
        self.reference_id = reference_id
        self.enabled = config.get("enabled", False)
        self.sources_ = config.get("sources_", [])
        self.targets_ = config.get("targets_", [])
        self.started = False
        self.stopped = False
        self.cleared_links = False
        self.restarted = False
        self.thread_needs_restart = False
        self.subscribed = []
        self.unsubscribed = []
        self.reader = None
        self.parser = None
        self.applied_configs = []

    def start(self):
        # Mirrors BaseDaemon.start(): a real daemon no-ops when not enabled.
        if self.enabled:
            self.started = True

    def stop(self):
        self.stopped = True

    def unsubscribe(self, ref):
        self.unsubscribed.append(ref)

    def clear_all_links(self):
        self.cleared_links = True

    def restart(self):
        self.restarted = True

    def subscribe(self, ref):
        self.subscribed.append(ref)

    def apply_config(self, config):
        self.applied_configs.append(config)
        old_sources, old_targets = set(self.sources_), set(self.targets_)
        self.sources_ = config.get("sources_", self.sources_)
        self.targets_ = config.get("targets_", self.targets_)
        self.enabled = config.get("enabled", self.enabled)
        return set(self.sources_) != old_sources or set(self.targets_) != old_targets

    def get_config_schema(self):
        return {"schema": "for-" + str(self.reference_id)}


class FakeFactories:
    def __init__(self):
        self.built = []
        self.next_pipeline = None

    def build(self, category, config, shared, local_ctx):
        self.built.append((category, config, shared, local_ctx))
        pipeline = self.next_pipeline or FakePipeline(config)
        self.next_pipeline = None
        return pipeline

    def get_base_schema(self, category):
        return {"base_schema_for": category}


class FakeDevice:
    def __init__(self, name):
        self.name = name


class FakeIdRegistry:
    def get_device(self, name):
        return FakeDevice(name)


def make_manager():
    manager = PipelineManager()
    manager.logger = PrintLogger("test.pipeline_manager")
    registry = FakeRegistry()
    factories = FakeFactories()
    manager.shared = SimpleNamespace(registry=registry, factories=factories, id_registry=FakeIdRegistry())
    return manager, registry, factories


class TestDefaults:
    def test_starts_with_no_pipelines_and_needs_delayed_init(self):
        manager = PipelineManager()
        assert manager.pipelines == {}
        assert manager.needs_delayed_init is True


class TestApplyConfigCreatesPipelines:
    def test_first_apply_builds_pipeline_but_does_not_start_it(self):
        manager, registry, factories = make_manager()

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})

        assert "dev1" in manager.pipelines
        pipeline = manager.pipelines["dev1"]
        assert pipeline.started is False
        category, config, shared, local_ctx = factories.built[0]
        assert category == "parser"
        assert config == {"name": "dev1", "enabled": True}
        assert shared is manager.shared
        assert local_ctx.device_id.name == "dev1"

    def test_first_apply_clears_needs_delayed_init(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})
        assert manager.needs_delayed_init is False

    def test_new_pipeline_gets_reference_id_and_registers_for_config_updates(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})

        pipeline = manager.pipelines["dev1"]
        assert pipeline.reference_id == "dev1"
        assert (f"/pipelines/dev1", pipeline) in registry.config.subscribed

    def test_second_apply_starts_newly_added_enabled_pipeline(self):
        manager, registry, factories = make_manager()
        manager.apply_config({})  # first call just clears needs_delayed_init

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})

        pipeline = manager.pipelines["dev1"]
        assert pipeline.started is True

    def test_second_apply_does_not_start_a_disabled_pipeline(self):
        manager, registry, factories = make_manager()
        manager.apply_config({})

        manager.apply_config({"dev1": {"name": "dev1", "enabled": False}})

        pipeline = manager.pipelines["dev1"]
        assert pipeline.started is False


class TestApplyConfigRemovesPipelines:
    def test_removed_pipeline_is_stopped_and_unlinked(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})
        pipeline = manager.pipelines["dev1"]

        manager.apply_config({})

        assert "dev1" not in manager.pipelines
        assert pipeline.stopped is True
        assert pipeline.cleared_links is True
        assert ("/pipelines/dev1", pipeline) in registry.config.unsubscribed


class TestApplyConfigUpdatesPipelines:
    def test_existing_pipeline_gets_apply_config_called(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "sources_": []}})
        pipeline = manager.pipelines["dev1"]

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "sources_": []}})

        assert pipeline.applied_configs[-1] == {"name": "dev1", "enabled": True, "sources_": []}

    def test_source_change_rewires_upstream_subscriptions(self):
        manager, registry, factories = make_manager()
        upstream_old = FakeTarget("old_source")
        upstream_new = FakeTarget("new_source")
        registry._targets["old_src"] = upstream_old
        registry._targets["new_src"] = upstream_new

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "sources_": ["old_src"]}})
        pipeline = manager.pipelines["dev1"]

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "sources_": ["new_src"]}})

        assert pipeline in upstream_old.unsubscribed
        assert pipeline in upstream_new.subscribed

    def test_target_change_rewires_downstream_subscriptions(self):
        manager, registry, factories = make_manager()
        downstream_old = FakeTarget("old_target")
        downstream_new = FakeTarget("new_target")
        registry._targets["old_tgt"] = downstream_old
        registry._targets["new_tgt"] = downstream_new

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "targets_": ["old_tgt"]}})
        pipeline = manager.pipelines["dev1"]

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "targets_": ["new_tgt"]}})

        assert downstream_old in pipeline.unsubscribed
        assert downstream_new in pipeline.subscribed

    def test_thread_needs_restart_triggers_restart_on_change(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "sources_": ["a"]}})
        pipeline = manager.pipelines["dev1"]
        pipeline.thread_needs_restart = True

        manager.apply_config({"dev1": {"name": "dev1", "enabled": True, "sources_": ["b"]}})

        assert pipeline.restarted is True

    def test_error_in_one_pipeline_does_not_block_others(self):
        manager, registry, factories = make_manager()
        manager.apply_config(
            {
                "dev1": {"name": "dev1", "enabled": True},
                "dev2": {"name": "dev2", "enabled": True},
            }
        )
        pipeline1 = manager.pipelines["dev1"]

        def boom(config):
            raise RuntimeError("kaboom")

        pipeline1.apply_config = boom

        # must not raise, and dev2 should still get processed normally
        manager.apply_config(
            {
                "dev1": {"name": "dev1", "enabled": True},
                "dev2": {"name": "dev2", "enabled": True, "sources_": ["x"]},
            }
        )
        pipeline2 = manager.pipelines["dev2"]
        assert pipeline2.applied_configs[-1] == {"name": "dev2", "enabled": True, "sources_": ["x"]}


class TestStartStop:
    def test_start_starts_every_pipeline(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})
        manager.pipelines["dev1"].started = False

        manager.start()

        assert manager.pipelines["dev1"].started is True

    def test_stop_stops_every_pipeline(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})

        manager.stop()

        assert manager.pipelines["dev1"].stopped is True


class TestApplyTarget:
    def test_skips_disabled_pipelines(self):
        manager, registry, factories = make_manager()
        pipeline = FakePipeline({"enabled": False})

        manager.apply_target("dev1", pipeline)

        assert pipeline.subscribed == []

    def test_subscribes_to_declared_sources_and_targets(self):
        manager, registry, factories = make_manager()
        source_ref = FakeTarget("source")
        target_ref = FakeTarget("target")
        registry._targets["src1"] = source_ref
        registry._targets["tgt1"] = target_ref

        pipeline = FakePipeline({"enabled": True})
        pipeline.sources_ = ["src1"]
        pipeline.targets_ = ["tgt1"]

        manager.apply_target("dev1", pipeline)

        assert pipeline in source_ref.subscribed
        assert target_ref in pipeline.subscribed


class TestGetSchemaAndGet:
    def test_get_schema_returns_the_pipelines_own_schema(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})

        assert manager.get_schema("dev1") == {"schema": "for-dev1"}

    def test_get_schema_falls_back_to_base_schema_for_unknown_pipeline(self):
        manager, registry, factories = make_manager()

        assert manager.get_schema("missing") == {"base_schema_for": "parser"}

    def test_get_returns_pipeline_by_id_or_none(self):
        manager, registry, factories = make_manager()
        manager.apply_config({"dev1": {"name": "dev1", "enabled": True}})

        assert manager.get("dev1") is manager.pipelines["dev1"]
        assert manager.get("missing") is None
