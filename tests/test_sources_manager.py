# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Direct unit coverage for SourcesManager's config-apply/topology-reconciliation logic
(additions, removals, source/target link diffing, restart-on-change, error isolation) -
previously only exercised incidentally through Registry.configure_system() in the playback
integration tests, which never varies config across two apply_config() calls and so never
touched the reconcile-on-change path at all."""

from types import SimpleNamespace

import pytest

from blinkview.core.sources import SourcesManager


class FakeLogger:
    def __init__(self):
        self.errors = []
        self.infos = []

    def warn(self, *a, **k):
        pass

    def debug(self, *a, **k):
        pass

    def info(self, msg):
        self.infos.append(msg)

    def error(self, msg, exc=None):
        self.errors.append((msg, exc))


class FakeItem:
    def __init__(self, enabled=True, sources_=None, targets_=None):
        self.enabled = enabled
        self.sources_ = sources_ if sources_ is not None else []
        self.targets_ = targets_ if targets_ is not None else []
        self.reference_id = None
        self.thread_needs_restart = False
        self.stop_calls = 0
        self.start_calls = 0
        self.restart_calls = 0
        self.clear_all_links_calls = 0
        self.subscribed = []
        self.unsubscribed = []
        self.apply_config_result = False
        self.apply_config_raises = None
        self.apply_config_calls = []

    def apply_config(self, config):
        self.apply_config_calls.append(config)
        if self.apply_config_raises is not None:
            raise self.apply_config_raises
        return self.apply_config_result

    def subscribe(self, target):
        self.subscribed.append(target)

    def unsubscribe(self, target):
        self.unsubscribed.append(target)

    def stop(self):
        self.stop_calls += 1

    def start(self):
        self.start_calls += 1

    def restart(self):
        self.restart_calls += 1

    def clear_all_links(self):
        self.clear_all_links_calls += 1

    def get_config_schema(self):
        return {"type": "fake-item"}


class FakeTarget:
    def __init__(self):
        self.subscribed_items = []
        self.unsubscribed_items = []

    def subscribe(self, item):
        self.subscribed_items.append(item)

    def unsubscribe(self, item):
        self.unsubscribed_items.append(item)


class FakeFactories:
    def __init__(self, item_factory=None):
        self._item_factory = item_factory or (lambda config: FakeItem())
        self.built = []

    def build(self, category, config, system_ctx, local_ctx, **kwargs):
        item = self._item_factory(config)
        self.built.append((category, config))
        return item

    def get_base_schema(self, category):
        return {"type": "base-schema", "category": category}


class FakeRegistry:
    def __init__(self):
        self.system_ctx = SimpleNamespace()
        self.reorder = None
        self.central = SimpleNamespace(put=lambda *a, **k: None)
        self.targets = {}

    def logger_creator(self, *args, **kwargs):
        return lambda: FakeLogger()

    def get_reference_target(self, ref_id):
        return self.targets.get(ref_id)


def make_manager(registry=None, factories=None):
    mgr = SourcesManager()
    registry = registry or FakeRegistry()
    factories = factories or FakeFactories()
    shared = SimpleNamespace(registry=registry, factories=factories)
    local = SimpleNamespace(get_logger=lambda: FakeLogger())
    mgr.bind_system(shared, local)
    return mgr, registry, factories


def test_apply_config_creates_and_registers_a_new_source():
    item = FakeItem()
    mgr, _, factories = make_manager(factories=FakeFactories(item_factory=lambda cfg: item))

    mgr.apply_config({"src1": {"type": "udp"}})

    assert mgr.sources["src1"] is item
    assert item.reference_id == "src1"
    assert factories.built == [("source", {"type": "udp"})]


def test_apply_config_defers_target_wiring_while_needs_delayed_init():
    registry = FakeRegistry()
    registry.targets["upstream"] = FakeTarget()
    item = FakeItem(sources_=["upstream"])
    mgr, _, _ = make_manager(registry=registry, factories=FakeFactories(item_factory=lambda cfg: item))
    assert mgr.needs_delayed_init is True

    mgr.apply_config({"src1": {"type": "udp"}})

    assert registry.targets["upstream"].subscribed_items == []


def test_apply_config_wires_targets_immediately_when_not_delayed_init_and_enabled():
    registry = FakeRegistry()
    registry.targets["upstream"] = FakeTarget()
    item = FakeItem(enabled=True, sources_=["upstream"])
    mgr, _, _ = make_manager(registry=registry, factories=FakeFactories(item_factory=lambda cfg: item))
    mgr.needs_delayed_init = False

    mgr.apply_config({"src1": {"type": "udp"}})

    assert registry.targets["upstream"].subscribed_items == [item]


def test_apply_config_removes_source_no_longer_present():
    item = FakeItem()
    mgr, _, _ = make_manager()
    mgr.sources["src1"] = item

    mgr.apply_config({})

    assert "src1" not in mgr.sources
    assert item.stop_calls == 1
    assert item.clear_all_links_calls == 1


def test_apply_config_calls_start_on_all_sources_from_the_second_call_onward():
    """needs_delayed_init is only cleared at the end of apply_config(), so the very first call
    never reaches self.start() (guarded by `if not self.needs_delayed_init`) - only the second
    and subsequent calls do."""
    item = FakeItem()
    mgr, _, _ = make_manager()
    mgr.sources["existing"] = item
    assert mgr.needs_delayed_init is True

    mgr.apply_config({"existing": {"type": "udp"}})
    assert item.start_calls == 0
    assert mgr.needs_delayed_init is False

    mgr.apply_config({"existing": {"type": "udp"}})
    assert item.start_calls == 1


def test_apply_config_skips_reconcile_when_item_config_is_unchanged():
    registry = FakeRegistry()
    registry.targets["upstream"] = FakeTarget()
    item = FakeItem(sources_=["upstream"])
    item.apply_config_result = False  # "nothing changed"
    mgr, _, _ = make_manager(registry=registry)
    mgr.sources["src1"] = item
    mgr.needs_delayed_init = False

    mgr.apply_config({"src1": {"type": "udp"}})

    assert registry.targets["upstream"].subscribed_items == []


def test_apply_config_reconciles_added_and_removed_source_links_on_change():
    registry = FakeRegistry()
    old_upstream = FakeTarget()
    new_upstream = FakeTarget()
    registry.targets["old"] = old_upstream
    registry.targets["new"] = new_upstream

    item = FakeItem(sources_=["old"])
    mgr, _, _ = make_manager(registry=registry)
    mgr.sources["src1"] = item
    mgr.needs_delayed_init = False

    def apply_config_and_switch(config):
        item.sources_ = ["new"]  # simulate the config swapping which source feeds this item
        return True

    item.apply_config = apply_config_and_switch

    mgr.apply_config({"src1": {"type": "udp"}})

    assert item in old_upstream.unsubscribed_items
    assert item in new_upstream.subscribed_items


def test_apply_config_reconciles_added_and_removed_target_links_on_change():
    registry = FakeRegistry()
    old_downstream = FakeTarget()
    new_downstream = FakeTarget()
    registry.targets["old"] = old_downstream
    registry.targets["new"] = new_downstream

    item = FakeItem(targets_=["old"])
    mgr, _, _ = make_manager(registry=registry)
    mgr.sources["src1"] = item
    mgr.needs_delayed_init = False

    def apply_config_and_switch(config):
        item.targets_ = ["new"]
        return True

    item.apply_config = apply_config_and_switch

    mgr.apply_config({"src1": {"type": "udp"}})

    assert old_downstream in item.unsubscribed
    assert new_downstream in item.subscribed


def test_apply_config_restarts_item_when_thread_needs_restart_flag_set():
    item = FakeItem()
    item.apply_config_result = True
    item.thread_needs_restart = True
    mgr, _, _ = make_manager()
    mgr.sources["src1"] = item
    mgr.needs_delayed_init = False

    mgr.apply_config({"src1": {"type": "udp"}})

    assert item.restart_calls == 1


def test_apply_config_catches_and_logs_exception_from_a_source():
    item = FakeItem()
    item.apply_config_raises = RuntimeError("boom")
    mgr, _, _ = make_manager()
    mgr.sources["src1"] = item
    mgr.needs_delayed_init = False

    mgr.apply_config({"src1": {"type": "udp"}})  # must not raise

    assert len(mgr.logger.errors) == 1
    assert "src1" in mgr.logger.errors[0][0]


def test_get_schema_returns_the_sources_own_schema_when_present():
    item = FakeItem()
    mgr, _, _ = make_manager()
    mgr.sources["src1"] = item

    assert mgr.get_schema("src1") == {"type": "fake-item"}


def test_get_schema_falls_back_to_base_schema_when_source_missing():
    mgr, _, factories = make_manager()

    schema = mgr.get_schema("missing")

    assert schema == {"type": "base-schema", "category": "source"}


def test_send_data_is_a_noop_when_source_missing():
    mgr, _, _ = make_manager()
    mgr.send_data("missing", "cmd")  # must not raise


def test_send_data_forwards_command_to_the_source():
    class SendableItem(FakeItem):
        def __init__(self):
            super().__init__()
            self.commands = []

        def send_data(self, command):
            self.commands.append(command)

    item = SendableItem()
    mgr, _, _ = make_manager()
    mgr.sources["src1"] = item

    mgr.send_data("src1", "reset")

    assert item.commands == ["reset"]


def test_apply_target_subscribes_source_and_target_links():
    registry = FakeRegistry()
    upstream = FakeTarget()
    downstream = FakeTarget()
    registry.targets["up"] = upstream
    registry.targets["down"] = downstream
    item = FakeItem(sources_=["up"], targets_=["down"])
    mgr, _, _ = make_manager(registry=registry)

    mgr.apply_target("src1", item)

    assert upstream.subscribed_items == [item]
    assert item.subscribed == [downstream]


def test_apply_targets_only_wires_enabled_sources():
    registry = FakeRegistry()
    upstream = FakeTarget()
    registry.targets["up"] = upstream
    enabled_item = FakeItem(enabled=True, sources_=["up"])
    disabled_item = FakeItem(enabled=False, sources_=["up"])
    mgr, _, _ = make_manager(registry=registry)
    mgr.sources["enabled"] = enabled_item
    mgr.sources["disabled"] = disabled_item

    mgr.apply_targets()

    assert upstream.subscribed_items == [enabled_item]
