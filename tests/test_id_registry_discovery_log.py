# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""IDRegistry.discovery_log/dump_discovery_log/replay_discovery_log - lets a fresh IDRegistry
reproduce bit-identical device/module ids from a previous run's recorded creation order. Exists
so cold-storage segment files persisted across app restarts (cold_storage_persist_on_close) -
which only carry numeric device_id/module_id columns - can have their names restored on a later
run without re-parsing/re-ingesting the original source (see Registry._dump_id_registry/
_restore_id_registry_if_resumed)."""

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry import IDRegistry


def make_registry():
    return IDRegistry(NumpyArrayPool())


class TestDiscoveryLog:
    def test_get_device_appends_a_device_event_only_on_first_creation(self):
        reg = make_registry()
        reg.get_device("client")
        reg.get_device("client")  # cache hit - must not append again
        reg.get_device("server")

        device_events = [e for e in reg.discovery_log if e[0] == "device"]
        assert device_events == [("device", "client"), ("device", "server")]

    def test_get_module_appends_one_event_per_newly_created_path_segment(self):
        reg = make_registry()
        device = reg.get_device("client")
        device.get_module("ui.generator.rng")

        module_events = [e for e in reg.discovery_log if e[0] == "module" and e[1] == "client"]
        # Root module ("") from get_device's implicit DeviceIdentity construction, then each
        # dotted segment in walk order.
        assert module_events == [
            ("module", "client", ""),
            ("module", "client", "ui"),
            ("module", "client", "ui.generator"),
            ("module", "client", "ui.generator.rng"),
        ]

    def test_repeat_module_lookup_does_not_append_new_events(self):
        reg = make_registry()
        device = reg.get_device("client")
        device.get_module("ui.generator")
        before = list(reg.discovery_log)

        device.get_module("ui.generator")  # fully cached - no new segments

        assert reg.discovery_log == before

    def test_partial_overlap_only_logs_the_newly_created_segments(self):
        reg = make_registry()
        device = reg.get_device("client")
        device.get_module("ui.generator.rng")
        before = list(reg.discovery_log)

        device.get_module("ui.keys")  # "ui" already exists - only "ui.keys" is new

        new_events = reg.discovery_log[len(before) :]
        assert new_events == [("module", "client", "ui.keys")]


class TestDumpAndReplay:
    def test_dump_is_json_serializable(self):
        import json

        reg = make_registry()
        reg.get_device("client").get_module("ui.generator.rng")

        dumped = reg.dump_discovery_log()
        # Must round-trip through JSON (str keys/values, lists not tuples) - this is exactly the
        # shape Registry._dump_id_registry writes to disk.
        round_tripped = json.loads(json.dumps(dumped))
        assert round_tripped == dumped
        assert all(isinstance(e, list) for e in dumped)

    def test_replay_reproduces_identical_ids_for_interleaved_devices(self):
        """The real-world shape: two devices' module trees discovered in an interleaved order,
        not one device fully then the other - exercises that the log's global (not per-device)
        chronological order is what gets replayed."""
        reg = make_registry()
        dev_a = reg.get_device("client")
        dev_b = reg.get_device("server")
        dev_a.get_module("ui.generator.rng")
        dev_b.get_module("server.generator.rng")
        dev_a.get_module("ui.keys.remote")
        dev_b.get_module("server.keys")

        log = reg.dump_discovery_log()

        replayed = make_registry()
        replayed.replay_discovery_log(log)

        for device in (dev_a, dev_b):
            replayed_device = replayed.get_device(device.name)
            assert replayed_device.id == device.id
            for path, module in device.path_lookup.items():
                if path == "":
                    continue
                assert replayed_device.get_module(path).id == module.id

    def test_replay_does_not_duplicate_or_shift_ids_when_run_twice(self):
        """replay_discovery_log calls get_device/get_module, which are already idempotent
        (cache hit on repeat) - replaying the same log twice against the same registry must be a
        no-op the second time, not double-register anything."""
        reg = make_registry()
        reg.get_device("client").get_module("ui.generator.rng")
        log = reg.dump_discovery_log()

        replayed = make_registry()
        replayed.replay_discovery_log(log)
        first_pass_log = list(replayed.discovery_log)

        replayed.replay_discovery_log(log)

        assert replayed.discovery_log == first_pass_log

    def test_replay_onto_a_registry_with_preexisting_unrelated_devices_still_works(self):
        """Mirrors the real Registry.__init__ shape - a SYSTEM device (and its own modules) is
        always registered before configure_system() (and thus any restore) ever runs, so
        replayed ids won't start at 0 in practice. Confirms replay still resolves to internally
        self-consistent, distinct ids even when it isn't starting from a truly empty registry."""
        reg = make_registry()
        reg.get_device("client").get_module("ui.generator.rng")
        log = reg.dump_discovery_log()

        replayed = make_registry()
        replayed.get_device("system").get_module("some.pre.existing.thing")

        replayed.replay_discovery_log(log)

        device = replayed.get_device("client")
        module = device.get_module("ui.generator.rng")
        assert device.name == "client"
        assert module.name == "ui.generator.rng"
        assert device.id != replayed.get_device("system").id
