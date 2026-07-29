# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Registry._dump_id_registry and IDRegistry's own construction-time rehydration (Registry's
replay_source_dir constructor kwarg flows into FileManager and IDRegistry before anything else
runs - see IDRegistry.__init__ -> _rehydrate_from_persisted_cold_storage) - the glue that
persists/restores IDRegistry.discovery_log alongside cold_storage_persist_on_close'd cold segment
files. Cold segment rows only carry numeric device_id/module_id columns; without this, a session
resumed from persisted cold storage would render every row with unknown/blank device and module
names, since a fresh IDRegistry starts empty.

IDRegistry.dump_discovery_log/replay_discovery_log's own id-reproduction correctness is exercised
directly in tests/test_id_registry_discovery_log.py - these tests are specifically about the
Registry-level wiring: where the JSON file gets written, and that construction-time rehydration
actually happens (and happens *before* configure_system(), not as a later step)."""

import json

from blinkview.core.registry import Registry
from tests.fakes.real_registry import make_real_registry


class TestDumpIdRegistry:
    def test_writes_discovery_log_as_json_next_to_cold_files(self, tmp_path):
        reg = make_real_registry(tmp_path, "dump_test")
        try:
            device = reg.id_registry.get_device("mydevice")
            device.get_module("sensor.temp")

            cold_dir = tmp_path / "cold_out"
            cold_dir.mkdir()
            reg._dump_id_registry(cold_dir)

            dumped = json.loads((cold_dir / "id_registry.json").read_text())
            assert ["device", "mydevice"] in dumped
            assert ["module", "mydevice", "sensor"] in dumped
            assert ["module", "mydevice", "sensor.temp"] in dumped
        finally:
            reg.stop()

    def test_survives_an_unwritable_cold_dir_without_raising(self, tmp_path):
        """A best-effort persistence step - if it can't write, log and move on rather than
        blowing up Registry.stop()."""
        reg = make_real_registry(tmp_path, "dump_unwritable_test")
        try:
            reg.id_registry.get_device("mydevice")
            reg._dump_id_registry(tmp_path / "does_not_exist" / "nested" / "missing")  # no mkdir
        finally:
            reg.stop()


class TestIdRegistryRehydratesAtRegistryConstruction:
    def test_dump_next_to_replay_source_dir_is_replayed_before_configure_system_runs(self, tmp_path):
        """The whole point of moving this into IDRegistry.__init__: the registry's device/module
        ids are already correct immediately after Registry(...) returns - no separate
        "configure, then restore" step, and no dependency on CircularLogPool having mounted
        anything (id_registry rehydration and cold-segment mounting are two independent
        consumers of the same persisted directory, not coupled to each other)."""
        old_session_dir = tmp_path / "old_session"
        cold_dir = old_session_dir / "cold"
        cold_dir.mkdir(parents=True)
        dump = [["device", "mydevice"], ["module", "mydevice", "sensor"], ["module", "mydevice", "sensor.temp"]]
        (cold_dir / "id_registry.json").write_text(json.dumps(dump))

        reg = Registry(
            session_name="rehydrate_test",
            log_dir=tmp_path / "live",
            config_path=tmp_path / "test_config.json",
            replay_mode=True,
            replay_source_dir=old_session_dir,
        )
        try:
            # Not calling configure_system() at all - rehydration must already be done.
            assert "mydevice" in reg.id_registry.device_lookup
            device = reg.id_registry.device_lookup["mydevice"]
            assert "sensor" in device.path_lookup
            assert "sensor.temp" in device.path_lookup
            assert device.path_lookup["sensor.temp"].id != device.path_lookup["sensor"].id

            # A subsequent lookup must be a cache hit (no new discovery_log entries), confirming
            # rehydration actually populated the registry rather than something else making
            # these names coincidentally resolvable.
            log_len = len(reg.id_registry.discovery_log)
            reg.id_registry.get_device("mydevice").get_module("sensor.temp")
            assert len(reg.id_registry.discovery_log) == log_len
        finally:
            reg.stop()

    def test_no_dump_present_is_a_quiet_noop(self, tmp_path):
        old_session_dir = tmp_path / "old_session_no_dump"
        old_session_dir.mkdir()

        reg = Registry(
            session_name="rehydrate_noop_test",
            log_dir=tmp_path / "live",
            config_path=tmp_path / "test_config.json",
            replay_mode=True,
            replay_source_dir=old_session_dir,
        )
        try:
            assert "mydevice" not in reg.id_registry.device_lookup
        finally:
            reg.stop()

    def test_replay_source_dir_none_is_the_ordinary_live_session_case(self, tmp_path):
        """No regression for the common (non-replay) path - nothing to rehydrate, nothing
        breaks."""
        reg = make_real_registry(tmp_path, "no_replay_source_dir_test")
        try:
            assert reg.file_manager.replay_source_dir is None
        finally:
            reg.stop()
