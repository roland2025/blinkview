# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from tests.fakes.real_registry import make_real_registry


class TestSaveGoesIntoOwnSessionFolder:
    def test_adding_a_range_writes_a_sidecar_file_in_session_dir(self, tmp_path):
        reg = make_real_registry(tmp_path, "range_save_test")
        try:
            reg.playback_ranges.add("boot sequence", 100, 500)

            path = reg.file_manager.get_playback_ranges_path()
            assert path.exists()
            assert path.parent == reg.file_manager.session_dir

            import json

            data = json.loads(path.read_text())
            assert data["ranges"][0]["name"] == "boot sequence"
            assert (data["ranges"][0]["start_ts_ns"], data["ranges"][0]["end_ts_ns"]) == (100, 500)
        finally:
            reg.stop()

    def test_every_mutation_updates_the_file(self, tmp_path):
        reg = make_real_registry(tmp_path, "range_save_test2")
        try:
            rng = reg.playback_ranges.add("a", 0, 10)
            reg.playback_ranges.rename(rng.id, "renamed")

            import json

            data = json.loads(reg.file_manager.get_playback_ranges_path().read_text())
            assert data["ranges"][0]["name"] == "renamed"

            reg.playback_ranges.remove(rng.id)
            data = json.loads(reg.file_manager.get_playback_ranges_path().read_text())
            assert data["ranges"] == []
        finally:
            reg.stop()


class TestDiscoverAndLoadReplaySourceRanges:
    def test_finds_and_merges_ranges_from_a_replayed_session_folder(self, tmp_path):
        # Session A: a previous capture that accumulated some named ranges.
        original = make_real_registry(tmp_path, "original_capture")
        try:
            original.playback_ranges.add("crash", 1000, 2000)
            source_dir = original.file_manager.session_dir
            # The raw file a BinaryFileReader would actually be pointed at, replaying session A.
            replayed_file = source_dir / "adb_reader.0000.bin"
            replayed_file.write_bytes(b"fake captured bytes")
        finally:
            original.stop()

        # Session B: a fresh run "replaying" that file - simulate a configured BinaryFileReader
        # via a lightweight stand-in with just the duck-typed `file_path` attribute
        # _discover_replay_ranges_path actually reads, rather than fighting full sources config
        # wiring for a unit test of this one lookup.
        replay = make_real_registry(tmp_path, "replay_run")
        try:
            replay.sources.sources["binary_file_1"] = SimpleNamespace(file_path=str(replayed_file))

            replay._load_replay_playback_ranges()

            names = [r.name for r in replay.playback_ranges.ranges]
            assert names == ["crash"]
        finally:
            replay.stop()

    def test_own_session_ranges_survive_the_merge_alongside_replay_source_ranges(self, tmp_path):
        original = make_real_registry(tmp_path, "original_capture2")
        try:
            original.playback_ranges.add("crash", 1000, 2000)
            source_dir = original.file_manager.session_dir
            replayed_file = source_dir / "adb_reader.0000.bin"
            replayed_file.write_bytes(b"fake captured bytes")
        finally:
            original.stop()

        replay = make_real_registry(tmp_path, "replay_run2")
        try:
            replay.playback_ranges.add("my own marker", 0, 10)
            replay.sources.sources["binary_file_1"] = SimpleNamespace(file_path=str(replayed_file))

            replay._load_replay_playback_ranges()

            names = sorted(r.name for r in replay.playback_ranges.ranges)
            assert names == ["crash", "my own marker"]
        finally:
            replay.stop()

    def test_no_source_with_file_path_is_a_noop(self, tmp_path):
        reg = make_real_registry(tmp_path, "no_source_test")
        try:
            reg.sources.sources["adb_1"] = SimpleNamespace()  # no file_path attribute at all
            reg._load_replay_playback_ranges()
            assert reg.playback_ranges.ranges == []
        finally:
            reg.stop()
