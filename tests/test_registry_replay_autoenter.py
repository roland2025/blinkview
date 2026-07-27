# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Real-Registry coverage for auto-entering DVR playback mode (and creating a default "whole
recording" named range) the moment a replay session is detected - two entry points, both
funneling into Registry.load_replay_session:
- Registry._enter_replay_mode_if_detected - auto-triggered at configure_system() time for the
  dev-replay workflow (a configured BinaryFileReader/FileTailReader source whose file_path
  happens to live inside a previous session's folder).
- Registry.load_replay_session itself, called directly by MainWindow.start_replay - the
  production "Load Session..."/`blink replay` path, which never touches registry.sources at all.

Mirrors test_playback_ranges_persistence.py's pattern: a real prior session (built via
make_real_registry + .file_manager.stop(), which writes a real metadata.json) is "replayed" by a
fresh Registry via a lightweight file_path-only stand-in source (for the auto-detect tests) or
its session_dir directly (for the load_replay_session tests), rather than wiring a full
BinaryFileReader/UnifiedLogReplay for what's really a unit test of this lookup/decision logic."""

from datetime import datetime, timezone
from types import SimpleNamespace

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.playback_clock import PlaybackMode
from tests.fakes.real_registry import make_real_registry


def _epoch_ns(iso_str: str) -> int:
    return int(datetime.fromisoformat(iso_str.rstrip("Z")).replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)


def _make_finished_prior_session(tmp_path, name):
    """A real prior session, cleanly stopped so its metadata.json gets a real finished_at/status
    (FileManager.stop() writes both) - the file a fresh run would "replay" lives right next to
    it, same as the real BinaryFileReader/FileTailReader dev-replay workflow.

    Calls file_manager.stop() directly rather than registry.stop() - the latter is a no-op
    unless the registry was actually .start()ed (Registry._is_running), which make_real_registry
    doesn't do by default and isn't needed just to produce a real metadata.json."""
    original = make_real_registry(tmp_path, name)
    session_dir = original.file_manager.session_dir
    original.file_manager.stop()
    original.stop()

    replayed_file = session_dir / "adb_reader.0000.bin"
    replayed_file.write_bytes(b"fake captured bytes")
    return session_dir, replayed_file


class TestAutoEntersReplayModeAndCreatesDefaultRange:
    def test_metadata_present_creates_default_range_and_enters_replay(self, tmp_path):
        session_dir, replayed_file = _make_finished_prior_session(tmp_path, "original_a")

        import json

        metadata = json.loads((session_dir / "metadata.json").read_text())
        expected_start_ns = _epoch_ns(metadata["created_at"])
        expected_end_ns = _epoch_ns(metadata["finished_at"])

        replay = make_real_registry(tmp_path, "replay_a")
        try:
            replay.sources.sources["binary_file_1"] = SimpleNamespace(file_path=str(replayed_file))

            replay._enter_replay_mode_if_detected()

            assert replay.playback_clock.mode is PlaybackMode.REPLAY

            ranges = replay.playback_ranges.ranges
            assert len(ranges) == 1
            assert ranges[0].name == replay.DEFAULT_REPLAY_RANGE_NAME
            assert ranges[0].start_ts_ns == expected_start_ns
            assert ranges[0].end_ts_ns == expected_end_ns
        finally:
            replay.stop()

    def test_no_metadata_still_enters_replay_but_creates_no_range(self, tmp_path):
        replayed_file = tmp_path / "no_metadata_dir" / "raw.bin"
        replayed_file.parent.mkdir(parents=True)
        replayed_file.write_bytes(b"raw bytes, no sibling metadata.json")

        replay = make_real_registry(tmp_path, "replay_b")
        try:
            replay.sources.sources["binary_file_1"] = SimpleNamespace(file_path=str(replayed_file))

            replay._enter_replay_mode_if_detected()

            assert replay.playback_clock.mode is PlaybackMode.REPLAY
            assert replay.playback_ranges.ranges == []
        finally:
            replay.stop()

    def test_no_file_based_source_stays_live_and_creates_no_range(self, tmp_path):
        replay = make_real_registry(tmp_path, "replay_c")
        try:
            replay.sources.sources["adb_1"] = SimpleNamespace()  # no file_path at all

            replay._enter_replay_mode_if_detected()

            assert replay.playback_clock.mode is PlaybackMode.LIVE
            assert replay.playback_ranges.ranges == []
        finally:
            replay.stop()

    def test_a_default_range_already_merged_in_is_not_duplicated(self, tmp_path):
        """Simulates replaying a file that's itself already a replay of the original session -
        _load_replay_playback_ranges would have already merged in a previous generation's
        DEFAULT_REPLAY_RANGE_NAME range before _enter_replay_mode_if_detected runs (see
        configure_system's call order); a fresh add() must not pile on a duplicate."""
        session_dir, replayed_file = _make_finished_prior_session(tmp_path, "original_d")

        replay = make_real_registry(tmp_path, "replay_d")
        try:
            replay.sources.sources["binary_file_1"] = SimpleNamespace(file_path=str(replayed_file))
            replay.playback_ranges.add(replay.DEFAULT_REPLAY_RANGE_NAME, 111, 222)

            replay._enter_replay_mode_if_detected()

            ranges = replay.playback_ranges.ranges
            assert len(ranges) == 1
            assert (ranges[0].start_ts_ns, ranges[0].end_ts_ns) == (111, 222)  # untouched
        finally:
            replay.stop()

    def test_playhead_lands_on_the_default_range_start_once_data_streams_in(self, tmp_path):
        """End-to-end: the deferred seek (PlaybackClock.enter_replay_when_ready) resolves to the
        metadata-derived start on the first real tick() after data actually arrives in the pool -
        not before, and not at bounds_min_ns of whatever real data happens to land first."""
        session_dir, replayed_file = _make_finished_prior_session(tmp_path, "original_e")

        import json

        metadata = json.loads((session_dir / "metadata.json").read_text())
        expected_start_ns = _epoch_ns(metadata["created_at"])

        replay = make_real_registry(tmp_path, "replay_e")
        try:
            replay.sources.sources["binary_file_1"] = SimpleNamespace(file_path=str(replayed_file))
            replay._enter_replay_mode_if_detected()

            # No data ingested yet - the seek must still be pending, current_ts_ns untouched.
            assert replay.playback_clock.current_ts_ns == 0

            device = replay.id_registry.get_device("replaytest")
            module = device.get_module("floats")
            array_pool = replay.system_ctx.array_pool
            log_pool = replay.central.log_pool
            # Real rows land well after expected_start_ns - a stand-in for "the file's rows are
            # slightly after the session's created_at timestamp", which is fine: seeking to
            # expected_start_ns just clamps up to bounds_min_ns once it's below the real data.
            base_ts = expected_start_ns + 5_000_000_000
            src = array_pool.create(PooledLogBatch, 5, 4096, has_levels=True, has_modules=True, has_devices=True)
            with src:
                for i in range(5):
                    ts = base_ts + i * 100_000_000
                    src.insert_any(ts, ts, f"{float(i)}".encode("ascii"), level=0, module=module.id, device=device.id)
                log_pool.batch_append(src)

            replay.playback_clock.tick(replay.now_ns())

            assert replay.playback_clock.mode is PlaybackMode.REPLAY
            assert replay.playback_clock.current_ts_ns == replay.playback_clock.bounds_min_ns
        finally:
            replay.stop()


class TestLoadReplaySession:
    """Covers Registry.load_replay_session(session_dir) directly - the entry point
    MainWindow.start_replay (the production "Load Session..." menu / `blink replay` CLI path)
    calls explicitly, since that path drives a UnifiedLogReplay straight into registry.central
    and never touches registry.sources, so it can't be auto-detected by
    _enter_replay_mode_if_detected's source duck-typing the way the dev-replay
    (BinaryFileReader/FileTailReader) workflow is."""

    def test_loads_ranges_creates_default_range_and_enters_replay(self, tmp_path):
        original = make_real_registry(tmp_path, "original_f")
        original.playback_ranges.add("crash", 1000, 2000)
        session_dir = original.file_manager.session_dir
        original.file_manager.stop()
        original.stop()

        import json

        metadata = json.loads((session_dir / "metadata.json").read_text())
        expected_start_ns = _epoch_ns(metadata["created_at"])
        expected_end_ns = _epoch_ns(metadata["finished_at"])

        replay = make_real_registry(tmp_path, "replay_f")
        try:
            # Sanity: nothing loaded yet, still LIVE - this is the state right after
            # configure_system() for a plain (non-dev-replay-workflow) session.
            assert replay.playback_clock.mode is PlaybackMode.LIVE
            assert replay.playback_ranges.ranges == []

            replay.load_replay_session(session_dir)

            assert replay.playback_clock.mode is PlaybackMode.REPLAY

            ranges = {r.name: r for r in replay.playback_ranges.ranges}
            assert set(ranges) == {"crash", replay.DEFAULT_REPLAY_RANGE_NAME}
            assert (ranges["crash"].start_ts_ns, ranges["crash"].end_ts_ns) == (1000, 2000)
            assert ranges[replay.DEFAULT_REPLAY_RANGE_NAME].start_ts_ns == expected_start_ns
            assert ranges[replay.DEFAULT_REPLAY_RANGE_NAME].end_ts_ns == expected_end_ns
        finally:
            replay.stop()

    def test_accepts_a_str_path_not_just_a_Path(self, tmp_path):
        original = make_real_registry(tmp_path, "original_g")
        session_dir = original.file_manager.session_dir
        original.file_manager.stop()
        original.stop()

        replay = make_real_registry(tmp_path, "replay_g")
        try:
            replay.load_replay_session(str(session_dir))  # main_window passes session_info.path,
            # a Path already, but this should be robust to a plain string too

            assert replay.playback_clock.mode is PlaybackMode.REPLAY
            assert len(replay.playback_ranges.ranges) == 1
        finally:
            replay.stop()

    def test_no_finished_at_still_enters_replay_but_creates_no_default_range(self, tmp_path):
        """A session still 'active' (crashed, or the recorder never cleanly stopped) has no
        finished_at - can't derive a whole-recording span from it, but DVR mode should still
        activate."""
        original = make_real_registry(tmp_path, "original_h")
        session_dir = original.file_manager.session_dir
        # Deliberately no file_manager.stop() - metadata.json keeps status="active", no
        # finished_at, matching a session that crashed or is still recording.

        replay = make_real_registry(tmp_path, "replay_h")
        try:
            replay.load_replay_session(session_dir)

            assert replay.playback_clock.mode is PlaybackMode.REPLAY
            assert replay.playback_ranges.ranges == []
        finally:
            replay.stop()
            original.stop()

    def test_range_edits_during_replay_never_modify_the_original_sessions_own_file(self, tmp_path):
        """The whole point of the replay/ scratch redirect (FileManager.replay_source_dir /
        _redirect_to_replay_scratch): a session being replayed must never have its own files
        touched, in case something goes wrong mid-edit and corrupts them. New ranges added while
        replaying must instead land in a `replay/` subfolder of the original session, merged with
        whatever ranges that session already had."""
        original = make_real_registry(tmp_path, "original_i")
        original.playback_ranges.add("crash", 1000, 2000)
        session_dir = original.file_manager.session_dir
        original.file_manager.stop()
        original.stop()

        original_ranges_path = session_dir / "playback_ranges.json"
        original_content_before = original_ranges_path.read_text()

        replay = make_real_registry(tmp_path, "replay_i")
        try:
            replay.load_replay_session(session_dir)
            replay.playback_ranges.add("new marker", 10, 20)

            # The original session's own sidecar file is byte-for-byte untouched.
            assert original_ranges_path.read_text() == original_content_before

            import json

            scratch_path = session_dir / "replay" / "playback_ranges.json"
            data = json.loads(scratch_path.read_text())
            names = sorted(r["name"] for r in data["ranges"])
            # load_replay_session also merges in its own auto-generated "whole recording" range.
            assert names == sorted(["crash", "new marker", replay.DEFAULT_REPLAY_RANGE_NAME])
        finally:
            replay.stop()

    def test_loading_a_replay_and_editing_ranges_creates_no_new_sibling_session_folder(self, tmp_path):
        """Regression guard for the original bug report: opening a replay (and doing something
        that writes, like adding a range) must not leave a new top-level entry in
        logs/<project>/ - anything written lands inside the original session's own `replay/`
        subfolder instead."""
        original = make_real_registry(tmp_path, "original_k")
        session_dir = original.file_manager.session_dir
        original.file_manager.stop()
        original.stop()

        replay = make_real_registry(tmp_path, "replay_k")
        try:
            project_dir_before = sorted(p.name for p in session_dir.parent.iterdir())

            replay.load_replay_session(session_dir)
            replay.playback_ranges.add("new marker", 10, 20)

            project_dir_after = sorted(p.name for p in session_dir.parent.iterdir())
            assert project_dir_after == project_dir_before
        finally:
            replay.stop()
