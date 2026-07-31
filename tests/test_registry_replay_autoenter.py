# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Real-Registry coverage for Registry.load_replay_session - entering DVR playback mode (and
recording the session's fixed length into replay_session_bounds_ns, used to pin the main seek bar
- see playback_control.py's _seek_bar_bounds). Called only explicitly by MainWindow.start_replay,
the production "Load Session..."/`blink replay` path - replay mode is never auto-detected from a
configured source's file_path (a configured BinaryFileReader/FileTailReader alone must not put
the registry into REPLAY; see feedback that motivated removing the old
Registry._enter_replay_mode_if_detected auto-detect path).

Mirrors test_playback_ranges_persistence.py's pattern: a real prior session (built via
make_real_registry + .file_manager.stop(), which writes a real metadata.json) is "replayed" by a
fresh Registry via its session_dir directly, rather than wiring a full
BinaryFileReader/UnifiedLogReplay for what's really a unit test of this lookup/decision logic."""

from datetime import datetime, timezone

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.playback_clock import PlaybackMode
from blinkview.core.registry import Registry
from tests.fakes.real_registry import make_real_registry


def _epoch_ns(iso_str: str) -> int:
    return int(datetime.fromisoformat(iso_str.rstrip("Z")).replace(tzinfo=timezone.utc).timestamp() * 1_000_000_000)


class TestLoadReplaySession:
    """Covers Registry.load_replay_session(session_dir) directly - the entry point
    MainWindow.start_replay (the production "Load Session..." menu / `blink replay` CLI path)
    calls explicitly, since that path drives a UnifiedLogReplay straight into registry.central
    and never touches registry.sources. Replay mode is only ever entered this way - a configured
    file-based source alone must not put the registry into REPLAY."""

    def test_loads_ranges_records_session_bounds_and_enters_replay(self, tmp_path):
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

            # The pre-existing user range is loaded as-is; the session's own fixed length is
            # recorded separately, not as a selectable named range (see
            # playback_control.py's _seek_bar_bounds).
            ranges = {r.name: r for r in replay.playback_ranges.ranges}
            assert set(ranges) == {"crash"}
            assert (ranges["crash"].start_ts_ns, ranges["crash"].end_ts_ns) == (1000, 2000)
            assert replay.replay_session_bounds_ns == (expected_start_ns, expected_end_ns)
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
            assert replay.playback_ranges.ranges == []
            assert replay.replay_session_bounds_ns is not None
        finally:
            replay.stop()

    def test_no_finished_at_still_enters_replay_but_records_no_bounds(self, tmp_path):
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
            assert replay.replay_session_bounds_ns is None
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
            assert names == sorted(["crash", "new marker"])
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

    def test_range_added_during_a_replay_is_restored_next_time_the_session_is_reloaded(self, tmp_path):
        """A range added while replaying gets saved into the original session's `replay/`
        scratch copy (FileManager._redirect_to_replay_scratch), not the original top-level
        playback_ranges.json (which must stay untouched). load_replay_session must therefore read
        that same scratch copy back on the *next* replay of this session, not the original file -
        otherwise anything added during a replay is silently lost the moment that replay ends."""
        original = make_real_registry(tmp_path, "original_l")
        session_dir = original.file_manager.session_dir
        original.file_manager.stop()
        original.stop()

        first_replay = make_real_registry(tmp_path, "replay_l1")
        try:
            first_replay.load_replay_session(session_dir)
            first_replay.playback_ranges.add("added during replay", 500, 600)
        finally:
            first_replay.stop()

        second_replay = make_real_registry(tmp_path, "replay_l2")
        try:
            second_replay.load_replay_session(session_dir)

            names = [r.name for r in second_replay.playback_ranges.ranges]
            assert "added during replay" in names
        finally:
            second_replay.stop()

    def test_playhead_lands_on_the_metadata_derived_start_once_data_streams_in(self, tmp_path):
        """End-to-end: the deferred seek (PlaybackClock.enter_replay_when_ready) resolves to the
        metadata-derived start on the first real tick() after data actually arrives in the pool -
        not before, and not at bounds_min_ns of whatever real data happens to land first."""
        original = make_real_registry(tmp_path, "original_e")
        session_dir = original.file_manager.session_dir
        original.file_manager.stop()
        original.stop()

        import json

        metadata = json.loads((session_dir / "metadata.json").read_text())
        expected_start_ns = _epoch_ns(metadata["created_at"])

        replay = make_real_registry(tmp_path, "replay_e")
        try:
            replay.load_replay_session(session_dir)

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


class TestSystemLoggingGatedUntilHistoricalReplayDataLands:
    """Registry.log_append (the sink for every SystemLogger call anywhere in the app -
    registry.logger.info/warn/..., configure_system()'s own "Configuring sources"/reorder/central
    setup messages included) carries this process's real wall-clock "now". Central storage's
    segment bounds (PooledLogBatch.start_ts/end_ts) assume rows arrive in non-decreasing ts order
    - if a handful of "now"-timestamped system messages land in a brand-new hot segment before the
    historical (much older) rows a replay session is about to stream in, that segment's own
    start_ts/end_ts end up inverted, corrupting get_time_bounds() and freezing
    PlaybackClock._clamp() (the actual regression: a loaded replay session's scrubber went
    completely unresponsive). Only replay_mode=True registries are at risk - a live session's own
    "now" logging is always chronologically consistent with the live data arriving alongside it -
    so make_real_registry (replay_mode=False) isn't useful here; these build a Registry directly."""

    def test_log_append_is_a_no_op_while_gate_is_closed(self, tmp_path):
        registry = Registry(
            session_name="gate_closed",
            log_dir=tmp_path,
            config_path=tmp_path / "gate_closed_config.json",
            replay_mode=True,
        )
        registry.configure_system()
        try:
            # configure_system() itself already emitted a bunch of SystemLogger calls
            # ("Configuring sources", reorder/central setup, ...) - none of them should have
            # reached central storage. log_batch stays None (log_append's very first line,
            # untouched) rather than checking downstream delivery through central/reorder, since
            # neither has a processing thread running without registry.start().
            assert registry._system_log_to_central_enabled is False
            assert registry.log_batch is None

            registry.logger.info("more startup noise, still gated")
            registry.flush_log_queue()

            assert registry.log_batch is None
        finally:
            registry.stop()

    def test_load_replay_session_opens_the_gate(self, tmp_path):
        original = make_real_registry(tmp_path, "gate_original")
        session_dir = original.file_manager.session_dir
        original.file_manager.stop()
        original.stop()

        registry = Registry(
            session_name="gate_open",
            log_dir=tmp_path,
            config_path=tmp_path / "gate_open_config.json",
            replay_mode=True,
        )
        registry.configure_system()
        try:
            assert registry._system_log_to_central_enabled is False

            registry.load_replay_session(session_dir)
            assert registry._system_log_to_central_enabled is True

            # Bypasses reorder's own delay/background thread and CentralStorage's own ingestion
            # thread (neither is running - this registry is never .start()ed, keeping the test
            # fast): stub central.put to just record that log_append actually attempted delivery,
            # rather than relying on it landing in log_pool through threads that aren't running.
            put_calls = []
            registry.central.put = put_calls.append
            registry.reorder.enabled = False  # flush_log_queue then calls central.put directly
            registry.logger.info("live system log after historical load")
            registry.flush_log_queue()

            assert len(put_calls) == 1
        finally:
            registry.stop()
