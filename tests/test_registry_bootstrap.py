# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Real end-to-end bootstrap tests: boot a real Registry (no mocked daemons) and confirm the
startup log lines actually land in central storage via the real log_append -> flush_log_queue ->
central.put -> daemon thread -> log_pool.batch_append pipeline - not just that logger.warn(...)
was called. Complements the per-module unit tests, which structurally can't see a break at one of
these seams."""

import json
import time

import pytest

from blinkview.core.registry import Registry
from tests.fakes.real_registry import make_real_registry


def _poll_for_message(log_pool, device_id, needle: bytes, timeout=2.0):
    """Polls log_pool's real snapshot until a row tagged with device_id whose message contains
    needle shows up (mirrors test_central_storage.py's deadline-loop convention for waiting on a
    real daemon thread), or fails with a clear assertion message."""
    deadline = time.time() + timeout
    last_seen = []
    while time.time() < deadline:
        with log_pool.get_snapshot() as segments:
            for seg in segments:
                for ts, msg, _rx_ts, _level, _module, device, *_rest in seg:
                    if device == device_id:
                        last_seen.append(bytes(msg))
                        if needle in bytes(msg):
                            return bytes(msg)
        time.sleep(0.02)

    pytest.fail(f"never saw a SYSTEM row containing {needle!r} within {timeout}s; saw: {last_seen}")


class TestFreshBootstrap:
    """No config file on disk - ConfigManager falls back to Registry's built-in default_config,
    which already has truthy reorder/central sub-dicts. This is the practical "fresh install /
    empty initial config" case (make_real_registry never writes a config file up front)."""

    def test_boots_and_initializes_central_and_reorder_without_crashing(self, tmp_path):
        registry = make_real_registry(tmp_path, "fresh_boot_test", start=True)
        try:
            assert registry.initialized is True
            assert registry.central is not None
            assert registry.reorder is not None
            assert registry.playback_clock is not None
        finally:
            registry.stop()

    def test_starting_session_message_lands_in_central_storage(self, tmp_path):
        registry = make_real_registry(tmp_path, "fresh_boot_test", start=True)
        try:
            registry.flush_log_queue()  # don't wait on the periodic TaskManager timer
            msg = _poll_for_message(registry.central.log_pool, registry.system_device.id, b"Starting Session")
            assert b"fresh_boot_test" in msg
        finally:
            registry.stop()

    def test_live_message_after_start_also_lands_in_central_storage(self, tmp_path):
        """Confirms the post-configure_system() SystemLogger path (not just the pre-init
        _temp_log_queue-drain path) makes it all the way through too."""
        registry = make_real_registry(tmp_path, "fresh_boot_test", start=True)
        try:
            registry.flush_log_queue()
            _poll_for_message(registry.central.log_pool, registry.system_device.id, b"BlinkView is now live")
        finally:
            registry.stop()


class TestReplayModeSkipsTempLogDump:
    """_dump_temp_logs() (registry.py ~658) used to unconditionally flush PrintLogger messages
    buffered before central storage existed into central.log_pool - real wall-clock "now"
    timestamps. In replay_mode, start_replay() runs later (off a QTimer, once the main window is
    up) and streams in a historical session's own timestamps, which can be arbitrarily far in the
    past. Central storage's segment bounds (PooledLogBatch.start_ts/end_ts) assume rows arrive
    ts-ordered; today's boot messages landing in the same hot segment ahead of yesterday's
    replayed rows breaks that and corrupts get_time_bounds()/the playback scrubber's range. Fixed
    by dropping the buffered startup messages entirely when replay_mode is True, rather than
    flushing them into central storage."""

    def test_temp_log_queue_dropped_without_reaching_central_storage(self, tmp_path):
        registry = Registry(
            session_name="replay_mode_temp_log_test",
            log_dir=tmp_path,
            config_path=tmp_path / "test_config.json",
            replay_mode=True,
        )
        try:
            registry.configure_system()
            assert registry._temp_log_queue is None

            with registry.central.log_pool.get_snapshot() as segments:
                for seg in segments:
                    for _ts, msg, *_rest in seg:
                        pytest.fail(f"unexpected row in central storage before replay data loaded: {bytes(msg)!r}")
        finally:
            registry.stop()


class TestExplicitlyEmptyConfigDict:
    """A config file that is literally '{}' - a distinct, worse case than "no file at all": every
    top-level get_by_path(..., "/x") lookup (plugins/reorder/central) resolves to None since
    dict_utils.get_by_path has no fallback default, not {}."""

    def _make_registry_with_empty_config(self, tmp_path):
        config_path = tmp_path / "test_config.json"
        config_path.write_text(json.dumps({}), encoding="utf-8")
        return Registry(session_name="empty_dict_config_test", log_dir=tmp_path, config_path=config_path)

    def test_configure_system_does_not_raise_but_builds_no_storage(self, tmp_path):
        """self.plugins.apply_config(None) (get_by_path("/plugins") on a real {} config returns
        None, not {}) raises AttributeError inside configure_system()'s own outer try/except,
        which swallows it - so the method returns cleanly, but aborts before ever reaching the
        reorder/central build steps. central/reorder end up None, same as if the guards had
        simply evaluated falsy, but for a different underlying reason worth knowing about."""
        registry = self._make_registry_with_empty_config(tmp_path)
        try:
            registry.configure_system()  # must not raise
            assert registry.central is None
            assert registry.reorder is None
        finally:
            registry.stop()

    def test_start_crashes_because_module_value_tracker_assumes_central_exists(self, tmp_path):
        """Known bug, documented rather than fixed (see memory): Registry.start() (registry.py
        ~488-493) unconditionally builds LatestModuleValueTracker(self.central.log_pool, ...) with
        no None-guard on self.central. When central never got built (this empty-config case),
        start() raises AttributeError instead of booting - "must boot from an empty config" does
        NOT currently hold for a genuinely empty {} config file (only for the no-file-at-all
        case, which falls back to Registry's built-in default_config)."""
        registry = self._make_registry_with_empty_config(tmp_path)
        try:
            with pytest.raises(AttributeError, match="log_pool"):
                registry.start()
        finally:
            registry.stop()
