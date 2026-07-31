# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from blinkview.storage import file_manager as file_manager_module
from blinkview.storage.file_manager import FileManager, _get_file_hash, get_session_identity


class FakeBatchProcessor:
    def __init__(self, extension="log"):
        self.extension = extension


class FakeFileLogger:
    def __init__(self, logging_id="log-1", extension="log", part_index=0):
        self.local = SimpleNamespace(logging_id=logging_id)
        self.batch_processor = FakeBatchProcessor(extension)
        self.part_index = part_index
        self.stopped = False

    def stop(self):
        self.stopped = True


def make_manager(tmp_path, **overrides):
    """Bypasses FileManager.__init__ (which touches real global/project settings and creates
    real directories relative to cwd/home) and wires up just the attributes the methods under
    test actually need, all rooted under tmp_path."""
    fm = FileManager.__new__(FileManager)
    fm.system_context = None
    fm.gui_context = None
    fm.log_dir = tmp_path / "logs"
    fm.project_name = "proj"
    fm.profile_name = "profile"
    fm.session_display_name = "Untitled"
    fm.session_dir = tmp_path / "session"
    fm.session_dir.mkdir(parents=True, exist_ok=True)
    fm.replay_source_dir = None
    fm.replay_mode = False
    fm.config_dir = tmp_path / "config"
    fm.config_dir.mkdir(parents=True, exist_ok=True)
    fm.config_file_name = "myconfig"
    fm.metadata = {
        "loggers": {},
        "status": "active",
        "created_at": datetime.now(timezone.utc).isoformat() + "Z",
    }
    fm._file_loggers = []
    for key, value in overrides.items():
        setattr(fm, key, value)
    return fm


class TestModuleFunctions:
    def test_get_file_hash_returns_unknown_for_missing_file(self, tmp_path):
        assert _get_file_hash(tmp_path / "nope.json") == "unknown"

    def test_get_file_hash_matches_md5_of_contents(self, tmp_path):
        import hashlib

        f = tmp_path / "data.json"
        f.write_bytes(b"hello world")

        assert _get_file_hash(f) == hashlib.md5(b"hello world").hexdigest()

    def test_get_session_identity_falls_back_to_stem_when_no_project_root(self, tmp_path, monkeypatch):
        monkeypatch.setattr(file_manager_module, "get_project_root", lambda: None)
        config_path = tmp_path / "my_config.json"

        assert get_session_identity(config_path) == "my_config"

    def test_get_session_identity_joins_relative_parts_under_workspace(self, tmp_path, monkeypatch):
        workspace = tmp_path / "workspace"
        (workspace / "configs" / "sub").mkdir(parents=True)
        config_path = workspace / "configs" / "sub" / "my_config.json"
        config_path.write_text("{}")

        monkeypatch.setattr(file_manager_module, "get_project_root", lambda: workspace)

        assert get_session_identity(config_path) == "configs_sub_my_config"

    def test_get_session_identity_falls_back_when_path_is_outside_workspace(self, tmp_path, monkeypatch):
        workspace = tmp_path / "workspace"
        workspace.mkdir()
        outside = tmp_path / "elsewhere" / "my_config.json"
        outside.parent.mkdir(parents=True)
        outside.write_text("{}")

        monkeypatch.setattr(file_manager_module, "get_project_root", lambda: workspace)

        assert get_session_identity(outside) == "my_config"


class TestSanitize:
    def test_replaces_invalid_characters_with_underscore(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm._sanitize("hello world!") == "hello_world"

    def test_squeezes_repeated_underscores(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm._sanitize("a---b") == "a_b"

    def test_strips_leading_and_trailing_underscores(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm._sanitize("!!!clean!!!") == "clean"

    def test_empty_result_falls_back_to_unnamed(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm._sanitize("!!!") == "Unnamed"


class TestPathsAndRepr:
    def test_get_path_joins_session_dir(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.get_path("foo.bin") == fm.session_dir / "foo.bin"

    def test_repr_includes_session_dir_name(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.session_dir.name in repr(fm)

    def test_get_config_path_without_type_name(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.get_config_path() == fm.config_dir / "myconfig.json"

    def test_get_config_path_with_type_name(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.get_config_path("gui_state") == fm.config_dir / "myconfig.gui_state.json"

    def test_get_session_path_defaults(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.get_session_path() == fm.session_dir / "myconfig.json"

    def test_get_session_path_with_type_and_suffix(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.get_session_path("gui_state", "final") == fm.session_dir / "myconfig.gui_state.final.json"

    def test_get_path_for_log_uses_logging_id_extension_and_padded_part(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger(logging_id="abc123", extension="bin")

        path = fm.get_path_for_log(logger, part=7)

        assert path == fm.session_dir / "abc123.0007.bin"


class TestWriteMetadata:
    def test_writes_metadata_json_matching_in_memory_dict(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.metadata["status"] = "active"

        fm.write_metadata()

        written = json.loads((fm.session_dir / "metadata.json").read_text())
        assert written == fm.metadata


class TestFileLoggerLifecycle:
    def test_add_file_logger_initializes_a_new_metadata_entry(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger(logging_id="log-1", extension="log", part_index=0)

        fm.add_file_logger(logger)

        entry = fm.metadata["loggers"]["log-1"]
        assert entry["extension"] == "log"
        assert entry["last_part"] == 0
        assert entry["total_bytes"] == 0
        assert entry["processor"] == "FakeBatchProcessor"
        assert logger in fm._file_loggers

    def test_add_file_logger_does_not_duplicate_in_the_tracking_list(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger()

        fm.add_file_logger(logger)
        fm.add_file_logger(logger)

        assert fm._file_loggers.count(logger) == 1

    def test_re_adding_a_known_logger_id_advances_last_part_and_updates_part_index(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.metadata["loggers"]["log-1"] = {"last_part": 2, "total_bytes": 500}
        logger = FakeFileLogger(logging_id="log-1")

        fm.add_file_logger(logger)

        assert fm.metadata["loggers"]["log-1"]["last_part"] == 3
        assert logger.part_index == 3

    def test_add_file_logger_persists_metadata(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger(logging_id="log-1")

        fm.add_file_logger(logger)

        written = json.loads((fm.session_dir / "metadata.json").read_text())
        assert "log-1" in written["loggers"]

    def test_remove_file_logger_removes_from_tracking_list(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger()
        fm.add_file_logger(logger)

        fm.remove_file_logger(logger)

        assert logger not in fm._file_loggers

    def test_remove_unknown_file_logger_is_a_noop(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.remove_file_logger(FakeFileLogger())  # must not raise


class TestUpdateLoggerStats:
    def test_absolute_replaces_the_total(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger(logging_id="log-1")
        fm.add_file_logger(logger)
        fm.metadata["loggers"]["log-1"]["total_bytes"] = 100

        fm.update_logger_stats(logger, 50, absolute=True)

        assert fm.metadata["loggers"]["log-1"]["total_bytes"] == 50

    def test_relative_accumulates_the_total(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger(logging_id="log-1")
        fm.add_file_logger(logger)
        fm.metadata["loggers"]["log-1"]["total_bytes"] = 100

        fm.update_logger_stats(logger, 50, absolute=False)

        assert fm.metadata["loggers"]["log-1"]["total_bytes"] == 150

    def test_unknown_logger_id_is_a_noop(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger(logging_id="never-added")

        fm.update_logger_stats(logger, 50)  # must not raise

        assert "never-added" not in fm.metadata["loggers"]


class TestGuiSaving:
    def test_save_gui_config_is_a_noop_without_gui_context(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.save_gui_config()  # must not raise
        assert not fm.get_config_path("gui_config").exists()

    def test_save_gui_config_writes_workspace_and_session_copies(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.gui_context = SimpleNamespace(gui_config=SimpleNamespace(get_data=lambda: {"a": 1}))

        fm.save_gui_config(suffix="autosave")

        workspace_path = fm.get_config_path("gui_config")
        session_path = fm.get_session_path("gui_config", "autosave")
        assert json.loads(workspace_path.read_text()) == {"a": 1}
        assert json.loads(session_path.read_text()) == {"a": 1}

    def test_save_gui_config_session_only_skips_the_workspace_copy(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.gui_context = SimpleNamespace(gui_config=SimpleNamespace(get_data=lambda: {"a": 1}))

        fm.save_gui_config(suffix="autosave", session_only=True)

        assert not fm.get_config_path("gui_config").exists()
        assert fm.get_session_path("gui_config", "autosave").exists()

    def test_save_gui_state_writes_workspace_and_session_copies(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.gui_context = SimpleNamespace(gui_state=SimpleNamespace(get_data=lambda: {"layout": "x"}))

        fm.save_gui_state(suffix="autosave")

        assert json.loads(fm.get_config_path("gui_state").read_text()) == {"layout": "x"}
        assert json.loads(fm.get_session_path("gui_state", "autosave").read_text()) == {"layout": "x"}

    def test_save_gui_calls_config_and_state_with_final_suffix(self, tmp_path):
        fm = make_manager(tmp_path)
        calls = []
        fm.save_gui_config = lambda suffix="autosave", session_only=False: calls.append(("config", suffix))
        fm.save_gui_state = lambda suffix="autosave", session_only=False: calls.append(("state", suffix))

        fm.save_gui()

        assert calls == [("config", "final"), ("state", "final")]


class TestSnapshotMasterToSession:
    def test_copies_the_master_file_when_it_exists(self, tmp_path):
        fm = make_manager(tmp_path)
        master = fm.get_config_path("gui_config")
        master.parent.mkdir(parents=True, exist_ok=True)
        master.write_text('{"x": 1}')

        fm._snapshot_master_to_session("gui_config")

        session_start = fm.get_session_path("gui_config", "start")
        assert session_start.read_text() == '{"x": 1}'

    def test_is_a_noop_when_master_does_not_exist(self, tmp_path):
        fm = make_manager(tmp_path)
        fm._snapshot_master_to_session("gui_config")
        assert not fm.get_session_path("gui_config", "start").exists()


class TestSaveSnapshot:
    def test_copies_files_into_the_snapshot_dir(self, tmp_path):
        fm = make_manager(tmp_path)
        src = tmp_path / "external" / "a.txt"
        src.parent.mkdir(parents=True)
        src.write_text("hello")

        fm.save_snapshot([str(src)])

        assert (fm.session_dir / "snapshot" / "a.txt").read_text() == "hello"

    def test_copies_directories_into_the_snapshot_dir(self, tmp_path):
        fm = make_manager(tmp_path)
        src_dir = tmp_path / "external_dir"
        (src_dir / "nested").mkdir(parents=True)
        (src_dir / "nested" / "b.txt").write_text("nested-content")

        fm.save_snapshot([str(src_dir)])

        assert (fm.session_dir / "snapshot" / "external_dir" / "nested" / "b.txt").read_text() == "nested-content"

    def test_skips_paths_that_do_not_exist(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.save_snapshot([str(tmp_path / "does_not_exist.txt")])  # must not raise
        snapshot_dir = fm.session_dir / "snapshot"
        assert not snapshot_dir.exists() or list(snapshot_dir.iterdir()) == []


class TestCreateSessionDir:
    def test_creates_a_directory_named_with_timestamp_identity_and_display_name(self, tmp_path):
        fm = make_manager(tmp_path)

        session_dir = fm._create_session_dir()

        assert session_dir.exists()
        assert session_dir.parent == fm.log_dir / fm.project_name
        assert session_dir.name.endswith("_profile_Untitled")


class TestReplayScratchRedirect:
    """FileManager.replay_source_dir - once set, the original session's own files (and the live
    workspace profile) must never be opened for writing; get_config_path/get_session_path/
    get_playback_ranges_path instead mirror into a `replay/` scratch subfolder of the original
    session, seeded with a one-time copy of whatever already exists."""

    def test_get_playback_ranges_path_redirects_and_seeds_from_original(self, tmp_path):
        fm = make_manager(tmp_path)
        original_session = tmp_path / "original_session"
        original_session.mkdir()
        (original_session / "playback_ranges.json").write_text('{"version": 1, "ranges": []}')
        fm.replay_source_dir = original_session

        path = fm.get_playback_ranges_path()

        assert path == original_session / "replay" / "playback_ranges.json"
        assert path.read_text() == '{"version": 1, "ranges": []}'

    def test_get_playback_ranges_path_does_not_require_original_to_exist(self, tmp_path):
        fm = make_manager(tmp_path)
        original_session = tmp_path / "original_session"
        original_session.mkdir()
        fm.replay_source_dir = original_session

        path = fm.get_playback_ranges_path()

        assert path == original_session / "replay" / "playback_ranges.json"
        assert not path.exists()

    def test_writing_into_the_redirected_path_never_touches_the_original_file(self, tmp_path):
        fm = make_manager(tmp_path)
        original_session = tmp_path / "original_session"
        original_session.mkdir()
        original_ranges = original_session / "playback_ranges.json"
        original_ranges.write_text('{"version": 1, "ranges": [{"id": "a"}]}')
        fm.replay_source_dir = original_session

        path = fm.get_playback_ranges_path()
        path.write_text('{"version": 1, "ranges": []}')

        assert original_ranges.read_text() == '{"version": 1, "ranges": [{"id": "a"}]}'

    def test_get_config_path_redirects_into_replay_scratch(self, tmp_path):
        fm = make_manager(tmp_path)
        original_session = tmp_path / "original_session"
        original_session.mkdir()
        fm.replay_source_dir = original_session

        path = fm.get_config_path("gui_config")

        assert path == original_session / "replay" / "myconfig.gui_config.json"

    def test_get_session_path_redirects_into_replay_scratch_and_seeds_from_original_session(self, tmp_path):
        fm = make_manager(tmp_path)
        original_session = tmp_path / "original_session"
        original_session.mkdir()
        (original_session / "myconfig.gui_state.autosave.json").write_text('{"layout": "old"}')
        fm.replay_source_dir = original_session

        path = fm.get_session_path("gui_state", "autosave")

        assert path == original_session / "replay" / "myconfig.gui_state.autosave.json"
        assert path.read_text() == '{"layout": "old"}'

    def test_repeated_calls_do_not_re_seed_over_an_already_edited_scratch_copy(self, tmp_path):
        fm = make_manager(tmp_path)
        original_session = tmp_path / "original_session"
        original_session.mkdir()
        (original_session / "playback_ranges.json").write_text('{"version": 1, "ranges": []}')
        fm.replay_source_dir = original_session

        path = fm.get_playback_ranges_path()
        path.write_text('{"version": 1, "ranges": [{"id": "new"}]}')

        path_again = fm.get_playback_ranges_path()
        assert path_again.read_text() == '{"version": 1, "ranges": [{"id": "new"}]}'

    def test_non_replay_mode_is_unaffected(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.get_playback_ranges_path() == fm.session_dir / "playback_ranges.json"
        assert fm.get_config_path("gui_config") == fm.config_dir / "myconfig.gui_config.json"


class TestStop:
    def test_stop_without_system_context_still_finalizes_metadata(self, tmp_path):
        fm = make_manager(tmp_path)
        fm.metadata["created_at"] = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat() + "Z"

        fm.stop()

        assert fm.metadata["status"] == "finished"
        assert "finished_at" in fm.metadata
        assert fm.metadata["duration_seconds"] >= 0

    def test_stop_stops_all_tracked_file_loggers(self, tmp_path):
        fm = make_manager(tmp_path)
        logger = FakeFileLogger()
        fm.add_file_logger(logger)

        fm.stop()

        assert logger.stopped is True

    def test_stop_persists_final_metadata_to_disk(self, tmp_path):
        fm = make_manager(tmp_path)

        fm.stop()

        written = json.loads((fm.session_dir / "metadata.json").read_text())
        assert written["status"] == "finished"

    def test_stop_saves_full_config_when_system_context_is_present(self, tmp_path):
        fm = make_manager(tmp_path)
        saved_paths = []
        fm.system_context = SimpleNamespace(
            registry=SimpleNamespace(config=SimpleNamespace(save_full_config=lambda path: saved_paths.append(path)))
        )

        fm.stop()

        assert saved_paths == [fm.get_session_path("final")]

    def test_stop_reports_on_progress_once_per_file_logger_with_its_logging_id(self, tmp_path):
        fm = make_manager(tmp_path)
        logger_a = FakeFileLogger(logging_id="session")
        logger_b = FakeFileLogger(logging_id="src_abcd1234")
        fm.add_file_logger(logger_a)
        fm.add_file_logger(logger_b)

        calls = []
        fm.stop(on_progress=calls.append)

        assert calls == ["session", "src_abcd1234"]

    def test_stop_with_no_progress_callback_still_stops_loggers(self, tmp_path):
        """The default on_progress=None must not be treated as truthy/called."""
        fm = make_manager(tmp_path)
        logger = FakeFileLogger()
        fm.add_file_logger(logger)

        fm.stop()  # must not raise

        assert logger.stopped is True

    def test_on_progress_fires_after_stop_not_before(self, tmp_path):
        """Progress for a logger must be reported only once its .stop() (which triggers that
        logger's final-part compression) has actually returned, not before."""
        fm = make_manager(tmp_path)

        class OrderTrackingLogger(FakeFileLogger):
            def stop(self):
                order.append("stop")
                super().stop()

        order = []
        fm.add_file_logger(OrderTrackingLogger())

        fm.stop(on_progress=lambda label: order.append("progress"))

        assert order == ["stop", "progress"]


class TestFileLoggerCount:
    def test_reflects_the_number_of_registered_file_loggers(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.file_logger_count == 0

        fm.add_file_logger(FakeFileLogger(logging_id="a"))
        assert fm.file_logger_count == 1

        fm.add_file_logger(FakeFileLogger(logging_id="b"))
        assert fm.file_logger_count == 2

        fm.remove_file_logger(fm._file_loggers[0])
        assert fm.file_logger_count == 1
