# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
from pathlib import Path
from unittest.mock import patch

from blinkview.utils.session_lister import (
    SessionInfo,
    existing_part,
    list_sessions,
    resolve_log_root,
    resolve_session,
    unified_log_parts,
)


class FakeSettings:
    def __init__(self, data=None):
        self._data = dict(data or {})

    def get(self, key, default=None):
        return self._data.get(key, default)


def _write_session(project_dir, folder_name, meta):
    session_dir = project_dir / folder_name
    session_dir.mkdir(parents=True)
    (session_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    return session_dir


class TestResolveLogRoot:
    def test_uses_explicit_log_dir_when_given(self):
        settings = FakeSettings()
        with patch("blinkview.utils.session_lister.get_project_root", return_value=None):
            log_dir, project_name = resolve_log_root(log_dir="/explicit/logs", settings=settings)

        assert log_dir == Path("/explicit/logs")

    def test_project_name_setting_overrides_project_dir_name(self, tmp_path):
        settings = FakeSettings({"project_name": "MyProject"})
        with patch("blinkview.utils.session_lister.get_project_root", return_value=tmp_path):
            _, project_name = resolve_log_root(settings=settings)

        assert project_name == "MyProject"

    def test_falls_back_to_project_dir_name_when_project_scoped(self, tmp_path):
        project_dir = tmp_path / "SomeRepo"
        project_dir.mkdir()
        settings = FakeSettings({})
        with patch("blinkview.utils.session_lister.get_project_root", return_value=project_dir):
            _, project_name = resolve_log_root(settings=settings)

        assert project_name == "SomeRepo"

    def test_falls_back_to_cwd_name_when_standalone(self):
        settings = FakeSettings({})
        with (
            patch("blinkview.utils.session_lister.get_project_root", return_value=None),
            patch("blinkview.utils.session_lister.Path.cwd", return_value=type("P", (), {"name": "CwdDir"})()),
        ):
            _, project_name = resolve_log_root(settings=settings)

        assert project_name == "CwdDir"

    def test_project_name_is_sanitized(self, tmp_path):
        settings = FakeSettings({"project_name": "My Project!!"})
        with patch("blinkview.utils.session_lister.get_project_root", return_value=tmp_path):
            _, project_name = resolve_log_root(settings=settings)

        assert project_name == "My_Project"

    def test_standalone_default_log_dir_is_under_blink_home(self, tmp_path):
        settings = FakeSettings({})
        with (
            patch("blinkview.utils.session_lister.get_project_root", return_value=None),
            patch("blinkview.utils.session_lister.get_blink_home", return_value=tmp_path),
        ):
            log_dir, _ = resolve_log_root(settings=settings)

        assert log_dir == tmp_path / "logs"

    def test_project_scoped_default_log_dir_is_relative_logs(self, tmp_path):
        settings = FakeSettings({})
        with patch("blinkview.utils.session_lister.get_project_root", return_value=tmp_path):
            log_dir, _ = resolve_log_root(settings=settings)

        assert str(log_dir) == "logs"

    def test_settings_log_dir_setting_is_honored(self, tmp_path):
        settings = FakeSettings({"log_dir": "/custom/logs"})
        with patch("blinkview.utils.session_lister.get_project_root", return_value=tmp_path):
            log_dir, _ = resolve_log_root(settings=settings)

        assert log_dir == Path("/custom/logs")


class TestListSessions:
    def test_returns_empty_list_when_project_dir_does_not_exist(self, tmp_path):
        assert list_sessions(tmp_path / "nope", "proj") == []

    def test_skips_non_directory_entries(self, tmp_path):
        project_dir = tmp_path / "proj"
        project_dir.mkdir()
        (project_dir / "not_a_dir.txt").write_text("x")

        assert list_sessions(tmp_path, "proj") == []

    def test_skips_directories_without_metadata_json(self, tmp_path):
        project_dir = tmp_path / "proj"
        (project_dir / "session1").mkdir(parents=True)

        assert list_sessions(tmp_path, "proj") == []

    def test_skips_directories_with_unparseable_metadata_json(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = project_dir / "session1"
        session_dir.mkdir(parents=True)
        (session_dir / "metadata.json").write_text("{not valid json")

        assert list_sessions(tmp_path, "proj") == []

    def test_parses_a_valid_session_into_session_info(self, tmp_path):
        project_dir = tmp_path / "proj"
        _write_session(
            project_dir,
            "session1",
            {
                "session_id": "session1",
                "project": {"display_name": "My Run"},
                "config": {"profile": "default"},
                "status": "finished",
                "created_at": "2026-01-01T00:00:00Z",
                "finished_at": "2026-01-01T01:00:00Z",
                "duration_seconds": 3600.0,
            },
        )

        sessions = list_sessions(tmp_path, "proj")

        assert sessions == [
            SessionInfo(
                session_id="session1",
                path=project_dir / "session1",
                display_name="My Run",
                profile="default",
                status="finished",
                created_at="2026-01-01T00:00:00Z",
                finished_at="2026-01-01T01:00:00Z",
                duration_seconds=3600.0,
            )
        ]

    def test_missing_optional_fields_use_sensible_defaults(self, tmp_path):
        project_dir = tmp_path / "proj"
        _write_session(project_dir, "session1", {})

        sessions = list_sessions(tmp_path, "proj")

        assert sessions[0].session_id == "session1"
        assert sessions[0].display_name == "session1"
        assert sessions[0].profile == ""
        assert sessions[0].status == "unknown"
        assert sessions[0].created_at is None

    def test_sorted_newest_first_by_created_at(self, tmp_path):
        project_dir = tmp_path / "proj"
        _write_session(project_dir, "older", {"session_id": "older", "created_at": "2026-01-01T00:00:00Z"})
        _write_session(project_dir, "newer", {"session_id": "newer", "created_at": "2026-06-01T00:00:00Z"})

        sessions = list_sessions(tmp_path, "proj")

        assert [s.session_id for s in sessions] == ["newer", "older"]

    def test_sessions_without_created_at_sort_last(self, tmp_path):
        project_dir = tmp_path / "proj"
        _write_session(project_dir, "dated", {"session_id": "dated", "created_at": "2026-01-01T00:00:00Z"})
        _write_session(project_dir, "undated", {"session_id": "undated"})

        sessions = list_sessions(tmp_path, "proj")

        assert [s.session_id for s in sessions] == ["dated", "undated"]


class TestResolveSession:
    def test_returns_none_when_no_sessions_exist(self, tmp_path):
        assert resolve_session(tmp_path, "proj", name="anything") is None

    def test_last_returns_the_newest_session(self, tmp_path):
        project_dir = tmp_path / "proj"
        _write_session(project_dir, "older", {"session_id": "older", "created_at": "2026-01-01T00:00:00Z"})
        newer_dir = _write_session(project_dir, "newer", {"session_id": "newer", "created_at": "2026-06-01T00:00:00Z"})
        (newer_dir / "session.0000.log").write_text("data")
        older_dir = project_dir / "older"
        (older_dir / "session.0000.log").write_text("data")

        result = resolve_session(tmp_path, "proj", last=True)

        assert result.session_id == "newer"

    def test_matches_by_exact_session_id(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(project_dir, "session1", {"session_id": "session1"})
        (session_dir / "session.0000.log").write_text("data")

        result = resolve_session(tmp_path, "proj", name="session1")

        assert result.session_id == "session1"

    def test_matches_by_exact_display_name(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(
            project_dir, "session1", {"session_id": "session1", "project": {"display_name": "My Run"}}
        )
        (session_dir / "session.0000.log").write_text("data")

        result = resolve_session(tmp_path, "proj", name="My Run")

        assert result.session_id == "session1"

    def test_matches_by_case_insensitive_substring(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(
            project_dir, "session1", {"session_id": "session1", "project": {"display_name": "My Special Run"}}
        )
        (session_dir / "session.0000.log").write_text("data")

        result = resolve_session(tmp_path, "proj", name="special")

        assert result.session_id == "session1"

    def test_no_name_and_no_last_returns_none(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(project_dir, "session1", {"session_id": "session1"})
        (session_dir / "session.0000.log").write_text("data")

        assert resolve_session(tmp_path, "proj") is None

    def test_sessions_without_a_unified_log_are_excluded_by_default(self, tmp_path):
        project_dir = tmp_path / "proj"
        _write_session(project_dir, "session1", {"session_id": "session1"})  # no session.* file

        assert resolve_session(tmp_path, "proj", name="session1") is None

    def test_require_unified_log_false_includes_sessions_without_one(self, tmp_path):
        project_dir = tmp_path / "proj"
        _write_session(project_dir, "session1", {"session_id": "session1"})

        result = resolve_session(tmp_path, "proj", name="session1", require_unified_log=False)

        assert result.session_id == "session1"

    def test_unmatched_name_returns_none(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(project_dir, "session1", {"session_id": "session1"})
        (session_dir / "session.0000.log").write_text("data")

        assert resolve_session(tmp_path, "proj", name="nonexistent") is None


def _session_info(path):
    return SessionInfo(
        session_id="s1",
        path=path,
        display_name="s1",
        profile="",
        status="unknown",
        created_at=None,
        finished_at=None,
        duration_seconds=None,
    )


class TestUnifiedLogParts:
    def test_returns_parts_in_index_order(self, tmp_path):
        (tmp_path / "session.0002.log").write_text("b")
        (tmp_path / "session.0000.log.zst").write_text("a")
        (tmp_path / "session.0001.log").write_text("c")

        parts = unified_log_parts(_session_info(tmp_path))

        assert [p.name for p in parts] == ["session.0000.log.zst", "session.0001.log", "session.0002.log"]

    def test_ignores_everything_that_is_not_a_unified_log_part(self, tmp_path):
        """A `.zst.tmp` is compress_file's not-yet-renamed output - replaying it would mmap
        partial zstd bytes as plain text."""
        (tmp_path / "session.0000.log").write_text("a")
        (tmp_path / "session.0001.log.zst.tmp").write_text("partial")
        (tmp_path / "session.000").write_text("x")
        (tmp_path / "session.0002.bin").write_text("x")
        (tmp_path / "src_0001.0000.bin").write_text("x")
        (tmp_path / "metadata.json").write_text("{}")
        (tmp_path / "session.0003.log").mkdir()

        parts = unified_log_parts(_session_info(tmp_path))

        assert [p.name for p in parts] == ["session.0000.log"]

    def test_plain_part_wins_over_its_compressed_sibling(self, tmp_path):
        """Both exist between compress_file's rename and FileLogger's unlink, or for good when the
        unlink failed - in which case a restart() may have kept appending to the plain file, so
        it can be a superset of the .zst. Replaying both would also duplicate rows."""
        (tmp_path / "session.0000.log.zst").write_text("old")
        (tmp_path / "session.0000.log").write_text("old + new")
        (tmp_path / "session.0001.log.zst").write_text("next")

        parts = unified_log_parts(_session_info(tmp_path))

        assert [p.name for p in parts] == ["session.0000.log", "session.0001.log.zst"]

    def test_sorts_by_index_past_four_digits(self, tmp_path):
        (tmp_path / "session.10000.log").write_text("b")
        (tmp_path / "session.9999.log.zst").write_text("a")

        parts = unified_log_parts(_session_info(tmp_path))

        assert [p.name for p in parts] == ["session.9999.log.zst", "session.10000.log"]

    def test_returns_empty_list_when_no_parts_exist(self, tmp_path):
        assert unified_log_parts(_session_info(tmp_path)) == []

    def test_returns_empty_list_when_the_session_folder_is_gone(self, tmp_path):
        assert unified_log_parts(_session_info(tmp_path / "deleted")) == []


class TestExistingPart:
    def test_returns_a_part_that_still_exists(self, tmp_path):
        part = tmp_path / "session.0000.log"
        part.write_text("a")
        (tmp_path / "session.0000.log.zst").write_text("a")

        assert existing_part(part) == part

    def test_falls_back_to_the_compressed_sibling_of_a_vanished_plain_part(self, tmp_path):
        """Listed by unified_log_parts(), then compressed and unlinked before the reader got
        to it."""
        compressed = tmp_path / "session.0000.log.zst"
        compressed.write_text("a")

        assert existing_part(tmp_path / "session.0000.log") == compressed

    def test_returns_none_when_a_plain_part_and_its_sibling_are_both_gone(self, tmp_path):
        assert existing_part(tmp_path / "session.0000.log") is None

    def test_returns_none_for_a_vanished_compressed_part(self, tmp_path):
        """No `.zst.zst` lookup - a compressed part has no further fallback."""
        (tmp_path / "session.0000.log.zst.zst").write_text("a")

        assert existing_part(tmp_path / "session.0000.log.zst") is None
