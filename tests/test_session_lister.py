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
        newer_dir = _write_session(
            project_dir, "newer", {"session_id": "newer", "created_at": "2026-06-01T00:00:00Z"}
        )
        (newer_dir / "session.000").write_text("data")
        older_dir = project_dir / "older"
        (older_dir / "session.000").write_text("data")

        result = resolve_session(tmp_path, "proj", last=True)

        assert result.session_id == "newer"

    def test_matches_by_exact_session_id(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(project_dir, "session1", {"session_id": "session1"})
        (session_dir / "session.000").write_text("data")

        result = resolve_session(tmp_path, "proj", name="session1")

        assert result.session_id == "session1"

    def test_matches_by_exact_display_name(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(
            project_dir, "session1", {"session_id": "session1", "project": {"display_name": "My Run"}}
        )
        (session_dir / "session.000").write_text("data")

        result = resolve_session(tmp_path, "proj", name="My Run")

        assert result.session_id == "session1"

    def test_matches_by_case_insensitive_substring(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(
            project_dir, "session1", {"session_id": "session1", "project": {"display_name": "My Special Run"}}
        )
        (session_dir / "session.000").write_text("data")

        result = resolve_session(tmp_path, "proj", name="special")

        assert result.session_id == "session1"

    def test_no_name_and_no_last_returns_none(self, tmp_path):
        project_dir = tmp_path / "proj"
        session_dir = _write_session(project_dir, "session1", {"session_id": "session1"})
        (session_dir / "session.000").write_text("data")

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
        (session_dir / "session.000").write_text("data")

        assert resolve_session(tmp_path, "proj", name="nonexistent") is None


class TestUnifiedLogParts:
    def test_returns_sorted_session_dot_star_files(self, tmp_path):
        info = SessionInfo(
            session_id="s1",
            path=tmp_path,
            display_name="s1",
            profile="",
            status="unknown",
            created_at=None,
            finished_at=None,
            duration_seconds=None,
        )
        (tmp_path / "session.002").write_text("b")
        (tmp_path / "session.000").write_text("a")
        (tmp_path / "session.001").write_text("c")
        (tmp_path / "other.txt").write_text("x")

        parts = unified_log_parts(info)

        assert [p.name for p in parts] == ["session.000", "session.001", "session.002"]

    def test_returns_empty_list_when_no_parts_exist(self, tmp_path):
        info = SessionInfo(
            session_id="s1",
            path=tmp_path,
            display_name="s1",
            profile="",
            status="unknown",
            created_at=None,
            finished_at=None,
            duration_seconds=None,
        )

        assert unified_log_parts(info) == []
