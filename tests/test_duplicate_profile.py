# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Tests for `blink switch <src> --copy <new>` (utils/project_settings.duplicate_profile)."""

import json

import pytest

from blinkview.utils.project_settings import duplicate_profile


@pytest.fixture
def project(tmp_path, monkeypatch):
    (tmp_path / ".blinkview").mkdir()
    (tmp_path / ".blinkview" / "project.json").write_text("{}")
    monkeypatch.setenv("BLINK_PROJECT_ROOT", str(tmp_path))
    profiles = tmp_path / ".blinkview" / "profiles"
    src = profiles / "can"
    (src / "sub").mkdir(parents=True)
    (src / "can.json").write_text(json.dumps({"sources": {"can": {}}}))
    (src / "can.gui_state.json").write_text("{}")
    (src / "notes.txt").write_text("keep")
    (src / "sub" / "can.json").write_text("nested")
    return profiles


def test_copies_and_renames_profile_prefixed_files(project):
    dst = duplicate_profile("can", "can_copy")

    assert dst == project / "can_copy"
    assert sorted(p.name for p in dst.iterdir()) == ["can_copy.gui_state.json", "can_copy.json", "notes.txt", "sub"]
    # Contents are untouched - "can" inside the config is a source name, not the profile name.
    assert json.loads((dst / "can_copy.json").read_text()) == {"sources": {"can": {}}}
    # Only top-level files are renamed; FileManager never looks in subfolders by profile name.
    assert (dst / "sub" / "can.json").read_text() == "nested"
    # Source is left intact.
    assert sorted(p.name for p in (project / "can").iterdir()) == ["can.gui_state.json", "can.json", "notes.txt", "sub"]


def test_refuses_existing_target(project):
    (project / "taken").mkdir()
    with pytest.raises(FileExistsError):
        duplicate_profile("can", "taken")


def test_refuses_missing_source(project):
    with pytest.raises(FileNotFoundError):
        duplicate_profile("nope", "new")
    assert not (project / "new").exists()


@pytest.mark.parametrize("name", ["bad name", "a-b", "_x", "x__y", ""])
def test_refuses_names_filemanager_would_sanitize(project, name):
    with pytest.raises(ValueError):
        duplicate_profile("can", name)


def test_view_presets_file_follows_the_profile(project):
    """Layout presets (plans/view-layout-presets.md) live in <profile>.view_presets.json and must
    come along, renamed, or the copied profile would start with no presets."""
    (project / "can" / "can.view_presets.json").write_text('{"version": 1, "presets": {}}')

    dst = duplicate_profile("can", "can_copy")

    assert (dst / "can_copy.view_presets.json").read_text() == '{"version": 1, "presets": {}}'
