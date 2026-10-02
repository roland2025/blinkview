# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink replay <session>` defaults to the profile the session was recorded with
(metadata.json config.profile) - see __main__.apply_session_profile."""

from types import SimpleNamespace

import pytest

from blinkview.__main__ import apply_session_profile


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    (tmp_path / "profiles" / "hw10").mkdir(parents=True)
    monkeypatch.setattr("blinkview.utils.project_settings.get_workspace_dir", lambda: tmp_path)
    return tmp_path


def make_args(profile=None, config=None):
    return SimpleNamespace(profile=profile, config=config)


def test_uses_the_sessions_profile_when_none_given(workspace):
    args = make_args()
    apply_session_profile(args, SimpleNamespace(profile="hw10"))
    assert args.profile == "hw10"


def test_explicit_profile_wins(workspace):
    args = make_args(profile="other")
    apply_session_profile(args, SimpleNamespace(profile="hw10"))
    assert args.profile == "other"


def test_explicit_config_wins(workspace):
    """--profile and --config are mutually exclusive, so setting profile here would conflict."""
    args = make_args(config="some.json")
    apply_session_profile(args, SimpleNamespace(profile="hw10"))
    assert args.profile is None


def test_missing_profile_falls_back_to_active_instead_of_creating_one(workspace):
    args = make_args()
    apply_session_profile(args, SimpleNamespace(profile="deleted_profile"))
    assert args.profile is None
    assert not (workspace / "profiles" / "deleted_profile").exists()


def test_session_without_a_recorded_profile(workspace):
    args = make_args()
    apply_session_profile(args, SimpleNamespace(profile=""))
    assert args.profile is None
