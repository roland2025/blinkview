# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink replay --list`: the table/JSON on stdout, hints on stderr, -p/-n filtering."""

import json
from argparse import ArgumentParser

import pytest

from blinkview.__main__ import run_replay
from blinkview.ui.cli_args import setup_replay_parser


def _write_session(log_dir, folder_name, profile, created_at):
    session_dir = log_dir / "proj" / folder_name
    session_dir.mkdir(parents=True)
    meta = {
        "session_id": folder_name,
        "status": "finished",
        "created_at": created_at,
        "duration_seconds": 65.0,
        "project": {"display_name": "bench"},
        "config": {"profile": profile},
    }
    (session_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    (session_dir / "session.0000.log").write_text("data")


def _parse(*argv):
    parser = ArgumentParser()
    setup_replay_parser(parser)
    return parser.parse_args(list(argv))


@pytest.fixture
def run_list(tmp_path, monkeypatch, capsys):
    _write_session(tmp_path, "20260927_100000_nrf_bench", "nrf", "2026-09-27T07:00:00+00:00Z")
    _write_session(tmp_path, "20260928_100000_adb_bench", "adb", "2026-09-28T07:00:00+00:00Z")
    _write_session(tmp_path, "20260929_100000_nrf_bench", "nrf", "2026-09-29T07:00:00+00:00Z")
    monkeypatch.setattr(
        "blinkview.utils.session_lister.resolve_log_root", lambda log_dir=None, settings=None: (tmp_path, "proj")
    )

    def run(*argv):
        run_replay(_parse("--list", *argv))
        return capsys.readouterr()

    return run


def _ids(out):
    return [line.split()[0] for line in out.splitlines()[1:]]


def test_lists_every_session_newest_first_with_hints_on_stderr(run_list):
    captured = run_list()

    assert captured.out.splitlines()[0].startswith("SESSION ID")
    assert _ids(captured.out) == [
        "20260929_100000_nrf_bench",
        "20260928_100000_adb_bench",
        "20260927_100000_nrf_bench",
    ]
    assert "1m 05s" in captured.out
    assert "blink export" not in captured.out
    assert "3 session(s)" in captured.err
    assert "blink export <SESSION ID>" in captured.err


def test_profile_and_limit_narrow_the_list(run_list):
    assert _ids(run_list("-p", "nrf").out) == ["20260929_100000_nrf_bench", "20260927_100000_nrf_bench"]

    captured = run_list("-p", "nrf", "-n", "1")
    assert _ids(captured.out) == ["20260929_100000_nrf_bench"]
    assert "1 of 2 session(s)" in captured.err


def test_json_is_the_only_output(run_list):
    captured = run_list("--json", "-n", "2")

    sessions = json.loads(captured.out)
    assert [s["session_id"] for s in sessions] == ["20260929_100000_nrf_bench", "20260928_100000_adb_bench"]
    assert sessions[0]["duration_seconds"] == 65.0
    assert sessions[0]["started_at"] == "2026-09-29T07:00:00+00:00"
    assert captured.err == ""


def test_json_with_no_sessions_is_an_empty_array(run_list):
    assert json.loads(run_list("--json", "-p", "nope").out) == []


def test_json_without_list_is_a_usage_error(run_list, capsys):
    with pytest.raises(SystemExit) as exit_info:
        run_replay(_parse("--json", "--last"))

    assert exit_info.value.code == 2
    assert "only apply to --list" in capsys.readouterr().err
