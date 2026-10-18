# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink borrow` / `blink return` argument handling and the PlatformIO script it generates.
The real subprocess runs against a lending reader are in test_source_borrow.py."""

import os
import subprocess
import sys

import pytest

from blinkview import borrow
from blinkview.utils import borrow_cli
from blinkview.utils.borrow_cli import PIO_SCRIPT, parse_borrow_args


@pytest.fixture
def borrow_dir(tmp_path, monkeypatch):
    directory = tmp_path / "borrow"
    monkeypatch.setenv(borrow.BORROW_DIR_ENV, str(directory))
    return directory


class TestParseBorrowArgs:
    def test_command_after_double_dash(self):
        args = parse_borrow_args(["COM5", "--", "pio", "run", "-t", "upload"])
        assert args.name == "COM5"
        assert args.command == ["pio", "run", "-t", "upload"]

    def test_options_after_the_name(self):
        """argparse before 3.12 mishandles `NAME --opt X -- cmd` with an optional positional;
        the command is split off before argparse sees it."""
        args = parse_borrow_args(["COM5", "--timeout", "10", "--by", "me", "--", "x", "--timeout", "1"])
        assert (args.name, args.timeout, args.by) == ("COM5", 10.0, "me")
        assert args.command == ["x", "--timeout", "1"]

    def test_without_a_command(self):
        args = parse_borrow_args(["COM5", "--strict"])
        assert args.command == [] and args.strict

    def test_a_command_without_double_dash_is_an_error(self, capsys):
        with pytest.raises(SystemExit):
            parse_borrow_args(["COM5", "pio", "run"])
        assert "put the command after --" in capsys.readouterr().err


class TestRunBorrow:
    def test_needs_a_name(self, borrow_dir):
        with pytest.raises(SystemExit) as e:
            borrow_cli.run_borrow_args(parse_borrow_args([]))
        assert e.value.code == 2

    def test_split_borrow_leaves_the_file_with_the_callers_pid(self, borrow_dir, monkeypatch):
        monkeypatch.setattr(borrow, "owner_pid", lambda: 4321)
        borrow_cli.run_borrow_args(parse_borrow_args(["COM5", "--timeout", "0", "--by", "pio"]))

        info = borrow.read_borrow(borrow.resolve("COM5"))
        assert info["pid"] == 4321 and info["by"] == "pio"

    def test_a_stale_ack_is_removed_before_borrowing(self, borrow_dir):
        """Otherwise the tool would go ahead before blink has closed the port."""
        paths = borrow.resolve("COM5")
        borrow.write_lent(paths, {})

        borrow_cli.run_borrow_args(parse_borrow_args(["COM5", "--timeout", "0"]))

        assert not paths.lent.exists()

    def test_command_runs_and_the_borrow_is_removed(self, borrow_dir):
        with pytest.raises(SystemExit) as e:
            borrow_cli.run_borrow_args(
                parse_borrow_args(["COM5", "--timeout", "0", "--", sys.executable, "-c", "raise SystemExit(4)"])
            )
        assert e.value.code == 4
        assert not borrow.resolve("COM5").borrow.exists()

    def test_with_a_command_the_owner_is_this_process(self, borrow_dir, monkeypatch):
        seen = {}

        def fake_run(command):
            seen["info"] = borrow.read_borrow(borrow.resolve("COM5"))
            return 0

        monkeypatch.setattr(borrow_cli, "_run", fake_run)
        with pytest.raises(SystemExit):
            borrow_cli.run_borrow_args(parse_borrow_args(["COM5", "--timeout", "0", "--", "esptool.py", "write"]))

        assert seen["info"]["pid"] == os.getpid()
        assert seen["info"]["by"] == "esptool"

    def test_missing_command_is_127(self, borrow_dir):
        with pytest.raises(SystemExit) as e:
            borrow_cli.run_borrow_args(parse_borrow_args(["COM5", "--timeout", "0", "--", "no-such-command-xyz"]))
        assert e.value.code == 127

    def test_return_removes_the_borrow(self, borrow_dir):
        paths = borrow.resolve("COM5")
        borrow.write_borrow(paths, pid=1)
        borrow_cli.run_return(type("Args", (), {"name": "COM5"})())
        assert not paths.borrow.exists()

    def test_status_lists_borrows(self, borrow_dir, capsys):
        borrow.write_borrow(borrow.resolve("ttyUSB0"), pid=os.getpid(), by="pio")
        borrow_cli.run_borrow_args(parse_borrow_args(["--status"]))
        out = capsys.readouterr().out
        assert "ttyUSB0: waiting for blink, borrowed by pio" in out
        assert "running" in out


class FakeSConsEnv:
    def __init__(self, upload_port):
        self.upload_port = upload_port
        self.pre, self.post = {}, {}

    def subst(self, text):
        assert text == "$UPLOAD_PORT"
        return self.upload_port

    def AddPreAction(self, target, action):
        self.pre[target] = action

    def AddPostAction(self, target, action):
        self.post[target] = action


class TestPioScript:
    def _load(self, env, calls, monkeypatch):
        def fake_run(argv, check):
            calls.append(argv)
            return subprocess.CompletedProcess(argv, 0)

        monkeypatch.setattr(subprocess, "run", fake_run)
        exec(compile(PIO_SCRIPT, "blink_borrow.py", "exec"), {"Import": lambda name: None, "env": env})

    def test_borrows_before_and_returns_after_the_upload(self, monkeypatch):
        env, calls = FakeSConsEnv("COM5"), []
        self._load(env, calls, monkeypatch)

        # SCons treats an action's non-None return as failure: both must return None.
        assert env.pre["upload"](None, None, env) is None
        assert env.post["upload"](None, None, env) is None
        assert calls == [["blink", "borrow", "COM5", "--by", "pio"], ["blink", "return", "COM5"]]

    def test_no_upload_port_borrows_nothing(self, monkeypatch):
        env, calls = FakeSConsEnv(""), []
        self._load(env, calls, monkeypatch)
        env.pre["upload"](None, None, env)
        assert calls == []

    def test_printed_by_the_cli(self):
        out = subprocess.run(
            [sys.executable, "-m", "blinkview", "borrow", "--pio-script"], capture_output=True, text=True, timeout=30
        ).stdout
        assert out == PIO_SCRIPT
