# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink tail` (utils/session_tail.py): its loop against a scripted instance, the command in a
fresh process, and end to end against a real running `blink` fed through a socket."""

import json
import os
import queue
import socket
import subprocess
import sys
import threading
import time
from argparse import ArgumentParser
from pathlib import Path

import pytest

from blinkview.core.agent_api import AgentApiError, call_instance
from blinkview.utils import session_tail
from blinkview.utils.session_export import line_timestamp
from blinkview.utils.session_tail import run_tail, setup_tail_parser

REPO_ROOT = Path(__file__).resolve().parents[1]


def _line(device: str, module: str, message: str) -> str:
    return f"2026-10-19T12:00:00.000000Z I {device} {module}: {message}\n"


class FakeInstance:
    """BlinkView.connect()'s result: read_logs replies come from `replies`; once they run out
    the instance is gone."""

    pid = 4242
    project = "C:/work/demo"
    profile = "default"

    def __init__(self, replies, seq=100, devices=("board", "system"), session="s1", session_dir=None):
        self.replies = list(replies)
        self.seq = seq
        self.devices = devices
        self.session = session
        self.session_dir = session_dir
        self.calls = []

    def status(self):
        return {"session_id": self.session, "session_dir": self.session_dir, "seq": self.seq}

    def list_modules(self, **args):
        return {"devices": [{"name": name, "modules": []} for name in self.devices]}

    def read_logs(self, **args):
        self.calls.append(args)
        if not self.replies:
            raise AgentApiError("BlinkView instance (pid 4242) did not answer")
        reply = self.replies.pop(0)
        return {"session_id": self.session, "lines": 0, "truncated": False, **reply}


@pytest.fixture
def instance(monkeypatch):
    holder = {}

    def connect(pid=None):
        if "instance" not in holder:
            raise AgentApiError("No BlinkView instance with an agent server is running.")
        return holder["instance"]

    monkeypatch.setattr(session_tail.BlinkView, "connect", staticmethod(connect))
    monkeypatch.setattr(session_tail, "list_instances", lambda: [])  # gone, once replies run out

    def install(*args, **kwargs):
        holder["instance"] = FakeInstance(*args, **kwargs)
        return holder["instance"]

    return install


def _tail(argv):
    parser = ArgumentParser(prog="blink tail")
    setup_tail_parser(parser)
    with pytest.raises(SystemExit) as exc:
        run_tail(parser.parse_args(argv))
    return exc.value.code


def _err_lines(err: bytes) -> list[str]:
    return err.decode().splitlines()


class TestFollow:
    def test_prints_filtered_lines_and_follows_the_cursor(self, instance, capsysbinary):
        fake = instance(
            [
                {"text": _line("board", "app", "event one") + _line("board", "app", "noise"), "next_seq": 105},
                {"text": "", "next_seq": 105},
                {"text": _line("board", "net", "event two"), "next_seq": 107},
            ]
        )

        code = _tail(["-e", "event"])

        out, err = capsysbinary.readouterr()
        assert out == (_line("board", "app", "event one") + _line("board", "net", "event two")).encode()
        assert [call["after_seq"] for call in fake.calls] == [100, 105, 105, 107]
        assert all(call["export_format"] is True for call in fake.calls)
        assert code == 1  # the scripted instance went away
        lines = _err_lines(err)
        assert lines[0].startswith("blink tail: following pid 4242 (project C:/work/demo, profile default), session s1")
        assert "instance pid 4242 is gone" in lines[-2]
        assert lines[-1] == "blink tail: next_seq 107"

    def test_bytes_that_are_not_utf8_come_out_as_they_were(self, instance, capsysbinary):
        raw = b"2026-10-19T12:00:00.000000Z I board app: bad \xff"
        instance([{"text": raw.decode("utf-8", "surrogateescape") + "\n", "next_seq": 101}])

        _tail([])

        assert capsysbinary.readouterr().out == raw + b"\n"

    def test_max_matches_stops_mid_batch_with_exit_0(self, instance, capsysbinary):
        text = "".join(_line("board", "app", f"event {i}") for i in range(5))
        instance([{"text": text, "next_seq": 110}])

        code = _tail(["--max-matches", "2"])

        out, err = capsysbinary.readouterr()
        assert code == 0
        assert out.decode().splitlines() == [_line("board", "app", f"event {i}").rstrip("\n") for i in range(2)]
        assert _err_lines(err)[-2:] == ["blink tail: --max-matches 2 reached", "blink tail: next_seq 110"]

    def test_timeout_exits_0(self, instance, capsysbinary, monkeypatch):
        monkeypatch.setattr(session_tail, "WAIT_SECONDS_PER_CALL", 0.01)

        class Idle(FakeInstance):
            def read_logs(self, **args):
                time.sleep(args["wait_s"])
                return {"session_id": "s1", "text": "", "next_seq": self.seq}

        fake = Idle([])
        monkeypatch.setattr(session_tail.BlinkView, "connect", staticmethod(lambda pid=None: fake))

        assert _tail(["--timeout", "0.2"]) == 0
        assert _err_lines(capsysbinary.readouterr().err)[-2:] == [
            "blink tail: --timeout 0.2 s reached",
            "blink tail: next_seq 100",
        ]

    def test_reports_rotation_and_lost_lines(self, instance, capsysbinary):
        fake = instance([{"text": "", "next_seq": 100}])
        fake.replies.append(
            {"text": _line("board", "app", "new"), "next_seq": 140, "session_id": "s2", "lost_rows": 12}
        )

        _tail([])

        err = capsysbinary.readouterr().err.decode()
        assert "blink tail: session rotated: s1 -> s2" in err
        assert "warning: 12 line(s) after seq 100 were lost" in err
        assert "`blink export s1` has them" in err

    def test_lost_lines_without_a_rotation(self, instance, capsysbinary):
        instance([{"text": "", "next_seq": 300, "lost_rows": 5}])

        _tail([])

        err = capsysbinary.readouterr().err.decode()
        assert "warning: 5 line(s) after seq 100 were lost" in err and "blink export" not in err


class TestStart:
    def test_last_n_and_from_seq_set_the_first_cursor(self, instance, capsysbinary):
        fake = instance([], seq=1000)
        _tail(["--last-n", "200"])
        fake2 = instance([], seq=1000)
        _tail(["--from-seq", "950"])
        fake3 = instance([], seq=50)
        _tail(["--last-n", "200"])

        assert [f.calls[0]["after_seq"] for f in (fake, fake2, fake3)] == [800, 950, 0]

    def test_from_seq_ahead_of_the_instance_is_a_command_line_error(self, instance, capsysbinary):
        instance([], seq=10)

        assert _tail(["--from-seq", "11"]) == 2
        err = capsysbinary.readouterr().err.decode()
        assert "ahead of this instance's log" in err and "next_seq" not in err

    def test_no_instance_is_exit_1(self, instance, capsysbinary):
        assert _tail([]) == 1
        assert "No BlinkView instance" in capsysbinary.readouterr().err.decode()

    @pytest.mark.parametrize(
        "argv, message",
        [
            (["-e", "("], "not a valid regular expression"),
            (["--device", "a b"], "--device 'a b'"),
            (["--last-n", "-1"], "must not be negative"),
            (["--max-matches", "0"], "at least 1"),
            (["--timeout", "0"], "more than 0"),
        ],
    )
    def test_bad_values_are_exit_2_before_connecting(self, instance, capsysbinary, argv, message):
        """No instance installed: these fail before one is looked for."""
        assert _tail(argv) == 2
        assert message in capsysbinary.readouterr().err.decode()

    def test_argparse_errors_are_exit_2(self):
        parser = ArgumentParser(prog="blink tail")
        setup_tail_parser(parser)
        for argv in (["--from-seq", "1", "--last-n", "1"], ["--min-level", "LOUD"]):
            with pytest.raises(SystemExit) as exc:
                parser.parse_args(argv)
            assert exc.value.code == 2


class TestServerFilters:
    def test_one_known_device_and_min_level_are_filtered_by_the_server(self, instance, capsysbinary):
        fake = instance([])
        _tail(["--device", "board", "--min-level", "warn"])

        call = fake.calls[0]
        assert call["device"] == "board" and call["min_level"] == "WARN"
        assert "include_system" not in call

    def test_an_unknown_device_is_filtered_here_so_the_follow_can_start_first(self, instance, capsysbinary):
        fake = instance([{"text": _line("board", "app", "x") + _line("late", "app", "y"), "next_seq": 102}])
        _tail(["--device", "late"])

        assert "device" not in fake.calls[0]
        assert capsysbinary.readouterr().out == _line("late", "app", "y").encode()

    def test_several_devices_are_filtered_here_and_system_needs_include_system(self, instance, capsysbinary):
        fake = instance([])
        _tail(["--device", "board", "--device", "system"])
        assert "device" not in fake.calls[0] and fake.calls[0]["include_system"] is True

        fake = instance([])
        _tail(["--include-system"])
        assert fake.calls[0]["include_system"] is True

    def test_preset_comes_from_the_running_sessions_profile(self, instance, capsysbinary, tmp_path):
        profile = tmp_path / "profiles" / "default"
        profile.mkdir(parents=True)
        (profile / "default.export_presets.json").write_text(json.dumps({"watch": {"match": ["event"]}}))
        session = tmp_path / "logs" / "s1"
        session.mkdir(parents=True)
        (session / "metadata.json").write_text(json.dumps({"config": {"source_file": str(profile / "default.json")}}))
        text = _line("board", "app", "event") + _line("board", "app", "other")
        instance([{"text": text, "next_seq": 102}], session_dir=str(session))

        _tail(["--preset", "watch"])

        out, err = capsysbinary.readouterr()
        assert out == _line("board", "app", "event").encode()
        assert "preset 'watch' from" in err.decode()


def _subprocess_env(tmp_path: Path) -> dict:
    env = dict(os.environ)
    env["HOME"] = env["USERPROFILE"] = str(tmp_path / "home")
    env["BLINK_PROJECT_ROOT"] = str(tmp_path / "no_project")
    return env


class TestFreshProcess:
    def test_help_runs_and_the_command_imports_nothing_heavy(self, tmp_path):
        """A subprocess: in-process, sys.modules holds whatever other tests imported, and a
        subcommand that fails to register only shows in a fresh `blink`."""
        code = (
            "import sys, atexit\n"
            "atexit.register(lambda: sys.stderr.write('MODULES:' + ','.join(sorted(sys.modules)) + '\\n'))\n"
            "sys.argv = ['blink', 'tail', '--help']\n"
            "from blinkview.__main__ import main\n"
            "main()\n"
        )

        result = subprocess.run(
            [sys.executable, "-c", code], env=_subprocess_env(tmp_path), capture_output=True, text=True, timeout=60
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.startswith("usage: blink tail")
        assert "--max-matches" in result.stdout and "-e, --match REGEX" in result.stdout
        modules = result.stderr.split("MODULES:")[1].strip().split(",")
        heavy = [
            m
            for m in modules
            if m.split(".")[0] in ("numba", "llvmlite", "PySide6", "qtpy", "numpy")
            or m.startswith(("blinkview.parsers", "blinkview.ops", "blinkview.storage", "blinkview.core.id_registry"))
        ]
        assert heavy == []

    def test_listed_in_blink_help(self, tmp_path):
        result = subprocess.run(
            [sys.executable, "-m", "blinkview", "--help"],
            env=_subprocess_env(tmp_path),
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0 and "tail" in result.stdout


STARTUP_TIMEOUT_SECONDS = 120
SHUTDOWN_TIMEOUT_SECONDS = 15


class _Lines:
    """A pipe read on a thread: each line with the time it arrived."""

    def __init__(self, pipe):
        self.queue = queue.Queue()
        self.seen = []
        threading.Thread(target=self._read, args=(pipe,), daemon=True).start()

    def _read(self, pipe):
        for raw in iter(pipe.readline, b""):
            self.queue.put((time.monotonic(), raw.rstrip(b"\r\n")))
        self.queue.put((time.monotonic(), None))

    def next(self, timeout):
        arrived, line = self.queue.get(timeout=timeout)
        if line is not None:
            self.seen.append(line)
        return arrived, line

    def wait_for(self, needle: bytes, timeout):
        deadline = time.monotonic() + timeout
        while True:
            _arrived, line = self.next(max(deadline - time.monotonic(), 0.01))
            if line is None:
                raise AssertionError(f"stream ended waiting for {needle!r}; got {self.seen}")
            if needle in line:
                return line


def _stop(proc):
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)


@pytest.mark.skipif(
    sys.platform == "linux",
    reason="ui/run.py forces QT_QPA_PLATFORM=xcb on Linux, which needs a display "
    "(see tests/test_blink_gui_subprocess.py).",
)
def test_tail_follows_a_running_blink_across_a_session_rotation(tmp_path):
    """Two real processes, like tests/test_blink_mcp_subprocess.py: `blink gui` on a scratch
    profile with a tcp_client source this test feeds, and `blink tail` following it."""
    fake_home = tmp_path / "home"
    (fake_home / ".blinkview").mkdir(parents=True)
    (fake_home / ".blinkview" / "settings.json").write_text(json.dumps({"update": {"path": str(REPO_ROOT)}}))

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(STARTUP_TIMEOUT_SECONDS)

    config_path = tmp_path / "config.json"
    tcp = {"enabled": True, "type": "tcp_client", "host": "127.0.0.1", "port": listener.getsockname()[1]}
    config_path.write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {"src_board": dict(tcp, name="board")},
                "pipelines": {
                    "pipe_board": {"enabled": True, "type": "default", "name": "board", "sources_": ["src_board"]},
                    "pipe_mcp": {"enabled": True, "type": "mcp_server", "name": "mcp"},
                },
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )

    env = dict(os.environ)
    env["QT_QPA_PLATFORM"] = "offscreen"
    env["QT_API"] = "pyside6"
    env["HOME"] = env["USERPROFILE"] = str(fake_home)
    env["BLINK_PROJECT_ROOT"] = str(tmp_path / "fake_project_root")
    env["PYTHONUNBUFFERED"] = "1"
    env.pop("NUMBA_CACHE_DIR", None)

    gui_log_path = tmp_path / "gui.log"
    procs = []
    conn = None
    with open(gui_log_path, "w", encoding="utf-8", errors="replace") as gui_log:
        gui = subprocess.Popen(
            [sys.executable, "-u", "-m", "blinkview", "gui", "-s", "tail_e2e", "-l", str(tmp_path / "logs")]
            + ["-c", str(config_path)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=gui_log,
            stderr=subprocess.STDOUT,
        )
        procs.append(gui)
        try:
            try:
                conn, _addr = listener.accept()
            except socket.timeout:
                pytest.fail(f"blink never connected its source.\n{gui_log_path.read_text(errors='replace')[-3000:]}")
            conn.sendall(b"priming line\n")  # only syncs the parser's framing (FrameDecoder.frame_resync_on_start)

            run_dir = fake_home / ".blinkview" / "run"
            deadline = time.monotonic() + 30
            while not list(run_dir.glob("*.json")):
                assert time.monotonic() < deadline, "the mcp_server pipeline never wrote its run file"
                time.sleep(0.1)
            instance = json.loads(next(run_dir.glob("*.json")).read_text(encoding="utf-8"))

            def tail(*argv):
                proc = subprocess.Popen(
                    [sys.executable, "-m", "blinkview", "tail", *argv],
                    cwd=str(tmp_path),
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                procs.append(proc)
                return proc, _Lines(proc.stdout), _Lines(proc.stderr)

            proc, out, err = tail("--device", "board", "-e", "event", "--exclude", "skip", "--max-matches", "3")
            err.wait_for(b"following pid", timeout=60)

            # A new line comes out within a second - no file-logger flush delay.
            sent = time.monotonic()
            conn.sendall(b"event one\nnoise\nevent skip\n")
            arrived, line = out.next(timeout=10)
            assert (
                line_timestamp(line) is not None
                and line[27:].startswith(b" I board ")
                and line.endswith(b": event one")
            )
            assert arrived - sent < 1.0, f"took {arrived - sent:.2f} s"

            conn.sendall(b"event two\n")
            assert out.next(timeout=10)[1].endswith(b": event two")

            rotated = call_instance(instance, "rotate_session", {"name": "second"}, timeout=150)
            err.wait_for(b"session rotated", timeout=10)

            conn.sendall(b"event three\n")
            assert out.next(timeout=10)[1].endswith(b": event three")

            assert proc.wait(timeout=30) == 0
            assert out.next(timeout=5)[1] is None  # exactly three lines
            err.wait_for(b"--max-matches 3 reached", timeout=5)
            final = err.wait_for(b"next_seq", timeout=5)
            stderr_text = b"\n".join(err.seen).decode()
            assert rotated["old_session_id"] in stderr_text and rotated["session_id"] in stderr_text
            assert "lost" not in stderr_text

            # Resuming from the printed cursor gives what came after it, and nothing before.
            conn.sendall(b"event four\n")
            time.sleep(0.5)
            resumed, out2, err2 = tail("--from-seq", final.split()[-1].decode(), "--device", "board")
            assert out2.next(timeout=60)[1].endswith(b": event four")

            # The instance goes away: exit 1, with the cursor.
            gui.kill()
            gui.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            assert resumed.wait(timeout=30) == 1
            err2.wait_for(b"is gone", timeout=5)
            err2.wait_for(b"next_seq", timeout=5)
        finally:
            if conn is not None:
                conn.close()
            listener.close()
            for proc in procs:
                _stop(proc)
