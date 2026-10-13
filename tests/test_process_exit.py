# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import atexit
import subprocess
import sys
import textwrap
import threading

import pytest

from blinkview import __main__ as main_module
from blinkview.core import base_daemon
from blinkview.utils import process_exit


@pytest.fixture
def terminated(monkeypatch):
    """Records what the process would have been ended with, instead of ending the test run."""
    codes = []
    monkeypatch.setattr(process_exit, "_terminate_process", codes.append)
    monkeypatch.setattr(atexit, "_run_exitfuncs", lambda: codes.append("atexit"))
    return codes


@pytest.fixture
def stuck_thread():
    release = threading.Event()
    thread = threading.Thread(target=release.wait, daemon=True, name="StuckReader")
    thread.start()
    base_daemon._stuck_threads.append(thread)
    yield thread
    release.set()
    thread.join()


class TestTerminateIfThreadsStuck:
    def test_returns_when_no_thread_is_stuck(self, terminated):
        process_exit.terminate_if_threads_stuck(3)
        assert terminated == []

    def test_a_thread_that_ended_after_all_is_not_stuck(self, terminated):
        thread = threading.Thread(target=lambda: None)
        thread.start()
        thread.join()
        base_daemon._stuck_threads.append(thread)

        process_exit.terminate_if_threads_stuck(3)

        assert terminated == []

    def test_runs_the_exit_handlers_then_terminates_with_the_code(self, terminated, stuck_thread, capsys):
        process_exit.terminate_if_threads_stuck(3)

        assert terminated == ["atexit", 3]
        assert "StuckReader" in capsys.readouterr().err

    def test_no_code_means_success(self, terminated, stuck_thread):
        process_exit.terminate_if_threads_stuck(None)
        assert terminated[-1] == 0

    def test_a_message_as_exit_code_is_printed_and_becomes_one(self, terminated, stuck_thread, capsys):
        process_exit.terminate_if_threads_stuck("Error: bad argument")

        assert terminated[-1] == 1
        assert "Error: bad argument" in capsys.readouterr().err


class TestMain:
    def test_main_passes_the_exit_code_of_a_sys_exit(self, terminated, stuck_thread, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["blink", "cli"])
        monkeypatch.setattr(main_module, "run_cli", lambda args: sys.exit(4))

        with pytest.raises(SystemExit):  # the terminate stand-in returns
            main_module.main()

        assert terminated[-1] == 4

    def test_main_terminates_after_a_normal_return(self, terminated, stuck_thread, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["blink", "cli"])
        monkeypatch.setattr(main_module, "run_cli", lambda args: None)

        main_module.main()

        assert terminated[-1] == 0

    def test_main_exits_normally_when_nothing_is_stuck(self, terminated, monkeypatch):
        monkeypatch.setattr(sys, "argv", ["blink", "cli"])
        monkeypatch.setattr(main_module, "run_cli", lambda args: sys.exit(4))

        with pytest.raises(SystemExit) as exc:
            main_module.main()

        assert exc.value.code == 4
        assert terminated == []


def test_a_real_process_ends_with_its_exit_code_and_its_exit_handlers_run(tmp_path):
    """The real TerminateProcess/os._exit, which no in-process test can call."""
    marker = tmp_path / "atexit_ran"
    script = textwrap.dedent(
        f"""
        import atexit, sys, time
        from blinkview.core.base_daemon import BaseDaemon
        from blinkview.utils.process_exit import terminate_if_threads_stuck

        atexit.register(lambda: open({str(marker)!r}, "w").close())

        daemon = BaseDaemon()
        daemon.logger = None
        daemon.enabled = True
        daemon.run = lambda: time.sleep(600)  # ignores the stop event
        daemon.start()
        daemon.stop(timeout=0.1)

        terminate_if_threads_stuck(7)
        sys.exit(99)  # not reached
        """
    )

    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60)

    assert result.returncode == 7, result.stderr
    assert marker.exists()
    assert "did not stop within 0.1 seconds" in result.stderr
    assert "time.sleep(600)" in result.stderr  # where the thread is blocked
