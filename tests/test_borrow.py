# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The port-borrowing protocol module (blinkview/borrow.py): paths, the name rule, the borrow
file's JSON and the pid checks. Both the source watcher and `blink borrow` build on it."""

import json
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from blinkview import borrow


@pytest.fixture
def borrow_dir(tmp_path, monkeypatch):
    directory = tmp_path / "borrow"
    monkeypatch.setenv(borrow.BORROW_DIR_ENV, str(directory))
    return directory


class TestBorrowDir:
    def test_defaults_to_blinkview_home(self, monkeypatch):
        monkeypatch.delenv(borrow.BORROW_DIR_ENV, raising=False)
        assert borrow.borrow_dir() == borrow.Path.home() / ".blinkview" / "borrow"

    def test_env_overrides_it(self, borrow_dir):
        assert borrow.borrow_dir() == borrow_dir


class TestNameFromPort:
    @pytest.mark.parametrize(
        "url, name",
        [
            ("COM5", "COM5"),
            ("  COM12 ", "COM12"),
            ("\\\\.\\COM10", "COM10"),
            ("/dev/ttyUSB0", "ttyUSB0"),
            ("/dev/cu.usbserial-0001", "cu.usbserial-0001"),
            ("socket://127.0.0.1:1234", "socket_127.0.0.1_1234"),
            ("rfc2217://host:2217", "rfc2217_host_2217"),
            ("", ""),
        ],
    )
    def test_names(self, url, name):
        assert borrow.name_from_port(url) == name


class TestResolve:
    def test_plain_name_lives_in_the_borrow_dir(self, borrow_dir):
        paths = borrow.resolve("ttyUSB0")
        assert paths.borrow == borrow_dir / "ttyUSB0.borrow"
        assert paths.lent == borrow_dir / "ttyUSB0.lent"

    @pytest.mark.parametrize(
        "name, stem", [("COM5", "COM5_"), ("com1", "com1_"), ("NUL", "NUL_"), ("lpt3.x", "lpt3_.x"), ("COM10", "COM10")]
    )
    def test_windows_device_names_are_escaped(self, name, stem, borrow_dir):
        """On Windows `COM5.borrow` opens the serial port COM5, not a file."""
        assert borrow.resolve(name).borrow == borrow_dir / f"{stem}.borrow"

    def test_an_escaped_name_is_a_real_file(self, borrow_dir):
        paths = borrow.resolve("COM5")
        borrow.write_borrow(paths, pid=1)
        assert paths.borrow.is_file()
        assert [p.name for p in borrow_dir.iterdir()] == ["COM5_.borrow"]

    def test_a_trailing_borrow_suffix_is_the_same_name(self, borrow_dir):
        assert borrow.resolve("esp32-main.borrow") == borrow.resolve("esp32-main")

    def test_unsafe_characters_are_replaced(self, borrow_dir):
        assert borrow.resolve("my board:1").borrow == borrow_dir / "my_board_1.borrow"

    def test_a_path_is_the_borrow_file_itself(self, tmp_path, borrow_dir):
        lock = tmp_path / "tools" / "flash.lock"
        paths = borrow.resolve(str(lock))
        assert paths.borrow == lock
        assert paths.lent == tmp_path / "tools" / "flash.lent"

    @pytest.mark.parametrize("name", ["", "   ", "..."])
    def test_unusable_names_raise(self, name, borrow_dir):
        with pytest.raises(ValueError):
            borrow.resolve(name)


class TestBorrowFile:
    def test_write_then_read_round_trips(self, borrow_dir):
        paths = borrow.resolve("COM5")
        written = borrow.write_borrow(paths, pid=1234, by="pio")

        assert borrow.read_borrow(paths) == written
        assert written["pid"] == 1234 and written["by"] == "pio"
        # Atomic write: no temp file left next to it.
        assert sorted(p.name for p in borrow_dir.iterdir()) == ["COM5_.borrow"]

    def test_no_file_is_no_borrow(self, borrow_dir):
        assert borrow.read_borrow(borrow.resolve("COM5")) is None

    @pytest.mark.parametrize("content", ["", "x", "[1, 2]"])
    def test_a_file_that_is_not_a_json_object_is_still_a_borrow(self, content, borrow_dir):
        paths = borrow.resolve("COM5")
        borrow_dir.mkdir()
        paths.borrow.write_text(content)
        assert borrow.read_borrow(paths) == {}

    def test_lent_ack(self, borrow_dir):
        paths = borrow.resolve("COM5")
        assert not borrow.is_lent(paths)
        borrow.write_lent(paths, {"pid": 1})
        assert borrow.is_lent(paths)
        assert json.loads(paths.lent.read_text()) == {"pid": 1}

    def test_remove_reports_whether_it_was_there(self, borrow_dir):
        paths = borrow.resolve("COM5")
        borrow.write_lent(paths, {})
        assert borrow.remove(paths.lent) is True
        assert borrow.remove(paths.lent) is False


class TestWaitForLent:
    def test_true_once_lent(self, borrow_dir):
        paths = borrow.resolve("COM5")
        borrow.write_lent(paths, {})
        assert borrow.wait_for_lent(paths, timeout_s=0.5) is True

    def test_false_on_timeout(self, borrow_dir):
        assert borrow.wait_for_lent(borrow.resolve("COM5"), timeout_s=0.1) is False


class TestPidAlive:
    def test_own_pid_is_alive(self):
        assert borrow.pid_alive(os.getpid()) is True

    def test_an_exited_process_is_gone(self):
        child = subprocess.Popen([sys.executable, "-c", "pass"])
        child.wait()
        assert borrow.pid_alive(child.pid) is False

    @pytest.mark.parametrize("pid", [None, "1234", 0, -5, True])
    def test_no_usable_pid_counts_as_alive(self, pid):
        assert borrow.pid_alive(pid) is True

    def test_without_psutil(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "psutil", None)
        assert borrow.pid_alive(os.getpid()) is True
        child = subprocess.Popen([sys.executable, "-c", "pass"])
        child.wait()
        assert borrow.pid_alive(child.pid) is False


class FakeProcess:
    def __init__(self, pid, name, parent=None):
        self.pid = pid
        self._name = name
        self._parent = parent

    def name(self):
        return self._name

    def parent(self):
        return self._parent


class TestOwnerPid:
    def _fake_psutil(self, monkeypatch, chain):
        by_pid = {p.pid: p for p in chain}
        psutil = SimpleNamespace(Process=lambda pid: by_pid[pid], Error=Exception)
        monkeypatch.setitem(sys.modules, "psutil", psutil)
        monkeypatch.setattr(borrow.os, "getppid", lambda: chain[0].pid)

    def test_is_the_parent(self, monkeypatch):
        scons = FakeProcess(100, "python.exe")
        self._fake_psutil(monkeypatch, [scons])
        assert borrow.owner_pid() == 100

    def test_skips_the_console_script_launcher(self, monkeypatch):
        """On Windows `blink.exe` starts python.exe and dies with it: its pid would end the
        borrow the moment `blink borrow` returns."""
        scons = FakeProcess(100, "python.exe")
        launcher = FakeProcess(200, "blink.exe", parent=scons)
        self._fake_psutil(monkeypatch, [launcher, scons])
        assert borrow.owner_pid() == 100

    def test_without_psutil_is_the_parent(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "psutil", None)
        assert borrow.owner_pid() == os.getppid()
