# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

from blinkview import __version__
from blinkview.utils import updater as updater_module
from blinkview.utils.install_info import Installer, InstallInfo, UpdateSource
from blinkview.utils.updater import GitUpdater, PyPIUpdater, RunningInstance, UpdateError, make_updater

UV_PACKAGE = InstallInfo(Installer.UV_TOOL, None, False, ("all",))
PIP_PACKAGE = InstallInfo(Installer.PIP, None, False, ())


class FakeSettings:
    def __init__(self, values=None):
        self.values = dict(values or {})

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value, scope="project"):
        assert scope == "global", f"update state must be global, got scope={scope!r} for {key}"
        self.values[key] = value


def _file(yanked=False):
    return {"filename": "blinkview.whl", "yanked": yanked}


# Trimmed shape of https://pypi.org/pypi/<name>/json
PYPI_RESPONSE = {
    "info": {"name": "blinkview", "version": "0.19.0"},
    "releases": {
        "0.17.0": [_file()],
        "0.18.0": [_file(), _file()],
        "0.18.1rc1": [_file()],
        "0.19.0.dev0": [_file()],
        "0.19.0": [_file()],
        "0.20.0": [_file(yanked=True)],  # yanked: not installable
        "0.20.1": [],  # registered but no files uploaded
        "0.18.5": [_file(yanked=True), _file()],  # one good file is enough
        "not-a-version": [_file()],
    },
}


@pytest.fixture(autouse=True)
def no_other_instances(monkeypatch):
    """No scan of the machine's real processes, whatever the developer has running."""
    monkeypatch.setattr(updater_module, "find_other_instances", lambda: [])


class FakeResponse:
    def __init__(self, data, status=200):
        self._data = data
        self.status_code = status

    def raise_for_status(self):
        import requests

        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._data


@pytest.fixture
def fake_index(monkeypatch):
    import requests

    calls = []

    def _get(url, timeout=None):
        calls.append(url)
        return FakeResponse(PYPI_RESPONSE)

    monkeypatch.setattr(requests, "get", _get)
    return calls


class TestPyPIVersions:
    def test_available_versions_skip_yanked_empty_and_unparseable(self):
        assert sorted(PyPIUpdater.available_versions(PYPI_RESPONSE)) == sorted(
            ["0.17.0", "0.18.0", "0.18.1rc1", "0.19.0.dev0", "0.19.0", "0.18.5"]
        )

    @pytest.mark.parametrize(
        "channel, expected",
        [
            ("stable", ["0.19.0", "0.18.5", "0.18.0", "0.17.0"]),
            ("rc", ["0.19.0", "0.18.5", "0.18.1rc1", "0.18.0", "0.17.0"]),
            ("dev", ["0.19.0", "0.19.0.dev0", "0.18.5", "0.18.1rc1", "0.18.0", "0.17.0"]),
        ],
    )
    def test_fetch_then_list_by_channel(self, fake_index, channel, expected):
        settings = FakeSettings({"update.channel": channel})
        u = PyPIUpdater(settings, UV_PACKAGE)

        assert u.fetch(force=True) is True
        assert u.get_versions() == expected
        assert fake_index == ["https://pypi.org/pypi/blinkview/json"]

    def test_fetch_records_cooldown_and_latest_on_channel(self, fake_index):
        settings = FakeSettings({"update.channel": "dev"})

        PyPIUpdater(settings, UV_PACKAGE).fetch(force=True)

        assert settings.values["update.latest_version"] == "0.19.0"
        assert settings.values["update.last_fetch_time"] > 0

    def test_cooldown_skips_network_the_same_day(self, fake_index):
        settings = FakeSettings()
        u = PyPIUpdater(settings, UV_PACKAGE)
        u.fetch()
        fake_index.clear()

        assert u.fetch() is False
        assert fake_index == []
        assert u.fetch(force=True) is True
        assert len(fake_index) == 1

    def test_versions_come_from_the_cache_without_network(self, fake_index):
        settings = FakeSettings({"update.index_versions": ["1.0.0", "1.1.0"]})

        assert PyPIUpdater(settings, UV_PACKAGE).get_versions() == ["1.1.0", "1.0.0"]
        assert fake_index == []

    def test_network_failure_is_an_update_error(self, monkeypatch):
        import requests

        def _boom(url, timeout=None):
            raise requests.ConnectionError("offline")

        monkeypatch.setattr(requests, "get", _boom)
        settings = FakeSettings()

        with pytest.raises(UpdateError, match="offline"):
            PyPIUpdater(settings, UV_PACKAGE).fetch(force=True)
        assert "update.last_fetch_time" not in settings.values  # a failed fetch doesn't start the cooldown

    def test_http_error_is_an_update_error(self, monkeypatch):
        import requests

        monkeypatch.setattr(requests, "get", lambda url, timeout=None: FakeResponse({}, status=404))

        with pytest.raises(UpdateError):
            PyPIUpdater(FakeSettings(), UV_PACKAGE).fetch(force=True)

    def test_custom_index_url(self, fake_index):
        settings = FakeSettings({"update.index_url": "https://test.pypi.org/pypi/"})

        PyPIUpdater(settings, UV_PACKAGE).fetch(force=True)

        assert fake_index == ["https://test.pypi.org/pypi/blinkview/json"]


class TestPyPIInstallCommand:
    def test_uv_tool_reinstalls_pinned_with_recorded_extras(self):
        cmd = PyPIUpdater(FakeSettings(), UV_PACKAGE).install_command("0.19.0")

        assert cmd == [
            "uv",
            "tool",
            "install",
            "blinkview[all]==0.19.0",
            "--python",
            sys.executable,
            "--force",
            # A version the fetch just found must not be hidden by uv's cached index page.
            "--refresh-package",
            "blinkview",
        ]

    def test_pip_uses_this_interpreter(self):
        cmd = PyPIUpdater(FakeSettings(), PIP_PACKAGE).install_command("0.19.0")

        # No recorded extras and no update.features -> the documented default, all
        assert cmd == [sys.executable, "-m", "pip", "install", "blinkview[all]==0.19.0"]

    def test_explicit_features_setting_beats_recorded_extras(self):
        cmd = PyPIUpdater(FakeSettings({"update.features": "hardware, pyside6"}), UV_PACKAGE).install_command("1.0")

        assert "blinkview[hardware,pyside6]==1.0" in cmd

    def test_leading_v_is_stripped(self):
        assert "blinkview[all]==1.0.0" in PyPIUpdater(FakeSettings(), UV_PACKAGE).install_command("v1.0.0")

    def test_test_index_routes_both_installers(self):
        settings = FakeSettings({"update.index_url": "https://test.pypi.org/pypi"})

        uv_cmd = PyPIUpdater(settings, UV_PACKAGE).install_command("1.0")
        pip_cmd = PyPIUpdater(settings, PIP_PACKAGE).install_command("1.0")

        assert uv_cmd[-2:] == ["--index", "https://test.pypi.org/simple"]
        assert pip_cmd[-4:] == [
            "--index-url",
            "https://test.pypi.org/simple",
            "--extra-index-url",
            "https://pypi.org/simple",
        ]

    def test_install_marks_pending_and_runs_the_command(self, monkeypatch):
        settings = FakeSettings()
        u = PyPIUpdater(settings, UV_PACKAGE)
        ran = []
        monkeypatch.setattr(u, "_run_install", lambda cmd: ran.append(cmd) or True)

        assert u.install("0.19.0") is True
        assert ran == [u.install_command("0.19.0")]
        assert settings.values["update.pending_version"] == "0.19.0"

    def test_failed_install_clears_pending(self, monkeypatch):
        settings = FakeSettings()
        u = PyPIUpdater(settings, UV_PACKAGE)

        def _fail(cmd):
            raise UpdateError("boom")

        monkeypatch.setattr(u, "_run_install", _fail)

        with pytest.raises(UpdateError):
            u.install("0.19.0")
        assert settings.values["update.pending_version"] is None


class TestOtherInstances:
    """A second BlinkView process of the same install (`blink mcp`, another window) keeps
    blink.exe locked on Windows: the install is refused up front instead of failing halfway."""

    def test_windows_install_is_refused_while_another_instance_runs(self, monkeypatch):
        settings = FakeSettings()
        u = PyPIUpdater(settings, UV_PACKAGE)
        ran = []
        monkeypatch.setattr(updater_module.sys, "platform", "win32")
        monkeypatch.setattr(updater_module, "find_other_instances", lambda: [RunningInstance(42, "blink mcp")])
        monkeypatch.setattr(u, "_run_install", lambda cmd: ran.append(cmd) or True)

        with pytest.raises(UpdateError, match=r"blink mcp \(pid 42\)"):
            u.install("0.19.0")
        assert ran == []
        assert "update.pending_version" not in settings.values

    def test_elsewhere_running_instances_do_not_block(self, monkeypatch):
        u = PyPIUpdater(FakeSettings(), UV_PACKAGE)
        monkeypatch.setattr(updater_module.sys, "platform", "linux")
        monkeypatch.setattr(updater_module, "find_other_instances", lambda: [RunningInstance(42, "blink mcp")])
        monkeypatch.setattr(u, "_run_install", lambda cmd: False)

        assert u.install("0.19.0") is False

    def test_close_other_instances_kills_what_is_running(self, monkeypatch):
        running = [RunningInstance(42, "blink mcp")]
        killed = []
        monkeypatch.setattr(updater_module.sys, "platform", "win32")
        monkeypatch.setattr(updater_module, "find_other_instances", lambda: list(running))

        def _terminate(instances, on_wait=None):
            killed.extend(instances)
            running.clear()

        monkeypatch.setattr(updater_module, "close_instances", _terminate)

        PyPIUpdater.close_other_instances()

        assert killed == [RunningInstance(42, "blink mcp")]

    def test_close_other_instances_reports_survivors(self, monkeypatch):
        monkeypatch.setattr(updater_module.sys, "platform", "win32")
        monkeypatch.setattr(updater_module, "find_other_instances", lambda: [RunningInstance(42, "blink mcp")])
        monkeypatch.setattr(updater_module, "close_instances", lambda instances, on_wait=None: None)

        with pytest.raises(UpdateError, match=r"blink mcp \(pid 42\)"):
            PyPIUpdater.close_other_instances()

    def test_close_instances_kills_the_whole_chain_of_a_windowless_one(self, tmp_path):
        """A real process tree standing in for blink.exe -> python.exe -> python.exe: the
        instance is the middle one, and its child must not outlive it."""
        psutil = pytest.importorskip("psutil")

        child_code = "import time; time.sleep(60)"
        parent_code = (
            f"import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', {child_code!r}]); time.sleep(60)"
        )
        parent = subprocess.Popen([sys.executable, "-c", parent_code])
        try:
            proc = psutil.Process(parent.pid)
            deadline = time.monotonic() + 10
            while not proc.children(recursive=True) and time.monotonic() < deadline:
                time.sleep(0.05)
            tree = [proc, *proc.children(recursive=True)]
            assert len(tree) >= 2

            updater_module.close_instances([RunningInstance(parent.pid, "blink mcp")])

            assert not any(p.is_running() and p.status() != psutil.STATUS_ZOMBIE for p in tree)
        finally:
            parent.kill()
            parent.wait()

    @pytest.mark.skipif(sys.platform != "win32", reason="WM_CLOSE: only Windows needs the others closed")
    def test_close_instances_asks_a_window_to_close_instead_of_killing_it(self, tmp_path):
        """A real window in a real process: its closeEvent must run (that is where BlinkView
        saves its layout and stops the registry), which a kill would skip."""
        pytest.importorskip("psutil")
        pytest.importorskip("qtpy")

        shown, closed = tmp_path / "shown", tmp_path / "closed"
        code = f"""
import sys
from qtpy.QtWidgets import QApplication, QWidget

class Window(QWidget):
    def closeEvent(self, event):
        open({str(closed)!r}, "w").close()
        event.accept()

app = QApplication([])
window = Window()
window.show()
open({str(shown)!r}, "w").close()
sys.exit(app.exec())
"""
        env = {k: v for k, v in os.environ.items() if k != "QT_QPA_PLATFORM"}  # a real window, not offscreen
        child = subprocess.Popen([sys.executable, "-c", code], env=env)
        try:
            deadline = time.monotonic() + 30
            while not shown.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert shown.exists()
            time.sleep(0.3)  # shown() has returned; let the window reach the screen

            waited = []
            updater_module.close_instances(
                [RunningInstance(child.pid, "blink")], window_timeout=20, on_wait=lambda: waited.append(1)
            )

            assert closed.exists()
            assert child.wait(timeout=5) == 0  # exited by itself; a kill reports 15 / 1
            assert waited
        finally:
            child.kill()
            child.wait()

    @pytest.mark.skipif(sys.platform != "win32", reason="WM_CLOSE: only Windows needs the others closed")
    def test_a_window_that_refuses_to_close_is_left_running(self, tmp_path):
        """Not killed after the timeout: it may be in the middle of saving. The caller reports
        it as still running instead."""
        psutil = pytest.importorskip("psutil")
        pytest.importorskip("qtpy")

        shown = tmp_path / "shown"
        code = f"""
import sys
from qtpy.QtWidgets import QApplication, QWidget

class Window(QWidget):
    def closeEvent(self, event):
        event.ignore()

app = QApplication([])
window = Window()
window.show()
open({str(shown)!r}, "w").close()
sys.exit(app.exec())
"""
        env = {k: v for k, v in os.environ.items() if k != "QT_QPA_PLATFORM"}
        child = subprocess.Popen([sys.executable, "-c", code], env=env)
        try:
            deadline = time.monotonic() + 30
            while not shown.exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            assert shown.exists()
            time.sleep(0.3)

            updater_module.close_instances([RunningInstance(child.pid, "blink")], window_timeout=1.5)

            assert psutil.pid_exists(child.pid) and child.poll() is None
        finally:
            child.kill()
            child.wait()

    def test_close_instances_ignores_one_that_already_exited(self):
        pytest.importorskip("psutil")
        gone = subprocess.Popen([sys.executable, "-c", "pass"])
        gone.wait()

        updater_module.close_instances([RunningInstance(gone.pid, "blink mcp")])  # must not raise

    @pytest.mark.parametrize(
        "cmdline, expected",
        [
            (["python.exe", r"C:\Users\x\.local\bin\blink.exe", "mcp"], "blink mcp"),
            (["python.exe", "/home/x/.local/bin/blink"], "blink"),
            (["python.exe", "-m", "blinkview", "setup", "--no-gui"], "blink setup --no-gui"),
            (["python.exe", "-m", "pip", "list"], None),
            (["python.exe"], None),
            ([], None),
        ],
    )
    def test_describe_blink_command(self, cmdline, expected):
        assert updater_module._describe_blink_command(cmdline) == expected


class TestRunInstallThenSetup:
    """`blink setup` (ui/setup.py) follows a successful install as its own process, so the new
    version's Numba kernels are compiled before its first start."""

    def test_windows_chains_setup_behind_the_detached_install(self, monkeypatch):
        u = PyPIUpdater(FakeSettings(), UV_PACKAGE)
        calls = []
        monkeypatch.setattr(updater_module.sys, "platform", "win32")
        monkeypatch.setattr(subprocess, "DETACHED_PROCESS", 8, raising=False)
        monkeypatch.setattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 512, raising=False)
        monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kw: calls.append(cmd))

        assert u._run_install(["uv", "tool", "install", "blinkview==1.0"]) is True

        setup = subprocess.list2cmdline(u.setup_command())
        wait = u.wait_for_exit_command()
        install = "uv tool install blinkview==1.0"
        retry = "powershell -NoProfile -Command Start-Sleep 3"
        assert calls == [f'cmd /c "{wait} & ({install} || ({retry} & {install})) && {setup}"']

    @pytest.mark.skipif(sys.platform != "win32", reason="cmd.exe's operators")
    @pytest.mark.parametrize(
        "install_results, expected",
        [
            ([0], ["install", "setup"]),
            ([1, 0], ["install", "install", "setup"]),  # e.g. blink.exe still held for a moment
            ([1, 1], ["install", "install"]),  # no setup for a version that isn't there
        ],
    )
    def test_windows_chain_retries_a_failed_install_once(self, tmp_path, monkeypatch, install_results, expected):
        """The real command line through the real cmd.exe, with stand-ins for uv and setup."""
        log = tmp_path / "log.txt"
        results = tmp_path / "results.txt"
        results.write_text("\n".join(map(str, install_results)))
        fake_install = tmp_path / "fake_install.py"
        fake_install.write_text(
            "import sys\n"
            f"log, results = {str(log)!r}, {str(results)!r}\n"
            "open(log, 'a').write('install\\n')\n"
            "codes = open(results).read().split()\n"
            "open(results, 'w').write('\\n'.join(codes[1:]))\n"
            "sys.exit(int(codes[0]))\n"
        )
        fake_setup = tmp_path / "fake_setup.py"
        fake_setup.write_text(f"open({str(log)!r}, 'a').write('setup\\n')\n")

        u = PyPIUpdater(FakeSettings(), UV_PACKAGE)
        monkeypatch.setattr(updater_module, "INSTALL_RETRY_DELAY_SECONDS", 0)
        monkeypatch.setattr(u, "wait_for_exit_command", lambda: "ver > nul")
        monkeypatch.setattr(u, "setup_command", lambda: [sys.executable, str(fake_setup)])
        started = []
        real_popen = subprocess.Popen
        monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kw: started.append(real_popen(cmd, shell=True)))

        u._run_install([sys.executable, str(fake_install)])
        started[0].wait(timeout=60)

        assert log.read_text().split() == expected

    def test_windows_install_waits_for_this_process_to_exit(self):
        """Not a fixed delay: uv can't overwrite blink.exe while a GUI that is still shutting
        down keeps it running, and the install then fails after swapping the package."""
        wait = PyPIUpdater.wait_for_exit_command()

        assert f"Wait-Process -Id {os.getpid()} " in wait
        assert '"' not in wait  # it sits inside cmd /c "..."

    def test_child_env_drops_the_cache_dir_this_version_exported(self, monkeypatch):
        """The new version's `blink setup` must compile into its own versioned dir."""
        from blinkview.core import numba_setup

        monkeypatch.setenv("NUMBA_CACHE_DIR", "/cache/0.1")
        monkeypatch.setattr(numba_setup, "EXPORTED_CACHE_DIR", "/cache/0.1")

        assert "NUMBA_CACHE_DIR" not in PyPIUpdater.child_env()

    def test_child_env_keeps_a_cache_dir_the_user_chose(self, monkeypatch):
        from blinkview.core import numba_setup

        monkeypatch.setenv("NUMBA_CACHE_DIR", "/my/cache")
        monkeypatch.setattr(numba_setup, "EXPORTED_CACHE_DIR", None)

        assert PyPIUpdater.child_env()["NUMBA_CACHE_DIR"] == "/my/cache"

    def test_elsewhere_setup_starts_after_the_install(self, monkeypatch):
        u = PyPIUpdater(FakeSettings(), UV_PACKAGE)
        order = []
        monkeypatch.setattr(updater_module.sys, "platform", "linux")
        monkeypatch.setattr(subprocess, "run", lambda cmd, **kw: order.append(("install", cmd)))
        monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kw: order.append(("setup", cmd)))

        assert u._run_install(["pip", "install", "x"]) is False

        assert order == [("install", ["pip", "install", "x"]), ("setup", u.setup_command())]

    def test_failed_install_does_not_run_setup(self, monkeypatch):
        u = PyPIUpdater(FakeSettings(), UV_PACKAGE)
        started = []
        monkeypatch.setattr(updater_module.sys, "platform", "linux")

        def _fail(cmd, **kw):
            raise subprocess.CalledProcessError(1, cmd, stderr="no network")

        monkeypatch.setattr(subprocess, "run", _fail)
        monkeypatch.setattr(subprocess, "Popen", lambda cmd, **kw: started.append(cmd))

        with pytest.raises(UpdateError, match="no network"):
            u._run_install(["pip", "install", "x"])
        assert started == []

    def test_setup_command_runs_the_setup_subcommand_of_this_interpreter(self):
        assert PyPIUpdater.setup_command() == [sys.executable, "-m", "blinkview", "setup"]


class TestUpgrade:
    def test_installs_newest_on_channel_when_newer(self, fake_index, monkeypatch):
        u = PyPIUpdater(FakeSettings(), UV_PACKAGE)
        installed = []
        monkeypatch.setattr(u, "install", installed.append)

        assert u.upgrade("0.18.0") == (True, "0.19.0")
        assert installed == ["0.19.0"]

    def test_up_to_date_does_nothing(self, fake_index, monkeypatch):
        u = PyPIUpdater(FakeSettings(), UV_PACKAGE)
        monkeypatch.setattr(u, "install", lambda v: pytest.fail("must not install"))

        assert u.upgrade("0.19.0") == (False, "0.19.0")


class TestMakeUpdater:
    def test_package_install_gets_pypi(self):
        assert isinstance(make_updater(FakeSettings(), UV_PACKAGE), PyPIUpdater)

    def test_source_install_gets_git(self, monkeypatch):
        monkeypatch.setattr(GitUpdater, "is_valid_repo", staticmethod(lambda p: True))
        source = InstallInfo(Installer.UV_TOOL, Path("/repo"), True, ("all",))

        u = make_updater(FakeSettings({"update.path": "/repo"}), source)

        assert isinstance(u, GitUpdater)
        assert u.source == UpdateSource.GIT

    def test_update_source_setting_forces_pypi_on_a_checkout(self):
        source = InstallInfo(Installer.UV_TOOL, Path("/repo"), True, ("all",))

        assert isinstance(make_updater(FakeSettings({"update.source": "pypi"}), source), PyPIUpdater)

    def test_git_without_valid_path_raises(self):
        source = InstallInfo(Installer.UV_TOOL, Path("/repo"), True, ("all",))

        with pytest.raises(UpdateError, match="update.path"):
            make_updater(FakeSettings(), source)


@pytest.mark.skipif(shutil.which("git") is None, reason="needs git")
class TestGitUpdaterRealRepo:
    @pytest.fixture
    def repo(self, tmp_path):
        repo = tmp_path / "blinkview"
        (repo / "src" / "blinkview").mkdir(parents=True)
        (repo / "src" / "blinkview" / "__main__.py").write_text("")
        (repo / "pyproject.toml").write_text("")

        def git(*args):
            subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)

        git("init", "-q")
        git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "--allow-empty", "-m", "init")
        for tag in ["v0.17.0", "v0.18.0", "v0.18.1rc1", "v0.19.0.dev0", "not-a-version"]:
            git("tag", tag)
        return repo

    def test_lists_local_tags_by_channel(self, repo):
        settings = FakeSettings({"update.path": str(repo), "update.channel": "rc"})
        u = GitUpdater(settings, InstallInfo(Installer.UV_TOOL, repo, True, ("all",)))

        assert u.get_versions() == ["v0.18.1rc1", "v0.18.0", "v0.17.0"]
        assert u.describe().startswith("git checkout at ")

    def test_install_command_reinstalls_from_the_checkout(self, repo, monkeypatch):
        settings = FakeSettings({"update.path": str(repo)})
        u = GitUpdater(settings, InstallInfo(Installer.UV_TOOL, repo, True, ("all",)))
        ran = []
        monkeypatch.setattr(u, "_run_install", lambda cmd: ran.append(cmd) or False)

        u.install("v0.18.0")

        assert ran == [
            ["uv", "tool", "install", f"{repo.resolve()}[all]", "--python", sys.executable, "--force", "--refresh"]
        ]
        assert settings.values["update.pending_version"] == "v0.18.0"


class TestUpdateNotice:
    @pytest.fixture
    def home(self, tmp_path, monkeypatch):
        monkeypatch.setattr("blinkview.utils.global_settings.get_blink_home", lambda: tmp_path)
        return tmp_path

    def _write(self, home, latest):
        import json

        (home / "settings.json").write_text(json.dumps({"update": {"latest_version": latest}}))

    def test_newer_cached_version_prints_upgrade_hint(self, home):
        self._write(home, "999.0.0")

        msg = updater_module.get_update_notice()

        assert "v999.0.0" in msg and f"v{__version__}" in msg
        assert "blink update upgrade" in msg
        assert "git pull" not in msg

    @pytest.mark.parametrize("latest", [None, __version__, "0.0.1", "garbage!"])
    def test_no_notice_unless_strictly_newer(self, home, latest):
        self._write(home, latest)

        assert updater_module.get_update_notice() is None
