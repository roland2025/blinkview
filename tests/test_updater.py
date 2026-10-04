# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from blinkview import __version__
from blinkview.utils import updater as updater_module
from blinkview.utils.install_info import Installer, InstallInfo, UpdateSource
from blinkview.utils.updater import GitUpdater, PyPIUpdater, UpdateError, make_updater

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

        assert cmd == ["uv", "tool", "install", "blinkview[all]==0.19.0", "--python", sys.executable, "--force"]

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
