# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import sys
from pathlib import Path

import pytest

from blinkview.utils import install_info
from blinkview.utils.install_info import Installer, UpdateSource, detect_install, resolve_update_source

# Copied from real installs (uv 0.9, Windows): the editable source tool and a wheel-file tool.
EDITABLE_RECEIPT = """[tool]
requirements = [{ name = "blinkview", extras = ["all"], editable = "C:/Users/me/projects/blinkview" }]
entrypoints = [
    { name = "blink", install-path = "C:/Users/me/.local/bin/blink.exe", from = "blinkview" },
]
"""
INDEX_RECEIPT = """[tool]
requirements = [{ name = "blinkview", extras = ["hardware", "pyside6"] }]
python = "3.13"
entrypoints = [
    { name = "blink", install-path = "C:/Users/me/.local/bin/blink.exe", from = "blinkview" },
]
"""
EDITABLE_DIRECT_URL = json.dumps({"url": "file:///C:/Users/me/projects/blinkview", "dir_info": {"editable": True}})
WHEEL_DIRECT_URL = json.dumps({"url": "file:///C:/dist/blinkview-1.0-py3-none-any.whl", "archive_info": {}})
VCS_DIRECT_URL = json.dumps(
    {"url": "https://github.com/roland2025/blinkview.git", "vcs_info": {"vcs": "git", "commit_id": "abc"}}
)


class FakeSettings(dict):
    def get(self, key, default=None):
        return super().get(key, default)


def _prefix(tmp_path, receipt=None):
    if receipt is not None:
        (tmp_path / "uv-receipt.toml").write_text(receipt, encoding="utf-8")
    return tmp_path


class TestDetectInstall:
    def test_editable_uv_tool_is_a_source_install(self, tmp_path):
        info = detect_install(_prefix(tmp_path, EDITABLE_RECEIPT), EDITABLE_DIRECT_URL)

        assert info.installer == Installer.UV_TOOL
        assert info.from_source
        assert info.editable is True
        assert info.extras == ("all",)
        if sys.platform == "win32":
            assert info.source_dir == Path("C:/Users/me/projects/blinkview")

    def test_index_install_has_no_direct_url(self, tmp_path):
        info = detect_install(_prefix(tmp_path, INDEX_RECEIPT), None)

        assert info.installer == Installer.UV_TOOL
        assert not info.from_source
        assert info.editable is False
        assert info.extras == ("hardware", "pyside6")

    def test_wheel_file_install_is_not_a_source_install(self, tmp_path):
        """archive_info is a snapshot with no repo to fetch tags in - it updates from the index."""
        info = detect_install(_prefix(tmp_path, INDEX_RECEIPT), WHEEL_DIRECT_URL)

        assert not info.from_source

    def test_git_url_install_is_not_a_source_install(self, tmp_path):
        info = detect_install(_prefix(tmp_path), VCS_DIRECT_URL)

        assert not info.from_source

    def test_no_receipt_means_plain_pip(self, tmp_path):
        info = detect_install(_prefix(tmp_path), None)

        assert info.installer == Installer.PIP
        assert info.extras == ()

    def test_garbage_direct_url_and_receipt_are_tolerated(self, tmp_path):
        info = detect_install(_prefix(tmp_path, "this is [not toml"), "{not json")

        assert info.installer == Installer.UV_TOOL
        assert info.extras == ()
        assert not info.from_source


class TestResolveUpdateSource:
    SOURCE = install_info.InstallInfo(Installer.UV_TOOL, Path("/repo"), True, ("all",))
    PACKAGE = install_info.InstallInfo(Installer.UV_TOOL, None, False, ("all",))

    @pytest.mark.parametrize("value", [None, "auto", "AUTO", ""])
    def test_auto_follows_the_install(self, value):
        settings = FakeSettings() if value is None else FakeSettings({"update.source": value})

        assert resolve_update_source(settings, self.SOURCE) == UpdateSource.GIT
        assert resolve_update_source(settings, self.PACKAGE) == UpdateSource.PYPI

    def test_explicit_setting_overrides_detection(self):
        assert resolve_update_source(FakeSettings({"update.source": "pypi"}), self.SOURCE) == UpdateSource.PYPI
        assert resolve_update_source(FakeSettings({"update.source": "git"}), self.PACKAGE) == UpdateSource.GIT


def test_this_test_environment_is_detected_as_a_source_install():
    """The dev venv is an editable install of this repo - a smoke test that the real
    importlib.metadata path is read, not just the injected strings."""
    info = detect_install()

    assert info.from_source
    assert (info.source_dir / "pyproject.toml").is_file()
