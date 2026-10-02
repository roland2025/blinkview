# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os

import pytest

from blinkview import __version__
from blinkview.core import numba_setup


class FakeSettings:
    def __init__(self, update_path):
        self._path = str(update_path)

    def get(self, key, default=None):
        if key == "update.path":
            return self._path
        return default


def setup_function(_):
    numba_setup.IS_CACHE_WARM = False


@pytest.fixture(autouse=True)
def restore_numba_cache_dir():
    """export_numba_cache() writes os.environ["NUMBA_CACHE_DIR"] directly, and the tests'
    monkeypatch.delenv(raising=False) on an unset variable records nothing to undo - so the
    tmp_path value used to leak into every later test, and into the subprocesses they start (a
    `blink` child then compiled every Numba kernel cold into a dead tmp dir and timed out)."""
    saved = os.environ.get("NUMBA_CACHE_DIR")
    yield
    if saved is None:
        os.environ.pop("NUMBA_CACHE_DIR", None)
    else:
        os.environ["NUMBA_CACHE_DIR"] = saved


def test_creates_versioned_cache_dir_and_sets_env_var(tmp_path, monkeypatch):
    monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
    settings = FakeSettings(tmp_path)

    result = numba_setup.export_numba_cache(settings)

    expected = tmp_path / ".numba_cache" / __version__
    assert result == expected
    assert result.exists()
    assert numba_setup.IS_CACHE_WARM is False


def test_sets_numba_cache_dir_environment_variable(tmp_path, monkeypatch):
    monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
    settings = FakeSettings(tmp_path)

    result = numba_setup.export_numba_cache(settings)

    assert os.environ["NUMBA_CACHE_DIR"] == str(result.resolve())


def test_marks_cache_warm_when_versioned_dir_already_has_files(tmp_path, monkeypatch):
    monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
    versioned_dir = tmp_path / ".numba_cache" / __version__
    versioned_dir.mkdir(parents=True)
    (versioned_dir / "cached_kernel.o").write_bytes(b"x")

    settings = FakeSettings(tmp_path)
    numba_setup.export_numba_cache(settings)

    assert numba_setup.IS_CACHE_WARM is True


def test_marks_cache_not_warm_when_versioned_dir_exists_but_is_empty(tmp_path, monkeypatch):
    monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
    versioned_dir = tmp_path / ".numba_cache" / __version__
    versioned_dir.mkdir(parents=True)

    settings = FakeSettings(tmp_path)
    numba_setup.export_numba_cache(settings)

    assert numba_setup.IS_CACHE_WARM is False


def test_reuses_existing_numba_cache_dir_env_var_instead_of_recomputing(tmp_path, monkeypatch):
    existing = tmp_path / "already_set_dir"
    existing.mkdir()
    monkeypatch.setenv("NUMBA_CACHE_DIR", str(existing))

    settings = FakeSettings(tmp_path / "unused_settings_path")

    result = numba_setup.export_numba_cache(settings)

    assert result == existing
    assert os.environ["NUMBA_CACHE_DIR"] == str(existing)
