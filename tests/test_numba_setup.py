# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os

import pytest

from blinkview import __version__
from blinkview.core import numba_setup
from blinkview.utils.install_info import UpdateSource


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

    result = numba_setup.export_numba_cache(settings, UpdateSource.GIT)

    expected = tmp_path / ".numba_cache" / __version__
    assert result == expected
    assert result.exists()
    assert numba_setup.IS_CACHE_WARM is False


def test_sets_numba_cache_dir_environment_variable(tmp_path, monkeypatch):
    monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
    settings = FakeSettings(tmp_path)

    result = numba_setup.export_numba_cache(settings, UpdateSource.GIT)

    assert os.environ["NUMBA_CACHE_DIR"] == str(result.resolve())


def test_marks_cache_warm_when_versioned_dir_already_has_files(tmp_path, monkeypatch):
    monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
    versioned_dir = tmp_path / ".numba_cache" / __version__
    versioned_dir.mkdir(parents=True)
    (versioned_dir / "cached_kernel.o").write_bytes(b"x")

    settings = FakeSettings(tmp_path)
    numba_setup.export_numba_cache(settings, UpdateSource.GIT)

    assert numba_setup.IS_CACHE_WARM is True


def test_marks_cache_not_warm_when_versioned_dir_exists_but_is_empty(tmp_path, monkeypatch):
    monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
    versioned_dir = tmp_path / ".numba_cache" / __version__
    versioned_dir.mkdir(parents=True)

    settings = FakeSettings(tmp_path)
    numba_setup.export_numba_cache(settings, UpdateSource.GIT)

    assert numba_setup.IS_CACHE_WARM is False


def test_reuses_existing_numba_cache_dir_env_var_instead_of_recomputing(tmp_path, monkeypatch):
    existing = tmp_path / "already_set_dir"
    existing.mkdir()
    monkeypatch.setenv("NUMBA_CACHE_DIR", str(existing))

    settings = FakeSettings(tmp_path / "unused_settings_path")

    result = numba_setup.export_numba_cache(settings, UpdateSource.GIT)

    assert result == existing
    assert os.environ["NUMBA_CACHE_DIR"] == str(existing)


class TestCacheRoot:
    def test_source_checkout_keeps_cache_next_to_the_repo(self, tmp_path, monkeypatch):
        from blinkview.utils.install_info import UpdateSource

        monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
        result = numba_setup.export_numba_cache(FakeSettings(tmp_path), UpdateSource.GIT)

        assert result == tmp_path / ".numba_cache" / __version__

    def test_package_install_uses_blink_home_not_the_launch_directory(self, tmp_path, monkeypatch):
        """No repo: before this the cache landed in Path('.') / .numba_cache - wherever blink was
        launched from."""
        from blinkview.utils.install_info import UpdateSource

        home = tmp_path / "home"
        monkeypatch.setattr("blinkview.utils.global_settings.get_blink_home", lambda: home)
        monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)

        result = numba_setup.export_numba_cache(FakeSettings(tmp_path / "ignored"), UpdateSource.PYPI)

        assert result == home / "numba_cache" / __version__
        assert os.environ["NUMBA_CACHE_DIR"] == str(result.resolve())
        assert not (tmp_path / "ignored").exists()

    def test_last_used_marker_is_written_but_does_not_count_as_warm(self, tmp_path, monkeypatch):
        from blinkview.utils.install_info import UpdateSource

        monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
        result = numba_setup.export_numba_cache(FakeSettings(tmp_path), UpdateSource.GIT)
        assert (result / numba_setup.LAST_USED_MARKER).is_file()

        os.environ.pop("NUMBA_CACHE_DIR")
        numba_setup.export_numba_cache(FakeSettings(tmp_path), UpdateSource.GIT)

        assert numba_setup.IS_CACHE_WARM is False


class TestPruneOldCaches:
    NOW = 1_800_000_000.0
    DAY = 86400

    def _cache(self, root, name, last_used_days_ago=None):
        d = root / name
        d.mkdir(parents=True)
        (d / "kernel.nbi").write_bytes(b"x")
        if last_used_days_ago is not None:
            marker = d / numba_setup.LAST_USED_MARKER
            marker.touch()
            t = self.NOW - last_used_days_ago * self.DAY
            os.utime(marker, (t, t))
        return d

    def test_older_and_stale_is_deleted(self, tmp_path):
        old = self._cache(tmp_path, "0.17.0", last_used_days_ago=8)

        removed = numba_setup.prune_old_caches(tmp_path, "0.18.0", now=self.NOW)

        assert removed == [old]
        assert not old.exists()

    def test_older_but_recently_used_is_kept(self, tmp_path):
        """Grace period: switching between source branches with different versions stays warm."""
        recent = self._cache(tmp_path, "0.17.0", last_used_days_ago=2)

        assert numba_setup.prune_old_caches(tmp_path, "0.18.0", now=self.NOW) == []
        assert recent.exists()

    def test_newer_versions_are_never_deleted(self, tmp_path):
        newer = self._cache(tmp_path, "0.19.0", last_used_days_ago=365)
        dev_of_current = self._cache(tmp_path, "0.18.1.dev0", last_used_days_ago=365)

        assert numba_setup.prune_old_caches(tmp_path, "0.18.0", now=self.NOW) == []
        assert newer.exists() and dev_of_current.exists()

    def test_current_version_is_kept(self, tmp_path):
        current = self._cache(tmp_path, "0.18.0", last_used_days_ago=365)

        assert numba_setup.prune_old_caches(tmp_path, "0.18.0", now=self.NOW) == []
        assert current.exists()

    def test_pre_release_ordering(self, tmp_path):
        """0.18.0.dev0 < 0.18.0rc1 < 0.18.0 - a dev build's cache goes once the release runs."""
        dev = self._cache(tmp_path, "0.18.0.dev0", last_used_days_ago=30)
        rc = self._cache(tmp_path, "0.18.0rc1", last_used_days_ago=30)

        removed = numba_setup.prune_old_caches(tmp_path, "0.18.0", now=self.NOW)

        assert sorted(removed) == sorted([dev, rc])

    def test_non_version_names_and_files_are_never_touched(self, tmp_path):
        other = self._cache(tmp_path, "not-a-version", last_used_days_ago=365)
        stray = tmp_path / "0.1.0"  # a file, not a dir
        stray.write_bytes(b"x")

        assert numba_setup.prune_old_caches(tmp_path, "0.18.0", now=self.NOW) == []
        assert other.exists() and stray.exists()

    def test_falls_back_to_dir_mtime_without_marker(self, tmp_path):
        """Caches from before the marker existed are judged by the directory mtime."""
        legacy = self._cache(tmp_path, "0.10.0")
        t = self.NOW - 30 * self.DAY
        os.utime(legacy, (t, t))

        assert numba_setup.prune_old_caches(tmp_path, "0.18.0", now=self.NOW) == [legacy]

    def test_missing_root_is_a_no_op(self, tmp_path):
        assert numba_setup.prune_old_caches(tmp_path / "nope", "0.18.0", now=self.NOW) == []

    def test_external_numba_cache_dir_disables_pruning(self, tmp_path, monkeypatch):
        from blinkview.utils.install_info import UpdateSource

        old = self._cache(tmp_path / ".numba_cache", "0.0.1")
        os.utime(old, (0, 0))
        monkeypatch.setenv("NUMBA_CACHE_DIR", str(tmp_path / "elsewhere"))
        started = []
        monkeypatch.setattr(numba_setup.threading, "Thread", lambda *a, **kw: started.append(kw))

        numba_setup.export_numba_cache(FakeSettings(tmp_path), UpdateSource.GIT)

        assert started == []
        assert old.exists()

    def test_export_prunes_siblings_in_the_background(self, tmp_path, monkeypatch):
        from blinkview.utils.install_info import UpdateSource

        old = self._cache(tmp_path / ".numba_cache", "0.0.1")
        os.utime(old, (0, 0))
        monkeypatch.delenv("NUMBA_CACHE_DIR", raising=False)
        threads = []
        real_thread = numba_setup.threading.Thread

        def _capture(*a, **kw):
            t = real_thread(*a, **kw)
            threads.append(t)
            return t

        monkeypatch.setattr(numba_setup.threading, "Thread", _capture)

        numba_setup.export_numba_cache(FakeSettings(tmp_path), UpdateSource.GIT)
        for t in threads:
            t.join(timeout=10)

        assert len(threads) == 1
        assert not old.exists()
        assert (tmp_path / ".numba_cache" / __version__).exists()
