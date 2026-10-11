# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Dev mode (plans/dev-mode.md): BlinkView's own Speedometer/ThroughputAutoTuner loggers are only
enabled when the dev_mode setting (or BLINKVIEW_DEV) says so."""

import json
import re
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from blinkview.core.logger import PrintLogger, SystemLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.utils.dev_mode import DEV_MODE_ENV, parse_flag, resolve_dev_mode
from tests.fakes.real_registry import make_real_registry


class FakeSettings:
    def __init__(self, data=None):
        self._data = dict(data or {})

    def get(self, key, default=None):
        return self._data.get(key, default)


class TestParseFlag:
    @pytest.mark.parametrize("value", [True, "true", "True", " TRUE ", "1", "yes", "on"])
    def test_on(self, value):
        assert parse_flag(value) is True

    @pytest.mark.parametrize("value", [False, None, "false", "0", "no", "off", "", "maybe", 1, {}])
    def test_off(self, value):
        assert parse_flag(value) is False


class TestResolveDevMode:
    def test_off_by_default(self, monkeypatch):
        monkeypatch.delenv(DEV_MODE_ENV, raising=False)
        assert resolve_dev_mode(FakeSettings()) is False
        assert resolve_dev_mode(None) is False

    def test_reads_the_setting_as_stored_by_blink_config(self, monkeypatch):
        monkeypatch.delenv(DEV_MODE_ENV, raising=False)
        assert resolve_dev_mode(FakeSettings({"dev_mode": "true"})) is True

    @pytest.mark.parametrize("env, setting, expected", [("1", "false", True), ("0", "true", False)])
    def test_environment_wins_over_the_setting(self, monkeypatch, env, setting, expected):
        monkeypatch.setenv(DEV_MODE_ENV, env)
        assert resolve_dev_mode(FakeSettings({"dev_mode": setting})) is expected


class TestStatsChild:
    @pytest.mark.parametrize("dev_mode", [False, True])
    def test_system_logger_follows_the_registry_dev_mode(self, dev_mode):
        parent = SystemLogger("central", None, SimpleNamespace(dev_mode=dev_mode))

        stats = parent.stats_child("stats")

        assert stats.enabled is dev_mode
        assert stats.module_path == "central.stats"
        assert parent.child("other").enabled is True  # siblings unaffected

    def test_disabled_stats_logger_never_reaches_the_registry(self):
        # A registry with no system_device: _lazy_log would raise if it ever ran.
        stats = SystemLogger("central", None, SimpleNamespace(dev_mode=False)).stats_child("stats")
        stats.debug("mb_s=%s", 1)

    def test_print_logger_still_logs(self):
        lines = []
        stats = PrintLogger("central", queue_put=lines.append, time_ns=lambda: 0).stats_child("stats")

        stats.debug("mb_s=%s", 1)

        assert [(ctx, msg) for _, ctx, _, msg in lines] == [("central.stats", "mb_s=1")]


def test_no_stats_or_tuner_logger_bypasses_stats_child():
    """A new Speedometer/tuner call site using plain .child("stats...") would log in production.
    io/benchmark.py is the exception: its `stats` logger is the benchmark's own output."""
    src = Path(__file__).resolve().parents[1] / "src" / "blinkview"
    pattern = re.compile(r"""\.child\(\s*f?["'](stats|tuner)""")
    offenders = [
        f"{path.relative_to(src)}:{lineno}"
        for path in src.rglob("*.py")
        if path.relative_to(src).as_posix() != "io/benchmark.py"
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1)
        if pattern.search(line)
    ]
    assert offenders == []


def _push(registry, texts):
    device = registry.id_registry.get_device("devmode")
    module = device.get_module("mod")
    src = registry.system_ctx.array_pool.create(
        PooledLogBatch, len(texts), 4096, has_levels=True, has_modules=True, has_devices=True
    )
    with src:
        for text in texts:
            ts = registry.now_ns()
            src.insert_any(ts, ts, text.encode("ascii"), level=0, module=module.id, device=device.id)
        registry.central.put(src)


def _push_for(registry, seconds):
    """Speedometer logs on the first update() after its 1 s interval, so keep central busy."""
    deadline = time.monotonic() + seconds
    i = 0
    while time.monotonic() < deadline:
        _push(registry, [f"row-{i}"])
        i += 1
        time.sleep(0.05)


@pytest.mark.parametrize("dev_mode", [False, True])
def test_real_registry_logs_central_stats_only_in_dev_mode(tmp_path, monkeypatch, dev_mode):
    monkeypatch.setenv(DEV_MODE_ENV, "1" if dev_mode else "0")
    registry = make_real_registry(tmp_path, "devmode_test", start=True)
    try:
        assert registry.dev_mode is dev_mode
        meta = json.loads((registry.file_manager.session_dir / "metadata.json").read_text())
        assert meta["environment"]["dev_mode"] is dev_mode

        _push_for(registry, 3.0)

        assert ("central.stats" in registry.system_device.path_lookup) is dev_mode
    finally:
        registry.stop()


class TestConsoleTracing:
    """BlinkView's own tracing (what was configured, which thread started, ...) is only printed in
    dev mode - see dev_print() and PrintLogger."""

    @pytest.fixture
    def dev_mode(self, monkeypatch):
        """Sets the process-wide flag for one test (is_dev_mode() resolves it once per process)."""
        from blinkview.utils import dev_mode as dev_mode_module

        def set_(value):
            monkeypatch.setattr(dev_mode_module, "_process_dev_mode", value)

        return set_

    def test_dev_print_is_silent_outside_dev_mode(self, dev_mode, capsys):
        from blinkview.utils.dev_mode import dev_print

        dev_mode(False)
        dev_print("[Registry] config={'token': 'secret'}")

        assert capsys.readouterr().out == ""

    def test_dev_print_prints_in_dev_mode(self, dev_mode, capsys):
        from blinkview.utils.dev_mode import dev_print

        dev_mode(True)
        dev_print("[Warmup] Reorder ...", "done")

        assert capsys.readouterr().out == "[Warmup] Reorder ... done\n"

    def test_is_dev_mode_follows_the_env_var_and_asks_once(self, dev_mode, monkeypatch):
        from blinkview.utils.dev_mode import is_dev_mode

        dev_mode(None)  # not asked yet
        monkeypatch.setenv(DEV_MODE_ENV, "1")
        assert is_dev_mode() is True

        monkeypatch.setenv(DEV_MODE_ENV, "0")
        assert is_dev_mode() is True  # resolved once per process, like Registry.dev_mode

    def test_print_logger_keeps_info_off_the_console_but_still_queues_it(self, dev_mode, capsys):
        dev_mode(False)
        queued = []
        logger = PrintLogger("id_registry", queue_put=queued.append, time_ns=lambda: 7)

        logger.info("id=%d tm_ms=0.03", 0)
        logger.warn("pool is full")

        out = capsys.readouterr().out
        assert "tm_ms" not in out
        assert "pool is full" in out
        assert [msg for _ts, _ctx, _level, msg in queued] == ["id=0 tm_ms=0.03", "pool is full"]

    def test_print_logger_prints_everything_in_dev_mode(self, dev_mode, capsys):
        dev_mode(True)

        PrintLogger("id_registry").info("id=%d tm_ms=0.03", 0)

        assert "tm_ms" in capsys.readouterr().out

    def test_real_registry_start_prints_no_config_dump(self, dev_mode, tmp_path, capsys, monkeypatch):
        """The whole profile used to be printed twice per start ([Registry] _create_and_bind and
        [SourcesManager] Applying config) - noise, and anything secret in a profile with it."""
        monkeypatch.setenv(DEV_MODE_ENV, "0")
        dev_mode(False)
        registry = make_real_registry(tmp_path, "console_test", start=True)
        registry.stop()

        out = capsys.readouterr().out
        for needle in ("config=", "Applying config", "Thread starting", "apply_config", "tm_ms="):
            assert needle not in out, needle
