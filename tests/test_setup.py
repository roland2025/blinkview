# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import subprocess
import sys
import textwrap
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from blinkview.core.numba_setup import WARMUP_COMPLETE_MARKER
from blinkview.core.warmup import WarmupCancelled
from blinkview.ui import setup as blink_setup
from blinkview.ui.windows.setup_window import PROGRESS_STEPS, SetupWindow

REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeHelper:
    """Stand-in for NumbaWarmupHelper: compile_kernels() only calls run_all() and shuts the
    helper's TaskManager down."""

    def __init__(self, raises=None):
        self.raises = raises
        self.shutdowns = []
        self.shared = SimpleNamespace(tasks=SimpleNamespace(shutdown=lambda: self.shutdowns.append(True)))

    def run_all(self, on_progress=None, cancel=None):
        if on_progress is not None:
            on_progress(0.5, "BinaryParser")
        if self.raises is not None:
            raise self.raises


class TestCompileKernels:
    def test_success_marks_the_cache_complete(self, tmp_path, monkeypatch):
        monkeypatch.setenv("NUMBA_CACHE_DIR", str(tmp_path))
        helper = FakeHelper()
        seen = []

        error = blink_setup.compile_kernels(helper, on_progress=lambda f, label: seen.append((f, label)))

        assert error is None
        assert seen == [(0.5, "BinaryParser")]
        assert (tmp_path / WARMUP_COMPLETE_MARKER).is_file()
        assert helper.shutdowns == [True]

    def test_cancelled_is_reported_and_leaves_the_cache_unmarked(self, tmp_path, monkeypatch):
        monkeypatch.setenv("NUMBA_CACHE_DIR", str(tmp_path))
        helper = FakeHelper(raises=WarmupCancelled())

        assert blink_setup.compile_kernels(helper) == "cancelled"
        assert list(tmp_path.iterdir()) == []
        assert helper.shutdowns == [True]

    def test_a_failing_kernel_is_reported_and_leaves_the_cache_unmarked(self, tmp_path, monkeypatch, capsys):
        monkeypatch.setenv("NUMBA_CACHE_DIR", str(tmp_path))
        helper = FakeHelper(raises=RuntimeError("typing failed"))

        assert blink_setup.compile_kernels(helper) == "typing failed"
        assert list(tmp_path.iterdir()) == []
        assert "typing failed" in capsys.readouterr().err  # the traceback, for a bug report


class TestHeadlessFallback:
    def test_linux_without_a_display_has_no_window(self, monkeypatch):
        monkeypatch.setattr(blink_setup.sys, "platform", "linux")
        monkeypatch.delenv("DISPLAY", raising=False)
        monkeypatch.delenv("WAYLAND_DISPLAY", raising=False)

        assert blink_setup._has_display() is False

        monkeypatch.setenv("WAYLAND_DISPLAY", "wayland-0")
        assert blink_setup._has_display() is True

    def test_other_platforms_always_have_one(self, monkeypatch):
        monkeypatch.setattr(blink_setup.sys, "platform", "win32")
        monkeypatch.delenv("DISPLAY", raising=False)

        assert blink_setup._has_display() is True

    def test_printed_progress_is_one_line_per_kernel_group(self, capsys):
        on_progress = blink_setup._print_progress()

        on_progress(0.0, "BinaryParser")
        on_progress(0.2, "BinaryParser")  # a sub-step of the same group
        on_progress(0.5, "TelemetryPlotter")
        on_progress(1.0, "")

        assert capsys.readouterr().out.splitlines() == ["[  0%] BinaryParser", "[ 50%] TelemetryPlotter"]


class TestSetupWindow:
    """compile_fn is a fake that blocks until the test releases it, like test_main_window's
    controlled_warmup."""

    @pytest.fixture
    def compile_state(self):
        return SimpleNamespace(
            release=threading.Event(), started=threading.Event(), result=None, raises=None, thread=None, cancel=None
        )

    @pytest.fixture
    def window(self, qtbot, compile_state):
        def compile_fn(on_progress, cancel):
            compile_state.thread = threading.current_thread()
            compile_state.cancel = cancel
            compile_state.started.set()
            on_progress(0.25, "BinaryParser")
            while not compile_state.release.wait(0.01):
                if cancel.is_set():
                    return "cancelled"
            if compile_state.raises is not None:
                raise compile_state.raises
            return compile_state.result

        window = SetupWindow(compile_fn)
        qtbot.addWidget(window)
        window.show()
        yield window
        compile_state.release.set()  # never leave the compile thread running

    def test_compiles_off_the_main_thread_and_shows_progress(self, window, qtbot, compile_state):
        window.start()
        assert compile_state.started.wait(5)

        assert compile_state.thread is not threading.main_thread()
        qtbot.waitUntil(lambda: window.progress_bar.value() == PROGRESS_STEPS // 4, timeout=5000)
        assert window.status_label.text() == "25% - BinaryParser"
        assert window.exit_code is None

    def test_closes_itself_with_exit_code_0_when_done(self, window, qtbot, compile_state):
        window.start()
        compile_state.release.set()

        qtbot.waitUntil(lambda: window.exit_code == 0, timeout=5000)
        assert window.isVisible() is False
        assert window.error is None

    def test_cancel_waits_for_the_compile_thread_then_closes_with_exit_code_1(self, window, qtbot, compile_state):
        window.start()
        assert compile_state.started.wait(5)

        window.button.click()

        assert compile_state.cancel.is_set()
        assert window.button.isEnabled() is False
        qtbot.waitUntil(lambda: window.exit_code == 1, timeout=5000)
        assert window.isVisible() is False
        assert window.error is None  # cancelled, not failed

    def test_closing_the_window_mid_compile_cancels_instead(self, window, qtbot, compile_state):
        window.start()
        assert compile_state.started.wait(5)

        window.close()

        assert window.isVisible() is True  # still up until the thread has stopped
        assert compile_state.cancel.is_set()
        qtbot.waitUntil(lambda: window.exit_code == 1, timeout=5000)
        assert window.isVisible() is False

    def test_failure_stays_on_screen_until_closed(self, window, qtbot, compile_state):
        compile_state.result = "typing failed"
        window.start()
        compile_state.release.set()

        qtbot.waitUntil(lambda: window.error == "typing failed", timeout=5000)
        assert window.isVisible() is True
        assert "typing failed" in window.status_label.text()
        assert window.button.text() == "Close"

        window.button.click()

        assert window.isVisible() is False
        assert window.exit_code == 1

    def test_an_exception_in_the_compile_thread_is_shown_as_a_failure(self, window, qtbot, compile_state):
        compile_state.raises = RuntimeError("boom")
        window.start()
        compile_state.release.set()

        qtbot.waitUntil(lambda: window.error == "boom", timeout=5000)
        assert window.exit_code == 1

    def test_closing_before_the_compile_started_is_a_cancel(self, window):
        window.close()

        assert window.isVisible() is False
        assert window.exit_code == 1


def test_setup_is_a_blink_subcommand(monkeypatch, capsys):
    from blinkview import __main__ as blink_main

    ran = []
    monkeypatch.setattr(blink_main, "run_setup", ran.append)
    monkeypatch.setattr("blinkview.utils.updater.get_update_notice", lambda: None)
    monkeypatch.setattr(sys, "argv", ["blink", "setup", "--no-gui"])

    blink_main.main()

    assert len(ran) == 1 and ran[0].no_gui is True


def test_fresh_interpreter_setup_warms_the_same_kernels_as_the_gui():
    """`blink setup` never imports ui/main_window.py, so it only compiles what the GUI will use if
    both reach the same @register_warmup callbacks (and widgets). In-process every test module has
    already imported main_window, which hides a module missing from one of the two import lists -
    hence two fresh interpreters, one per path."""

    def registered(imports: str) -> list[str]:
        script = textwrap.dedent(
            f"""
            import os
            os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
            os.environ.setdefault("QT_API", "pyside6")
            {imports}
            from blinkview.core.warmup_registry import _WARMUP_CALLBACKS
            from blinkview.ui.widget_registry import build_widget_factory_map
            for _priority, cb in _WARMUP_CALLBACKS:
                print("warmup", cb.__module__, cb.__qualname__)
            for name in build_widget_factory_map():
                print("widget", name)
            """
        )
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, cwd=str(REPO_ROOT))
        assert result.returncode == 0, f"stdout:\n{result.stdout}\n\nstderr:\n{result.stderr}"
        return sorted(line for line in result.stdout.splitlines() if line.startswith(("warmup ", "widget ")))

    gui = registered("import blinkview.core.registry, blinkview.ui.main_window")
    setup = registered(
        "from blinkview.ui.setup import build_warmup_helper; build_warmup_helper(None).shared.tasks.shutdown()"
    )

    assert setup == gui
    assert sum(line.startswith("widget ") for line in gui) == 7
    assert any("plotter" in line for line in gui)  # a widget's kernels, not only the pipeline's


class TestEnsureSourceRepo:
    """A source checkout's cache lives next to the repo, so setup needs update.path like the GUI."""

    class Settings:
        def __init__(self, path=None):
            self.path = path

        def get(self, key, default=None):
            return self.path if key == "update.path" else default

    @pytest.fixture
    def asked(self, monkeypatch):
        from blinkview.ui.widgets.update_widget import UpdateWidget

        asked = SimpleNamespace(calls=[], answer=True)

        def fake_ensure(settings):
            asked.calls.append(settings)
            return asked.answer

        monkeypatch.setattr(UpdateWidget, "ensure_update_path", staticmethod(fake_ensure))
        return asked

    def test_a_valid_path_asks_nothing(self, asked, monkeypatch):
        monkeypatch.setattr("blinkview.utils.updater.GitUpdater.is_valid_repo", staticmethod(lambda p: True))

        assert blink_setup.ensure_source_repo(self.Settings("/repo"), headless=False) is True
        assert asked.calls == []

    def test_without_a_path_it_asks_with_the_guis_dialog(self, asked):
        settings = self.Settings()

        assert blink_setup.ensure_source_repo(settings, headless=False) is True
        assert asked.calls == [settings]

    def test_a_dismissed_dialog_stops_setup(self, asked):
        asked.answer = False

        assert blink_setup.ensure_source_repo(self.Settings(), headless=False) is False

    def test_headless_cannot_ask_and_says_how_to_set_it(self, asked, capsys):
        assert blink_setup.ensure_source_repo(self.Settings(), headless=True) is False
        assert asked.calls == []
        assert "blink config --global update.path" in capsys.readouterr().err
