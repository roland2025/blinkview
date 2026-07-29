# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from types import SimpleNamespace

import pytest

from blinkview.ui import run as run_module


class TestRegisterDesktopEntry:
    def test_non_linux_platform_is_a_noop(self, monkeypatch, tmp_path):
        monkeypatch.setattr(run_module.sys, "platform", "win32")
        monkeypatch.setattr(Path, "home", lambda: tmp_path)

        run_module.register_desktop_entry()

        assert not (tmp_path / ".local").exists()

    def test_linux_writes_a_new_desktop_file(self, monkeypatch, tmp_path):
        monkeypatch.setattr(run_module.sys, "platform", "linux")
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr("shutil.which", lambda name: None)

        run_module.register_desktop_entry()

        desktop_file = tmp_path / ".local" / "share" / "applications" / "ee.incubator.blinkview.desktop"
        assert desktop_file.exists()
        assert "Name=BlinkView" in desktop_file.read_text()

    def test_linux_skips_rewriting_when_content_is_already_current(self, monkeypatch, tmp_path):
        monkeypatch.setattr(run_module.sys, "platform", "linux")
        monkeypatch.setattr(Path, "home", lambda: tmp_path)
        monkeypatch.setattr("shutil.which", lambda name: None)

        run_module.register_desktop_entry()  # first call creates it

        write_calls = []
        original_write_text = Path.write_text

        def spy_write_text(self, *a, **kw):
            write_calls.append(self)
            return original_write_text(self, *a, **kw)

        monkeypatch.setattr(Path, "write_text", spy_write_text)

        run_module.register_desktop_entry()  # second call: content is identical, must not rewrite

        assert write_calls == []


class FakeApp:
    """Stand-in for QApplication - run() constructs one, wires a few cosmetic setters, and
    blocks on .exec() until the (real) Qt event loop stops. Faking this out lets these tests
    drive run()'s orchestration logic (what gets constructed, in what order, with what args)
    without a real event loop ever spinning up - .exec() below returns immediately."""

    instances = []

    @staticmethod
    def setAttribute(*a, **kw):
        pass

    @staticmethod
    def setHighDpiScaleFactorRoundingPolicy(*a, **kw):
        pass

    def __init__(self, argv):
        self.argv = argv
        self._stylesheet = ""
        self.exec_returns = 42
        FakeApp.instances.append(self)

    def setApplicationName(self, name):
        self.application_name = name

    def setDesktopFileName(self, name):
        self.desktop_file_name = name

    def setStyle(self, style):
        self.style = style

    def styleSheet(self):
        return self._stylesheet

    def setStyleSheet(self, sheet):
        self._stylesheet = sheet

    def setWindowIcon(self, icon):
        self.window_icon = icon

    def exec(self):
        return self.exec_returns


class FakeFileManager:
    def __init__(self):
        self.replay_source_dir = None


class FakeRegistry:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.file_manager = FakeFileManager()
        FakeRegistry.instances.append(self)


class FakeMainWindow:
    instances = []

    def __init__(self, registry, set_update_version=None):
        self.registry = registry
        self.set_update_version = set_update_version
        self.opacity = None
        self.shown = False
        self.load_ui_state_called = False
        self.start_replay_calls = []
        FakeMainWindow.instances.append(self)

    def setWindowOpacity(self, value):
        self.opacity = value

    def show(self):
        self.shown = True

    def load_ui_state(self):
        self.load_ui_state_called = True

    def start_replay(self, session_info):
        self.start_replay_calls.append(session_info)


def make_args(**overrides):
    fields = {"session": None, "profile": None, "logdir": None, "config": None}
    fields.update(overrides)
    return SimpleNamespace(**fields)


@pytest.fixture(autouse=True)
def isolate_run_dependencies(qapp, monkeypatch):
    """Patches every heavy/real-world side effect run() would otherwise trigger (a second real
    QApplication + blocking event loop, a real Registry/BlinkMainWindow, disk-writing numba cache
    export, network-touching update checks, the dark theme engine) so each test can drive run()
    directly and assert on the orchestration logic alone. qapp (pytest-qt) guarantees a real
    QApplication singleton already exists in the process, in case anything under test still
    touches genuine Qt machinery (e.g. QIcon construction)."""
    FakeApp.instances.clear()
    FakeRegistry.instances.clear()
    FakeMainWindow.instances.clear()

    monkeypatch.setattr("qtpy.QtWidgets.QApplication", FakeApp)
    monkeypatch.setattr("qtpy.QtCore.QTimer.singleShot", staticmethod(lambda delay, fn: fn()))
    monkeypatch.setattr("blinkview.core.registry.Registry", FakeRegistry)
    monkeypatch.setattr("blinkview.ui.main_window.BlinkMainWindow", FakeMainWindow)
    monkeypatch.setattr(
        "blinkview.ui.widgets.update_widget.UpdateWidget.ensure_update_path", staticmethod(lambda settings: True)
    )
    monkeypatch.setattr("blinkview.core.numba_setup.export_numba_cache", lambda settings: "fake_cache_path")
    monkeypatch.setattr("qdarktheme.setup_theme", lambda *a, **kw: None)


class TestNormalLaunch:
    def test_constructs_registry_with_args_and_replay_mode_false_by_default(self):
        args = make_args(session="s", profile="p", logdir="/logs", config="/cfg.json")

        with pytest.raises(SystemExit):
            run_module.run(args)

        assert len(FakeRegistry.instances) == 1
        reg = FakeRegistry.instances[0]
        assert reg.kwargs == {
            "session_name": "s",
            "profile_name": "p",
            "log_dir": "/logs",
            "config_path": "/cfg.json",
            "settings": reg.kwargs["settings"],
            "replay_mode": False,
            "replay_source_dir": None,
        }

    def test_does_not_set_replay_source_dir_when_not_replaying(self):
        args = make_args()

        with pytest.raises(SystemExit):
            run_module.run(args)

        assert FakeRegistry.instances[0].kwargs["replay_source_dir"] is None

    def test_shows_the_window_and_restores_ui_state_but_does_not_start_replay(self):
        args = make_args()

        with pytest.raises(SystemExit):
            run_module.run(args)

        win = FakeMainWindow.instances[0]
        assert win.opacity == 0
        assert win.shown is True
        assert win.load_ui_state_called is True
        assert win.start_replay_calls == []

    def test_exits_with_the_app_exec_return_code(self):
        args = make_args()

        with pytest.raises(SystemExit) as exc_info:
            run_module.run(args)

        assert exc_info.value.code == 42


class TestReplayLaunch:
    def test_passes_replay_source_dir_into_the_registry_constructor(self, tmp_path):
        """Passed as a constructor kwarg (not set on registry.file_manager afterward) so both
        FileManager and IDRegistry see it from their own construction - see
        IDRegistry.__init__'s self-rehydration and Registry.__init__'s own comment on this."""
        args = make_args()
        session_info = SimpleNamespace(path=tmp_path / "some_session", session_id="sess1")

        with pytest.raises(SystemExit):
            run_module.run(args, replay_mode=True, replay_session_info=session_info)

        assert FakeRegistry.instances[0].kwargs["replay_source_dir"] == session_info.path
        assert FakeRegistry.instances[0].kwargs["replay_mode"] is True

    def test_calls_start_replay_after_load_ui_state(self, tmp_path):
        args = make_args()
        session_info = SimpleNamespace(path=tmp_path / "some_session", session_id="sess1")

        with pytest.raises(SystemExit):
            run_module.run(args, replay_mode=True, replay_session_info=session_info)

        win = FakeMainWindow.instances[0]
        assert win.load_ui_state_called is True
        assert win.start_replay_calls == [session_info]


class TestUpdatePathAborted:
    def test_exits_early_without_constructing_a_registry(self, monkeypatch):
        monkeypatch.setattr(
            "blinkview.ui.widgets.update_widget.UpdateWidget.ensure_update_path", staticmethod(lambda settings: False)
        )
        args = make_args()

        with pytest.raises(SystemExit) as exc_info:
            run_module.run(args)

        assert exc_info.value.code == 0
        assert FakeRegistry.instances == []


class TestInstallVersionOnExit:
    def test_pending_update_version_is_installed_after_exec_returns(self, monkeypatch):
        installed = []

        class FakeUpdater:
            def install(self, version):
                installed.append(version)

        monkeypatch.setattr("blinkview.utils.updater.Updater", FakeUpdater)

        original_init = FakeMainWindow.__init__

        def init_and_request_update(self, registry, set_update_version=None):
            original_init(self, registry, set_update_version=set_update_version)
            set_update_version("9.9.9")  # simulates the user picking an update in the UI

        monkeypatch.setattr(FakeMainWindow, "__init__", init_and_request_update)

        args = make_args()

        with pytest.raises(SystemExit):
            run_module.run(args)

        assert installed == ["9.9.9"]


class TestMain:
    def test_parses_argv_and_calls_run(self, monkeypatch):
        calls = []
        monkeypatch.setattr(run_module, "run", lambda args: calls.append(args))
        monkeypatch.setattr(run_module.sys, "argv", ["blinkview", "-s", "mysession", "-p", "myprofile", "-l", "/logs"])

        run_module.main()

        assert len(calls) == 1
        parsed = calls[0]
        assert parsed.session == "mysession"
        assert parsed.profile == "myprofile"
        assert parsed.logdir == "/logs"
        assert parsed.config is None
