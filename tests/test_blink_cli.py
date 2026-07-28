# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Unit tests for blinkview.__main__.main() - the argparse dispatcher behind the `blink` console
script (see pyproject.toml's [project.scripts] blink = "blinkview.__main__:main"). main() reads
sys.argv directly rather than accepting an argv parameter, and its subcommand handlers
(run_gui/run_cli/run_daemon) are plain module-level names looked up at call time - so these tests
monkeypatch sys.argv and those module attributes rather than passing arguments in directly."""

import sys

import pytest

from blinkview import __main__ as main_module


@pytest.fixture
def set_argv(monkeypatch):
    def _set(*args):
        monkeypatch.setattr(sys, "argv", ["blink", *args])

    return _set


class TestSubcommandDispatch:
    def test_gui_command_dispatches_to_run_gui_with_parsed_args(self, set_argv, monkeypatch):
        calls = []
        monkeypatch.setattr(main_module, "run_gui", lambda args: calls.append(args))
        set_argv("gui", "-s", "myproject")

        main_module.main()

        assert len(calls) == 1
        assert calls[0].session == "myproject"

    def test_cli_command_dispatches_to_run_cli(self, set_argv, monkeypatch):
        calls = []
        monkeypatch.setattr(main_module, "run_cli", lambda args: calls.append(args))
        set_argv("cli")

        main_module.main()

        assert len(calls) == 1

    def test_daemon_command_prints_stub_message_and_does_not_dispatch_elsewhere(self, set_argv, capsys):
        """daemon is an intentional stub (src/blinkview/__main__.py::run_daemon) - just a print,
        no actual background service - so this only asserts the stub still fires correctly."""
        set_argv("daemon", "--port", "4242")

        main_module.main()

        assert "4242" in capsys.readouterr().out


class TestArgumentInjection:
    """main() rewrites sys.argv before argparse ever sees it: if the first token isn't a
    recognized subcommand (and isn't -h/--help/-v/--version), it inserts "gui" so bare flags or
    no arguments at all default to the GUI - see src/blinkview/__main__.py's
    '--- ARGUMENT INJECTION LOGIC ---' block."""

    def test_no_arguments_at_all_defaults_to_gui(self, set_argv, monkeypatch):
        calls = []
        monkeypatch.setattr(main_module, "run_gui", lambda args: calls.append(args))
        set_argv()  # sys.argv == ["blink"]

        main_module.main()

        assert len(calls) == 1

    def test_bare_flags_without_a_subcommand_default_to_gui(self, set_argv, monkeypatch):
        calls = []
        monkeypatch.setattr(main_module, "run_gui", lambda args: calls.append(args))
        set_argv("-s", "myproject")  # sys.argv == ["blink", "-s", "myproject"]

        main_module.main()

        assert len(calls) == 1
        assert calls[0].session == "myproject"

    def test_help_flag_is_not_treated_as_an_implicit_gui_invocation(self, set_argv, monkeypatch):
        calls = []
        monkeypatch.setattr(main_module, "run_gui", lambda args: calls.append(args))
        set_argv("--help")

        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

        # argparse's own --help handling exits 0 before ever reaching args.func
        assert exc_info.value.code == 0
        assert not calls


class TestDispatchedCommandFailure:
    def test_an_exception_from_the_dispatched_command_exits_with_code_1(self, set_argv, monkeypatch):
        def _boom(args):
            raise RuntimeError("simulated failure inside a dispatched command")

        monkeypatch.setattr(main_module, "run_gui", _boom)
        set_argv("gui")

        with pytest.raises(SystemExit) as exc_info:
            main_module.main()

        assert exc_info.value.code == 1
