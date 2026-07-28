# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end smoke test: launches the real `blink gui` command (python -m blinkview gui ...) as
an actual OS subprocess - not an in-process call with mocked-out Registry/QApplication like
tests/test_run.py uses - and waits for it to reach real registry startup, then tears it down.

This is deliberately isolated from two real hazards a naive `blink gui` subprocess launch would
hit:

1. UpdateWidget.ensure_update_path() (src/blinkview/ui/widgets/update_widget.py) pops a blocking,
   modal QFileDialog the first time settings["update.path"] isn't already a valid BlinkView
   source tree - which would hang this test forever even under QT_QPA_PLATFORM=offscreen (the
   dialog's event loop still spins waiting for input that will never come). Fixed by pre-seeding
   an isolated global settings.json with update.path pointing at THIS repo, which satisfies
   Updater.is_valid_repo() (checks for .git, pyproject.toml, src/blinkview/__main__.py) without
   needing a second checkout.

2. This repo is itself an actively-used BlinkView project (a real .blinkview/project.json marker
   exists at the repo root from real dogfooding use) - so SettingsManager's default project-root
   discovery would resolve to and could write into that real file. Fixed via the BLINK_PROJECT_ROOT
   env var, which src/blinkview/utils/project_settings.py::get_project_root() already special-cases
   as an override specifically for cases like this - pointing it at a scratch tmp_path directory
   fully isolates project-scope settings from the real repo state.
"""

import json
import os
import queue
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

STARTUP_MARKER = "[Registry] Starting central storage..."
STARTUP_TIMEOUT_SECONDS = 60
SHUTDOWN_TIMEOUT_SECONDS = 10


@pytest.mark.skipif(
    sys.platform == "linux",
    reason="src/blinkview/ui/run.py unconditionally forces QT_QPA_PLATFORM=xcb on Linux "
    "(overriding any offscreen override passed via env), which needs a real or virtual display.",
)
def test_blink_gui_subprocess_starts_and_reaches_registry_start(tmp_path):
    fake_home = tmp_path / "home"
    blink_home = fake_home / ".blinkview"
    blink_home.mkdir(parents=True)
    # Satisfies UpdateWidget.ensure_update_path() without a blocking QFileDialog - see module
    # docstring point 1.
    (blink_home / "settings.json").write_text(json.dumps({"update": {"path": str(REPO_ROOT)}}))

    log_dir = tmp_path / "logs"
    config_path = tmp_path / "config.json"
    fake_project_root = tmp_path / "fake_project_root"  # deliberately never created - see point 2

    env = dict(os.environ)
    env["QT_QPA_PLATFORM"] = "offscreen"
    env["QT_API"] = "pyside6"
    env["HOME"] = str(fake_home)
    env["USERPROFILE"] = str(fake_home)
    env["BLINK_PROJECT_ROOT"] = str(fake_project_root)
    # Without this, the child's stdout is fully buffered (it's a pipe, not a tty) - every
    # print() sits in the child's internal buffer and never reaches us until it fills or the
    # process exits, so the polling loop below would see nothing until far too late.
    env["PYTHONUNBUFFERED"] = "1"

    proc = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-m",
            "blinkview",
            "gui",
            "-s",
            "blink_gui_subprocess_smoke_test",
            "-l",
            str(log_dir),
            "-c",
            str(config_path),
        ],
        cwd=str(REPO_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    lines = []
    started = False

    # A plain blocking readline() loop can't respect an overall deadline on Windows (no
    # select()-on-pipes), so a background thread feeds a queue the main loop polls instead.
    line_queue = queue.Queue()

    def _pump_lines():
        for line in iter(proc.stdout.readline, ""):
            line_queue.put(line)
        line_queue.put(None)  # EOF sentinel

    reader = threading.Thread(target=_pump_lines, daemon=True)
    reader.start()

    try:
        deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            try:
                line = line_queue.get(timeout=max(0.0, deadline - time.monotonic()))
            except queue.Empty:
                break

            if line is None:
                break  # process closed stdout (likely exited) before starting

            lines.append(line)
            if STARTUP_MARKER in line:
                started = True
                break
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=SHUTDOWN_TIMEOUT_SECONDS)

    output = "".join(lines)
    assert started, (
        f"`blink gui` never reached registry startup (looked for {STARTUP_MARKER!r}) "
        f"within {STARTUP_TIMEOUT_SECONDS}s.\nProcess exit code: {proc.poll()}\nOutput so far:\n{output}"
    )
