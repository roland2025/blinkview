# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""--params/--param through the real `blink` entry point in a fresh process (argv parsing ->
ui/run.py -> Registry -> ConfigManager), isolated the same way as tests/test_blink_gui_subprocess.py
(see its module docstring: pre-seeded update.path, BLINK_PROJECT_ROOT, offscreen Qt)."""

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
TIMEOUT_SECONDS = 60

SERIAL_PATH = "/sources/src_a/serial_number"

pytestmark = pytest.mark.skipif(
    sys.platform == "linux",
    reason="ui/run.py forces QT_QPA_PLATFORM=xcb on Linux, which needs a display (see test_blink_gui_subprocess.py)",
)


@pytest.fixture
def blink(tmp_path):
    fake_home = tmp_path / "home"
    (fake_home / ".blinkview").mkdir(parents=True)
    (fake_home / ".blinkview" / "settings.json").write_text(json.dumps({"update": {"path": str(REPO_ROOT)}}))

    config_path = tmp_path / "rtt.json"
    config_path.write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {"src_a": {"enabled": False, "type": "jlink_rtt", "name": "rtt", "serial_number": "1"}},
                "pipelines": {},
                "parameters": {"rtt_serial": {"paths": [SERIAL_PATH]}},
                # A profile missing these top-level keys crashes configure_system/start regardless
                # of parameters (known, see tests/test_registry_bootstrap.py).
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )

    env = dict(os.environ)
    env.update(
        QT_QPA_PLATFORM="offscreen",
        QT_API="pyside6",
        HOME=str(fake_home),
        USERPROFILE=str(fake_home),
        BLINK_PROJECT_ROOT=str(tmp_path / "fake_project_root"),
        PYTHONUNBUFFERED="1",
    )
    # The child must resolve its own Numba cache (update.path above -> this repo's warm cache);
    # an inherited value may point at another test's tmp dir and force a >60s cold compile.
    env.pop("NUMBA_CACHE_DIR", None)

    def start(*extra):
        return subprocess.Popen(
            [
                sys.executable,
                "-u",
                "-m",
                "blinkview",
                "gui",
                "-l",
                str(tmp_path / "logs"),
                "-c",
                str(config_path),
                *extra,
            ],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

    start.config_path = config_path
    start.logs = tmp_path / "logs"
    return start


def wait_for(proc, marker: str) -> tuple[bool, str]:
    """Reads output until `marker` or exit/timeout (via a thread - no select() on Windows pipes)."""
    lines = queue.Queue()

    def pump():
        for line in iter(proc.stdout.readline, ""):
            lines.put(line)
        lines.put(None)

    threading.Thread(target=pump, daemon=True).start()
    seen = []
    deadline = time.monotonic() + TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            line = lines.get(timeout=max(0.0, deadline - time.monotonic()))
        except queue.Empty:
            break
        if line is None:
            break
        seen.append(line)
        if marker in line:
            return True, "".join(seen)
    return False, "".join(seen)


def stop(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)


def test_params_set_and_param_reach_the_running_registry(blink):
    blink.config_path.with_name("rtt.params.left.json").write_text(json.dumps({"rtt_serial": 51024923}))
    proc = blink("--params", "left")
    try:
        started, output = wait_for(proc, STARTUP_MARKER)
    finally:
        stop(proc)
    assert started, f"blink never reached registry start.\n{output}"

    sessions = [d for d in blink.logs.rglob("*") if (d / "metadata.json").is_file()]
    assert len(sessions) == 1 and sessions[0].name.endswith("_left")
    meta = json.loads((sessions[0] / "metadata.json").read_text())
    assert meta["config"]["params"] == {"rtt_serial": "51024923"}

    snapshot = [p for p in sessions[0].glob("*.json") if p.name.endswith(".start.json") and "gui" not in p.name]
    assert snapshot, f"no config .start snapshot in {list(sessions[0].iterdir())}"
    assert json.loads(snapshot[0].read_text())["sources"]["src_a"]["serial_number"] == "51024923"

    # The shared profile itself still holds its own default.
    assert json.loads(blink.config_path.read_text())["sources"]["src_a"]["serial_number"] == "1"


def test_unknown_param_exits_2_with_message_and_no_session(blink):
    proc = blink("--param", "nope=1")
    try:
        out, _ = proc.communicate(timeout=TIMEOUT_SECONDS)
    finally:
        stop(proc)
    assert proc.returncode == 2, out
    assert "Unknown parameter(s): nope. Declared parameters: rtt_serial" in out
    assert not [d for d in blink.logs.rglob("*") if (d / "metadata.json").is_file()]
