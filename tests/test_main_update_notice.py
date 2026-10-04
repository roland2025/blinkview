# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The cached "new version available" notice goes to stderr, not stdout.

Subprocess, because the point is what a redirected stdout gets: on Windows it's cp1252, and the
notice's emoji used to raise UnicodeEncodeError there before the command even ran. stderr uses
backslashreplace, so it can't crash."""

import json
import os
import subprocess
import sys


def test_update_notice_goes_to_stderr_and_stdout_stays_clean(tmp_path):
    home = tmp_path / "home"
    (home / ".blinkview").mkdir(parents=True)
    (home / ".blinkview" / "settings.json").write_text(json.dumps({"update": {"latest_version": "999.0.0"}}))

    env = dict(os.environ)
    env["HOME"] = str(home)
    env["USERPROFILE"] = str(home)
    env["BLINK_PROJECT_ROOT"] = str(tmp_path / "no_project")  # never created: no project settings
    env.pop("PYTHONIOENCODING", None)
    env.pop("PYTHONUTF8", None)

    result = subprocess.run(
        [sys.executable, "-m", "blinkview", "replay", "--list"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr.decode(errors="replace")
    assert b"New version available" in result.stderr
    assert b"New version available" not in result.stdout
    assert b"No replay sessions found" in result.stdout
