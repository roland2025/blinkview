# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import subprocess
import sys
import os


def run(cmd, capture=False):
    # Clean the environment for uvx
    # We remove PYTHONPATH and VIRTUAL_ENV so uvx doesn't get 'poisoned'
    # by the environment created by 'uv run'
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("VIRTUAL_ENV", None)
    # On some systems, PYTHONHOME can also cause issues
    env.pop("PYTHONHOME", None)

    cmd_str = " ".join(cmd) if isinstance(cmd, list) else cmd

    if not capture:
        print(f"Executing: {cmd_str}")
        return subprocess.run(
            cmd_str,
            check=True,
            shell=(sys.platform == "win32"),
            env=env
        )
    else:
        return subprocess.run(
            cmd_str,
            capture_output=True,
            text=True,
            shell=(sys.platform == "win32"),
            env=env
        )


def main():
    if len(sys.argv) < 2:
        print("Usage: uv run scripts/release.py [patch|minor|major|x.y.z]")
        return

    increment = sys.argv[1]

    # Bump version
    try:
        run(["uvx", "hatch", "version", increment])
    except subprocess.CalledProcessError:
        print("\n❌ Version bump failed.")
        return

    # Capture the new version
    result = run("uvx hatch version", capture=True)
    ver = result.stdout.strip()
    print(f"Bumped to version: v{ver}")

    # Git Workflow
    try:
        # We use standard run (no env cleaning needed for git, but safe to use)
        run(["git", "add", "src/blinkview/__init__.py"])
        run(["git", "commit", "-m", f'\"release: v{ver}\"'])
        run(["git", "tag", "-a", f"v{ver}", "-m", f"Release v{ver}"])
        run(["git", "push", "origin", "main"])
        run(["git", "push", "origin", "--tags"])
        print(f"\nSuccessfully released v{ver}")
    except subprocess.CalledProcessError as e:
        print(f"\nGit command failed: {e}")


if __name__ == "__main__":
    main()
