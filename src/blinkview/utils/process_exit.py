# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Ends the process when a daemon thread is stuck in a call that never returns.

A normal exit unloads every DLL, and a driver DLL whose own lock is held by such a thread waits
for it forever in its unload handler: the window is gone, the session is saved, but the process
stays and keeps its device open (seen with the J-Link DLL - the probe stayed "already open" for
the next start). os._exit() does not help, it runs the same unload handlers."""

import atexit
import os
import sys


def terminate_if_threads_stuck(exit_code=0) -> None:
    """Called where the process is about to exit, with what it would exit with (an int, or
    whatever SystemExit.code holds). Returns when no daemon thread is stuck."""
    # Not imported: a command that never started a daemon has nothing to check.
    base_daemon = sys.modules.get("blinkview.core.base_daemon")
    if base_daemon is None:
        return
    stuck = base_daemon.stuck_threads()
    if not stuck:
        return

    names = ", ".join(thread.name for thread in stuck)
    print(f"Exiting without unloading drivers: still blocked in a call: {names}", file=sys.stderr)

    if exit_code is None:
        exit_code = 0
    elif not isinstance(exit_code, int):
        print(exit_code, file=sys.stderr)  # what the interpreter does with sys.exit("message")
        exit_code = 1

    # Everything a normal exit would still have done on the Python side.
    try:
        atexit._run_exitfuncs()
    except Exception:
        pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.flush()
        except Exception:
            pass

    _terminate_process(exit_code)


def _terminate_process(exit_code: int) -> None:
    if sys.platform == "win32":
        import ctypes

        kernel32 = ctypes.windll.kernel32
        kernel32.GetCurrentProcess.restype = ctypes.c_void_p
        kernel32.TerminateProcess.argtypes = [ctypes.c_void_p, ctypes.c_uint]
        kernel32.TerminateProcess(kernel32.GetCurrentProcess(), exit_code & 0xFFFFFFFF)
    os._exit(exit_code)
