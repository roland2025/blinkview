# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from traceback import format_exc
from typing import Callable

from .log_row import LogRow
from ..utils.level_map import LogLevel
from ..utils.log_level import LevelIdentity


class BaseLogger:
    __slots__ = ('log',)

    def trace(self, msg: str):  self.log(msg, LogLevel.TRACE)

    def debug(self, msg: str): self.log(msg, LogLevel.DEBUG)

    def info(self, msg: str):  self.log(msg, LogLevel.INFO)

    def warn(self, msg: str):  self.log(msg, LogLevel.WARN)

    warning = warn  # Alias for convenience

    def error(self, msg: str, exc=None):
        if exc:
            # Provide the type and message of the exception for quick triage
            msg = f"{msg} | {type(exc).__name__}: {exc}"
        self.log(msg, LogLevel.ERROR)

    def exception(self, msg: str, exc=None):
        exc_text = format_exc()
        if exc:
            print(exc_text)
            # Provide the type and message of the exception for quick triage
            msg = f"{msg} | {type(exc).__name__}: {exc}"

        """Helper to catch the current sys.exc_info() automatically."""
        exc_str = exc_text.splitlines()[-1]  # Just the last line

        self.log(f"{msg} | {exc_str}", LogLevel.ERROR)

    log: Callable[[str, LevelIdentity], None]


class SystemLogger(BaseLogger):
    """
    A contextual logger that routes system events (Reader/Parser status)
    to the SYSTEM namespace in the Registry.
    """

    def __init__(self, category: str, owner_name: str, registry):
        """
        Args:
            category: e.g., 'reader', 'parser'
            owner_name: e.g., 'RNG', 'C3X'
            registry: The global Registry
            queue: The BatchQueue for logs
        """

        from .registry import Registry
        registry: Registry
        print(f"Logging: reorder: {registry.reorder is not None}, central: {registry.central is not None}")
        put_fn = registry.reorder.put if registry.reorder else registry.central.put
        time_ns = registry.now_ns
        dev_id = registry.get_device("SYSTEM")

        # Pre-resolve the module ID once (e.g., 'reader.RNG')
        module_path = f"{category}"
        if owner_name is not None:
            module_path += f".{owner_name}"

        mod_id = dev_id.get_module(module_path)
        LogRowCtr = LogRow

        def fast_log(msg: str, level: LevelIdentity):
            """Internal worker to package and queue the LogRow."""
            # Use time_ns() for high-precision telemetry alignment
            # print(f"Logging: {level} SYSTEM {module_path} \t{msg}")
            row = LogRowCtr(
                time_ns(),
                level,
                mod_id,
                msg
            )
            put_fn([row])

        self.log = fast_log


class PrintLogger(BaseLogger):
    __slots__ = ('ctx',)

    def __init__(self, category: str, owner_name: str = None, queue=None, time_ns=None):
        """
        Dummy Logger: Bypasses Registry/Queue and prints directly to console.
        """
        # Create a context string for the print output
        ctx = f"{category}"
        if owner_name:
            ctx += f".{owner_name}"

        self.ctx = ctx

        from time import strftime, localtime
        strftime_ = strftime
        localtime_ = localtime

        # The 'dummy' fast_log replaces the registry-based one
        def fast_log(msg: str, level_name: LevelIdentity):
            # Format: [TIME] LEVEL [CATEGORY.OWNER] MESSAGE
            # Using .2f for seconds to keep it readable
            t = strftime_("%H:%M:%S", localtime_())
            print(f"{t} {level_name} SYSTEM {ctx} \t{msg}")

            if queue is not None and time_ns is not None:
                queue.put((time_ns(), ctx, level_name, msg))

        self.log = fast_log
