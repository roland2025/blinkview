# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from traceback import format_exc
from typing import Callable

from ..utils.log_level import LevelIdentity, LogLevel


class BaseLogger:
    __slots__ = ("log",)

    def trace(self, msg: str):
        self.log(msg, LogLevel.TRACE)

    def debug(self, msg: str):
        self.log(msg, LogLevel.DEBUG)

    def info(self, msg: str):
        self.log(msg, LogLevel.INFO)

    def warn(self, msg: str):
        self.log(msg, LogLevel.WARN)

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

    def child(self, name: str) -> "BaseLogger":
        """
        Creates a child logger with an appended module path.
        This method should be overridden by subclasses to return the correct type.
        """
        raise NotImplementedError("Child loggers must implement the child() method.")


class SystemLogger(BaseLogger):
    """
    A contextual logger that routes system events to the SYSTEM namespace.
    Supports hierarchical child loggers.
    """

    def __init__(self, category: str, owner_name: str, registry, _internal_path: str = None):
        from .registry import Registry

        self.registry: Registry = registry
        self.category = category
        self.owner_name = owner_name

        # Determine the module path: either inherited from a parent or built from scratch
        if _internal_path:
            self.module_path = _internal_path
        else:
            self.module_path = f"{category}"
            if owner_name:
                self.module_path += f".{owner_name}"

        # Resolve IDs and resources once during initialization
        mod_id = self.registry.system_device.get_module(self.module_path).id

        system_log_append = self.registry.log_append

        time_ns = registry.now_ns

        # The fast_log closure remains optimized for speed
        def fast_log(msg: str, level: LevelIdentity):
            system_log_append(time_ns(), level.value, mod_id, msg)

        self.log = fast_log

    def child(self, name: str) -> "SystemLogger":
        """
        Creates a new SystemLogger instance with an appended module path.
        Example: 'reader.RNG' -> 'reader.RNG.Validator'
        """
        new_path = f"{self.module_path}.{name}"
        return SystemLogger(
            category=self.category, owner_name=self.owner_name, registry=self.registry, _internal_path=new_path
        )

    def child_creator(self, name: str) -> Callable[[], "SystemLogger"]:
        """
        Returns a callable that creates a child logger with the specified name.
        This is useful for deferred logger creation in factories or dynamic contexts.
        """

        def creator():
            return self.child(name)

        return creator


class PrintLogger(BaseLogger):
    __slots__ = ("ctx",)

    def __init__(self, category: str, owner_name: str = None, queue_put=None, time_ns=None):
        """
        Dummy Logger: Bypasses Registry/Queue and prints directly to console.
        """
        # Create a context string for the print output
        ctx = f"{category}"
        if owner_name:
            ctx += f".{owner_name}"

        self.ctx = ctx

        from time import localtime, strftime

        strftime_ = strftime
        localtime_ = localtime
        print_ = print

        # The 'dummy' fast_log replaces the registry-based one
        def fast_log(msg: str, level_name: LevelIdentity):
            # Format: [TIME] LEVEL [CATEGORY.OWNER] MESSAGE
            # Using .2f for seconds to keep it readable
            t = strftime_("%H:%M:%S", localtime_())
            print_(f"{t} {level_name} SYSTEM {ctx} \t{msg}")

            if queue_put is not None and time_ns is not None:
                queue_put((time_ns(), ctx, level_name, msg))

        self.log = fast_log
