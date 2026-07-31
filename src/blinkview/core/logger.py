# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from traceback import format_exc
from typing import Callable, Optional, Protocol

from blinkview.utils.log_level import LevelIdentity, LogLevel


class LogCallable(Protocol):
    def __call__(self, level: LevelIdentity, fmt: str, *args: object) -> None: ...


class BaseLogger:
    __slots__ = ("log",)

    def trace(self, fmt: str, *args):
        self.log(LogLevel.TRACE, fmt, *args)

    def debug(self, fmt: str, *args):
        self.log(LogLevel.DEBUG, fmt, *args)

    def info(self, fmt: str, *args):
        self.log(LogLevel.INFO, fmt, *args)

    def warn(self, fmt: str, *args):
        self.log(LogLevel.WARN, fmt, *args)

    warning = warn  # Alias for convenience

    def error(self, fmt: str, *args, exc=None):
        if exc is not None:
            # Provide the type and message of the exception for quick triage
            fmt = f"{fmt} | %s: %s"
            args = (*args, type(exc).__name__, exc)
        self.log(LogLevel.ERROR, fmt, *args)

    def exception(self, fmt: str, *args, exc=None):
        """Helper to catch the current sys.exc_info() automatically."""
        exc_text = format_exc()
        if exc is not None:
            print(exc_text)
            # Provide the type and message of the exception for quick triage
            fmt = f"{fmt} | %s: %s"
            args = (*args, type(exc).__name__, exc)

        exc_str = exc_text.splitlines()[-1]  # Just the last line

        self.log(LogLevel.ERROR, f"{fmt} | %s", *args, exc_str)

    log: LogCallable

    def child(self, name: str, enabled: Optional[bool] = None, essential: Optional[bool] = None) -> "BaseLogger":
        """
        Creates a child logger with an appended module path.
        This method should be overridden by subclasses to return the correct type.
        """
        raise NotImplementedError("Child loggers must implement the child() method.")


class SystemLogger(BaseLogger):
    __slots__ = "category", "owner_name", "module_path", "registry", "_enabled", "is_essential"

    """
    A contextual logger that routes system events to the SYSTEM namespace.
    Supports hierarchical child loggers, lazy initialization, and dynamic enablement.
    """

    def __init__(
        self,
        category: str,
        owner_name: str,
        registry,
        _internal_path: Optional[str] = None,
        enabled: bool = True,
        essential: bool = False,
    ):
        self.registry = registry
        self.category = category
        self.owner_name = owner_name
        self._enabled = enabled
        self.is_essential = essential

        # Determine the module path: either inherited from a parent or built from scratch
        if _internal_path:
            self.module_path = _internal_path
        else:
            self.module_path = f"{category}"
            if owner_name:
                self.module_path += f".{owner_name}"

        # Assign the appropriate initial log function based on the enabled flag
        self.log = self._lazy_log if enabled else self._noop_log

    @property
    def enabled(self) -> bool:
        """Returns the current enabled state of the logger."""
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool):
        """
        Dynamically toggle the logger.
        Updates the internal log pointer to ensure zero-overhead no-ops when disabled.
        """
        self._enabled = value
        if not value:
            self.log = self._noop_log
        else:
            # Revert to lazy initialization so resources are fetched on the next log call
            self.log = self._lazy_log

    def _lazy_log(self, level: LevelIdentity, fmt: str, *args):
        """
        Invoked only on the first log call. Resolves dependencies and overwrites
        itself with the highly optimized fast_log closure.
        """
        registry = self.registry

        # Resolve IDs and resources just-in-time
        module = registry.system_device.get_module(self.module_path)
        module.set_essential(self.is_essential)
        mod_id = module.id
        system_log_append = registry.log_append
        time_ns = registry.now_ns

        # The fast_log closure remains optimized for speed
        def fast_log(lvl: LevelIdentity, fmt: str, *args):
            message = fmt % args if args else fmt
            system_log_append(time_ns(), lvl.value, mod_id, message)

        # Overwrite self.log so future calls skip the initialization
        self.log = fast_log

        # Process the current log request
        fast_log(level, fmt, *args)

    def _noop_log(self, level: LevelIdentity, fmt: str, *args):
        """A zero-overhead discard function used when the logger is disabled."""
        pass

    def child(self, name: str, enabled: Optional[bool] = None, essential: Optional[bool] = None) -> "SystemLogger":
        """
        Creates a new SystemLogger instance with an appended module path.
        Example: 'reader.RNG' -> 'reader.RNG.Validator'
        """
        new_path = f"{self.module_path}.{name}"

        # Inherit parent's state unless explicitly overridden
        child_enabled = enabled if enabled is not None else self._enabled

        child_essential = essential if essential is not None else self.is_essential

        return SystemLogger(
            category=self.category,
            owner_name=self.owner_name,
            registry=self.registry,
            _internal_path=new_path,
            enabled=child_enabled,
            essential=child_essential,
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
    __slots__ = ("ctx", "queue_put", "time_ns", "is_essential")

    def __init__(
        self,
        category: str,
        owner_name: str = None,
        queue_put=None,
        time_ns=None,
        _internal_ctx: str = None,
        essential: bool = False,
    ):
        """
        Dummy Logger: Bypasses Registry/Queue and prints directly to console.
        Supports hierarchical child loggers.
        """
        self.is_essential = essential

        # Determine the context string (inherited if internal, otherwise built)
        if _internal_ctx:
            self.ctx = _internal_ctx
        else:
            ctx = f"{category}"
            if owner_name:
                ctx += f".{owner_name}"
            self.ctx = ctx

        # Store references to allow child logger creation
        self.queue_put = queue_put
        self.time_ns = time_ns

        from time import localtime, strftime

        # Localize variables for the fast_log closure
        strftime_ = strftime
        localtime_ = localtime
        print_ = print
        ctx_ = self.ctx
        q_put = self.queue_put
        t_ns = self.time_ns

        def fast_log(level_name: LevelIdentity, fmt: str, *args):
            # Format: [TIME] LEVEL SYSTEM [CONTEXT] MESSAGE
            msg = fmt % args if args else fmt
            t = strftime_("%H:%M:%S", localtime_())
            print_(f"{t} {level_name} SYSTEM {ctx_} \t{msg}")

            if q_put is not None and t_ns is not None:
                q_put((t_ns(), ctx_, level_name, msg))

        self.log = fast_log

    def child(self, name: str, enabled: Optional[bool] = None, essential: Optional[bool] = None) -> "PrintLogger":
        """
        Creates a new PrintLogger instance with an appended context path.
        Matches BaseLogger signature to support enabled/essential overrides.
        """
        new_path = f"{self.ctx}.{name}"
        child_essential = essential if essential is not None else self.is_essential

        return PrintLogger(
            category="",
            owner_name="",
            queue_put=self.queue_put,
            time_ns=self.time_ns,
            _internal_ctx=new_path,
            essential=child_essential,
        )

    def child_creator(self, name: str) -> Callable[[], "PrintLogger"]:
        """
        Returns a callable that creates a child logger with the specified name.
        Useful for deferred initialization.
        """

        def creator():
            return self.child(name)

        return creator
