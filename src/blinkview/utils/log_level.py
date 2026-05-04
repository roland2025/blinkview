# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Callable, Union


class LevelIdentity:
    __slots__ = ("value", "name", "color", "name_log", "name_conf")

    def __init__(self, value: int, name: str, name_log: str, name_conf: str, color: str):
        self.value = value
        self.name = name  # For display and debugging, e.g. "INFO"
        self.name_log = name_log  # For log output, e.g. "[I]" instead of "INFO"
        self.name_conf = name_conf
        self.color = color

    def __int__(self):
        return self.value

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"LogLevel.{self.name_conf}"

    # These allow LogLevel.INFO > LogLevel.DEBUG to still work
    def __gt__(self, other):
        return self.value > int(other)

    def __ge__(self, other):
        return self.value >= int(other)

    def __lt__(self, other):
        return self.value < int(other)

    def __le__(self, other):
        return self.value <= int(other)

    def __eq__(self, other):
        return self.value == int(other)


class LogLevel:
    ALL = LevelIdentity(0, "ALL", "A", "ALL", "#888888")
    """For filtering, ALL is the same as TRACE, but it can be used to indicate "no filtering" in APIs. It will not be printed as "ALL" in logs."""

    TRACE = LevelIdentity(1 << 0, "T", "T", "TRACE", "#888888")
    DEBUG = LevelIdentity(1 << 1, "D", "D", "DEBUG", "#aaaaaa")
    INFO = LevelIdentity(1 << 2, "I", "I", "INFO", "#eeeeee")
    WARN = LevelIdentity(1 << 3, "W", "W", "WARNING", "#FFCC00")
    ERROR = LevelIdentity(1 << 4, "E", "E", "ERROR", "#FF3333")
    FATAL = LevelIdentity(1 << 5, "F", "F", "FATAL", "#ff33cc")
    CRITICAL = LevelIdentity(1 << 6, "C", "C", "CRITICAL", "#ff33cc")

    OFF = LevelIdentity(1 << 7, "OFF", "O", "OFF", "#888888")
    """For filtering, OFF means "no logs", but it can be used to indicate "filter out all logs" in APIs. It will not be printed as "OFF" in logs."""

    # For mapping numeric values (from bytes/JSON) back to objects
    LIST = [ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, CRITICAL, OFF]

    # Levels meant for the user to select as a threshold
    LIST_UI = [ALL, TRACE, DEBUG, INFO, WARN, ERROR, FATAL, CRITICAL]

    DICT = {level.value: level for level in LIST}

    DICT_NAME = {level.name_conf: level for level in LIST}

    LIST_CONF = [TRACE, DEBUG, INFO, WARN, ERROR, FATAL, CRITICAL]

    @classmethod
    def from_value(cls, value: int, default=None) -> LevelIdentity:
        return cls.DICT.get(value, default)

    @classmethod
    def from_string(cls, name: Union[str, LevelIdentity], default=None) -> LevelIdentity:
        if isinstance(name, LevelIdentity):
            return name

        return cls.DICT_NAME.get(name, default)
