# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ.setdefault("QT_API", "pyside6")

import pytest  # noqa: E402

from blinkview.core.array_pool import NumpyArrayPool  # noqa: E402
from blinkview.core.id_registry.registry import IDRegistry  # noqa: E402
from blinkview.utils.log_filter import LogFilter  # noqa: E402
from blinkview.utils.log_level import LogLevel  # noqa: E402


@pytest.fixture
def id_registry():
    return IDRegistry(NumpyArrayPool())


@pytest.fixture
def array_pool():
    return NumpyArrayPool()


@pytest.fixture
def log_filter(id_registry):
    return LogFilter(id_registry, log_level=LogLevel.ALL.name_conf)


@pytest.fixture(autouse=True)
def _reset_window_title_prefix():
    """BlinkMainWindow sets a process-wide "{project} / {profile} - " title prefix
    (ui/utils/window_title.py); don't let one test's main window leak it into another's titles."""
    from blinkview.ui.utils import window_title

    yield
    window_title._prefix = ""
