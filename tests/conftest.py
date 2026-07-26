import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
os.environ.setdefault("QT_API", "pyside6")

# Pre-warms the core.device_identity/core.id_registry import cluster in a safe order before any
# test module gets a chance to import a blinkview.ui.widgets.* module as its very first touch of
# that cluster - which trips a pre-existing circular import (core.device_identity <->
# core.id_registry.registry) that only avoids tripping when something else imports
# core.id_registry first. blinkview.ui.main_window (the real app entry point) imports
# core.registry before any widget module for the same reason, which is why it never hits this.
import blinkview.core.registry  # noqa: E402, F401

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
