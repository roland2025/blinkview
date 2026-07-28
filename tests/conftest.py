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
