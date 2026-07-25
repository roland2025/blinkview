from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_history import IdHistory
from blinkview.core.logger import PrintLogger

from tests.fakes.log_pool import FakeLogPool


class FakeCentral:
    def __init__(self, log_pool=None):
        self.log_pool = log_pool if log_pool is not None else FakeLogPool()


class FakeSystemCtx:
    def __init__(self, array_pool=None):
        self.array_pool = array_pool if array_pool is not None else NumpyArrayPool()


class FakeRegistry:
    def __init__(self, log_pool=None, array_pool=None, pid_history=None):
        self.central = FakeCentral(log_pool)
        self.system_ctx = FakeSystemCtx(array_pool)
        self.pid_history = pid_history if pid_history is not None else IdHistory()


class FakeGuiContext:
    """Minimal stand-in for GUIContext: only exposes what LogTableStore touches directly."""

    def __init__(self, id_registry, registry=None, logger=None):
        self.id_registry = id_registry
        self.registry = registry if registry is not None else FakeRegistry()
        self.logger = logger if logger is not None else PrintLogger("test")
