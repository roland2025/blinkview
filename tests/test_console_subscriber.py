import tempfile
import time

import pytest
from rich.console import Console

import blinkview.subscribers.console  # noqa: F401 - registers "console" with SubscriberFactory
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "console_test", profile_name="console_test_profile", start=True)
    yield reg
    reg.stop()


def test_console_subscriber_prints_live_rows(registry):
    """Regression test: ConsoleSubscriber.run() used to iterate pushed PooledLogBatch rows as
    if they were objects with .level/.module.device/.message attributes - that contract predates
    the numpy_log rewrite where iteration became plain (ts, msg_bytes, ...) tuples of raw ids,
    so `blink cli` crashed with an AttributeError as soon as real log data flowed. It now pulls
    from central.log_pool the same way ui/widgets/log_viewer.py does (get_reversed_snapshot +
    segment_filter_reversed + nb_segment_format)."""
    console = Console(record=True, force_terminal=True, width=200)
    sub = registry.build_subscriber("CLI", "Console", console=console)
    sub.set_level(LogLevel.ALL)
    sub.start()

    try:
        time.sleep(0.2)  # let the poll loop establish its starting watermark

        device = registry.id_registry.get_device("consoletest")
        module = device.get_module("log")

        batch = registry.system_ctx.array_pool.create(
            PooledLogBatch, 8, 512, has_levels=True, has_modules=True, has_devices=True
        )
        now = registry.now_ns()
        batch.insert_any(
            now, now, b"HELLO_FROM_CONSOLE_TEST", level=LogLevel.INFO.value, module=module.id, device=device.id
        )
        with batch:
            registry.central.put(batch)

        deadline = time.time() + 2.0
        output = ""
        while time.time() < deadline:
            output = console.export_text()
            if "HELLO_FROM_CONSOLE_TEST" in output:
                break
            time.sleep(0.05)

        assert "HELLO_FROM_CONSOLE_TEST" in output
        assert "consoletest" in output
        assert "log" in output
    finally:
        sub.stop()
