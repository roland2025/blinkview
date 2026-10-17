# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import threading
from types import SimpleNamespace

import pylink
import pytest

from blinkview.core.logger import PrintLogger
from blinkview.io import rtt
from blinkview.io.rtt import JLinkRTTReader


@pytest.fixture(autouse=True)
def _fresh_reader_jlinks(monkeypatch):
    """open() reuses one JLink per serial number for the life of the process; give each test
    its own, built from that test's monkeypatched pylink.JLink."""
    monkeypatch.setattr(rtt, "_reader_jlinks", {})


def _fast_forward_clock():
    """A shared.time_ns stand-in that jumps straight past _drain_stale_data's 1.5s absolute
    timeout after its first couple of calls, so open()'s real drain call exits after only a
    couple of 10ms sleep() iterations instead of actually waiting out 1.5 real seconds - the
    FakeJLink below reports its RTT buffer as already empty, and open()'s drain step relies
    entirely on the absolute-timeout branch to exit in that case (see
    TestDrainStaleData/the memory note on this)."""
    clock = iter([0, 2_000_000_000])
    return lambda: next(clock, 2_000_000_000)


def make_reader(**config_overrides):
    reader = JLinkRTTReader()
    reader.logger = PrintLogger("test.rtt")
    reader.apply_config(config_overrides)
    return reader


class FakeJLink:
    """Stand-in for pylink.JLink - records every call so tests can assert on the real
    connect/drain/teardown sequence without any actual J-Link hardware attached."""

    def __init__(self, open_raises=None):
        self.calls = []
        self.open_raises = open_raises
        self.tif = None
        self.connected_target = None
        self.connected_speed = None
        self.rtt_started = False
        self.rtt_stopped = False
        self.closed = False
        self._opened = True
        self.rtt_read_queue = []
        self.rtt_write_calls = []
        self.reset_calls = []
        self.halt_result = True
        self.restart_result = True

    def exec_command(self, cmd):
        self.calls.append(("exec_command", cmd))

    def open(self, serial_no=None):
        self.calls.append(("open", serial_no))
        if self.open_raises:
            raise self.open_raises

    def set_tif(self, tif):
        self.tif = tif

    def connect(self, target, speed):
        self.connected_target = target
        self.connected_speed = speed

    def rtt_start(self):
        self.rtt_started = True

    def rtt_stop(self):
        self.rtt_stopped = True

    def rtt_read(self, channel, size):
        if self.rtt_read_queue:
            return self.rtt_read_queue.pop(0)
        return []

    def rtt_write(self, channel, data):
        self.rtt_write_calls.append((channel, data))
        return len(data)

    def close(self):
        self.closed = True

    def opened(self):
        return self._opened

    def reset(self, halt=False):
        self.reset_calls.append(halt)

    def halt(self):
        return self.halt_result

    def restart(self):
        return self.restart_result


@pytest.fixture
def fake_jlink_class(monkeypatch):
    """Monkeypatches pylink.JLink so open() constructs our FakeJLink instead of touching real
    hardware. Yields a mutable holder so tests can configure open_raises before open() runs, and
    inspect the constructed instance afterward."""
    holder = SimpleNamespace(instance=None, open_raises=None)

    def factory():
        holder.instance = FakeJLink(open_raises=holder.open_raises)
        return holder.instance

    monkeypatch.setattr(pylink, "JLink", factory)
    return holder


@pytest.fixture
def probe_held_by_another_thread(monkeypatch):
    """A reader whose _jlink_lock is held by another thread for the whole test, like a reader
    thread inside a J-Link DLL call that does not return."""
    monkeypatch.setattr(rtt, "JLINK_LOCK_TIMEOUT_S", 0.05)
    reader = make_reader()
    reader.jlink = FakeJLink()
    held, release = threading.Event(), threading.Event()

    def hold():
        with reader._jlink_lock:
            held.set()
            release.wait()

    thread = threading.Thread(target=hold, daemon=True)
    thread.start()
    held.wait()
    yield reader
    release.set()
    thread.join()


class TestApplyConfig:
    def test_creates_a_send_child_logger(self):
        reader = make_reader()
        assert reader.logger_send is not None

    def test_does_not_recreate_the_send_logger_on_a_second_apply(self):
        reader = make_reader()
        first = reader.logger_send
        reader.apply_config({})
        assert reader.logger_send is first


class TestReaderJlinkReuse:
    """A JLink dropped after a failed open left the DLL holding its freed OpenEx callbacks,
    and the next JLink() faulted (access violation) on every retry - so a reader never drops
    the JLink it opens its probe with."""

    def test_retries_after_a_failed_open_reuse_the_same_jlink(self, fake_jlink_class):
        reader = make_reader(serial_number="50110259")
        fake_jlink_class.open_raises = pylink.errors.JLinkException("Cannot connect to J-Link")

        assert reader.open() is None
        first = fake_jlink_class.instance
        assert reader.open() is None

        assert fake_jlink_class.instance is first
        assert first.calls.count(("open", 50110259)) == 2

    def test_reconnect_after_cleanup_reuses_the_same_jlink(self, fake_jlink_class):
        reader = make_reader(serial_number="")
        reader.shared = SimpleNamespace(time_ns=_fast_forward_clock())
        reader.jlink = reader.open()
        first = reader.jlink

        reader.cleanup_jlink()
        reader.shared = SimpleNamespace(time_ns=_fast_forward_clock())

        assert reader.open() is first
        assert fake_jlink_class.instance is first

    def test_another_reader_on_the_same_probe_shares_it(self, fake_jlink_class):
        fake_jlink_class.open_raises = pylink.errors.JLinkException("Cannot connect to J-Link")
        make_reader(serial_number="111").open()
        first = fake_jlink_class.instance

        make_reader(serial_number="111").open()

        assert fake_jlink_class.instance is first

    def test_each_probe_gets_its_own_jlink(self, fake_jlink_class):
        fake_jlink_class.open_raises = pylink.errors.JLinkException("Cannot connect to J-Link")
        make_reader(serial_number="111").open()
        first = fake_jlink_class.instance

        make_reader(serial_number="222").open()

        assert fake_jlink_class.instance is not first


class TestOpen:
    def test_happy_path_connects_and_drains_then_returns_the_jlink(self, fake_jlink_class):
        reader = make_reader(serial_number="", interface="swd", target_device="NRF52840_XXAA", speed=4000)
        reader.shared = SimpleNamespace(time_ns=_fast_forward_clock())

        result = reader.open()

        jl = fake_jlink_class.instance
        assert result is jl
        assert ("exec_command", "SuppressGUI = 1") in jl.calls
        assert ("open", None) in jl.calls  # no serial_number configured
        assert jl.connected_target == "NRF52840_XXAA"
        assert jl.connected_speed == 4000
        assert jl.rtt_started is True

    def test_uses_the_configured_serial_number(self, fake_jlink_class):
        reader = make_reader(serial_number="123456", interface="swd")
        reader.shared = SimpleNamespace(time_ns=_fast_forward_clock())

        reader.open()

        assert ("open", 123456) in fake_jlink_class.instance.calls

    def test_jtag_interface_selects_the_jtag_tif(self, fake_jlink_class):
        reader = make_reader(interface="jtag")
        reader.shared = SimpleNamespace(time_ns=_fast_forward_clock())

        reader.open()

        assert fake_jlink_class.instance.tif == pylink.enums.JLinkInterfaces.JTAG

    def test_connection_failure_returns_none_and_closes_the_partial_jlink(self, fake_jlink_class):
        fake_jlink_class.open_raises = ConnectionError("no device found")
        reader = make_reader()
        reader.shared = SimpleNamespace(time_ns=lambda: 0)

        result = reader.open()

        assert result is None
        assert fake_jlink_class.instance.closed is True


class TestCleanupJlink:
    def test_noop_when_no_jlink(self):
        reader = make_reader()
        reader.jlink = None
        reader.cleanup_jlink()  # must not raise
        assert reader.jlink is None

    def test_stops_rtt_and_closes_then_clears_the_reference(self):
        reader = make_reader()
        jl = FakeJLink()
        reader.jlink = jl

        reader.cleanup_jlink()

        assert jl.rtt_stopped is True
        assert jl.closed is True
        assert reader.jlink is None

    def test_swallows_exceptions_from_rtt_stop_and_still_closes(self):
        reader = make_reader()

        class BadStop(FakeJLink):
            def rtt_stop(self):
                raise RuntimeError("already gone")

        jl = BadStop()
        reader.jlink = jl

        reader.cleanup_jlink()  # must not raise

        assert jl.closed is True
        assert reader.jlink is None

    def test_swallows_exceptions_from_close(self):
        reader = make_reader()

        class BadClose(FakeJLink):
            def close(self):
                raise RuntimeError("already closed")

        jl = BadClose()
        reader.jlink = jl

        reader.cleanup_jlink()  # must not raise
        assert reader.jlink is None


class TestDrainStaleData:
    def test_stops_once_the_buffer_goes_dry(self):
        reader = make_reader(target_rtt_buffer_size=8192)
        reader.shared = SimpleNamespace(time_ns=lambda: 0)
        jl = FakeJLink()
        jl.rtt_read_queue = [b"abc", b"def", []]

        reader._drain_stale_data(jl)  # returns once rtt_read yields nothing after seeing data

    def test_stops_once_the_configured_horizon_is_reached(self):
        reader = make_reader(target_rtt_buffer_size=4)
        reader.shared = SimpleNamespace(time_ns=lambda: 0)
        jl = FakeJLink()
        # Each chunk is 3 bytes; horizon of 4 is reached after the second chunk (6 >= 4), well
        # before the queue would otherwise run dry.
        jl.rtt_read_queue = [b"abc", b"def", b"ghi", b"jkl"]

        reader._drain_stale_data(jl)

        assert len(jl.rtt_read_queue) > 0  # stopped early, didn't drain the whole queue

    def test_stops_after_the_absolute_timeout_even_if_data_keeps_arriving(self):
        reader = make_reader()
        # Simulate time jumping straight past the 1.5s absolute timeout on the very first check.
        clock = iter([0, 2_000_000_000, 2_000_000_000])
        reader.shared = SimpleNamespace(time_ns=lambda: next(clock, 2_000_000_000))
        jl = FakeJLink()
        jl.rtt_read_queue = [b"abc"] * 1000  # would never run dry on its own

        reader._drain_stale_data(jl)  # must return promptly rather than looping forever


class TestSendData:
    def test_returns_zero_without_an_active_jlink(self):
        reader = make_reader()
        reader.jlink = None
        assert reader.send_data("hello") == 0

    def test_returns_zero_when_jlink_is_not_opened(self):
        reader = make_reader()
        jl = FakeJLink()
        jl._opened = False
        reader.jlink = jl
        assert reader.send_data("hello") == 0

    def test_writes_encoded_bytes_and_returns_the_written_count(self):
        reader = make_reader()
        jl = FakeJLink()
        reader.jlink = jl

        written = reader.send_data("hello", channel=2)

        assert written == len(b"hello")
        assert jl.rtt_write_calls == [(2, b"hello")]

    def test_write_failure_is_caught_and_returns_zero(self):
        reader = make_reader()

        class BadWrite(FakeJLink):
            def rtt_write(self, channel, data):
                raise RuntimeError("link down")

        jl = BadWrite()
        reader.jlink = jl

        assert reader.send_data("hello") == 0

    def test_gives_up_when_the_reader_thread_never_lets_go_of_the_probe(self, probe_held_by_another_thread):
        reader = probe_held_by_another_thread

        assert reader.send_data("hello") == 0  # must return instead of waiting forever

        assert reader.jlink.rtt_write_calls == []


class TestSendCommand:
    def test_dropped_when_the_reader_thread_never_lets_go_of_the_probe(self, probe_held_by_another_thread):
        reader = probe_held_by_another_thread

        reader.send_command("reset")  # must return instead of waiting forever

        assert reader.jlink.reset_calls == []

    def test_dropped_without_an_active_jlink(self):
        reader = make_reader()
        reader.jlink = None
        reader.send_command("halt")  # must not raise

    def test_rtt_restart_closes_the_probe_so_the_reader_reconnects(self):
        reader = make_reader()
        jl = FakeJLink()
        reader.jlink = jl

        reader.send_command("rtt_restart")

        assert jl.rtt_stopped is True
        assert jl.closed is True
        assert reader.jlink is None

    def test_reset_calls_jlink_reset_with_halt_false(self):
        reader = make_reader()
        jl = FakeJLink()
        reader.jlink = jl

        reader.send_command("reset")

        assert jl.reset_calls == [False]

    def test_reset_failure_is_caught(self):
        reader = make_reader()

        class BadReset(FakeJLink):
            def reset(self, halt=False):
                raise RuntimeError("nope")

        jl = BadReset()
        reader.jlink = jl

        reader.send_command("reset")  # must not raise

    def test_halt_success_and_failure_do_not_raise(self):
        reader = make_reader()
        jl = FakeJLink()
        jl.halt_result = False
        reader.jlink = jl

        reader.send_command("halt")  # must not raise regardless of result

    def test_restart_success_and_failure_do_not_raise(self):
        reader = make_reader()
        jl = FakeJLink()
        jl.restart_result = False
        reader.jlink = jl

        reader.send_command("restart")  # must not raise regardless of result

    def test_unknown_command_falls_back_to_raw_send_data(self):
        reader = make_reader()
        jl = FakeJLink()
        reader.jlink = jl

        reader.send_command("some raw string")

        assert jl.rtt_write_calls == [(0, b"some raw string")]


class TestGetCommands:
    def test_returns_the_expected_static_command_list(self):
        reader = make_reader()
        commands = reader.get_commands()
        tokens = [token for token, _label in commands]
        assert tokens == ["reset", "halt", "restart", "rtt_restart"]


class TestGetConfigSchema:
    @pytest.fixture(autouse=True)
    def _fresh_enumerator(self, monkeypatch):
        # The enumerator JLink is cached for the life of the process; drop it so each
        # test constructs one from its own monkeypatched pylink.JLink.
        monkeypatch.setattr("blinkview.io.rtt._enumerator_jlink", None)

    def test_reuses_one_jlink_instance_across_schema_fetches(self, monkeypatch):
        created = []

        class CountingJLink:
            def __init__(self):
                created.append(self)

            def connected_emulators(self):
                return []

        monkeypatch.setattr(pylink, "JLink", CountingJLink)

        JLinkRTTReader.get_config_schema()
        JLinkRTTReader.get_config_schema()

        assert len(created) == 1

    def test_populates_serial_number_enum_from_connected_emulators(self, monkeypatch):
        class FakeEmu:
            def __init__(self, sn):
                self.SerialNumber = sn

        class FakeJLinkForSchema:
            def connected_emulators(self):
                return [FakeEmu(111), FakeEmu(222)]

        monkeypatch.setattr(pylink, "JLink", FakeJLinkForSchema)

        schema = JLinkRTTReader.get_config_schema()

        sn_prop = schema["properties"]["serial_number"]
        assert sn_prop["enum"] == ["", "111", "222"]
        assert sn_prop["_allow_custom"] is True

    def test_gracefully_falls_back_when_pylink_raises(self, monkeypatch):
        class BrokenJLink:
            def __init__(self):
                raise RuntimeError("no DLL found")

        monkeypatch.setattr(pylink, "JLink", BrokenJLink)

        schema = JLinkRTTReader.get_config_schema()  # must not raise

        assert "serial_number" in schema["properties"]
