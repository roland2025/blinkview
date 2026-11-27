# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.id_registry import IDRegistry
from blinkview.core.limits import TUNER_MAX_BUFFER_BYTES, TUNER_MAX_CAPACITY
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.frame_decoders import FrameDecoderFactory
from blinkview.parsers.frame_parsers import FrameParserFactory, FrameSectionParserFactory
from blinkview.parsers.frame_pipeline_parser import (
    AdbLogcatLongParser,
    FramePipelineParser,
    RsyslogFileFormatParser,
)
from blinkview.parsers.state import MAX_FRAMES_PER_CALL
from blinkview.utils.log_level import LogLevel


def make_shared(id_registry):
    registry = FactoryRegistry()
    registry.register("frame_decoder", FrameDecoderFactory)
    registry.register("frame_parser", FrameParserFactory)
    registry.register("frame_section_parser", FrameSectionParserFactory)
    return SimpleNamespace(
        array_pool=NumpyArrayPool(),
        time_ns=time.time_ns,
        factories=registry,
        id_registry=id_registry,
    )


def make_parser(id_registry, device_name="frame_pipeline_parser_test", **config_overrides):
    parser = FramePipelineParser()
    parser.logger = PrintLogger("test.frame_pipeline_parser")
    parser.shared = make_shared(id_registry)
    parser.local = SimpleNamespace(device_id=id_registry.get_device(device_name))
    config = {
        "frame_decoder": {"type": "line_decoder"},
        "frame_parser": {"type": "default", "steps": []},
        "delay": 20,
    }
    config.update(config_overrides)
    parser.apply_config(config)
    return parser


# a line decoder that starts synced, so the first frame is delivered (no resync discard)
SYNCED_LINE_DECODER = {"type": "line_decoder", "frame_resync_on_start": False}


class QueueParser:
    def __init__(self):
        self.queue: "queue.Queue[bytes]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, _rx_ts, level, module, *_rest in batch:
                self.queue.put((bytes(msg), level, module))


def drain(q, count, timeout=5.0):
    items = []
    deadline = time.time() + timeout
    while len(items) < count and time.time() < deadline:
        try:
            items.append(q.get(timeout=max(0.0, deadline - time.time())))
        except queue.Empty:
            break
    return items


def make_preset_parser(preset_cls, id_registry, device_name, **config_overrides):
    parser = preset_cls()
    parser.logger = PrintLogger("test." + device_name)
    parser.shared = make_shared(id_registry)
    parser.local = SimpleNamespace(device_id=id_registry.get_device(device_name))
    config = {"delay": 20}
    config.update(config_overrides)
    hydrated = parser.hydrate_config(config)
    # set on the hydrated preset: a partial frame_decoder override would replace the preset's decoder settings
    hydrated["frame_decoder"]["frame_resync_on_start"] = False
    parser.apply_config(hydrated)
    return parser


class TestApplyConfig:
    def test_builds_frame_codec_and_frame_parser_via_the_real_factories(self, id_registry):
        parser = make_parser(id_registry)

        assert parser._frame_codec is not None
        assert parser._frame_parser is not None

    def test_sync_state_is_created_once_and_kept_across_reapply(self, id_registry):
        parser = make_parser(id_registry)
        first_sync_state = parser.sync_state

        parser.apply_config(
            {"frame_decoder": {"type": "line_decoder"}, "frame_parser": {"type": "default", "steps": []}}
        )

        assert parser.sync_state is first_sync_state

    def test_apply_config_marks_thread_needs_restart(self, id_registry):
        parser = make_parser(id_registry)
        assert parser.thread_needs_restart is True


class TestNameChanged:
    def test_updates_the_device_identity_name(self, id_registry):
        parser = make_parser(id_registry)
        device = parser.local.device_id

        parser.apply_config(
            {
                "frame_decoder": {"type": "line_decoder"},
                "frame_parser": {"type": "default", "steps": []},
                "name": "renamed-device",
            }
        )

        assert device.name == "renamed-device"

    def test_uppercase_name_keeps_discovery_log_dumpable(self, id_registry):
        """Regression: PipelineManager creates the device via get_device("RTT") (stored under
        "rtt"), then applying the config's name set device.name = "RTT" directly. Modules
        discovered afterwards were logged under "RTT", and Registry.stop()'s
        dump_discovery_log() raised KeyError: 'RTT', leaving "Compressing files..." hung."""
        parser = make_parser(id_registry, device_name="RTT", name="RTT")
        device = parser.local.device_id
        module = device.get_module("nrf_ble_gatt")

        assert device.name == "RTT"
        assert id_registry.get_device("RTT") is device
        # exported/rendered names (Numba string table) stay lowercase - only .name carries case
        assert id_registry.devices_table.get_string(device.id) == "rtt"
        dumped = id_registry.dump_discovery_log()

        replayed = IDRegistry(NumpyArrayPool())
        replayed.replay_discovery_log(dumped)
        assert replayed.get_device("RTT").id == device.id
        assert replayed.get_device("RTT").get_module("nrf_ble_gatt").id == module.id

    def test_renamed_device_stays_reachable_and_dumpable(self, id_registry):
        parser = make_parser(id_registry, device_name="old_name")
        device = parser.local.device_id
        device.get_module("before")

        parser.apply_config({"frame_decoder": {"type": "line_decoder"}, "name": "New_Name"})
        device.get_module("after")

        assert id_registry.get_device("new_name") is device
        assert id_registry.get_device("old_name") is device
        assert [e[1] for e in id_registry.dump_discovery_log()] == ["old_name"] * 4


class TestRunRealIngestion:
    """Runs FramePipelineParser.run() for real: real line_decoder framing (nb_decode_loop), real (empty) parser
    pipeline, real nb_finish_frames. Apart from the test of the default, the decoder runs with
    frame_resync_on_start=False so the first frame is delivered too."""

    def _run_lines(self, id_registry, lines, **decoder_config):
        parser = make_parser(id_registry, delay=20, frame_decoder={"type": "line_decoder", **decoder_config})
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = parser.shared.array_pool.create(PooledLogBatch, 8, 256)
        for line in lines:
            batch.insert(1000, 1000, line)
        parser.put(batch)

        parser.start()
        try:
            rows = drain(subscriber.queue, count=len(lines), timeout=2.0)
        finally:
            parser.stop()
        return [msg for msg, *_r in rows]

    def test_resync_on_start_discards_the_first_frame_by_default(self, id_registry):
        assert self._run_lines(id_registry, [b"partial\n", b"line2\n", b"line3\n"]) == [b"line2", b"line3"]

    def test_disabling_resync_on_start_delivers_the_first_frame(self, id_registry):
        rows = self._run_lines(id_registry, [b"line1\n", b"line2\n", b"line3\n"], frame_resync_on_start=False)

        assert rows == [b"line1", b"line2", b"line3"]

    def test_decoded_lines_are_distributed_with_default_level_and_module(self, id_registry):
        parser = make_parser(id_registry, delay=20, frame_decoder=SYNCED_LINE_DECODER)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = parser.shared.array_pool.create(PooledLogBatch, 8, 256)
        batch.insert(1000, 1000, b"hello world\n")
        batch.insert(1000, 1000, b"second line\n")
        parser.put(batch)

        parser.start()
        try:
            rows = drain(subscriber.queue, count=2)
        finally:
            parser.stop()

        assert [msg for msg, _level, _module in rows] == [b"hello world", b"second line"]
        for _msg, level, module in rows:
            assert level == LogLevel.INFO.value
            assert module == device.get_module("log").id

    def test_multiple_batches_all_get_delivered(self, id_registry):
        parser = make_parser(id_registry, delay=20, frame_decoder=SYNCED_LINE_DECODER)
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        pool = parser.shared.array_pool
        first_batch = pool.create(PooledLogBatch, 8, 256)
        first_batch.insert(1000, 1000, b"first batch line\n")
        parser.put(first_batch)

        parser.start()
        try:
            first_rows = drain(subscriber.queue, count=1)

            second_batch = pool.create(PooledLogBatch, 8, 256)
            second_batch.insert(1000, 1000, b"second batch line\n")
            parser.put(second_batch)

            second_rows = drain(subscriber.queue, count=1)
        finally:
            parser.stop()

        assert [msg for msg, *_r in first_rows] == [b"first batch line"]
        assert [msg for msg, *_r in second_rows] == [b"second batch line"]

    def test_a_restart_keeps_the_stream_state(self, id_registry):
        """A config edit restarts the parser thread. It used to rebuild the decoder state in
        resync mode and drop the first frame after every restart (wiki finding
        py-every-parser-thread-restart-drops-the-first-frame-it-sees-ru): here the frame split by
        the restart and, with it, the whole line after it."""
        parser = make_parser(id_registry, delay=20)  # resync on start, as on a UART
        parser.enabled = True
        subscriber = QueueParser()
        parser.subscribe(subscriber)
        pool = parser.shared.array_pool

        def put(*chunks):
            batch = pool.create(PooledLogBatch, 8, 256)
            for chunk in chunks:
                batch.insert(1000, 1000, chunk)
            parser.put(batch)

        put(b"joined mid-line\n", b"line1\n", b"split ")
        parser.start()
        try:
            before = drain(subscriber.queue, count=1)
            parser.restart()
            put(b"across the restart\n", b"line3\n")
            after = drain(subscriber.queue, count=2)
            parser.restart()
            put(b"line4\n")
            last = drain(subscriber.queue, count=1)
        finally:
            parser.stop()

        assert [msg for msg, *_r in before + after + last] == [
            b"line1",
            b"split across the restart",
            b"line3",
            b"line4",
        ]

    def test_a_new_frame_decoder_config_starts_over_with_a_resync(self, id_registry):
        parser = make_parser(id_registry, delay=20)
        parser.enabled = True
        subscriber = QueueParser()
        parser.subscribe(subscriber)
        pool = parser.shared.array_pool

        parser.start()
        try:
            parser.apply_config({"frame_decoder": {"type": "line_decoder", "filter_trim_r": False}})
            parser.restart()
            batch = pool.create(PooledLogBatch, 8, 256)
            batch.insert(1000, 1000, b"dropped by the resync\n")
            batch.insert(1000, 1000, b"kept\n")
            parser.put(batch)
            rows = drain(subscriber.queue, count=2, timeout=1.0)
        finally:
            parser.stop()

        assert [msg for msg, *_r in rows] == [b"kept"]

    def test_stop_releases_the_stream_state_and_a_new_start_resyncs(self, id_registry):
        parser = make_parser(id_registry, delay=20)
        parser.enabled = True
        subscriber = QueueParser()
        parser.subscribe(subscriber)
        pool = parser.shared.array_pool

        parser.start()
        parser.stop()
        assert parser._frame_state is None

        batch = pool.create(PooledLogBatch, 8, 256)
        batch.insert(1000, 1000, b"dropped by the resync\n")
        batch.insert(1000, 1000, b"kept\n")
        parser.put(batch)
        parser.start()
        try:
            rows = drain(subscriber.queue, count=2, timeout=1.0)
        finally:
            parser.stop()

        assert [msg for msg, *_r in rows] == [b"kept"]

    def test_a_big_input_batch_does_not_make_oversized_output_batches(self, id_registry):
        """The finding's case: a 128 MB input batch made ~292 MB output batches (rows derived from
        bytes, buffer at 1.5x the input), of which ~0.2% was used. A 20 MB input batch, above the
        tuner's cap, must come out in batches within the cap, with no line lost."""
        parser = make_parser(id_registry, delay=20, frame_decoder=SYNCED_LINE_DECODER)
        parser.enabled = True

        out_batches = queue.Queue()

        class BatchShapes:
            def put(self, batch):
                out_batches.put((batch.size, batch.capacity, batch.buffer_capacity()))

        parser.subscribe(BatchShapes())

        line = b"x" * 999 + b"\n"
        line_count = (20 * 1024 * 1024) // len(line)
        batch = parser.shared.array_pool.create(PooledLogBatch, line_count, line_count * len(line))
        for _ in range(line_count):
            batch.insert(1000, 1000, line)
        parser.put(batch)

        shapes = []
        parser.start()
        try:
            deadline = time.time() + 30.0
            while sum(size for size, *_r in shapes) < line_count and time.time() < deadline:
                try:
                    shapes.append(out_batches.get(timeout=0.5))
                except queue.Empty:
                    pass
        finally:
            parser.stop()

        # Before the fix: a 32 MB buffer slab (1.5x the input, rounded up) and ~1 M rows.
        assert sum(size for size, *_r in shapes) == line_count
        for _size, row_capacity, buffer_capacity in shapes:
            assert buffer_capacity <= TUNER_MAX_BUFFER_BYTES
            assert row_capacity <= TUNER_MAX_CAPACITY


class TestRsyslogFileFormatParser:
    """Runs the real 'rsyslog_file_format' preset end to end: RFC3339 timestamp, hostname,
    'tag[pid]: ' TAG field, matching rsyslog's default RSYSLOG_FileFormat template."""

    def test_parses_hostname_tag_pid_and_message(self, id_registry):
        parser = make_preset_parser(RsyslogFileFormatParser, id_registry, "rsyslog_parser_test", delay=20)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = parser.shared.array_pool.create(PooledLogBatch, 8, 256)
        batch.insert(
            1000,
            1000,
            b"2026-09-20T10:23:01.456789+00:00 myhost sshd[1234]: connection closed\n",
        )
        batch.insert(1000, 1000, b"2026-09-20T10:23:02.000000+00:00 myhost kernel: something happened\n")
        parser.put(batch)

        parser.start()
        try:
            rows = drain(subscriber.queue, count=2)
        finally:
            parser.stop()

        assert [msg for msg, _level, _module in rows] == [b"connection closed", b"something happened"]

        _msg0, _level0, module0 = rows[0]
        _msg1, _level1, module1 = rows[1]
        assert module0 == device.get_module("sshd").id
        assert module1 == device.get_module("kernel").id


class TestAdbLogcatLongParser:
    """Runs the real 'adb_logcat_long' preset end to end on `adb logcat -v long` frames."""

    def test_parses_level_tag_and_message(self, id_registry):
        parser = make_preset_parser(AdbLogcatLongParser, id_registry, "adb_logcat_long_test", delay=20)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = parser.shared.array_pool.create(PooledLogBatch, 8, 512)
        batch.insert(1000, 1000, b"[ 1700000000.123  345: 400 I/ActivityManager ]\nstart proc\n\n")
        batch.insert(1000, 1000, b"[ 1700000000.456  57: 57 E/Wifi ]\nlink down\n\n")
        # a frame ends where the next header starts
        batch.insert(1000, 1000, b"[ 1700000000.789  1: 1 D/Init ]\n")
        parser.put(batch)

        parser.start()
        try:
            rows = drain(subscriber.queue, count=2)
        finally:
            parser.stop()

        assert [msg for msg, _level, _module in rows] == [b"start proc", b"link down"]
        assert [level for _msg, level, _module in rows] == [LogLevel.INFO, LogLevel.ERROR]
        assert [module for _msg, _level, module in rows] == [
            device.get_module("ActivityManager").id,
            device.get_module("Wifi").id,
        ]


class TestOutputBatchFill:
    def test_output_batch_keeps_filling_after_the_frame_trackers_run_out(self, id_registry, monkeypatch):
        """The decoder reports "full" after MAX_FRAMES_PER_CALL frames even when the output batch
        has room; the parser used to hand that batch on, so every batch ended at ~8 K rows whatever
        its capacity (measured: ~7 K rows in 16 MB batches, which made the reorder queue's byte
        bound trip at a third of its row bound). With room for 4 x MAX_FRAMES_PER_CALL rows,
        2.5 x MAX_FRAMES_PER_CALL lines must come out as one batch, not as three."""
        import blinkview.parsers.frame_pipeline_parser as fpp

        line_count = 2 * MAX_FRAMES_PER_CALL + MAX_FRAMES_PER_CALL // 2

        class RoomyTuner(fpp.ThroughputAutoTuner):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.estimated_capacity = 4 * MAX_FRAMES_PER_CALL
                self.estimated_buffer_bytes = 4 * MAX_FRAMES_PER_CALL * 16

        monkeypatch.setattr(fpp, "ThroughputAutoTuner", RoomyTuner)

        parser = make_parser(id_registry, delay=200, frame_decoder=SYNCED_LINE_DECODER)
        parser.enabled = True

        out_batches = queue.Queue()

        class BatchSizes:
            def put(self, batch):
                out_batches.put(batch.size)

        parser.subscribe(BatchSizes())

        batch = parser.shared.array_pool.create(PooledLogBatch, line_count, line_count * 20)
        for i in range(line_count):
            batch.insert(1000, 1000, b"line %08d\n" % i)
        parser.put(batch)

        sizes = []
        parser.start()
        try:
            deadline = time.time() + 10.0
            while sum(sizes) < line_count and time.time() < deadline:
                try:
                    sizes.append(out_batches.get(timeout=0.5))
                except queue.Empty:
                    pass
        finally:
            parser.stop()

        assert sizes == [line_count]
