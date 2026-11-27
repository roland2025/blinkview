# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""AdbReader's `ps` poll feeding pid_history: process start times from ps's ELAPSED column
(so a process's first ~15 s of logs before the poll saw it still resolve to its name), every
PID of a shared process name, and the fallback for a ps without ELAPSED. The ps lines below are
in the format an Android 16 emulator's toybox ps prints for `ps -A -o NAME,PID,ETIME`."""

from types import SimpleNamespace

import pytest

from blinkview.core.id_history import IdHistory
from blinkview.core.logger import PrintLogger
from blinkview.io.adb_reader import AdbReader, parse_ps_elapsed_s

SEC = 1_000_000_000
NOW = 1_000 * SEC
DEVICE_ID = 7


def _reader(outputs):
    """outputs: {command substring: lines} - the first matching entry answers a command."""
    reader = AdbReader()
    reader.logger = PrintLogger("test.adb_reader_process_poll")
    reader.apply_config({})
    reader.commands = []

    def run(cmd):
        reader.commands.append(cmd)
        for needle, lines in outputs.items():
            if needle in cmd:
                return 0, list(lines)
        raise AssertionError(f"unexpected command {cmd!r}")

    reader._run = run
    reader._owning_pipeline_device_ids = lambda: [DEVICE_ID]
    reader.shared = SimpleNamespace(time_ns=lambda: NOW, pid_history=IdHistory())
    return reader


def _resolve(reader, pid, ts):
    return reader.shared.pid_history.resolve(AdbReader._pid_history_key(DEVICE_ID, pid), ts)


class TestParsePsElapsed:
    @pytest.mark.parametrize(
        "text, seconds",
        [
            ("00:00", 0),
            ("00:07", 7),
            ("12:34", 754),
            ("01:02:03", 3723),
            ("1-10:45:48", 125148),
            ("12-00:00:01", 1036801),
        ],
    )
    def test_formats(self, text, seconds):
        assert parse_ps_elapsed_s(text) == seconds

    @pytest.mark.parametrize("text", ["", "abc", "1:2:3:4", "-1:00", "ELAPSED", "5"])
    def test_rejects_garbage(self, text):
        assert parse_ps_elapsed_s(text) is None


class TestElapsedBackdating:
    def test_interval_starts_at_process_start(self):
        reader = _reader(
            {
                "ETIME": [
                    "NAME                          PID     ELAPSED",
                    "com.example.app             1234       00:07",
                ]
            }
        )

        reader._refresh_process_ids()

        # Started 7 s (to ps's 1 s resolution) before the poll: the whole second before counts too.
        assert _resolve(reader, 1234, NOW - 8 * SEC) == "com.example.app"
        assert _resolve(reader, 1234, NOW - 9 * SEC) is None

    def test_long_running_process_covers_the_whole_session(self):
        reader = _reader({"ETIME": ["NAME PID ELAPSED", "init 1 1-10:45:48"]})

        reader._refresh_process_ids()

        assert _resolve(reader, 1, NOW - 125_000 * SEC) == "init"

    def test_unparsable_elapsed_falls_back_to_poll_time(self):
        reader = _reader({"ETIME": ["NAME PID ELAPSED", "weird 5 ??:??"]})

        reader._refresh_process_ids()

        assert _resolve(reader, 5, NOW) == "weird"
        assert _resolve(reader, 5, NOW - 1) is None


class TestSharedProcessNames:
    def test_every_pid_of_a_shared_name_gets_history(self):
        reader = _reader({"ETIME": ["NAME PID ELAPSED", "sh 300 00:01", "sh 301 00:02", "logcat 400 00:03"]})

        reader._refresh_process_ids()

        assert _resolve(reader, 300, NOW) == "sh"
        assert _resolve(reader, 301, NOW) == "sh"
        assert _resolve(reader, 400, NOW) == "logcat"

    def test_vanished_pid_of_a_shared_name_is_closed(self):
        reader = _reader({"ETIME": ["NAME PID ELAPSED", "sh 300 00:01", "sh 301 00:02"]})
        reader._refresh_process_ids()

        reader._run = lambda cmd: (0, ["NAME PID ELAPSED", "sh 300 00:16"])
        reader.shared.time_ns = lambda: NOW + 15 * SEC
        reader._refresh_process_ids()

        assert _resolve(reader, 300, NOW + 15 * SEC) == "sh"
        assert _resolve(reader, 301, NOW + 15 * SEC) is None
        assert _resolve(reader, 301, NOW) == "sh"


class TestPsWithoutElapsed:
    def test_falls_back_to_name_pid_and_stops_asking_for_elapsed(self):
        reader = _reader(
            {
                "ETIME": ["ps: bad -o 'NAME,PID,ETIME'", "                   ^"],
                "NAME,PID": ["NAME PID", "com.example.app 1234"],
            }
        )

        reader._refresh_process_ids()
        reader._refresh_process_ids()

        assert _resolve(reader, 1234, NOW) == "com.example.app"
        assert [("ETIME" in c) for c in reader.commands] == [True, False, False]

    def test_two_column_output_is_used_as_is(self):
        """A ps that ignores the extra column (or a canned two-column answer) still works."""
        reader = _reader({"ETIME": ["NAME PID", "com.example.app 1234"]})

        reader._refresh_process_ids()

        assert _resolve(reader, 1234, NOW) == "com.example.app"
        assert reader._process_ids == {"com.example.app": 1234}
