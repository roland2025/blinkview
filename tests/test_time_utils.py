# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from unittest.mock import patch

from blinkview.utils.time_utils import ConsoleTimestampFormatter, ISO8601TimestampFormatter, TimeUtils


class TestTimeUtils:
    def test_now_ns_tracks_the_wall_clock_anchor_at_construction(self):
        with (
            patch("blinkview.utils.time_utils.time_ns", return_value=1_000_000_000),
            patch("blinkview.utils.time_utils.perf_counter_ns", return_value=500),
        ):
            tu = TimeUtils()
            assert tu.now_ns() == 1_000_000_000

    def test_now_ns_advances_with_perf_counter_delta(self):
        # bake()/resync() capture the module-level functions themselves (the mock object), not
        # a snapshot of their return_value - so the same patch context must span construction
        # and the later call for a mutated return_value to be observed.
        with (
            patch("blinkview.utils.time_utils.time_ns", return_value=1_000_000_000),
            patch("blinkview.utils.time_utils.perf_counter_ns", return_value=500) as mock_perf,
        ):
            tu = TimeUtils()
            mock_perf.return_value = 1500  # perf advanced by 1000ns since the anchor was captured
            assert tu.now_ns() == 1_000_001_000

    def test_now_returns_seconds_as_a_float(self):
        with (
            patch("blinkview.utils.time_utils.time_ns", return_value=2_000_000_000),
            patch("blinkview.utils.time_utils.perf_counter_ns", return_value=0),
        ):
            tu = TimeUtils()
            assert tu.now() == 2.0

    def test_resync_updates_anchors_in_place(self):
        with (
            patch("blinkview.utils.time_utils.time_ns") as mock_time,
            patch("blinkview.utils.time_utils.perf_counter_ns") as mock_perf,
        ):
            mock_time.return_value = 1_000_000_000
            mock_perf.return_value = 0
            tu = TimeUtils()

            mock_time.return_value = 5_000_000_000
            mock_perf.return_value = 100
            tu.resync()

            assert tu.now_ns() == 5_000_000_000

    def test_bake_installs_now_and_now_ns_as_bound_callables(self):
        tu = TimeUtils()
        assert callable(tu.now)
        assert callable(tu.now_ns)


class TestConsoleTimestampFormatter:
    def test_formats_hh_mm_ss_and_milliseconds(self):
        with (
            patch("blinkview.utils.time_utils.localtime"),
            patch("blinkview.utils.time_utils.strftime", return_value="14:02:01"),
        ):
            fmt = ConsoleTimestampFormatter()
            result = fmt.format(123_456_789)  # 0s + 123_456_789ns -> ms=123

        assert result == "14:02:01.123"

    def test_reformats_the_hh_mm_ss_part_only_once_per_second(self):
        with (
            patch("blinkview.utils.time_utils.localtime"),
            patch("blinkview.utils.time_utils.strftime", return_value="14:02:01") as mock_strftime,
        ):
            fmt = ConsoleTimestampFormatter()
            fmt.format(5_000_000_000)
            fmt.format(5_500_000_000)  # same whole-second, different ms

            assert mock_strftime.call_count == 1

    def test_recomputes_the_hh_mm_ss_part_when_the_second_changes(self):
        with (
            patch("blinkview.utils.time_utils.localtime"),
            patch("blinkview.utils.time_utils.strftime", return_value="14:02:01") as mock_strftime,
        ):
            fmt = ConsoleTimestampFormatter()
            fmt.format(5_000_000_000)
            fmt.format(6_000_000_000)

            assert mock_strftime.call_count == 2


class TestISO8601TimestampFormatter:
    def test_formats_full_iso8601_with_microseconds_and_zulu_suffix(self):
        with (
            patch("blinkview.utils.time_utils.localtime"),
            patch("blinkview.utils.time_utils.strftime", side_effect=["2026-02-21T", "14:02:01"]),
        ):
            fmt = ISO8601TimestampFormatter()
            result = fmt.format(123_456_789)

        assert result == "2026-02-21T14:02:01.123456Z"

    def test_date_is_only_recomputed_once_per_day(self):
        # First call at day 0, second call still within the same 86400s day.
        with (
            patch("blinkview.utils.time_utils.localtime"),
            patch(
                "blinkview.utils.time_utils.strftime",
                side_effect=["2026-02-21T", "00:00:00", "00:00:01"],
            ) as mock_strftime,
        ):
            fmt = ISO8601TimestampFormatter()
            fmt.format(0)
            fmt.format(1_000_000_000)

        # date format called once, time format called twice (once per distinct second)
        assert mock_strftime.call_count == 3

    def test_date_is_recomputed_when_the_day_rolls_over(self):
        one_day_ns = 86_400 * 1_000_000_000
        with (
            patch("blinkview.utils.time_utils.localtime"),
            patch(
                "blinkview.utils.time_utils.strftime",
                side_effect=["2026-02-21T", "00:00:00", "2026-02-22T", "00:00:00"],
            ) as mock_strftime,
        ):
            fmt = ISO8601TimestampFormatter()
            fmt.format(0)
            fmt.format(one_day_ns)

        assert mock_strftime.call_count == 4
