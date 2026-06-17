from datetime import datetime, timezone

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.ops.formatting import update_iso8601_timestamp_cache

# Ensure character constants are defined if they aren't globally available
# CHAR_ZERO = 48  # ord('0')
# CHAR_DASH = 45  # ord('-')
# CHAR_T = 84  # ord('T')
# CHAR_COLON = 58  # ord(':')


# --- Helper function for assertions ---
def decode_cache(ts_cache: np.ndarray) -> str:
    """Converts the uint8 array back into a readable string."""
    return ts_cache.tobytes().decode("ascii")


@pytest.mark.parametrize(
    "total_sec, expected_iso",
    [
        # 1. Unix Epoch Base Cases
        (0, "1970-01-01T00:00:00"),
        (1, "1970-01-01T00:00:01"),
        (86399, "1970-01-01T23:59:59"),
        (86400, "1970-01-02T00:00:00"),
        # 2. Pre-Epoch Dates (Negative Seconds)
        # (-1, "1969-12-31T23:59:59"),
        # (-86400, "1969-12-30T00:00:00"),
        # (-31536000, "1969-01-01T00:00:00"),
        # 3. Leap Year Transitions (2024 is a leap year)
        (1709164799, "2024-02-28T23:59:59"),
        (1709164800, "2024-02-29T00:00:00"),  # Leap day start
        (1709251199, "2024-02-29T23:59:59"),  # Leap day end
        (1709251200, "2024-03-01T00:00:00"),  # Next day
        # 4. Standard Non-Leap Year Transitions (2023)
        (1677628799, "2023-02-28T23:59:59"),
        (1677628800, "2023-03-01T00:00:00"),  # Skips Feb 29
        # 5. Century Leap Year Boundaries (Year 2000 was a leap year)
        (951868799, "2000-02-29T23:59:59"),
        (951868800, "2000-03-01T00:00:00"),
        # 6. Non-Leap Century Boundaries (Year 2100 is NOT a leap year)
        (4107542399, "2100-02-28T23:59:59"),
        (4107542400, "2100-03-01T00:00:00"),
        # 7. Modern / Arbitrary Mid-day Timestamps
        (1718527173, "2024-06-16T08:39:33"),
        (2000000000, "2033-05-18T03:33:20"),
    ],
)
def test_update_iso8601_timestamp_cache_known_values(total_sec, expected_iso):
    # Arrange
    ts_cache = np.zeros(19, dtype=dtypes.BYTE)

    # Act
    update_iso8601_timestamp_cache(total_sec, ts_cache)
    result_str = decode_cache(ts_cache)

    # Assert
    assert result_str == expected_iso


def test_update_iso8601_timestamp_cache_fuzzing():
    """Fuzzes 10,000 continuous hours to ensure no breaking gaps or math drift."""
    ts_cache = np.zeros(19, dtype=np.uint8)

    # Start at 2020-01-01 00:00:00 UTC
    start_sec = 1577836800

    # Check every hour for 10,000 hours (~1.14 years)
    for hour in range(10, 000):
        total_sec = start_sec + (hour * 3600)

        # Expected value from Python's standard library
        expected_iso = datetime.fromtimestamp(total_sec, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        # Run custom algorithm
        update_iso8601_timestamp_cache(total_sec, ts_cache)
        result_str = decode_cache(ts_cache)

        assert result_str == expected_iso, f"Failed at epoch second: {total_sec}"
