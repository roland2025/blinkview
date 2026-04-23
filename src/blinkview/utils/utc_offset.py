# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from datetime import datetime


def get_local_utc_offset_seconds() -> int:
    """
    Returns the host machine's current UTC offset in seconds.

    This method automatically accounts for Daylight Saving Time (DST)
    based on the system's timezone settings.

    Example:
        In Tallinn (EEST/UTC+3), this returns 10800.
    """
    # astimezone() with no arguments uses the system local timezone.
    # utcoffset() returns a timedelta representing the difference from UTC.
    offset = datetime.now().astimezone().utcoffset()

    if offset is None:
        return 0

    return int(offset.total_seconds())
