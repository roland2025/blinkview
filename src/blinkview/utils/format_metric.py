# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def format_metric(value):
    """
    Converts an integer or float into a human-readable metric string.
    e.g., 1450000 -> 1.45M, 1425 -> 1.425K
    """
    if value == 0:
        return "0"

    for unit in ["", "K", "M", "B", "T"]:
        if abs(value) < 1000.0:
            # :g handles precision and strips trailing .0 automatically
            return f"{value:.3f}{unit}"
        value /= 1000.0

    return f"{value:.3f}P"
