# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


def format_metric(value):
    """
    Converts an integer or float into a human-readable metric string.
    0-999 shows as an integer; larger values use K, M, B, etc.
    """
    if value == 0:
        return "0"

    # Use absolute value for the threshold check to handle negatives
    abs_val = abs(value)

    for unit in ["", "K", "M", "B", "T"]:
        if abs_val < 1000.0:
            if unit == "":
                # Range 0...999: format as an integer
                return f"{value:.0f}"

            # Larger ranges: use :g to show up to 3 sig figs and strip trailing .0
            return f"{value:.3f}{unit}"

        value /= 1000.0
        abs_val /= 1000.0

    return f"{value:.3f}P"
