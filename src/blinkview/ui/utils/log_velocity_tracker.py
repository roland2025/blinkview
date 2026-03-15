# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter


class LogVelocityTracker:
    def __init__(self,
                 limit_per_sec=1000,
                 burst_limit_seconds=3,
                 instant_cap=5000):
        """
        :param limit_per_sec: The sustained "safe" velocity.
        :param burst_limit_seconds: How many seconds we allow velocity > limit_per_sec.
        :param instant_cap: If a single batch or window exceeds this, pause immediately.
        """
        self.limit_per_sec = limit_per_sec
        self.burst_limit_seconds = burst_limit_seconds
        self.instant_cap = instant_cap

        self.msg_counter = 0
        self.last_check_time = perf_counter()
        self.over_limit_start_time = None

    def reset(self):
        self.msg_counter = 0
        self.last_check_time = perf_counter()
        self.over_limit_start_time = None

    def update_and_check(self, batch_size: int) -> bool:
        """
        Updates the tracker. Returns True if we should AUTO-PAUSE.
        """
        now = perf_counter()
        self.msg_counter += batch_size
        elapsed = now - self.last_check_time

        # --- RULE 1: THE INSTANT CAP ---
        # If we see a massive spike before the 1s window even finishes
        if self.msg_counter > self.instant_cap:
            return True

        # --- RULE 2: SUSTAINED VELOCITY (THE BURST) ---
        if elapsed >= 1.0:
            velocity = self.msg_counter / elapsed

            if velocity > self.limit_per_sec:
                if self.over_limit_start_time is None:
                    self.over_limit_start_time = now

                # Check if we've been over the limit for too long
                sustained_duration = now - self.over_limit_start_time
                if sustained_duration >= self.burst_limit_seconds:
                    return True
            else:
                # We dipped back into safety; reset the burst timer
                self.over_limit_start_time = None

            # Reset window
            self.msg_counter = 0
            self.last_check_time = now

        return False
