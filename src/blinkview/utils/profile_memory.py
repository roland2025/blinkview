# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import gc
import os
from typing import Any, Callable

import psutil


def profile_memory(target_func: Callable, *args, **kwargs) -> Any:
    """
    Executes target_func with *args and **kwargs, printing the
    OS memory impact (Private Bytes and USS).
    """
    process = psutil.Process(os.getpid())

    def get_stats():
        # Clean up garbage to get a 'true' reading of retained objects
        gc.collect()
        full_info = process.memory_full_info()
        basic_info = process.memory_info()

        # Windows-specific 'private' bytes vs RSS fallback
        private_bytes = getattr(basic_info, "private", basic_info.rss)
        uss = full_info.uss
        return private_bytes, uss

    def to_mb(b):
        return b / (1024 * 1024)

    # 1. Capture Baseline
    base_private, base_uss = get_stats()

    # 2. Execute Target
    print(f"--- Executing: {target_func.__name__} ---")
    result = target_func(*args, **kwargs)

    # 3. Capture Final
    final_private, final_uss = get_stats()

    # 4. Display Results
    p_delta = final_private - base_private
    u_delta = final_uss - base_uss

    print(f"\n{'Metric':<20} | {'Baseline':<12} | {'Final':<12} | {'Delta':<12}")
    print("-" * 65)
    print(
        f"{'Private Bytes':<20} | {to_mb(base_private):>8.2f} MB | {to_mb(final_private):>8.2f} MB | {to_mb(p_delta):>+8.2f} MB"
    )
    print(
        f"{'USS (Unique Set)':<20} | {to_mb(base_uss):>8.2f} MB | {to_mb(final_uss):>8.2f} MB | {to_mb(u_delta):>+8.2f} MB"
    )
    print("-" * 65)

    if p_delta > u_delta * 1.5 and p_delta > (1 * 1024 * 1024):  # Only warn if delta > 1MB
        print("\n[!] WARNING: Private Bytes are significantly higher than USS.")
        print("    This suggests heavy heap fragmentation or memory pre-allocation.")

    return result
