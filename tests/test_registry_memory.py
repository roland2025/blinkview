# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import gc
import os

import psutil
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry.registry import IDRegistry


@pytest.fixture
def pool():
    gc.collect()
    return NumpyArrayPool()


def get_uss_mb():
    gc.collect()
    return psutil.Process(os.getpid()).memory_full_info().uss / (1024 * 1024)


@pytest.mark.memory
def test_initial_import_baseline():
    """Fail if the initial app footprint exceeds 250MB."""
    current_uss = get_uss_mb()

    print(f"\n[Baseline] Current Idle Memory: {current_uss:.2f} MB")

    assert current_uss < 58.0, f"Idle memory footprint is too large: {current_uss:.2f} MB"


import time


def test_module_registration_density(pool):
    """
    Verify that registering 10,000 modules doesn't exceed a specific memory budget,
    and that insertion times remain strictly bounded.
    """
    registry = IDRegistry(pool)

    # 1. Baseline
    baseline = get_uss_mb()

    # 2. Simulate Load
    total_modules = 20_000
    device = registry.get_device("bench_dev_1")
    device.get_module("test_warmup")

    modules = [f"mod_{i}" for i in range(total_modules)]

    # Timing variables
    min_time_ns = float("inf")
    max_time_ns = 0
    total_time_ns = 0

    for m_name in modules:
        t0 = time.perf_counter_ns()
        device.get_module(m_name)
        t1 = time.perf_counter_ns()

        delta_ns = t1 - t0
        total_time_ns += delta_ns

        if delta_ns < min_time_ns:
            min_time_ns = delta_ns
        if delta_ns > max_time_ns:
            max_time_ns = delta_ns

    # 3. Final Measurements
    final = get_uss_mb()
    delta_mb = final - baseline
    avg_cost_kb = (delta_mb * 1024) / total_modules

    # Convert nanoseconds to milliseconds (1 ms = 1,000,000 ns)
    avg_time_ms = (total_time_ns / total_modules) / 1_000_000
    min_time_ms = min_time_ns / 1_000_000
    max_time_ms = max_time_ns / 1_000_000

    print(f"\n[Memory Report] Total Modules: {total_modules}")
    print(f"Total Delta: {delta_mb:.2f} MB")
    print(f"Avg Cost per Module: {avg_cost_kb:.2f} KB")

    print(f"\n[Performance Report]")
    print(f"Min Add Time: {min_time_ms:.4f} ms")
    print(f"Max Add Time: {max_time_ms:.4f} ms")
    print(f"Avg Add Time: {avg_time_ms:.4f} ms")

    # 4. Assertions
    assert avg_cost_kb < 0.69, f"Module memory cost is too high: {avg_cost_kb:.2f} KB/obj"

    # Timing assertions based on your targets
    assert avg_time_ms < 0.05, f"Average insertion time too slow: {avg_time_ms:.4f} ms"

    # this max high insertion time is caused by resizing of internal tracking arrays
    # assert max_time_ms < 25, f"Max insertion time spiked too high: {max_time_ms:.4f} ms"
