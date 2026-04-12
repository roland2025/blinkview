import gc
import os
import unittest

import psutil

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry.registry import IDRegistry


class TestRegistryMemory(unittest.TestCase):
    def setUp(self):
        self.process = psutil.Process(os.getpid())
        self.pool = NumpyArrayPool()
        # Ensure we start fresh
        gc.collect()

    def get_uss_mb(self):
        gc.collect()
        return self.process.memory_full_info().uss / (1024 * 1024)

    def test_initial_import_baseline(self):
        """Fail if the initial app footprint exceeds 250MB."""
        current_uss = self.get_uss_mb()

        # Print the current value clearly to the console
        print(f"\n[Baseline] Current Idle Memory: {current_uss:.2f} MB")

        self.assertLess(current_uss, 42.0, f"Idle memory footprint is too large: {current_uss:.2f} MB")

    def test_module_registration_density(self):
        """
        Verify that registering 10,000 modules doesn't exceed a specific memory budget.
        """
        registry = IDRegistry(self.pool)

        # 1. Baseline
        baseline = self.get_uss_mb()

        # 2. Simulate Load: 100 devices with 100 modules each (10,000 total)
        total_modules = 10000
        for d in range(100):
            device = registry.get_device(f"bench_dev_{d}")
            modules = [f"mod_{i}" for i in range(100)]
            # Assuming your DeviceIdentity handles module creation:
            for m_name in modules:
                device.get_module(m_name)

                # 3. Final Measurement
        final = self.get_uss_mb()
        delta = final - baseline
        avg_cost_kb = (delta * 1024) / total_modules

        print(f"\n[Memory Report] Total Modules: {total_modules}")
        print(f"Total Delta: {delta:.2f} MB")
        print(f"Avg Cost per Module: {avg_cost_kb:.2f} KB")

        # 4. Assertions (The Guardrails)
        # Based on your previous result of ~20KB, let's set a safe limit of 50KB.
        # If a future change makes modules cost 100KB, this test will FAIL.
        self.assertLess(avg_cost_kb, 0.69, f"Module memory cost is too high: {avg_cost_kb:.2f} KB/obj")


if __name__ == "__main__":
    unittest.main()
