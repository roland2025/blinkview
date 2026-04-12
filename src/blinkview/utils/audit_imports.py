# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import gc
import os

import psutil

process = psutil.Process(os.getpid())


def check_mem(label):
    gc.collect()
    mem = getattr(process.memory_info(), "private", process.memory_info().rss)
    print(f"{label:<30} | Private Bytes: {mem / 1024 / 1024:>8.2f} MB")


print(f"{'Step':<30} | {'Memory':<15}")
print("-" * 50)

check_mem("Initial (Clean Python)")

import numpy as np

check_mem("After 'import numpy'")

import threading

check_mem("After 'import threading'")

from blinkview.core import dtypes

check_mem("After 'import dtypes'")

from blinkview.core.id_registry.tables import IndexedStringTable

check_mem("After 'import IndexedStringTable'")
