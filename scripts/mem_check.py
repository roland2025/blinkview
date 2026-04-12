# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os

import psutil

p = psutil.Process(os.getpid())
priv = getattr(p.memory_info(), "private", p.memory_info().rss)
print(f"Absolute Bare Start: {priv / 1024 / 1024:.2f} MB")
