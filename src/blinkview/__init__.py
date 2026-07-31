# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
from pathlib import Path

__version__ = "0.14.0.dev0"
__author__ = "Roland Uuesoo"

__app_name__ = "BlinkView"
__org_domain__ = "ee.incubator"

# Your specific ID requirement
__app_id__ = f"{__org_domain__}.{__app_name__.lower()}"


# Robust icon path (relative to this file)
ICON_PATH = (Path(__file__).parent / "assets" / "icon.png").absolute()


# These must come BEFORE numpy, scipy, or any telemetry logic
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
