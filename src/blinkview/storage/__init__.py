# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

# file_logger registers FileLoggerFactory/BatchProcessorFactory categories as an import-time side
# effect. The "as x" re-export alias is deliberate, not decorative: it's the standard
# pyflakes/ruff/pyright signal for "this import is an intentional public re-export".
from . import file_logger as file_logger

__all__ = ["file_logger"]
