# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass, field

from qtpy.QtGui import QColor


@dataclass
class StyleConfig:
    # Timing (immutable floats/ints are fine as standard defaults)
    fade_duration: float = 0.6
    stale_threshold: float = 30.0
    ui_update_rate_ms: int = 1000 // 60  # 10 FPS update rate

    # Brand / Visual Colors (must use default_factory for QColor)
    color_flash_base: QColor = field(default_factory=lambda: QColor(0, 155, 0, 50))
    color_text_name: QColor = field(default_factory=lambda: QColor(200, 200, 200))
    color_text_stale: QColor = field(default_factory=lambda: QColor(120, 120, 120))
    color_text_default: QColor = field(default_factory=lambda: QColor(255, 255, 255))

    # Opacity levels
    flash_max_opacity: float = 0.2
