# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Session-rotation change counter for UI consumers - see plans/session-rotation.md.

Registry.rotate_session() empties the log pool in place and then bumps
`registry.session_generation`. Views compare it against the value they last saw in
apply_updates() (the same polling style as PlaybackClock) and throw away what they had fetched
from the previous session. Kept in its own tiny module so widgets don't have to import the
(heavy) registry module just to read one integer.
"""


def session_generation_of(registry) -> int:
    """The registry's current session generation, or 0 when there is no registry or it doesn't
    track one (test doubles, headless tools)."""
    return getattr(registry, "session_generation", 0) if registry is not None else 0
