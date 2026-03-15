# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

class TextFilter:
    def __init__(self):
        self._filters = []

    def add_replace(self, old: str, new: str):
        self._filters.append(lambda s, o=old, n=new: s.replace(o, n))

    def add_strip(self, chars=None):
        self._filters.append(lambda s, c=chars: s.strip(c))

    def add_custom(self, func):
        """func must take str and return str"""
        self._filters.append(func)

    def apply(self, text: str) -> str:
        for f in self._filters:
            text = f(text)
        return text
