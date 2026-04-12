# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import namedtuple

# --- 1. Signature Constants ---
EmptyState = namedtuple("EmptyState", [])
EMPTY_STATE = EmptyState()


# ASCII Character Ranges
CHAR_UPPER_A = 65
CHAR_UPPER_Z = 90
CHAR_LOWER_A = 97
CHAR_LOWER_Z = 122
CHAR_ZERO = 48
CHAR_NINE = 57

# ASCII Character Constants
CHAR_TAB = 9  # \t
CHAR_SPACE = 32  # ' '
CHAR_DOT = 46  # .
CHAR_COLON = 58  # :
CHAR_LBRACKET = 91  # [
CHAR_RBRACKET = 93  # ]
CHAR_UNDERSCORE = 95  # _
CHAR_NULL = 0

CHAR_QUESTION = 63  # '?'

# Common Delimiters
CHAR_COMMA = 44  # ,
CHAR_DASH = 45  # -
CHAR_SLASH = 47  # /
CHAR_BACKSLASH = 92  # \
CHAR_ESC = 27  # ANSI Escape

# Line Endings
CHAR_LF = 10  # \n (Line Feed)
CHAR_CR = 13  # \r (Carriage Return)

# Case Conversion
CASE_OFFSET = 32  # Difference between 'A' and 'a'
