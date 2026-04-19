# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os

# Global toggle to completely disable Numba imports
NUMBA_DISABLE = os.environ.get("BLINKVIEW_DISABLE_NUMBA", "0") == "1"  # or True
# NUMBA_DISABLE = True

NUMBA_ENABLE_CACHE = os.environ.get("BLINKVIEW_NO_CACHE", "0") == "0"  # and False
NUMBA_ENABLE_NOGIL = os.environ.get("BLINKVIEW_NO_NOGIL", "0") == "0"


if NUMBA_DISABLE:

    def literal_unroll(container):
        """Pure Python fallback: just returns the container so the standard for-loop works."""
        return container
else:
    # If Numba is enabled, we import the real one
    from warnings import simplefilter

    from numba import literal_unroll
    from numba.core.errors import NumbaExperimentalFeatureWarning

    # Silence Numba's warnings about our dynamic parser injection
    simplefilter("ignore", category=NumbaExperimentalFeatureWarning)


def app_njit(**kwargs):
    """
    Custom Numba JIT decorator with fallback support.
    Usage: @app_njit(fallback=my_python_func)
    """
    run_in_pure_python = kwargs.pop("debug", False)
    # Intercept the alternate function
    fallback = kwargs.pop("fallback", None)

    if NUMBA_DISABLE or run_in_pure_python:

        def dummy_decorator(func):
            # If a fallback was provided, swap the function entirely.
            # Otherwise, return the original function (pure Python).
            return fallback if fallback is not None else func

        return dummy_decorator

    # The Normal Numba Path
    if "cache" not in kwargs:
        kwargs["cache"] = NUMBA_ENABLE_CACHE
    if "nogil" not in kwargs:
        kwargs["nogil"] = NUMBA_ENABLE_NOGIL

    # Add these as "soft" defaults
    if "fastmath" not in kwargs:
        kwargs["fastmath"] = True  # Usually safe and highly beneficial for plotting
    if "boundscheck" not in kwargs:
        kwargs["boundscheck"] = False  # Use with caution!

    def decorator(func):
        from numba import njit

        return njit(**kwargs)(func)

    return decorator
