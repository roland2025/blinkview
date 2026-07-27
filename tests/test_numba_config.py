# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import blinkview.core.numba_config as numba_config


def plain_add(x):
    return x + 1


def fallback_add(x):
    return x + 100


def test_debug_true_returns_the_original_func_regardless_of_numba_state():
    wrapped = numba_config.app_njit(debug=True)(plain_add)
    assert wrapped is plain_add


def test_debug_true_with_fallback_returns_the_fallback():
    wrapped = numba_config.app_njit(debug=True, fallback=fallback_add)(plain_add)
    assert wrapped is fallback_add


def test_numba_disable_returns_the_original_func(monkeypatch):
    monkeypatch.setattr(numba_config, "NUMBA_DISABLE", True)
    wrapped = numba_config.app_njit()(plain_add)
    assert wrapped is plain_add


def test_numba_disable_with_fallback_returns_the_fallback(monkeypatch):
    monkeypatch.setattr(numba_config, "NUMBA_DISABLE", True)
    wrapped = numba_config.app_njit(fallback=fallback_add)(plain_add)
    assert wrapped is fallback_add


def test_numba_enabled_compiles_and_preserves_behavior():
    wrapped = numba_config.app_njit()(plain_add)
    assert wrapped is not plain_add
    assert wrapped(2) == 3


def test_default_kwargs_applied_when_numba_enabled():
    wrapped = numba_config.app_njit()(plain_add)
    wrapped(1)  # trigger compilation
    assert wrapped.targetoptions["boundscheck"] is False
    assert wrapped.targetoptions["fastmath"] is True


def test_explicit_kwargs_are_not_overridden():
    wrapped = numba_config.app_njit(boundscheck=True, fastmath=False)(plain_add)
    wrapped(1)
    assert wrapped.targetoptions["boundscheck"] is True
    assert wrapped.targetoptions["fastmath"] is False


def test_hunting_bugs_flag_forces_debug_safe_settings(monkeypatch):
    monkeypatch.setattr(numba_config, "NUMBA_HUNTING_BUGS", True)
    wrapped = numba_config.app_njit()(plain_add)
    wrapped(1)
    assert wrapped.targetoptions["boundscheck"] is True
    assert wrapped.targetoptions["fastmath"] is False
    assert wrapped.targetoptions["inline"] == "never"
