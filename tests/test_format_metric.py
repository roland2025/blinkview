# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.utils.format_metric import format_metric


class TestFormatMetric:
    def test_zero_is_the_literal_string_zero(self):
        assert format_metric(0) == "0"

    def test_sub_thousand_int_formats_with_no_decimals(self):
        assert format_metric(42) == "42"

    def test_negative_sub_thousand_formats_with_no_decimals(self):
        assert format_metric(-42) == "-42"

    def test_value_just_below_a_thousand_stays_an_integer(self):
        assert format_metric(999) == "999"

    def test_thousands_use_k_suffix(self):
        assert format_metric(1000) == "1.000K"

    def test_millions_use_m_suffix(self):
        assert format_metric(1_500_000) == "1.500M"

    def test_billions_use_b_suffix(self):
        assert format_metric(2_000_000_000) == "2.000B"

    def test_trillions_use_t_suffix(self):
        assert format_metric(3_000_000_000_000) == "3.000T"

    def test_quadrillions_fall_through_to_p_suffix(self):
        assert format_metric(4_000_000_000_000_000) == "4.000P"

    def test_negative_large_value_keeps_sign_and_suffix(self):
        assert format_metric(-1500) == "-1.500K"

    def test_float_input_sub_thousand_rounds_to_integer_string(self):
        assert format_metric(42.7) == "43"
