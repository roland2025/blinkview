# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.base_configurable import configuration_property
from blinkview.parsers.transformer import TransformerFactory, TransformStep


@TransformerFactory.register("fixed_width_normalizer")
@configuration_property(
    "module_index",
    title="Module Index",
    type="integer",
    minimum=0,
    ui_order=1,
    description="Word index where the module name field begins.",
)
@configuration_property(
    "max_chars",
    title="Max Characters",
    type="integer",
    minimum=1,
    default=25,
    ui_order=2,
    description="Number of characters to consume for the module name.",
)
class FixedWidthPathNormalizer(TransformStep):
    __doc__ = "A transformer that normalizes fixed-width module name fields in log lines. It extracts a substring from a specified word index and character length, then normalizes it by lowercasing, stripping whitespace, and replacing non-alphanumeric characters with underscores. This is useful for standardizing module names that may contain variable formatting (e.g., '3V3 / 5V' becomes '3v3_5v')."
    input_type = "str"
    output_type = "str"

    def __init__(self):
        super().__init__()
        self.module_index = 0
        self.max_chars = 25
        self._trans_table = str.maketrans(" /-", "___")

    def apply_config(self, config: dict):
        super().apply_config(config)
        idx = self.module_index
        length = self.max_chars

        trans_table = self._trans_table

        if idx == 0:

            def fast_call(data: str):
                # Direct Slicing (No split/join overhead)
                raw_module = data[:length]
                message_body = data[length:].lstrip()

                # Fast Normalize
                # Lower, strip, and translate in one chain
                tmp = raw_module.lower().strip().translate(trans_table)

                # Collapse multiple underscores efficiently
                # split("_") + filter(None) + join is faster than a 'while' loop
                # for long strings or multiple occurrences.
                clean_module = "_".join(filter(None, tmp.split("_")))

                # Final Assembly
                return f"{clean_module} {message_body}"
        else:

            def fast_call(data: str):
                # Pivot split to find the start of our target field
                parts = data.split(None, idx)
                if len(parts) <= idx:
                    return data

                # Everything after the split point is our work area
                remainder = parts[idx]

                # Extract the fixed-width field
                raw_module = remainder[:length]
                message_body = remainder[length:].lstrip()

                # Normalize the module name:
                # - Lowercase
                # - Strip whitespace
                # - Replace non-alphanumeric chars (like / and spaces) with underscores
                # - Collapse multiple underscores into one
                clean_module = raw_module.lower().strip().translate(trans_table)

                # Remove double underscores that might result from "3V3 / 5V"
                while "__" in clean_module:
                    clean_module = clean_module.replace("__", "_")

                clean_module = clean_module.strip("_")

                # Assembly
                pre_part = " ".join(parts[:idx])
                sep = " " if pre_part else ""

                return f"{pre_part}{sep}{clean_module} {message_body}".strip()

        self.process = fast_call
