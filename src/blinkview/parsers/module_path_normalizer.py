# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.configurable import configuration_property
from blinkview.parsers.transformer import TransformerFactory, TransformStep


@TransformerFactory.register("path_normalizer")
@configuration_property(
    "module_index",
    title="Module Index",
    type="integer",
    minimum=0,
    ui_order=3,
    description="Index where the module name begins (0-based).",
)
class PathNormalizerStep(TransformStep):
    __doc__ = "A transformation step that normalizes module paths in log lines. It extracts the module name from the specified index, processes any tags enclosed in brackets, and constructs a normalized path. For example, given a log line with 'app: [TAG1] [TAG2] Message', it will produce 'app.tag1.tag2 Message'. This helps standardize module paths for better filtering and analysis."
    input_type = "str"
    output_type = "str"

    def __init__(self):
        super().__init__()
        self.module_index = 0

    def apply_config(self, config: dict):
        super().apply_config(config)
        idx = self.module_index

        def fast_call(data: str):
            if not data:
                return data

            # Pivot split
            parts = data.split(None, idx + 1)
            if len(parts) <= idx:
                return data

            # Extract and clean the Prefix (e.g., 'app:')
            prefix = parts[idx].rstrip(":,").lower()
            remainder = parts[idx + 1] if len(parts) > idx + 1 else ""

            # Quick Bail: If remainder doesn't start with '[', skip tag logic
            stripped_remainder = remainder.lstrip()
            if not stripped_remainder.startswith("["):
                pre_part = " ".join(parts[:idx])
                sep = " " if pre_part else ""
                return f"{pre_part}{sep}{prefix} {remainder}".strip()

            # Continuous Tag Scan
            tags = []
            cursor = remainder.find("[")  # Start at the first bracket

            while cursor < len(remainder) and remainder[cursor] == "[":
                end_bracket = remainder.find("]", cursor)
                if end_bracket == -1:
                    break

                # Normalize tag: "CEL SDK DEBUG" -> "cel_sdk_debug"
                tag_content = remainder[cursor + 1 : end_bracket].lower().strip().replace(" ", "_")
                tags.append(tag_content)
                cursor = end_bracket + 1

                # Skip whitespace between tags if present (e.g., [TAG] [TAG])
                while cursor < len(remainder) and remainder[cursor] == " ":
                    cursor += 1

                # If the next character isn't '[', we've exited the tag block
                if cursor >= len(remainder) or remainder[cursor] != "[":
                    break

            # Check if the next non-space character is a colon
            if cursor < len(remainder) and remainder[cursor] == ":":
                cursor += 1

            # Final Assembly
            full_path = ".".join([prefix] + tags)
            pre_part = " ".join(parts[:idx])
            message_body = remainder[cursor:].lstrip()

            sep = " " if pre_part else ""
            return f"{pre_part}{sep}{full_path} {message_body}".strip()

        self.process = fast_call
