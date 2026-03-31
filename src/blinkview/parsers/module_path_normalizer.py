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
        changed = super().apply_config(config)
        idx = self.module_index

        def fast_call(data: str):
            if not data:
                return data

            # 1. Identify the prefix position.
            # We split by whitespace, but we must be careful if the module itself has spaces.
            parts = data.split(None, idx)
            if len(parts) < idx:
                return data

            # The 'remainder' starts from where the module should be
            # Example: "I (1510) [CAN TEST]: message" -> pre_part="I (1510)", remainder="[CAN TEST]: message"
            pre_part = " ".join(parts[:idx])
            remainder = data[len(pre_part) :].lstrip()

            tags = []
            cursor = 0

            # 2. Continuous Scan
            while cursor < len(remainder):
                # Skip whitespace
                while cursor < len(remainder) and remainder[cursor] == " ":
                    cursor += 1

                if cursor >= len(remainder):
                    break

                # Case A: Bracketed Tag [CAN TEST]
                if remainder[cursor] == "[":
                    end_bracket = remainder.find("]", cursor)
                    if end_bracket == -1:
                        break

                    # Clean content: "CAN TEST" -> "can_test"
                    tag_content = remainder[cursor + 1 : end_bracket].lower().strip().replace(" ", "_")
                    tags.append(tag_content)

                    cursor = end_bracket + 1
                    # Consume trailing colon if it exists: [VEH]:
                    if cursor < len(remainder) and remainder[cursor] == ":":
                        cursor += 1
                    continue

                # Case B: Word ending in colon (e.g., bcu_power_on:)
                next_space = remainder.find(" ", cursor)
                word_end = next_space if next_space != -1 else len(remainder)
                word = remainder[cursor:word_end]

                if word.endswith(":"):
                    tags.append(word.rstrip(":").lower())
                    cursor = word_end
                    continue

                # If we hit something without a bracket or a colon, it's the message body
                break

            # 3. Assembly
            full_path = ".".join(filter(None, tags))
            message_body = remainder[cursor:].lstrip()

            sep = " " if pre_part else ""
            return f"{pre_part}{sep}{full_path} {message_body}".strip()

        self.process = fast_call

        return changed
