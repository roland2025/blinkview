# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from typing import List

from ..core.configurable import configuration_property
from ..core.system_context import SystemContext
from .transformer import (
    PipelineDecodeFactory,
    PipelinePrintableFactory,
    PipelineTransformFactory,
    TransformerFactory,
    TransformStep,
)


@TransformerFactory.register("ansi_filter")
@TransformerFactory.register("regex_replace")
class RegexMagic(TransformStep):
    input_type = "str"
    output_type = "str"

    def __init__(self):
        super().__init__()
        # Pre-compiling here is much faster than doing it inside __call__
        self.pattern = None
        self.replacement = None
        self.compiled = None
        self.process = None

    def apply_config(self, config: dict):
        # Allow dynamic updates to the pattern and replacement if needed
        self.pattern = config.get("pattern", r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        self.replacement = config.get("replace", "")

        self.compiled = re.compile(self.pattern)
        fast_sub = self.compiled.sub
        replacement = self.replacement

        def fast_process(data: str):
            return fast_sub(replacement, data)

        self.process = fast_process


@PipelineDecodeFactory.register("bytes_decode")
@configuration_property("encoding", type="string", default="ascii")
@configuration_property("errors", type="string", enum=["strict", "ignore", "replace"], default="replace")
class DecoderStep(TransformStep):
    __doc__ = "A simple bytes to string decoder step that uses the specified encoding and error handling strategy."

    encoding: str
    errors: str

    input_type = "bytes"
    output_type = "str"

    def process(self, data: bytes) -> str:
        return data.decode(self.encoding, errors=self.errors)


@PipelinePrintableFactory.register("bytes_translate")
@configuration_property(
    "allow_escape",
    type="boolean",
    default=True,
    title="Allow Escape (ESC)",
    description="If enabled, the ASCII Escape character (decimal 27) will be preserved in the output.",
)
class BytesTranslateStep(TransformStep):
    __doc__ = "A bytes transformation step that removes all non-printable ASCII characters(except Escape byte) from the input bytes. It uses a pre-computed translation table for maximum performance."
    __slots__ = ("table", "delete", "process")

    allow_escape: bool

    input_type = "bytes"
    output_type = "bytes"

    def __init__(self):
        super().__init__()
        self.delete = None
        self.table = None
        self.process = None

    def apply_config(self, _: dict):
        allowed = set(range(32, 127))

        if self.allow_escape:
            allowed.add(27)

        # Create the 'delete' string of bytes NOT in our allowed set
        delete = bytes(b for b in range(256) if b not in allowed)
        self.delete = delete
        table = self.table

        def fast_call(data: bytes):
            return data.translate(table, delete)

        self.process = fast_call


@TransformerFactory.register("string_replace")
@configuration_property(
    "search",
    type="string",
    default="",
    required=True,
    ui_order=1,
    description="The substring to search for in the input string.",
)
@configuration_property(
    "replace",
    type="string",
    default="",
    required=True,
    ui_order=2,
    description="The substring to replace each occurrence of the search string with.",
)
class StringReplaceStep(TransformStep):
    __doc__ = "A simple string replacement step that replaces all occurrences of 'search' with 'replace'."
    search: str
    replace: str

    input_type = "str"
    output_type = "str"

    def __init__(self):
        super().__init__()
        self.process = None

    def apply_config(self, config: dict):
        super().apply_config(config)

        search = self.search
        replace = self.replace

        def fast_call(data: str):
            return data.replace(search, replace)

        self.process = fast_call


@configuration_property(
    "steps",
    type="array",
    required=True,
    items={
        "type": "object",
        "_factory": "pipeline_transformer",
        "title": "Processing Step",
    },
)
@PipelineTransformFactory.register("default")
class ConfigurableParser(TransformStep):
    def __init__(self):
        super().__init__()
        self.pipeline: List[TransformStep] = []
        self.process = str  # Default to simple string conversion if no steps are configured
        self._shared: SystemContext = None

    def bind_system(self, shared, _):
        self._shared = shared

    def apply_config(self, config: dict):
        """
        Expects a JSON structure like:
        "steps": [
            {"type": "decode", "encoding": "utf-8"},
            {"type": "ansi_filter"},
            {"type": "replace", "search": "\r", "replace": ""}
        ]
        """

        pipeline: List[TransformStep] = []

        for step_cfg in config.get("steps", []):
            if True:  # step_cfg.get("enabled", True):
                step = self._shared.factories.build("pipeline_transformer", config=step_cfg, system_ctx=self._shared)

                # Validation: If the last step output 'str' and this one needs 'bytes',
                # we can catch the error before the app even starts.
                if pipeline and pipeline[-1].output_type != step.input_type:
                    print(f"ConfigurableParser: Warning: Type mismatch between {pipeline[-1]} and {step}")

                pipeline.append(step)

        self.pipeline = pipeline

        self._bake()

    def _bake(self):
        # We create a local reference so the loop doesn't have to
        # look up 'self' every iteration
        steps = self.pipeline

        if not steps:
            self.process = str
            return

        # create list of functions to call in sequence
        fast_steps = tuple([step.process for step in steps])

        # Micro-optimization: If there's only 1 step, don't even loop!
        if len(fast_steps) == 1:
            self.process = fast_steps[0]
            return

        def baked_runner(data):
            for step in fast_steps:
                data = step(data)
            return data

        self.process = baked_runner


@TransformerFactory.register("whitespace_normalizer")
class WhitespaceNormalizerStep(TransformStep):
    """
    Collapses all contiguous whitespace (spaces, tabs, newlines) into
    single spaces and trims leading/trailing whitespace.
    """

    __doc__ = "A high-performance whitespace normalizer using str.split and str.join."

    input_type = "str"
    output_type = "str"

    def process(self, data: str) -> str:
        # data.split() without arguments splits on any whitespace run
        # " ".join(...) puts a single space between the resulting words
        return " ".join(data.split())
