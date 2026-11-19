#!/usr/bin/env python3
# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Refuse production code that reports a stopped runtime task as text.

A cancelled or expired `RuntimeTaskContext` must surface as the typed
`HawDBError::RuntimeTaskStopped(reason)` (#1850), never as a
`HawDBError::Execution` message, so hosts never match error text to recognise
a deadline or a cancellation. This scans production Rust sources (test files
and trailing `#[cfg(test)]` modules are skipped) for two shapes:

- a task check (`checkpoint()`, `check_cancellation()`,
  `check_deadline_at(..)`) or I/O admission (`acquire_io_wave(..)`,
  `io_wave()`, `io()`) whose `map_err` closure builds `Execution` or
  `Storage`;
- a `Stopped(..)` / `Cancelled(..)` match arm (a bound name, `_` or `..`,
  including rustfmt-wrapped forms) whose body builds `Execution` or `Storage`,
  whatever message it carries;
- a `map_err(|error| HawDBError::Execution(error.to_string()))` closure, which
  would hide a stop carried by `error`; use
  `HawDBError::from_execution_error`.

`Storage` is allowed only at the reviewed sites in `STORAGE_ALLOWLIST`, where
a stop can follow a durable rename and must keep poisoning hosts that treat
`Storage` as fatal.
"""

import argparse
import os
from pathlib import Path
import re
import sys


SOURCE_ROOTS = (
    "src",
    "crates/core/src",
    "crates/executor/src",
    "crates/search/src",
    "crates/relational/src",
    "crates/storage/src",
    "crates/analytics/src",
    "crates/runtime-tokio/src",
    "crates/vector-projection/src",
)
# (path, stripped source line of the flagged arm or check) for stops that intentionally
# stay `Storage`. Each entry needs a reason; none are needed today because
# post-rename stops are flattened into internal publication error text before
# they reach a `HawDBError`.
STORAGE_ALLOWLIST = frozenset()
TASK_CHECK = re.compile(
    r"(?:checkpoint\(\)|check_cancellation\(\)|check_deadline_at\([^()]*\)"
    r"|acquire_io_wave\([^()]*\)|\bio_wave\(\)|\.io\(\))"
    r"\s*\.map_err\(\s*\|(\w+)\|"
)
STRINGIFIED_EXECUTION = re.compile(
    r"\.map_err\(\s*\|(\w+)\|\s*\{?\s*(?:[\w:]+::)?HawDBError::Execution\(\s*\1\.to_string\(\)\s*\)"
)
STOP_ARM = re.compile(
    r"\b(?:Stopped|Cancelled)\s*(?:\(\s*(?:\w+|\.\.)\s*,?\s*\)|\{[^{}]*\})\s*=>"
)
TEXT_VARIANT = re.compile(r"\b(Execution|Storage)\(")


def is_test_source(relative):
    name = relative.rsplit("/", 1)[-1]
    return (
        "/tests/" in f"/{relative}"
        or name in ("tests.rs", "test.rs")
        or name.endswith("_tests.rs")
        or name.endswith("_test.rs")
    )


def production_text(source):
    """Drop a trailing inline test module; production code precedes it."""
    match = re.search(r"\n#\[cfg\(test\)\]\s*\nmod \w+ \{", source)
    return source[: match.start()] if match else source


def closure_body(source, start):
    """Text of the `map_err(` call opened just before `start`."""
    depth = 1
    index = start
    while index < len(source) and depth:
        if source[index] == "(":
            depth += 1
        elif source[index] == ")":
            depth -= 1
        index += 1
    return source[start:index]


def arm_body(source, start):
    """The match arm expression after `=>`: its block, or up to the comma."""
    index = start
    while index < len(source) and source[index].isspace():
        index += 1
    if index < len(source) and source[index] == "{":
        depth = 0
        for end in range(index, len(source)):
            if source[end] == "{":
                depth += 1
            elif source[end] == "}":
                depth -= 1
                if depth == 0:
                    return source[index : end + 1]
        return source[index:]
    depth = 0
    index = start
    while index < len(source):
        character = source[index]
        if character in "({[":
            depth += 1
        elif character in ")}]":
            if depth == 0:
                break
            depth -= 1
        elif character == "," and depth == 0:
            break
        index += 1
    return source[start:index]


def text_variant(body):
    match = TEXT_VARIANT.search(body)
    return match.group(1) if match else None


def allowed(relative, text, start, variant):
    if variant != "Storage":
        return False
    begin = text.rfind("\n", 0, start) + 1
    end = text.find("\n", start)
    line = text[begin : end if end >= 0 else len(text)].strip()
    return (relative, line) in STORAGE_ALLOWLIST


def violations_in(relative, source):
    text = production_text(source)
    found = []
    for match in TASK_CHECK.finditer(text):
        variant = text_variant(closure_body(text, match.end()))
        if variant and not allowed(relative, text, match.start(), variant):
            line = text.count("\n", 0, match.start()) + 1
            found.append(f"{relative}:{line}: task check maps its stop to {variant}")
    for match in STRINGIFIED_EXECUTION.finditer(text):
        line = text.count("\n", 0, match.start()) + 1
        found.append(
            f"{relative}:{line}: Execution(error.to_string()) can hide a stop; "
            "use HawDBError::from_execution_error"
        )
    for match in STOP_ARM.finditer(text):
        variant = text_variant(arm_body(text, match.end()))
        if variant and not allowed(relative, text, match.start(), variant):
            line = text.count("\n", 0, match.start()) + 1
            found.append(f"{relative}:{line}: stop arm reports {variant} text")
    return found


def scan(root):
    found = []
    for source_root in SOURCE_ROOTS:
        sources = sorted((root / source_root).rglob("*.rs"))
        if not sources:
            raise ValueError(f"missing source coverage: {source_root}")
        for path in sources:
            relative = path.relative_to(root).as_posix()
            if is_test_source(relative):
                continue
            found.extend(violations_in(relative, path.read_text(encoding="utf-8")))
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).resolve().parent.parent
    if "TEST_SRCDIR" in os.environ:
        root = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
    parser.add_argument("--root", type=Path, default=root)
    arguments = parser.parse_args()
    found = scan(arguments.root)
    for violation in found:
        print(violation, file=sys.stderr)
    if found:
        print(
            "report a stopped runtime task as HawDBError::RuntimeTaskStopped(reason), "
            "not as an Execution or Storage message (#1850)",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
