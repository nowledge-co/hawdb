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
  `check_deadline_at(..)`) whose `map_err` closure builds `Execution`;
- a `Stopped(reason)` / `Cancelled(reason)` match arm that formats the reason
  into `Execution`.
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
)
TASK_CHECK = re.compile(
    r"(?:checkpoint\(\)|check_cancellation\(\)|check_deadline_at\([^()]*\))"
    r"\s*\.map_err\(\s*\|(\w+)\|"
)
STOP_ARM = re.compile(r"\b(?:Stopped|Cancelled)\((\w+)\)\s*=>")
EXECUTION = "Execution("


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
    """The match arm expression after `=>`, up to its top-level comma."""
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


def formats(body, name):
    return re.search(r"\{" + re.escape(name) + r"(?::[^}]*)?\}", body) or re.search(
        r"\b" + re.escape(name) + r"\.to_string\(\)", body
    )


def violations_in(relative, source):
    text = production_text(source)
    found = []
    for match in TASK_CHECK.finditer(text):
        body = closure_body(text, match.end())
        if EXECUTION in body:
            line = text.count("\n", 0, match.start()) + 1
            found.append(f"{relative}:{line}: task check maps its stop to Execution")
    for match in STOP_ARM.finditer(text):
        body = arm_body(text, match.end())
        if EXECUTION in body and formats(body, match.group(1)):
            line = text.count("\n", 0, match.start()) + 1
            found.append(f"{relative}:{line}: stop reason formatted into Execution")
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
            "not as an Execution message (#1850)",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
