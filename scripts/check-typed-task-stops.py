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
# (path, message fragment) for formatted `map_err` messages whose error cannot
# carry a runtime-task stop, with the reason.
FORMAT_ALLOWLIST = {
    ("src/api/automatic_checkpoint.rs", "automatic checkpoint planning deferred"): "RuntimeAdmissionError: a governor refusal code, no stop",
    ("src/api/automatic_checkpoint.rs", "automatic checkpoint admission deferred"): "RuntimeAdmissionError: a governor refusal code, no stop",
    ("src/api/automatic_checkpoint.rs", "automatic checkpoint resume deferred"): "RuntimeAdmissionError: a governor refusal code, no stop",
    ("src/api/graph_analytics.rs", "background graph analytics not admitted"): "QosAdmission: a scheduler decision, no stop",
    ("src/api/graph_analytics.rs", "background analytics publication not admitted"): "QosAdmission: a scheduler decision, no stop",
    ("src/api/query_request/owned/host.rs", "owned host scorer is unavailable"): "TryLockError: no stop",
    ("crates/executor/src/kernel.rs", "failed to reserve merge descriptors"): "FileDescriptorError: no stop",
    ("crates/executor/src/load_csv.rs", "LOAD CSV cannot access"): "std::fs::canonicalize io::Error: no task context",
    ("crates/search/src/lexical_projection/block_encoding.rs", "invalid lexical posting frame"): "&'static str",
    ("crates/search/src/lexical_projection.rs", "invalid lexical posting frame"): "&'static str",
    ("crates/search/src/resident_diskann.rs", "resident DiskANN allocation refused"): "RuntimeMemoryError::Stopped is matched first",
    ("crates/search/src/vector_execution.rs", "vector physical execution failed"): "VectorExecutionError without its Source arm: plan/score invariants, no stop",
    ("crates/relational/src/query/access.rs", "invalid relational access path"): "&'static str",
    ("crates/relational/src/query/access.rs", "invalid relational join access"): "&'static str",
    ("crates/storage/src/store/graph_checkpoint.rs", "row-page compaction admission denied"): "String admission refusal",
    ("crates/storage/src/store/graph_checkpoint.rs", "relational overflow compaction admission denied"): "String admission refusal",
    ("crates/storage/src/text/envelope.rs", "zstd compression failed"): "zstd encoder construction io::Error: no task context",
}
TASK_CHECK = re.compile(
    r"(?:checkpoint\(\)|check_cancellation\(\)|check_deadline_at\([^()]*\)"
    r"|acquire_io_wave\([^()]*\)|\bio_wave\(\)|\.io\(\))"
    r"\s*\.map_err\(\s*\|(\w+)\|"
)
STRINGIFIED_EXECUTION = re.compile(
    r"\.map_err\(\s*\|(\w+)\|\s*\{?\s*(?:[\w:]+::)?HawDBError::Execution\(\s*\1\.to_string\(\)\s*\)"
)
# Point-free conversions of a task check that keep the stop typed (or keep it
# in the cause chain, which `HawDBError::from_storage_error`,
# `from_execution_error` and `or_stop` read).
TYPED_POINT_FREE = re.compile(
    r"(?:^|::)(?:Stopped|Cancelled|RuntimeTaskStopped|from_storage_error"
    r"|from_execution_error|from|into|other|Failed|Runtime|Io)$"
    r"|^(?:map_runtime_io_error|canonical_segment_error)$"
)
# Point-free conversions that flatten a stop into internal text on purpose.
POINT_FREE_ALLOWLIST = {
    # Checkpoint encoders and relational publishers report work failures as
    # internal admission text. Some of these checks follow a durable rename,
    # so the stop must keep poisoning hosts that treat `Storage` as fatal
    # (#1850 "remaining text stops").
    "work_error": "internal checkpoint/publication text (#1850)",
    # `relational::overflow::admission(impl ToString)`: same internal text.
    "admission": "internal checkpoint/publication text (#1850)",
    # `CheckpointBuffer` takes the caller's own work-error conversion.
    "map_work": "generic caller-supplied conversion",
}
TASK_POINT_FREE = re.compile(
    r"(?:checkpoint\(\)|check_cancellation\(\)|check_deadline_at\([^()]*\)"
    r"|acquire_io_wave\([^()]*\)|\bio_wave\(\)|\.io\(\)|\bcheckpoint\(\w+\))"
    r"\s*\.map_err\(\s*([A-Za-z_][\w:]*)\s*\)"
)
# A `map_err` closure (the parameter may be typed) that formats its error into
# an `Execution`/`Storage` message without `HawDBError::or_stop`.
FORMAT_CLOSURE = re.compile(r"\.map_err\(\s*(?:move\s+)?\|\s*(\w+)\s*(?::\s*([^|]+?))?\s*\|")
# Error types that cannot carry a stop; a formatted message is fine.
FORMAT_SAFE_TYPES = frozenset({"String", "&str", "str", "&String"})
STOP_ARM = re.compile(
    r"\b(?:Stopped|Cancelled|Work)\s*(?:\(\s*(?:\w+|\.\.)\s*,?\s*\)|\{[^{}]*\})\s*\)?\s*=>"
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


CFG_TEST = re.compile(r"#\[cfg\(test\)\]")
ATTRIBUTE = re.compile(r"\s*#\[[^\]]*\]")
PATH_ATTRIBUTE = re.compile(r'#\[path\s*=\s*"([^"]+)"\]')
MOD_DECLARATION = re.compile(r"\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;")


def balanced_end(source, open_index):
    """Index just past the `}` closing the `{` at `open_index`."""
    depth = 0
    for index in range(open_index, len(source)):
        if source[index] == "{":
            depth += 1
        elif source[index] == "}":
            depth -= 1
            if depth == 0:
                return index + 1
    return len(source)


ITEM_START = re.compile(
    r"\s*(?:pub(?:\s*\([^)]*\))?\s+)?(?:(?:unsafe|async|const|extern(?:\s+\"[^\"]*\")?)\s+)*"
    r"(mod|fn|impl|struct|enum|union|trait|use|const|static|type|macro_rules!)\b"
)
BLOCK_ITEMS = frozenset({"mod", "fn", "impl", "struct", "enum", "union", "trait", "macro_rules!"})


def item_end(source, start, keyword):
    """End of the item starting at `start`, or None if it cannot be bounded.

    Block items end at their first top-level `{...}` block or at a top-level
    `;` (`mod x;`, `struct S(u8);`). Other items end at a top-level `;`."""
    depth = 0
    for index in range(start, len(source)):
        character = source[index]
        if character in "([":
            depth += 1
        elif character in ")]":
            depth -= 1
        elif depth == 0 and character == ";":
            return index + 1
        elif character == "{":
            if keyword in BLOCK_ITEMS and depth == 0:
                return balanced_end(source, index)
            depth += 1
        elif character == "}":
            if depth == 0:
                return None
            depth -= 1
    return None


def production_text(source):
    """Blank out `#[cfg(test)]` items wherever they sit in the file.

    Only whole items are blanked: modules, functions, impls, types, traits,
    `use`, `const`, `static` and `type`. An attribute on a field, statement,
    expression or match arm blanks nothing, so the scan never hides the
    production code that follows it. Line breaks are kept so reported line
    numbers stay accurate."""
    text = source
    position = 0
    while True:
        match = CFG_TEST.search(text, position)
        if not match:
            return text
        index = match.end()
        while True:
            attribute = ATTRIBUTE.match(text, index)
            if not attribute:
                break
            index = attribute.end()
        item = ITEM_START.match(text, index)
        end = item_end(text, item.start(1), item.group(1)) if item else None
        if end is None:
            position = match.end()
            continue
        blank = re.sub(r"[^\n]", " ", text[match.start() : end])
        text = text[: match.start()] + blank + text[end:]
        position = match.start() + len(blank)


def test_module_paths(root, relative, source):
    """Files declared as `#[cfg(test)] mod name;` (or with `#[path]`)."""
    path = root / relative
    stem = path.stem
    base = path.parent if stem in ("mod", "lib", "main") else path.parent / stem
    found = []
    for match in CFG_TEST.finditer(source):
        index = match.end()
        declared = None
        while True:
            attribute = ATTRIBUTE.match(source, index)
            if not attribute:
                break
            explicit = PATH_ATTRIBUTE.search(attribute.group(0))
            if explicit:
                declared = path.parent / explicit.group(1)
            index = attribute.end()
        module = MOD_DECLARATION.match(source, index)
        if not module:
            continue
        if declared is None:
            name = module.group(1)
            candidate = base / f"{name}.rs"
            declared = candidate if candidate.exists() else base / name / "mod.rs"
        found.append(declared.resolve())
        # A test module's own submodules are test code too.
        found.append((declared.parent / declared.stem).resolve())
    return found


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
    for match in TASK_POINT_FREE.finditer(text):
        name = match.group(1)
        short = name.rsplit("::", 1)[-1]
        if TYPED_POINT_FREE.search(name) or short in POINT_FREE_ALLOWLIST:
            continue
        line = text.count("\n", 0, match.start()) + 1
        found.append(
            f"{relative}:{line}: point-free `{name}` may flatten a task stop; "
            "map it to RuntimeTaskStopped or allowlist it with a reason"
        )
    for match in FORMAT_CLOSURE.finditer(text):
        name, annotation = match.group(1), (match.group(2) or "").strip()
        body = closure_body(text, text.rfind("(", 0, match.end()) + 1)
        if "or_stop(" in body or annotation in FORMAT_SAFE_TYPES:
            continue
        formatted = re.search(
            r"HawDBError::(Execution|Storage)\(\s*format!\([^;]*\{"
            + re.escape(name)
            + r"(?::[^}]*)?\}",
            body,
            re.S,
        )
        if not formatted:
            continue
        if any(
            path == relative and fragment in body for path, fragment in FORMAT_ALLOWLIST
        ):
            continue
        line = text.count("\n", 0, match.start()) + 1
        found.append(
            f"{relative}:{line}: {formatted.group(1)}(format!(..{{{name}}}..)) can hide a stop; "
            "wrap it in HawDBError::or_stop"
        )
    for match in STOP_ARM.finditer(text):
        body = arm_body(text, match.end())
        if "or_stop(" in body:
            continue
        variant = text_variant(body)
        if variant and not allowed(relative, text, match.start(), variant):
            line = text.count("\n", 0, match.start()) + 1
            found.append(f"{relative}:{line}: stop arm reports {variant} text")
    return found


def scan(root):
    found = []
    sources = []
    test_paths = set()
    for source_root in SOURCE_ROOTS:
        paths = sorted((root / source_root).rglob("*.rs"))
        if not paths:
            raise ValueError(f"missing source coverage: {source_root}")
        for path in paths:
            relative = path.relative_to(root).as_posix()
            source = path.read_text(encoding="utf-8")
            sources.append((path, relative, source))
            test_paths.update(test_module_paths(root, relative, source))
    for path, relative, source in sources:
        resolved = path.resolve()
        if is_test_source(relative) or resolved in test_paths:
            continue
        if any(parent in test_paths for parent in resolved.parents):
            continue
        found.extend(violations_in(relative, source))
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
