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
  `check_deadline_at(..)`), admitted work/allocation operation, or I/O
  admission (`acquire_io_wave(..)`, `io_wave()`, `io()`) whose `map_err`
  closure builds `Admission`, `Execution` or `Storage`;
- a `Stopped(..)` / `Cancelled(..)` match arm (a bound name, `_` or `..`,
  including rustfmt-wrapped forms) whose body builds `Execution` or `Storage`,
  whatever message it carries;
- a `map_err(|error| HawDBError::Execution(error.to_string()))` closure, which
  would hide a stop carried by `error`; use
  `HawDBError::from_execution_error`.

Stops observed after a durable rename must become the typed
`PublishOutcomeUnknown` error instead of either a rollback-safe task stop or
unstructured `Storage` text. `STORAGE_ALLOWLIST` therefore remains empty.
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
    r"(?:checkpoint\(\)|start_unit\(\)|check_cancellation\(\)|check_deadline_at\([^()]*\)"
    r"|acquire_io_wave\([^()]*\)|\bio_wave\(\)|\.io\(\))"
    r"\s*\.map_err\(\s*\|(\w+)\|"
)
TASK_RESULT_CALL = re.compile(
    r"\.(?:reserve|vector_capacity|string|string_capacity|push|before_insert|"
    r"to_owned_value_with_allocations|append|append_retained|integrity)\("
)
MAP_ERR_CLOSURE = re.compile(r"\s*\.map_err\(\s*\|(\w+)\|")
STRINGIFIED_EXECUTION = re.compile(
    r"\.map_err\(\s*\|(\w+)\|\s*\{?\s*(?:[\w:]+::)?HawDBError::Execution\(\s*\1\.to_string\(\)\s*\)"
)
# Point-free conversions of a task check that keep the stop typed (or keep it
# in the cause chain, which `HawDBError::from_storage_error`,
# `from_execution_error` and `or_stop` read). Constructors of a typed stop
# variant and the typed helpers are recognised by name; anything else must be
# listed in `KNOWN_POINT_FREE` with its precondition.
TYPED_POINT_FREE = re.compile(
    r"(?:^|::)(?:Stopped|Cancelled|RuntimeTaskStopped|from_storage_error|from_execution_error)$"
)
KNOWN_POINT_FREE = {
    # Every `From<_> for HawDBError` maps a stop to `RuntimeTaskStopped` or has
    # no stop to carry: RuntimeIoWaveError, RuntimeCancellationReason,
    # ResourceBudgetError, io::Error (via from_storage_error),
    # RuntimeAdmissionError, BranchLifecycleError, LockAcquireError.
    "HawDBError::from": "every From<_> for HawDBError keeps a stop typed",
    "crate::HawDBError::from": "every From<_> for HawDBError keeps a stop typed",
    "hawdb_core::HawDBError::from": "every From<_> for HawDBError keeps a stop typed",
    # The stop stays in the io::Error payload, which the chain walk reads.
    "io::Error::other": "payload read through io::Error::get_ref",
    "std::io::Error::other": "payload read through io::Error::get_ref",
    # `CheckpointWorkError -> CanonicalSegmentError::Work` keeps the work error.
    "Into::into": "canonical decoder: CheckpointWorkError -> CanonicalSegmentError::Work",
    # Wrappers that hold the original typed error and expose it as source.
    "IndexReadPreflightError::Failed": "holds the HawDBError",
    "BranchLifecycleError::Runtime": "holds the HawDBError",
    "CheckpointWorkError::Io": "holds the RuntimeIoWaveError, exposed as source",
    "CheckpointOperationError::Work": "holds the CheckpointWorkError without flattening it",
    "map_runtime_io_error": "maps RuntimeIoWaveError::Stopped to its Stopped variant",
    "canonical_segment_error": "routes through HawDBError::from_storage_error",
    # Every CheckpointWorkError work_error helper now calls stop_reason() and
    # constructs its internal typed Stopped variant before handling admission.
    "work_error": "all checkpoint work-error mappings preserve the typed stop",
    "block_work_error": "immutable_object::block_work_error maps stop_reason() to ImmutableObjectError::Stopped; non-stop admission errors retain their typed source",
    "RelationalRowDeltaError::from_work": "maps stop_reason() to RelationalRowDeltaError::Stopped",
    "RelationalError::from_work": "maps stop_reason() to RelationalError::Stopped",
    "super::RelationalError::from_work": "maps stop_reason() to RelationalError::Stopped",
    "crate::relational::RelationalError::from_work": "maps stop_reason() to RelationalError::Stopped",
    "OrderedRelationalKeyError::from_work": "maps stop_reason() to OrderedRelationalKeyError::Stopped",
}
# Point-free conversions that flatten a stop into internal text on purpose.
POINT_FREE_ALLOWLIST = {
    # Every production `work_error(CheckpointWorkError)` definition is scanned
    # below and must inspect stop_reason() before building admission text.
    "work_error": "definition is checked for typed stop preservation",
    # `CheckpointBuffer` takes the caller's own work-error conversion.
    "map_work": "generic caller-supplied conversion",
}
TASK_POINT_FREE = re.compile(
    r"(?:checkpoint\(\)|start_unit\(\)|check_cancellation\(\)|check_deadline_at\([^()]*\)"
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
TYPED_STOP_ARM = re.compile(
    r"\b(?:Stopped|Cancelled)\s*\(\s*\w+\s*\)\s*=>\s*"
    r"(?:[\w:]+::)?(?:Stopped|Cancelled|RuntimeTaskStopped)\s*\("
)
# Internal storage `Admission(String)` wrappers also become public `Storage`
# errors. Scan them alongside the public text variants so a typed stop cannot
# disappear one layer before the HawDBError boundary.
TEXT_VARIANT = re.compile(r"\b(Admission|Execution|Storage)\(")
WORK_ERROR_DEFINITION = re.compile(
    r"fn\s+(?:[A-Za-z_]\w*_)?work_error\s*\(\s*error\s*:\s*"
    r"(?:[A-Za-z_]\w*::)*CheckpointWorkError\s*,?\s*\)[^{]*\{"
)


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


def mask(source):
    """`source` with comments and the contents of string, raw-string, byte and
    char literals replaced by spaces, keeping every offset and line break.

    Brace and paren counting runs on this text so `b'{'`, `"}"`, `r#"{"#`,
    `// {` or `/* } */` can never unbalance an extent."""
    out = list(source)
    length = len(source)

    def blank(begin, end):
        for index in range(begin, min(end, length)):
            if out[index] != "\n":
                out[index] = " "

    index = 0
    while index < length:
        character = source[index]
        following = source[index + 1] if index + 1 < length else ""
        if character == "/" and following == "/":
            end = source.find("\n", index)
            end = length if end == -1 else end
            blank(index, end)
            index = end
        elif character == "/" and following == "*":
            depth, end = 1, index + 2
            while end < length and depth:
                if source.startswith("/*", end):
                    depth, end = depth + 1, end + 2
                elif source.startswith("*/", end):
                    depth, end = depth - 1, end + 2
                else:
                    end += 1
            blank(index, end)
            index = end
        elif (
            character in "rb"
            and (index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_"))
            and re.match(r'(?:br|r)#*"', source[index:])
        ):
            opening = re.match(r'(?:br|r)(#*)"', source[index:])
            closing = '"' + opening.group(1)
            end = source.find(closing, index + opening.end())
            end = length if end == -1 else end + len(closing)
            blank(index, end)
            index = end
        elif character == '"' or (
            character == "b"
            and following == '"'
            and (index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_"))
        ):
            end = index + (2 if character == "b" else 1)
            while end < length and source[end] != '"':
                end += 2 if source[end] == "\\" else 1
            blank(index, end + 1)
            index = end + 1
        elif character == "'" or (
            character == "b"
            and following == "'"
            and (index == 0 or not (source[index - 1].isalnum() or source[index - 1] == "_"))
        ):
            start = index + (2 if character == "b" else 1)
            literal = re.match(r"(?:\\(?:u\{[0-9a-fA-F]{1,6}\}|x[0-9a-fA-F]{2}|.)|[^\\\n])'", source[start:])
            if literal:
                blank(index, start + literal.end())
                index = start + literal.end()
            else:
                # A lifetime or label: leave it.
                index += 1
        else:
            index += 1
    return "".join(out)


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


def balanced_paren_end(source, open_index):
    """Index just past the `)` closing the `(` at `open_index`."""
    depth = 0
    for index in range(open_index, len(source)):
        if source[index] == "(":
            depth += 1
        elif source[index] == ")":
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
    production code that follows it. Extents are measured on the masked text
    (comments and literals blanked). Line breaks are kept so reported line
    numbers stay accurate."""
    text = source
    structure = mask(source)
    position = 0
    while True:
        match = CFG_TEST.search(structure, position)
        if not match:
            return text
        index = match.end()
        while True:
            attribute = ATTRIBUTE.match(structure, index)
            if not attribute:
                break
            index = attribute.end()
        item = ITEM_START.match(structure, index)
        end = item_end(structure, item.start(1), item.group(1)) if item else None
        if end is None:
            position = match.end()
            continue
        blank = re.sub(r"[^\n]", " ", text[match.start() : end])
        text = text[: match.start()] + blank + text[end:]
        structure = structure[: match.start()] + blank + structure[end:]
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


def closure_body(source, start, structure=None):
    """Text of the `map_err(` call opened just before `start`."""
    structure = source if structure is None else structure
    depth = 1
    index = start
    while index < len(structure) and depth:
        if structure[index] == "(":
            depth += 1
        elif structure[index] == ")":
            depth -= 1
        index += 1
    return source[start:index]


def arm_body(source, start, structure=None):
    """The match arm expression after `=>`: its block, or up to the comma."""
    structure = source if structure is None else structure
    index = start
    while index < len(structure) and structure[index].isspace():
        index += 1
    if index < len(structure) and structure[index] == "{":
        depth = 0
        for end in range(index, len(structure)):
            if structure[end] == "{":
                depth += 1
            elif structure[end] == "}":
                depth -= 1
                if depth == 0:
                    return source[index : end + 1]
        return source[index:]
    depth = 0
    index = start
    while index < len(structure):
        character = structure[index]
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
    structure = mask(text)

    def code(match):
        # Ignore matches that start inside a comment or a literal.
        return structure[match.start()] == text[match.start()]

    found = []
    for match in WORK_ERROR_DEFINITION.finditer(text):
        if not code(match):
            continue
        body = closure_body(text, match.end() - 1, structure)
        if "stop_reason()" not in body:
            line = text.count("\n", 0, match.start()) + 1
            found.append(
                f"{relative}:{line}: work_error flattens CheckpointWorkError without preserving stop_reason()"
            )
    for match in TASK_CHECK.finditer(text):
        if not code(match):
            continue
        body = closure_body(text, match.end(), structure)
        # A fallible I/O admission may have both a typed stop arm and a
        # capacity-refusal Admission arm. The latter does not flatten the stop.
        if TYPED_STOP_ARM.search(body):
            continue
        variant = text_variant(body)
        if variant and not allowed(relative, text, match.start(), variant):
            line = text.count("\n", 0, match.start()) + 1
            found.append(f"{relative}:{line}: task check maps its stop to {variant}")
    for match in TASK_RESULT_CALL.finditer(text):
        if not code(match):
            continue
        call_end = balanced_paren_end(structure, match.end() - 1)
        mapping = MAP_ERR_CLOSURE.match(structure, call_end)
        if not mapping:
            continue
        body = closure_body(text, mapping.end(), structure)
        if TYPED_STOP_ARM.search(body) or "or_stop(" in body:
            continue
        variant = text_variant(body)
        format_allowed = any(
            path == relative and fragment in body for path, fragment in FORMAT_ALLOWLIST
        )
        if variant and not format_allowed and not allowed(relative, text, match.start(), variant):
            line = text.count("\n", 0, match.start()) + 1
            found.append(
                f"{relative}:{line}: admitted operation maps a possible stop to {variant}"
            )
    for match in STRINGIFIED_EXECUTION.finditer(text):
        if not code(match):
            continue
        line = text.count("\n", 0, match.start()) + 1
        found.append(
            f"{relative}:{line}: Execution(error.to_string()) can hide a stop; "
            "use HawDBError::from_execution_error"
        )
    for match in TASK_POINT_FREE.finditer(text):
        if not code(match):
            continue
        name = match.group(1)
        short = name.rsplit("::", 1)[-1]
        if TYPED_POINT_FREE.search(name) or name in KNOWN_POINT_FREE or short in POINT_FREE_ALLOWLIST:
            continue
        line = text.count("\n", 0, match.start()) + 1
        found.append(
            f"{relative}:{line}: point-free `{name}` may flatten a task stop; "
            "map it to RuntimeTaskStopped or allowlist it with a reason"
        )
    for match in FORMAT_CLOSURE.finditer(text):
        if not code(match):
            continue
        name, annotation = match.group(1), (match.group(2) or "").strip()
        body = closure_body(text, text.rfind("(", 0, match.end()) + 1, structure)
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
        if not code(match):
            continue
        body = arm_body(text, match.end(), structure)
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
            "report a stopped runtime task as a typed Stopped/RuntimeTaskStopped reason, "
            "not as Admission, Execution or Storage text (#1850, #1856)",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
