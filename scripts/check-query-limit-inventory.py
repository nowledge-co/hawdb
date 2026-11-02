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

"""Require reviewed classifications for query truncation report additions."""

import argparse
from collections import Counter
import json
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
    "crates/vector-projection/src",
    "crates/nowledge-contracts/src",
    "crates/readiness/src",
)
FIELD = re.compile(r"\bpub(?:\([^)]*\))?\s+(\w*truncat\w*)\s*:")
ENUM = re.compile(r"\bpub(?:\([^)]*\))?\s+enum\s+(\w*Truncation\w*)\s*\{")
VARIANT = re.compile(r"^[A-Za-z_][A-Za-z_0-9]*$")


def observe(root):
    """Read declarations only; do not infer runtime control flow from names."""
    fields = Counter()
    enums = {}
    for source_root in SOURCE_ROOTS:
        directory = root / source_root
        sources = sorted(directory.rglob("*.rs"))
        if not sources:
            raise ValueError(f"missing source coverage: {source_root}")
        for path in sources:
            relative = path.relative_to(root).as_posix()
            source = path.read_text(encoding="utf-8")
            for match in FIELD.finditer(source):
                fields[(relative, match.group(1))] += 1
            for match in ENUM.finditer(source):
                end = source.find("}", match.end())
                body = source[match.end():end] if end >= 0 else "{"
                body = re.sub(r"//[^\n]*", "", body)
                variants = [value.strip() for value in body.split(",") if value.strip()]
                if not variants or not all(VARIANT.fullmatch(value) for value in variants):
                    raise ValueError(
                        f"review unsupported truncation enum shape: {relative}:{match.group(1)}"
                    )
                key = (relative, match.group(1))
                if key in enums:
                    raise ValueError(f"duplicate truncation enum declaration: {key}")
                enums[key] = variants
    return fields, enums


def verify(root, inventory):
    if inventory.get("source_roots") != list(SOURCE_ROOTS):
        raise ValueError("inventory source roots must match the complete checked scope")
    expected_fields = Counter()
    expected_enums = {}
    for entry in inventory["reports"]:
        if entry["classification"] not in {"semantic", "resource_failure", "diagnostic"}:
            raise ValueError(f"unclassified report: {entry}")
        if not entry.get("rationale", "").strip() or not entry.get("stage", "").strip():
            raise ValueError(f"missing report rationale or stage: {entry}")
        key = (entry["path"], entry["name"])
        if entry["kind"] == "field":
            count = entry.get("count", 1)
            if not isinstance(count, int) or isinstance(count, bool) or count < 1:
                raise ValueError(f"invalid field count: {entry}")
            if key in expected_fields:
                raise ValueError(f"duplicate field inventory: {key}")
            expected_fields[key] = count
        elif entry["kind"] == "enum":
            if key in expected_enums:
                raise ValueError(f"duplicate enum inventory: {key}")
            expected_enums[key] = entry["variants"]
        else:
            raise ValueError(f"unknown inventory record kind: {entry['kind']}")
    actual_fields, actual_enums = observe(root)
    missing = actual_fields - expected_fields
    stale = expected_fields - actual_fields
    if missing or stale or actual_enums != expected_enums:
        raise ValueError(
            "query truncation reports changed; review the producer and update its "
            "classification in docs/QUERY_LIMIT_INVENTORY.md and its JSON inventory. "
            f"Added fields: {dict(missing)}; removed fields: {dict(stale)}; "
            f"enum declarations match: {actual_enums == expected_enums}"
        )
    return sum(actual_fields.values()), len(actual_enums)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    root = Path(__file__).absolute().parent.parent
    if "TEST_SRCDIR" in os.environ:
        root = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
    parser.add_argument("--root", type=Path, default=root)
    args = parser.parse_args()
    try:
        inventory = json.loads((args.root / "docs/query-limit-reports.json").read_text())
        fields, enums = verify(args.root, inventory)
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(error, file=sys.stderr)
        return 1
    print(f"Reviewed truncation report inventory: {fields} fields, {enums} enums")
    return 0


if __name__ == "__main__":
    sys.exit(main())
