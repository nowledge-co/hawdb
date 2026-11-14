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

"""Negative controls for the query report classification gate."""

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check-query-limit-inventory.py")
if not SCRIPT.exists():
    SCRIPT = (
        Path(os.environ["TEST_SRCDIR"])
        / os.environ["TEST_WORKSPACE"]
        / "scripts/check-query-limit-inventory.py"
    )
SPEC = importlib.util.spec_from_file_location("query_limit_inventory", SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class InventoryTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for source_root in CHECK.SOURCE_ROOTS:
            path = self.root / source_root / "lib.rs"
            path.parent.mkdir(parents=True)
            path.write_text("pub struct Empty;\n")
        self.inventory = {"source_roots": list(CHECK.SOURCE_ROOTS), "reports": []}

    def test_new_partial_success_report_requires_review(self):
        path = self.root / "crates/executor/src/new_stage.rs"
        path.write_text("pub struct Output {\n    pub truncation_reason: Option<String>,\n}\n")
        with self.assertRaisesRegex(ValueError, "Added fields"):
            CHECK.verify(self.root, self.inventory)

    def test_changed_existing_reason_enum_requires_review(self):
        path = self.root / "crates/search/src/lib.rs"
        path.write_text("pub enum SearchTruncationReasonCode { LimitExceeded, }\n")
        self.inventory["reports"].append({
            "kind": "enum", "path": "crates/search/src/lib.rs",
            "name": "SearchTruncationReasonCode", "variants": ["LimitExceeded"],
            "classification": "semantic", "stage": "search-page",
            "rationale": "Caller-selected page window.",
        })
        CHECK.verify(self.root, self.inventory)
        path.write_text(
            "pub enum SearchTruncationReasonCode { LimitExceeded, MemoryExhausted, }\n"
        )
        with self.assertRaisesRegex(ValueError, "enum declarations match: False"):
            CHECK.verify(self.root, self.inventory)

    def test_inline_and_restricted_visibility_fields_require_review(self):
        path = self.root / "crates/executor/src/lib.rs"
        path.write_text("pub struct Output { pub(crate) new_truncated: bool }\n")
        with self.assertRaisesRegex(ValueError, "new_truncated"):
            CHECK.verify(self.root, self.inventory)

    def test_missing_sources_cannot_produce_a_false_green(self):
        (self.root / "crates/executor/src/lib.rs").unlink()
        with self.assertRaisesRegex(ValueError, "missing source coverage"):
            CHECK.verify(self.root, self.inventory)

    def test_removed_fields_require_inventory_cleanup(self):
        self.inventory["reports"].append({
            "kind": "field", "path": "crates/search/src/lib.rs", "name": "truncated",
            "classification": "semantic", "stage": "search-page",
            "rationale": "Caller-selected page window.",
        })
        with self.assertRaisesRegex(ValueError, "removed fields"):
            CHECK.verify(self.root, self.inventory)

    def test_unclassified_entries_are_rejected(self):
        self.inventory["reports"].append({"classification": "unknown"})
        with self.assertRaisesRegex(ValueError, "unclassified report"):
            CHECK.verify(self.root, self.inventory)


if __name__ == "__main__":
    unittest.main()
