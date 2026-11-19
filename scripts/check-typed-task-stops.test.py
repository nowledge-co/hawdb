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

"""Negative and positive controls for the typed task stop source scan."""

import importlib.util
import os
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("check-typed-task-stops.py")
if not SCRIPT.exists():
    SCRIPT = (
        Path(os.environ["TEST_SRCDIR"])
        / os.environ["TEST_WORKSPACE"]
        / "scripts/check-typed-task-stops.py"
    )
SPEC = importlib.util.spec_from_file_location("typed_task_stops", SCRIPT)
CHECK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(CHECK)


class TypedTaskStopsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for source_root in CHECK.SOURCE_ROOTS:
            (self.root / source_root).mkdir(parents=True)
            (self.root / source_root / "lib.rs").write_text("fn ok() {}\n")

    def write(self, relative, text):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    def test_clean_sources_pass(self):
        self.write(
            "crates/search/src/query.rs",
            "fn check(task: &Task) -> Result<()> {\n"
            "    task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)\n"
            "}\n"
            "fn other(error: E) -> HawDBError {\n"
            "    match error {\n"
            "        E::Stopped(reason) => HawDBError::RuntimeTaskStopped(reason),\n"
            "        E::Io(error) => HawDBError::Execution(format!(\"io {error}\")),\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(CHECK.scan(self.root), [])

    def test_task_check_mapped_to_execution_is_refused(self):
        self.write(
            "crates/search/src/query.rs",
            "fn check(task: &Task) -> Result<()> {\n"
            "    task.checkpoint()\n"
            "        .map_err(|reason| HawDBError::Execution(format!(\"lexical query {reason}\")))\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            ["crates/search/src/query.rs:2: task check maps its stop to Execution"],
        )

    def test_multiline_closure_and_to_string_are_refused(self):
        self.write(
            "src/session.rs",
            "fn check(task: &Task) -> Result<()> {\n"
            "    task.check_deadline_at(now).map_err(|error| {\n"
            "        HawDBError::Execution(error.to_string())\n"
            "    })\n"
            "}\n",
        )
        self.assertEqual(len(CHECK.scan(self.root)), 1)

    def test_stop_arm_formatted_into_execution_is_refused(self):
        self.write(
            "crates/storage/src/replay.rs",
            "fn map(error: E) -> HawDBError {\n"
            "    match error {\n"
            "        E::Stopped(reason) => {\n"
            "            HawDBError::Execution(format!(\"replay stopped: {reason}\"))\n"
            "        }\n"
            "        error => HawDBError::from_storage_error(error),\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            ["crates/storage/src/replay.rs:3: stop reason formatted into Execution"],
        )

    def test_test_sources_and_trailing_test_modules_are_skipped(self):
        bad = "fn t(task: &Task) { task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string())); }\n"
        self.write("crates/search/src/tests.rs", bad)
        self.write("crates/search/src/scan_tests.rs", bad)
        self.write("crates/search/tests/integration.rs", bad)
        self.write(
            "crates/search/src/module.rs",
            "fn ok() {}\n\n#[cfg(test)]\nmod tests {\n    " + bad + "}\n",
        )
        self.assertEqual(CHECK.scan(self.root), [])

    def test_missing_source_root_is_refused(self):
        (self.root / "crates/storage/src/lib.rs").unlink()
        with self.assertRaises(ValueError):
            CHECK.scan(self.root)


if __name__ == "__main__":
    unittest.main()
