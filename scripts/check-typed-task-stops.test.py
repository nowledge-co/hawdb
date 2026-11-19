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
            [
                "crates/search/src/query.rs:2: task check maps its stop to Execution",
                "crates/search/src/query.rs:3: Execution(format!(..{reason}..)) can hide a stop; "
                "wrap it in HawDBError::or_stop",
            ],
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
        # Both the task-check rule and the stringified-error rule apply.
        self.assertEqual(len(CHECK.scan(self.root)), 2)

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
            ["crates/storage/src/replay.rs:3: stop arm reports Execution text"],
        )

    def test_wildcard_stop_arm_with_prebuilt_message_is_refused(self):
        self.write(
            "crates/storage/src/overflow.rs",
            "fn map(error: E) -> HawDBError {\n"
            "    let message = error.to_string();\n"
            "    match error {\n"
            "        E::Corrupt(_) => HawDBError::StorageIntegrity(message),\n"
            "        E::Stopped(_) => {\n"
            "            HawDBError::Execution(message)\n"
            "        }\n"
            "        E::Stale { .. } => HawDBError::Storage(message),\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            ["crates/storage/src/overflow.rs:5: stop arm reports Execution text"],
        )

    def test_struct_and_rest_stop_arms_are_refused(self):
        self.write(
            "crates/search/src/vector.rs",
            "fn map(error: E) -> HawDBError {\n"
            "    match error {\n"
            "        E::Cancelled { .. } => HawDBError::Execution(\"stopped\".into()),\n"
            "        E::Stopped(..) => HawDBError::Storage(error.to_string()),\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            [
                "crates/search/src/vector.rs:3: stop arm reports Execution text",
                "crates/search/src/vector.rs:4: stop arm reports Storage text",
            ],
        )

    def test_task_check_mapped_to_storage_is_refused_unless_allowlisted(self):
        source = (
            "fn check(task: &Task) -> Result<()> {\n"
            "    task.checkpoint().map_err(|reason| HawDBError::Storage(reason.to_string()))\n"
            "}\n"
        )
        self.write("crates/storage/src/maintenance.rs", source)
        self.assertEqual(
            CHECK.scan(self.root),
            ["crates/storage/src/maintenance.rs:2: task check maps its stop to Storage"],
        )
        allowlist = CHECK.STORAGE_ALLOWLIST
        CHECK.STORAGE_ALLOWLIST = frozenset(
            {
                (
                    "crates/storage/src/maintenance.rs",
                    "task.checkpoint().map_err(|reason| HawDBError::Storage(reason.to_string()))",
                )
            }
        )
        try:
            self.assertEqual(CHECK.scan(self.root), [])
        finally:
            CHECK.STORAGE_ALLOWLIST = allowlist

    def test_block_arm_does_not_leak_into_the_next_arm(self):
        self.write(
            "src/transactions.rs",
            "fn map(error: E) -> HawDBError {\n"
            "    match error {\n"
            "        E::Stopped(reason) => {\n"
            "            HawDBError::RuntimeTaskStopped(reason)\n"
            "        }\n"
            "        error @ E::Admission(_) => HawDBError::Execution(error.to_string()),\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(CHECK.scan(self.root), [])

    def test_io_admission_mapped_to_execution_is_refused(self):
        self.write(
            "crates/search/src/consumer.rs",
            "fn read(task: &Task, admission: &Admission, work: &Work) -> Result<()> {\n"
            "    let _io = task\n"
            "        .acquire_io_wave(NonZeroUsize::MIN)\n"
            "        .map_err(|error| HawDBError::Execution(format!(\"io {error}\")))?;\n"
            "    let _io = admission.io().map_err(|error| HawDBError::Storage(error.to_string()))?;\n"
            "    let _io = work.io_wave().map_err(|error| HawDBError::Execution(error.to_string()))?;\n"
            "    let _io = admission.io().map_err(HawDBError::from)?;\n"
            "    Ok(())\n"
            "}\n",
        )
        found = CHECK.scan(self.root)
        self.assertIn(
            "crates/search/src/consumer.rs:3: task check maps its stop to Execution", found
        )
        self.assertIn(
            "crates/search/src/consumer.rs:5: task check maps its stop to Storage", found
        )
        self.assertIn(
            "crates/search/src/consumer.rs:6: task check maps its stop to Execution", found
        )
        self.assertFalse(any(":7:" in violation for violation in found), found)

    def test_rustfmt_wrapped_stop_arm_is_refused(self):
        self.write(
            "crates/storage/src/rows.rs",
            "fn map(error: E) -> HawDBError {\n"
            "    match error {\n"
            "        hawdb_storage::relational::RelationalRowPageSnapshotReadError::Stopped(\n"
            "            reason,\n"
            "        ) => crate::error::HawDBError::Execution(format!(\"{reason}\")),\n"
            "        _ => crate::error::HawDBError::Storage(error.to_string()),\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            ["crates/storage/src/rows.rs:3: stop arm reports Execution text"],
        )

    def test_stringified_execution_closure_is_refused(self):
        self.write(
            "crates/executor/src/spill.rs",
            "fn read(reader: &mut R) -> Result<()> {\n"
            "    reader.read(&mut []).map_err(|error| HawDBError::Execution(error.to_string()))?;\n"
            "    reader.read(&mut []).map_err(HawDBError::from_execution_error)?;\n"
            "    Ok(())\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            [
                "crates/executor/src/spill.rs:2: Execution(error.to_string()) can hide a stop; "
                "use HawDBError::from_execution_error"
            ],
        )

    def test_cfg_test_items_anywhere_and_declared_test_files_are_skipped(self):
        bad = "fn t(task: &Task) { task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string())); }\n"
        self.write(
            "crates/search/src/reader.rs",
            "fn ok() {}\n"
            "#[cfg(test)]\n"
            "mod inline {\n    " + bad + "}\n"
            "#[cfg(test)]\n"
            "mod reader_checks;\n"
            "#[cfg(test)]\n"
            '#[path = "reader/oracle.rs"]\n'
            "mod oracle;\n"
            "fn production(task: &Task) -> Result<()> {\n"
            "    task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)\n"
            "}\n",
        )
        self.write("crates/search/src/reader/reader_checks.rs", bad)
        self.write("crates/search/src/reader/reader_checks/nested.rs", bad)
        self.write("crates/search/src/reader/oracle.rs", bad)
        self.assertEqual(CHECK.scan(self.root), [])
        # The same code outside a test item is still found, at its real line.
        self.write("crates/search/src/later.rs", "fn ok() {}\n#[cfg(test)]\nfn helper() {}\n" + bad)
        self.assertEqual(len(CHECK.scan(self.root)), 2)
        self.assertTrue(all(v.startswith("crates/search/src/later.rs:4:") for v in CHECK.scan(self.root)))

    def test_cfg_test_on_fields_statements_and_arms_hides_nothing(self):
        bad = "task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string()))"
        self.write(
            "crates/storage/src/field.rs",
            "struct Reader {\n"
            "    #[cfg(test)]\n"
            "    probe: Probe,\n"
            "    task: Task,\n"
            "}\n"
            "fn read(task: &Task) -> Result<()> {\n"
            "    " + bad + "?;\n"
            "    Ok(())\n"
            "}\n",
        )
        self.write(
            "crates/storage/src/statement.rs",
            "fn read(task: &Task) -> Result<()> {\n"
            "    #[cfg(test)]\n"
            "    hooks::before_read();\n"
            "    " + bad + "?;\n"
            "    #[cfg(test)]\n"
            "    let observed = 1;\n"
            "    " + bad + "?;\n"
            "    Ok(())\n"
            "}\n",
        )
        self.write(
            "crates/storage/src/arm.rs",
            "fn map(task: &Task, error: E) -> Result<()> {\n"
            "    match error {\n"
            "        #[cfg(test)]\n"
            "        E::Injected => {}\n"
            "        _ => {}\n"
            "    }\n"
            "    " + bad + "?;\n"
            "    Ok(())\n"
            "}\n",
        )
        lines = sorted(v.split(": ", 1)[0] for v in CHECK.scan(self.root))
        self.assertIn("crates/storage/src/field.rs:7", lines)
        self.assertIn("crates/storage/src/statement.rs:4", lines)
        self.assertIn("crates/storage/src/statement.rs:7", lines)
        self.assertIn("crates/storage/src/arm.rs:7", lines)

    def test_cfg_test_field_does_not_hide_the_following_impl(self):
        self.write(
            "src/api/coordinator.rs",
            "pub struct Coordinator {\n"
            "    #[cfg(test)]\n"
            "    hook: Hook,\n"
            "    state: State,\n"
            "}\n"
            "impl Coordinator {\n"
            "    fn run(&self, task: &Task) -> Result<()> {\n"
            "        task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string()))\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(
            sorted({v.split(": ", 1)[0] for v in CHECK.scan(self.root)}),
            ["src/api/coordinator.rs:8"],
        )

    def test_generic_from_and_into_are_not_assumed_typed(self):
        self.write(
            "crates/storage/src/convert.rs",
            "fn read(task: &Task) -> Result<()> {\n"
            "    task.checkpoint().map_err(HawDBError::from)?;\n"
            "    task.checkpoint().map_err(ReaderError::from)?;\n"
            "    task.checkpoint().map_err(SomeError::into)?;\n"
            "    Ok(())\n"
            "}\n",
        )
        self.assertEqual(
            [v.split(": ", 1)[0] for v in CHECK.scan(self.root)],
            ["crates/storage/src/convert.rs:3", "crates/storage/src/convert.rs:4"],
        )

    def test_braces_in_literals_and_comments_do_not_unbalance_test_items(self):
        bad = "task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string()))"
        for index, noise in enumerate(
            [
                "let _ = b'{';",
                "let _ = '}';",
                'let _ = "{";',
                'let _ = r#"}"#;',
                'let _ = br"{";',
                "let _ = '\\u{7b}';",
                "let _ = '\\'';",
                "// {",
                "/* } /* nested { */ */",
            ]
        ):
            path = f"crates/search/src/literal_{index}.rs"
            self.write(
                path,
                "#[cfg(test)]\n"
                "fn helper() {\n"
                f"    {noise}\n"
                "}\n"
                "fn production(task: &Task) -> Result<()> {\n"
                f"    {bad}\n"
                "}\n",
            )
            self.assertEqual(
                sorted({v.split(": ", 1)[0] for v in CHECK.scan(self.root)}),
                [f"{path}:6"],
                noise,
            )
            (self.root / path).unlink()

    def test_lifetimes_and_loop_labels_are_not_char_literals(self):
        bad = "task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string()))"
        self.write(
            "crates/storage/src/lifetimes.rs",
            "#[cfg(test)]\n"
            "fn helper<'a>(value: &'a str) -> Cow<'_, str> { value.into() }\n"
            "fn scan<'a, T: 'static>(rows: &'a mut Vec<T>, task: &Task) -> Result<()> {\n"
            "    'outer: for row in rows.iter() {\n"
            "        loop { break 'outer; }\n"
            "    }\n"
            "    let c = 'x';\n"
            "    " + bad + "\n"
            "}\n",
        )
        self.assertEqual(
            sorted({v.split(": ", 1)[0] for v in CHECK.scan(self.root)}),
            ["crates/storage/src/lifetimes.rs:8"],
        )

    def test_matches_inside_comments_and_strings_are_ignored(self):
        bad = "task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string()))"
        self.write(
            "crates/search/src/docs.rs",
            "/// Never write `" + bad + "`.\n"
            "const EXAMPLE: &str = \"" + bad.replace('"', '\\"') + "\";\n"
            "fn ok() {}\n",
        )
        self.assertEqual(CHECK.scan(self.root), [])

    def test_cfg_test_items_are_blanked_to_their_extent(self):
        bad = "task.checkpoint().map_err(|r| HawDBError::Execution(r.to_string()))"
        self.write(
            "crates/search/src/items.rs",
            "#[cfg(test)]\n"
            "use crate::hooks;\n"
            "#[cfg(test)]\n"
            "const LIMIT: Limits = Limits { rows: 1 };\n"
            "#[cfg(test)]\n"
            "impl Reader {\n"
            "    fn t(task: &Task) { " + bad + "; }\n"
            "}\n"
            "#[cfg(test)]\n"
            "pub(crate) fn helper(task: &Task) { " + bad + "; }\n"
            "fn production(task: &Task) -> Result<()> {\n"
            "    " + bad + "\n"
            "}\n",
        )
        self.assertEqual(
            sorted({v.split(": ", 1)[0] for v in CHECK.scan(self.root)}),
            ["crates/search/src/items.rs:12"],
        )

    def test_point_free_conversions_must_keep_the_stop_typed(self):
        self.write(
            "crates/storage/src/writer.rs",
            "fn write(task: &Task, work: &Work, admission: &A) -> Result<()> {\n"
            "    task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)?;\n"
            "    task.checkpoint().map_err(io::Error::other)?;\n"
            "    work.checkpoint().map_err(HawDBError::from_storage_error)?;\n"
            "    work.checkpoint().map_err(work_error)?;\n"
            "    admission.io().map_err(flatten_to_text)?;\n"
            "    checkpoint(task).map_err(describe)?;\n"
            "    Ok(())\n"
            "}\n",
        )
        found = CHECK.scan(self.root)
        self.assertEqual(
            [v.split(": ", 1)[0] for v in found],
            ["crates/storage/src/writer.rs:6", "crates/storage/src/writer.rs:7"],
        )
        self.assertIn("point-free `flatten_to_text`", found[0])

    def test_formatted_closure_needs_or_stop_unless_the_error_cannot_stop(self):
        self.write(
            "crates/search/src/encoding.rs",
            "fn encode(result: io::Result<()>, other: Result<(), String>) -> Result<()> {\n"
            "    result.map_err(|error: io::Error| {\n"
            "        HawDBError::Storage(format!(\"encoding failed: {error}\"))\n"
            "    })?;\n"
            "    result.map_err(|error| {\n"
            "        HawDBError::or_stop(&error, || HawDBError::Storage(format!(\"failed: {error}\")))\n"
            "    })?;\n"
            "    other.map_err(|error: String| HawDBError::Execution(format!(\"bad: {error}\")))?;\n"
            "    Ok(())\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            [
                "crates/search/src/encoding.rs:2: Storage(format!(..{error}..)) can hide a stop; "
                "wrap it in HawDBError::or_stop"
            ],
        )
        allowlist = CHECK.FORMAT_ALLOWLIST
        CHECK.FORMAT_ALLOWLIST = {("crates/search/src/encoding.rs", "encoding failed"): "test"}
        try:
            self.assertEqual(CHECK.scan(self.root), [])
        finally:
            CHECK.FORMAT_ALLOWLIST = allowlist

    def test_work_arm_needs_or_stop(self):
        self.write(
            "crates/storage/src/decode.rs",
            "fn decode(result: R) -> Result<()> {\n"
            "    match result {\n"
            "        Err(CheckpointOperationError::Work(error)) => Err(HawDBError::Execution(format!(\n"
            "            \"decoding stopped: {error}\"\n"
            "        ))),\n"
            "        Err(CheckpointOperationError::Operation(error)) => Err(error),\n"
            "        Ok(value) => Ok(value),\n"
            "    }\n"
            "}\n"
            "fn typed(result: R) -> Result<()> {\n"
            "    match result {\n"
            "        Err(CheckpointOperationError::Work(error)) => Err(HawDBError::or_stop(&error, || {\n"
            "            HawDBError::Execution(format!(\"refused: {error}\"))\n"
            "        })),\n"
            "        _ => Ok(()),\n"
            "    }\n"
            "}\n",
        )
        self.assertEqual(
            CHECK.scan(self.root),
            ["crates/storage/src/decode.rs:3: stop arm reports Execution text"],
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
