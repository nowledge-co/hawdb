# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Qualification failures must survive artifact selection and reporting."""

import contextlib
import io
import json
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import run as driver


def complete_profile(layer):
    profile = dict.fromkeys((
        "allocation_calls", "allocated_bytes", "deallocation_calls",
        "deallocated_bytes", "reallocation_calls", "live_requested_bytes_before",
        "live_requested_bytes_after", "process_peak_requested_bytes",
    ), 0)
    if layer != "rust":
        profile.update(parameter_conversion_ns=0, engine_call_ns=0, result_conversion_ns=0)
    return profile


class DriverTests(unittest.TestCase):
    def matrix(self, native_profiles, supplied_profiles, process_exit=0, read_ns=10,
               replace_artifact=False):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            artifacts = {}
            for name in ("rust", "python", "go", "library", "python-extension"):
                path = root / name
                path.write_bytes(name.encode())
                artifacts[name] = path
            output = root / "result"
            arguments = ["run.py", "--output", str(output), "--sizes", "2",
                         "--cases", "select", "--backends", "memory", "--samples", "1"]
            for name, path in artifacts.items():
                arguments.extend(("--" + name, str(path)))
            if native_profiles:
                arguments.append("--native-profiles")
            commands = []

            def child(command, artifact):
                commands.append(command)
                job = json.loads(pathlib.Path(command[1]).read_text())
                layer = pathlib.Path(command[0]).name
                if replace_artifact:
                    artifacts["rust"].write_bytes(b"changed")
                return {
                    "status": "ok", "process_exit": process_exit,
                    "output_rows": len(job["rows"]), "checksum": job["expected_checksum"],
                    "query_boundary_ns": 10, "write_boundary_ns": 0,
                    "read_boundary_ns": read_ns, "elapsed_ns": 20,
                    "native_profile": supplied_profiles(layer),
                }

            with mock.patch.object(sys, "argv", arguments), \
                    mock.patch.object(driver, "source_identity", return_value={"head": "head", "tree": "tree"}), \
                    mock.patch.object(driver, "command_text", return_value="test-runtime"), \
                    mock.patch.object(driver, "run_child", side_effect=child), \
                    contextlib.redirect_stdout(io.StringIO()):
                if replace_artifact:
                    with self.assertRaisesRegex(RuntimeError, "artifacts changed"):
                        driver.main()
                    status = 1
                else:
                    status = driver.main()
            report = json.loads((output / "report.json").read_text())
            if replace_artifact:
                self.assertEqual(len(report["records"]), 1)
                self.assertFalse(report["terminal"])
                return status, report
            for command in commands:
                if pathlib.Path(command[0]).name == "python":
                    self.assertEqual(command[2], str(artifacts["python-extension"].resolve()))
            self.assertEqual(len(report["records"]), 6)
            self.assertTrue(report["terminal"])
            return status, report

    def test_fixed_ordinary_extension_is_not_instrumentation(self):
        status, report = self.matrix(False, lambda _: None)
        self.assertEqual(status, 0)
        self.assertFalse(report["instrumented"])
        self.assertTrue(report["all_paths_succeeded"])
        self.assertIsNotNone(report["python_extension_sha256"])

    def test_missing_counters_fail_even_with_matching_rows(self):
        status, report = self.matrix(True, lambda _: None)
        self.assertEqual(status, 1)
        self.assertEqual(report["parity_failures"], 0)
        self.assertEqual(report["profile_failures"], 6)
        self.assertEqual(report["successful_records"], 0)
        self.assertEqual(report["value_parity_records"], 6)
        self.assertFalse(report["all_paths_succeeded"])
        self.assertTrue(all(summary["successful_samples"] == 0 for summary in report["summaries"]))

    def test_unexpected_instrumentation_fails_ordinary_latency(self):
        status, report = self.matrix(False, complete_profile)
        self.assertEqual(status, 1)
        self.assertFalse(report["instrumented"])
        self.assertEqual(report["profile_failures"], 6)
        self.assertFalse(report["all_paths_succeeded"])

    def test_complete_instrumented_matrix_passes(self):
        status, report = self.matrix(True, complete_profile)
        self.assertEqual(status, 0)
        self.assertTrue(report["instrumented"])
        self.assertEqual(report["profile_failures"], 0)

    def test_nonzero_exit_cannot_supply_a_successful_latency_sample(self):
        status, report = self.matrix(False, lambda _: None, process_exit=1)
        self.assertEqual(status, 1)
        self.assertEqual(report["successful_records"], 0)
        self.assertTrue(all(summary["successful_samples"] == 0 for summary in report["summaries"]))

    def test_unbalanced_phases_cannot_supply_a_successful_latency_sample(self):
        status, report = self.matrix(False, lambda _: None, read_ns=11)
        self.assertEqual(status, 1)
        self.assertEqual(report["phase_timing_failures"], 6)
        self.assertEqual(report["successful_records"], 0)

    def test_conversion_fields_and_counter_types_are_required(self):
        result = {"status": "ok", "native_profile": complete_profile("rust")}
        self.assertTrue(driver.native_profile_valid(result, "rust", True))
        self.assertFalse(driver.native_profile_valid(result, "python", True))
        result["native_profile"]["allocation_calls"] = False
        self.assertFalse(driver.native_profile_valid(result, "rust", True))

    def test_mid_child_artifact_replacement_preserves_raw_outcome(self):
        status, report = self.matrix(False, lambda _: None, replace_artifact=True)
        self.assertEqual(status, 1)
        self.assertIn("artifacts changed", report["qualification_error"])
        self.assertEqual(report["records"][0]["process_exit"], 0)
        self.assertTrue(report["records"][0]["parity"])

    def test_same_length_artifact_replacement_aborts_qualification(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = pathlib.Path(temporary) / "engine"
            path.write_bytes(b"before")
            identity = driver.artifact_identity((path,))
            path.write_bytes(b"after!")
            with self.assertRaisesRegex(RuntimeError, "artifacts changed"):
                driver.require_artifact_identity((path,), identity)


if __name__ == "__main__":
    unittest.main()
