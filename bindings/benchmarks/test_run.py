# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Qualification failures must survive artifact selection and reporting."""

import contextlib
import io
import json
import os
import pathlib
import sys
import tempfile
import unittest
from unittest import mock

import run as driver
import compare


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


class ComparisonTests(unittest.TestCase):
    def report(self, elapsed=20):
        return {"samples": 2, "terminal": True, "records": [
            {"layer": "python", "case": "point", "size": 1000, "backend": "file",
             "iteration": iteration, "discarded": False, "status": "ok",
             "process_exit": 0, "parity": True, "profile_valid": True,
             "phase_timing_valid": True, "elapsed_ns": elapsed,
             "query_boundary_ns": elapsed, "write_boundary_ns": 0, "read_boundary_ns": elapsed}
            for iteration in (1, 2)
        ]}

    def test_complete_groups_compare_independent_read_and_write_phases(self):
        group = compare.summarize(self.report(), self.report(10))[0]
        self.assertTrue(group["qualified"])
        self.assertEqual(group["read_boundary_ns_median_speedup"], 2)
        self.assertIsNone(group["write_boundary_ns_median_speedup"])

    def test_one_refusal_prevents_median_speedup_claim(self):
        after = self.report(10)
        after["records"][1].update(status="error", parity=None, process_exit=1)
        group = compare.summarize(self.report(), after)[0]
        self.assertFalse(group["qualified"])
        self.assertNotIn("elapsed_ns_median_speedup", group)

    def test_missing_sample_or_nonterminal_report_cannot_qualify(self):
        for kind in ("missing", "nonterminal"):
            with self.subTest(kind=kind):
                after = self.report()
                if kind == "missing":
                    after["records"].pop()
                else:
                    after["terminal"] = False
                self.assertFalse(compare.summarize(self.report(), after)[0]["qualified"])

    def test_failed_parity_cannot_qualify_even_with_zero_exit(self):
        after = self.report()
        after["records"][0]["parity"] = False
        self.assertFalse(compare.summarize(self.report(), after)[0]["qualified"])

    def test_baseline_adapter_refuses_unqualified_or_mutable_revision(self):
        compare.validate_revision(compare.BASELINE)
        for revision in ("main", "69d526a2", "f" * 40):
            with self.subTest(revision=revision):
                with self.assertRaises(ValueError):
                    compare.validate_revision(revision)


class ProducerFreezeTests(unittest.TestCase):
    def producer(self, root):
        repository = root / "repository"
        launcher = repository / "bazel-bin/bindings/benchmarks/python_boundary"
        launcher.parent.mkdir(parents=True)
        launcher.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "sys.path.insert(0, os.environ['RUNFILES_DIR'])\n"
            "import wrapper\n"
            "print(json.dumps({'status': 'ok', 'version': wrapper.VERSION}))\n")
        launcher.chmod(0o755)
        runfiles = pathlib.Path(str(launcher) + ".runfiles")
        runfiles.mkdir()
        wrapper = repository / "wrapper.py"
        wrapper.write_text("VERSION = 'baseline'\n")
        (runfiles / "wrapper.py").symlink_to(wrapper)
        (runfiles / "runtime").mkdir()
        (runfiles / "runtime/python").write_text("frozen interpreter\n")
        (runfiles / "venv").mkdir()
        (runfiles / "venv/python").symlink_to("../runtime/python")
        (runfiles / "data with space.txt").write_text("manifest path control\n")
        (runfiles / "__pycache__").mkdir()
        (runfiles / "__pycache__/wrapper.pyc").write_bytes(b"stale bytecode")
        manifest = pathlib.Path(str(launcher) + ".runfiles_manifest")
        manifest.write_text("wrapper.py " + str(wrapper) + "\n")
        (runfiles / "MANIFEST").symlink_to(manifest)
        suffix = ".dylib" if sys.platform == "darwin" else ".so"
        for relative in ("hawdb_bench_host_boundary", "boundary",
                         "bindings/ffi/libhawdb_ffi" + suffix,
                         "bindings/python/python/hawdb/_hawdb.so"):
            path = repository / "bazel-bin" / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"native producer")
        with mock.patch.object(compare.subprocess, "run"), \
                mock.patch.object(compare, "command", return_value="bazel-bin/boundary"):
            frozen = compare.build_producer(repository, root / "frozen producer")
        return frozen, launcher, wrapper

    def test_rebuilding_shared_output_cannot_replace_frozen_package(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            producer, launcher, wrapper = self.producer(root)
            wrapper.write_text("VERSION = 'candidate'\n")
            launcher.write_text("raise RuntimeError('replaced launcher')\n")
            compare.require_producer(producer)
            python = producer["python"]
            runfiles = pathlib.Path(python["runfiles"]["path"])
            self.assertNotEqual(python["path"], str(launcher))
            self.assertFalse((runfiles / "wrapper.py").is_symlink())
            self.assertEqual(os.readlink(runfiles / "venv/python"), "../runtime/python")
            self.assertFalse((runfiles / "__pycache__").exists())
            manifest = (runfiles / "MANIFEST").read_text()
            self.assertIn(" data\\swith\\sspace.txt " + str(runfiles / "data with space.txt"), manifest)
            self.assertIn("wrapper.py " + str(runfiles / "wrapper.py"), manifest)
            self.assertNotIn(str(wrapper), manifest)
            with mock.patch.dict(os.environ, {"RUNFILES_DIR": str(launcher) + ".runfiles"}):
                result = driver.run_child([python["path"]], root / "frozen-import")
            self.assertEqual(result["process_exit"], 0)
            self.assertEqual(result["version"], "baseline")
            compare.require_producer(producer)

    def test_changed_frozen_package_prevents_qualification(self):
        with tempfile.TemporaryDirectory() as temporary:
            producer, _, _ = self.producer(pathlib.Path(temporary))
            runfiles = pathlib.Path(producer["python"]["runfiles"]["path"])
            (runfiles / "wrapper.py").write_text("VERSION = 'changed'\n")
            with self.assertRaisesRegex(RuntimeError, "Python runfiles changed"):
                compare.require_producer(producer)


if __name__ == "__main__":
    unittest.main()
