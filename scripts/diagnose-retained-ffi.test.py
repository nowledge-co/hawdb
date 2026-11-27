#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "diagnostic", Path(__file__).with_name("diagnose-retained-ffi.py")
)
diagnostic = importlib.util.module_from_spec(spec)
spec.loader.exec_module(diagnostic)


class DiagnosticTests(unittest.TestCase):
    def test_preserves_windows_crash_code_and_parallel_serial_commands(self):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            kwargs["stdout"].write(b"running 19 tests\n")
            kwargs["stderr"].write(b"native failure\n")
            return subprocess.CompletedProcess(command, -1073741819)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            diagnostic.diagnose(Path("fixture.exe"), output, run)
            record = json.loads((output / "parallel.json").read_text())
            self.assertEqual(record["returncode_hex"], "0xc0000005")
            self.assertEqual(record["returncode"], -1073741819)
            self.assertFalse(record["positive_terminal_summary"])
            self.assertNotIn("--test-threads=1", calls[0])
            self.assertIn("--test-threads=1", calls[1])
            self.assertEqual((output / "parallel.stderr.txt").read_bytes(),
                             b"native failure\n")

    def test_timeout_and_launch_failure_remain_distinct(self):
        calls = []

        def run(command, **kwargs):
            calls.append(command)
            if len(calls) == 1:
                raise subprocess.TimeoutExpired(command, kwargs["timeout"])
            raise OSError("missing executable")

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            diagnostic.diagnose(Path("fixture.exe"), output, run)
            parallel = json.loads((output / "parallel.json").read_text())
            serial = json.loads((output / "serial.json").read_text())
            self.assertTrue(parallel["timed_out"])
            self.assertIn("missing executable", serial["launch_error"])
            self.assertNotIn("returncode", parallel)
            self.assertNotIn("returncode", serial)

    def test_zero_exit_without_summary_is_not_complete(self):
        def run(command, **kwargs):
            kwargs["stdout"].write(b"running 19 tests\n")
            self.assertEqual(kwargs["env"]["RUST_BACKTRACE"], "1")
            return subprocess.CompletedProcess(command, 0)

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            diagnostic.diagnose(Path("fixture.exe"), output, run)
            record = json.loads((output / "parallel.json").read_text())
            self.assertEqual(record["returncode"], 0)
            self.assertFalse(record["positive_terminal_summary"])


if __name__ == "__main__":
    unittest.main()
