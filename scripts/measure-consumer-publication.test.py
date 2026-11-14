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

"""Manual preflight contracts for source-bound Consumer measurements."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "consumer_measurement", Path(__file__).with_name("measure-consumer-publication.py"))
measurement = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measurement)


class BuildEvidenceTests(unittest.TestCase):
    def test_selected_artifact_proves_nondefault_features(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            binary = root / "consumer"
            binary.write_bytes(b"fixture")
            artifact = {"reason": "compiler-artifact", "target": {"name": "consumer_publication"},
                        "executable": str(binary), "features": ["full-text-search", "background-maintenance"],
                        "profile": {"opt_level": "3", "debug_assertions": False}}
            foreign = dict(artifact, executable=str(root / "other"), features=["default"])
            receipts = root / "build.jsonl"
            receipts.write_text("\n".join(json.dumps(row) for row in (
                {"reason": "build-script-executed"}, foreign, artifact)))
            result = measurement.build_evidence(receipts, binary)
            self.assertFalse(result["default_features"])
            self.assertEqual(result["features"], ["background-maintenance", "full-text-search"])
            self.assertEqual(result["build_artifacts_sha256"], measurement.digest(receipts))
            for rows in ([foreign], [artifact, artifact]):
                receipts.write_text("\n".join(json.dumps(row) for row in rows))
                with self.assertRaisesRegex(ValueError, "exactly one Cargo artifact"):
                    measurement.build_evidence(receipts, binary)

    def test_checked_profile_cannot_qualify_latency(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            binary = root / "consumer"
            binary.write_bytes(b"fixture")
            receipts = root / "build.jsonl"
            receipts.write_text(json.dumps({
                "reason": "compiler-artifact", "target": {"name": "consumer_publication"},
                "executable": str(binary), "features": ["default"],
                "profile": {"opt_level": "1", "debug_assertions": True}}))
            with self.assertRaisesRegex(ValueError, "optimized bench profile"):
                measurement.build_evidence(receipts, binary)


class ReadEvidenceTests(unittest.TestCase):
    def fixture(self):
        resources = {"status_available": True, "VmRSS": 64, "VmHWM": 80,
                     "cpu_user_ticks": 10, "cpu_system_ticks": 2}
        samples = [{"worker": 0, "sequence": index, "started_ns": start,
                    "query_started_ns": start + 1, "finished_ns": start + 2,
                    "elapsed_ns": 2, "active_writers_after": 1,
                    "committed_writes_before": index, "committed_writes_after": index + 1}
                   for index, start in enumerate([2, 6])]
        return {"readers": 1, "reads_per_reader": 2, "mode": "detached", "writes": 10,
                "foreground_read": {"requests": 2, "latency": {"count": 2},
                                    "samples": samples, "completed_during_preparation": 2,
                                    "completed_with_active_writers": 2},
                "concurrent_phase_observation": {"phase": "concurrent_phase",
                                                 "resources_before": resources,
                                                 "resources_after": dict(resources)},
                "publication_attempts": [{"phase_observations": [
                    {"phase": "coverage_preparation", "started_ns": 1, "finished_ns": 10}]}]}

    def test_complete_reads_with_real_overlap(self):
        self.assertTrue(measurement.validate_reads(self.fixture(), 1, 2))

    def test_drain_only_reads_do_not_qualify(self):
        result = self.fixture()
        result["publication_attempts"][0]["phase_observations"][0]["finished_ns"] = 2
        result["foreground_read"]["completed_during_preparation"] = 0
        self.assertFalse(measurement.validate_reads(result, 1, 2))

    def test_independent_active_and_preparation_counts_cannot_be_combined(self):
        result = self.fixture()
        result["publication_attempts"][0]["phase_observations"][0]["started_ns"] = 5
        result["foreground_read"]["samples"][1]["active_writers_after"] = 0
        result["foreground_read"]["samples"][1]["committed_writes_after"] = 10
        result["foreground_read"]["completed_during_preparation"] = 1
        result["foreground_read"]["completed_with_active_writers"] = 1
        self.assertFalse(measurement.validate_reads(result, 1, 2))

    def test_missing_resource_observation_is_not_coverage(self):
        result = self.fixture()
        result["concurrent_phase_observation"]["resources_after"]["status_available"] = False
        with self.assertRaises(AssertionError):
            measurement.validate_reads(result, 1, 2)

    def test_zero_or_missing_reader_cannot_pass(self):
        result = self.fixture()
        result["foreground_read"]["samples"].pop()
        with self.assertRaises(AssertionError):
            measurement.validate_reads(result, 1, 2)


if __name__ == "__main__":
    unittest.main()
