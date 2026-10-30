#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Exercise the actual PyPI workflow snippets without publishing packages."""

from contextlib import redirect_stdout
import hashlib
import io
import json
import os
from pathlib import Path
import tempfile
import textwrap
import unittest
from unittest.mock import patch
import urllib.error

ROOT = Path(__file__).resolve().parent.parent
if "TEST_SRCDIR" in os.environ:
    ROOT = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
WORKFLOW = ROOT / ".github/workflows/pypi.yaml"


def workflow_python(step):
    text = WORKFLOW.read_text(encoding="utf-8")
    body = text.split(f"      - name: {step}\n", 1)[1].split("      - ", 1)[0]
    heredoc = body.split("          python - <<'PY'\n", 1)[1]
    python = heredoc.split("          PY\n", 1)[0]
    return compile(textwrap.dedent(python), f"{WORKFLOW}:{step}", "exec")


class PublisherVerificationTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.dist = self.root / "dist"
        self.dist.mkdir()
        self.manifest = self.root / "pypi-verified-files.json"
        cwd = Path.cwd()
        self.addCleanup(os.chdir, cwd)
        os.chdir(self.root)
        self.environment = patch.dict(
            os.environ,
            {
                "RELEASE_VERSION": "0.21.0",
                "RELEASE_LOCK_HASH": "fixture-lock-hash",
                "RUNNER_TEMP": str(self.root),
            },
        )
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.expected = {}
        for platform in [
            "macosx_10_12_x86_64",
            "macosx_11_0_arm64",
            "manylinux_2_28_x86_64",
            "manylinux_2_28_aarch64",
            "win_amd64",
        ]:
            self.add_distribution(f"hawdb-0.21.0-cp311-abi3-{platform}.whl")
        self.add_distribution("hawdb-0.21.0.tar.gz")
        self.preflight = workflow_python(
            "Verify the complete artifact set and any earlier uploads"
        )
        self.verify = workflow_python("Verify registry file hashes")
        self.output = io.StringIO()

    def add_distribution(self, name):
        content = f"fixture: {name}".encode()
        (self.dist / name).write_bytes(content)
        self.expected[name] = hashlib.sha256(content).hexdigest()

    def response(self, hashes):
        return io.StringIO(
            json.dumps(
                {
                    "urls": [
                        {"filename": name, "digests": {"sha256": digest}}
                        for name, digest in hashes.items()
                    ]
                }
            )
        )

    def capture_manifest(self, existing=None):
        response = (
            urllib.error.HTTPError(
                "https://pypi.org/fixture", 404, "Not Found", {}, None
            )
            if existing is None
            else self.response(existing)
        )
        with patch("urllib.request.urlopen", side_effect=[response]), redirect_stdout(
            self.output
        ):
            exec(self.preflight, {})

    def verify_registry(self, responses):
        with patch("urllib.request.urlopen", side_effect=responses) as request, patch(
            "time.sleep"
        ) as sleep, redirect_stdout(self.output):
            exec(self.verify, {})
        return request, sleep

    def test_attestation_sidecars_do_not_become_expected_distributions(self):
        self.capture_manifest()
        self.assertEqual(json.loads(self.manifest.read_text()), self.expected)
        self.assertEqual(len(list(self.dist.iterdir())), 6)
        for name in self.expected:
            (self.dist / f"{name}.publish.attestation").write_text("attestation")
        request, sleep = self.verify_registry([self.response(self.expected)])
        self.assertEqual(request.call_count, 1)
        sleep.assert_not_called()
        self.assertIn("Verified 6 PyPI file hashes", self.output.getvalue())

    def test_registry_check_uses_original_verified_hashes(self):
        self.capture_manifest()
        name = next(iter(self.expected))
        (self.dist / name).write_bytes(b"changed after preflight")
        self.verify_registry([self.response(self.expected)])

    def test_missing_or_mismatched_distributions_fail_with_details(self):
        self.capture_manifest()
        name = next(iter(self.expected))
        for failure in ["missing", "mismatched"]:
            with self.subTest(failure=failure):
                self.output = io.StringIO()
                actual = dict(self.expected)
                if failure == "missing":
                    del actual[name]
                else:
                    actual[name] = "0" * 64
                with self.assertRaisesRegex(
                    SystemExit, "PyPI did not expose every expected file"
                ):
                    self.verify_registry([self.response(actual) for _ in range(12)])
                self.assertIn(name, self.output.getvalue())
                if failure == "mismatched":
                    self.assertIn(self.expected[name], self.output.getvalue())
                    self.assertIn(actual[name], self.output.getvalue())

    def test_eventually_visible_distributions_succeed(self):
        self.capture_manifest()
        request, sleep = self.verify_registry(
            [
                urllib.error.HTTPError(
                    "https://pypi.org/fixture", 404, "Not Found", {}, None
                ),
                self.response({}),
                self.response(self.expected),
            ]
        )
        self.assertEqual(request.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_persistent_network_failures_are_reported_and_bounded(self):
        self.capture_manifest()
        for error in [
            urllib.error.URLError("network unavailable"),
            TimeoutError("timed out"),
        ]:
            with self.subTest(error=error), patch(
                "urllib.request.urlopen", side_effect=error
            ) as request, patch("time.sleep") as sleep, redirect_stdout(self.output):
                with self.assertRaisesRegex(
                    SystemExit, "PyPI did not expose every expected file"
                ):
                    exec(self.verify, {})
                self.assertEqual(request.call_count, 12)
                self.assertEqual(sleep.call_count, 11)
                self.assertIn(str(error), self.output.getvalue())

    def test_matching_earlier_partial_upload_is_allowed(self):
        self.capture_manifest(dict(list(self.expected.items())[:2]))
        self.assertEqual(json.loads(self.manifest.read_text()), self.expected)

    def test_conflicting_earlier_upload_is_rejected_before_manifest(self):
        for existing in [
            {next(iter(self.expected)): "0" * 64},
            {"unexpected.whl": "0" * 64},
        ]:
            with self.subTest(existing=existing):
                with self.assertRaisesRegex(SystemExit, "Existing PyPI files differ"):
                    self.capture_manifest(existing)
                self.assertFalse(self.manifest.exists())

    def test_incomplete_artifact_set_is_rejected_before_manifest(self):
        (self.dist / next(iter(self.expected))).unlink()
        with self.assertRaises(AssertionError):
            self.capture_manifest()
        self.assertFalse(self.manifest.exists())


if __name__ == "__main__":
    unittest.main()
