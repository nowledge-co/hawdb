#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Exercise the actual PyPI workflow snippets without publishing packages."""

from contextlib import redirect_stdout
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch
import urllib.error

ROOT = Path(__file__).resolve().parent.parent
if "TEST_SRCDIR" in os.environ:
    ROOT = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
WORKFLOW = ROOT / ".github/workflows/pypi.yaml"
MODE_SCRIPT = ROOT / "scripts/pypi-release-mode.py"
SPEC = importlib.util.spec_from_file_location("pypi_release_mode", MODE_SCRIPT)
release_mode = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_mode)
GATE_STEP = "Validate the release tag and source"


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


class PackagingToolchainTests(unittest.TestCase):
    def test_packaging_selects_the_installed_pinned_toolchain(self):
        workflow = WORKFLOW.read_text(encoding="utf-8")
        toolchain = (ROOT / "rust-toolchain.toml").read_text(encoding="utf-8")
        channel = re.search(r'^channel = "([^"]+)"$', toolchain, re.MULTILINE)[1]
        # A default toolchain still lets the released source request development
        # components. Packaging must select the installed toolchain explicitly.
        environment = re.search(r"(?m)^env:\n((?:  .+\n)+)", workflow)
        self.assertIsNotNone(environment, "Packaging needs a workflow toolchain override")
        override = re.search(
            r'^  RUSTUP_TOOLCHAIN: "([^"]+)"$', environment[1], re.MULTILINE
        )
        self.assertIsNotNone(override, "Packaging must set RUSTUP_TOOLCHAIN")
        self.assertEqual(override[1], channel)
        self.assertIn(f"uses: dtolnay/rust-toolchain@{channel}", workflow)
        self.assertIn(f'rust-toolchain: "{channel}"', workflow)


def parse_jobs(text):
    """Each job's `if`, `needs` and ordered step names."""
    jobs = {}
    job = None
    for line in text.split("\n")[text.split("\n").index("jobs:") + 1 :]:
        header = re.fullmatch(r"  ([A-Za-z0-9_-]+):", line)
        if header:
            job = jobs.setdefault(header.group(1), {"if": None, "needs": [], "steps": []})
            continue
        if job is None:
            continue
        key = re.fullmatch(r"    (if|needs):\s*(.*)", line)
        if key:
            name, value = key.groups()
            if name == "if":
                job["if"] = value
            else:
                job["needs"] = [part.strip() for part in value.strip("[]").split(",") if part.strip()]
            continue
        named = re.fullmatch(r"      - (?:uses|name): (.*)", line) or re.fullmatch(
            r"        name: (.*)", line
        )
        if named:
            job["steps"].append(named.group(1))
    return jobs


TOKEN = re.compile(
    r"\s*(?:(?P<string>'[^']*')|(?P<op>==|!=|\|\||&&|!|\(|\))|(?P<name>[A-Za-z_][A-Za-z0-9_.-]*))"
)


def evaluate(expression, context):
    """Evaluate the subset of GitHub expression syntax these conditions use."""
    tokens = []
    position = 0
    expression = expression.strip()
    while position < len(expression):
        match = TOKEN.match(expression, position)
        if match is None or match.end() == position:
            raise ValueError(f"unsupported expression {expression!r} at {position}")
        position = match.end()
        if match.group("string") is not None:
            tokens.append(("value", match.group("string")[1:-1]))
        elif match.group("op") is not None:
            tokens.append(("op", match.group("op")))
        else:
            name = match.group("name")
            if name in ("true", "false"):
                tokens.append(("value", name == "true"))
            elif name.endswith("()"):
                raise ValueError(f"status function in {expression!r}")
            else:
                value = context
                for part in name.split("."):
                    value = value.get(part) if isinstance(value, dict) else None
                tokens.append(("value", value))

    def take(expected=None):
        token = tokens.pop(0)
        if expected is not None and token != ("op", expected):
            raise ValueError(f"expected {expected!r} in {expression!r}")
        return token

    def primary():
        kind, value = take()
        if (kind, value) == ("op", "!"):
            return not primary()
        if (kind, value) == ("op", "("):
            result = disjunction()
            take(")")
            return result
        if kind != "value":
            raise ValueError(f"unexpected {value!r} in {expression!r}")
        return value

    def comparison():
        left = primary()
        if tokens and tokens[0] in (("op", "=="), ("op", "!=")):
            operator = take()[1]
            right = primary()
            return (left == right) if operator == "==" else (left != right)
        return left

    def conjunction():
        result = comparison()
        while tokens and tokens[0] == ("op", "&&"):
            take()
            right = comparison()
            result = result and right
        return result

    def disjunction():
        result = conjunction()
        while tokens and tokens[0] == ("op", "||"):
            take()
            right = conjunction()
            result = result or right
        return result

    result = disjunction()
    if tokens:
        raise ValueError(f"trailing tokens in {expression!r}")
    return bool(result)


class StableReleaseExclusionTests(unittest.TestCase):
    """AGENTS.md keeps HawDB out of stable and GA releases.

    These tests read the real workflow, evaluate its job conditions, and run the
    real gate commands of its `sdist` job with a stubbed `gh`, proving that a
    stable release cannot reach the sdist, wheel or PyPI publish jobs.
    """

    def setUp(self):
        self.jobs = parse_jobs(WORKFLOW.read_text(encoding="utf-8"))

    def gate_commands(self):
        text = WORKFLOW.read_text(encoding="utf-8")
        body = text.split(f"      - name: {GATE_STEP}\n", 1)[1].split("\n      - ", 1)[0]
        script = textwrap.dedent(body.split("        run: |\n", 1)[1])
        # Run everything before the checks that need the real release clone.
        return script.split("git merge-base", 1)[0]

    def run_gate(self, event, tag, version, release):
        """Run the step's gate commands; return (exit code, outputs, stderr)."""
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            (root / "scripts").mkdir()
            (root / "scripts/pypi-release-mode.py").write_bytes(MODE_SCRIPT.read_bytes())
            source = root / "release-source"
            (source / "bindings/python").mkdir(parents=True)
            (source / "bindings/python/Cargo.toml").write_text(
                f'[package]\nname = "hawdb-python"\nversion = "{version}"\n', encoding="utf-8"
            )
            fixture = root / "release-fixture.json"
            fixture.write_text(json.dumps(release) if release is not None else "", encoding="utf-8")
            stubs = {
                "gh": f'#!/bin/sh\n[ -s "{fixture}" ] || exit 1\ncat "{fixture}"\n',
                "python": f'#!/bin/sh\nexec "{sys.executable}" "$@"\n',
            }
            for name, content in stubs.items():
                (bin_dir / name).write_text(content, encoding="utf-8")
                (bin_dir / name).chmod(stat.S_IRWXU)
            runner_temp = root / "runner"
            runner_temp.mkdir()
            output = root / "github-output"
            output.write_text("", encoding="utf-8")
            result = subprocess.run(
                ["bash", "-e", "-c", self.gate_commands()],
                cwd=source,
                env={
                    "PATH": f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '/usr/bin:/bin')}",
                    "EVENT": event,
                    "RELEASE_TAG": tag,
                    "RUNNER_TEMP": str(runner_temp),
                    "GITHUB_OUTPUT": str(output),
                },
                capture_output=True,
                text=True,
                check=False,
            )
            outputs = dict(
                line.split("=", 1) for line in output.read_text(encoding="utf-8").splitlines() if line
            )
            return result.returncode, outputs, result.stderr

    def reachable_jobs(self, event, release, mode):
        """Jobs that start for this trigger, given the sdist job's mode output."""
        context = {
            "github": {"event_name": event, "event": {"release": release} if release else {}},
            "needs": {"sdist": {"outputs": {"mode": mode}}},
        }
        started = set()
        for name in self.jobs:
            job = self.jobs[name]
            if all(need in started for need in job["needs"]) and (
                job["if"] is None or evaluate(job["if"], context)
            ):
                started.add(name)
        # A refused gate fails the sdist job, so no dependent job starts.
        if "sdist" in started and mode != "development":
            started = {"sdist"}
        return started

    def assert_cannot_publish(self, event, tag, version, release, reason):
        github_release = (
            {"tag_name": tag, "prerelease": release["isPrerelease"], "draft": release["isDraft"]}
            if event == "release"
            else None
        )
        status, outputs, stderr = self.run_gate(event, tag, version, release)
        self.assertNotEqual(status, 0, stderr)
        self.assertNotIn("mode", outputs)
        self.assertIn(reason, stderr)
        self.assertLessEqual(self.reachable_jobs(event, github_release, ""), {"sdist"})

    def test_workflow_jobs_all_depend_on_the_gated_sdist_job(self):
        self.assertEqual(set(self.jobs), {"sdist", "wheels", "publish"})
        self.assertEqual(
            self.jobs["sdist"]["if"], "github.event_name != 'release' || github.event.release.prerelease"
        )
        for name in ("wheels", "publish"):
            self.assertIn("sdist", self.jobs[name]["needs"])
            self.assertEqual(self.jobs[name]["if"], "needs.sdist.outputs.mode == 'development'")
        steps = self.jobs["sdist"]["steps"]
        gate = steps.index(GATE_STEP)
        for later in ("Install packaging tools", "Validate the reviewed dependency graph and build the source distribution"):
            self.assertLess(gate, steps.index(later))
        # The gate runs from the workflow revision, never from the released tag.
        self.assertIn("python ../scripts/pypi-release-mode.py", self.gate_commands())

    def test_stable_release_cannot_reach_sdist_wheels_or_publish(self):
        release = {"tagName": "v0.30.0", "isDraft": False, "isPrerelease": False}
        self.assertEqual(
            self.reachable_jobs("release", {"tag_name": "v0.30.0", "prerelease": False}, ""), set()
        )
        self.assert_cannot_publish("release", "v0.30.0", "0.30.0", release, "is a stable release")

    def test_stable_tag_replay_is_refused(self):
        release = {"tagName": "v0.30.0", "isDraft": False, "isPrerelease": False}
        self.assert_cannot_publish("workflow_dispatch", "v0.30.0", "0.30.0", release, "is a stable release")

    def test_prerelease_flag_on_a_non_development_version_is_refused(self):
        for version in ["0.31.0", "0.31.0-rc.1", "0.31.0-alpha.1", "0.31.0-nightly.1", "0.31.0-dev", "0.31.0-dev.01", "0.31.0.dev1"]:
            with self.subTest(version=version):
                release = {"tagName": f"v{version}", "isDraft": False, "isPrerelease": True}
                self.assert_cannot_publish(
                    "release", f"v{version}", version, release, "is not a development version"
                )

    def test_draft_mismatched_or_missing_release_is_refused(self):
        cases = [
            ("is a draft release", "v0.31.0-dev.1", {"tagName": "v0.31.0-dev.1", "isDraft": True, "isPrerelease": True}),
            ("does not match the source version", "v0.31.0-dev.2", {"tagName": "v0.31.0-dev.2", "isDraft": False, "isPrerelease": True}),
            ("GitHub Release metadata is for", "v0.31.0-dev.1", {"tagName": "v0.30.0", "isDraft": False, "isPrerelease": True}),
        ]
        for reason, tag, release in cases:
            with self.subTest(reason=reason):
                self.assert_cannot_publish("workflow_dispatch", tag, "0.31.0-dev.1", release, reason)
        status, outputs, _ = self.run_gate("workflow_dispatch", "v0.31.0-dev.1", "0.31.0-dev.1", None)
        self.assertNotEqual(status, 0)
        self.assertNotIn("mode", outputs)

    def test_development_prerelease_publishes_its_pep440_dev_version(self):
        for event in ("release", "workflow_dispatch"):
            with self.subTest(event=event):
                release = {"tagName": "v0.31.0-dev.1", "isDraft": False, "isPrerelease": True}
                status, outputs, stderr = self.run_gate(event, "v0.31.0-dev.1", "0.31.0-dev.1", release)
                self.assertEqual(status, 0, stderr)
                self.assertEqual(outputs, {"mode": "development", "python_version": "0.31.0.dev1"})
                github_release = {"tag_name": "v0.31.0-dev.1", "prerelease": True} if event == "release" else None
                self.assertEqual(
                    self.reachable_jobs(event, github_release, outputs["mode"]),
                    {"sdist", "wheels", "publish"},
                )

    def test_python_version_matches_packaging_normalization(self):
        from packaging.version import Version

        for version, expected in [("0.31.0-dev.1", "0.31.0.dev1"), ("1.2.3-dev.0", "1.2.3.dev0")]:
            with self.subTest(version=version):
                outputs = release_mode.resolve(
                    "release",
                    version,
                    f"v{version}",
                    {"tagName": f"v{version}", "isDraft": False, "isPrerelease": True},
                )
                self.assertEqual(outputs["python_version"], expected)
                parsed = Version(version)
                self.assertEqual(str(parsed), expected)
                self.assertTrue(parsed.is_devrelease)
                self.assertIsNone(parsed.pre)

    def test_unsupported_event_is_refused(self):
        with self.assertRaisesRegex(release_mode.Refused, "unsupported event"):
            release_mode.resolve("push", "0.31.0-dev.1", "v0.31.0-dev.1", None)


if __name__ == "__main__":
    unittest.main()
