#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Prove how each Native Release trigger maps to builds, uploads and npm dist-tags.

These tests read the real workflow, evaluate its job and step conditions for
each trigger, and run the real mode script for the `source` job: a stable
release ships under npm `latest`, a `-dev.<n>` or `-nightly.<n>` prerelease
under `dev` or `nightly`, and a draft, a tag/version mismatch or a prerelease
flag that disagrees with the version reaches no build, upload or npm job.
"""

import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
if "TEST_SRCDIR" in os.environ:
    ROOT = Path(os.environ["TEST_SRCDIR"]) / os.environ["TEST_WORKSPACE"]
WORKFLOW = ROOT / ".github/workflows/native-release.yml"
PUBLISH_ASSEMBLED = ROOT / "scripts/npm/publish-assembled.sh"
SPEC = importlib.util.spec_from_file_location("native_release_mode", ROOT / "scripts/native-release-mode.py")
release_mode = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release_mode)

SHIPPING_JOBS = {"build", "assets", "npm", "npm-not-configured"}
UPLOAD_STEP = "Upload to the GitHub Release, refusing different existing bytes"
PUBLISH_STEP = "Publish platform packages, then the main package"


def parse_jobs(text):
    """Job-level `if`/`needs` and each step's name, `if` and body."""
    jobs = {}
    job = None
    in_steps = False
    for line in text.split("\n")[text.split("\n").index("jobs:") + 1 :]:
        header = re.fullmatch(r"  ([A-Za-z0-9_-]+):", line)
        if header:
            job = jobs.setdefault(header.group(1), {"if": None, "needs": [], "steps": []})
            in_steps = False
            continue
        if job is None:
            continue
        key = re.fullmatch(r"    (if|needs|steps):\s*(.*)", line)
        if key:
            name, value = key.groups()
            if name == "if":
                job["if"] = value
            elif name == "needs":
                job["needs"] = [part.strip() for part in value.strip("[]").split(",") if part.strip()]
            in_steps = name == "steps"
            continue
        if re.match(r"    \S", line):
            in_steps = False
            continue
        if not in_steps:
            continue
        if line.startswith("      - "):
            job["steps"].append({"name": None, "if": None, "body": ""})
        step = job["steps"][-1]
        step["body"] += line + "\n"
        named = re.match(r"      (?:- |  )name: (.*)", line)
        if named and step["name"] is None:
            step["name"] = named.group(1)
        condition = re.fullmatch(r"        if: (.*)", line)
        if condition:
            step["if"] = condition.group(1)
    return jobs


TOKEN = re.compile(r"\s*(?:(?P<string>'[^']*')|(?P<op>==|!=|\|\||&&|!|\(|\))|(?P<name>[A-Za-z_][A-Za-z0-9_.-]*))")


def evaluate(expression, context):
    """Evaluate the GitHub expression subset this workflow may use.

    Function calls (`always()`, `failure()`, `cancelled()`, ...) are refused, so
    a job cannot run after its gate was skipped or failed.
    """
    python = []
    position = 0
    while position < len(expression.rstrip()):
        match = TOKEN.match(expression, position)
        if not match:
            raise ValueError(f"unsupported expression {expression!r}")
        position = match.end()
        if match.group("string") is not None:
            python.append(repr(match.group("string")[1:-1]))
        elif match.group("op") is not None:
            python.append({"||": "or", "&&": "and", "!": "not"}.get(match.group("op"), match.group("op")))
        else:
            name = match.group("name")
            if expression[position:].lstrip().startswith("("):
                raise ValueError(f"function {name}() is not allowed in {expression!r}")
            if name in ("true", "false", "null"):
                python.append({"true": "True", "false": "False", "null": "None"}[name])
                continue
            value = context
            for part in name.split("."):
                value = value.get(part, "") if isinstance(value, dict) else ""
            python.append(repr(value))
    return bool(eval(" ".join(python), {"__builtins__": {}}))  # noqa: S307 - tokens are literals only


def simulate(event, payload=None, release=None, version="0.31.0", tag=None, npm_enabled=True):
    """Return {job: [steps that run]} for jobs that run, using the real mode script for `source`."""
    jobs = parse_jobs(WORKFLOW.read_text(encoding="utf-8"))
    context = {
        "github": {"event_name": event, "event": payload or {}},
        "vars": {"NPM_TRUSTED_PUBLISHING": "enabled" if npm_enabled else ""},
        "needs": {},
    }
    succeeded, ran = set(), {}
    pending = list(jobs)
    while pending:
        name = next(job for job in pending if not set(jobs[job]["needs"]) & set(pending))
        pending.remove(name)
        job = jobs[name]
        if not all(need in succeeded for need in job["needs"]):
            continue
        if job["if"] is not None and not evaluate(job["if"], context):
            continue
        if name == "source":
            try:
                outputs = release_mode.resolve(event, version, tag, release)
            except release_mode.Refused:
                ran[name] = ["refused"]
                continue
            context["needs"]["source"] = {"outputs": outputs}
        # Inside a job that runs, only steps gated on the source mode are
        # evaluated; runner-local conditions (cache saves) count as running.
        ran[name] = [
            step["name"]
            for step in job["steps"]
            if "needs." not in (step["if"] or "") or evaluate(step["if"], context)
        ]
        succeeded.add(name)
    return ran


def release_event(version, prerelease, draft=False):
    tag = f"v{version}"
    payload = {"release": {"tag_name": tag, "prerelease": prerelease, "draft": draft}}
    metadata = {"tagName": tag, "isDraft": draft, "isPrerelease": prerelease}
    return payload, metadata, tag


class ReleaseModeMappingTests(unittest.TestCase):
    def test_each_release_kind_uploads_and_publishes_under_its_dist_tag(self):
        for version, prerelease, npm_tag in (
            ("0.31.0", False, "latest"),
            ("0.31.0-dev.1", True, "dev"),
            ("0.31.0-nightly.20261112", True, "nightly"),
        ):
            payload, metadata, tag = release_event(version, prerelease)
            ran = simulate("release", payload, metadata, version, tag)
            self.assertEqual(set(ran), {"source", "build", "assets", "npm"}, version)
            self.assertIn(UPLOAD_STEP, ran["assets"])
            self.assertIn(PUBLISH_STEP, ran["npm"])
            self.assertEqual(
                release_mode.resolve("release", version, tag, metadata),
                {"mode": "release", "tag": tag, "npm_tag": npm_tag},
            )

    def test_replaying_a_stable_tag_by_workflow_dispatch_ships_under_latest(self):
        _, metadata, tag = release_event("0.30.0", prerelease=False)
        ran = simulate("workflow_dispatch", {"inputs": {"tag": tag}}, metadata, "0.30.0", tag)
        self.assertEqual(set(ran), {"source", "build", "assets", "npm"})
        self.assertEqual(release_mode.resolve("workflow_dispatch", "0.30.0", tag, metadata)["npm_tag"], "latest")

    def test_refused_releases_ship_nothing(self):
        cases = {
            "draft": ("0.31.0", dict(prerelease=False, draft=True), None),
            "prerelease flag on a stable version": ("0.31.0", dict(prerelease=True), None),
            "stable flag on a dev version": ("0.31.0-dev.1", dict(prerelease=False), None),
            "prerelease flag on an rc version": ("0.31.0-rc.1", dict(prerelease=True), None),
            "tag differs from the source version": ("0.31.0", dict(prerelease=False), "v0.31.1"),
        }
        for label, (version, flags, tag_override) in cases.items():
            payload, metadata, tag = release_event(version, **flags)
            ran = simulate("release", payload, metadata, version, tag_override or tag)
            self.assertEqual(ran, {"source": ["refused"]}, label)
            self.assertEqual(SHIPPING_JOBS & set(ran), set(), label)

    def test_every_shipping_job_requires_the_source_mode(self):
        jobs = parse_jobs(WORKFLOW.read_text(encoding="utf-8"))
        self.assertEqual(set(jobs), SHIPPING_JOBS | {"source"})
        self.assertIsNone(jobs["source"]["if"])
        for name in SHIPPING_JOBS:
            self.assertIn("source", jobs[name]["needs"], name)
            self.assertIn("needs.source.outputs.mode", jobs[name]["if"] or "", name)
        for name in ("npm", "npm-not-configured"):
            self.assertTrue(jobs[name]["if"].startswith("needs.source.outputs.mode == 'release' && "), name)

    def test_npm_publish_passes_the_mode_dist_tag(self):
        jobs = parse_jobs(WORKFLOW.read_text(encoding="utf-8"))
        step = next(step for step in jobs["npm"]["steps"] if step["name"] == PUBLISH_STEP)
        self.assertIn("NPM_TAG: ${{ needs.source.outputs.npm_tag }}", step["body"])
        self.assertIn('publish-assembled.sh "$RUNNER_TEMP/npm-summary.json" --tag "$NPM_TAG" --publish', step["body"])

    def test_release_without_npm_variable_only_reports_skip(self):
        for version, prerelease in (("0.31.0", False), ("0.31.0-dev.2", True)):
            payload, metadata, tag = release_event(version, prerelease)
            ran = simulate("release", payload, metadata, version, tag, npm_enabled=False)
            self.assertEqual(set(ran), {"source", "build", "assets", "npm-not-configured"}, version)

    def test_pull_request_builds_without_upload_or_publish(self):
        ran = simulate("pull_request", version="0.30.0")
        self.assertEqual(set(ran), {"source", "build", "assets"})
        self.assertNotIn(UPLOAD_STEP, ran["assets"])


class ModeScriptTests(unittest.TestCase):
    def refused(self, *arguments):
        with self.assertRaises(release_mode.Refused) as caught:
            release_mode.resolve(*arguments)
        return str(caught.exception)

    def test_refusals(self):
        dev = {"tagName": "v0.31.0-dev.1", "isDraft": False, "isPrerelease": True}
        stable = {"tagName": "v0.31.0", "isDraft": False, "isPrerelease": False}
        self.assertIn("not a stable version", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.1", dict(dev, isPrerelease=False)))
        self.assertIn("not a development version", self.refused("release", "0.31.0", "v0.31.0", dict(stable, isPrerelease=True)))
        self.assertIn("draft", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.1", dict(dev, isDraft=True)))
        self.assertIn("draft", self.refused("release", "0.31.0", "v0.31.0", dict(stable, isDraft=True)))
        self.assertIn("not a development version", self.refused("release", "0.31.0-rc.1", "v0.31.0-rc.1", dict(dev, tagName="v0.31.0-rc.1")))
        self.assertIn("not a stable version", self.refused("release", "0.31.0-rc.1", "v0.31.0-rc.1", dict(stable, tagName="v0.31.0-rc.1")))
        self.assertIn("prerelease flag", self.refused("release", "0.31.0", "v0.31.0", dict(stable, isPrerelease=None)))
        self.assertIn("does not match", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.2", dev))
        self.assertIn("does not match", self.refused("release", "0.31.0", "v0.31.1", stable))
        self.assertIn("metadata", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.1", dict(dev, tagName="v0.30.0")))
        self.assertIn("no GitHub Release", self.refused("workflow_dispatch", "0.31.0-dev.1", "v0.31.0-dev.1", None))
        self.assertIn("unsupported event", self.refused("push", "0.31.0-dev.1", "v0.31.0-dev.1", dev))
        for version in ("0.31.0-dev", "0.31.0-devel.1", "01.2.3-dev.1", "0.31.0-dev.1+build"):
            self.refused("release", version, f"v{version}", dict(dev, tagName=f"v{version}"))
        for version in ("01.2.3", "0.31", "0.31.0+build", "0.31.0-dev.1"):
            self.refused("release", version, f"v{version}", dict(stable, tagName=f"v{version}"))

    def test_command_line_outputs_and_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            release = Path(directory) / "release.json"
            command = [sys.executable, str(ROOT / "scripts/native-release-mode.py"), "--event", "release", "--release", str(release)]
            for version, prerelease, npm_tag in (("0.31.0-dev.3", True, "dev"), ("0.31.0", False, "latest")):
                release.write_text(json.dumps({"tagName": f"v{version}", "isDraft": False, "isPrerelease": prerelease}))
                accepted = subprocess.run(
                    command + ["--version", version, "--tag", f"v{version}"], capture_output=True, text=True
                )
                self.assertEqual(accepted.returncode, 0, accepted.stderr)
                self.assertEqual(accepted.stdout, f"mode=release\ntag=v{version}\nnpm_tag={npm_tag}\n")
            release.write_text(json.dumps({"tagName": "v0.31.0", "isDraft": True, "isPrerelease": False}))
            refused = subprocess.run(command + ["--version", "0.31.0", "--tag", "v0.31.0"], capture_output=True, text=True)
            self.assertEqual(refused.returncode, 1)
            self.assertEqual(refused.stdout, "")
            self.assertIn("::error::v0.31.0 is a draft release", refused.stderr)


# Stand-ins for the two jq filters and the npm subcommands publish-assembled.sh
# uses, so the dist-tag checks run without a registry or a jq install.
FAKE_JQ = """#!/usr/bin/env python3
import json, sys
query, rest = sys.argv[2], sys.argv[3:]
if query == ".[0].shasum":
    print(json.load(sys.stdin)[0]["shasum"])
else:
    summary = json.load(open(rest[0]))
    groups = ["placeholders", "publish"] if ".placeholders[]" in query else ["publish"]
    for group in groups:
        for entry in summary.get(group, []):
            print("\t".join(entry[key] for key in ("name", "version", "directory", "kind")))
"""
FAKE_NPM = """#!/usr/bin/env bash
case "$1" in
  pack) echo '[{"shasum":"local"}]' ;;
  view) exit 1 ;;
  publish)
    # npm reads stdin when it asks for a 2FA confirmation; this stand-in
    # always does, so a publish that could reach the package list eats it.
    cat > /dev/null
    echo "$*" >> "$NPM_LOG" ;;
  *) exit 2 ;;
esac
"""


class NpmDistTagTests(unittest.TestCase):
    def run_publish(self, versions, *arguments):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bin_dir = root / "bin"
            bin_dir.mkdir()
            for name, body in (("jq", FAKE_JQ), ("npm", FAKE_NPM)):
                (bin_dir / name).write_text(body)
                (bin_dir / name).chmod(0o755)
            summary = root / "summary.json"
            entries = [
                {"name": f"@nowledge/package-{index}", "version": version, "directory": str(root), "kind": "library"}
                for index, version in enumerate(versions)
            ]
            summary.write_text(json.dumps({"publish": entries}))
            log = root / "npm.log"
            log.touch()
            environment = dict(os.environ, PATH=f"{bin_dir}{os.pathsep}{os.environ['PATH']}", NPM_LOG=str(log))
            result = subprocess.run(
                ["bash", str(PUBLISH_ASSEMBLED), str(summary), *arguments, "--publish"],
                capture_output=True,
                text=True,
                env=environment,
                # No controlling terminal, as in CI: /dev/tty cannot be opened.
                start_new_session=True,
            )
            return result, log.read_text().splitlines()

    def test_publish_assembled_refuses_a_missing_or_unknown_dist_tag(self):
        for arguments in ([], ["--tag"], ["--tag", "next"]):
            result, published = self.run_publish(["0.31.0"], *arguments)
            self.assertEqual(result.returncode, 64, arguments)
            self.assertIn("--tag must be latest, dev or nightly", result.stderr)
            self.assertEqual(published, [])

    def test_publish_assembled_maps_each_version_to_its_dist_tag(self):
        for version, tag in (("0.31.0", "latest"), ("0.31.0-dev.1", "dev"), ("0.31.0-nightly.7", "nightly")):
            result, published = self.run_publish([version], "--tag", tag)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(len(published), 1, version)
            self.assertIn(f"--tag {tag}", published[0])
            self.assertNotIn("--dry-run", published[0])

    def test_publish_assembled_keeps_the_package_list_from_npm(self):
        # npm reading the loop's stdin (as it does for a 2FA prompt) once
        # swallowed every later package and failed a local publish with EOTP.
        result, published = self.run_publish(["0.31.0", "0.31.0", "0.31.0"], "--tag", "latest")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(published), 3, published)

    def test_publish_assembled_refuses_latest_for_a_prerelease_and_channel_mismatches(self):
        for version, tag in (
            ("0.31.0-dev.1", "latest"),
            ("0.31.0-nightly.1", "latest"),
            ("0.31.0-rc.1", "latest"),
            ("0.31.0", "dev"),
            ("0.31.0-nightly.1", "dev"),
            ("0.31.0-dev.1", "nightly"),
        ):
            result, published = self.run_publish([version], "--tag", tag)
            self.assertEqual(result.returncode, 1, (version, tag))
            self.assertIn("refusing to publish", result.stderr)
            self.assertEqual(published, [], (version, tag))


if __name__ == "__main__":
    unittest.main()
