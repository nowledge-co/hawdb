#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Prove the Native Release workflow cannot ship HawDB from a stable release.

AGENTS.md keeps HawDB out of stable and GA releases. These tests read the real
workflow, evaluate its job and step conditions for each trigger, and run the
real mode script for the `source` job, so a stable release is shown not to
reach compilation, the GitHub Release upload or either npm job.
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


class StableReleaseExclusionTests(unittest.TestCase):
    def assert_nothing_ships(self, ran):
        self.assertEqual(SHIPPING_JOBS & set(ran), set(), ran)

    def test_stable_release_cannot_reach_build_upload_or_npm_jobs(self):
        payload, metadata, tag = release_event("0.31.0", prerelease=False)
        ran = simulate("release", payload, metadata, "0.31.0", tag)
        self.assertEqual(ran, {}, "a stable release must not even start the source job")

    def test_stable_release_is_refused_even_if_the_source_job_runs(self):
        # Defense in depth: the mode script refuses on its own, for example when
        # GitHub's payload and the release metadata disagree.
        payload, metadata, tag = release_event("0.31.0", prerelease=True)
        metadata["isPrerelease"] = False
        ran = simulate("release", payload, metadata, "0.31.0", tag)
        self.assertEqual(ran, {"source": ["refused"]})

    def test_prerelease_flag_on_a_stable_version_ships_nothing(self):
        payload, metadata, tag = release_event("0.31.0", prerelease=True)
        ran = simulate("release", payload, metadata, "0.31.0", tag)
        self.assertEqual(ran, {"source": ["refused"]})

    def test_replaying_a_stable_tag_by_workflow_dispatch_ships_nothing(self):
        _, metadata, tag = release_event("0.30.0", prerelease=False)
        ran = simulate("workflow_dispatch", {"inputs": {"tag": tag}}, metadata, "0.30.0", tag)
        self.assertEqual(ran, {"source": ["refused"]})
        self.assert_nothing_ships(ran)

    def test_every_shipping_job_requires_the_source_mode(self):
        jobs = parse_jobs(WORKFLOW.read_text(encoding="utf-8"))
        self.assertEqual(set(jobs), SHIPPING_JOBS | {"source"})
        self.assertEqual(jobs["source"]["if"], "github.event_name != 'release' || github.event.release.prerelease")
        for name in SHIPPING_JOBS:
            self.assertIn("source", jobs[name]["needs"], name)
            self.assertIn("needs.source.outputs.mode", jobs[name]["if"] or "", name)
        for name in ("npm", "npm-not-configured"):
            self.assertTrue(jobs[name]["if"].startswith("needs.source.outputs.mode == 'development' && "), name)


class AllowedModeTests(unittest.TestCase):
    def test_development_prerelease_uploads_and_publishes_under_its_channel(self):
        for version, channel in (("0.31.0-dev.1", "dev"), ("0.31.0-nightly.20261112", "nightly")):
            payload, metadata, tag = release_event(version, prerelease=True)
            ran = simulate("release", payload, metadata, version, tag)
            self.assertEqual(set(ran), {"source", "build", "assets", "npm"}, version)
            self.assertIn(UPLOAD_STEP, ran["assets"])
            self.assertIn(PUBLISH_STEP, ran["npm"])
            self.assertEqual(release_mode.resolve("release", version, tag, metadata)["npm_tag"], channel)

    def test_npm_publish_passes_the_channel_dist_tag(self):
        jobs = parse_jobs(WORKFLOW.read_text(encoding="utf-8"))
        step = next(step for step in jobs["npm"]["steps"] if step["name"] == PUBLISH_STEP)
        self.assertIn("NPM_TAG: ${{ needs.source.outputs.npm_tag }}", step["body"])
        self.assertIn('publish-assembled.sh "$RUNNER_TEMP/npm-summary.json" --tag "$NPM_TAG" --publish', step["body"])

    def test_development_prerelease_without_npm_variable_only_reports_skip(self):
        payload, metadata, tag = release_event("0.31.0-dev.2", prerelease=True)
        ran = simulate("release", payload, metadata, "0.31.0-dev.2", tag, npm_enabled=False)
        self.assertEqual(set(ran), {"source", "build", "assets", "npm-not-configured"})

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
        self.assertIn("stable release", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.1", dict(dev, isPrerelease=False)))
        self.assertIn("draft", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.1", dict(dev, isDraft=True)))
        self.assertIn("not a development version", self.refused("release", "0.31.0-rc.1", "v0.31.0-rc.1", dict(dev, tagName="v0.31.0-rc.1")))
        self.assertIn("does not match", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.2", dev))
        self.assertIn("metadata", self.refused("release", "0.31.0-dev.1", "v0.31.0-dev.1", dict(dev, tagName="v0.30.0")))
        self.assertIn("no GitHub Release", self.refused("workflow_dispatch", "0.31.0-dev.1", "v0.31.0-dev.1", None))
        self.assertIn("unsupported event", self.refused("push", "0.31.0-dev.1", "v0.31.0-dev.1", dev))
        for version in ("0.31.0-dev", "0.31.0-devel.1", "01.2.3-dev.1", "0.31.0-dev.1+build"):
            self.refused("release", version, f"v{version}", dict(dev, tagName=f"v{version}"))

    def test_command_line_outputs_and_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            release = Path(directory) / "release.json"
            command = [sys.executable, str(ROOT / "scripts/native-release-mode.py"), "--event", "release", "--release", str(release)]
            release.write_text(json.dumps({"tagName": "v0.31.0-dev.3", "isDraft": False, "isPrerelease": True}))
            accepted = subprocess.run(
                command + ["--version", "0.31.0-dev.3", "--tag", "v0.31.0-dev.3"], capture_output=True, text=True
            )
            self.assertEqual(accepted.returncode, 0, accepted.stderr)
            self.assertEqual(accepted.stdout, "mode=development\ntag=v0.31.0-dev.3\nnpm_tag=dev\n")
            release.write_text(json.dumps({"tagName": "v0.31.0", "isDraft": False, "isPrerelease": False}))
            refused = subprocess.run(command + ["--version", "0.31.0", "--tag", "v0.31.0"], capture_output=True, text=True)
            self.assertEqual(refused.returncode, 1)
            self.assertEqual(refused.stdout, "")
            self.assertIn("::error::v0.31.0 is a stable release", refused.stderr)


class NpmDistTagTests(unittest.TestCase):
    def test_publish_assembled_refuses_latest_or_missing_dist_tag(self):
        for arguments in ([], ["--tag", "latest"], ["--tag"], ["--tag", "next"]):
            result = subprocess.run(
                ["bash", str(PUBLISH_ASSEMBLED), "/nonexistent/summary.json", *arguments, "--publish"],
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 64, arguments)
            self.assertIn("--tag must be dev or nightly", result.stderr)


if __name__ == "__main__":
    unittest.main()
