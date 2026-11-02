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

"""Build the fixed baseline and candidate before the manual writer qualification."""

import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import tarfile

from concurrent_writer_topology import counts, detect, matrix

BASELINE = "a703cc0f18f549918c73f454cda521edb4f0fb13"


def build(source, manifest, target, label, output):
    environment = dict(os.environ, CARGO_TARGET_DIR=str(target))
    command = ["cargo", "bench", "--locked", "--manifest-path", str(manifest),
               "--bench", "concurrent_writers", "--no-run", "--message-format=json"]
    with (output / f"{label}-build.jsonl").open("x") as stdout, \
            (output / f"{label}-build.log").open("x") as stderr:
        subprocess.run(command, cwd=source, env=environment, stdout=stdout,
                       stderr=stderr, check=True)
    executables = []
    for line in (output / f"{label}-build.jsonl").read_text().splitlines():
        item = json.loads(line)
        if (item.get("reason") == "compiler-artifact"
                and item["target"]["name"] == "concurrent_writers"
                and item.get("executable")):
            executables.append(Path(item["executable"]))
    assert len(executables) == 1, executables
    binary = output / f"{label}-concurrent-writers"
    shutil.copy2(executables[0], binary)
    return binary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixture-parent", type=Path, required=True)
    parser.add_argument("--baseline-revision", default=BASELINE)
    parser.add_argument("--include-captured-wal", action="store_true",
                        help="Run an additional fixed 40-pair cohort after reopening durable fixtures")
    parser.add_argument("--concurrency-matrix", action="store_true",
                        help="Qualify multiple writers and readers using affinity, physical cores and cgroup limits")
    parser.add_argument("--writer-counts", type=lambda value: counts(value, 1),
                        help="Comma-separated manual writer counts; may exceed the automatic CPU bound")
    parser.add_argument("--reader-counts", type=lambda value: counts(value, 0),
                        help="Comma-separated manual reader counts; default: 0, W, 2W for each writer count")
    args = parser.parse_args()
    concurrent = args.concurrency_matrix or args.writer_counts is not None or args.reader_counts is not None
    if args.writer_counts and max(args.writer_counts) > 256:
        parser.error("fixed 256-commit qualification requires at most 256 writers")
    if concurrent:
        print(json.dumps({"cpu_preflight": detect()}), flush=True)
    if not re.fullmatch(r"[0-9a-f]{40}", args.baseline_revision):
        parser.error("--baseline-revision must be a full lowercase commit SHA")
    baseline_revision = args.baseline_revision
    source = Path(__file__).resolve().parents[1]
    work = args.work.resolve()
    output = args.output.resolve()
    work.mkdir(parents=True, exist_ok=False)
    output.mkdir(parents=True, exist_ok=False)
    if subprocess.run(["git", "cat-file", "-e", baseline_revision + "^{commit}"],
                      cwd=source, capture_output=True, check=False).returncode:
        subprocess.run(["git", "fetch", "--no-tags", "--depth=1", "origin", baseline_revision],
                       cwd=source, check=True)
    archive = work / "baseline.tar"
    with archive.open("xb") as contents:
        subprocess.run(["git", "archive", baseline_revision], cwd=source, stdout=contents,
                       check=True)
    baseline = work / "baseline"
    baseline.mkdir()
    with tarfile.open(archive) as contents:
        contents.extractall(baseline, filter="data")
    shutil.copy2(source / "benches/concurrent_writers.rs",
                 baseline / "benches/concurrent_writers.rs")
    shutil.copytree(source / "benches/concurrent_writers", baseline / "benches/concurrent_writers",
                    dirs_exist_ok=True)
    manifest = baseline / "Cargo.toml"
    registration = '\n[[bench]]\nname = "concurrent_writers"\nharness = false\n'
    if 'name = "concurrent_writers"' not in manifest.read_text():
        with manifest.open("a") as file:
            file.write(registration)
    build_record = {"baseline": baseline_revision,
                    "candidate": subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], cwd=source, text=True).strip(),
                    "harness_sha256": hashlib.sha256(
                        (source / "benches/concurrent_writers.rs").read_bytes()).hexdigest(),
                    "reader_harness_sha256": hashlib.sha256(
                        (source / "benches/concurrent_writers/readers.rs").read_bytes()).hexdigest(),
                    "order": "build baseline, build candidate, then all timing; no concurrent builds",
                    "fixture_profiles": ["post-checkpoint-control", "reopened-captured-wal"] if args.include_captured_wal else ["post-checkpoint-control"],
                    "cohort_rule": "complete all declared cohorts despite inconclusive/regressed endpoints; never pool intervals across profiles or concurrency levels",
                    "comparison": "whole versions; identical harness, effective Cargo config and pinned toolchain; separate lockfiles and target directories"}
    (output / "build-protocol.json").write_text(json.dumps(build_record, indent=2) + "\n")
    binaries = {}
    for label, manifest in [("baseline", manifest), ("candidate", source / "Cargo.toml")]:
        print(f"Building {label}", flush=True)
        binaries[label] = build(source, manifest, work / f"{label}-target", label, output)
    profiles = [("measurement", False)]
    if args.include_captured_wal:
        profiles.append(("captured-measurement", True))
    if concurrent:
        cpu = detect()
        selected = matrix(cpu, args.writer_counts, args.reader_counts)
        if max(w for w, _ in selected) > 256:
            parser.error("fixed 256-commit qualification requires at most 256 writers")
        topology = {"cpu": cpu, "cohorts": selected,
                    "writer_counts_override": args.writer_counts,
                    "reader_counts_override": args.reader_counts,
                    "manual_override": args.writer_counts is not None or args.reader_counts is not None,
                    "exceeds_auto_writer_limit": any(w > cpu["maximum_writers"] for w, _ in selected),
                    "multi_writer_coverage": any(w > 1 for w, _ in selected),
                    "mixed_coverage": any(w > 1 and r > 1 for w, r in selected)}
        (output / "cpu-topology.json").write_text(json.dumps(topology, indent=2) + "\n")
        cohorts = [(f"{directory}/{writers}w-{readers}r", reopen, writers, readers)
                   for directory, reopen in profiles for writers, readers in selected]
    else:
        cohorts = [(directory, reopen, 1, 0) for directory, reopen in profiles]
    # Freeze the complete selected matrix before the first timed process.
    (output / "cohorts.json").write_text(json.dumps(cohorts, indent=2) + "\n")
    receipts = []
    for directory, reopen, writers, readers in cohorts:
        command = ["python3", str(source / "scripts/measure-concurrent-writer-gate.py"),
                    "--baseline", str(binaries["baseline"]),
                    "--candidate", str(binaries["candidate"]),
                    "--baseline-source", str(baseline),
                    "--baseline-revision", baseline_revision,
                    "--writers", str(writers), "--readers", str(readers),
                    "--fixture-parent", str(args.fixture_parent.resolve()),
                    "--output", str(output / directory)]
        if reopen:
            command.append("--reopen-before-measure")
        if concurrent:
            command.extend(["--cpu-topology", str(output / "cpu-topology.json")])
        subprocess.run(command, cwd=source, check=True)
        receipts.append(json.loads((output / directory / "receipt.json").read_text()))
    mixed_coverage = any(w > 1 and r > 1 for _, _, w, r in cohorts)
    (output / "matrix-receipt.json").write_text(json.dumps({
        "timing_complete": True, "cohorts": cohorts,
        "strict_gate_passed": all(receipt["strict_gate_passed"] for receipt in receipts),
        "multi_writer_coverage": any(w > 1 for _, _, w, _ in cohorts),
        "mixed_coverage": mixed_coverage,
        "acceptance_passed": all(r["strict_gate_passed"] for r in receipts) and (not concurrent or mixed_coverage),
        "measured_cases": sum(r["measured_cases"] for r in receipts),
        "measured_commits": sum(r["measured_commits"] for r in receipts),
        "measured_reads": sum(r["measured_reads"] for r in receipts),
        "measured_reopens": sum(r["measured_reopens"] for r in receipts),
        "coverage": "intervals apply separately within cohorts; no joint matrix confidence claim",
    }, indent=2) + "\n")
    if args.include_captured_wal:
        # Trace only after all untraced timing. Counts include setup and reopen;
        # retain those outputs separately and never use traced latency samples.
        tracer = shutil.which("strace")
        if tracer is None:
            raise SystemExit("captured-WAL qualification requires strace for separate syscall evidence")
        for directory, reopen, writers, readers in cohorts:
            for label, binary in binaries.items():
                command = [tracer, "-f", "-c", "-o", str(output / directory / f"{label}-syscalls.txt"),
                           str(binary), "--writers", str(writers), "--readers", str(readers), "--round", "0",
                           "--fixture-parent", str(args.fixture_parent.resolve())]
                if reopen:
                    command.append("--reopen-before-measure")
                with (output / directory / f"{label}-traced.jsonl").open("x") as stdout, \
                        (output / directory / f"{label}-strace.log").open("x") as stderr:
                    subprocess.run(command, cwd=source, stdout=stdout, stderr=stderr, check=True)
    if not all(receipt["strict_gate_passed"] for receipt in receipts):
        raise SystemExit("strict zero-increase gate not passed; complete evidence retained")
    if concurrent and not mixed_coverage:
        raise SystemExit("selected matrix lacks multiple writers/readers; evidence retained without concurrent acceptance")


if __name__ == "__main__":
    main()
