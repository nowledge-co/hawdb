#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Retain a fixed instrumented diagnostic, separate from the acceptance gates."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import time

from concurrent_writer_topology import detect

PAIRS = 5
CASES = {(readers, serial) for readers in [0, 2] for serial in [False, True]}
PHASES = ["host_gate", "begin", "query", "commit"]


def git(source, *args):
    return subprocess.check_output(["git", *args], cwd=source, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def manifest(source):
    return {
        name: digest(source / name)
        for name in sorted(git(source, "ls-files", "-z").split("\0"))
        if name
    }


def build(source, manifest_path, target, label, output):
    command = ["cargo", "bench", "--locked", "--manifest-path", str(manifest_path),
               "--bench", "concurrent_writers", "--no-run", "--message-format=json"]
    with (output / f"{label}-build.jsonl").open("x") as stdout, \
            (output / f"{label}-build.log").open("x") as stderr:
        subprocess.run(command, cwd=source,
                       env=dict(os.environ, CARGO_TARGET_DIR=str(target)),
                       stdout=stdout, stderr=stderr, check=True)
    binaries = [json.loads(line)["executable"]
                for line in (output / f"{label}-build.jsonl").read_text().splitlines()
                if json.loads(line).get("reason") == "compiler-artifact"
                and json.loads(line).get("target", {}).get("name") == "concurrent_writers"
                and json.loads(line).get("executable")]
    assert len(binaries) == 1, binaries
    binary = output / f"{label}-concurrent-writers"
    shutil.copy2(binaries[0], binary)
    return binary


def validate(rows):
    cases = {(row["readers"], row["serialized_control"]): row for row in rows}
    assert len(rows) == len(CASES) and set(cases) == CASES
    for row in rows:
        assert row["schema"] == "sort-writer-phase-diagnostic-v1"
        assert row["debug_assertions"] is False
        assert row["writers"] == 2 and row["commits"] == row["verified_rows"] == 256
        assert row["durable"] is False and row["grouped"] is False
        assert sorted(tx["id"] for tx in row["transactions"]) == list(range(256))
        for index, phase in enumerate(PHASES):
            samples = [tx["boundaries_ns"][index + 1] - tx["boundaries_ns"][index]
                       for tx in row["transactions"]]
            assert min(samples) >= 0 and samples == row[phase]["samples_ns"]
        assert row["coordinator"]["commit_execution"]["count"] == 256
        assert row["coordinator"]["group_durability"]["count"] == 0
        if row["readers"]:
            reads = row["read_workload"]
            assert reads["requests"] == 128 and reads["pinned_snapshot_verified"]
            assert min(reads[key] for key in ["reads_during_writer_phase",
                                             "writes_during_reader_phase",
                                             "intermediate_snapshot_reads"]) > 0
    return cases


def prepare(source, work, output, baseline_revision):
    work.mkdir(parents=True, exist_ok=False)
    output.mkdir(parents=True, exist_ok=False)
    baseline = work / "baseline"
    subprocess.run(["git", "worktree", "add", "--detach", str(baseline),
                    baseline_revision], cwd=source, check=True)
    shutil.copy2(source / "benches/concurrent_writers.rs",
                 baseline / "benches/concurrent_writers.rs")
    shutil.copytree(source / "benches/concurrent_writers",
                    baseline / "benches/concurrent_writers", dirs_exist_ok=True)
    write(output / "candidate-source.json", manifest(source))
    overlay = {str(path.relative_to(source)): digest(path)
               for path in sorted((source / "benches/concurrent_writers").rglob("*.rs"))}
    overlay["benches/concurrent_writers.rs"] = digest(source / "benches/concurrent_writers.rs")
    assert all(digest(baseline / name) == value for name, value in overlay.items())
    protocol = {
        "schema": "paired-sort-writer-phase-diagnostic-v1",
        "purpose": "descriptive diagnosis only; not performance acceptance",
        "candidate": git(source, "rev-parse", "HEAD"),
        "baseline": baseline_revision, "identical_benchmark_overlay": overlay,
        "baseline_overlay_patch": git(baseline, "diff", "HEAD"),
        "candidate_source_manifest_sha256": digest(output / "candidate-source.json"),
        "build_profile": "global bench profile; both Cargo invocations from candidate root",
        "cargo_config_sha256": digest(source / ".cargo/config.toml"),
        "cargo_lock_sha256": {label: digest(root / "Cargo.lock")
                              for label, root in [("baseline", baseline), ("candidate", source)]},
        "rustc": subprocess.check_output(["rustc", "-Vv"], cwd=source, text=True),
        "cpu": detect(), "pairs": PAIRS, "warmup_pairs": 1,
        "cases": sorted(CASES), "phases": PHASES,
        "scope": "in-memory, ungrouped, optimistic 2W with 0/2 readers; host-serialized controls",
        "order": "finish both builds, then alternate process order; retain every result",
        "limitations": "opt-in timings perturb execution; coordinator totals overlap; no confidence interval, acceptance waiver, phase heap or exclusivity claim",
    }
    write(output / "protocol.json", protocol)
    binaries = {}
    for label, root in [("baseline", baseline), ("candidate", source)]:
        print(f"Building {label}", flush=True)
        binaries[label] = build(source, root / "Cargo.toml", work / f"{label}-target",
                                label, output)
    protocol["binary_sha256"] = {label: digest(path) for label, path in binaries.items()}
    write(output / "protocol.json", protocol)
    return protocol, binaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--prepare-only", action="store_true",
                      help="Build and retain both binaries without running timing")
    mode.add_argument("--measure-prepared", action="store_true",
                      help="Measure previously prepared, unchanged source and binaries once")
    args = parser.parse_args()
    assert re.fullmatch(r"[0-9a-f]{40}", args.baseline_revision)
    source = Path(__file__).resolve().parents[1]
    work, output = args.work.resolve(), args.output.resolve()
    assert source not in work.parents and source not in output.parents
    assert not git(source, "status", "--porcelain"), "Commit the candidate before diagnosis"
    if args.measure_prepared:
        protocol = json.loads((output / "protocol.json").read_text())
        assert protocol["candidate"] == git(source, "rev-parse", "HEAD")
        assert protocol["baseline"] == args.baseline_revision
        assert protocol["candidate_source_manifest_sha256"] == digest(output / "candidate-source.json")
        assert manifest(source) == json.loads((output / "candidate-source.json").read_text())
        binaries = {label: output / f"{label}-concurrent-writers"
                    for label in ["baseline", "candidate"]}
        assert {label: digest(path) for label, path in binaries.items()} == protocol["binary_sha256"]
        assert not (output / "processes.json").exists(), "Prepared evidence was already measured"
    else:
        protocol, binaries = prepare(source, work, output, args.baseline_revision)
        if args.prepare_only:
            return
    results, processes, errors = {}, [], []
    for pair in range(-1, PAIRS):
        order = ["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]
        for label in order:
            name = f"{'warmup' if pair < 0 else f'pair-{pair}'}-{label}"
            record = {"pair": pair, "label": label, "command": [str(binaries[label]),
                      "--phase-diagnostic"], "load_before": os.getloadavg(),
                      "started_unix_ns": time.time_ns()}
            with (output / f"{name}.jsonl").open("x") as stdout, \
                    (output / f"{name}.stderr").open("x") as stderr:
                status = subprocess.run(record["command"], cwd=source, stdout=stdout,
                                        stderr=stderr, check=False)
            record.update(returncode=status.returncode, finished_unix_ns=time.time_ns(),
                          stdout_sha256=digest(output / f"{name}.jsonl"),
                          stderr_sha256=digest(output / f"{name}.stderr"))
            processes.append(record)
            write(output / "processes.json", processes)
            try:
                assert status.returncode == 0
                rows = [json.loads(line) for line in (output / f"{name}.jsonl").read_text().splitlines()]
                results[pair, label] = validate(rows)
            except (AssertionError, KeyError, ValueError) as error:
                errors.append({"process": name, "error": repr(error)})
            print(f"Retained {name}: exit {status.returncode}", flush=True)
    assert manifest(source) == json.loads((output / "candidate-source.json").read_text())
    endpoints = []
    if not errors:
        for readers, serial in sorted(CASES):
            for phase in PHASES:
                for percent in [50, 95, 99]:
                    ratios = [results[pair, "candidate"][readers, serial][phase][f"p{percent}_ns"]
                              / results[pair, "baseline"][readers, serial][phase][f"p{percent}_ns"]
                              for pair in range(PAIRS)]
                    endpoints.append({"readers": readers, "serialized_control": serial,
                                      "phase": phase, "percentile": percent,
                                      "paired_ratios": ratios, "median_ratio": statistics.median(ratios)})
    write(output / "receipt.json", {"diagnostic_complete": not errors, "errors": errors,
                                     "acceptance_claimed": False, "endpoints": endpoints,
                                     "protocol_sha256": digest(output / "protocol.json"),
                                     "processes_sha256": digest(output / "processes.json")})
    assert not errors, errors


if __name__ == "__main__":
    main()
