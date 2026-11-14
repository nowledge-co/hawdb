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

"""Measure a fixed 40-pair writer or mixed reader/writer cohort without sample removal."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time

from concurrent_writer_topology import detect


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--baseline-source", type=Path, required=True)
    parser.add_argument("--baseline-revision", default="a703cc0f18f549918c73f454cda521edb4f0fb13")
    parser.add_argument("--fixture-parent", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reopen-before-measure", action="store_true")
    parser.add_argument("--writers", type=int, default=1)
    parser.add_argument("--readers", type=int, default=0)
    parser.add_argument("--cpu-topology", type=Path)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    topology = None
    if args.cpu_topology:
        topology = json.loads(args.cpu_topology.read_text())
        if topology["cpu"] != detect() or [args.writers, args.readers] not in topology["cohorts"]:
            parser.error("CPU constraints changed or cohort is outside the frozen matrix")
    elif (args.writers, args.readers) != (1, 0):
        parser.error("concurrent qualification requires frozen CPU topology")
    binaries = {name: path.resolve(strict=True) for name, path in
                [("baseline", args.baseline), ("candidate", args.candidate)]}
    harness = source / "benches/concurrent_writers.rs"
    assert sha256(harness) == sha256(args.baseline_source / "benches/concurrent_writers.rs")
    helper = Path("benches/concurrent_writers/readers.rs")
    assert sha256(source / helper) == sha256(args.baseline_source / helper)
    metrics = [("commit_latency", name) for name in
               (["p50_ns", "p95_ns", "p99_ns"] if topology else ["p50_ns", "p95_ns"])]
    if topology:
        metrics.append(("workload", "elapsed_ns"))
    if args.readers:
        metrics.extend(("read_latency", name) for name in ["p50_ns", "p95_ns", "p99_ns"])
    endpoint_count = 3 * len(metrics)
    # Exact sign/order-statistic intervals, Bonferroni-adjusted within this
    # cohort. Freeze ranks before sampling; do not pool claims across cohorts.
    rank = max(k for k in range(1, 21) if
               endpoint_count * 2 * sum(math.comb(40, j) for j in range(k)) / 2**40 <= 0.05)
    args.fixture_parent.mkdir(parents=True, exist_ok=True)
    filesystem = subprocess.check_output(["df", "-T", str(args.fixture_parent)], text=True)
    if "tmpfs" in filesystem:
        parser.error("durable qualification requires a disk-backed fixture parent")
    args.output.mkdir(parents=True, exist_ok=False)
    source_names = sorted(set(name for name in subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=source, text=True
    ).split("\0") if name and Path(name).suffix in {".rs", ".toml", ".lock", ".bzl", ".bazel", ".py"}))
    source_manifest = {name: sha256(source / name) for name in source_names}
    (args.output / "candidate-source.json").write_text(json.dumps(source_manifest, indent=2) + "\n")
    protocol = {
        "schema": "concurrent-writer-strict-gate-v2",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "baseline_revision": args.baseline_revision,
        "candidate_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip(),
        "candidate_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True),
        "candidate_source_manifest_sha256": sha256(args.output / "candidate-source.json"),
        "candidate_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=source)).hexdigest(),
        "harness_sha256": sha256(harness),
        "reader_harness_sha256": sha256(source / helper),
        "binary_sha256": {name: sha256(path) for name, path in binaries.items()},
        "binary_paths": {name: str(path) for name, path in binaries.items()},
        "cargo_lock_sha256": {"baseline": sha256(args.baseline_source / "Cargo.lock"), "candidate": sha256(source / "Cargo.lock")},
        "effective_cargo_config_sha256": sha256(source / ".cargo/config.toml"),
        "build_profile": "bench; opt-level=3, codegen-units=1, thin LTO, strip=none; both builds invoked from candidate repository",
        "toolchain": "Rust 1.97.1", "default_features": True,
        "baseline_overlay": "identical writer harness and Cargo benchmark registration only",
        "measured_pairs": 40, "warm_up_pairs": 1, "interval_ranks_one_based": [rank, 41 - rank],
        "writers": args.writers, "readers": args.readers, "cpu_topology": topology,
        "fixture_profile": "reopened-captured-wal" if args.reopen_before_measure else "post-checkpoint-control",
        "reopen_before_measure": args.reopen_before_measure,
        "primary_endpoints": metrics, "primary_endpoint_count": endpoint_count,
        "primary_cases": "non-serialized API in memory, durable ungrouped, durable grouped",
        "pass_rule": "all unrounded upper ratio endpoints <= 1.0; no adaptive stopping or sample removal",
        "coverage": "95% simultaneous intervals within this cohort only; no joint matrix claim",
        "order": "baseline first for even pairs, candidate first for odd pairs; alternate --round 0/1",
        "platform": platform.platform(), "filesystem": filesystem,
        "limitations": "shared host; no exclusive machine, affinity, cache flush, or power-loss claim; median-ratio intervals assume independent pairs from continuous distributions",
    }
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    measured = {}
    with (args.output / "results.jsonl").open("x") as raw:
        for pair in range(-1, 40):
            order = ["baseline", "candidate"] if pair < 0 or pair % 2 == 0 else ["candidate", "baseline"]
            for revision in order:
                command = [str(binaries[revision]), "--writers", str(args.writers),
                           "--readers", str(args.readers), "--round", str(max(pair, 0) % 2),
                           "--fixture-parent", str(args.fixture_parent)]
                if args.reopen_before_measure:
                    command.append("--reopen-before-measure")
                started = time.monotonic()
                load = os.getloadavg()
                result = subprocess.run(command, text=True, capture_output=True, check=False)
                record = {"pair": pair, "warm_up": pair < 0, "revision": revision,
                          "command": command, "elapsed_seconds": time.monotonic() - started,
                          "load_before": load, "load_after": os.getloadavg(),
                          "returncode": result.returncode, "stderr": result.stderr}
                if result.returncode:
                    record["stdout"] = result.stdout
                    (args.output / "failure.json").write_text(json.dumps(record, indent=2) + "\n")
                    raise SystemExit("benchmark failed; original failure evidence retained")
                try:
                    rows = [json.loads(line) for line in result.stdout.splitlines()]
                    assert len(rows) == 6 and all(not row["smoke"] for row in rows)
                    assert len({(row["result"]["durable"], row["result"]["grouped"], row["result"]["serialized_control"]) for row in rows}) == 6
                    for row in rows:
                        case = row["result"]
                        assert case["commits"] == case["verified_rows"] == case["commit_latency"]["count"] == 256
                        assert case["reopen_verified"] == case["durable"]
                        assert case["reopened_before_measure"] == (args.reopen_before_measure and case["durable"])
                        assert (case["writers"], case["readers"]) == (args.writers, args.readers)
                        assert len(case["workers"]) == args.writers
                        assert sum(w["commits"] for w in case["workers"]) == 256
                        assert all(w["commits"] > 0 for w in case["workers"])
                        if args.readers:
                            reads = case["read_workload"]
                            assert reads["requests"] == reads["latency"]["count"] == args.readers * 64
                            assert len(reads["workers"]) == args.readers
                            assert all(w["requests"] == 64 for w in reads["workers"])
                            assert reads["pinned_snapshot_verified"]
                            assert min(reads["reads_during_writer_phase"], reads["writes_during_reader_phase"],
                                       reads["intermediate_snapshot_reads"]) > 0
                except (ValueError, KeyError, AssertionError):
                    record["stdout"] = result.stdout
                    (args.output / "failure.json").write_text(json.dumps(record, indent=2) + "\n")
                    raise
                record["cases"] = rows
                raw.write(json.dumps(record, separators=(",", ":")) + "\n")
                raw.flush()
                if pair >= 0:
                    measured[pair, revision] = rows
                print(f"pair={pair} revision={revision} cases=6 seconds={record['elapsed_seconds']:.3f}", flush=True)
    endpoints = []
    for durable, grouped in [(False, False), (True, False), (True, True)]:
        for family, metric in metrics:
            samples = {"baseline": [], "candidate": []}
            for pair in range(40):
                for revision in samples:
                    case = next(row["result"] for row in measured[pair, revision]
                                if row["result"]["durable"] == durable and row["result"]["grouped"] == grouped
                                and not row["result"]["serialized_control"])
                    container = case["read_workload"]["latency"] if family == "read_latency" else (
                        case if family == "workload" else case[family])
                    samples[revision].append(container[metric])
            ratios = [c / b for b, c in zip(samples["baseline"], samples["candidate"])]
            ordered = sorted(ratios)
            low, high = ordered[rank - 1], ordered[40 - rank]
            endpoints.append({"durable": durable, "grouped": grouped, "family": family, "metric": metric,
                              "baseline_median_ns": statistics.median(samples["baseline"]),
                              "candidate_median_ns": statistics.median(samples["candidate"]),
                              "paired_ratios": ratios, "median_ratio": statistics.median(ratios),
                              "ratio_interval": [low, high],
                              "disposition": "pass" if high <= 1 else "regression" if low > 1 else "inconclusive"})
    receipt = {"strict_gate_passed": all(row["disposition"] == "pass" for row in endpoints),
               "measured_cases": 480, "measured_commits": 122880, "measured_reopens": 320,
               "warm_up_cases": 12, "primary_endpoints": endpoints,
               "measured_reads": 480 * args.readers * 64,
               "writers": args.writers, "readers": args.readers,
               "cpu_constraints_unchanged": topology is None or topology["cpu"] == detect(),
               "protocol_sha256": sha256(args.output / "protocol.json"),
               "raw_sha256": sha256(args.output / "results.jsonl")}
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if not receipt["cpu_constraints_unchanged"]:
        raise SystemExit("CPU constraints changed; complete evidence retained but cohort is not qualified")
    print(json.dumps({"strict_gate_passed": receipt["strict_gate_passed"]}), flush=True)


if __name__ == "__main__":
    main()
