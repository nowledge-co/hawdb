#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Retain fixed paired single-column controls after both writer builds finish."""

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import subprocess
import time

from concurrent_writer_topology import detect

PAIRS = 40
KINDS = ["integer", "float", "string", "null", "mixed"]
CASES = {(rows, kind) for rows in [256, 4096] for kind in KINDS}


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def validate(rows):
    assert len(rows) == len(CASES)
    cases = {(row["rows"], row["kind"]): row for row in rows}
    assert set(cases) == CASES
    for case in cases.values():
        assert case["schema"] == "sort-key-preparation-control-v1"
        assert case["typed_rows_exact"] and case["iterations"] == 8
        assert "SortExec" in case["physical_plan"]
        assert 'Column("sort_value")' in case["physical_plan"]
        assert case["warm_up_ns"] > 0 and case["setup_ns"] > 0
        latency = case["latency"]
        samples = sorted(latency["samples_ns"])
        assert len(samples) == latency["count"] == 8 and samples[0] > 0
        for percent in [50, 95, 99]:
            assert (
                latency[f"p{percent}_ns"]
                == samples[(len(samples) * percent + 99) // 100 - 1]
            )
        assert latency["max_ns"] == samples[-1]
    return cases


def interval_ranks(endpoints):
    # Simultaneous sign-test bounds for the declared control endpoint family.
    tail = 0.05 / (2 * endpoints)
    rank = max(
        k
        for k in range(1, PAIRS // 2 + 1)
        if sum(math.comb(PAIRS, j) for j in range(k)) / 2**PAIRS <= tail
    )
    return [rank, PAIRS - rank + 1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--writer-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    source = Path(__file__).resolve().parents[1]
    evidence = args.writer_evidence.resolve()
    output = args.output.resolve()
    build = json.loads((evidence / "build-protocol.json").read_text())
    matrix = json.loads((evidence / "matrix-receipt.json").read_text())
    assert matrix["timing_complete"], "Earlier native qualification is incomplete"
    head = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=source, text=True
    ).strip()
    assert head == build["candidate"]
    assert not subprocess.check_output(["git", "diff", "HEAD"], cwd=source)
    binaries = {
        revision: evidence / (revision + "-concurrent-writers")
        for revision in ["baseline", "candidate"]
    }
    assert all(path.is_file() for path in binaries.values())
    output.mkdir(parents=True, exist_ok=False)
    ranks = interval_ranks(len(CASES) * 3)
    toolchain = subprocess.check_output(
        ["rustc", "--version"], cwd=source, text=True
    ).strip()
    assert toolchain.startswith("rustc 1.97.1 ")
    protocol = {
        "schema": "paired-sort-key-controls-v1",
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "source": head,
        "baseline": build["baseline"],
        "build_protocol_sha256": digest(evidence / "build-protocol.json"),
        "binary_sha256": {key: digest(path) for key, path in binaries.items()},
        "script_sha256": digest(Path(__file__)),
        "harness_sha256": digest(source / "benches/concurrent_writers.rs"),
        "control_sha256": digest(source / "benches/concurrent_writers/sort_control.rs"),
        "toolchain": toolchain,
        "cpu": detect(),
        "scope": "Read-only resident controls; no physical exclusivity or whole-matrix claim",
        "cases": sorted(CASES),
        "measured_pairs": PAIRS,
        "warm_up_pairs": 1,
        "queries_per_case": 8,
        "warm_up_queries_per_case": 1,
        "endpoint_count": len(CASES) * 3,
        "interval_ranks_one_based": ranks,
        "threshold_ratio": 1.0,
    }
    write(output / "protocol.json", protocol)
    measured = {}
    raw = output / "results.jsonl"
    with raw.open("x") as log:
        for pair in range(-1, PAIRS):
            order = ["baseline", "candidate"]
            if pair % 2:
                order.reverse()
            for revision in order:
                command = [str(binaries[revision]), "--sort-key-control"]
                started = time.monotonic()
                load_before = os.getloadavg()
                result = subprocess.run(
                    command, cwd=source, capture_output=True, text=True
                )
                parse_error = None
                try:
                    rows = [json.loads(line) for line in result.stdout.splitlines()]
                except ValueError as error:
                    rows = []
                    parse_error = str(error)
                record = {
                    "pair": pair,
                    "warm_up": pair < 0,
                    "revision": revision,
                    "command": command,
                    "returncode": result.returncode,
                    "stderr": result.stderr,
                    "stdout": result.stdout,
                    "parse_error": parse_error,
                    "elapsed_seconds": time.monotonic() - started,
                    "load_before": load_before,
                    "load_after": os.getloadavg(),
                    "cases": rows,
                }
                log.write(json.dumps(record) + "\n")
                log.flush()
                assert result.returncode == 0, "Native error retained; no retry"
                assert (
                    parse_error is None
                ), "Native output retained; invalid control JSON"
                cases = validate(rows)
                if pair >= 0:
                    measured[pair, revision] = cases
                print(f"pair={pair} revision={revision} cases={len(cases)}", flush=True)
    endpoints = []
    for key in sorted(CASES):
        for pair in range(PAIRS):
            assert (
                measured[pair, "baseline"][key]["checksum"]
                == measured[pair, "candidate"][key]["checksum"]
            )
        for percent in [50, 95, 99]:
            metric = f"p{percent}_ns"
            ratios = [
                measured[pair, "candidate"][key]["latency"][metric]
                / measured[pair, "baseline"][key]["latency"][metric]
                for pair in range(PAIRS)
            ]
            ordered = sorted(ratios)
            lower, upper = [ordered[rank - 1] for rank in ranks]
            disposition = (
                "pass" if upper <= 1 else "regression" if lower > 1 else "inconclusive"
            )
            endpoints.append(
                {
                    "rows": key[0],
                    "kind": key[1],
                    "metric": metric,
                    "paired_ratios": ratios,
                    "median_ratio": statistics.median(ratios),
                    "ratio_interval": [lower, upper],
                    "disposition": disposition,
                }
            )
    passed = all(row["disposition"] == "pass" for row in endpoints)
    write(
        output / "receipt.json",
        {
            "source": head,
            "baseline": build["baseline"],
            "protocol_sha256": digest(output / "protocol.json"),
            "raw_sha256": digest(raw),
            "measured_cases": PAIRS * 2 * len(CASES),
            "measured_query_samples": PAIRS * 2 * len(CASES) * 8,
            "endpoints": endpoints,
            "dispositions": dict(Counter(row["disposition"] for row in endpoints)),
            "strict_zero_increase_passed": passed,
            "complete_concurrent_acceptance": False,
        },
    )
    if not passed:
        raise SystemExit(
            "sort-key zero-increase controls not passed; all evidence retained"
        )


if __name__ == "__main__":
    main()
