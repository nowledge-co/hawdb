#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Manual paired component comparison; unchanged native bench-one harness."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def compiler_count():
    commands = subprocess.check_output(["ps", "-axo", "comm="], text=True)
    return sum(Path(command.strip()).name in ("rustc", "clippy-driver") for command in commands.splitlines())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--journals", type=Path, required=True)
    parser.add_argument("--pairs", type=int, default=5)
    args = parser.parse_args()
    assert args.pairs >= 5
    args.output.mkdir(parents=True, exist_ok=True)
    args.journals.mkdir(parents=True, exist_ok=False)
    binaries = {"baseline": args.baseline.resolve(), "candidate": args.candidate.resolve()}
    protocol = {
        "pairs": args.pairs,
        "layouts": ["versioned", "hybrid", "cow"],
        "workloads": [[16, 2], [512, 24], [2048, 48]],
        "order": "rotate layouts and reverse workload/executable order on odd pairs",
        "processes": "new process and journal per executable/layout/workload/pair",
        "profile": "repository release; Rust 1.97.1; no custom compiler flags",
        "platform": platform.platform(),
        "durability": "sync_all per complete batch; identical journal bytes required per pair",
        "samples": "4096 point reads and 32 scans per retained/current snapshot; scan p99 is a maximum",
        "binaries": {
            name: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
            for name, path in binaries.items()
        },
        "limits": "Shared host, five pairs, memory index; no production or power-loss qualification",
        "compiler_observation": "Counts before/after each child; boundary observations do not prove continuous host quiescence",
    }
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    samples = {}
    with gzip.open(args.output / "results.jsonl.gz", "wt") as raw:
        for pair in range(args.pairs):
            workloads = protocol["workloads"][:: -1 if pair % 2 else 1]
            order = list(binaries)[:: -1 if pair % 2 else 1]
            layouts = protocol["layouts"]
            layouts = layouts[pair % 3 :] + layouts[: pair % 3]
            for keys, rounds in workloads:
                for layout in layouts:
                    journal_hashes = []
                    for name in order:
                        path = args.journals / f"{pair}-{keys}-{rounds}-{layout}-{name}.journal"
                        command = [str(binaries[name]), "bench-one", str(path), layout, str(keys), str(rounds)]
                        started = time.monotonic_ns()
                        load = os.getloadavg()
                        compilers_before = compiler_count()
                        result = subprocess.run(command, capture_output=True, text=True, timeout=240)
                        receipt = {
                            "pair": pair, "revision": name, "layout": layout, "keys": keys,
                            "rounds": rounds, "command": command, "load_before": load,
                            "elapsed_ns": time.monotonic_ns() - started,
                            "returncode": result.returncode, "stderr": result.stderr,
                            "stdout": result.stdout,
                            "rust_compilers_before": compilers_before,
                            "rust_compilers_after": compiler_count(),
                        }
                        raw.write(json.dumps(receipt) + "\n")
                        raw.flush()
                        assert result.returncode == 0, receipt
                        report = json.loads(result.stdout)
                        assert report["profile"] == "release"
                        assert report["keys"] == keys and report["update_rounds"] == rounds
                        assert report["current_get"]["operations"] == 4096
                        assert report["old_get"]["operations"] == 4096
                        assert report["current_full_scan"]["operations"] == 32
                        assert report["old_full_scan"]["operations"] == 32
                        digest = hashlib.sha256(path.read_bytes()).hexdigest()
                        journal_hashes.append(digest)
                        samples.setdefault((keys, rounds, layout), {}).setdefault(pair, {})[name] = {
                            "report": report, "journal_sha256": digest
                        }
                    assert len(set(journal_hashes)) == 1, (pair, keys, rounds, layout)
            print(f"Completed pair {pair + 1}/{args.pairs}", flush=True)
    summary = []
    phases = ["commit_batches", "prepare_and_sync", "index_install", "current_get", "old_get", "current_full_scan", "old_full_scan"]
    for (keys, rounds, layout), pairs in samples.items():
        row = {"keys": keys, "rounds": rounds, "layout": layout, "pairs": len(pairs)}
        fields = [(phase, q) for phase in phases for q in ["p50_ns", "p95_ns", "p99_ns", "total_ns"]]
        fields += [(name,) for name in ["reopen_ns", "resident_bytes", "process_peak_bytes", "journal_bytes"]]
        for path in fields:
            values = {name: [] for name in binaries}
            ratios = []
            for pair in pairs.values():
                current = {}
                for name in binaries:
                    value = pair[name]["report"]
                    for component in path:
                        value = value[component]
                    current[name] = value
                    values[name].append(value)
                if all(current[name] is not None and current[name] > 0 for name in binaries):
                    ratios.append(current["candidate"] / current["baseline"])
            row["/".join(path)] = {
                "baseline_median": statistics.median(v for v in values["baseline"] if v is not None) if any(v is not None for v in values["baseline"]) else None,
                "candidate_median": statistics.median(v for v in values["candidate"] if v is not None) if any(v is not None for v in values["candidate"]) else None,
                "paired_ratio_median": statistics.median(ratios) if ratios else None,
                "paired_ratios": ratios,
            }
        row["journal_sha256_by_pair"] = {pair: values["baseline"]["journal_sha256"] for pair, values in pairs.items()}
        summary.append(row)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
