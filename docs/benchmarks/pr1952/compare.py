# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Manual paired comparison; freeze executables and retain all measured responses."""

import argparse
import hashlib
import json
import math
import pathlib
import random
import selectors
import shutil
import statistics
import subprocess
import time


def percentile(values, fraction):
    return sorted(values)[math.ceil(fraction * len(values)) - 1]


class Worker:
    def __init__(self, command):
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True, bufsize=1)

    def call(self, request):
        self.process.stdin.write(json.dumps(request, ensure_ascii=False) + "\n")
        self.process.stdin.flush()
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            if not selector.select(120):
                raise RuntimeError("worker timed out")
        line = self.process.stdout.readline()
        if not line:
            raise RuntimeError(self.process.stderr.read())
        value = json.loads(line)
        if "error" in value:
            raise RuntimeError(value["error"])
        return value

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=30)
        errors = self.process.stderr.read()
        if self.process.returncode:
            raise RuntimeError(errors)


def semantic(value, phase):
    if phase == "checkpoint":
        return {key: value[key] for key in ("rows", "branch", "precheckpoint", "verified_nodes")}
    return {key: value[key] for key in ("hits", "total_hits", "truncated", "document_count",
                                       "filtered_document_count")}


def summary(rows):
    groups = {}
    for row in rows:
        groups.setdefault(row["case"], [[], []])[row["arm"]].append(row)
    output = []
    for case, arms in groups.items():
        times = [[row["elapsed_ns"] / 1e6 for row in arm] for arm in arms]
        ratios = [right / left for left, right in zip(*times)]
        rng = random.Random(1952)
        boot = [statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(2000)]
        output.append({"case": case, "pairs": len(ratios),
                       "baseline_p50_ms": statistics.median(times[0]),
                       "candidate_p50_ms": statistics.median(times[1]),
                       "baseline_p95_ms": percentile(times[0], .95),
                       "candidate_p95_ms": percentile(times[1], .95),
                       "baseline_p99_ms": percentile(times[0], .99),
                       "candidate_p99_ms": percentile(times[1], .99),
                       "paired_median_ratio": statistics.median(ratios),
                       "paired_median_ratio_bootstrap_95": [percentile(boot, .025), percentile(boot, .975)],
                       "lexical_bytes": [sum(sum(report["posting_bytes_read"] for report in row.get("retrievers", [])) for row in arm) for arm in arms]})
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["search", "scifact", "checkpoint"])
    parser.add_argument("baseline", type=pathlib.Path)
    parser.add_argument("candidate", type=pathlib.Path)
    parser.add_argument("fixture", type=pathlib.Path)
    parser.add_argument("output", type=pathlib.Path)
    parser.add_argument("--queries", type=pathlib.Path)
    parser.add_argument("--repetitions", type=int, default=51)
    parser.add_argument("--case", help="repeat one named request shape without changing its parameters")
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")
    args.output.mkdir(parents=True, exist_ok=False)
    binaries = []
    for index, original in enumerate([args.baseline, args.candidate]):
        frozen = args.output / f"worker-{index}"
        shutil.copy2(original, frozen)
        binaries.append({"path": str(frozen.resolve()), "sha256": hashlib.sha256(frozen.read_bytes()).hexdigest()})
    (args.output / "binaries.json").write_text(json.dumps(binaries, indent=2) + "\n")
    if args.phase == "checkpoint":
        cases = [(f"nodes{rows}-prior{prior}", {"rows": rows, "precheckpoint": prior, "branch": True})
                 for rows in [0, 1, 128, 2000] for prior in [False, True]]
        commands = [[binary["path"], str(args.fixture.resolve()) + f"-{index}"] for index, binary in enumerate(binaries)]
    elif args.phase == "scifact":
        cases = [(query["_id"], {"query": query["text"]}) for query in json.loads(args.queries.read_text())]
        commands = [[binary["path"], "worker", str(args.fixture.resolve())] for binary in binaries]
    else:
        cases = [(f"{prefix}-{count}", {"query": " ".join(f"{prefix}{i:03}" for i in range(count))})
                 for prefix in ["sparse", "dense"] for count in [1, 4, 8, 16]]
        cases += [("repeated", {"query": "database database storage recovery"}),
                  ("chinese", {"query": "\u4e2d\u6587\u6570\u636e\u5e93"}),
                  ("filtered", {"query": "sparse000 sparse017 storage", "scope": "1", "offset": 3}),
                  ("absent", {"query": "no_such_term_ever"}),
                  ("mixed", {"query": "dense001 sparse003 unique000007 absent", "offset": 7})]
        commands = [[binary["path"], "worker", str(args.fixture.resolve())] for binary in binaries]
    if args.case:
        cases = [(name, request) for name, request in cases if name == args.case]
        if not cases:
            raise ValueError("unknown request shape")
    workers = [Worker(command) for command in commands]
    rows = []
    started = time.time()
    try:
        with (args.output / "samples.jsonl").open("w") as raw:
            for case, request in cases:
                for index in range(args.repetitions + 3):
                    pair = {}
                    for arm in ([0, 1] if index % 2 == 0 else [1, 0]):
                        value = workers[arm].call(request)
                        pair[arm] = value
                        if index >= 3:
                            row = {"case": case, "sample": index - 3, "arm": arm,
                                   "request": request, **value}
                            raw.write(json.dumps(row, ensure_ascii=False) + "\n")
                            raw.flush()
                            rows.append(row)
                    if semantic(pair[0], args.phase) != semantic(pair[1], args.phase):
                        raise AssertionError((case, request, pair))
                result = summary([row for row in rows if row["case"] == case])[0]
                print(json.dumps(result), flush=True)
                (args.output / "summary.json").write_text(json.dumps(summary(rows), indent=2) + "\n")
    finally:
        for worker in workers:
            worker.close()
    (args.output / "receipt.json").write_text(json.dumps({"phase": args.phase, "pairs": len(rows) // 2,
        "warmup_pairs": len(cases) * 3, "elapsed_s": time.time() - started, "all_results_equal": True}, indent=2) + "\n")


if __name__ == "__main__":
    main()
