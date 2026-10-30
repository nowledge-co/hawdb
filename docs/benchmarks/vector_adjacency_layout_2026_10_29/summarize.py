#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Recompute paired summaries from the saved component measurements."""

import collections
import json
from pathlib import Path
import statistics
import sys


def summarize(path):
    data = json.loads(path.read_text())
    keys = {
        "vector": ["rows", "dimension", "exponent", "selected"],
        "adjacency": ["rows", "body_bytes", "properties", "cache_budget_bytes"],
    }
    groups = collections.defaultdict(list)
    assert len(data["runs"]) == 6
    assert {(run["probe"], run["cohort"]) for run in data["runs"]} == {
        (probe, cohort) for probe in keys for cohort in range(3)
    }
    for run in data["runs"]:
        probe = run["probe"]
        assert len(run["result"]["cases"]) == {"vector": 504, "adjacency": 336}[probe]
        pairs = collections.defaultdict(dict)
        for case in run["result"]["cases"]:
            shape = tuple(case[key] for key in keys[probe])
            arms = pairs[(shape, case["repetition"])]
            assert case["candidate"] not in arms
            arms[case["candidate"]] = case
        for (shape, repetition), arms in pairs.items():
            assert set(arms) == {False, True}
            checksum = "result_checksum" if probe == "adjacency" else "score_checksum"
            assert arms[False][checksum] == arms[True][checksum]
            groups[(probe, shape)].append(
                (run["cohort"], repetition, arms[False], arms[True])
            )

    result = []
    assert len(groups) == 60
    for (probe, shape), pairs in sorted(groups.items()):
        assert len(pairs) == 21
        assert {(cohort, repetition) for cohort, repetition, _, _ in pairs} == {
            (cohort, repetition) for cohort in range(3) for repetition in range(7)
        }
        ratios = [candidate["elapsed_ns"] / baseline["elapsed_ns"]
                  for _, _, baseline, candidate in pairs]
        group = {
            "probe": probe,
            "shape": dict(zip(keys[probe], shape)),
            "pairs": len(pairs),
            "median_paired_candidate_over_baseline": statistics.median(ratios),
            "min_paired_ratio": min(ratios),
            "max_paired_ratio": max(ratios),
            "per_process_median_ratios": [
                statistics.median(candidate["elapsed_ns"] / baseline["elapsed_ns"]
                                  for cohort, _, baseline, candidate in pairs if cohort == index)
                for index in range(3)
            ],
        }
        for index, name in [(2, "baseline"), (3, "candidate")]:
            cases = [pair[index] for pair in pairs]
            arm = {"median_ns_per_iteration": statistics.median(
                case["elapsed_ns"] / case.get("iterations", 1) for case in cases
            )}
            for field in cases[0]:
                if field in keys[probe] or field in {
                    "kind", "candidate", "repetition", "elapsed_ns", "iterations"
                }:
                    continue
                values = [case[field] for case in cases]
                if min(values) == max(values):
                    arm[field] = values[0]
                else:
                    arm[field + "_min"] = min(values)
                    arm[field + "_max"] = max(values)
            group[name] = arm
        result.append(group)
    return {
        "definition": "median of21 within-process, within-shape, within-repetition candidate/baseline elapsed ratios;7 pairs per process x3fresh processes; per-iteration elapsed medians also provided",
        "groups": result,
    }


if __name__ == "__main__":
    source = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name("measurements.json")
    print(json.dumps(summarize(source), indent=2))
