# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0
import json
import math
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])


def stats(values):
    v = sorted(values)
    return dict(
        p50=statistics.median(v),
        p95=v[math.ceil(len(v) * 0.95) - 1],
        p99=v[math.ceil(len(v) * 0.99) - 1],
        minimum=v[0],
        maximum=v[-1],
    )


report = []
for receipt in json.loads((root / "run-receipts.json").read_text()):
    assert all(v == 0 for v in receipt["exit_codes"].values())
    rows = [
        json.loads(line)
        for line in (root / receipt["raw"]).read_text().splitlines()
        if not json.loads(line)["warmup"]
    ]
    pairs = {}
    groups = {v: [] for v in ["baseline", "candidate"]}
    for row in rows:
        assert row["variant"] not in pairs.setdefault(row["pair_index"], {})
        pairs[row["pair_index"]][row["variant"]] = row
        groups[row["variant"]].append(row["elapsed_ns"])
    assert len(pairs) == receipt["measured_pairs"]
    changes = [
        (
            i,
            (p["candidate"]["elapsed_ns"] / p["baseline"]["elapsed_ns"] - 1) * 100,
            p["baseline"]["position"],
        )
        for i, p in sorted(pairs.items())
    ]
    assert all(len(p) == 2 for p in pairs.values())
    report.append(
        dict(
            group=receipt["group"],
            distributions={v: stats(ns) for v, ns in groups.items()},
            paired_percent=stats([c for _, c, _ in changes]),
            first_half_median=statistics.median(
                [c for _, c, _ in changes[: len(changes) // 2]]
            ),
            second_half_median=statistics.median(
                [c for _, c, _ in changes[len(changes) // 2 :]]
            ),
            baseline_first_median=statistics.median(
                [c for _, c, p in changes if p == 0]
            ),
            candidate_first_median=statistics.median(
                [c for _, c, p in changes if p == 1]
            ),
        )
    )
print(json.dumps(report, indent=2))
