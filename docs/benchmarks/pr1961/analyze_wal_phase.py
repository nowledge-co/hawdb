# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0
"""Separate process CPU and existing WAL telemetry from wall-clock samples."""
import json
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
report = []
for receipt in json.loads((root / "run-receipts.json").read_text()):
    assert all(code == 0 for code in receipt["exit_codes"].values())
    variants = {name: [] for name in ["baseline", "candidate"]}
    for row in map(json.loads, (root / receipt["raw"]).read_text().splitlines()):
        if row["warmup"]:
            continue
        usage = row["process_usage"]
        phases = {}
        for phase in ["mutation", "checkpoint"]:
            before, after = usage[phase + "_before"], usage[phase + "_after"]
            delta = {field: after[field] - before[field] for field in before}
            assert all(value >= 0 for value in delta.values())
            phases[phase] = dict(
                wall_ns=row[phase + "_ns"],
                cpu_ns=delta["user_ns"] + delta["system_ns"],
                **delta
            )
        assert phases["mutation"]["wal_appends"] == 1
        assert phases["checkpoint"]["wal_appends"] == 0
        phases["mutation"]["outside_wal_fsync_ns"] = (
            phases["mutation"]["wall_ns"]
            - phases["mutation"]["wal_fsync_micros"] * 1000
        )
        variants[row["variant"]].append(
            dict(pair_index=row["pair_index"], phases=phases)
        )
    distributions = {}
    for variant, rows in variants.items():
        assert len(rows) == receipt["measured_pairs"]
        distributions[variant] = {
            phase: {
                field: dict(
                    median=statistics.median([r["phases"][phase][field] for r in rows]),
                    mean=statistics.mean([r["phases"][phase][field] for r in rows]),
                    maximum=max(r["phases"][phase][field] for r in rows),
                )
                for field in rows[0]["phases"][phase]
            }
            for phase in ["mutation", "checkpoint"]
        }
    report.append(
        dict(
            nodes=receipt["nodes"],
            edges=receipt["edges"],
            workload=receipt["workload"],
            measured_pairs=receipt["measured_pairs"],
            distributions=distributions,
            samples=variants,
        )
    )
print(json.dumps(report, indent=2))
