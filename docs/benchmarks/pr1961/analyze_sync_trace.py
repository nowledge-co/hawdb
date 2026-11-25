# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0
import collections
import json
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
report = []
for receipt in json.loads((root / "run-receipts.json").read_text()):
    assert all(code == 0 for code in receipt["exit_codes"].values())
    rows = [
        json.loads(line) for line in (root / receipt["raw"]).read_text().splitlines()
    ]
    variants = {}
    for variant in ["baseline", "candidate"]:
        measured = [r for r in rows if not r["warmup"] and r["variant"] == variant]
        assert len(measured) == receipt["measured_pairs"]
        pid = measured[0]["worker_pid"]
        events = []
        for line in (root / "sync-trace" / f"{pid}.tsv").read_text().splitlines():
            fields = line.split("\t", 7)
            events.append(
                dict(
                    start=int(fields[0]),
                    end=int(fields[1]),
                    fd=int(fields[2]),
                    command=int(fields[3]),
                    result=int(fields[4]),
                    error=int(fields[5]),
                    directory=int(fields[6]),
                    path=fields[7],
                )
            )
        samples = []
        groups = collections.defaultdict(list)
        for row in measured:
            selected = [
                e
                for e in events
                if row["step_started_ns"]
                <= e["start"]
                <= e["end"]
                <= row["step_ended_ns"]
            ]
            assert len(selected) > 10
            total = sum(e["end"] - e["start"] for e in selected)
            assert total <= row["step_ended_ns"] - row["step_started_ns"]
            for e in selected:
                path = e["path"].replace(str(root.resolve()), "<run>")
                fixture = path.find("-fixture/")
                suffix = path[fixture + 9 :] if fixture >= 0 else path
                if "/data/" in suffix:
                    suffix = suffix.split("/data/", 1)[1]
                if "/objects/" in suffix:
                    suffix = "objects/" + suffix.split("/objects/", 1)[1]
                if e["directory"] and "objects" not in suffix:
                    suffix = "ancestor-directory"
                if not e["directory"] and "/.staging/" in suffix:
                    suffix = "objects/.staging/<file>"
                if not e["directory"] and suffix.startswith("objects/"):
                    suffix = "/".join(suffix.split("/")[:2]) + "/<object>"
                if not e["directory"] and (
                    suffix.startswith("wal.") or "/wal." in suffix
                ):
                    suffix = "mutable-wal"
                key = ("directory:" if e["directory"] else "file:") + suffix
                groups[key].append(e["end"] - e["start"])
            samples.append(
                dict(
                    pair_index=row["pair_index"],
                    count=len(selected),
                    sync_ns=total,
                    directory_sync_ns=sum(
                        e["end"] - e["start"] for e in selected if e["directory"]
                    ),
                    wall_ns=row["checkpoint_ns"] + row["mutation_ns"],
                    longest=sorted(
                        selected, key=lambda e: e["end"] - e["start"], reverse=True
                    )[:3],
                )
            )
        variants[variant] = dict(
            counts=sorted(set(s["count"] for s in samples)),
            median_sync_ns=statistics.median(s["sync_ns"] for s in samples),
            median_directory_sync_ns=statistics.median(
                s["directory_sync_ns"] for s in samples
            ),
            median_wall_ns=statistics.median(s["wall_ns"] for s in samples),
            calls=sorted(
                [
                    dict(
                        path=key,
                        count=len(values),
                        mean_ns=statistics.mean(values),
                        maximum_ns=max(values),
                        sum_ns=sum(values),
                    )
                    for key, values in groups.items()
                ],
                key=lambda e: e["sum_ns"],
                reverse=True,
            ),
            samples=samples,
        )
    report.append(
        dict(
            group=receipt["group"],
            measured_pairs=receipt["measured_pairs"],
            variants=variants,
        )
    )
print(json.dumps(report, indent=2))
