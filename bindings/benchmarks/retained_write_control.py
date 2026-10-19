# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Ordinary writes with absent, closed or slow retained readers."""

import argparse
import gc
import json
from pathlib import Path
import resource
import statistics
import sys
import tempfile
import time

import hawdb

QUERY = "MATCH (n:Item) WHERE n.score >= $min RETURN n.score AS score, id(n) AS identity"
UPDATE = "MATCH (n:Item) SET n.score = $score"
MODES = ("ordinary", "closed_cursor", "open_cursor")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def check_values(db, expected, rows):
    result = db.execute("MATCH (n:Item) RETURN n.score AS score")
    count = 0
    for row in result:
        require(type(row["score"]) is int and row["score"] == expected,
                "post-write score mismatch")
        count += 1
    require(count == rows, "post-write row-count mismatch")


def consume_old_source(cursor, expected, rows):
    count = 0
    while True:
        batch = cursor.next_batch()
        if batch is None:
            break
        column = batch.column(0)
        values = memoryview(column)
        selection = batch.selection()
        indices = memoryview(selection)
        for index in indices:
            require(values[index] == expected, "old snapshot changed after write")
            count += 1
        values.release()
        indices.release()
        selection.close()
        column.close()
        batch.close()
    require(count == rows, "old snapshot row-count mismatch")
    require(cursor.profile_copy()["source_pinned_capacity_bytes"] == 0,
            "source capacity remains charged at EOF")
    cursor.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--pad-bytes", type=int, default=4096)
    parser.add_argument("--samples", type=int, default=7)
    parser.add_argument("--backend", choices=("memory", "file"), required=True)
    parser.add_argument("--mode", choices=("all", *MODES), default="all")
    args = parser.parse_args()
    if args.rows <= 0 or args.pad_bytes < 0 or args.samples <= 0:
        parser.error("rows/samples must be positive and pad-bytes nonnegative")
    modes = MODES if args.mode == "all" else (args.mode,)
    records = []
    with tempfile.TemporaryDirectory(prefix="hawdb-retained-write-") as temporary:
        databases = {}
        for mode in modes:
            db = hawdb.open() if args.backend == "memory" else hawdb.open(Path(temporary) / mode)
            db.execute("CREATE NODE TABLE Item")
            db.execute("CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT")
            for start in range(0, args.rows, 512):
                db.execute("UNWIND $rows AS row CREATE (:Item {score: row.score, pad: row.pad})",
                           {"rows": [{"score": 0, "pad": "x" * args.pad_bytes}
                                     for _ in range(start, min(start + 512, args.rows))]})
            databases[mode] = db
        try:
            for iteration in range(args.samples + 1):
                rotation = iteration % len(modes)
                order = modes[rotation:] + modes[:rotation]
                for mode in order:
                    db = databases[mode]
                    gc.collect()
                    cursor = None
                    source = None
                    if mode != "ordinary":
                        cursor = db.execute_retained(QUERY, {"min": 0})
                        source = cursor.profile_copy()
                        require(source["visited_rows"] == 0,
                                "source advanced before write")
                        require(source["source_pinned_capacity_bytes"] >= args.rows * args.pad_bytes,
                                "unrequested source capacity omitted")
                        if mode == "closed_cursor":
                            cursor.close()
                            cursor = None
                    expected = iteration + 1
                    started = time.perf_counter_ns()
                    result = db.execute(UPDATE, {"score": expected})
                    del result
                    write_ns = time.perf_counter_ns() - started
                    if cursor is not None:
                        consume_old_source(cursor, iteration, args.rows)
                    check_values(db, expected, args.rows)
                    snapshot = db.retained_snapshot_copy() if hasattr(db, "retained_snapshot_copy") else None
                    require(snapshot is None or all(snapshot[k] == 0 for k in ("retained_bytes", "view_handles", "buffer_owners")),
                            "retained resources leaked after write control")
                    records.append({"iteration": iteration, "discarded": iteration == 0,
                                    "mode": mode, "write_boundary_ns": write_ns,
                                    "updated_rows": args.rows, "verified_score": expected,
                                    "source_before_write": source, "retained_after": snapshot})
        finally:
            for db in databases.values():
                db.close()
    measured = [r for r in records if not r["discarded"]]
    summary = {mode: {"median_write_boundary_ns": statistics.median(r["write_boundary_ns"] for r in measured if r["mode"] == mode),
                      "minimum_write_boundary_ns": min(r["write_boundary_ns"] for r in measured if r["mode"] == mode)} for mode in modes}
    print(json.dumps({"status": "ok", **vars(args), "records": records, "summary": summary,
                      "durability": "SyncOnEveryWrite", "prefetch": 0,
                      "shape": "numeric score and unrequested fixed-size string per row",
                      "scope": "Bulk ordinary SET; setup, source admission, old snapshot consumption and post-write verification outside the write timer. No Arrow API is called.",
                      "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)}))


if __name__ == "__main__":
    main()
