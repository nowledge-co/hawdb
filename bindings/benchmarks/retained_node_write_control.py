# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Point writes with absent, closed, owned and source-backed row readers."""

import argparse
import gc
import hashlib
import json
from pathlib import Path
import resource
import statistics
import sys
import tempfile
import time

QUERY = "MATCH (n:Item {id: $id}) RETURN n AS node"
UPDATE = "MATCH (n:Item {id: $id}) SET n.score = $score"
MODES = ("ordinary", "closed_source", "owned_result", "retained_buffers")


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def percentile(values, probability):
    ordered = sorted(values)
    return ordered[max(0, min(len(ordered) - 1, int((len(ordered) - 1) * probability)))]


def empty(snapshot):
    return all(snapshot[key] == 0 for key in ("retained_bytes", "view_handles", "buffer_owners"))


def allocation_delta(before, after):
    if before is None:
        return None
    delta = {key: after[key] - before[key] for key in before
             if key not in ("live_requested_bytes", "process_peak_requested_bytes")}
    delta.update(live_requested_bytes_before=before["live_requested_bytes"],
                 live_requested_bytes_after=after["live_requested_bytes"],
                 process_peak_requested_bytes=after["process_peak_requested_bytes"])
    return delta


def run(args):
    sys.path.insert(0, args.package)
    import hawdb
    from hawdb import _hawdb as native

    profile = getattr(native, "_boundary_profile_snapshot", None)
    snapshot = profile if profile is not None else lambda: None
    pad = "x" * args.pad_bytes + "\x00\u754c\U0001f642"
    pad_bytes = pad.encode("utf-8")
    expected_pad = hashlib.sha256(pad_bytes).hexdigest()
    modes = MODES if args.mode == "all" else (args.mode,)
    records = []
    with tempfile.TemporaryDirectory(prefix="hawdb-node-write-") as temporary:
        databases = {}
        setup_ns = {}
        for mode in modes:
            started = time.perf_counter_ns()
            db = hawdb.open() if args.backend == "memory" else hawdb.open(Path(temporary) / mode)
            db.execute("CREATE NODE TABLE Item")
            db.execute("CREATE INDEX ON :Item(id)")
            for start in range(0, args.rows, 512):
                db.execute("UNWIND $rows AS row CREATE (:Item {id: row.id, score: row.score, pad: row.pad, nested: row.nested})",
                           {"rows": [{"id": key, "score": 0, "pad": pad,
                                      "nested": {"items": [key, None, True]}}
                                     for key in range(start, min(start + 512, args.rows))]})
            db.execute("CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE")
            # Initialize the same lazy retained governor for every mode via
            # an empty indexed cursor; ordinary modes retain no row or buffer.
            missing = db.execute_retained_node(QUERY, {"id": -1})
            require(missing.next_node() is None, "empty initialization found a row")
            missing.close()
            # Every ID warms the complete owned point path; retained modes also
            # warm every ID through the real node owner and buffer protocol.
            for key in range(args.rows):
                require(db.execute(QUERY, {"id": key}).fetchone()["node"]["id"] == key,
                        "warmup identity mismatch")
                if mode in ("closed_source", "retained_buffers"):
                    cursor = db.execute_retained_node(QUERY, {"id": key})
                    node = cursor.next_node()
                    field = node.field("pad")
                    buffer = memoryview(field)
                    require(buffer.readonly and buffer.nbytes == len(pad_bytes), "warmup buffer mismatch")
                    buffer.release()
                    field.close()
                    node.close()
                    require(cursor.next_node() is None, "warmup cardinality mismatch")
                    cursor.close()
            require(empty(db.retained_snapshot_copy()), "warmup retained leak")
            setup_ns[mode] = time.perf_counter_ns() - started
            databases[mode] = db
        try:
            for iteration in range(args.samples + 1):
                rotation = iteration % len(modes)
                order = modes[rotation:] + modes[:rotation]
                for mode in order:
                    db = databases[mode]
                    gc.collect()
                    require(empty(db.retained_snapshot_copy()), "pre-round retained leak")
                    initial = snapshot()
                    held = []
                    started = time.perf_counter_ns()
                    for key in range(args.hold_rows):
                        if mode == "owned_result":
                            held.append(db.execute(QUERY, {"id": key}).fetchone()["node"])
                        elif mode in ("closed_source", "retained_buffers"):
                            cursor = db.execute_retained_node(QUERY, {"id": key})
                            node = cursor.next_node()
                            pad_field = node.field("pad")
                            score_field = node.field("score")
                            buffers = (memoryview(pad_field), memoryview(score_field))
                            require(cursor.next_node() is None, "reader cardinality mismatch")
                            for owner in (pad_field, score_field, node):
                                owner.close()
                            cursor.close()
                            if mode == "retained_buffers":
                                held.append(buffers)
                            else:
                                for buffer in buffers:
                                    buffer.release()
                    reader_ns = time.perf_counter_ns() - started
                    reader_allocations = allocation_delta(initial, snapshot())
                    retained_before_write = db.retained_snapshot_copy()
                    require((retained_before_write["view_handles"] > 0) == (mode == "retained_buffers"),
                            "source ownership does not match mode")
                    before_write = snapshot()
                    point_ns = []
                    for key in range(args.hold_rows):
                        started = time.perf_counter_ns()
                        output = db.execute(UPDATE, {"id": key, "score": iteration + 1})
                        del output
                        point_ns.append(time.perf_counter_ns() - started)
                    after_write = snapshot()
                    retained_after_write = db.retained_snapshot_copy()
                    for key, old in enumerate(held):
                        if mode == "owned_result":
                            require(old["id"] == key and old["score"] == iteration,
                                    "owned old value changed")
                            require(hashlib.sha256(old["pad"].encode("utf-8")).hexdigest() == expected_pad,
                                    "owned old payload changed")
                            require(old["nested"]["items"] == [key, None, True], "owned nested mismatch")
                        else:
                            pad_buffer, score_buffer = old
                            require(pad_buffer.readonly and score_buffer.readonly and score_buffer.format == "q",
                                    "retained buffer layout mismatch")
                            require(hashlib.sha256(pad_buffer).hexdigest() == expected_pad,
                                    "original retained payload changed")
                            require(score_buffer[0] == iteration, "original retained scalar changed")
                            pad_buffer.release()
                            score_buffer.release()
                    if held:
                        del old
                    held.clear()
                    gc.collect()
                    released = snapshot()
                    final_retained = db.retained_snapshot_copy()
                    require(empty(final_retained), "post-round retained leak")
                    # Verification is outside all reader/write timers. Visit
                    # every changed ID and the final unmodified ID, not a scan.
                    for key in (*range(args.hold_rows), args.rows - 1):
                        row = db.execute(QUERY, {"id": key}).fetchone()["node"]
                        expected_score = iteration + 1 if key < args.hold_rows else 0
                        require(row["id"] == key and row["score"] == expected_score,
                                "committed new value mismatch")
                        require(row["pad"] == pad and row["nested"]["items"] == [key, None, True],
                                "committed payload mismatch")
                    del row
                    records.append({"iteration": iteration, "discarded": iteration == 0, "mode": mode,
                                    "reader_boundary_ns": reader_ns, "write_boundary_ns": sum(point_ns),
                                    "point_write_ns": point_ns, "updated_rows": args.hold_rows,
                                    "reader_native_allocations": reader_allocations,
                                    "write_native_allocations": allocation_delta(before_write, after_write),
                                    "native_after_release": released,
                                    "retained_before_write": retained_before_write,
                                    "retained_after_write": retained_after_write,
                                    "retained_after_release": final_retained,
                                    "verified_old_score": iteration if mode in ("owned_result", "retained_buffers") else None,
                                    "verified_new_score": iteration + 1,
                                    "payload_sha256": expected_pad})
        finally:
            for db in databases.values():
                db.close()
    summary = {}
    for mode in modes:
        selected = [row for row in records if row["mode"] == mode and not row["discarded"]]
        points = [value for row in selected for value in row["point_write_ns"]]
        summary[mode] = {"median_round_write_ns": statistics.median(row["write_boundary_ns"] for row in selected),
                         "point_samples": len(points), "point_p50_ns": percentile(points, .50),
                         "point_p95_ns": percentile(points, .95), "point_p99_ns": percentile(points, .99),
                         "maximum_retained_bytes": max(row["retained_before_write"]["retained_bytes"] for row in selected)}
        if profile is not None:
            summary[mode]["median_write_native_allocated_bytes"] = statistics.median(row["write_native_allocations"]["allocated_bytes"] for row in selected)
            summary[mode]["median_reader_native_allocated_bytes"] = statistics.median(row["reader_native_allocations"]["allocated_bytes"] for row in selected)
    return {"status": "ok", **vars(args), "profiled": profile is not None,
            "records": records, "summary": summary, "setup_ns": setup_ns,
            "durability": "SyncOnEveryWrite", "prefetch": 0,
            "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024),
            "setup_unique_constraint": "Item(id)",
            "scope": "Same-source point SET with held old row owners. Reader setup and old/new checks are outside writes. Instrumented allocations and times are distinct from ordinary runs. RSS includes all fixtures/setup; no mode-specific RSS attribution or comprehensive performance qualification."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--pad-bytes", type=int, default=4096)
    parser.add_argument("--hold-rows", type=int, default=32)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--backend", choices=("memory", "file"), required=True)
    parser.add_argument("--mode", choices=("all", *MODES), default="all")
    args = parser.parse_args()
    if args.rows < 2 or args.pad_bytes < 0 or args.samples < 1 or not 0 < args.hold_rows < args.rows:
        parser.error("positive samples/hold-rows, rows > hold-rows and nonnegative pad-bytes are required")
    print(json.dumps(run(args)))


if __name__ == "__main__":
    main()
