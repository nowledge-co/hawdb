# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Numeric owned/retained/Arrow comparison; setup is outside the query timer."""

import argparse
import gc
import json
import pathlib
import resource
import statistics
import sys
import tempfile
import time

import hawdb

QUERY = (
    "MATCH (n:Delivery) WHERE n.score >= $min "
    "RETURN n.score AS score, id(n) AS identity, n.score AS again"
)
PARAMS = {"min": 0}
MASK = (1 << 64) - 1


def fold(checksum, cells):
    for value in cells:
        if type(value) is not int:
            raise TypeError(f"unexpected numeric cell: {type(value)}")
        checksum = ((checksum ^ value) * 1099511628211) & MASK
    return checksum


def consume(db, mode):
    started = time.perf_counter_ns()
    checksum = 14695981039346656037
    count = 0
    boundary = 0
    if mode == "owned":
        before = time.perf_counter_ns()
        result = db.execute(QUERY, PARAMS)
        boundary += time.perf_counter_ns() - before
        for row in result:
            checksum = fold(checksum, (row["score"], row["identity"], row["again"]))
            count += 1
        before = time.perf_counter_ns()
        del result
        boundary += time.perf_counter_ns() - before
    else:
        before = time.perf_counter_ns()
        cursor = db.execute_retained(QUERY, PARAMS)
        if mode == "arrow":
            import pyarrow as pa
            reader = pa.RecordBatchReader.from_stream(cursor)
        boundary += time.perf_counter_ns() - before
        try:
            while True:
                before = time.perf_counter_ns()
                if mode == "retained":
                    batch = cursor.next_batch()
                    if batch is None:
                        boundary += time.perf_counter_ns() - before
                        break
                    columns = [batch.column(i) for i in range(3)]
                    values = [memoryview(column) for column in columns]
                    selection = batch.selection()
                    indices = memoryview(selection)
                else:
                    try:
                        batch = next(reader)
                    except StopIteration:
                        boundary += time.perf_counter_ns() - before
                        break
                    values = [memoryview(batch.column(i).buffers()[1]).cast(fmt)
                              for i, fmt in enumerate(("q", "Q", "q"))]
                    # A standard Arrow batch describes a contiguous selection.
                    offset = batch.column(0).offset
                    indices = range(offset, offset + batch.num_rows)
                boundary += time.perf_counter_ns() - before
                for index in indices:
                    checksum = fold(checksum, (values[0][index], values[1][index], values[2][index]))
                    count += 1
                before = time.perf_counter_ns()
                for view in values:
                    view.release()
                if mode == "retained":
                    indices.release()
                    selection.close()
                    for column in columns:
                        column.close()
                    batch.close()
                    del columns, selection
                del values, batch
                boundary += time.perf_counter_ns() - before
        finally:
            before = time.perf_counter_ns()
            if mode == "arrow":
                reader.close()
            cursor.close()
            boundary += time.perf_counter_ns() - before
    elapsed = time.perf_counter_ns() - started
    snapshot = db.retained_snapshot_copy()
    if snapshot and any(snapshot[key] for key in ("retained_bytes", "view_handles", "buffer_owners")):
        raise RuntimeError(f"retained leak: {snapshot}")
    return {"rows": count, "checksum": f"{checksum:016x}",
            "query_boundary_ns": boundary, "elapsed_ns": elapsed,
            "retained_snapshot": snapshot}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--backend", choices=("memory", "file"), required=True)
    parser.add_argument("--mode", choices=("owned", "retained", "arrow", "all"), required=True)
    parser.add_argument("--samples", type=int, default=5)
    args = parser.parse_args()
    if args.rows <= 0 or args.samples <= 0:
        parser.error("rows and samples must be positive")
    # Import optional consumer code before the measured/warmup iterations.
    if args.mode in ("arrow", "all"):
        import pyarrow
        arrow_version = pyarrow.__version__
    else:
        arrow_version = None
    with tempfile.TemporaryDirectory(prefix="hawdb-retained-bench-") as directory:
        db = hawdb.open() if args.backend == "memory" else hawdb.open(pathlib.Path(directory) / "db")
        try:
            db.execute("CREATE NODE TABLE Delivery")
            db.execute("CREATE PROPERTY ON NODE TABLE Delivery(score) TYPE INT")
            for start in range(0, args.rows, 512):
                db.execute("UNWIND $rows AS row CREATE (:Delivery {score: row.score})",
                           {"rows": [{"score": value} for value in range(start, min(start + 512, args.rows))]})
            reference = consume(db, "owned")
            modes = ("owned", "retained", "arrow") if args.mode == "all" else (args.mode,)
            warmups = {mode: consume(db, mode) for mode in modes}
            samples = []
            for index in range(args.samples):
                # Rotate the paired order to avoid assigning thermal/load drift
                # systematically to the same representation.
                rotation = index % len(modes)
                for mode in modes[rotation:] + modes[:rotation]:
                    gc.collect()
                    sample = consume(db, mode)
                    if (sample["rows"], sample["checksum"]) != (args.rows, reference["checksum"]):
                        raise RuntimeError("result/order checksum mismatch")
                    samples.append({"sample": index, "mode": mode, **sample})
            if any((warmup["rows"], warmup["checksum"]) != (args.rows, reference["checksum"])
                   for warmup in warmups.values()):
                raise RuntimeError("warmup parity mismatch")
            summary = {mode: {"median_query_boundary_ns": statistics.median(s["query_boundary_ns"] for s in samples if s["mode"] == mode),
                              "median_elapsed_ns": statistics.median(s["elapsed_ns"] for s in samples if s["mode"] == mode)} for mode in modes}
            print(json.dumps({"status": "ok", **vars(args), "pyarrow": arrow_version,
                              "reference": reference, "warmups": warmups, "records": samples, "summary": summary,
                              "process_peak_rss_bytes": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)}))
        except Exception as error:
            print(json.dumps({"status": "error", **vars(args), "error": str(error)}))
            raise
        finally:
            db.close()


if __name__ == "__main__":
    main()
