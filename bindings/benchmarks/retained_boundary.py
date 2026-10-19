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

QUERY = (
    "MATCH (n:Delivery) WHERE n.score >= $min "
    "RETURN n.score AS score, id(n) AS identity, n.score AS again"
)
PARAMS = {"min": 0}
MASK = (1 << 64) - 1
OFFSET = 14695981039346656037


def fold(checksum, cells):
    for value in cells:
        if type(value) is not int:
            raise TypeError(f"unexpected numeric cell: {type(value)}")
        checksum = ((checksum ^ value) * 1099511628211) & MASK
    return checksum


def consume(db, mode):
    started = time.perf_counter_ns()
    checksum = OFFSET
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
        reader = None
        try:
            if mode == "arrow":
                import pyarrow as pa
                reader = pa.RecordBatchReader.from_stream(cursor)
            boundary += time.perf_counter_ns() - before
            while True:
                batch = selection = indices = None
                columns, values = [], []
                before = time.perf_counter_ns()
                try:
                    if mode == "retained":
                        batch = cursor.next_batch()
                        if batch is None:
                            boundary += time.perf_counter_ns() - before
                            break
                        for i in range(3):
                            columns.append(batch.column(i))
                        for column in columns:
                            values.append(memoryview(column))
                        selection = batch.selection()
                        indices = memoryview(selection)
                    else:
                        try:
                            batch = next(reader)
                        except StopIteration:
                            boundary += time.perf_counter_ns() - before
                            break
                        for i, fmt in enumerate(("q", "Q", "q")):
                            values.append(memoryview(batch.column(i).buffers()[1]).cast(fmt))
                        # A standard Arrow batch describes a contiguous selection.
                        offset = batch.column(0).offset
                        indices = range(offset, offset + batch.num_rows)
                    boundary += time.perf_counter_ns() - before
                    for index in indices:
                        checksum = fold(checksum, (values[0][index], values[1][index], values[2][index]))
                        count += 1
                finally:
                    before = time.perf_counter_ns()
                    for view in values:
                        view.release()
                    if mode == "retained":
                        if indices is not None:
                            indices.release()
                        if selection is not None:
                            selection.close()
                        for column in columns:
                            column.close()
                        if batch is not None:
                            batch.close()
                    del values, batch, columns, selection, indices
                    boundary += time.perf_counter_ns() - before
        finally:
            before = time.perf_counter_ns()
            try:
                if reader is not None:
                    reader.close()
            finally:
                cursor.close()
            boundary += time.perf_counter_ns() - before
    elapsed = time.perf_counter_ns() - started
    snapshot = db.retained_snapshot_copy()
    if snapshot and any(snapshot[key] for key in ("retained_bytes", "view_handles", "buffer_owners")):
        raise RuntimeError(f"retained leak: {snapshot}")
    return {"rows": count, "checksum": f"{checksum:016x}",
            "query_boundary_ns": boundary, "elapsed_ns": elapsed,
            "retained_snapshot": snapshot}


def fixture_reference(rows):
    # This fresh database assigns IDs 0..rows-1 to the only inserted label.
    # Keep the oracle independent of every result representation and its caps.
    checksum = OFFSET
    for value in range(rows):
        checksum = fold(checksum, (value, value, value))
    return {"rows": rows, "checksum": f"{checksum:016x}"}


def error_record(error):
    return {"status": "error", "error_type": type(error).__name__, "error": str(error)}


def attempt(db, mode, expected, unavailable):
    if mode in unavailable:
        return {**unavailable[mode], "phase": "prerequisite"}
    try:
        result = consume(db, mode)
        parity = (result["rows"], result["checksum"]) == (expected["rows"], expected["checksum"])
        if not parity:
            return {**result, **error_record(RuntimeError("result/order checksum mismatch")), "parity": False}
        return {**result, "status": "ok", "parity": True}
    except Exception as error:
        result = error_record(error)
        try:
            result["retained_snapshot"] = db.retained_snapshot_copy()
        except Exception as snapshot_error:
            result["snapshot_error"] = error_record(snapshot_error)
        return result


def benchmark(db, rows, modes, sample_count, unavailable):
    expected = fixture_reference(rows)
    reference = attempt(db, "owned", expected, unavailable)
    warmups = {mode: attempt(db, mode, expected, unavailable) for mode in modes}
    samples = []
    for index in range(sample_count):
        # Rotate paired order even after a refusal; never drop requested paths.
        rotation = index % len(modes)
        for mode in modes[rotation:] + modes[:rotation]:
            gc.collect()
            samples.append({"sample": index, "mode": mode,
                            **attempt(db, mode, expected, unavailable)})
    summary = {}
    for mode in modes:
        successful = [s for s in samples if s["mode"] == mode and s["status"] == "ok"]
        qualified = (reference["status"] == warmups[mode]["status"] == "ok"
                     and len(successful) == sample_count)
        summary[mode] = {"qualified": qualified, "successful_samples": len(successful),
                         "expected_samples": sample_count}
        if qualified:
            summary[mode].update(
                median_query_boundary_ns=statistics.median(s["query_boundary_ns"] for s in successful),
                median_elapsed_ns=statistics.median(s["elapsed_ns"] for s in successful),
            )
    succeeded = all(s["qualified"] for s in summary.values())
    return {"status": "ok" if succeeded else "error", "all_paths_succeeded": succeeded,
            "fixture_reference": expected, "reference": reference, "warmups": warmups,
            "records": samples, "summary": summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--backend", choices=("memory", "file"), required=True)
    parser.add_argument("--mode", choices=("owned", "retained", "arrow", "all"), required=True)
    parser.add_argument("--samples", type=int, default=5)
    args = parser.parse_args()
    if args.rows <= 0 or args.samples <= 0:
        parser.error("rows and samples must be positive")
    import hawdb

    modes = ("owned", "retained", "arrow") if args.mode == "all" else (args.mode,)
    unavailable = {}
    arrow_version = None
    # Import optional consumer code before the measured/warmup iterations.
    if "arrow" in modes:
        try:
            import pyarrow
            arrow_version = pyarrow.__version__
        except Exception as error:
            unavailable["arrow"] = error_record(error)
    report = {**vars(args), "pyarrow": arrow_version}
    with tempfile.TemporaryDirectory(prefix="hawdb-retained-bench-") as directory:
        db = None
        try:
            db = hawdb.open() if args.backend == "memory" else hawdb.open(pathlib.Path(directory) / "db")
            db.execute("CREATE NODE TABLE Delivery")
            db.execute("CREATE PROPERTY ON NODE TABLE Delivery(score) TYPE INT")
            for start in range(0, args.rows, 512):
                db.execute("UNWIND $rows AS row CREATE (:Delivery {score: row.score})",
                           {"rows": [{"score": value} for value in range(start, min(start + 512, args.rows))]})
            report.update(benchmark(db, args.rows, modes, args.samples, unavailable))
        except Exception as error:
            report.update(error_record(error), all_paths_succeeded=False)
        finally:
            if db is not None:
                try:
                    db.close()
                except Exception as error:
                    report.update(status="error", all_paths_succeeded=False,
                                  close_error=error_record(error))
    report["process_peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    if report["status"] != "ok":
        for summary in report.get("summary", {}).values():
            summary["qualified"] = False
            summary.pop("median_query_boundary_ns", None)
            summary.pop("median_elapsed_ns", None)
    print(json.dumps(report))
    return 0 if report["status"] == "ok" else 1


if __name__ == "__main__":
    sys.exit(main())
