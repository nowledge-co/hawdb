# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Ordinary eager-dict Python baseline; no engine/binding behavior overrides."""

import json
import resource
import sys
import time
import importlib.util
from importlib.machinery import ExtensionFileLoader

# The driver passes the hashed ordinary or instrumented extension explicitly.
# This replacement is local to the benchmark process.
if len(sys.argv) == 3:
    loader = ExtensionFileLoader("hawdb._hawdb", sys.argv[2])
    spec = importlib.util.spec_from_file_location("hawdb._hawdb", sys.argv[2], loader=loader)
    native = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(native)
    sys.modules["hawdb._hawdb"] = native

import hawdb
from checksum import Checksum

from hawdb import _hawdb as native


def native_delta(before, after):
    result = {key: after[key] - before[key] for key in before
              if key not in ("live_requested_bytes", "process_peak_requested_bytes")}
    result["live_requested_bytes_before"] = before["live_requested_bytes"]
    result["live_requested_bytes_after"] = after["live_requested_bytes"]
    result["process_peak_requested_bytes"] = after["process_peak_requested_bytes"]
    result["scope"] = "Rust allocator requests, excluding foreign heap/rounding/non-Rust workspace"
    return result


def run(job):
    db = hawdb.open() if job["backend"] == "memory" else hawdb.open(job["path"])
    phase = "setup"
    try:
        setup = time.perf_counter_ns()
        db.execute("CREATE INDEX ON :Boundary(id)")
        if job["case"] in ("select", "point", "wide"):
            for offset in range(0, len(job["rows"]), 512):
                db.execute(job["insert_bulk"], {"rows": job["rows"][offset : offset + 512]})
        setup_ns = time.perf_counter_ns() - setup
        phase = "warmup"
        if job["case"] in ("select", "point", "wide"):
            calls = len(job["rows"]) if job["case"] == "point" else 1
            for row in job["rows"][:calls]:
                output = db.execute(job["point"], {"id": row["id"]}) if job["case"] == "point" else db.execute(job["scan"])
                # Existing result conversion is eager; merely retaining/dropping
                # it warms the complete same binding/query path.
                del output
        query_ns = consume_ns = rows_seen = 0
        checksum = Checksum(job["columns"])
        phase = "execute"
        profiling = hasattr(native, "_boundary_profile_snapshot")
        if profiling:
            import tracemalloc
            tracemalloc.start()
            native_before = native._boundary_profile_snapshot()
        started = time.perf_counter_ns()
        if job["case"] == "fill":
            for row in job["rows"]:
                called = time.perf_counter_ns()
                db.execute(job["insert_single"], row)
                query_ns += time.perf_counter_ns() - called
        elif job["case"] == "fill_bulk":
            called = time.perf_counter_ns()
            db.execute(job["insert_bulk"], {"rows": job["rows"]})
            query_ns += time.perf_counter_ns() - called
        write_ns = query_ns
        calls = len(job["rows"]) if job["case"] == "point" else 1
        for row in job["rows"][:calls]:
            called = time.perf_counter_ns()
            output = db.execute(job["point"], {"id": row["id"]}) if job["case"] == "point" else db.execute(job["scan"])
            query_ns += time.perf_counter_ns() - called
            if output.columns != job["columns"]:
                raise ValueError("result schema mismatch")
            consumed = time.perf_counter_ns()
            for result in output:
                checksum.row([result[column] for column in job["columns"]])
                rows_seen += 1
            consume_ns += time.perf_counter_ns() - consumed
        elapsed_ns = time.perf_counter_ns() - started
        profile = None
        if profiling:
            native_after = native._boundary_profile_snapshot()
            host_current, host_peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            profile = native_delta(native_before, native_after)
            profile["python_traced_current_bytes"] = host_current
            profile["python_traced_peak_bytes"] = host_peak
        return {
            "status": "ok", "layer": "python", "case": job["case"], "backend": job["backend"],
            "input_rows": len(job["rows"]), "output_rows": rows_seen,
            "values": rows_seen * len(job["columns"]), "checksum": checksum.hex(),
            "setup_ns": setup_ns, "query_boundary_ns": query_ns,
            "write_boundary_ns": write_ns, "read_boundary_ns": query_ns - write_ns,
            "consumer_ns": consume_ns, "elapsed_ns": elapsed_ns,
            "native_profile": profile,
            "durability": "SyncOnEveryWrite", "prefetch": 0,
        }
    except Exception as error:
        return {"status": "error", "layer": "python", "phase": phase,
                "error_type": type(error).__name__, "error": str(error)}
    finally:
        db.close()


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as source:
        result = run(json.load(source))
    result["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == "darwin" else 1024)
    print(json.dumps(result, ensure_ascii=False))
