# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Reproducible local Rust/Python/purego boundary matrix; never changes budgets.

Build all native dependencies with the same Bazel optimization mode first:
  bazel build -c opt //:hawdb_bench_host_boundary \
    //bindings/benchmarks:python_boundary //bindings/go/cmd/boundary \
    //bindings/ffi:hawdb_ffi
"""

import argparse
import hashlib
import json
import os
import pathlib
import platform
import re
import statistics
import subprocess
import sys
import time

from checksum import Checksum

ROOT = pathlib.Path(__file__).resolve().parents[2]
CASES = ("select", "point", "fill", "fill_bulk", "wide")
SEED = 0x5EED


def dataset(count, wide):
    state = SEED
    for index in range(count):
        state = (1664525 * state + 1013904223) & 0xFFFFFFFF
        row = {"id": index, "score": (state >> 8) + 0.5,
               "title": "mem-%08x-知识\x00🙂" % index}
        if wide:
            for column in range(3, 20):
                kind = column % 7
                row["c%02d" % column] = (
                    -(state % 100000), (state % 10000) + 0.25,
                    "thread-%d-界" % (index % 37), [index, None, "内容", True],
                    {"id": index, "nested": ["\x00", False, {"标题": "value"}]},
                    index % 2 == 0, None,
                )[kind]
        yield row


def fixture(count, case):
    rows = list(dataset(count, case == "wide"))
    columns = sorted(rows[0])
    properties = ", ".join("%s: row.%s" % (column, column) for column in columns)
    projection = ", ".join("n.%s AS %s" % (column, column) for column in columns)
    checksum = Checksum(columns)
    for row in rows:
        checksum.row([row[column] for column in columns])
    return {
        "case": case, "rows": rows, "columns": columns,
        "insert_single": "CREATE (:Boundary {%s})" % ", ".join("%s: $%s" % (column, column) for column in columns),
        "insert_bulk": "UNWIND $rows AS row CREATE (:Boundary {%s})" % properties,
        "scan": "MATCH (n:Boundary) RETURN %s ORDER BY id" % projection,
        "point": "MATCH (n:Boundary {id: $id}) RETURN %s" % projection,
        "expected_checksum": checksum.hex(), "seed": SEED,
    }


def command_text(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def source_identity():
    # Staging is done by the caller before qualification. Detect modifications
    # rather than silently giving evidence to whatever is compiled later.
    if subprocess.run(["git", "diff", "--quiet"], cwd=ROOT).returncode:
        raise RuntimeError("stage the final source before running the matrix")
    if command_text("git", "ls-files", "--others", "--exclude-standard"):
        raise RuntimeError("stage or remove untracked files before running the matrix")
    return {"head": command_text("git", "rev-parse", "HEAD"),
            "tree": command_text("git", "write-tree")}


def run_child(command, artifact):
    started = time.monotonic()
    timer = ["/usr/bin/time", "-l"] if sys.platform == "darwin" else ["/usr/bin/time", "-v"]
    with artifact.with_suffix(".stdout").open("w") as stdout, artifact.with_suffix(".stderr").open("w") as stderr:
        process = subprocess.Popen(timer + command, cwd=ROOT, stdout=stdout, stderr=stderr)
        return_code = process.wait()
    lines = artifact.with_suffix(".stdout").read_text().splitlines()
    try:
        result = json.loads(lines[-1])
    except (ValueError, IndexError):
        result = {"status": "process_error", "error": "missing result JSON"}
    native = artifact.with_suffix(".stderr").read_text()
    peak = re.search(r"(\d+)\s+maximum resident set size", native) if sys.platform == "darwin" else re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", native)
    result.update({"process_exit": return_code, "process_seconds": time.monotonic() - started,
                   "command": command, "stdout": str(artifact.with_suffix(".stdout")),
                   "stderr": str(artifact.with_suffix(".stderr")),
                   "process_peak_rss_bytes": int(peak[1]) * (1 if sys.platform == "darwin" else 1024) if peak else None})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--sizes", default="1000,100000,1000000")
    parser.add_argument("--cases", default=",".join(CASES))
    parser.add_argument("--backends", default="memory,file")
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--rust", type=pathlib.Path, default=ROOT / "bazel-bin/hawdb_bench_host_boundary")
    parser.add_argument("--python", type=pathlib.Path, default=ROOT / "bazel-bin/bindings/benchmarks/python_boundary")
    parser.add_argument("--go", type=pathlib.Path)
    parser.add_argument("--library", type=pathlib.Path)
    parser.add_argument("--python-extension", type=pathlib.Path)
    parser.add_argument("--cpu-profiles", action="store_true")
    args = parser.parse_args()
    sizes = [int(size) for size in args.sizes.split(",")]
    cases = args.cases.split(",")
    backends = args.backends.split(",")
    if args.samples < 1 or any(size < 1 for size in sizes) or set(cases) - set(CASES) or set(backends) - {"memory", "file"}:
        parser.error("positive sizes/samples and known cases/backends are required")
    args.output.mkdir(parents=True, exist_ok=False)
    identity = source_identity()
    if args.go is None:
        # rules_go applies a pure-Go configuration transition, so its binary
        # does not necessarily live under the root bazel-bin symlink.
        args.go = ROOT / command_text("bazel", "cquery", "-c", "opt", "//bindings/go/cmd/boundary", "--output=files")
    library = args.library or ROOT / "bazel-bin/bindings/ffi" / ("libhawdb_ffi.dylib" if sys.platform == "darwin" else "libhawdb_ffi.so")
    args.rust, args.python, args.go, library = (path.resolve() for path in (args.rust, args.python, args.go, library))
    # Freeze the extension as well: another Bazel configuration can retarget
    # bazel-bin between jobs, but every job must load the hashed artifact.
    if args.python_extension is not None:
        args.python_extension = args.python_extension.resolve()
        if not args.python_extension.is_file():
            parser.error("build the matching Python extension first: " + str(args.python_extension))
    commands = {"rust": [str(args.rust)], "python": [str(args.python)], "go": [str(args.go)]}
    for path in (args.rust, args.python, args.go, library):
        if not path.is_file():
            parser.error("build the matching benchmark first: " + str(path))
    report = {
        "source": identity, "spec": "ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md",
        "seed": SEED, "sizes": sizes, "cases": cases, "backends": backends,
        "samples": args.samples, "discarded_iterations": 1,
        "os": platform.platform(), "machine": platform.machine(),
        "cpu": command_text("sysctl", "-n", "machdep.cpu.brand_string") if sys.platform == "darwin" else platform.processor(),
        "rust": command_text("rustc", "--version"), "go": command_text("go", "version"),
        "build": "Bazel opt, all three engine dependencies compiled together",
        "defaults_changed": False, "strict_zero_copy": False, "terminal": False,
        "instrumented": args.python_extension is not None,
        "binaries": {layer: {"path": command[0], "sha256": hashlib.sha256(pathlib.Path(command[0]).read_bytes()).hexdigest()} for layer, command in commands.items()},
        "library_sha256": hashlib.sha256(library.read_bytes()).hexdigest(),
        "python_extension_path": str(args.python_extension) if args.python_extension else None,
        "python_extension_sha256": hashlib.sha256(args.python_extension.read_bytes()).hexdigest() if args.python_extension else None,
        "records": [], "summaries": [],
        "measurement_limits": [
            "Process peak RSS includes runtime, fixture, setup and query; it is not a query-only allocation ledger.",
            "Allocator observations count Rust requests; Go/Python heaps and non-Rust workspace are separate." if args.python_extension else "Go allocation counts exclude the native engine; run the separately instrumented matrix for native/Python profiles.",
            "Each iteration opens its own store; seeded read runs measure a warm store, not cold artifact reads.",
            "The query timer excludes the identical checksum algorithm; consumer time is reported separately.",
        ],
    }
    destination = args.output / "report.json"
    def save():
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    save()
    parity_failures = 0
    for count in sizes:
        for case in cases:
            data = fixture(count, case)
            for backend in backends:
                for iteration in range(args.samples + 1):
                    # Rotate the layer order deterministically to avoid always
                    # giving one binding the first/cold CPU scheduling position.
                    layers = list(commands)
                    offset = iteration % len(layers)
                    for layer in layers[offset:] + layers[:offset]:
                        if source_identity() != identity:
                            raise RuntimeError("source changed during qualification")
                        stem = "%s-%s-%d-%s-%d" % (layer, case, count, backend, iteration)
                        job_path = args.output / (stem + ".job.json")
                        job = dict(data, backend=backend, path=str(args.output / (stem + ".db")))
                        if args.cpu_profiles and layer == "go":
                            job["cpu_profile_path"] = str(args.output / (stem + ".cpu.pprof"))
                        job_path.write_text(json.dumps(job, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
                        command = commands[layer] + [str(job_path)] + ([str(library)] if layer == "go" else [])
                        if layer == "python" and args.python_extension is not None:
                            command.append(str(args.python_extension.resolve()))
                        result = run_child(command, args.output / stem)
                        result.update({"layer": layer, "case": case, "size": count, "backend": backend, "iteration": iteration,
                                       "discarded": iteration == 0, "fixture_sha256": hashlib.sha256(job_path.read_bytes()).hexdigest(),
                                       "expected_checksum": data["expected_checksum"]})
                        if result["status"] == "ok":
                            result["parity"] = result.get("checksum") == data["expected_checksum"] and result.get("output_rows") == count
                            parity_failures += not result["parity"]
                        else:
                            result["parity"] = None
                        profile = result.get("native_profile")
                        required = ("allocation_calls", "allocated_bytes", "deallocation_calls",
                                    "deallocated_bytes", "reallocation_calls",
                                    "live_requested_bytes_before", "live_requested_bytes_after",
                                    "process_peak_requested_bytes")
                        if layer != "rust":
                            required += ("parameter_conversion_ns", "engine_call_ns", "result_conversion_ns")
                        result["profile_valid"] = (
                            isinstance(profile, dict)
                            and all(type(profile.get(key)) is int and profile[key] >= 0 for key in required)
                        ) if args.python_extension and result["status"] == "ok" else None
                        report["records"].append(result)
                        save()
                        print(stem, result["status"], result["parity"], flush=True)
                        job_path.unlink()  # retain hashes/inputs recipe, not many identical giant fixtures
                        if backend == "file":
                            import shutil
                            shutil.rmtree(job["path"], ignore_errors=False) if pathlib.Path(job["path"]).exists() else None
                for layer in commands:
                    records = [r for r in report["records"] if r["case"] == case and r["size"] == count and r["backend"] == backend and r["layer"] == layer and not r["discarded"]]
                    valid = [r for r in records if r["status"] == "ok" and r["parity"]]
                    report["summaries"].append({"layer": layer, "case": case, "size": count, "backend": backend,
                        "successful_samples": len(valid), "failed_samples": len(records) - len(valid),
                        "median_query_boundary_ns": statistics.median(r["query_boundary_ns"] for r in valid) if valid else None,
                        "median_elapsed_ns": statistics.median(r["elapsed_ns"] for r in valid) if valid else None})
                save()
            del data
    if source_identity() != identity:
        raise RuntimeError("source changed before completion")
    report.update(terminal=True, parity_failures=parity_failures,
                  successful_records=sum(r["status"] == "ok" and r["parity"] for r in report["records"]),
                  refused_or_failed_records=sum(r["status"] != "ok" for r in report["records"]))
    save()
    # A completed experiment may contain useful budget-refusal evidence, but
    # cannot be represented as a successful parity/performance qualification.
    report["profile_failures"] = sum(r["profile_valid"] is False for r in report["records"])
    report["all_paths_succeeded"] = all(
        r["status"] == "ok" and r["parity"] and r["process_exit"] == 0
        and r["profile_valid"] is not False for r in report["records"]
    )
    save()
    return int(not report["all_paths_succeeded"])


if __name__ == "__main__":
    sys.exit(main())
