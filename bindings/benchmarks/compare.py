# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Build and compare two immutable ordinary-boundary producers on one host."""

import argparse
import hashlib
import json
import pathlib
import platform
import re
import shutil
import statistics
import subprocess
import sys

import run as matrix

ROOT = pathlib.Path(__file__).resolve().parents[2]
BASELINE = "69d526a2d2d4b75741a731d0203949ffc1d123fb"
TARGETS = (
    "//:hawdb_bench_host_boundary", "//bindings/benchmarks:python_boundary",
    "//bindings/go/cmd/boundary", "//bindings/ffi:hawdb_ffi",
)
HARNESS = (
    "benches/host_boundary.rs", "benches/host_boundary/workload.rs",
    "benches/host_boundary/checksum.rs", "bindings/benchmarks/run.py",
    "bindings/benchmarks/checksum.py", "bindings/benchmarks/python_boundary.py",
    "bindings/go/cmd/boundary/BUILD.bazel", "bindings/go/cmd/boundary/main.go",
    "bindings/go/cmd/boundary/profile_unix.go", "bindings/go/cmd/boundary/profile_windows.go",
)
BASELINE_BUILD = '''load("@rules_python//python:defs.bzl", "py_binary", "py_library")
package(default_visibility = ["//visibility:public"])
py_library(name = "checksum", srcs = ["checksum.py"], imports = ["."])
py_binary(name = "python_boundary", srcs = ["python_boundary.py"],
          deps = [":checksum", "//bindings/python:hawdb"], tags = ["manual"])
'''


def command(arguments, cwd):
    return subprocess.check_output(arguments, cwd=cwd, text=True).strip()


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_revision(revision):
    if not re.fullmatch(r"[0-9a-f]{40}", revision):
        raise ValueError("baseline must be a full immutable Git SHA")
    if revision != BASELINE:
        raise ValueError("baseline adapters are qualified only for " + BASELINE)


def require_source(repository, expected):
    subprocess.run(["git", "diff", "--quiet"], cwd=repository, check=True)
    if command(["git", "ls-files", "--others", "--exclude-standard"], repository):
        raise RuntimeError("source contains untracked files")
    actual = {"head": command(["git", "rev-parse", "HEAD"], repository),
              "tree": command(["git", "write-tree"], repository)}
    if any(actual[key] != expected[key] for key in actual):
        raise RuntimeError("source changed during comparison")


def prepare_baseline(destination, revision):
    validate_revision(revision)
    subprocess.run(["git", "merge-base", "--is-ancestor", revision, "HEAD"], cwd=ROOT, check=True)
    subprocess.run(["git", "clone", "--shared", "--no-checkout", str(ROOT), str(destination)], check=True)
    subprocess.run(["git", "checkout", "--detach", revision], cwd=destination, check=True)
    adapter = ROOT / "bindings/benchmarks/main-baseline-adapters.json"
    specification = json.loads(adapter.read_text())
    if specification["baseline"] != revision:
        raise ValueError("adapter specification does not match the baseline")
    patch = destination.parent / "baseline-adapters.patch"
    patch.write_text("".join(specification["patch_lines"]))
    subprocess.run(["git", "apply", "--check", str(patch)], cwd=destination, check=True)
    subprocess.run(["git", "apply", str(patch)], cwd=destination, check=True)
    for relative in HARNESS:
        target = destination / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / relative, target)
    (destination / "bindings/benchmarks/BUILD.bazel").write_text(BASELINE_BUILD)
    subprocess.run(["git", "add", "--all"], cwd=destination, check=True)
    return {"head": command(["git", "rev-parse", "HEAD"], destination),
            "tree": command(["git", "write-tree"], destination),
            "adapter_patch_sha256": sha256(patch), "adapter_spec_sha256": sha256(adapter),
            "adaptations": ["Identical ordinary harness and manual targets.",
                            "Identical untimed in-memory opening adapter.",
                            "Exhaustive Python graph-expansion error mapping; success path unchanged."]}


def build_producer(repository, directory):
    directory.mkdir()
    with (directory / "build.log").open("w") as log:
        subprocess.run(["bazel", "build", "-c", "opt", *TARGETS], cwd=repository,
                       stdout=log, stderr=subprocess.STDOUT, check=True)
    suffix = ".dylib" if sys.platform == "darwin" else ".so"
    paths = {"rust": repository / "bazel-bin/hawdb_bench_host_boundary",
             "python": repository / "bazel-bin/bindings/benchmarks/python_boundary",
             "library": repository / ("bazel-bin/bindings/ffi/libhawdb_ffi" + suffix),
             "python_extension": repository / "bazel-bin/bindings/python/python/hawdb/_hawdb.so"}
    paths["go"] = repository / command(
        ["bazel", "cquery", "-c", "opt", "//bindings/go/cmd/boundary", "--output=files"], repository)
    frozen = {}
    for name, original in paths.items():
        original = original.resolve(strict=True)
        before = sha256(original)
        # A Bazel Python launcher needs its original runfiles. Its native
        # extension is frozen separately and passed explicitly to the consumer.
        target = original if name == "python" else directory / original.name
        if target != original:
            shutil.copy2(original, target)
        if sha256(original) != before or sha256(target) != before:
            raise RuntimeError("producer changed while freezing " + name)
        frozen[name] = {"path": str(target), "source": str(original), "sha256": before}
    (directory / "manifest.json").write_text(json.dumps(frozen, indent=2) + "\n")
    return frozen


def summarize(before, after):
    summaries = []
    keys = sorted({(r["layer"], r["case"], r["size"], r["backend"])
                   for report in (before, after) for r in report["records"]})
    for key in keys:
        groups = [[r for r in report["records"] if not r["discarded"]
                   and (r["layer"], r["case"], r["size"], r["backend"]) == key]
                  for report in (before, after)]
        expected = before["samples"]
        complete = (before["terminal"] and after["terminal"]
                    and expected > 0 and expected == after["samples"]
                    and all(len(group) == expected and all(matrix.qualified_record(r) for r in group)
                            for group in groups))
        item = dict(zip(("layer", "case", "size", "backend"), key))
        item.update(qualified=complete, before_samples=len(groups[0]), after_samples=len(groups[1]))
        if complete:
            for metric in ("elapsed_ns", "query_boundary_ns", "write_boundary_ns", "read_boundary_ns"):
                medians = [statistics.median(r[metric] for r in group) for group in groups]
                item[metric + "_median_speedup"] = medians[0] / medians[1] if medians[1] else None
        summaries.append(item)
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--baseline", default=BASELINE)
    parser.add_argument("--sizes", default="1000,100000,1000000")
    parser.add_argument("--cases", default=",".join(matrix.CASES))
    parser.add_argument("--backends", default="memory,file")
    parser.add_argument("--samples", type=int, default=3)
    args = parser.parse_args()
    validate_revision(args.baseline)
    if (args.samples < 1 or any(int(size) < 1 for size in args.sizes.split(","))
            or set(args.cases.split(",")) - set(matrix.CASES)
            or set(args.backends.split(",")) - {"memory", "file"}):
        parser.error("positive sizes/samples and known cases/backends are required")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    evidence = output / "evidence"
    evidence.mkdir()
    source = matrix.source_identity()
    baseline_repository = output / "baseline"
    baseline_source = prepare_baseline(baseline_repository, args.baseline)
    sources = {"baseline": baseline_source, "candidate": source}
    producers = {}
    # Finish all compilation before measuring either version. Never compare
    # ordinary latency against an allocation-instrumented artifact.
    for name, repository in (("baseline", baseline_repository), ("candidate", ROOT)):
        require_source(repository, sources[name])
        print(name, "building optimized producer", flush=True)
        producers[name] = build_producer(repository, evidence / (name + "-producer"))
        require_source(repository, sources[name])
    identity = {"sources": sources, "producers": producers, "platform": platform.platform(),
                "build": "default Bazel configuration, -c opt", "defaults_changed": False,
                "ordering": "baseline then candidate; ratios of medians, not alternated pairs",
                "measurement_limits": ["Process RSS includes fixture/setup/host runtime.",
                                       "Ordinary APIs do not use Arrow or retained cursors.",
                                       "Any refusal/failure prevents a group speedup claim."]}
    (evidence / "manifest.json").write_text(json.dumps(identity, indent=2) + "\n")
    shutil.copyfile(ROOT / "bindings/benchmarks/main-baseline-adapters.json", evidence / "baseline-adapters.json")
    statuses = {}
    reports = {}
    for name, repository in (("baseline", baseline_repository), ("candidate", ROOT)):
        require_source(repository, sources[name])
        producer = producers[name]
        arguments = [sys.executable, "-B", str(repository / "bindings/benchmarks/run.py"),
                     "--output", str(evidence / name), "--sizes", args.sizes, "--cases", args.cases,
                     "--backends", args.backends, "--samples", str(args.samples)]
        for flag in ("rust", "python", "go", "library", "python_extension"):
            arguments += ["--" + flag.replace("_", "-"), producer[flag]["path"]]
        with (evidence / (name + "-driver.log")).open("w") as log:
            statuses[name] = subprocess.run(arguments, cwd=repository, stdout=log, stderr=subprocess.STDOUT).returncode
        report_path = evidence / name / "report.json"
        if report_path.exists():
            reports[name] = json.loads(report_path.read_text())
            if reports[name]["source"] != {key: sources[name][key] for key in ("head", "tree")}:
                raise RuntimeError("matrix source does not match the frozen producer")
        require_source(repository, sources[name])
        print(name, "driver exit", statuses[name], flush=True)
    identity["driver_exits"] = statuses
    if len(reports) == 2:
        identity["groups"] = summarize(reports["baseline"], reports["candidate"])
    identity["all_paths_succeeded"] = (
        len(reports) == 2 and all(status == 0 for status in statuses.values())
        and all(report.get("terminal") and report.get("all_paths_succeeded") for report in reports.values())
        and bool(identity.get("groups")) and all(group["qualified"] for group in identity["groups"])
    )
    (evidence / "comparison.json").write_text(json.dumps(identity, indent=2) + "\n")
    raise SystemExit(0 if identity["all_paths_succeeded"] else 1)


if __name__ == "__main__":
    main()
