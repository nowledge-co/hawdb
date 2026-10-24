#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Manual three-way comparison of the real MVCC adapter with patched Bf-Tree."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compiler_count():
    commands = subprocess.check_output(["ps", "-axo", "comm="], text=True)
    return sum(Path(command.strip()).name in ("rustc", "clippy-driver")
               for command in commands.splitlines())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True, help="new directory")
    parser.add_argument("--journals", type=Path, required=True, help="new directory")
    parser.add_argument("--repetitions", type=int, default=5)
    args = parser.parse_args()
    assert args.repetitions >= 5
    cfg = subprocess.check_output(["rustc", "--print", "cfg"], text=True)
    assert 'target_arch="aarch64"' in cfg and 'target_feature="neon"' in cfg
    assert 'target_endian="little"' in cfg, "this run must execute actual NEON"
    assert not any(value and (name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS")
                              or name.startswith("CARGO_PROFILE_")
                              or (name.startswith("CARGO_TARGET_") and name.endswith("RUSTFLAGS")))
                   for name, value in os.environ.items()), "compiler/profile override"
    scratch = args.scratch.resolve()
    sources = json.loads((scratch / "source.json").read_text())
    variants = ["upstream", "word", "neon"]
    harness_sources = []
    for variant in variants:
        files = sources["variants"][variant]["source_sha256"]
        for relative, expected in files.items():
            assert sha(scratch / variant / relative) == expected, relative
        harness_sources.append({name: digest for name, digest in files.items()
                                if name.startswith("harness/") and name.endswith(".rs")})
    assert harness_sources[0] == harness_sources[1] == harness_sources[2]
    binaries = {v: scratch / "bin" / v for v in variants}
    binary_hashes = {v: sha(p) for v, p in binaries.items()}
    assert len(set(binary_hashes.values())) == 3, "variants must produce distinct code"
    args.output.mkdir(parents=True, exist_ok=False)
    args.journals.mkdir(parents=True, exist_ok=False)
    cases = [(16, 2, "numeric8"), (512, 24, "numeric8"), (2048, 48, "numeric8"),
             (512, 24, "prefix64"), (512, 24, "spread64")]
    layouts = ["versioned", "hybrid", "cow"]
    protocol = {
        "repetitions": args.repetitions, "cases": cases, "layouts": layouts,
        "binaries": {v: {"path": str(p), "sha256": binary_hashes[v]} for v, p in binaries.items()},
        "toolchain": subprocess.check_output(["rustc", "-Vv"], text=True),
        "platform": platform.platform(), "profile": "repository release, no overrides",
        "order": "rotate variants/layouts; reverse variants and cases on odd repetitions",
        "isolation": "fresh process and synced journal per variant/case/layout/repetition",
        "durability": "unchanged sync_all before index installation and publication",
        "samples_per_process": {"current_get": 4096, "old_get": 4096,
                                "current_full_scan": 32, "old_full_scan": 32},
        "limits": "Shared host. Scan p99 is a maximum. No production or power-loss claim.",
        "compiler_observation": "Counts at child boundaries only; no continuous quiescence claim",
        "drivers": {p.name: sha(p) for p in Path(__file__).resolve().parent.glob("*.py")},
    }
    if platform.system() == "Darwin":
        protocol["cpu"] = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
        protocol["memory_bytes"] = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True))
    protocol["journal_filesystem"] = subprocess.check_output(["df", "-h", str(args.journals)], text=True)
    (args.output / "protocol.json").write_text(json.dumps(protocol, indent=2) + "\n")
    (args.output / "source.json").write_text(json.dumps(sources, indent=2) + "\n")
    samples = {}
    with gzip.open(args.output / "results.jsonl.gz", "wt") as raw:
        for repetition in range(args.repetitions):
            ordered_cases = cases[::-1] if repetition % 2 else cases
            ordered_layouts = layouts[repetition % 3:] + layouts[:repetition % 3]
            for case_index, (keys, rounds, shape) in enumerate(ordered_cases):
                rotation = (repetition + case_index) % 3
                order = variants[rotation:] + variants[:rotation]
                if repetition % 2:
                    order.reverse()
                for layout in ordered_layouts:
                    journal_hashes = []
                    for variant in order:
                        path = args.journals / f"{repetition}-{keys}-{rounds}-{shape}-{layout}-{variant}.journal"
                        command = [str(binaries[variant]), "bench-shaped", str(path), layout,
                                   str(keys), str(rounds), shape]
                        compilers_before = compiler_count()
                        load = os.getloadavg()
                        started = time.monotonic_ns()
                        try:
                            result = subprocess.run(command, capture_output=True, text=True, timeout=240)
                        except subprocess.TimeoutExpired as error:
                            # Preserve the failed child as an observation; no
                            # timeout retry or incomplete summary is a pass.
                            raw.write(json.dumps({"repetition": repetition, "variant": variant,
                                                  "keys": keys, "rounds": rounds, "key_shape": shape,
                                                  "layout": layout, "command": command,
                                                  "error": "child timeout after 240 seconds"}) + "\n")
                            raw.flush()
                            raise RuntimeError("Incomplete comparison: see raw observations") from error
                        receipt = {"repetition": repetition, "variant": variant, "keys": keys,
                                   "rounds": rounds, "key_shape": shape, "layout": layout,
                                   "command": command, "load_before": load,
                                   "elapsed_ns": time.monotonic_ns() - started,
                                   "returncode": result.returncode,
                                   "stdout": result.stdout, "stderr": result.stderr,
                                   "rust_compilers_before": compilers_before,
                                   "rust_compilers_after": compiler_count()}
                        raw.write(json.dumps(receipt) + "\n")
                        raw.flush()
                        assert result.returncode == 0, receipt
                        report = json.loads(result.stdout)
                        assert report["profile"] == "release"
                        assert report["keys"] == keys and report["update_rounds"] == rounds
                        assert report["layout"].lower() == layout and report["key_shape"] == shape
                        for phase, count in protocol["samples_per_process"].items():
                            assert report[phase]["operations"] == count
                        digest = sha(path)
                        journal_hashes.append(digest)
                        samples.setdefault((keys, rounds, shape, layout), {}).setdefault(repetition, {})[variant] = {
                            "report": report, "journal_sha256": digest}
                    assert len(set(journal_hashes)) == 1, (repetition, keys, rounds, shape, layout)
            print(f"Completed repetition {repetition + 1}/{args.repetitions}", flush=True)
    summary = []
    phases = ["commit_batches", "prepare_and_sync", "index_install", "current_get", "old_get",
              "current_full_scan", "old_full_scan"]
    fields = [(phase, q) for phase in phases for q in ["p50_ns", "p95_ns", "p99_ns", "total_ns"]]
    fields += [(n,) for n in ["reopen_ns", "resident_bytes", "process_peak_bytes", "journal_bytes"]]
    for (keys, rounds, shape, layout), repetitions in samples.items():
        row = {"keys": keys, "rounds": rounds, "key_shape": shape, "layout": layout,
               "repetitions": len(repetitions)}
        for path in fields:
            values = {v: [] for v in variants}
            for repetition in repetitions.values():
                for variant in variants:
                    value = repetition[variant]["report"]
                    for component in path:
                        value = value[component]
                    values[variant].append(value)
            ratios = {}
            for numerator, denominator in [("word", "upstream"), ("neon", "upstream"), ("neon", "word")]:
                paired = [a / b for a, b in zip(values[numerator], values[denominator], strict=True)
                          if a is not None and b is not None and b > 0]
                ratios[f"{numerator}/{denominator}"] = {
                    "median": statistics.median(paired) if paired else None, "paired": paired}
            row["/".join(path)] = {
                "medians": {v: statistics.median([n for n in ns if n is not None])
                            if any(n is not None for n in ns) else None for v, ns in values.items()},
                "ratios": ratios}
        summary.append(row)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Saved {len(summary)} case summaries and all raw observations", flush=True)


if __name__ == "__main__":
    main()
