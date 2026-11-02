#!/usr/bin/env python3
# Copyright 2026 Nowledge
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run sequential same-binary Consumer controls and retain adverse samples."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build_evidence(artifacts, binary):
    matches = []
    for line in artifacts.read_text().splitlines():
        item = json.loads(line)
        if (item.get("reason") == "compiler-artifact"
                and item.get("executable")
                and Path(item["executable"]).resolve() == binary
                and item["target"]["name"] == "consumer_publication"):
            matches.append(item)
    if len(matches) != 1:
        raise ValueError("expected exactly one Cargo artifact for the selected Consumer executable")
    artifact = matches[0]
    profile = artifact["profile"]
    if profile["opt_level"] != "3" or profile["debug_assertions"]:
        raise ValueError("Consumer timing requires the repository's optimized bench profile")
    return {"build_artifacts_sha256": digest(artifacts),
            "features": sorted(set(artifact["features"]) - {"default"}),
            "default_features": "default" in artifact["features"],
            "cargo_profile": profile}


def validate_reads(result, readers, requests):
    assert result["readers"] == readers and result["reads_per_reader"] == requests
    reads = result["foreground_read"]
    samples = reads["samples"]
    assert reads["requests"] == reads["latency"]["count"] == len(samples) == readers * requests
    for worker in range(readers):
        assert sorted(sample["sequence"] for sample in samples if sample["worker"] == worker) == list(range(requests))
    for sample in samples:
        assert sample["started_ns"] <= sample["query_started_ns"] <= sample["finished_ns"]
        assert sample["elapsed_ns"] > 0
        assert 0 <= sample["committed_writes_before"] <= sample["committed_writes_after"] <= result["writes"]
    observation = result["concurrent_phase_observation"]
    assert observation["phase"] == "concurrent_phase"
    for boundary in ["resources_before", "resources_after"]:
        resource = observation[boundary]
        assert resource["status_available"] and 0 < resource["VmRSS"] <= resource["VmHWM"]
        assert resource["cpu_user_ticks"] >= 0 and resource["cpu_system_ticks"] >= 0
    phases = [phase for attempt in result["publication_attempts"]
              for phase in attempt["phase_observations"]
              if phase["phase"] in {"coverage_preparation", "native_construction"}]
    def during_preparation(sample):
        return any(phase["started_ns"] <= sample["query_started_ns"]
                   and sample["finished_ns"] <= phase["finished_ns"] for phase in phases)
    during = sum(during_preparation(sample) for sample in samples)
    simultaneous = sum(during_preparation(sample) and sample["active_writers_after"] > 0
                       and sample["committed_writes_after"] < result["writes"] for sample in samples)
    active = sum(sample["active_writers_after"] > 0 for sample in samples)
    assert during == reads["completed_during_preparation"]
    assert active == reads["completed_with_active_writers"]
    reads["completed_during_preparation_before_writer_drain"] = simultaneous
    before_drain = sum(sample["active_writers_after"] > 0
                       and sample["committed_writes_after"] < result["writes"] for sample in samples)
    return before_drain > 0 and (result["mode"] == "disabled" or simultaneous > 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--build-artifacts", type=Path, required=True,
                        help="Cargo --message-format=json output for this executable")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fixture-parent", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--writes", type=int, default=4096)
    parser.add_argument("--quiet-samples", type=int, default=32)
    parser.add_argument("--sustained", action="store_true",
                        help="Run the separate detached 100,000-write checkpointed progress cohort")
    parser.add_argument("--with-readers", action="store_true",
                        help="Separate pinned-read/resource cohort; one reader per writer")
    parser.add_argument("--reads-per-reader", type=int, default=512)
    args = parser.parse_args()
    if args.rounds < 1 or args.writes < 8 or args.quiet_samples < 1:
        parser.error("positive rounds/quiet samples and at least eight writes are required")
    if args.sustained and args.writes != 100_000:
        parser.error("the sustained cohort requires --writes 100000")
    if args.with_readers and (args.sustained or not 1 <= args.reads_per_reader <= 4096):
        parser.error("read/resource qualification is separate from sustained writes and requires 1..4096 reads per reader")
    binary = args.binary.resolve(strict=True)
    build = build_evidence(args.build_artifacts.resolve(strict=True), binary)
    source = Path(__file__).resolve().parents[1]
    args.output.mkdir(parents=True, exist_ok=False)
    args.fixture_parent.mkdir(parents=True, exist_ok=True)
    filesystem = subprocess.check_output(["df", "-T", str(args.fixture_parent)], text=True)
    if "tmpfs" in filesystem:
        parser.error("durable qualification requires a disk-backed fixture parent")
    source_files = sorted(set(
        name for name in subprocess.check_output(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
            cwd=source, text=True).split("\0")
        if name and (Path(name).suffix in {".rs", ".toml", ".lock", ".bzl", ".bazel"}
                     or name == "scripts/measure-consumer-publication.py")
    ))
    manifest = {
        "schema": "consumer-publication-measurement-v3",
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "source_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip(),
        "source_status": subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True),
        "tracked_diff_sha256": hashlib.sha256(subprocess.check_output(["git", "diff", "HEAD"], cwd=source)).hexdigest(),
        "source_sha256": {name: digest(source / name) for name in source_files},
        "binary": str(binary), "binary_sha256": digest(binary),
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "cpuinfo": Path("/proc/cpuinfo").read_text() if Path("/proc/cpuinfo").exists() else None,
        "filesystem": filesystem,
        "profile": "repository Cargo bench profile; no optimization overrides",
        **build,
        "rounds": args.rounds, "writes_per_case": args.writes,
        "quiescent_publications_per_enabled_case": args.quiet_samples,
        "sustained": args.sustained,
        "with_readers": args.with_readers,
        "reads_per_reader": args.reads_per_reader if args.with_readers else 0,
        "reader_policy": "one per writer; fresh pinned Memory query per request; observed overlap required",
        "staging_advance_call_limit": 200001,
        "publication_attempt_ceiling": 4096 if args.sustained else 16,
        "checkpoint_every_foreground_writes": 16384 if args.sustained else None,
        "concurrent_phase_publications_per_enabled_case": None if args.sustained else 16,
        "shared_maintenance_trigger_commits_per_case": None if args.sustained else 16,
        "changefeed_retention_entries": args.writes + args.quiet_samples + 64 + (4096 if args.sustained else 0),
        "changefeed_retention_bytes": 64 << 20,
        "shapes": [[32, 128], [512, 1024]], "writers": [1, 4, 8],
        "modes": ["detached"] if args.sustained else ["disabled", "scoped-blocking", "detached"],
        "isolation": "sequential cases; shared host, no exclusive machine or cold-device claim",
    }
    (args.output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    results = []
    with (args.output / "results.jsonl").open("x") as raw:
        for round_index in range(args.rounds):
            cases = [(docs, payload, writers, mode)
                     for docs, payload in manifest["shapes"]
                     for writers in manifest["writers"] for mode in manifest["modes"]]
            if round_index % 2:
                cases.reverse()
            for index, (docs, payload, writers, mode) in enumerate(cases):
                fixture = args.fixture_parent / f"consumer-publication-{os.getpid()}-{round_index}-{index}"
                command = [str(binary), "--mode", mode, "--writers", str(writers),
                           "--documents", str(docs), "--payload", str(payload),
                           "--writes", str(args.writes), "--quiet-samples", str(args.quiet_samples),
                           "--root", str(fixture)]
                if args.sustained:
                    command.extend(["--sustained", "true"])
                if args.with_readers:
                    command.extend(["--readers", str(writers), "--reads-per-reader", str(args.reads_per_reader)])
                started = time.monotonic()
                load_before = os.getloadavg()
                completed = subprocess.run(command, text=True, capture_output=True, check=False)
                try:
                    if completed.returncode:
                        raise ValueError(f"process exited {completed.returncode}")
                    result = json.loads(completed.stdout)
                    assert (result["documents"], result["payload_bytes_per_document"],
                            result["writers"], result["writes"], result["mode"]) == (
                                docs, payload, writers, args.writes, mode)
                    if args.with_readers:
                        result["read_resource_qualified"] = validate_reads(result, writers, args.reads_per_reader)
                    attempts = result["publication_attempts"] + result["quiescent_publication_samples"]
                    for attempt in attempts:
                        assert attempt["staging_advance_call_limit"] == manifest["staging_advance_call_limit"]
                        assert 0 < attempt["staging_advance_calls"] <= manifest["staging_advance_call_limit"]
                except (ValueError, KeyError, TypeError, AssertionError) as error:
                    (args.output / "failure.json").write_text(json.dumps({
                        "command": command, "returncode": completed.returncode,
                        "stdout": completed.stdout, "stderr": completed.stderr,
                        "validation_error": str(error),
                    }, indent=2) + "\n")
                    raise SystemExit(f"case failed; evidence retained in {args.output / 'failure.json'}")
                result["run"] = {"round": round_index, "command": command,
                                 "wall_seconds_including_setup": time.monotonic() - started,
                                 "load_before": load_before, "load_after": os.getloadavg()}
                results.append(result)
                raw.write(json.dumps(result, separators=(",", ":")) + "\n")
                raw.flush()
                print(f"round={round_index} docs={docs} writers={writers} mode={mode} "
                      f"p95_ms={result['foreground_write']['p95_ns'] / 1e6:.3f} "
                      f"p99_ms={result['foreground_write']['p99_ns'] / 1e6:.3f} "
                      f"active_advances={result['watermark_advances_with_active_writers']}", flush=True)
    lines = [
        "# Consumer publication measurements", "",
        "Median of per-run percentiles across repetitions; all individual samples are retained in results.jsonl.",
        "Publication callback time is a lower bound on the outer owner critical section and excludes lock-entry/drop overhead.",
        "Quiescent publication samples cannot establish simultaneous source-write/Consumer-watermark progress.", "",
        "| Documents / payload | Writers | Mode | Write p50 ms | Write p95 ms | Write p99 ms | Wait p95 ms | Quiet publish p50 ms | Writes/s | Active advances |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for docs, payload in manifest["shapes"]:
        for writers in manifest["writers"]:
            for mode in manifest["modes"]:
                group = [row for row in results if row["documents"] == docs and row["writers"] == writers and row["mode"] == mode]
                def median(metric, field):
                    return statistics.median(row[metric][field] for row in group) / 1e6
                publish = "n/a" if mode == "disabled" else f"{median('quiescent_publication_exclusive_callback', 'p50_ns'):.3f}"
                advances = ",".join(str(row["watermark_advances_with_active_writers"]) for row in group)
                throughput = statistics.median(row["writes_per_second"] for row in group)
                lines.append(f"| {docs} / {payload} B | {writers} | {mode} | "
                             f"{median('foreground_write', 'p50_ns'):.3f} | {median('foreground_write', 'p95_ns'):.3f} | "
                             f"{median('foreground_write', 'p99_ns'):.3f} | {median('writer_wait_plus_entry', 'p95_ns'):.3f} | "
                             f"{publish} | {throughput:.1f} | {advances} |")
    if args.with_readers:
        lines.extend(["", "## Pinned foreground reads and process observations", "",
                      "Process RSS is observed at phase boundaries; VmHWM is the process lifetime high-water mark, not a phase heap peak.",
                      "| Documents | Readers | Mode | Read p50 ms | Read p95 ms | Read p99 ms | Reads completed during preparation |",
                      "| --- | --- | --- | --- | --- | --- | --- |"])
        for row in results:
            reads = row["foreground_read"]
            p = reads["latency"]
            lines.append(f"| {row['documents']} | {row['readers']} | {row['mode']} | "
                         f"{p['p50_ns']/1e6:.3f} | {p['p95_ns']/1e6:.3f} | {p['p99_ns']/1e6:.3f} | "
                         f"{reads['completed_during_preparation']} |")
    (args.output / "summary.md").write_text("\n".join(lines) + "\n")
    receipt = {
        "source": manifest["source_head"], "cases_complete": len(results),
        "expected_cases": args.rounds * len(manifest["shapes"]) * len(manifest["writers"]) * len(manifest["modes"]),
        "process_and_exact_checks_passed": True,
        "read_resource_qualified": args.with_readers and all(row["read_resource_qualified"] for row in results),
        "sustained": args.sustained,
        "sustained_progress_qualified": args.sustained and all(
            row.get("sustained_workload", {}).get("qualified", False) for row in results),
        "limits": "Finite fixed work, no steady-state, starvation-freedom or latency-improvement claim. Preserve separate strict writer gates and historical adverse datasets.",
    }
    assert receipt["cases_complete"] == receipt["expected_cases"]
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    if args.with_readers and not receipt["read_resource_qualified"]:
        raise SystemExit("read/preparation overlap criterion not met in every case; complete evidence retained")
    if args.sustained and not receipt["sustained_progress_qualified"]:
        raise SystemExit("sustained progress criterion not met in every declared case; complete evidence retained")


if __name__ == "__main__":
    main()
