#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Manually verify and build all three prepared comparison variants."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--target-dir", type=Path, required=True)
    parser.add_argument("--dev-target-dir", type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args()
    scratch = args.scratch.resolve()
    (scratch / "bin").mkdir(exist_ok=False)
    logs = scratch / "validation"
    logs.mkdir(exist_ok=True)
    env = os.environ.copy()
    assert not any(value and (name in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS")
                              or name.startswith("CARGO_PROFILE_")
                              or (name.startswith("CARGO_TARGET_") and name.endswith("RUSTFLAGS")))
                   for name, value in env.items()), "compiler/profile override"
    env["CARGO_BUILD_JOBS"] = str(args.jobs)
    stages = [("test-dev", ["test", "--all-targets", "--all-features"], args.dev_target_dir),
              ("clippy", ["clippy", "--all-targets", "--all-features", "--", "-D", "warnings"], args.dev_target_dir),
              ("test-release", ["test", "--release", "--all-targets", "--all-features"], args.target_dir),
              ("build-release", ["build", "--release"], args.target_dir)]
    receipts = []
    for variant in ("upstream", "word", "neon"):
        for stage, options, target in stages:
            manifest = scratch / variant / "harness/Cargo.toml"
            command = ["cargo", options[0], "--locked", "--manifest-path", str(manifest), *options[1:]]
            env["CARGO_TARGET_DIR"] = str(target.resolve())
            start = time.monotonic()
            print(variant, stage, "starting", flush=True)
            with (logs / f"{variant}-{stage}.log").open("w") as log:
                result = subprocess.run(command, cwd=scratch, env=env, stdout=log, stderr=subprocess.STDOUT)
            receipt = {"variant": variant, "stage": stage, "command": command,
                       "returncode": result.returncode, "seconds": time.monotonic() - start}
            receipts.append(receipt)
            (logs / "build.json").write_text(json.dumps(receipts, indent=2) + "\n")
            print(variant, stage, "exit", result.returncode, "seconds", round(receipt["seconds"], 2), flush=True)
            assert result.returncode == 0, f"See {logs / f'{variant}-{stage}.log'}"
        binary = scratch / "bin" / variant
        shutil.copy2(args.target_dir.resolve() / "release/hawdb-bftree-mvcc-experiment", binary)
        (scratch / "bin" / f"{variant}.sha256").write_text(hashlib.sha256(binary.read_bytes()).hexdigest() + "\n")
        with (logs / f"{variant}-shape-smoke.jsonl").open("w") as log:
            for shape in ("numeric8", "prefix64", "spread64"):
                for layout in ("versioned", "hybrid", "cow"):
                    journal = scratch / f"smoke-{variant}-{shape}-{layout}.journal"
                    result = subprocess.run([str(binary), "bench-shaped", str(journal), layout,
                                             "16", "1", shape], capture_output=True, text=True)
                    log.write(json.dumps({"shape": shape, "layout": layout,
                                          "returncode": result.returncode,
                                          "stdout": result.stdout, "stderr": result.stderr}) + "\n")
                    log.flush()
                    assert result.returncode == 0, result.stderr
        print(variant, "all key-shape/layout smoke checks passed", flush=True)


if __name__ == "__main__":
    main()
