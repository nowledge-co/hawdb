#!/usr/bin/env python3
# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Capture bounded direct-process evidence after an incomplete Bazel FFI run."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def diagnose(binary, output, run=subprocess.run):
    output.mkdir(parents=True, exist_ok=True)
    for mode, extra in [("parallel", []), ("serial", ["--test-threads=1"])]:
        command = [str(binary), "retained::", "--nocapture", *extra]
        record = {"command": command, "timeout_seconds": 60}
        with (output / f"{mode}.stdout.txt").open("wb") as stdout:
            with (output / f"{mode}.stderr.txt").open("wb") as stderr:
                try:
                    result = run(
                        command, stdout=stdout, stderr=stderr, timeout=60,
                        check=False, env={**os.environ, "RUST_BACKTRACE": "1"},
                    )
                    record["returncode"] = result.returncode
                    record["returncode_hex"] = f"0x{result.returncode & 0xFFFFFFFF:08x}"
                except subprocess.TimeoutExpired:
                    record["timed_out"] = True
                except OSError as error:
                    record["launch_error"] = str(error)
        record["positive_terminal_summary"] = bool(re.search(
            rb"^test result: ok\. [1-9][0-9]* passed;",
            (output / f"{mode}.stdout.txt").read_bytes(), re.MULTILINE,
        ))
        (output / f"{mode}.json").write_text(
            json.dumps(record, indent=2) + "\n", encoding="utf-8",
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    diagnose(args.binary, args.output)
