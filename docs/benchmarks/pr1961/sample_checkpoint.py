# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0
"""Sample only the benchmark child created by this manual diagnostic."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

root = Path(sys.argv[1]).resolve()
assert not root.exists()
root.mkdir()
binary = Path(sys.argv[2]).resolve()
command = [
    str(binary),
    str(root / "fixture"),
    "512",
    "4096",
    "22",
    "carry",
    "step-relationship",
    "cpu-profile",
]
with (root / "worker.stderr").open("w") as error, (root / "sample.stderr").open(
    "w"
) as sample_error, (root / "worker.jsonl").open("w") as output:
    worker = subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=error,
        text=True,
        bufsize=1,
    )
    sampler = None
    try:
        ready = json.loads(worker.stdout.readline())
        assert ready["storage_version"] == "hawdb-storage-v3"
        output.write(json.dumps(ready) + "\n")
        sampler = subprocess.Popen(
            [
                "/usr/bin/sample",
                str(worker.pid),
                "8",
                "1",
                "-file",
                str(root / "candidate-sample.txt"),
            ],
            stdout=sample_error,
            stderr=sample_error,
        )
        for index in range(24):
            worker.stdin.write("step\n")
            worker.stdin.flush()
            row = json.loads(worker.stdout.readline())
            assert row["round"] == index
            output.write(json.dumps(row) + "\n")
            output.flush()
        worker.stdin.write("finish\n")
        worker.stdin.flush()
        worker.stdin.close()
        assert worker.wait(timeout=180) == 0
        sample_status = sampler.wait(timeout=30)
        receipt = dict(
            command=command,
            worker_pid=worker.pid,
            worker_exit=worker.returncode,
            sampler_exit=sample_status,
            binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
            purpose="under-load stack attribution only; not comparative acceptance",
        )
        (root / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt), flush=True)
    finally:
        for process in [worker, sampler]:
            if process is not None and process.poll() is None:
                process.kill()
                process.wait()
