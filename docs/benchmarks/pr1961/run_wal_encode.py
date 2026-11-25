# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0
import datetime
import hashlib
import json
import select
import subprocess
import sys
from pathlib import Path

binaries, root = map(Path, sys.argv[1:3])
binaries, root = binaries.resolve(), root.resolve()
rounds = int(sys.argv[3])
assert rounds > 0 and not root.exists()
root.mkdir()
receipts = []


def read(worker):
    assert select.select([worker.stdout], [], [], 180)[0]
    line = worker.stdout.readline()
    assert line, worker.poll()
    return json.loads(line)


for workload, count in [
    ("node", 1),
    ("node", 128),
    ("node", 512),
    ("relationship", 4096),
    ("large-small", 128),
    ("list", 4096),
    ("map", 4096),
    ("temporal", 1),
    ("temporal", 4096),
    ("temporal-list", 4096),
    ("temporal-map", 4096),
    ("temporal-properties", 4096),
]:
    workers = {}
    ready = {}
    errors = []
    stem = f"{workload}-{count}"
    started = datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        for variant in ["baseline", "candidate"]:
            binary = binaries / ("wal-encode-" + variant)
            error = (root / (stem + "-" + variant + ".stderr")).open("w")
            errors.append(error)
            worker = subprocess.Popen(
                [str(binary), workload, str(count), str(rounds)],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=error,
                text=True,
                bufsize=1,
            )
            workers[variant] = worker
            ready[variant] = read(worker)
        assert ready["baseline"] == ready["candidate"]
        with (root / (stem + ".jsonl")).open("w") as output:
            for index in range(rounds + 2):
                order = (
                    ["baseline", "candidate"]
                    if index % 2 == 0
                    else ["candidate", "baseline"]
                )
                for position, variant in enumerate(order):
                    worker = workers[variant]
                    worker.stdin.write("step\n")
                    worker.stdin.flush()
                    row = read(worker)
                    assert row["round"] == index and row["elapsed_ns"] > 0
                    row.update(variant=variant, position=position, pair_index=index)
                    output.write(json.dumps(row) + "\n")
        for worker in workers.values():
            worker.stdin.write("finish\n")
            worker.stdin.flush()
            worker.stdin.close()
            assert worker.wait(timeout=180) == 0
        receipts.append(
            dict(
                group=stem,
                ready=ready,
                measured_pairs=rounds,
                warmup_pairs=2,
                started=started,
                raw=stem + ".jsonl",
                exit_codes={v: w.returncode for v, w in workers.items()},
                binary_sha256={
                    v: hashlib.sha256(
                        (binaries / ("wal-encode-" + v)).read_bytes()
                    ).hexdigest()
                    for v in workers
                },
            )
        )
        (root / "run-receipts.json").write_text(json.dumps(receipts, indent=2) + "\n")
        print(stem, flush=True)
    finally:
        for worker in workers.values():
            if worker.poll() is None:
                worker.kill()
                worker.wait()
        for error in errors:
            error.close()
