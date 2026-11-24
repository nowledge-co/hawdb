# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Alternate one complete synchronized mutation/checkpoint at a time."""
import datetime
import hashlib
import json
import select
import subprocess
import sys
import time
from pathlib import Path

binaries = Path(sys.argv[1]).resolve()
root = Path(sys.argv[2]).resolve()
rounds = int(sys.argv[3])
smoke = len(sys.argv) > 4 and sys.argv[4] == "smoke"
assert rounds > 0 and not root.exists()
root.mkdir(parents=True)
receipts = []

def read_row(process):
    assert select.select([process.stdout], [], [], 180)[0], "worker response timeout"
    line = process.stdout.readline()
    assert line, f"worker closed output (exit={process.poll()})"
    return json.loads(line)

for nodes, edges in ([(16, 64)] if smoke else [(128, 512), (512, 4096)]):
    for workload, mode in [("node_property", "step-node"), ("relationship_property", "step-relationship")]:
        stem = f"paired-{nodes}-{edges}-{workload}"
        workers = {}
        errors = []
        ready = {}
        commands = {}
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        tick = time.monotonic()
        try:
            # Both fixtures are prepared before either timed sequence starts.
            for variant in ["baseline", "candidate"]:
                binary = binaries / ("embedded-paired-" + variant)
                commands[variant] = [str(binary), str(root / (stem + "-" + variant + "-fixture")), str(nodes), str(edges), str(rounds), "carry" if variant == "candidate" else "rebuild", mode]
                error = (root / (stem + "-" + variant + ".stderr")).open("w")
                errors.append(error)
                worker = subprocess.Popen(commands[variant], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=error, text=True, bufsize=1)
                workers[variant] = worker
                row = read_row(worker)
                assert row['protocol'] == 'hawdb-checkpoint-pair-v1'
                assert row['ready'] == workload and row['automatic_checkpoint'] is False
                assert row['storage_version'] == ('hawdb-storage-v3' if variant == 'candidate' else 'hawdb-storage-v2')
                ready[variant] = row
            with (root / (stem + ".jsonl")).open("w") as output:
                for index in range(rounds + 2):
                    order = ["baseline", "candidate"] if index % 2 == 0 else ["candidate", "baseline"]
                    for position, variant in enumerate(order):
                        worker = workers[variant]
                        worker.stdin.write("step\n")
                        worker.stdin.flush()
                        row = read_row(worker)
                        assert row['workload'] == workload and row['round'] == index
                        assert row['automatic_checkpoint'] is False and row['mutation_ns'] > 0
                        row.update(variant=variant, position=position, pair_index=index)
                        output.write(json.dumps(row) + "\n")
                        output.flush()
                    if (index + 1) % 20 == 0:
                        print(json.dumps(dict(group=stem, completed_pairs=index+1, total_pairs=rounds+2)), flush=True)
            # Neither worker performs cleanup while the other is still timing.
            for worker in workers.values():
                worker.stdin.write("finish\n")
                worker.stdin.flush()
                worker.stdin.close()
                assert worker.wait(timeout=180) == 0
            receipt = dict(group=stem, nodes=nodes, edges=edges, workload=workload, measured_pairs=rounds, warmup_pairs=2, raw=stem+".jsonl", ready=ready, commands=commands, started=started, elapsed_seconds=time.monotonic()-tick, exit_codes={v:p.returncode for v,p in workers.items()}, binary_sha256={v:hashlib.sha256((binaries/("embedded-paired-"+v)).read_bytes()).hexdigest() for v in workers})
            receipts.append(receipt)
            (root / "run-receipts.json").write_text(json.dumps(receipts, indent=2) + "\n")
            print(json.dumps(receipt), flush=True)
        finally:
            for worker in workers.values():
                if worker.poll() is None:
                    worker.kill()
                    worker.wait()
            for error in errors:
                error.close()
