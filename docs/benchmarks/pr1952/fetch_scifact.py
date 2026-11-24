# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Fetch pinned public BEIR inputs; run with a temporary PyArrow environment."""

import csv
import hashlib
import json
import pathlib
import subprocess
import sys

import pyarrow.parquet as pq

root = pathlib.Path(sys.argv[1])
root.mkdir(parents=True, exist_ok=False)
files = [
    ("corpus.parquet", "BeIR/scifact", "b3b5335604bf5ee3c4447671af975ea25143d4f5",
     "corpus/corpus-00000-of-00001.parquet",
     "243324b35f03d82bd6d98a5f575966876e86cad7ce16e5333a35b1b793dc4f45"),
    ("queries.parquet", "BeIR/scifact", "b3b5335604bf5ee3c4447671af975ea25143d4f5",
     "queries/queries-00000-of-00001.parquet",
     "1c37956c5dc8b810b60302323c24d1a9e79e26411ba8f5ad9d0888642e2a9034"),
    ("test.tsv", "BeIR/scifact-qrels", "2938d17dc3b09882fdb8c12bbbe2e2dc0e75a029",
     "test.tsv", "0864bb985e0ca2367ba217977e72004d549054b2b06666ed9d4825ac7c21284c"),
]
for name, dataset, revision, path, expected in files:
    destination = root / name
    url = f"https://huggingface.co/datasets/{dataset}/resolve/{revision}/{path}"
    subprocess.run(["curl", "-fLsS", "--retry", "2", "--max-time", "60", url,
                    "-o", str(destination)], check=True)
    if hashlib.sha256(destination.read_bytes()).hexdigest() != expected:
        raise ValueError(f"dataset hash mismatch: {name}")
corpus = pq.read_table(root / "corpus.parquet").to_pylist()
queries = pq.read_table(root / "queries.parquet").to_pylist()
corpus.sort(key=lambda row: row["_id"])
with (root / "test.tsv").open(encoding="utf-8") as source:
    query_ids = {row["query-id"] for row in csv.DictReader(source, delimiter="\t")}
selected = [row for row in queries if row["_id"] in query_ids]
if len(corpus) != 5183 or len(selected) != 300:
    raise ValueError("incomplete SciFact input")
(root / "corpus.jsonl").write_text("".join(json.dumps(row, ensure_ascii=False) + "\n"
                                           for row in corpus), encoding="utf-8")
(root / "queries-test.json").write_text(json.dumps(selected), encoding="utf-8")
for name, expected in [
    ("corpus.jsonl", "42e456e90f0db2a88f70334c6ce72456faccd0a5042de06781a63eba966142ae"),
    ("queries-test.json", "c7830e355aa9868a22ee98e015fba2fb20f4fcf52fedf449d2867b57f90d810f"),
]:
    actual = hashlib.sha256((root / name).read_bytes()).hexdigest()
    if actual != expected:
        raise ValueError(f"converted input hash mismatch: {name}")
    print(name, actual)
