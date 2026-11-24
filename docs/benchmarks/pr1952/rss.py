# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Darwin process RSS control, including reader open; not query-account memory."""

import json
import pathlib
import re
import subprocess
import sys

baseline, candidate, fixture, queries, output = sys.argv[1:]
root = pathlib.Path(output)
root.mkdir(exist_ok=False, parents=True)
requests = [{"query": row["text"]} for row in json.loads(pathlib.Path(queries).read_text())]
results = []
for arm, binary in enumerate([baseline, candidate]):
    path = root / f"arm-{arm}.txt"
    response = subprocess.run(["/usr/bin/time", "-l", "-o", str(path), binary, "worker", fixture],
                              input="".join(json.dumps(row) + "\n" for row in requests),
                              text=True, capture_output=True, check=True)
    rows = [json.loads(line) for line in response.stdout.splitlines()]
    if len(rows) != len(requests) or any("error" in row for row in rows):
        raise AssertionError("incomplete RSS control")
    match = re.search(r"(\d+)\s+maximum resident set size", path.read_text())
    if not match:
        raise ValueError("missing Darwin RSS counter")
    results.append({"arm": arm, "max_rss_bytes": int(match.group(1)), "queries": len(rows)})
    semantic = [{key: row[key] for key in ["hits", "total_hits", "truncated", "document_count",
                                          "filtered_document_count"]} for row in rows]
    if arm == 0:
        expected = semantic
    elif semantic != expected:
        raise AssertionError("RSS control ranking mismatch")
(root / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
print(json.dumps(results))
