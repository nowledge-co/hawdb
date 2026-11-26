# Copyright 2026 Nowledge
# SPDX-License-Identifier: Apache-2.0

"""Summarize all paired samples without modifying the recorded inputs."""
import json
import math
import statistics
import sys
from pathlib import Path

root = Path(sys.argv[1])
receipts = json.loads((root / 'run-receipts.json').read_text())
report = []
def stats(values):
    values = sorted(values)
    return dict(p50=statistics.median(values), p95=values[math.ceil(len(values)*.95)-1], p99=values[math.ceil(len(values)*.99)-1], minimum=values[0], maximum=values[-1])
for receipt in receipts:
    assert all(code == 0 for code in receipt['exit_codes'].values())
    records = [json.loads(line) for line in (root/receipt['raw']).read_text().splitlines()]
    pairs = {}
    groups = {v:[] for v in ['baseline','candidate']}
    for row in records:
        assert row['automatic_checkpoint'] is False
        row['total_ns'] = row['checkpoint_ns'] + row['mutation_ns']
        if row['warmup']: continue
        groups[row['variant']].append(row)
        pair = pairs.setdefault(row['pair_index'], {})
        assert row['variant'] not in pair
        pair[row['variant']] = row
    count = receipt['measured_pairs']
    assert len(pairs) == count
    assert all(len(rows)==count for rows in groups.values())
    fields = ['checkpoint_ns','mutation_ns','total_ns','new_graph_bytes','new_adjacency_bytes']
    distributions = {v:{field:stats([row[field] for row in rows]) for field in fields} for v,rows in groups.items()}
    ratios = {}
    for field in ['checkpoint_ns','mutation_ns','total_ns']:
        changes = [(index,(pair['candidate'][field]/pair['baseline'][field]-1)*100,pair['baseline']['position']) for index,pair in sorted(pairs.items())]
        ratios[field] = dict(percent=stats([change for _,change,_ in changes]), absolute_ns=stats([pair['candidate'][field]-pair['baseline'][field] for pair in pairs.values()]), baseline_first_median_percent=statistics.median([change for _,change,position in changes if position==0]), candidate_first_median_percent=statistics.median([change for _,change,position in changes if position==1]), first_half_median_percent=statistics.median([change for _,change,_ in changes[:count//2]]),second_half_median_percent=statistics.median([change for _,change,_ in changes[count//2:]]))
    report.append(dict(nodes=receipt['nodes'],edges=receipt['edges'],workload=receipt['workload'],measured_pairs=count,distributions=distributions,paired_changes=ratios))
print(json.dumps(report,indent=2))
