"""Bind all named publication families to the actual fresh recovery log."""
import hashlib
import json
import re
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-post913-qualification'
source = json.loads((BASE / 'hawdb-291-post913-source.json').read_text())
native = json.loads((BASE / 'hawdb-291-post913-native.json').read_text())
assert native['state'] == 'passed' and native['source_unchanged']
power = next(row for row in native['checks'] if row['label'] == 'power-loss')
log = Path(power['log'])
assert power['exit_code'] == 0
assert hashlib.sha256(log.read_bytes()).hexdigest() == power['log_sha256']
text = log.read_text()
assert 'test result: ok. 11 passed; 0 failed; 0 ignored;' in text
names = re.findall(r'^test api::tests::power_loss::search_projection::(\w+) \.\.\.', text, re.M)
assert len(names) == len(set(names)) == 11
family_lines = re.findall(r'^search-power-(?:publication|initial)-v1 .*\bplans=\d+.*$', text, re.M)
plans = [int(re.search(r'\bplans=(\d+)', line).group(1)) for line in family_lines]
assert len(plans) == power['cut_families'] == 30
assert all(value > 0 for value in plans) and sum(plans) == power['fault_plans'] == 3260
expected_boundaries = {('Write', 'After'), ('Rename', 'Before'), ('Rename', 'After')}
for operation in ['Append', 'Compaction', 'PartitionedAppend', 'PartitionedMutation', 'Mutation']:
    rows = [line for line in family_lines if 'operation=' + operation + ' ' in line]
    boundaries = {(re.search(r'event=(\w+)', line).group(1), re.search(r'boundary=(\w+)', line).group(1)) for line in rows}
    expected = expected_boundaries - {('Write', 'After')} if operation == 'Mutation' else expected_boundaries
    assert boundaries == expected and len(rows) == len(expected), operation
for compaction in ['false', 'true']:
    rows = [line for line in family_lines if 'compaction=' + compaction + ' ' in line]
    boundaries = {(re.search(r'event=(\w+)', line).group(1), re.search(r'boundary=(\w+)', line).group(1)) for line in rows}
    assert boundaries == expected_boundaries and len(rows) == 3
    assert any('event=Write' in line and int(re.search(r'torn=(\d+)', line).group(1)) > 0 for line in rows)
for replacement in ['false', 'true']:
    rows = [line for line in family_lines if 'replaces_existing=' + replacement + ' ' in line]
    assert {int(re.search(r'cut=(\d+)', line).group(1)) for line in rows} == set(range(5))
    assert len(rows) == 5 and any('cut=0' in line and int(re.search(r'torn=(\d+)', line).group(1)) > 0 for line in rows)
bindings = {}
for name in ['src/api/tests/power_loss/search_projection.rs', 'src/api/tests/power_loss.rs',
             'crates/storage/src/power_loss.rs', 'crates/storage/src/power_loss/image.rs']:
    actual = hashlib.sha256((WORK / name).read_bytes()).hexdigest()
    assert actual == source['source_files_sha256'][name], name
    bindings[name] = actual
result = {'state': 'passed', 'qualified_commit': source['commit'], 'log_sha256': power['log_sha256'],
          'actual_cases': names, 'cut_families': 30, 'fault_plans': sum(plans),
          'actual_family_lines': family_lines, 'source_sha256': bindings,
          'coverage': [
              'Partitioned initial import: new and existing roots; data write, private-prefix rename before/after, final real-selector rename before/after.',
              'Append, overlapping compaction, replacement/delete and partitioned append/mutation: real write/rename cuts and complete old/new hydration/query oracles.',
              'Lost unsynchronized writes, actual torn byte ranges, namespace reordering and lost responses after completed publication.',
              'Pinned old hydration/query closure verified before opening a second full reader under unchanged project FD32.',
          ],
          'limits': 'Finite modeled images under completed POSIX synchronization and same-directory atomic rename. Not exhaustive hardware power-loss qualification or a universal ANN recall proof.'}
(BASE / 'hawdb-291-post913-recovery-coverage.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps({key: result[key] for key in ['state', 'qualified_commit', 'cut_families', 'fault_plans']}))
