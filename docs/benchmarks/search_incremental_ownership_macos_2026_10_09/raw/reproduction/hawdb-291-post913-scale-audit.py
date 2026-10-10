import hashlib
import json
import runpy
import subprocess
from pathlib import Path

evidence = Path('/private/tmp/hawdb-291-post913-scale-evidence')
worktree = Path('/private/tmp/hawdb-291-post913-qualification')
manifest = json.loads((evidence / 'manifest.json').read_text())
candidate = manifest['commit']
source = json.loads(Path('/private/tmp/hawdb-291-post913-source.json').read_text())
assert candidate == source['commit'] == '41e10dacf8b2589e416c01b9b047db98dfe01acd'
assert manifest['git_tree'] == source['integrated_tree']
assert manifest['source_files_sha256'] == source['source_files_sha256']
# Read committed blobs in one batch, preserving the original SHA-256 binding.
file_names = list(manifest['source_files_sha256'])
requests = ''.join(candidate + ':' + name + '\n' for name in file_names)
process = subprocess.run(['git', 'cat-file', '--batch'], cwd=worktree, input=requests.encode(), capture_output=True, check=True)
raw = process.stdout
cursor = 0
source_matches = True
for name in file_names:
    line_end = raw.index(b'\n', cursor)
    fields = raw[cursor:line_end].split()
    assert len(fields) == 3 and fields[1] == b'blob'
    length = int(fields[2])
    cursor = line_end + 1
    content = raw[cursor:cursor + length]
    source_matches = source_matches and hashlib.sha256(content).hexdigest() == manifest['source_files_sha256'][name]
    cursor += length
    assert raw[cursor:cursor + 1] == b'\n'
    cursor += 1
assert cursor == len(raw)
frozen_worktree_matches = all(hashlib.sha256((worktree / name).read_bytes()).hexdigest() == expected for name, expected in manifest['source_files_sha256'].items()) and not subprocess.check_output(['git', 'status', '--porcelain'], cwd=worktree, text=True)
rows = []
raw_results = []
for documents, touches in [(81920, 10), (327680, 10), (327680, 100)]:
    label = f'd{documents}-k{touches}'
    cell = next((row for row in manifest['cells'] if row['label'] == label), None)
    if cell is None or cell['state'] != 'finished':
        rows.append({'label': label, 'state': 'pending' if cell is None else cell['state']})
        continue
    stdout = evidence / (label + '.stdout.log')
    stderr = evidence / (label + '.stderr.log')
    log_hashes_match = (
        hashlib.sha256(stdout.read_bytes()).hexdigest() == cell['stdout_sha256']
        and hashlib.sha256(stderr.read_bytes()).hexdigest() == cell['stderr_sha256']
    )
    lines = [line.removeprefix('search_mutation ') for line in stdout.read_text().splitlines() if line.startswith('search_mutation ')]
    if cell['exit_code'] != 0 or len(lines) != 1:
        rows.append({'label': label, 'state': 'failed', 'exit_code': cell['exit_code'], 'log_hashes_match': log_hashes_match})
        continue
    result = json.loads(lines[0])
    raw_results.append(result)
    result_path = evidence / (label + '.result.json')
    rounds = result['sustained_rounds']
    peaks = [result['lifetime_peak_resident_bytes'], result['upsert_lifetime_peak_resident_bytes']] + [row['lifetime_peak_resident_bytes'] for row in rounds]
    steady = [result['steady_resident_bytes'], result['upsert_steady_resident_bytes']] + [row['steady_resident_bytes'] for row in rounds]
    budget = 268435456
    checks = {
        'successful_exit_and_log_hashes': log_hashes_match,
        'raw_result_matches_logged_result': result_path.is_file() and json.loads(result_path.read_text()) == result,
        'fixture_shape': result['document_count'] == documents and result['touches'] == touches and result['logical_corpus_body_bytes'] == documents * 65536,
        'vector_fixture': result['embedding_dimension'] == 384 and result['full_vector_document_count'] == documents and result['full_vector_payload_bytes'] > 0 and result['full_rabitq_artifact_bytes'] > 0 and result['upsert_vector_payload_bytes'] > 0 and result['upsert_rabitq_artifact_bytes'] > 0,
        'explicit_resource_configuration': result['build_memory_budget_bytes'] == budget and result['lexical_build_memory_bytes'] == 8388608 and result['max_segment_uncompressed_bytes'] == 67108864 and result['configured_file_descriptor_limit'] == 1024 and manifest['os_file_descriptor_limits'][0] == 4096 and result['compaction_every_rounds'] == 1 and result['reuse_validated_retractions'] is True,
        'sustained_rounds_and_merges': len(rounds) == 128 and all(row['compaction_published'] and row['upserted_documents'] == 2 and row['deleted_documents'] == 0 and row['checkpoint_bytes'] > 0 and row['compaction_artifact_bytes'] > 0 for row in rounds),
        'expected_visible_counts': all(row['document_count'] == documents + 32 - touches + row['round'] + 1 for row in rounds),
        'complete_peak_and_steady_rss': all(value is not None and 0 < value <= budget for value in peaks + steady),
        'new_bytes_smaller_than_full': 0 < result['upsert_checkpoint_bytes'] < result['full_generation_bytes'] and 0 < result['mutation_checkpoint_bytes'] < result['full_generation_bytes'],
    }
    rows.append({
        'label': label,
        'state': 'passed' if all(checks.values()) else 'failed',
        'checks': checks,
        'logical_corpus_bytes': result['logical_corpus_body_bytes'],
        'touches': touches,
        'full_artifact_bytes': result['full_generation_bytes'],
        'upsert_artifact_bytes': result['upsert_checkpoint_bytes'],
        'delete_artifact_bytes': result['mutation_checkpoint_bytes'],
        'upsert_vector_payload_bytes': result['upsert_vector_payload_bytes'],
        'upsert_rabitq_artifact_bytes': result['upsert_rabitq_artifact_bytes'],
        'upsert_source_hydrated_documents': result['upsert_source_hydrated_documents'],
        'delete_source_hydrated_documents': result['source_hydrated_documents'],
        'peak_resident_bytes': max((value for value in peaks if value is not None), default=None),
        'max_steady_resident_bytes': max((value for value in steady if value is not None), default=None),
        'rounds': len(rounds),
        'merges': sum(row['compaction_published'] for row in rounds),
        'sustained_checkpoint_bytes': sum(row['checkpoint_bytes'] for row in rounds),
        'sustained_merge_artifact_bytes': sum(row['compaction_artifact_bytes'] for row in rounds),
        'sustained_merge_source_bytes': sum(row['compaction_source_bytes'] for row in rounds),
        'sustained_artifact_write_amplification': sum(row['checkpoint_bytes'] + row['compaction_artifact_bytes'] for row in rounds) / sum(row['upserted_documents'] * (65536 + 384 * 4) for row in rounds),
        'elapsed_seconds': cell['elapsed_seconds'],
        'scratch_allocated_highwater_bytes': cell['scratch_allocated_highwater_bytes'],
        'raw_stdout_sha256': cell['stdout_sha256'],
        'raw_stderr_sha256': cell['stderr_sha256'],
        'raw_result_sha256': hashlib.sha256(result_path.read_bytes()).hexdigest() if result_path.is_file() else None,
    })
terminal_source_receipt = manifest.get('commit_after') == candidate and manifest.get('source_files_sha256_after') == manifest['source_files_sha256'] and manifest.get('patch_sha256_after') == manifest['patch_sha256_before'] and manifest.get('working_tree_status_after') == ''
report = {'candidate_commit': candidate, 'qualified_source_hashes_match_commit': source_matches, 'frozen_worktree_source_unchanged': frozen_worktree_matches, 'driver_terminal_source_receipt_valid': terminal_source_receipt, 'all_three_cells_passed': source_matches and frozen_worktree_matches and terminal_source_receipt and all(row['state'] == 'passed' for row in rows), 'cells': rows}
byte_gate = runpy.run_path('/private/tmp/hawdb-291-post913-byte-scaling.py')['evaluate_byte_scaling'](raw_results)
report['incremental_byte_scaling'] = byte_gate
if all(row['state'] == 'passed' for row in rows):
    small, large, many = rows
    report['scaling'] = {
        'full_artifact_n4_ratio': large['full_artifact_bytes'] / small['full_artifact_bytes'],
        'upsert_fixed_k_n4_ratio': large['upsert_artifact_bytes'] / small['upsert_artifact_bytes'],
        'delete_fixed_k_n4_ratio': large['delete_artifact_bytes'] / small['delete_artifact_bytes'],
        'upsert_fixed_n_k10_ratio': many['upsert_artifact_bytes'] / large['upsert_artifact_bytes'],
        'delete_fixed_n_k10_ratio': many['delete_artifact_bytes'] / large['delete_artifact_bytes'],
        'upsert_vector_payload_fixed_n_k10_ratio': many['upsert_vector_payload_bytes'] / large['upsert_vector_payload_bytes'],
        'upsert_rabitq_fixed_n_k10_ratio': many['upsert_rabitq_artifact_bytes'] / large['upsert_rabitq_artifact_bytes'],
    }
historical_path = Path('/private/tmp/hawdb-291-exact-allocation-historical-audit.json')
historical = json.loads(historical_path.read_text())
report['historical_matched_before_after_verified'] = False
if report['all_three_cells_passed']:
    old = historical['raw_result']
    current = json.loads((evidence / 'd327680-k10.result.json').read_text())
    matched = (historical.get('passed') is True and all(historical['checks'].values())
        and all(historical['provenance_checks'].values())
        and old['base_documents'] == current['document_count'] == 327680
        and old['seed_documents'] == 32
        and old['touches'] == current['touches'] == 10
        and old['content_bytes_per_document'] == 65536
        and old['embedding_dimension'] == current['embedding_dimension'] == 384
        and old['logical_base_body_bytes'] == current['logical_corpus_body_bytes'] == 21474836480
        and historical['actual_checkpoint_bytes'] == old['actual_checkpoint_build']['generation_bytes'])
    report['historical_matched_before_after_verified'] = matched
    report['before_after'] = {'historical_revision': historical['revision'],
        'current_revision': candidate, 'qualified_main_commit': candidate,
        'historical_audit_sha256': hashlib.sha256(historical_path.read_bytes()).hexdigest(),
        'before_artifact_bytes': historical['actual_checkpoint_bytes'],
        'after_artifact_bytes': current['upsert_checkpoint_bytes'],
        'before_over_after_ratio': historical['actual_checkpoint_bytes'] / current['upsert_checkpoint_bytes'],
        'scope': historical['scope'], 'comparison_limit': historical['before_after']['comparison_limit']}

Path('/private/tmp/hawdb-291-post913-scale-audit.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
