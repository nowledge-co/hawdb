"""Audit the unchanged historical writer, not a full-build proxy."""
import hashlib
import json
from pathlib import Path
import subprocess

BASE = Path('/private/tmp')
WORKTREE = BASE / 'hawdb-291-historical-checkpoint-review'
EVIDENCE = BASE / 'hawdb-291-historical-checkpoint-evidence'
REVISION = '8cbe16f8f76f3472149b971970bda7d7511da3b8'


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


manifest = json.loads((EVIDENCE / 'manifest.json').read_text())
sources = manifest['source_files_sha256']
compile_receipt = manifest['compile_receipt']
source_matches_commit = all(
    hashlib.sha256(subprocess.check_output(
        ['git', 'show', REVISION + ':' + name], cwd=WORKTREE
    )).hexdigest() == expected for name, expected in sources.items()
)
frozen = (
    subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORKTREE, text=True).strip() == REVISION
    and not subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORKTREE, text=True)
    and all(sha(WORKTREE / name) == expected for name, expected in sources.items())
)
configuration = manifest['configuration']
provenance = {
    'revision_and_source': manifest['revision'] == REVISION and source_matches_commit and frozen,
    'wrapper_source': sha(BASE / 'hawdb-291-historical-checkpoint-probe.rs') == manifest['wrapper_source_sha256'] == compile_receipt['source_sha256'],
    'binary': sha(manifest['command'][0]) == compile_receipt['binary_sha256'],
    'release_libraries': all(sha(row['rlib']) == row['sha256'] for row in compile_receipt['libraries'].values()),
    'fixture_and_common_settings': configuration['base_documents'] == 327680 and configuration['seed_documents'] == 32 and configuration['content_bytes'] == 65536 and configuration['embedding_dimension'] == 384 and configuration['touches'] == 10 and configuration['rabitq_bit_width'] == 4 and configuration['rabitq_segment_rows'] == 1024 and configuration['rabitq_transform_seed'] == 0x534b45494e565134 and configuration['lexical_build_memory_bytes'] == 8388608 and configuration['max_segment_uncompressed_bytes'] == 67108864 and manifest['os_fd_limits'][0] == 4096,
}
report = {
    'schema': 'hawdb.search.historical-checkpoint.audit.v1',
    'revision': REVISION,
    'state': manifest['state'],
    'passed': False,
    'provenance_checks': provenance,
    'elapsed_seconds': manifest.get('elapsed_seconds'),
    'source_files_sha256': sources,
    'wrapper_source_sha256': manifest['wrapper_source_sha256'],
    'binary_sha256': compile_receipt['binary_sha256'],
    'scope': manifest['scope'],
    'resource_boundary': 'The old API has no complete-operation reservation or project descriptor admission. No same-budget historical RSS qualification is claimed. Historical and candidate formats/dependencies differ; observed bytes do not isolate only the incremental-segment change.',
}
stdout = EVIDENCE / 'd327680-k10.stdout.log'
stderr = EVIDENCE / 'd327680-k10.stderr.log'
if manifest['state'] == 'finished':
    checks = {
        'successful_exit': manifest['exit_code'] == 0,
        'unchanged_source_after_execution': manifest['source_unchanged'] and manifest['source_files_sha256_after'] == sources,
        'complete_logs': sha(stdout) == manifest['stdout_sha256'] and sha(stderr) == manifest['stderr_sha256'],
    }
    lines = [json.loads(line.removeprefix('historical_checkpoint ')) for line in stdout.read_text().splitlines() if line.startswith('historical_checkpoint ')]
    result_path = EVIDENCE / 'd327680-k10.result.json'
    checks['one_complete_result'] = len(lines) == 1 and result_path.is_file() and json.loads(result_path.read_text()) == lines[0]
    if checks['one_complete_result']:
        result = lines[0]
        initial = result['initial_build']
        checkpoint = result['actual_checkpoint_build']
        reads = result['source_reads']
        checks.update({
            'complete_logical_fixture': result['revision'] == REVISION and result['base_documents'] == 327680 and result['seed_documents'] == 32 and result['visible_documents'] == 327712 and result['logical_base_body_bytes'] == 21474836480 and result['content_bytes_per_document'] == 65536 and result['embedding_dimension'] == 384 and result['touches'] == 10,
            'entire_corpus_and_vectors_rebuilt': initial['document_count'] == initial['vector_document_count'] == checkpoint['document_count'] == checkpoint['vector_document_count'] == 327712 and initial['embedding_dimension'] == checkpoint['embedding_dimension'] == 384,
            'full_source_payload_traversed': reads['complete_source_payload_read'] and reads['hydration_segment_bytes_read'] == initial['document_payload_bytes'] > 0,
            'published_successfully': initial['generation'] < checkpoint['generation'] and initial['active_manifest_published_last'] and checkpoint['active_manifest_published_last'] and not initial['cleanup_retry_required'] and not checkpoint['cleanup_retry_required'] and checkpoint['generation_bytes'] > 0 and checkpoint['lexical_artifact_bytes'] > 0 and checkpoint['vector_payload_bytes'] > 0 and checkpoint['rabitq_artifact_bytes'] > 0,
            'ordinary_reopen_and_changed_row_values': result['ordinary_final_reopen'] and result['changed_rows_validated'] == 10,
            'reported_common_settings': result['historical_resource_limits']['rabitq_bit_width'] == 4 and result['historical_resource_limits']['rabitq_transform_seed'] == 0x534b45494e565134 and result['historical_resource_limits']['rabitq_segment_rows'] == 1024 and result['historical_resource_limits']['lexical_build_memory_bytes'] == 8388608 and result['historical_resource_limits']['max_segment_uncompressed_bytes'] == 67108864,
        })
        report.update(actual_checkpoint_bytes=checkpoint['generation_bytes'], actual_checkpoint_lexical_bytes=checkpoint['lexical_artifact_bytes'], actual_checkpoint_dense_vector_bytes=checkpoint['vector_payload_bytes'], actual_checkpoint_rabitq_bytes=checkpoint['rabitq_artifact_bytes'], complete_source_payload_bytes=reads['hydration_segment_bytes_read'], raw_result=result, raw_result_sha256=sha(result_path))
    report.update(checks=checks, passed=all(provenance.values()) and all(checks.values()), raw_stdout_sha256=sha(stdout), raw_stderr_sha256=sha(stderr))
    report['state'] = 'passed' if report['passed'] else 'failed'
candidate_path = BASE / 'hawdb-291-scale-evidence-exact-allocation/d327680-k10.result.json'
candidate_manifest = json.loads((BASE / 'hawdb-291-scale-evidence-exact-allocation/manifest.json').read_text())
candidate_cell = next((row for row in candidate_manifest['cells'] if row['label'] == 'd327680-k10'), None)
if report['passed'] and candidate_path.is_file() and candidate_cell and candidate_cell['state'] == 'finished' and candidate_cell['exit_code'] == 0:
    candidate = json.loads(candidate_path.read_text())
    assert candidate['document_count'] == 327680 and candidate['touches'] == 10 and candidate['embedding_dimension'] == 384
    report['before_after'] = {
        'before_revision': REVISION,
        'after_revision': candidate_manifest['commit'],
        'same_pre_update_logical_documents': 327712,
        'before_actual_checkpoint_bytes': report['actual_checkpoint_bytes'],
        'after_actual_checkpoint_bytes': candidate['upsert_checkpoint_bytes'],
        'after_over_before_ratio': candidate['upsert_checkpoint_bytes'] / report['actual_checkpoint_bytes'],
        'before_over_after_ratio': report['actual_checkpoint_bytes'] / candidate['upsert_checkpoint_bytes'],
        'candidate_raw_result_sha256': sha(candidate_path),
        'comparison_limit': 'Old initialization includes all32seed rows at once; new initialization publishes32seed segments. Same logical data andK10 replacements, different persistent formats and locked dependencies. No isolated incremental-only speedup or device-write ratio is claimed.',
    }
path = BASE / 'hawdb-291-exact-allocation-historical-audit.json'
path.write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({key: report.get(key) for key in ['state', 'passed', 'provenance_checks', 'checks', 'actual_checkpoint_bytes', 'before_after']}))
