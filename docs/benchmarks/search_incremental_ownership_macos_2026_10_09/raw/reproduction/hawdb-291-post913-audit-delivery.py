"""Inspect actual main artifacts and independent delivery gates for issue291."""
import hashlib
import json
import math
import re
import runpy
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path, PurePosixPath

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-incremental-recovery-followup'
REPO = 'nowledge-co/hawdb'
QUALIFIED = '41e10dacf8b2589e416c01b9b047db98dfe01acd'
ROOT = 'docs/benchmarks/search_incremental_ownership_macos_2026_10_09/'
REQUIRED_CI = ['ci/skein-bazel-build', 'ci/skein-bazel-test-crates', 'ci/skein-bazel-test-root', 'ci/skein-storage-crash-recovery']


def command(*args):
    return subprocess.check_output(args, cwd=WORK)


def gh(*args):
    return json.loads(command('gh', *args))


def git(*args):
    return command('git', *args).decode().strip()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_blobs(ref, names):
    requests = ''.join(ref + ':' + name + '\n' for name in names).encode()
    raw = subprocess.run(['git', 'cat-file', '--batch'], cwd=WORK, input=requests, capture_output=True, check=True).stdout
    values = {}
    cursor = 0
    for name in names:
        end = raw.index(b'\n', cursor)
        fields = raw[cursor:end].split()
        assert len(fields) == 3 and fields[1] == b'blob', (name, fields)
        length = int(fields[2])
        cursor = end + 1
        values[name] = raw[cursor:cursor + length]
        cursor += length
        assert raw[cursor:cursor + 1] == b'\n'
        cursor += 1
    assert cursor == len(raw)
    return values


issue = gh('issue', 'view', '291', '--repo', REPO, '--json', 'state,body,url')
main = gh('api', 'repos/' + REPO + '/git/ref/heads/main')['object']['sha']
assert main == git('rev-parse', 'origin/main'), 'fetch live main before classifying delivery'
prs = gh('pr', 'list', '--repo', REPO, '--head', 'docs/291-incremental-qualification', '--state', 'all', '--json', 'number')
pr = gh('pr', 'view', str(prs[0]['number']), '--repo', REPO,
        '--json', 'number,state,headRefOid,baseRefName,body,mergeCommit,comments,reviews,statusCheckRollup,labels,url') if prs else None
classifier = runpy.run_path(str(BASE / 'hawdb-291-post913-classify-main.py'))
classification = classifier['classify_revision'](WORK, QUALIFIED, main)
source_changes = [row['path'] for row in classification['changes']]
unqualified_changes = classification['unqualified_changes']
source_equivalence = classification['search_runtime_benchmark_tests_and_external_dependencies_unchanged']
report_path = ROOT + 'report.json'
report_exists = subprocess.run(['git', 'cat-file', '-e', main + ':' + report_path], cwd=WORK, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
requirements = {name: False for name in ['AC1', 'AC2', 'AC3', 'AC4', 'AC5']}
raw_ok = native_ok = profiles_ok = fuzz_ok = False
report_sha = None
if report_exists:
    report_bytes = command('git', 'show', main + ':' + report_path)
    report_sha = digest(report_bytes)
    report = json.loads(report_bytes)
    assert report['qualified_commit'] == QUALIFIED and report['includes_PR979']
    assert report['schema'] == 'hawdb.search.incremental-ownership.qualification.v2'
    names = list(report['raw_files'])
    assert all(not PurePosixPath(name).is_absolute() and '..' not in PurePosixPath(name).parts and '\n' not in name for name in names)
    blobs = read_blobs(main, [ROOT + name for name in names])
    raw_ok = all(digest(blobs[ROOT + name]) == row['sha256'] and len(blobs[ROOT + name]) == row['bytes'] for name, row in report['raw_files'].items())
    assert raw_ok

    def raw_json(name):
        return json.loads(blobs[ROOT + name])

    def raw_text(name):
        return blobs[ROOT + name].decode()

    native = raw_json('provenance/hawdb-291-post913-native.json')
    native_ok = native['state'] == 'passed' and native['source_unchanged']
    native_ok = native_ok and all(row['exit_code'] == 0 and digest(blobs[ROOT + 'raw/commands/' + Path(row['log']).name]) == row['log_sha256'] for row in native['checks'])
    profiles = raw_json('raw/current-search-profiles/receipt.json')
    profiles_ok = profiles['state'] == 'passed' and profiles['source_unchanged'] and profiles['exit_code'] == 0 and len(profiles['targets']) == 5
    for row in profiles['targets']:
        package, target = row['target'][2:].split(':', 1)
        log = blobs[ROOT + 'raw/current-search-profiles/testlogs/' + package + '/' + target + '/test.log']
        counts = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', log.decode())
        profiles_ok = profiles_ok and bool(counts) and all(int(p) > 0 and int(f) == 0 and int(i) == 20 for p, f, i in counts)
        profiles_ok = profiles_ok and digest(log) == row['artifacts']['test.log']['sha256']
    if '.bazelversion' in source_changes:
        source_equivalence = source_equivalence and 'Starting local Bazel server (9.3.0)' in raw_text('raw/current-search-profiles/full-five-profiles.log')
    initial = raw_json('raw/current-fuzz/receipt.json')
    resolution = report['local_fuzz']['resolution']
    resolved = raw_json('raw/current-fuzz-retries/receipt.json') if 'raw/current-fuzz-retries/receipt.json' in report['raw_files'] else resolution
    phase = raw_json('raw/current-bazel-phase/receipt.json')
    fuzz_ok = phase['state'] == 'passed' and phase['source_unchanged'] and initial['source_unchanged']
    fuzz_ok = fuzz_ok and len(initial['targets']) == len(initial['artifacts']) == 96 and resolved['state'] == 'passed' and resolved['final_exit_code'] == 0
    retries = {row['target']: row for row in resolved['checks']}
    for original in initial['artifacts']:
        retry = retries.get(original['target'])
        artifacts = retry['artifacts'] if retry else original['files']
        folder = BASE / ('hawdb-291-post913-fuzz-resolved' if retry else 'hawdb-291-post913-fuzz-evidence')
        prefix = 'raw/current-fuzz-retries/' if retry else 'raw/current-fuzz/'
        for file in artifacts:
            relative = str(Path(file['path']).relative_to(folder))
            content = blobs[ROOT + prefix + relative]
            fuzz_ok = fuzz_ok and digest(content) == file['sha256'] and len(content) == file['bytes']
            if Path(relative).name == 'test.log' and original['target'] != '//:hawdb_linux_ci_fuzz_smoke_test':
                counts = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', content.decode())
                fuzz_ok = fuzz_ok and bool(counts) and all(int(p) > 0 and int(f) == int(i) == 0 for p, f, i in counts)
            elif Path(relative).name == 'test.xml':
                xml = ET.fromstring(content)
                fuzz_ok = fuzz_ok and len(list(xml.iter('testcase'))) > 0 and not list(xml.iter('failure')) and not list(xml.iter('error'))
    results = [raw_json(label + '.result.json') for label in ['d81920-k10', 'd327680-k10', 'd327680-k100']]
    scale_ok = True
    for result, (documents, touches) in zip(results, [(81920, 10), (327680, 10), (327680, 100)]):
        rounds = result['sustained_rounds']
        rss = [result[key] for key in ['lifetime_peak_resident_bytes', 'upsert_lifetime_peak_resident_bytes', 'steady_resident_bytes', 'upsert_steady_resident_bytes']]
        rss += [row[key] for row in rounds for key in ['lifetime_peak_resident_bytes', 'steady_resident_bytes']]
        scale_ok = scale_ok and result['document_count'] == documents and result['touches'] == touches and result['logical_corpus_body_bytes'] == documents * 65536
        scale_ok = scale_ok and result['embedding_dimension'] == 384 and result['full_vector_document_count'] == documents
        scale_ok = scale_ok and result['build_memory_budget_bytes'] == 268435456 and result['lexical_build_memory_bytes'] == 8388608 and result['max_segment_uncompressed_bytes'] == 67108864 and result['configured_file_descriptor_limit'] == 1024
        scale_ok = scale_ok and len(rounds) == 128 and all(row['compaction_published'] and row['upserted_documents'] == 2 for row in rounds)
        scale_ok = scale_ok and all(value is not None and 0 < value <= 268435456 for value in rss)
        scale_ok = scale_ok and 0 < result['upsert_checkpoint_bytes'] < result['full_generation_bytes'] and 0 < result['mutation_checkpoint_bytes'] < result['full_generation_bytes']
    byte_gate = runpy.run_path(str(BASE / 'hawdb-291-post913-byte-scaling.py'))['evaluate_byte_scaling'](results)
    requirements['AC1'] = scale_ok and report['scale']['all_three_cells_passed'] and report['scale']['historical_matched_before_after_verified'] and byte_gate['passed'] and byte_gate == report['scale']['incremental_byte_scaling']
    requirements['AC2'] = 'compressed_compaction passed comparisons=294 distinct_vectors=true changed_embedding=true merges=3 initial_owners=2 pinned_reader=true rabitq_required=true' in raw_text('raw/commands/hawdb-291-post913-compressed-probe-run.log')
    search = raw_text('raw/current-search-profiles/testlogs/crates/search/hawdb_search_tests/test.log')
    policy = [
        'tier_configuration_bounds_normal_merges_without_starting_background_work',
        'crisis_merge_uses_a_bounded_pair_and_does_not_exceed_the_top_level',
        'crisis_merge_reduces_mixed_levels_and_preserves_mutations_and_pins',
        'scheduled_mixed_level_crisis_defers_before_staging_and_releases_its_permit',
        'scheduled_compaction_tracks_and_releases_the_qos_budget',
        'scheduled_compaction_observes_cancellation_before_qos_admission',
        'scheduled_compaction_requires_background_maintenance_capability',
        'cancelled_compaction_preserves_the_active_manifest',
    ]
    requirements['AC3'] = profiles_ok and all(re.search(r'::' + name + r' \.\.\. ok$', search, re.M) for name in policy)
    power = raw_text('raw/commands/hawdb-291-post913-power-loss.log')
    plans = re.findall(r'^search-power-(?:publication|initial)-v1 .*\bplans=(\d+)', power, re.M)
    requirements['AC4'] = native_ok and len(plans) == 30 and sum(map(int, plans)) == report['native_recovery']['plans'] > 0 and 'test result: ok. 11 passed; 0 failed; 0 ignored;' in power
    requirements['AC5'] = scale_ok and all(math.isclose(cell['sustained_artifact_write_amplification'], sum(row['checkpoint_bytes'] + row['compaction_artifact_bytes'] for row in result['sustained_rounds']) / sum(row['upserted_documents'] * (65536 + 384 * 4) for row in result['sustained_rounds'])) for cell, result in zip(report['scale']['cells'], results))
checks = {row.get('name', row.get('context')): row for row in pr.get('statusCheckRollup', [])} if pr else {}
ci_ok = all(checks.get(name, {}).get('conclusion', checks.get(name, {}).get('state')) == 'SUCCESS' for name in REQUIRED_CI)
head = pr['headRefOid'] if pr else ''
review_ok = False
if pr:
    review_ok = any(row['state'] == 'APPROVED' and row.get('commit', {}).get('oid') == head for row in pr['reviews'])
    comments = sorted(pr['comments'], key=lambda row: row['createdAt'])
    for review in comments:
        if head[:8] in review['body'] and len(review['body']) > 200:
            review_ok = review_ok or any(c['body'].strip() == '/approve' and c['createdAt'] >= review['createdAt'] and c['author']['login'] == review['author']['login'] for c in comments)
    labels = {row['name'] for row in pr['labels']}
    review_ok = review_ok and 'approved' in labels and 'do-not-merge/hold' not in labels and 'hold' not in labels
issue_reference = bool(pr and re.search(r'Issue Number:.*(?:close|ref) #291\b', pr['body']))
delivery = {
    'report_exists_on_live_main': report_exists, 'all_published_raw_hashes_match': raw_ok,
    'current_main_runtime_and_qualified_toolchain_equivalence': source_equivalence,
    'required_native_checks': native_ok, 'all_five_search_profiles': profiles_ok,
    'required96_fuzz_command_and_positive_current_source_evidence': fuzz_ok,
    'documentation_PR_merged_into_main': bool(pr and pr['state'] == 'MERGED' and pr['baseRefName'] == 'main'),
    'documentation_PR_required_CI': ci_ok, 'documentation_PR_current_head_review_and_approval': review_ok,
    'issue_reference_verified': issue_reference, 'issue_closed': issue['state'] == 'CLOSED',
}
audit = {'current_main': main, 'qualified_commit': QUALIFIED, 'issue_url': issue['url'],
         'issue_body_sha256': digest(issue['body'].encode()), 'documentation_PR': pr['url'] if pr else None,
         'report_sha256': report_sha, 'requirements': requirements, 'delivery': delivery,
         'unqualified_runtime_or_build_changes': unqualified_changes,
         'source_change_classification': classification,
         'completion_proven': all(requirements.values()) and all(delivery.values())}
(BASE / 'hawdb-291-post913-delivery-audit.json').write_text(json.dumps(audit, indent=2) + '\n')
print(json.dumps(audit))
