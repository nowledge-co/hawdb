"""Preserve all five issue291 criteria and distinguish local from actual delivery."""
import hashlib
import json
import re
import subprocess
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-post913-qualification'


def read(name):
    path = BASE / name
    return json.loads(path.read_text()) if path.is_file() else {}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


source = read('hawdb-291-post913-source.json')
frozen = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORK, text=True).strip() == source['commit']
frozen = frozen and not subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORK, text=True)
frozen = frozen and all(sha(WORK / name) == value for name, value in source['source_files_sha256'].items())
release = read('hawdb-291-post913-release.json')
probe = read('hawdb-291-post913-compressed-probe.json')
native = read('hawdb-291-post913-native.json')
profiles = read('hawdb-291-post913-bazel-evidence/receipt.json')
phase = read('hawdb-291-post913-bazel-phase-evidence/receipt.json')
checks = read('hawdb-291-post913-required-checks.json')
if (BASE / 'hawdb-291-post913-scale-evidence/manifest.json').exists():
    subprocess.run(['python3', str(BASE / 'hawdb-291-post913-scale-audit.py')], check=True, stdout=subprocess.PIPE)
scale = read('hawdb-291-post913-scale-audit.json')
release_ok = release.get('state') == 'passed' and release.get('source_unchanged')
release_ok = release_ok and sha(release['library']['path']) == release['library']['sha256']
probe_ok = probe.get('state') == 'passed' and probe.get('all_294_comparisons_passed') and probe.get('unchanged_source') and probe.get('unchanged_library')
if probe_ok:
    probe_ok = sha(probe['source']) == probe['source_sha256']
    run = next(row for row in probe['checks'] if row['label'] == 'run')
    probe_ok = probe_ok and sha(run['log']) == run['log_sha256']
    probe_ok = probe_ok and 'compressed_compaction passed comparisons=294 distinct_vectors=true changed_embedding=true merges=3 initial_owners=2 pinned_reader=true rabitq_required=true' in Path(run['log']).read_text()
profiles_ok = profiles.get('state') == 'passed' and profiles.get('source_unchanged') and len(profiles.get('targets', [])) == 5
policy_ok = False
if profiles_ok:
    row = next(row for row in profiles['targets'] if row['target'] == '//crates/search:hawdb_search_tests')
    path = row['artifacts']['test.log']['path']
    raw = Path(path).read_text()
    names = [
        'tier_configuration_bounds_normal_merges_without_starting_background_work',
        'crisis_merge_uses_a_bounded_pair_and_does_not_exceed_the_top_level',
        'crisis_merge_reduces_mixed_levels_and_preserves_mutations_and_pins',
        'scheduled_mixed_level_crisis_defers_before_staging_and_releases_its_permit',
        'scheduled_compaction_tracks_and_releases_the_qos_budget',
        'scheduled_compaction_observes_cancellation_before_qos_admission',
        'scheduled_compaction_requires_background_maintenance_capability',
        'cancelled_compaction_preserves_the_active_manifest',
    ]
    policy_ok = sha(path) == row['artifacts']['test.log']['sha256'] and all(re.search(r'::' + name + r' \.\.\. ok$', raw, re.M) for name in names)
recovery_ok = False
if native.get('state') == 'passed' and native.get('source_unchanged'):
    row = next(row for row in native['checks'] if row['label'] == 'power-loss')
    raw = Path(row['log']).read_text()
    plans = re.findall(r'^search-power-(?:publication|initial)-v1 .*\bplans=(\d+)', raw, re.M)
    recovery_ok = row['exit_code'] == 0 and sha(row['log']) == row['log_sha256']
    recovery_ok = recovery_ok and len(plans) == 30 and sum(map(int, plans)) == row['fault_plans'] > 0 and 'test result: ok. 11 passed; 0 failed; 0 ignored;' in raw
requirements = {
    'AC1_K_proportional_artifact_bytes_at_tens_GB_before_after': bool(frozen and scale.get('all_three_cells_passed') and scale.get('historical_matched_before_after_verified') and scale.get('incremental_byte_scaling', {}).get('passed')),
    'AC2_multisegment_results_and_scores_match_merged_reference': bool(frozen and release_ok and probe_ok),
    'AC3_configurable_levels_fan_in_crisis_admission_and_cancellation': bool(frozen and profiles_ok and policy_ok),
    'AC4_atomic_complete_publication_and_merge_recovery': bool(frozen and recovery_ok),
    'AC5_complete_sustained_RSS_and_amplification_report': bool(frozen and scale.get('all_three_cells_passed')),
}
checks_ok = checks.get('state') == 'passed' and checks.get('source_unchanged') and len(checks.get('checks', [])) == 6
checks_ok = checks_ok and all(row['exit_code'] == 0 and sha(row['log']) == row['log_sha256'] for row in checks.get('checks', []))
delivery = read('hawdb-291-post913-delivery-audit.json')
report = {
    'original_scope_preserved': True, 'qualified_commit': source['commit'], 'requirements': requirements,
    'local_required_checks': bool(frozen and checks_ok), 'all_five_search_profiles': bool(frozen and profiles_ok),
    'fresh_local_fuzz_and_shared_IO_phase': bool(frozen and phase.get('state') == 'passed' and phase.get('source_unchanged')),
    'actual_main_delivery': delivery,
    'completion_proven': all(requirements.values()) and checks_ok and profiles_ok and phase.get('state') == 'passed' and delivery.get('completion_proven', False),
    'publication_rule': 'Before closure rerun actual-main delivery audit after fetching main; local passes cannot replace public report, source classification, independent head review, required CI and actual issue closure.',
}
(BASE / 'hawdb-291-post913-completion-audit.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report))
