import hashlib
import json
import os
import resource
from pathlib import Path
import subprocess
import time

ROOT = Path('/private/tmp/hawdb-291-post913-scale-evidence')
ROOT.mkdir(exist_ok=False)
SCRATCH = Path('/private/tmp/hawdb-291-post913-scale-scratch')
SCRATCH.mkdir(exist_ok=False)
WORKTREE = Path('/private/tmp/hawdb-291-post913-qualification')
COMMIT = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORKTREE, text=True).strip()
STATUS = subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORKTREE, text=True)
if STATUS:
    raise SystemExit('Scale qualification requires a clean committed source tree')
RELEASE = json.loads(Path('/private/tmp/hawdb-291-post913-release.json').read_text())
assert RELEASE['state'] == 'passed' and RELEASE['source_unchanged']
NATIVE = json.loads(Path('/private/tmp/hawdb-291-post913-native.json').read_text())
assert NATIVE['state'] == 'passed' and NATIVE['source_unchanged']
SOURCE = json.loads(Path('/private/tmp/hawdb-291-post913-source.json').read_text())
SOURCE_NAMES = sorted(SOURCE['source_files_sha256'])
assert COMMIT == SOURCE['commit'] == '41e10dacf8b2589e416c01b9b047db98dfe01acd'
for gate_path in ['/private/tmp/hawdb-291-post913-compressed-probe.json', '/private/tmp/hawdb-291-post913-bazel-evidence/receipt.json', '/private/tmp/hawdb-291-post913-bazel-phase-evidence/receipt.json']:
    gate = json.loads(Path(gate_path).read_text())
    assert gate['state'] == 'passed', gate_path
COMMAND = ['cargo', 'bench', '--locked', '--bench', 'search_mutation']
soft_fd_limit, hard_fd_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (4096, hard_fd_limit))

def scratch_size():
    logical = allocated = 0
    for directory, _, files in os.walk(SCRATCH):
        for name in files:
            try:
                stat = (Path(directory) / name).stat()
            except FileNotFoundError:
                continue
            logical += stat.st_size
            allocated += stat.st_blocks * 512
    return logical, allocated

def save(path, value):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(value, indent=2) + '\n')
    temp.replace(path)

manifest = {
    'commit': COMMIT,
    'state': 'running',
    'qualification_generation': 'current-main-after-PR913',
    'integrated_source_tree': SOURCE['integrated_tree'],
    'source_receipt': '/private/tmp/hawdb-291-post913-source.json',
    'source_receipt_sha256': hashlib.sha256(Path('/private/tmp/hawdb-291-post913-source.json').read_bytes()).hexdigest(),
    'release_binary_receipt': RELEASE,
    'initial_ownership_source_defaults': {'max_content_documents': 8192, 'max_content_artifact_bytes': 67108864, 'note': 'Unmodified benchmark options inherit a hard owner document cap and byte split target with an indivisible one-document exception. Independent six-case incremental regressions and the release294-reference probe supply boundary/query witnesses.'},
    'patch_sha256_before': hashlib.sha256(subprocess.check_output(['git', 'diff', '--binary'], cwd=WORKTREE)).hexdigest(),
    'os_file_descriptor_limits': resource.getrlimit(resource.RLIMIT_NOFILE),
    'source_files_sha256': {name: hashlib.sha256((WORKTREE / name).read_bytes()).hexdigest() for name in SOURCE_NAMES},
    'git_tree': subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'], cwd=WORKTREE, text=True).strip(),
    'rustc_verbose': subprocess.check_output(['rustc', '-Vv'], cwd=WORKTREE, text=True),
    'process_inspection': 'No vmmap, sample or debugger attachment. Internal process counters plus passive ps/scratch sampling only. Host and cache state are uncontrolled; one run per cell. Cells may overlap separately recorded final-source regression checks and unrelated host tasks; per-process memory is measured independently. Timings are supporting evidence, not controlled throughput comparison.',
    'earlier_failures': ['/private/tmp/hawdb-291-cleanup-scale-audit.json','/private/tmp/hawdb-291-bound-artifact-cold-seed.json','/private/tmp/hawdb-291-scale-evidence/manifest.json','/private/tmp/hawdb-291-scale-evidence-final/manifest.json','/private/tmp/hawdb-291-streamed-scale-audit.json','/private/tmp/hawdb-291-obsolete-k100-cancellation.json'],
    'earlier_failure_disposition': 'Keep all failures. The c9252815 runtime completed 5GiB K10 within 256MiB and20GiB K10 with failed RSS; vmmap/sample diagnosis affected that latter run. Same-source K100 was intentionally interrupted. An independent uninspected same-snapshot one-update pair proved writer cleanup duplicates historical metadata:283525120 versus152453120 peak bytes with identical47302 published bytes. This candidate admits writer-local reference copies and shares previously validated immutable metadata/handles; The uninspected29f54b9b full20GiB K10 run also completed128 rounds/merges butfailed RSS at736591872 bytes. Independent allocation regressions exposed bound-buffer andlexical-directory geometric reallocations; the corrected cold32seed/K10 diagnostic reached157319168 butexcludes freshconstruction. This exact combined-main candidate fixes both allocation paths; full sustained qualification still requires allthree unchangedcells. Selected budgets andfixture remainidentical.',
    'parent_main': subprocess.check_output(['git', 'rev-parse', 'HEAD^'], cwd=WORKTREE, text=True).strip(),
    'merged_main_commit': COMMIT,
    'main_source_binding_receipt': '/private/tmp/hawdb-291-post913-source.json',
    'fresh_required_checks_receipt': '/private/tmp/hawdb-291-post913-bazel-phase-evidence/receipt.json',
    'original_frozen594_pipeline': '/private/tmp/hawdb-291-post-merge-verification.json',
    'original_frozen594_scale_manifest': '/private/tmp/hawdb-291-incremental-repair-scale-evidence/manifest.json',
    'original_frozen594_scope': 'Separate completed or failed archive. Its measurements are not substituted for any current-main cell.',
    'original_earlier_runtime_scale': '/private/tmp/hawdb-291-exact-allocation-scale-audit.json',
    'scope_boundary': 'All three fresh cells qualify main41e10dac including PR979 FD/LRU/crisis policy and PR913 shared immutable identity validation. Fresh native/full-profile/local-fuzz gates precede the matrix. Original594 and historical13aa measurements do not substitute. Fixture, all128 rounds/merges, budgets, seeds and ordinal/column vector construction are unchanged. The driver alone explicitly sets its own process soft FD limit to4096, as in the original benchmark; the embedded library never changes host limits.',
    'working_tree_status_before': subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORKTREE, text=True),
    'platform': subprocess.check_output(['sw_vers'], text=True),
    'host_ram_bytes': int(subprocess.check_output(['sysctl', '-n', 'hw.memsize'], text=True)),
    'host_model': subprocess.check_output(['sysctl', '-n', 'hw.model'], text=True).strip(),
    'scratch': str(SCRATCH),
    'filesystem': 'APFS; internal Apple Fabric SSD; 4096-byte allocation blocks',
    'writer_budget_bytes': 268435456,
    'fixture': 'Repeated bounded-search-mutation text, 65536 bytes per body and ordinal/column-hashed 384-dimensional dense embeddings; streaming full build and 32 seed segments, then K changed-text/vector replacements, K deletes, and128 replacement/insert/compaction rounds. Logical body size is corpus-shaped; emitted artifact bytes are compressed files, not device-level writes.',
    'cells': [],
}
assert manifest['git_tree'] == manifest['integrated_source_tree']
assert manifest['source_files_sha256'] == SOURCE['source_files_sha256']
assert len(RELEASE['bench_binaries']) == 1
BENCH_BINARY = Path(RELEASE['bench_binaries'][0]['path'])
assert hashlib.sha256(BENCH_BINARY.read_bytes()).hexdigest() == RELEASE['bench_binaries'][0]['sha256']
save(ROOT / 'manifest.json', manifest)
for documents, touches in [(81920, 10), (327680, 10), (327680, 100)]:
    label = f'd{documents}-k{touches}'
    env_values = {
        'TMPDIR': str(SCRATCH),
        'HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS': str(documents),
        'HAWDB_SEARCH_MUTATION_BENCH_CONTENT_BYTES': '65536',
        'HAWDB_SEARCH_MUTATION_BENCH_TOUCHES': str(touches),
        'HAWDB_SEARCH_MUTATION_BENCH_ROUNDS': '128',
        'HAWDB_SEARCH_MUTATION_BENCH_MEMORY_BYTES': '268435456',
        'HAWDB_SEARCH_MUTATION_BENCH_SEGMENT_BYTES': '67108864',
        'HAWDB_SEARCH_MUTATION_BENCH_LEXICAL_BUILD_MEMORY_BYTES': '8388608',
        'HAWDB_SEARCH_MUTATION_BENCH_OPEN_FILES': '1024',
        'HAWDB_SEARCH_MUTATION_BENCH_VECTOR_DIMENSIONS': '384',
        'HAWDB_SEARCH_MUTATION_BENCH_COMPACTION_EVERY': '1',
        'HAWDB_SEARCH_MUTATION_BENCH_REUSE_VALIDATION': '1',
    }
    env = os.environ.copy()
    env.update(env_values)
    env['CARGO_TARGET_DIR'] = RELEASE['target_directory']
    env['RUSTUP_TOOLCHAIN'] = '1.97.1'
    out_path = ROOT / (label + '.stdout.log')
    err_path = ROOT / (label + '.stderr.log')
    row = {'label': label, 'command': COMMAND, 'environment': env_values, 'started_unix_seconds': time.time(), 'state': 'running'}
    manifest['cells'].append(row)
    save(ROOT / 'manifest.json', manifest)
    high_logical = high_allocated = 0
    with out_path.open('wb') as stdout, err_path.open('wb') as stderr:
        process = subprocess.Popen(COMMAND, cwd=WORKTREE, env=env, stdout=stdout, stderr=stderr)
        row['cargo_pid'] = process.pid
        while process.poll() is None:
            logical, allocated = scratch_size()
            high_logical = max(high_logical, logical)
            high_allocated = max(high_allocated, allocated)
            row.update(elapsed_seconds=time.time() - row['started_unix_seconds'], scratch_logical_highwater_bytes=high_logical, scratch_allocated_highwater_bytes=high_allocated)
            save(ROOT / 'manifest.json', manifest)
            time.sleep(10)
        row['exit_code'] = process.wait()
    row.update(state='finished', elapsed_seconds=time.time() - row['started_unix_seconds'], scratch_logical_highwater_bytes=high_logical, scratch_allocated_highwater_bytes=high_allocated)
    row['stdout_sha256'] = hashlib.sha256(out_path.read_bytes()).hexdigest()
    row['stderr_sha256'] = hashlib.sha256(err_path.read_bytes()).hexdigest()
    data_lines = [line.removeprefix('search_mutation ') for line in out_path.read_text().splitlines() if line.startswith('search_mutation ')]
    if len(data_lines) == 1:
        result = json.loads(data_lines[0])
        save(ROOT / (label + '.result.json'), result)
        rounds = result['sustained_rounds']
        rss = [result['lifetime_peak_resident_bytes']] + [item['lifetime_peak_resident_bytes'] for item in rounds]
        row['qualification'] = {
            'round_count': len(rounds),
            'compactions': sum(item['compaction_published'] for item in rounds),
            'full_generation_bytes': result['full_generation_bytes'],
            'full_vector_document_count': result['full_vector_document_count'],
            'full_vector_payload_bytes': result['full_vector_payload_bytes'],
            'full_rabitq_artifact_bytes': result['full_rabitq_artifact_bytes'],
            'sustained_artifact_write_amplification': sum(item['checkpoint_bytes'] + item['compaction_artifact_bytes'] for item in rounds) / sum(item['upserted_documents'] * (65536 + 384 * 4) for item in rounds),
            'mutation_checkpoint_bytes': result['mutation_checkpoint_bytes'],
            'upsert_checkpoint_bytes': result['upsert_checkpoint_bytes'],
            'upsert_vector_payload_bytes': result['upsert_vector_payload_bytes'],
            'upsert_rabitq_artifact_bytes': result['upsert_rabitq_artifact_bytes'],
            'upsert_peak_resident_bytes': result['upsert_lifetime_peak_resident_bytes'],
            'upsert_rss_within_writer_budget': result['upsert_lifetime_peak_resident_bytes'] is not None and result['upsert_lifetime_peak_resident_bytes'] <=268435456,
            'peak_resident_bytes': max(value for value in rss if value is not None),
            'all_rss_samples_available': all(value is not None for value in rss),
            'rss_within_writer_budget': all(value is not None and value <= 268435456 for value in rss),
            'total_sustained_checkpoint_bytes': sum(item['checkpoint_bytes'] for item in rounds),
            'total_compaction_artifact_bytes': sum(item['compaction_artifact_bytes'] for item in rounds),
            'total_compaction_source_bytes': sum(item['compaction_source_bytes'] for item in rounds),
        }
    save(ROOT / 'manifest.json', manifest)
    print(json.dumps(row), flush=True)
manifest['bench_binary_sha256_after'] = hashlib.sha256(BENCH_BINARY.read_bytes()).hexdigest()
assert manifest['bench_binary_sha256_after'] == RELEASE['bench_binaries'][0]['sha256']
manifest['patch_sha256_after'] = hashlib.sha256(subprocess.check_output(['git', 'diff', '--binary'], cwd=WORKTREE)).hexdigest()
manifest['source_files_sha256_after'] = {name: hashlib.sha256((WORKTREE / name).read_bytes()).hexdigest() for name in SOURCE_NAMES}
manifest['commit_after'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORKTREE, text=True).strip()
manifest['working_tree_status_after'] = subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORKTREE, text=True)
save(ROOT / 'manifest.json', manifest)

if manifest['source_files_sha256_after'] != manifest['source_files_sha256'] or manifest['commit_after'] != COMMIT or manifest['working_tree_status_after']:
    raise SystemExit('Source changed during scale qualification')
valid = len(manifest['cells']) == 3 and all(
    row['exit_code'] == 0
    and row.get('qualification', {}).get('rss_within_writer_budget')
    and row['qualification'].get('upsert_rss_within_writer_budget')
    and row['qualification'].get('all_rss_samples_available')
    and row['qualification'].get('round_count') == 128
    and row['qualification'].get('compactions') == 128
    for row in manifest['cells']
)
manifest['state'] = 'passed' if valid else 'failed'
save(ROOT / 'manifest.json', manifest)
raise SystemExit(0 if valid else 1)
