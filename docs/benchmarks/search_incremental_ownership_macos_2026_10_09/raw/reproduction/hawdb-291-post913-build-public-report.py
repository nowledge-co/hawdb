"""Build post-913 evidence only when the complete source-bound matrix passes."""
import datetime
import hashlib
import json
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-post913-qualification'
OUT = BASE / 'hawdb-291-post913-publication-bundle'


def read(name):
    return json.loads((BASE / name).read_text())


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def verify_log(row):
    assert row['exit_code'] == 0, row
    assert digest(row['log']) == row['log_sha256'], row['log']


pipeline = read('hawdb-291-post913-pipeline.json')
assert pipeline['state'] == 'passed' and pipeline['source_unchanged'], 'current-main pipeline is incomplete'
source = read('hawdb-291-post913-source.json')
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORK, text=True).strip() == source['commit']
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORK, text=True)
assert all(digest(WORK / name) == value for name, value in source['source_files_sha256'].items())
for row in pipeline['prerequisite_receipts'].values():
    assert digest(row['path']) == row['sha256']
for path, value in pipeline['original_terminal_receipts'].items():
    assert digest(path) == value
subprocess.run(['python3', str(BASE / 'hawdb-291-post913-scale-audit.py')], check=True, stdout=subprocess.PIPE)
scale = read('hawdb-291-post913-scale-audit.json')
assert scale['all_three_cells_passed'] and scale['historical_matched_before_after_verified']
assert scale['incremental_byte_scaling']['passed'], scale['incremental_byte_scaling']
release = read('hawdb-291-post913-release.json')
probe = read('hawdb-291-post913-compressed-probe.json')
native = read('hawdb-291-post913-native.json')
recovery_coverage = read('hawdb-291-post913-recovery-coverage.json')
profiles = read('hawdb-291-post913-bazel-evidence/receipt.json')
phase = read('hawdb-291-post913-bazel-phase-evidence/receipt.json')
fuzz = read('hawdb-291-post913-fuzz-evidence/receipt.json')
fuzz_initial = fuzz
resolved_path = BASE / 'hawdb-291-post913-fuzz-resolved/receipt.json'
fuzz_resolved = read('hawdb-291-post913-fuzz-resolved/receipt.json') if resolved_path.exists() else {'state': 'passed' if fuzz['state'] == 'passed' else 'failed', 'resolution_mode': 'initial-command-passed-no-retry', 'checks': [], 'final_exit_code': fuzz['exit_code'], 'source_unchanged': fuzz['source_unchanged'], 'final_log': fuzz['log'], 'final_log_sha256': fuzz['log_sha256']}
precommit = read('hawdb-291-post913-required-checks.json')
fuzz_positive = read('hawdb-291-post913-fuzz-positive-audit.json')
historical = read('hawdb-291-exact-allocation-historical-audit.json')
documentation = read('hawdb-291-post913-documentation-base.json')
assert all(r['state'] == 'passed' for r in [release, probe, native, profiles, phase, fuzz_resolved, precommit, documentation])
assert fuzz_positive['state'] == 'passed' and fuzz_positive['qualified_commit'] == source['commit']
assert fuzz_positive['raw_fuzz_receipt_sha256'] == digest(BASE / 'hawdb-291-post913-fuzz-evidence/receipt.json')
assert phase['source_unchanged'] and precommit['source_unchanged'] and fuzz['source_unchanged']
assert fuzz['state'] == 'passed' or resolved_path.exists()
assert fuzz_resolved['final_exit_code'] == 0 and fuzz_resolved['source_unchanged']
assert len(fuzz_initial['targets']) == len(fuzz_initial['artifacts']) == 96
assert release['source_unchanged'] and native['source_unchanged'] and profiles['source_unchanged']
assert probe['all_294_comparisons_passed'] and probe['unchanged_source'] and probe['unchanged_library']
assert digest(probe['source']) == probe['source_sha256']
assert digest(release['library']['path']) == release['library']['sha256']
for row in release['bench_binaries']:
    assert digest(row['path']) == row['sha256']
for row in release['checks'] + probe['checks'] + native['checks'] + pipeline['steps']:
    verify_log(row)
for row in precommit['checks']:
    verify_log(row)
for row in phase['steps']:
    assert digest(row['log']) == row['log_sha256']
    if row['label'] == 'required96-fuzz' and row['exit_code'] != 0:
        assert resolved_path.exists() and fuzz_resolved['state'] == 'passed'
        assert fuzz['state'] == 'failed' and fuzz['exit_code'] != 0
    else:
        assert row['exit_code'] == 0, row
assert historical['passed'] and all(historical['provenance_checks'].values())
assert digest(BASE / 'hawdb-291-historical-checkpoint-probe.rs') == historical['wrapper_source_sha256']
for suffix, key in [('result.json', 'raw_result_sha256'), ('stdout.log', 'raw_stdout_sha256'), ('stderr.log', 'raw_stderr_sha256')]:
    assert digest(BASE / 'hawdb-291-historical-checkpoint-evidence' / ('d327680-k10.' + suffix)) == historical[key]
assert len(profiles['targets']) == 5
for row in profiles['targets']:
    assert row['state'] == 'passed' and all(row['witnesses'].values())
    for file in row['artifacts'].values():
        assert digest(file['path']) == file['sha256']
for row in fuzz_initial['artifacts']:
    for file in row['files']:
        assert digest(file['path']) == file['sha256'] and Path(file['path']).stat().st_size == file['bytes']
for row in fuzz_resolved['checks']:
    assert row['state'] == 'passed' and row['exit_code'] == 0 and row['unchanged_deadline']
    for file in row['artifacts']:
        assert digest(file['path']) == file['sha256'] and Path(file['path']).stat().st_size == file['bytes']
# Positive XML and Rust witnesses for all96, choosing preserved retry only for its target.
retry_by_target = {r['target']: r for r in fuzz_resolved['checks']}
for original in fuzz_initial['artifacts']:
    retry = retry_by_target.get(original['target'])
    artifacts = retry['artifacts'] if retry else original['files']
    for file in artifacts:
        if file['path'].endswith('test.xml'):
            xml = ET.parse(file['path']).getroot()
            assert len(list(xml.iter('testcase'))) > 0 and not list(xml.iter('failure')) and not list(xml.iter('error'))
        elif file['path'].endswith('test.log') and original['target'] != '//:hawdb_linux_ci_fuzz_smoke_test':
            summaries = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', Path(file['path']).read_text())
            assert summaries and all(int(p) > 0 and int(f) == int(i) == 0 for p, f, i in summaries)
assert digest(fuzz_resolved['final_log']) == fuzz_resolved['final_log_sha256']
power = next(row for row in native['checks'] if row['label'] == 'power-loss')
power_text = Path(power['log']).read_text()
plans = re.findall(r'^search-power-(?:publication|initial)-v1 .*\bplans=(\d+)', power_text, re.M)
assert len(plans) == power['cut_families'] == 30 and sum(map(int, plans)) == power['fault_plans'] > 0
assert 'test result: ok. 11 passed; 0 failed; 0 ignored;' in power_text
assert recovery_coverage['state'] == 'passed' and recovery_coverage['qualified_commit'] == source['commit']
assert recovery_coverage['log_sha256'] == power['log_sha256']
assert recovery_coverage['cut_families'] == 30 and recovery_coverage['fault_plans'] == power['fault_plans']
assert len(recovery_coverage['actual_cases']) == 11
assert all(value == source['source_files_sha256'][name] for name, value in recovery_coverage['source_sha256'].items())
assert documentation['runtime_and_benchmark_semantics_unchanged']
assert documentation['main_change_classification']['compared_revision'] == documentation['documentation_base']
assert not documentation['main_change_classification']['unqualified_changes']
assert read('hawdb-291-post913-main-change-classification.json') == documentation['main_change_classification']
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=BASE / 'hawdb-291-incremental-recovery-followup', text=True).strip() == documentation['documentation_base']
assert not OUT.exists(), 'publication bundle already exists; inspect rather than overwrite'
OUT.mkdir()
files = {}


def copy_file(path, relative):
    path = Path(path)
    destination = OUT / relative
    assert relative not in files
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(path, destination)
    files[relative] = {'sha256': digest(destination), 'bytes': destination.stat().st_size}


def copy_folder(name, relative):
    folder = BASE / name
    assert folder.is_dir(), name
    for path in sorted(folder.rglob('*')):
        if path.is_file():
            copy_file(path, relative + '/' + str(path.relative_to(folder)))


receipt_names = [
    'hawdb-291-post913-source.json', 'hawdb-291-post913-release.json',
    'hawdb-291-post913-compressed-probe.json', 'hawdb-291-post913-native.json',
    'hawdb-291-post913-required-checks.json', 'hawdb-291-post913-pipeline.json',
    'hawdb-291-post913-scale-audit.json', 'hawdb-291-post913-documentation-base.json',
    'hawdb-291-post913-main-change-classification.json',
    'hawdb-291-post913-fuzz-positive-audit.json',
    'hawdb-291-post913-recovery-coverage.json',
    'hawdb-291-post913-issue.json', 'hawdb-291-post913-remote-ref.json',
    'hawdb-979-observe-precommit-binding.json', 'hawdb-979-observe-main-binding.json',
    'hawdb-979-observe-committed-source.json', 'hawdb-979-observe-final-source.json',
    'hawdb-979-observe-regressions.json', 'hawdb-291-exact-allocation-historical-audit.json',
]
for name in receipt_names:
    copy_file(BASE / name, 'provenance/' + name)
copy_file(BASE / 'hawdb-291-post913-scale-evidence/manifest.json', 'provenance/scale-manifest.json')
for row in release['checks'] + probe['checks'] + native['checks'] + pipeline['steps']:
    copy_file(row['log'], 'raw/commands/' + Path(row['log']).name)
for row in precommit['checks']:
    copy_file(row['log'], 'raw/commands/' + Path(row['log']).name)
for label in ['d81920-k10', 'd327680-k10', 'd327680-k100']:
    for suffix in ['result.json', 'stdout.log', 'stderr.log']:
        name = label + '.' + suffix
        copy_file(BASE / 'hawdb-291-post913-scale-evidence' / name, name)
copy_folder('hawdb-291-post913-bazel-evidence', 'raw/current-search-profiles')
copy_folder('hawdb-979-observe-evidence', 'raw/979-historical-qualification')
copy_folder('hawdb-291-post913-bazel-phase-evidence', 'raw/current-bazel-phase')
copy_folder('hawdb-291-post913-fuzz-evidence', 'raw/current-fuzz')
if resolved_path.exists():
    copy_folder('hawdb-291-post913-fuzz-resolved', 'raw/current-fuzz-retries')
copy_folder('hawdb-291-current-main-bazel-evidence', 'raw/earlier7cb-profiles')
for name in ['source.json', 'native.json', 'release.json', 'compressed-probe.json', 'pipeline.json']:
    copy_file(BASE / ('hawdb-291-current-main-' + name), 'provenance/earlier7cb-' + name)
for name in ['power-loss.log', 'maintenance-facade.log', 'rebuild-facade.log', 'compressed-probe-run.log']:
    copy_file(BASE / ('hawdb-291-current-main-' + name), 'raw/earlier7cb-commands/' + name)
copy_folder('hawdb-291-historical-checkpoint-evidence', 'raw/historical-checkpoint')
copy_folder('hawdb-291-incremental-repair-scale-evidence', 'raw/original594-scale')
copy_folder('hawdb-291-incremental-repair-bazel-terminal-evidence', 'raw/original594-profiles-and-fuzz')
copy_folder('hawdb-291-incremental-repair-fuzz-resolved-evidence', 'raw/original594-fuzz-retries')
for folder in [
    'hawdb-291-scale-evidence', 'hawdb-291-scale-evidence-final',
    'hawdb-291-scale-evidence-cleanup', 'hawdb-291-scale-evidence-streamed',
    'hawdb-291-scale-evidence-exact-allocation', 'hawdb-291-initial-main-scale-evidence',
]:
    copy_folder(folder, 'raw/earlier-scale-runs/' + folder)
original_manifest = read('hawdb-291-incremental-repair-scale-evidence/manifest.json')
for name in original_manifest['earlier_failures']:
    copy_file(name, 'raw/earlier-failures/' + str(Path(name).relative_to(BASE)))
copy_file(BASE / 'hawdb-291-post-merge-verification.json', 'provenance/original594-pipeline.json')
for name in [
    'hawdb-291-post913-install-docs.py', 'hawdb-291-post913-audit-delivery.py',
    'hawdb-291-post913-completion-audit.py', 'hawdb-291-post913-recovery-coverage.py',
    'hawdb-291-post913-required-checks.py', 'hawdb-291-post913-bazel-phase.py',
    'hawdb-291-post913-fuzz.py', 'hawdb-291-post913-resolve-fuzz.py',
    'hawdb-291-post913-scale-driver.py', 'hawdb-291-post913-scale-audit.py',
    'hawdb-291-post913-byte-scaling.py',
    'hawdb-291-post913-pipeline.py', 'hawdb-291-post913-native.py',
    'hawdb-291-post913-bazel.py', 'hawdb-291-post913-release.py',
    'hawdb-291-post913-compressed-probe.py', 'hawdb-291-post913-compressed-compaction-probe.rs',
    'hawdb-291-post913-documentation-base.py', 'hawdb-291-post913-classify-main.py',
    'hawdb-291-post913-build-public-report.py',
    'hawdb-291-historical-checkpoint-probe.rs', 'hawdb-291-historical-checkpoint-driver.py',
    'hawdb-291-audit-exact-allocation-historical.py', 'hawdb-291-historical-probe-compile.json',
    'hawdb-291-historical-release-build.log',
]:
    copy_file(BASE / name, 'raw/reproduction/' + name)
readme = '''# Incremental search ownership qualification

The qualified source is `{commit}` (tree `{tree}`). Start from that revision
with the repository's pinned Rust 1.97.1, locked dependencies and default
features. The measured host is recorded in `provenance/scale-manifest.json`.
The report keeps fresh post-913 evidence separate from older measurements.

## Reproduce the matrix

Use an isolated checkout of the qualified revision. Build the same release
library and benchmark:

```console
cargo build --locked --release --lib
cargo bench --locked --bench search_mutation --no-run
```

Run the following from that checkout. This developer process explicitly sets
its own OS soft FD limit to 4,096; the embedded library does not change host
limits. The hard limit must already permit that setting. The three cells run
sequentially, with fresh fixtures, 32 seed segments and every 128 write/merge
rounds. All benchmark environment values match the measured manifest.

```python
import os
import resource
import subprocess
import tempfile

soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
assert hard == resource.RLIM_INFINITY or hard >= 4096
resource.setrlimit(resource.RLIMIT_NOFILE, (4096, hard))
fixed = {{
    "HAWDB_SEARCH_MUTATION_BENCH_CONTENT_BYTES": "65536",
    "HAWDB_SEARCH_MUTATION_BENCH_ROUNDS": "128",
    "HAWDB_SEARCH_MUTATION_BENCH_MEMORY_BYTES": "268435456",
    "HAWDB_SEARCH_MUTATION_BENCH_SEGMENT_BYTES": "67108864",
    "HAWDB_SEARCH_MUTATION_BENCH_LEXICAL_BUILD_MEMORY_BYTES": "8388608",
    "HAWDB_SEARCH_MUTATION_BENCH_OPEN_FILES": "1024",
    "HAWDB_SEARCH_MUTATION_BENCH_VECTOR_DIMENSIONS": "384",
    "HAWDB_SEARCH_MUTATION_BENCH_COMPACTION_EVERY": "1",
    "HAWDB_SEARCH_MUTATION_BENCH_REUSE_VALIDATION": "1",
}}
with tempfile.TemporaryDirectory(prefix="hawdb-291-reproduce-") as scratch:
    for documents, touches in [(81920, 10), (327680, 10), (327680, 100)]:
        env = dict(os.environ, **fixed)
        env["TMPDIR"] = scratch
        env["HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS"] = str(documents)
        env["HAWDB_SEARCH_MUTATION_BENCH_TOUCHES"] = str(touches)
        subprocess.run(
            ["cargo", "bench", "--locked", "--bench", "search_mutation"],
            env=env, check=True,
        )
```

Each cell emits one `search_mutation` JSON result. Inspect all peak and steady
RSS samples, the 128 actual compactions, changed and deleted document counts,
checkpoint bytes and merge bytes. The budget is 268,435,456 bytes. Artifact
write amplification sums checkpoint plus merge artifacts and divides by all
sustained changed body and embedding bytes. It excludes graph/WAL and device
writes. `raw/reproduction/hawdb-291-post913-scale-audit.py` records the complete
checks, while each `.result.json` preserves the actual measurement.

## Query, recovery and local fuzz commands

The complete feature profiles and recovery suite use:

```console
bazel test //crates/search:hawdb_search_tests //crates/search:hawdb_search_acl_tests //crates/search:hawdb_search_text_background_tests //crates/search:hawdb_search_text_only_tests //crates/search:hawdb_search_text_vector_tests --local_test_jobs=1 --test_output=errors
cargo test --locked -p hawdb --all-features --lib api::tests::power_loss::search_projection:: -- --test-threads=1 --nocapture
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test --local_test_jobs=1 --test_output=errors
```

The 294-comparison probe source is
`raw/reproduction/hawdb-291-post913-compressed-compaction-probe.rs`. Compile it
with the release library, then pass an unused synthetic fixture directory.
The original `compressed-probe.py` records all linker paths from this host;
the following selects paths from the new checkout instead:

```python
from pathlib import Path
import subprocess
import tempfile

release = Path("target/release").resolve()
probe = Path("/absolute/path/to/copied/evidence/raw/reproduction/hawdb-291-post913-compressed-compaction-probe.rs")
with tempfile.TemporaryDirectory(prefix="hawdb-291-reference-") as scratch:
    binary = Path(scratch) / "compressed-reference"
    command = [
        "rustc", "--edition=2021", str(probe), "-o", str(binary),
        "-C", "opt-level=3", "-C", "codegen-units=1", "-C", "lto=thin",
        "-L", "dependency=" + str(release / "deps"),
        "--extern", "hawdb=" + str(release / "libhawdb.rlib"),
    ]
    for native in sorted((release / "build").glob("*/out")):
        command += ["-L", "native=" + str(native)]
    subprocess.run(command, check=True)
    subprocess.run([str(binary), str(Path(scratch) / "fixture")], check=True)
```

Use the actual `CARGO_TARGET_DIR` when it differs from `target`. Expected
probe output is 294 comparisons, distinct/changed vectors, three real merges,
two initial owners, required RaBitQ artifacts and a retained old reader.

The full command and raw test logs distinguish actual executions from cached
passes. Any preserved timeout is resolved only by a recorded isolated retry
at its original deadline; no seed, case or resource cap changes. Formatting,
strict native and target Clippy commands are in the required-check receipt.

## Evidence interpretation

`report.json` binds every included raw file by SHA-256 and byte count. The
historical 20 GiB/K10 baseline includes all 32 seed rows at initialization.
Formats, dependencies and admission APIs differ, so compare emitted search
artifact bytes rather than claiming controlled throughput or RSS improvement.
The body corpus is compressible synthetic text. Host/cache state is
uncontrolled, and no debugger or memory-inspection process was attached.

Finite query/fault witnesses assume completed POSIX synchronization and atomic
same-directory rename. They do not prove universal ANN recall or every hardware
power-loss behavior. This report does not authorize stable Mem activation.
'''.format(commit=source['commit'], tree=source['integrated_tree'])
readme_path = OUT / 'README.md'
readme_path.write_text(readme)
files['README.md'] = {'sha256': digest(readme_path), 'bytes': readme_path.stat().st_size}
report = {
    'schema': 'hawdb.search.incremental-ownership.qualification.v2',
    'issue': 'https://github.com/nowledge-co/hawdb/issues/291',
    'prepared_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'qualified_commit': source['commit'], 'qualified_tree': source['integrated_tree'],
    'qualified_source_file_count': len(source['source_files_sha256']), 'includes_PR979': True, 'includes_PR913': True,
    'documentation_base_binding': documentation, 'scale': scale,
    'historical_baseline_receipt': historical,
    'finite_reference_query_comparisons': 294,
    'native_recovery': {'actual_cases': 11, 'cut_families': power['cut_families'], 'plans': power['fault_plans'],
                        'actual_test_names': recovery_coverage['actual_cases'],
                        'actual_family_lines': recovery_coverage['actual_family_lines'],
                        'coverage_evidence': 'provenance/hawdb-291-post913-recovery-coverage.json'},
    'current_search_profiles': profiles,
    'local_fuzz': {'source_receipt': 'provenance/hawdb-291-post913-source.json', 'initial': fuzz, 'resolution': fuzz_resolved, 'phase': phase},
    'original594_scale_archive': {'commit': original_manifest['commit'], 'cells': original_manifest['cells'],
        'scope': 'Original pre979 measurements retained; no cell substitutes for this report current-main matrix.'},
    'raw_files': files,
    'publication_status': 'Prepared evidence; final docs checks, PR required CI, independent head review, actual main artifacts and issue closure remain separate gates.',
    'boundaries': [
        'All three unchanged shapes, every128 rounds/actual merges,256MiB full writer budget and unchanged segment/lexical/FD caps/seeds.',
        'Host/cache state is uncontrolled; artifact bytes are not device-level writes and timing is not controlled throughput evidence.',
        'Historical8cbe16f8 includes32 seed rows in initialization; formats/dependencies/admission APIs differ. Compare artifact bytes only.',
        'Required96-target command executes at this fixed source; actual fresh/cached counts come from its raw log. Any unchanged-deadline isolated retries preserve all original failures; no older-source run substitutes.',
        'Original594 scale, earlier7cb query/recovery and all preceding resource/campaign failures remain separate archives. No7cb scale began; shared IO changes moved the planned current matrix to41e before start. No budget/deadline/seed change makes failures passing.',
        'Finite query/fault witnesses assume completed POSIX synchronization and atomic same-directory rename; no universal ANN or hardware power-loss proof.',
        'Library FD policy only observes host limits. Benchmark host explicitly selects soft4096/project1024; constrained-host/low-budget behavior has separate end-to-end regressions.',
        'Large indivisible documents retain hard publication/segment caps and require enough compaction input admission.',
        'No stable Mem release or production activation is authorized by this qualification.',
    ],
}
(OUT / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps({'bundle': str(OUT), 'report_sha256': digest(OUT / 'report.json'), 'raw_file_count': len(files)}))
