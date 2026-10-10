"""Install complete current-source evidence on the direct-main documentation branch."""
import hashlib
import json
import shutil
import subprocess
from pathlib import Path

BASE = Path('/private/tmp')
WORK = BASE / 'hawdb-291-incremental-recovery-followup'
BUNDLE = BASE / 'hawdb-291-post913-publication-bundle'
REPORT = json.loads((BUNDLE / 'report.json').read_text())
assert REPORT['qualified_commit'] == '41e10dacf8b2589e416c01b9b047db98dfe01acd'
assert REPORT['includes_PR979'] and REPORT['includes_PR913']
assert REPORT['scale']['all_three_cells_passed'] and REPORT['scale']['historical_matched_before_after_verified']
assert REPORT['scale']['incremental_byte_scaling']['passed']
binding = REPORT['documentation_base_binding']
assert binding['state'] == 'passed' and binding['runtime_and_benchmark_semantics_unchanged']
assert not binding['main_change_classification']['unqualified_changes']
assert subprocess.check_output(['git', 'branch', '--show-current'], cwd=WORK, text=True).strip() == 'docs/291-incremental-qualification'
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=WORK, text=True).strip() == binding['documentation_base']
assert not subprocess.check_output(['git', 'status', '--porcelain'], cwd=WORK, text=True)
for name, row in REPORT['raw_files'].items():
    path = BUNDLE / name
    assert hashlib.sha256(path.read_bytes()).hexdigest() == row['sha256'] and path.stat().st_size == row['bytes']
relative = 'benchmarks/search_incremental_ownership_macos_2026_10_09'
target = WORK / 'docs' / relative
assert not target.exists()
doc = WORK / 'docs/SEARCH_MUTATION_SEGMENTS.md'
existing = doc.read_text()
assert existing.count('## Source-bound resource qualification\n') == 1
assert existing.count('## Non-goals\n') == 1
table = []
for cell in REPORT['scale']['cells']:
    table.append('| {corpus} GiB | {k} | {upsert:,} | {delete:,} | {peak:.6f} | {steady:.6f} | {merges} | {wa:.4f} |'.format(
        corpus=cell['logical_corpus_bytes'] // (1024 ** 3), k=cell['touches'],
        upsert=cell['upsert_artifact_bytes'], delete=cell['delete_artifact_bytes'],
        peak=cell['peak_resident_bytes'] / (1024 ** 2), steady=cell['max_steady_resident_bytes'] / (1024 ** 2),
        merges=cell['merges'], wa=cell['sustained_artifact_write_amplification']))
profile_counts = []
for row in REPORT['current_search_profiles']['targets']:
    assert row['state'] == 'passed'
    name = row['target'].split(':')[1]
    summary = row['test_summaries'][-1]
    profile_counts.append('`{}`: {} passed, {} failed, {} dedicated campaigns ignored'.format(name, *summary[1:]))
before = REPORT['scale']['before_after']
recovery = REPORT['native_recovery']
fuzz = REPORT['local_fuzz']
initial = fuzz['initial']
actual = initial['execution_summary']
assert len(actual) == 1 and int(actual[0][1]) == 96
executed = int(actual[0][0])
cached = 96 - executed
if initial['state'] == 'passed':
    fuzz_summary = 'The initial exact command passes: {} of 96 targets execute and {} use passing cached results. No isolated retry is required.'.format(executed, cached)
else:
    resolved = fuzz['resolution']
    assert resolved['state'] == 'passed' and resolved['final_exit_code'] == 0
    fuzz_summary = 'The initial command executes {} of 96 targets and uses {} cached results. Its {} timeout targets then pass actual isolated retries under their original seeds, cases and deadlines; the final exact command passes. The report retains its actual fresh/cached execution summary and every original failure.'.format(executed, cached, len(resolved['timeout_targets']))
section = '''## Initial and incremental ownership qualification

The [current report and complete raw evidence]({path}/report.json) qualify
main `{commit}` (tree `{tree}`), including #924's partitioned initial and
incremental ownership, #977's recovery-reader lifetime correction, and #979's
observed FD ceiling, idle LRU and bounded mixed-level crisis policy. #913's
shared immutable validation/admission changes are included in this fresh run.
The documentation branch starts directly from main `{base}`. The local
search run uses the pinned Bazel 9.3.0. Its source binding classifies any
later changes; required CI for the final documentation PR is recorded
separately from earlier implementation CI.

Later #982 advances package/module and C/Python/SQL version metadata to 0.6.0.
Its exact 43-file delta preserves search/storage/benchmark source, features,
external dependency blocks and dependency edges. These measurements remain
bound to `{commit}` and its recorded binaries; they are not new measurements
of rebuilt 0.6.0 binaries. The source binding retains both commit identities
and every classified before/after hash.

The later #985 change adds only Bazel configuration logging. #983 changes
parameter conversion in the separate downstream Python binding workspace;
its exact six-file delta does not change the Rust library/search benchmark
dependency graph. These changes are classified separately and do not turn
the source-bound measurements into a fresh run of those binding changes.

All three fresh macOS ARM64 cells retain pinned Rust 1.97.1, repeated synthetic
65,536-byte bodies, ordinal/column-hashed 384-dimensional dense vectors,
32 cold seed segments, the original 256 MiB complete writer budget, 8 MiB
lexical build memory, 64 MiB segment limit, and 1,024 project descriptors.
The benchmark host explicitly selects a 4,096 OS soft FD limit; the embedded
library only observes host limits. Each cell includes fresh construction,
K changed-text/vector replacements, K deletions, and every one of 128 sustained
rounds with two upserts and an actual merge. Every peak and steady RSS sample
is available and within the admitted writer budget. All {source_count:,}
source hashes and the release binary remain unchanged across the matrix.

| Logical body corpus | K | Replacement artifact bytes | Delete artifact bytes | Lifetime peak RSS MiB | Maximum steady RSS MiB | Actual merges | Sustained artifact amplification |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
{rows}

The sustained amplification denominator is every round's changed body and
embedding bytes; its numerator is newly emitted checkpoint plus merge
artifacts. It excludes device/filesystem writes and graph/WAL bytes. The raw
report retains every round, initial/upsert vector and RaBitQ payload bytes,
fixed-K corpus scaling and fixed-corpus K scaling.

The artifact-byte qualification requires all three shapes: less than twofold
checkpoint-byte growth at fixed K when the corpus grows fourfold; at most
tenfold byte growth when K grows tenfold at fixed corpus; increasing changed
vector/RaBitQ payloads; no base-document hydration; and incremental checkpoints
below one percent of full artifacts. This is an empirical check including
manifest overhead, not an asymptotic proof.

For the matched 20 GiB/K10 shape, historical `8cbe16f8` writes {old:,} search
artifact bytes at checkpoint, versus {new:,} here: an observed artifact-byte
ratio of {ratio:.2f}. Historical initialization includes all 32 seed rows
together. Persistent formats, locked dependencies and admission APIs differ,
so this qualifies search artifact bytes only. Repeated synthetic text is
compressible; logical corpus size is distinct from emitted artifact size.
Host/cache state is uncontrolled and timing is supporting evidence.

All five full current-source search profiles pass:

{profiles}

Coverage includes all six initial and six incremental ownership regressions,
complete query results/scores, configurable tier/fan-in/crisis policy,
bounded background QoS, cancellation, pinned old readers and rejection before
publication. Background capability differences stay explicit across profiles.
The release reference probe passes all 294 comparisons with distinct and
changed vectors, required RaBitQ artifacts, three actual merges and retained
reader checks.

The complete recovery suite passes {cases} actual cases, zero failed or
ignored, covering {plans:,} fault plans across {families} cut families. It
retains the FD32 fixture, old/new hydration and query oracles, publication
cuts and namespace cases. Its model covers loss, tearing and reordering
around publication and overlapping merge boundaries. Finite fault images
assume completed POSIX synchronization and same-directory atomic rename;
process-kill tests alone are not hardware power-loss proof.

Partitioned initial import covers both new and existing roots at data writes,
private-prefix manifest renames and final real-selector publication. The
append/mutation/compaction families check complete old/new documents and
text/vector/hybrid query oracles, including partitioned batches. Named cases
and every actual cut-family count are retained in the coverage evidence.

The required 96-target local fuzz command runs at this fixed source, with
unchanged target registration, seeds, cases, resource caps and deadlines.
{fuzz_summary}
Every selected successful raw Rust summary is positive with zero failures or
ignored cases; shell smoke coverage retains its positive XML case. All actual
logs, XML, command outcomes and source/binary receipts remain in the report.
Older #979 fuzz qualification is archived and does not substitute for this run.

The original pre-#979 `594d8a00` matrix, older allocation/resource failures,
and completed `7cb639b9` query/recovery suites are separate raw archives;
none replaces a cell above. Fresh native formatting, strict all-feature/all-target
workspace Clippy, explicit prek, Linux cross-Clippy and documented browser
WASM Clippy pass on this fixed source. Linux cross-Clippy is compilation/lint
evidence. The final documentation commit still runs its own required precommit
checks, and its PR records independent review, required CI and actual main
delivery.

Reproduce with the unchanged commands and environment values in the report's
scale manifest and scripts. Finite query witnesses are not universal ANN
recall guarantees. Private adaptive partitions are not explicit states in
the abstract model. Crash-orphan reclamation (#392), further owner-count
lookup/FD work (#819) and rebuild bisection costs remain separate work.
Large single-document owners retain hard publication/segment limits and
require sufficient compaction input admission. This qualification does not
authorize a stable Mem release or production activation.

'''.format(
    path=relative, commit=REPORT['qualified_commit'], tree=REPORT['qualified_tree'],
    base=binding['documentation_base'], source_count=REPORT['qualified_source_file_count'],
    rows='\n'.join(table), old=before['before_artifact_bytes'], new=before['after_artifact_bytes'],
    ratio=before['before_over_after_ratio'], profiles='\n'.join('- ' + row for row in profile_counts),
    cases=recovery['actual_cases'], plans=recovery['plans'], families=recovery['cut_families'], fuzz_summary=fuzz_summary)
shutil.copytree(BUNDLE, target)
text = existing.replace('## Source-bound resource qualification\n', '## Historical initial ownership qualification\n')
text = text.replace('## Non-goals\n', section + '## Non-goals\n')
doc.write_text(text)
print(json.dumps({'installed': str(target), 'documentation': str(doc),
                  'report_sha256': hashlib.sha256((target / 'report.json').read_bytes()).hexdigest()}))
