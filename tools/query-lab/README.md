# Frozen query experiments

An independent, unpublished developer tool over the ordinary embedded HawDB
API. `query-workloads` owns pure generation; this tool owns frozen manifests,
execution, complete typed result retention and experiment reports. Shiro's
opt-in `sqlsmith` subcommand delegates to this same library. Production HawDB
has no dependency on either tool. All targets and campaigns are manual only.

## Compare template and recursive generation

Run from the repository root. Paths must be new; artifacts are never overwritten.

```console
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- freeze /tmp/smith.json 7 144 128
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- compare /tmp/smith.json /tmp/smith-report.json
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- replay /tmp/smith.json 3
```

The equivalent Shiro entry point is:

```console
cargo run --locked --manifest-path tools/shiro/Cargo.toml --bin shiro -- sqlsmith freeze /tmp/shiro-smith.json 7 144 128
cargo run --locked --manifest-path tools/shiro/Cargo.toml --bin shiro -- sqlsmith compare /tmp/shiro-smith.json /tmp/shiro-smith-report.json
```

`freeze` stores every DDL/insert/index statement, typed parameter, query,
generator mode, seed/index and budget. SHA-256 covers the canonical serialized
manifest. Loading verifies the digest and budgets; execution and replay do not
call generation. Tool users can edit a `Manifest` and call `Frozen::seal` to
create a new identity for a minimized case. The digest detects accidental
changes; it is not authentication for untrusted SQL. Use an isolated developer
process and known manifests.

Both modes execute against one materialized fixture and pinned snapshot.
Reports retain every cell, including generation refusals, parse/bind failures,
unsupported capabilities, runtime errors, resource refusals, timeouts, panics
and unexecuted cells. Invalid setup censors all cells and retains its error.
The denominator is the entire frozen inventory. Syntax combinations count only
completed executions. Access-shape diversity uses executed operator/access
profiles; these do not identify every physical operator. No failed query is
counted as successful coverage.

Errors are replayed using the exact stored setup, SQL and parameters in a fresh
database while the campaign budget remains. Failure replay rate is reproduced
signatures divided by all failed cells; it is null when there are no failures.
Repeated result equality retains schema, scalar types, NULLs and multiplicity;
ordered outputs retain order and bags are canonically sorted. This experiment
measures executable coverage and repeatability. It has no independent result
oracle, so successful execution does not prove semantic correctness. Shiro's
existing TLP/NoREC/metamorphic/plan oracles remain unchanged and separate.

## Cardinality diagnostics

Freeze a separate 77-case directed inventory, execute it through the same
snapshot/budget/replay path, and analyze one validation observation per cell:

```console
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- freeze-ce /tmp/ce.json 7 128
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- ce-run /tmp/ce.json /tmp/ce-base-run.json
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- ce-report /tmp/ce.json /tmp/ce-base-run.json /tmp/ce-base-report.json
```

`freeze-ce` accepts 64 or 128 rows per populated table. Its versioned manifest
retains stable case IDs, cohort/family labels and independently enumerated
expected result counts. It freezes typed parameters, complete setup and budgets
before execution, just like SQLsmith. Existing SQLsmith manifest hashes remain
unchanged. `ce-report` also accepts existing SQLsmith/cost run records; those
cases have no independent expected-result count. Repeated warmup/measurement
rounds never multiply the CE sample weight.
Later failed or unexecuted rounds censor that case's CE metrics rather than
silently accepting its initial successful observation.

Each report retains all frozen cases and physical access/join observations.
Query-result counts are checked against fixture truth separately from CE.
Estimated/actual counts are compared only at the same operator output boundary,
with complete consumption. A residual predicate or aggregate without its own
profile is not measured merely because its input scan has a profile.
Missing observations, failed setup/execution, result-count mismatches, missing
operator evidence, uninvoked operators and partial consumption remain visible.
The independent oracle checks counts, not the complete semantics of result values.

For positive counts, Q-error is `max(estimated / actual, actual / estimated)`;
signed bias is `log2(estimated) - log2(actual)`. Zero/zero, zero actual and zero
estimate are separate cohorts without smoothed Q-errors. An estimate of one on
empty output is counted explicitly; it is compatible with a minimum-one floor
but does not prove that provenance. Percentiles use nearest rank, report null
for empty positive cohorts, and include p50/p90/p95/p99/max. Reports retain
under/overestimation counts, cohort/family/operator breakdowns and the ten worst
positive pairs. A large Q-error is diagnostic evidence, not a correctness or
performance failure by itself.

Run the candidate binary against **the same frozen manifest** into a new raw
run file, then compare the two source-bound observations:

```console
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- ce-diff /tmp/ce.json /tmp/ce-base-run.json /tmp/ce-candidate-run.json /tmp/ce-diff.json
```

The comparison refuses foreign manifest identities and malformed/duplicate
operator evidence. It retains the complete case inventory, including cases
missing from one run. Complete typed results, executed access shapes and operator
identities must agree before pairing. Changed actual counts and partial/absent
observations are not scored as CE regressions. Paired Q-error distributions use
the same positive pairs on both sides; zero-case changes are separate. Each
input's canonical run-record digest and environment are retained. Checkout
observations still do not attest the compiled engine: retain source revision,
build configuration and binary with each run.

Access shape is not a complete physical fingerprint, and these diagnostics do
not re-cost plans, inject actual cardinalities, change statistics or establish
plan regret. `ce-diff` returns success for a well-formed diagnostic even if it
reports regressions or unpaired cases; inspect the comparison counts. `ce-report`
returns failure when a frozen case lacks valid result/operator evidence while
still writing its diagnostic artifact. All artifacts remain outside Git.

## Compressed graph statistics

The graph adapter freezes 20 indexed/unindexed cases with hot/cold equality,
numeric ranges, missing properties, duplicate IN values and not-equal. Fixture
rows are bounded to 64..=1024. The manifest includes `frontend: cypher`; legacy
SQL manifests omit this default field and preserve their canonical hashes.

```console
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- freeze-graph-stats /tmp/graph-stats.json 42 128
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- ce-run /tmp/graph-stats.json /tmp/graph-stats-run.json
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- ce-report /tmp/graph-stats.json /tmp/graph-stats-run.json /tmp/graph-stats-ce.json
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- cost-check /tmp/graph-stats.json /tmp/graph-stats-cost-check.json
```

Graph observations use the embedded qualification API: plan, raw cost and actual
operator outputs belong to the same execution on a pinned snapshot. Each call
bypasses the physical cache. Only successful whitelisted no-LIMIT/no-existence
queries establish complete child consumption; missing/uninvoked counters remain
unobserved. Frozen counts check fixture truth independently from estimates.
Graph evidence includes the complete selected tree fingerprint, statistics
decisions and whole-call latency. Stage timings and allocator peaks remain null;
CE IDs are native graph ordinals plus one to preserve the common nonzero ID
contract; `native_ordinal` retains the exact graph identity in raw evidence.
RSS fields are optional process observations, not per-query allocation peaks.
`cost` runs the same rounds in release mode. This measures selected-plan
observations, not candidate regret or an automatically fitted cost policy.
See [the distribution contract](../../docs/COMPRESSED_HISTOGRAMS.md).

## Frozen cost baseline

`freeze-cost` invokes the same generator with 32 nodes, depth four, at most two
relations, 16 output rows and 64 KiB payload. Defaults use 24 cases per mode and
1,024 fixture rows. The ordinary `freeze` profile uses 96 nodes, depth six,
three relations, 64 output rows and 256 KiB payload. Typed callers can provide
other validated `Limits` without changing engine policy.

```console
cargo run --locked --manifest-path tools/query-lab/Cargo.toml -- freeze-cost /tmp/cost.json 42 24 1024
cargo run --locked --release --manifest-path tools/query-lab/Cargo.toml -- cost /tmp/cost.json /tmp/cost-report.json
```

One validation, three warmups and fifteen measurements execute per cell, on the
same snapshot with no intervening mutation. A separate frozen schedule seed
shuffles each round. Each measurement must match validation's complete typed
result and executed access shape. Failed cells retain all later attempts as
`not_run`; incomplete sampling has no median. `cost-check` exercises exactly
this protocol under the checked profile; `cost` refuses checked builds.
Compilation and fixture setup are outside query timings. Raw samples include
parse/bind/plan/execute, complete profiled-call latency, available cost components,
operator rows, storage counters and tracked blocking memory. Timing includes
query preparation and owned output, but excludes host report serialization.
Results describe warmed materialized data; no cold/device-cache claim is made.

This implements workload reuse and ordinary-plan baseline collection, **not**
the complete [optimizer cost qualification protocol](../../docs/specs/OPTIMIZER_COST_QUALIFICATION_SPEC.md).
The SQL adapter has no candidate selector or complete physical fingerprint;
neither adapter provides alternative-plan
coverage, regret or fitted coefficient change. SQL from different generator
modes is never treated as alternative plans of the same query. Missing cost,
allocator/RSS and complete-plan evidence remains null. Full scan/index,
covering, order and Sort/TopN qualification remains in the development plan.

## Bounds and evidence

Defaults: 5 seconds per query, 120 seconds per campaign including setup/replay,
10 million work units per query (a separate shared setup budget), 256 MiB
query memory and strict output row/payload caps. The frozen intermediate-row
budget separately reserves for dimension rescans and unfavorable join orders:
`clamp((rows + 8) * 8^(max_relations - 1) * max_relations, 1024, 100000000)`.
It is an admission ceiling, not an output-size estimate. Manifest limits cap cases at 512 per
mode, fixture rows at 100,000 and serialized input at 64 MiB. After 32 MiB of
retained observation payload, later attempts are refused; the final admitted
observation and all omitted-cell records remain. Metadata/report serialization
is outside engine query budgets. Raw reports and progress logs belong outside
Git. These runtime deadlines are cooperative, not process isolation or a hard
wall-clock guarantee. Run an external process timeout for untrusted experiments.

Before each query the CLI flushes its replay address to
`REPORT.progress.jsonl`. An abort, external timeout or crash leaves that address
for `replay`; an incomplete log is not a successful campaign. Panic stops use of
the current fixture. Process crashes cannot be caught by the Rust runner.

Environment records distinguish observed checkout state from compiled-engine
provenance. Retain the binary, build command, source commit and uncommitted diff
with a measurement report; a runtime `git rev-parse` alone cannot attest which
engine was compiled. Compare profiles and feature sets explicitly: standalone
query-lab uses HawDB without default features, while Cargo Shiro unifies its features. The Bazel query-lab target explicitly
uses `hawdb_minimal`, including when called from the Shiro binary.
Do not claim a performance improvement from these diagnostic baseline samples.

Manual controls:

```console
cargo test --locked --manifest-path tools/query-lab/Cargo.toml --all-targets
cargo fmt --manifest-path tools/query-lab/Cargo.toml -- --check
cargo clippy --locked --manifest-path tools/query-lab/Cargo.toml --all-targets -- -D warnings
bazel test //tools/shiro:shiro_tests //tools/shiro:shiro_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

`//tools/query-lab:query_lab_tests` owns fixed-fixture validity, exact replay,
NULL/multiplicity, negative parse/output-refusal controls, setup censoring and
frozen cost sampling. It is selected only by the manual Shiro test suite.

## Initial local evidence (2026-11-04 UTC)

Engine base: `612d010d4188fda6c6d9dca7c03a7ce82dd288d0`; tool changes are in
this delivery. Rust 1.97.1, Linux x86_64, ordinary repository profiles. With
144 cases per mode and the same 128-row fixture, the checked-profile comparison
produced the following completed-execution coverage:

| Seed | Template / recursive completed | Syntax combinations | Executed access shapes |
| --- | --- | --- | --- |
| 0 | 144 / 144 | 5 / 91 | 5 / 8 |
| 7 | 144 / 144 | 5 / 103 | 6 / 8 |
| 42 | 144 / 144 | 5 / 99 | 6 / 6 |

All 864 queries completed. Nine selected CLI replays retained identical complete
typed output. There were no natural failures, so the observed failure-replay
rate is null; dedicated missing-column and output-refusal controls reproduce
failures without treating them as agreement. The initial 2,160-record v5 extraction inventory remained unchanged. After
synchronizing main, the committed inventory is independently frozen from
`37c0c50ec` (campaign v6), preserving its scalar-regex shape and rewrite mapping. Eight query-lab controls cover frozen inputs, repeated-result
and access-shape drift, NULL/bag multiplicity, setup failure and refusal.

The constrained seed-42/24-cases/1,024-rows checked-profile cost replay completed
all 912 attempts across 48 cells, including all 15 samples per cell. These are
functional sampling controls, not latency evidence. An earlier pilot exposed an
undersized input-row budget; the explicit intermediate-row budget above fixes
that prerequisite without weakening the output bound. Its refused pilot is not
counted as successful sampling.

The initial release prerequisite build took 1,153 seconds; the final relink took
296 seconds while other required local checks were compiling. These are build
wall times, not query timings or evidence of a CI speedup. Raw manifests,
reports, progress streams and build/test logs remain outside Git.

After the build/test processes stopped, the release replay of the same frozen
48-cell cost manifest completed all 912 attempts. All complete typed outputs
matched the checked run; the snapshot epoch remained fixed. Campaign wall time
was 1.242 seconds, fixture setup 0.0377 seconds and the sum of the 720 measured
query-call durations 0.610 seconds. These are warm, materialized diagnostic
samples on this host, not a speedup, candidate comparison or qualification gate.

The required manual Bazel command reported 101/101 targets green (99 executed,
two cached); final-source confirmation reused those results. Shiro's actual
`sqlsmith` CLI completed its freeze/compare control. Independent tool formatting
and strict Clippy passed, as did the root formatting check and package/dependency
exclusion checks. The separate MVCC local habit completed 1,612 cases in 30.007
seconds after its compilation.
