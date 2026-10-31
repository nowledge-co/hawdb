# Shiro

`shiro` is a development-only correctness harness over HawDB's public embedded API. The
production `hawdb` crate does not depend on it.

Opt-in optimizer qualification compares one identical statement/parameter set
on a pinned snapshot with optional rule disabling, Applied-event assertions and
bounded seeded ranking changes. It records the actual executed trace, preserves
errors and distinguishes different-plan agreement from same-plan agreement.
See the [API, rule scope and replay command](../../docs/specs/OPTIMIZER_PROOFS_SPEC.md).

The local process-memory state-machine campaign exercises 32 seeds with 512
sample/reserve/release/resource-refresh steps each through two shared governors.
An independent interval-debt model checks admission, hysteresis, missing and
unsupported samples, stable-capacity errors, reservation release, pinned
resources, and observed-growth accounting. Lifetime peak RSS is deliberately
above the limit so accidental peak-based admission fails the oracle.

```console
cargo test --locked --manifest-path tools/shiro/Cargo.toml --lib process_memory_state_machine_matches_interval_oracle -- --nocapture
```

The campaign is included in `//tools/shiro:shiro_tests` and remains local-only.
Native OS metrics and embedded query recovery are qualified separately by
`tests/process_memory_policy_contract.rs`; the deterministic oracle does not
claim platform RSS or allocator evidence.

The [DST scoping recommendation](../../docs/DST_SCOPING.md) explains the existing
I/O and scheduling seams, the limits of seeded replay, and the decision gates for
a possible storage-only simulator. It does not authorize simulator implementation
or replace these campaigns.

The generator first creates a deterministic graph state, then chooses query shapes and typed
predicate and query AST nodes that are validated against the generated schema before rendering. A
campaign runs the original nine complementary oracles plus the operator relations below:

- The plan-differential oracle applies mutations once, pins one read snapshot, and executes the
  same parameterized Cypher query through memo child resolution and deterministic direct child
  resolution. Both modes now share the physical lowerer; this checks child wiring and directive
  handling, not independent lowering implementations. Agreement cannot detect a shared lowering
  bug, so the TLP, predicate-rewrite, and metamorphic oracles remain necessary.
- The Graph TLP oracle evaluates `Q`, `Q WHERE p`, `Q WHERE NOT p`, and a constrained
  `Q WHERE nullable_operand IS NULL` partition on one pinned snapshot. The generated predicate is
  a single comparison against a non-null parameter, so the null-operand partition is exactly the
  unknown partition. Under bag semantics, the original rows must equal the multiset union of all
  three partitions. This checks nullable node-property, range, and relationship-property
  predicates without implementing another graph executor.
- The Graph TLP Aggregate oracle runs `count(variable)` over the original match and the same three
  predicate partitions on one pinned snapshot. The original count must equal the checked sum of
  the partition counts. This follows SQLancer's TLP Aggregate construction and exercises HawDB's
  aggregate execution path without adding a reference executor or host-side graph semantics.
- The graph predicate-rewrite oracle cycles through double negation, conjunction idempotence,
  disjunction idempotence, null totality, and the two predicate absorption laws. It
  compares the original and rewritten Cypher on one pinned snapshot under bag semantics. Unlike
  TLP recombination, each relation must preserve the selected rows directly, so a
  predicate-classification defect cannot be hidden by compensating partitions. The absorption
  relations combine the generated predicate with a nullable secondary predicate, so they also
  stress nested three-valued logic without treating plan shape as truth.
- The graph-metamorphic oracle executes an identifier-bijection transform for every query and a
  direction-reversal transform whenever the typed AST contains a directed relationship. The
  transformed graph, parameters, and relationship pattern change together; identifier values are
  normalized before comparison. Applicability guards and independent mismatch/error signatures
  keep an unsupported transform or setup failure distinct from a semantic mismatch.
- The SQL TLP oracle creates a deterministic PostgreSQL-style relational schema with primary keys,
  nullable scalar columns, optional indexes, and parameterized inner/left joins. It compares an
  unfiltered SELECT with the bag union of its predicate-true, predicate-false, and predicate-null
  partitions through `Database::query_sql_with_params` on one pinned snapshot. Generated
  predicates cover scalar comparisons, `IN`, column comparisons, and nullable `AND`/`OR`
  composition; each shape is constructed so its unknown partition is non-empty.
- The SQL TLP Aggregate oracle applies `COUNT(*)` to the same generated FROM/JOIN and predicate
  variants. The original count must equal the checked sum of all three partition counts. SQL setup,
  typed parameters, queries, `EXPLAIN` plan rows, evidence, and a fresh-state reduced setup are
  retained in the failure report.
- The SQL predicate-rewrite oracle applies the same four three-valued-logic relations to the
  generated PostgreSQL-style predicates. Positional parameters in the unknown partition are
  deterministically rebased before composition. Both variants retain their `EXPLAIN` rows and run
  against the same relational snapshot.
- The SQL join-rewrite oracle generates three- through six-relation INNER/LEFT trees over duplicate
  and nullable values. It compares an optimizer-eligible ordered query with the same query without
  ordering, executed through an explicit syntax-order planning directive, on one pinned snapshot
  under bag semantics. Planning evidence must prove memo selection for the optimized variant and
  `explicit_syntax_order` selection for the independent reference. Shapes include preserved outer
  rows and null-rejection that permits LEFT-to-INNER conversion.

The TLP relations rely on Cypher and SQL three-valued predicate logic: missing or null operands
evaluate to unknown, and `NOT unknown` remains unknown. Duplicate rows, missing values, null
values, and floating-point bit patterns remain distinct in oracle comparisons.

Run a deterministic campaign with:

```console
make fuzz-optimizer FUZZ_SEED=7 FUZZ_CASES=128
make fuzz-optimizer FUZZ_SEED=7 FUZZ_CASE_INDEX=19
make fuzz-optimizer-resume FUZZ_SEED=7 FUZZ_CASES=1024 \
  FUZZ_SHARD_INDEX=0 FUZZ_SHARD_COUNT=4
```

Successful campaigns keep stdout quiet and refresh a multi-oracle JSON report under
`target/fuzz-logs`. Set `FUZZ_PRINT_REPORT=1` to also print it to stdout. Any mismatch exits
non-zero and preserves a failure report containing the exact mutations, typed parameters,
rendered query AST metadata, all query variants, result semantics, plan fingerprints, optimizer
stages, predicate rewrites, metamorphic transforms, and a direct reproduction command. Every
failure carries a stable oracle-specific signature; reducers retain only candidates that
reproduce that signature.
Plan-fingerprint novelty is reported as coverage telemetry and never changes a correctness verdict.
The differential reducer first minimizes graph mutations and then typed query AST nodes. It accepts
a candidate only when the same failure signature still triggers, so a setup, parse, or execution
error cannot replace a semantic mismatch during reduction.

## SQL maintenance and composite ranges

Every generated case also runs a bounded stateful campaign over an indexed
source and an unindexed reference table. Identical parameterized writes update
both in one transaction. Materialized projection, grouped and ungrouped
COUNT/COUNT(column)/SUM/MIN/MAX results are compared with full SELECT
recomputation on the reference; projection bags retain duplicates and NULLs.
BIGINT and TEXT extrema include duplicate extrema, NULL keys, all-NULL groups,
filter changes, primary-key moves and deletion of the last contributing row.
Views are backfilled over existing rows and include an index on visible outputs.

The deterministic sequence mixes INSERT, UPSERT, UPDATE and DELETE with random
values and target keys, explicit rollback, clear/repopulation and aggregate
BIGINT overflow rejection after an earlier staged write. Each transaction checks
read-your-writes, committed or rolled-back results, and the unchanged pinned
pre-write snapshot. Rejection must match the specific aggregate overflow
condition and leave the source, views, reference and commit epoch unchanged;
arbitrary matching SQL errors never count as agreement.

Composite `(bucket, score, id)` queries compare indexed and unindexed execution
with inclusive/exclusive endpoints, equal/reversed/NULL bounds, ASC/DESC order,
a non-indexed residual filter, and LIMIT/OFFSET with a total order. The existing
seed/case-index replay controls all choices. `sql_maintenance_coverage` reports
completed operations and actual range-index versus fallback plan selections
separately. Plan counters describe the initial fixture, not every later step.

Failures retain typed rows, queries/parameters, plans for result mismatches,
the transaction step, complete setup and operation sequence. Reduction removes
whole transactions on fresh databases and retains only the same failure
signature. The campaign protocol is `shiro-multi-oracle-fuzz-v4`; older progress
files cannot resume with the stronger oracle. Maintenance replay bundles use
`shiro-sql-maintenance-v2` and describe explicit commit/rollback boundaries.

Cases rotate deterministically through memory, file-backed Shadow indexes and
file-backed DemandPaged indexes (`case_index % 3`). Each disk case reopens after
initial WAL-tail writes, after checkpoints, and after further WAL mutations.
Setup appends data after its final DDL because schema changes themselves require
a checkpoint; the initial recovery must replay a non-empty WAL tail.
The sequence covers rollback/refusal before reopen, empty checkpoints, later
repopulation and continued view/index maintenance after recovery. Checkpoints
also retain the pre-write reader until its old results have been verified.
Before closing, the oracle keeps the complete query results and commit epoch
outside the database. Reopened results must match these saved values as well as
the recomputed reference, so losing both source and reference cannot pass.
Recovery reports must prove durable admission, the expected commit/checkpoint
epochs and actual WAL replay; a checkpoint-only reopen must replay zero entries.
`storage_profiles` and `recovery` counters distinguish these paths in coverage.

Disk fixtures use the default SyncOnEveryWrite durability. Source rows remain
resident because materialized views require that mode; DemandPaged exercises
file-backed indexes. Authoritative/out-of-core materialized views are unsupported.
Automatic checkpoint age is one hour so age-triggered publication does not
interfere with the explicit schedule in these short cases. Storage mode and
per-step recovery boundaries survive reduction.
Original failures retain their scratch database path in the report; successful
and reduction-attempt fixtures are removed. Scratch paths use the platform
temporary directory; a local invocation can set TMPDIR to a disk-backed directory.

For example, replay Shadow with `--seed 7 --cases 32 --case-index 1`, and
DemandPaged with `--seed 7 --cases 32 --case-index 2`. These test file codecs,
WAL/checkpoint reopen and continued maintenance. They do not establish machine
power-loss safety, crash/torn-write recovery, concurrent-writer serializability,
or correctness of SQL semantics shared by both executions. Existing fault and
MVCC campaigns remain complementary. No CI job or production dependency is added.

## Extended SQL operator relations

Shiro now additionally runs these relations against one pinned relational snapshot:

| Oracle | Relation and operator coverage |
| --- | --- |
| NoREC-style count | COUNT with WHERE versus COALESCE(SUM(1) FILTER (WHERE p), 0), including nullable predicates and joins; require aggregate FILTER plan evidence |
| DQP | Identical SQL and parameters under selected Auto memo and explicit SyntaxOrder; record normalized physical-plan diversity separately from result agreement |
| HAVING TLP | Partition completed GROUP BY results using SUM predicates and SQL UNKNOWN; project COUNT, COUNT DISTINCT, SUM, MIN and MAX |
| DISTINCT TLP | Compare DISTINCT output against a deduplicated union of all predicate partitions, preserving set versus bag semantics |
| SUM TLP | Recombine nullable SUM partitions with checked integer addition; preserve NULL for empty/all-NULL inputs |
| COUNT DISTINCT | Compare the SQL aggregate with independently deduplicated non-NULL raw values |
| CROSS/comma JOIN | Compare CROSS JOIN plus WHERE, comma FROM plus WHERE and INNER JOIN ON under bag semantics |
| Ordered pagination | Compare LIMIT/OFFSET against the corresponding slice of a full result with total ordering and explicit NULL placement |

CASE WHEN and boolean predicate projections remain unsupported by the engine.
The NoREC-style reference deliberately uses supported aggregate FILTER and SUM;
it does not claim the classic SUM(CASE WHEN ...) construction. No production
SQL support or executor path was added for fuzzing. Auto/SyntaxOrder comparison
is limited to supported join-planning alternatives; same-plan pairs do not count
as diverse DQP coverage. Shared expression/execution defects can escape these
relations, so independent references and complementary oracles remain necessary.

`sql_operator_coverage` reports attempted/passed/failed cases and distinct/same
DQP plan pairs. Successful cases keep compact deterministic plan/row fingerprints
and operator identities; failures retain complete plans, typed results, exact
setup/query parameters and signature-preserving reduced replay bundles. Query
campaign and SQL replay protocols are versioned for the new report contract;
old progress cannot be silently resumed as the stronger campaign.

## Tool and build boundaries

Shiro is an unpublished independent workspace, excluded from the root Cargo
workspace and published library archive. Its supporting `shiro_core` library
serves only its four command-line binaries. Existing public readiness smoke
contracts in `hawdb-fuzz-contracts` retain their own API and are not Shiro.
Historical `//crates/fuzz:*` executable/test labels are local compatibility
aliases; no Cargo crate or implementation remains at that path.

```sh
make shiro-fuzz
make mvcc-fuzz
make shiro-fuzz SHIRO_SEED=42 SHIRO_SECONDS=300
make mvcc-fuzz MVCC_SEED=42 MVCC_SECONDS=10
bazel run //tools/shiro:shiro -- --seed 7 --cases 32 --print-report
bazel run //tools/mvcc-fuzz:hawdb_mvcc_fuzz -- run --seconds 30 --explain
cargo run --locked --manifest-path tools/shiro/Cargo.toml --bin shiro -- --seed 7 --cases 32
cargo fmt --manifest-path tools/shiro/Cargo.toml -- --check
cargo clippy --locked --manifest-path tools/shiro/Cargo.toml --all-targets --all-features -- -D warnings
```

The default query runner continuously generates fresh fixtures until Ctrl+C or
an oracle failure. `--timeout-seconds 30` (or `make shiro-fuzz SHIRO_SECONDS=30`)
stops after the in-flight case finishes. Progress prints every five seconds; `--print-report` additionally prints the full
JSON evidence at termination. Make uses concise progress by default.
The legacy `make fuzz` aggregate keeps finite batches so it can also reach its
storage, append and parser campaigns.
Each bounded generation batch uses a new deterministic seed; only the current
batch (at most 128 cases) is retained in the rolling JSON report. Session counters
cover the whole run; detailed coverage describes the retained batch. Ctrl+C uses
the operating system's normal termination behavior; the last atomic progress
checkpoint remains available. A failure stops the run and saves exact seed/index
reproduction commands and reduced SQL evidence.

Unlike SQLancer's many queries per generated database, Shiro currently constructs
a fresh fixture per case and evaluates its oracle bundle. Explicit `--cases` and
`--case-index` select finite verification or replay; they are not the default
fuzzing stop condition. `--resume` applies only to finite `--cases` campaigns.

The companion executables are `shiro-storage`, `shiro-append`, and `shiro-parser`
(Cargo), or `shiro_storage`, `shiro_append`, and `shiro_parser` (Bazel). Targets
are manual/local only. Neither tool belongs in CI or scheduled workflows; the
normal CI smoke dependency graph excludes the local fuzz executables.

The parser byte campaign uses a corpus derived from the Cypher, relational SQL, and SQL/PGQ test
suites. It keeps the non-ASCII keyword-probe and excessive-nesting regressions as exact seeds, then
alternates arbitrary byte strings with deterministic byte mutations of the corpus. Every input is
bounded and runs all four frontend entry points on an explicitly bounded worker stack. Parser
acceptance and rejection are both valid; a panic or stack overflow fails the campaign. The stable
seed and case index reproduce the exact bytes under `shiro-parser-v1`:

```console
make fuzz-parser FUZZ_SEED=7 FUZZ_CASES=256
make fuzz-parser FUZZ_SEED=7 FUZZ_CASE_INDEX=19
```

The storage campaign mutates one bounded parser input in a generated graph and search fixture per
case. A clean open or a typed storage error are both valid outcomes; a panic is a failure. Reports
include the target artifact, mutation, case seed, and exact replay command:

```console
make fuzz-storage FUZZ_SEED=7 FUZZ_CASES=256
make fuzz-storage FUZZ_SEED=7 FUZZ_CASE_INDEX=19
```

The storage fixture includes a checkpointed Strict Append segment and manifest
plus a post-checkpoint append WAL suffix. Target selection rotates across the
fixture before repeating, so a sufficiently large deterministic campaign covers
both append artifact parsers instead of relying on random selection.

The Strict Append state-machine oracle compares the embedded database with a
pure partition-watermark model. It exercises valid append, duplicate and
out-of-order rejection, arity and type rejection, unknown tables, bounded tail
reads, payload-budget failure, checkpoint reopen, and WAL replay. Every rejected
operation must leave the commit epoch and visible state unchanged.

```console
make fuzz-append FUZZ_SEED=7 FUZZ_CASES=128 FUZZ_STEPS=256
make fuzz-append FUZZ_SEED=7 FUZZ_CASE_INDEX=19 FUZZ_STEPS=256
make fuzz-append-resume FUZZ_SEED=7 FUZZ_CASES=1024 FUZZ_STEPS=256 \
  FUZZ_SHARD_INDEX=0 FUZZ_SHARD_COUNT=4
```

Long append campaigns use the same shard selection and periodic `*-cur.json`
checkpoint contract as optimizer campaigns. Resume validates the protocol,
seed, configured case count, steps per case, shard identity, and the exact
completed case prefix before continuing, so it cannot silently combine cases
from different campaigns.
