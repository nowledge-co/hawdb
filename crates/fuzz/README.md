# skein-fuzz

`skein-fuzz` is a development-only correctness harness over Skein's public embedded API. The
production `skein` crate does not depend on it.

The [DST scoping recommendation](../../docs/DST_SCOPING.md) explains the existing
I/O and scheduling seams, the limits of seeded replay, and the decision gates for
a possible storage-only simulator. It does not authorize simulator implementation
or replace these campaigns.

The generator first creates a deterministic graph state, then chooses query shapes and typed
predicate and query AST nodes that are validated against the generated schema before rendering. A
campaign runs nine complementary oracles:

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
  the partition counts. This follows SQLancer's TLP Aggregate construction and exercises Skein's
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
- The SQL join-rewrite oracle generates three- and four-relation INNER/LEFT trees over duplicate
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

NoREC is intentionally deferred until the supported Cypher or SQL subset can express a general
row-wise `SUM(CASE WHEN predicate THEN 1 ELSE 0 END)` relation without adding a fuzz-only executor
path.

The parser byte campaign uses a corpus derived from the Cypher, relational SQL, and SQL/PGQ test
suites. It keeps the non-ASCII keyword-probe and excessive-nesting regressions as exact seeds, then
alternates arbitrary byte strings with deterministic byte mutations of the corpus. Every input is
bounded and runs all four frontend entry points on an explicitly bounded worker stack. Parser
acceptance and rejection are both valid; a panic or stack overflow fails the campaign. The stable
seed and case index reproduce the exact bytes under `skein-parser-fuzz-v1`:

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
