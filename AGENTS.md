# Agent Notes

## Crate Boundaries

- Do not split crates for their own sake.
- Add a new crate only when it has a clear ownership boundary, dependency-direction benefit, compile-time isolation benefit, or stable reuse contract.
- Keep `hawdb` as the SQLite-like embedded library facade; internal crates should support that facade instead of becoming accidental production integration points.

## Library-First Integration

- Treat Hawdb as an embedded Rust database library, similar to SQLite or LanceDB usage from a host process.
- Production Mem integration must call Rust library APIs directly; command-line binaries may exist only as thin developer, fixture, or preflight wrappers over the same library path.
- New readiness, slow-query, blackbox, background-maintenance, and replacement-gate capabilities should expose typed Rust APIs first, then derive JSON or CLI output from those APIs when needed.
- Avoid environment variables, command arguments, helper processes, or shell-out behavior as production control planes.

## Mem Release Modes and Full Verification

- During the current development phase, Hawdb code must not appear in any stable or GA release. Stable release artifacts and the public release dependency graph must exclude Hawdb crates, features, binaries, bundled source, and transitive Hawdb dependencies.
- Every Mem App persistence-schema change must update the Hawdb integration contract in the same delivery. Canonical data changes require matching Hawdb DDL, import or dual-write mapping, and full-verification coverage. Source-only migration or operational tables must be named explicitly with a rationale and an exclusion test; they must not disappear from the contract merely because Hawdb does not store them.
- Hawdb has not entered production and has no persisted production compatibility obligation. Until that changes, App schema evolution may replace the greenfield Hawdb baseline destructively and require development databases to be recreated. Do not add compatibility migrations solely for earlier development-only Hawdb schemas, and do not apply this exception to any authoritative legacy or Cloud datastore.
- Only nightly or development builds may compile, bundle, or execute Hawdb during this phase. The stable dual-write and full-activation rules below remain inactive until a separate explicit release-policy decision authorizes Hawdb code in stable artifacts.
- Nightly or development builds may support three explicit storage modes:
  1. dual-write to the legacy stores and Hawdb while legacy reads remain authoritative;
  2. dual-write and dual-read from the legacy stores and Hawdb, with mismatches surfaced as verification evidence;
  3. Hawdb-only reads and writes.
- After Hawdb is explicitly authorized for stable release artifacts, but before the first full Hawdb activation, stable or GA builds support only the dual-write transition mode and keep legacy reads authoritative. Dual-write startup must not run or require a full consistency comparison between Hawdb and the legacy stores.
- The first stable or GA activation that makes Hawdb the active data plane must complete this bootstrap sequence before reporting Hawdb as ready: successfully open Hawdb, import all required data from Kuzu, LanceDB, and SQLite, then complete one full-data verification of the imported result.
- The activation verification may use bounded pages or streaming, but it must cover the complete imported datasets rather than a sample. A mismatch or incomplete import or verification must fail closed for Hawdb readiness, keep the legacy stores authoritative and retained, and must not publish ownership, authorize cutover, or delete legacy data.

## Query and Storage Discipline

- Mem route behavior should be expressed as parameterized Cypher and executed through
  the embedded query runtime. Prefer adding a readable query over adding a
  route-specific typed API or handwritten executor branch.
- Prefer multiple small, named Cypher statements when a route has distinct
  exact-lookup, aggregate/count, candidate-page, or bounded-hydration phases.
  Do not force those phases into one complex statement, and do not move them
  into a host-side graph scan, join, filter, sort, or aggregate merely to
  reduce query count. Push graph semantics and per-phase result-size limits
  into the statements; keep the host to request normalization, non-graph local
  reads, cross-statement budget accounting, and legacy response shaping.
- Keep route queries readable and adjacent to their query-runtime helper. Each
  statement must be parameterized, have an explicit row and payload budget, and
  fail rather than silently returning a partial result when its budget is
  exceeded.
- Add a typed API only when one stable, reusable library contract must coordinate
  multiple statements, a mutation/WAL boundary, recovery, or a capability that
  cannot be represented safely by Cypher alone. Typed APIs must not become a
  convenience wrapper for each REST route.
- Fast paths should be derived from AST or logical-plan shape and remain observable in query reports.
- Keep storage changes recovery-oriented: WAL, checkpoint, pruning, and scan-filter features need targeted tests that prove replay boundaries, torn-tail handling, and no partial mutation recovery.
- Do not add broad indexing, filtering, or optimizer features unless they map to active Mem replacement needs for Kuzu, LanceDB, or the graph-first read path.

## Local Fuzz Verification

- Keep fuzz targets available through Bazel, but do not add them to default or dedicated CI jobs.
- Routine local verification must run `bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test`.
