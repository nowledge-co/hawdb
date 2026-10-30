# Agent Notes

## Crate Boundaries

- Do not split crates for their own sake.
- Add a new crate only when it has a clear ownership boundary, dependency-direction benefit, compile-time isolation benefit, or stable reuse contract.
- Keep `hawdb` as the SQLite-like embedded library facade; internal crates should support that facade instead of becoming accidental production integration points.

## Library-First Integration

- Treat HawDB as an embedded Rust database library, similar to SQLite or LanceDB usage from a host process.
- Production Mem integration must call Rust library APIs directly; command-line binaries may exist only as thin developer, fixture, or preflight wrappers over the same library path.
- New readiness, slow-query, blackbox, background-maintenance, and replacement-gate capabilities should expose typed Rust APIs first, then derive JSON or CLI output from those APIs when needed.
- Avoid environment variables, command arguments, helper processes, or shell-out behavior as production control planes.

## Mem Release Modes and Full Verification

- During the current development phase, HawDB code must not appear in any stable or GA release. Stable release artifacts and the public release dependency graph must exclude HawDB crates, features, binaries, bundled source, and transitive HawDB dependencies.
- Every Mem App persistence-schema change must update the HawDB integration contract in the same delivery. Canonical data changes require matching HawDB DDL, import or dual-write mapping, and full-verification coverage. Source-only migration or operational tables must be named explicitly with a rationale and an exclusion test; they must not disappear from the contract merely because HawDB does not store them.
- HawDB has not entered production and has no persisted production compatibility obligation. Until that changes, App schema evolution may replace the greenfield HawDB baseline destructively and require development databases to be recreated. Do not add compatibility migrations solely for earlier development-only HawDB schemas, and do not apply this exception to any authoritative legacy or Cloud datastore.
- Only nightly or development builds may compile, bundle, or execute HawDB during this phase. The stable dual-write and full-activation rules below remain inactive until a separate explicit release-policy decision authorizes HawDB code in stable artifacts.
- Nightly or development builds may support three explicit storage modes:
  1. dual-write to the legacy stores and HawDB while legacy reads remain authoritative;
  2. dual-write and dual-read from the legacy stores and HawDB, with mismatches surfaced as verification evidence;
  3. HawDB-only reads and writes.
- After HawDB is explicitly authorized for stable release artifacts, but before the first full HawDB activation, stable or GA builds support only the dual-write transition mode and keep legacy reads authoritative. Dual-write startup must not run or require a full consistency comparison between HawDB and the legacy stores.
- The first stable or GA activation that makes HawDB the active data plane must complete this bootstrap sequence before reporting HawDB as ready: successfully open HawDB, import all required data from Kuzu, LanceDB, and SQLite, then complete one full-data verification of the imported result.
- The activation verification may use bounded pages or streaming, but it must cover the complete imported datasets rather than a sample. A mismatch or incomplete import or verification must fail closed for HawDB readiness, keep the legacy stores authoritative and retained, and must not publish ownership, authorize cutover, or delete legacy data.

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

## Durability and Power-Loss Safety

- Persistent HawDB defaults to power-loss-safe commits (`SyncOnEveryWrite`). Hosts may explicitly select `SyncOnCheckpoint` through typed Rust configuration for DDL/DML, accepting loss of recent acknowledged transactions after power failure. Never silently weaken the default.
- Branch creation, logical deletion, and head publication must remain power-loss safe in every mode. In synchronous mode, persist all transaction recovery dependencies before acknowledging commit; group commit callers wait for the shared flush. In relaxed mode, acknowledge only after flushing the complete transaction WAL to the OS, document the unsynced loss window, and synchronize covered writes at successful checkpoint/seal boundaries.
- A lost response may leave a fully committed operation. Preserve atomic recovery and idempotent branch-creation outcomes rather than treating a missing acknowledgment as proof of rollback.
- Branches remain durable across handle closure, process crashes, and machine restarts until explicitly deleted. Runtime leases/open locks do not determine branch existence; unleased branches remain GC roots. TTL/automatic expiry is not required at this stage.
- Recovery must preserve complete schema/data transactions and consistent checkpoint/head generations. Corruption or uncertain publication must fail closed and retain evidence; never discard commits covered by a completed durability barrier or recreate an empty branch. Relaxed mode may lose unsynchronized transactions, but never expose partial schema/data transactions.
- Qualification must model lost unsynchronized writes, torn writes, and write reordering across WAL, catalog, head, checkpoint, and GC boundaries. Process-kill/reopen tests alone do not prove power-loss safety. State platform/storage synchronization assumptions and distinguish required behavior from verified implementation.

## PR and Branch Workflow

- Follow `CONTRIBUTING.md` and the templates in `.github/ISSUE_TEMPLATE/` and `.github/PULL_REQUEST_TEMPLATE.md` for issue and PR content, titles, validation evidence, and review dispositions.
- Every PR, including documentation and mechanical maintenance changes, must link an existing, relevant issue in an `Issue Number:` line. Create the issue first if none fits. Use `close #123` only when the PR fully resolves the issue and completes all acceptance criteria; otherwise use `ref #123` and describe the remaining work. `None`, missing references, and unreplaced placeholders are not allowed. Verify the issue and relationship before approving a PR.
- Every PR must target `main` directly. Do not stack a PR's branch on top of another PR's branch as its base.
- Stacking causes real problems in this repo's merge flow: a PR merged into a non-`main` base only lands in that feature branch, not in `main`, even though it shows as `MERGED`. If the upstream PR later gets retargeted straight to `main` and merges there, the downstream branch is silently orphaned — it no longer has any path back to `main` until someone notices and retargets it by hand.
- If work is naturally sequential, land each piece as its own PR against `main` before starting the next one, or keep it as commits within a single PR instead of a chain of branches.

## Required Pre-commit Lint Checks

- The pinned HawkEye 7.2.0 pre-commit hook checks staged license headers against `licenserc.toml` before the Rust checks. The read-only wrapper also accepts canonical Nowledge Apache-2.0 SPDX short headers through HawkEye, without requiring unrelated header rewrites. It is read-only and uses an isolated environment. Preserve existing copyright evidence; full-tree historical header debt is distinct from staged-file validation.
- Enable the repository's `prek.toml` hooks with `prek install`. Use `prek run --all-files` to run the formatting and native Clippy checks explicitly; the hooks also run automatically at pre-commit. Target-specific checks remain additional requirements.
- Before every commit, run `cargo fmt --all -- --check` and `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings` with the repository's pinned Rust toolchain. Both checks must pass on the final changes before committing.
- Fix all formatting and Clippy diagnostics found by these checks, including problems in existing code outside the current diff. Do not dismiss a failure as pre-existing, disable a lint, add suppression attributes, or weaken the check to obtain a pass.
- When changing target-specific code, also run strict Clippy for the affected supported target and feature configuration. For the minimal browser WASM runtime, use the compiler and archiver setup in `docs/WASM.md` and run `cargo clippy --locked -p hawdb --no-default-features --target wasm32-unknown-unknown --lib --test in_memory_portable -- -D warnings`.
- After fixes, rerun the affected checks and appropriate regression tests. Record the commands and results in the PR. If an environment or toolchain problem prevents a required check from passing, report the blocker and leave the changes uncommitted until it is resolved; passing CI or unrelated tests does not replace these checks.

## Test Ownership and CI Cost

- Strict append preparation diagnostics use
  `cargo bench --locked -p hawdb-storage --bench strict_append_prepare` manually.
  Keep preparation-only results separate from synchronized commit throughput
  and p99; the workload and existing CI regression owners are in TEST_INDEX.

- Maintain [docs/TEST_INDEX.md](docs/TEST_INDEX.md) as the test-family index:
  purpose/failure mode, owning target and execution lane, exact feature or
  fixture scope, known slow paths/issues and local replay entry points.
- Update the index in the same change when adding, moving, optimizing or
  changing the scope, profile, isolation, timeout or execution lane of tests.
  Keep detailed component specifications linked from the index rather than
  duplicating all Rust case names there.
- Diagnose slow CI with complete, source-bound BEP. Separate queue/analysis,
  compilation, actual test execution, cached result durations and exclusive
  serialization. Never describe a timeout ceiling as a fuzz run duration.
- Keep compilation parameters uniform by repository profile, never by target,
  crate or test family. Cargo profiles live in `.cargo/config.toml`, including
  independent workspaces. Bazel uses `.bazelrc`'s low-optimization checked test
  profile by default; explicit release verification uses `--config=release`.
  Do not add target-specific `rustc_flags` optimization/check overrides, custom
  optimization transitions or private differently optimized copies of libraries.
  Platform ABI/linker requirements remain separate from profile settings.
- Preserve complete feature matrices, fixtures, assertions, timeout ceilings
  and exclusive measurement gates. Release-only regressions must run under the
  global release profile, not a target-local assertion override. Keep them owned
  by an explicit required CI step when excluded from ordinary checked suites.
- Report first-build cost as well as execution time. Local improvements do not
  establish remote CI speedup; record final-head CI evidence before that claim.

- Keep checkpoint headroom timing/phase tests in the required isolated
  `//:hawdb_unit_checkpoint_headroom_tests` lane, using the ordinary checked root
  binary. Joined-read default/minimal targets shard the complete discovered case
  inventory over one executable per mode; retain every scenario and refuse empty
  shards. See `docs/TEST_INDEX.md` for scheduling evidence and replay.

- Windows native contracts run as seventeen independent Bazel shards plus independent Cargo Clippy; the original required check aggregates their results. See [Windows native CI](docs/WINDOWS_NATIVE_CI.md) for feature ownership, cache layout and evidence. Keep disk/repository/external cache roots disjoint and preserve default/minimal/all-feature and daily qualification selections.

## Consumer Failure Recovery

- Consumer failure recovery must retain original native birth, scratch and
  prepared-publication custody across resource refusals and registry refreshes.
  Retry cleanup before reinitializing the same target; never adopt or recursively
  delete an unpublished directory by its saved pathname. Keep pre-effect
  refusal, uncertain publication, process recovery and power-loss evidence distinct.
- Terminal unselected creation and pre-selection source advancement must settle
  the operation and release runtime custody/capacity. Do not retain them as
  retryable Busy; require an explicit rebuild on a fresh target.

## Local Fuzz Verification

- Keep fuzz targets available through Bazel, but do not add them to default or dedicated CI jobs.
- Routine local verification must run `bazel test //tools/shiro:shiro_tests //tools/shiro:shiro_cli_tests //:hawdb_linux_ci_fuzz_smoke_test`.
- The standalone MVCC campaign lives in `tools/mvcc-fuzz/` as a binary-only,
  unpublished, independent Cargo workspace. Do not move it into `crates/`, add
  it to the library workspace/dependency graph, bundle it in published library
  archives, or register it in CI, scheduled workflows or test suites.
- During routine local development, run a short campaign from the repository
  root: `cargo run --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- run --seconds 30 --explain`.
  The time budget starts after compilation and stops between complete cases;
  this is a manual local habit, not an automated scheduled task.
- Use `generate --seed 42 --index 5` to print an editable `.mvcc` case, and
  `replay path/to/case.mvcc --explain` for SQL, parameters and expected/actual
  results. Preserve failure artifacts, replay them, and use `shrink` before
  turning a confirmed bug into an ordinary focused regression test.
- Format and lint this independent tool explicitly when changing it:
  `cargo fmt --manifest-path tools/mvcc-fuzz/Cargo.toml -- --check` and
  `cargo clippy --locked --manifest-path tools/mvcc-fuzz/Cargo.toml --all-targets -- -D warnings`.

## Shiro developer tool

- The SQL/Cypher multi-oracle campaign is named Shiro and lives in
  `tools/shiro/`, an independent unpublished Cargo workspace. Its supporting
  library is private to the tool; production HawDB must not depend on Shiro.
- Prefer `make mvcc-fuzz` (30 seconds) and `make shiro-fuzz` (until Ctrl+C) for
  routine local runs. Override `MVCC_SECONDS`, `MVCC_SEED`, `SHIRO_SECONDS`,
  `SHIRO_SEED` and the corresponding log-directory variables as needed.
- Use `bazel run //tools/shiro:shiro -- --seed 7 --cases 32 --print-report` for a
  short local campaign, or `cargo run --locked --manifest-path tools/shiro/Cargo.toml --bin shiro -- --seed 7 --cases 32 --print-report`.
  The MVCC tool remains available through `bazel run //tools/mvcc-fuzz:hawdb_mvcc_fuzz -- run --seconds 30 --explain`.
- Keep both tools out of CI, scheduled jobs, production dependencies and library
  package archives. Shiro's test targets are explicit local verification only;
  do not add them to crate presubmits or the ordinary CI smoke dependency graph.
- Preserve typed rows, duplicate multiplicities, NULL/empty-input semantics,
  pinned snapshots, exact replay statements/parameters and failure signatures.
  NoREC currently uses `COALESCE(SUM(1) FILTER (WHERE p), 0)` as its reference
  because CASE WHEN is unsupported; do not claim the classic CASE oracle.
  DQP must compare identical SQL/parameters and distinguish actual different
  plans from same-plan agreement in coverage reports.
- Run independent tool formatting and strict Clippy explicitly when changing it:
  `cargo fmt --manifest-path tools/shiro/Cargo.toml -- --check` and
  `cargo clippy --locked --manifest-path tools/shiro/Cargo.toml --all-targets --all-features -- -D warnings`.

## PostgreSQL 18.0 Compatibility

- Keep the manual compatibility project in `tests/pg-regress/`, following the
  independent `src/`, `data/sql`, `data/expected`, isolation and schedule layout.
  It is an unpublished independent workspace, excluded from library archives.
- Pin upstream inputs to PostgreSQL `REL_18_0` commit
  `3d6a828938a5fa0444275d3d2f67b64ec3199eb7`. Keep checksums, source line ranges,
  license and explicit adaptations with cases. Do not silently rewrite SQL.
- Use `make pg-regress-list` to inspect the selected subset and
  `PGREGRESS_DSN='...' make pg-regress` against an isolated test database. The
  reference must report server version 180000; later 18.x servers are refused.
  The driver creates and removes only its own uniquely named schemas.
- Report PASS, MISMATCH, UNSUPPORTED and infrastructure errors separately.
  Unselected upstream statements are not coverage. An unsupported case is never
  a pass; matching arbitrary errors are never agreement. Preserve duplicate
  multiplicity and explicit ordering; document integer-width/column-label maps.
- Do not claim PostgreSQL SSI equivalence from strict stepwise comparisons:
  valid serialization failures may occur at different steps or select different
  victims. Keep wait-aware isolation schedules and such comparisons explicit.
- Keep this harness out of CI and scheduled jobs. The comparison CLI is pure
  Rust: manifest/schedule loading, typed adapters, comparison and reporting stay
  in this independent project. Use the Rust `libpq` crate for the reference
  rather than hand-written FFI or a Python/process orchestration layer.
- Format/lint the independent workspace with all features. Run its native
  controls with `cargo test --locked --manifest-path tests/pg-regress/Cargo.toml
  --all-targets --all-features`. The additional manual Bazel target
  `//tests/pg-regress:hawdb_pg_regress_worker` is only an embedded API fixture;
  the complete native comparison CLI is built/run through Cargo and Make.

## Bf-Tree MVCC experiment

- Repository version bumps must also relock `experiments/bftree-mvcc/Cargo.lock`
  so its local `hawdb-*` path package versions stay current.

- `experiments/bftree-mvcc/` is an independent, binary-only, unpublished workspace.
  Keep it and `bf-tree` out of production dependencies and library archives.
- Run its focused tests and strict Clippy manually when changing it; do not add
  this experiment to CI or scheduled jobs. Follow its README for exact commands.
- Changes to MVCC layouts require release-profile comparisons of versioned,
  hybrid and COW reference layouts under identical journal synchronization,
  including current/old reads, scan and write tails, recovery and memory costs.
  Keep component results distinct from end-to-end HawDB performance claims.
