# Contributing to HawDB

HawDB is an embedded Rust database. Contributions should improve a concrete
host workload while preserving query correctness, bounded resource use, and
recoverable storage. Start with the [architecture](docs/ARCHITECTURE.md),
[development plan](docs/EMBEDDED_DEVELOPMENT_PLAN.md), and [open work](TODO.md).
[AGENTS.md](AGENTS.md) records the repository's architecture and integration
constraints and also applies to code produced with an assistant.

## Opening an issue

Search open and closed issues before filing a new one. Choose the closest form:

| Form | Evidence that makes it actionable |
| --- | --- |
| Bug report | Minimal reproduction, expected and actual behavior, version/commit, environment, and relevant configuration. |
| Performance issue | Reproducible workload and data shape, baseline or observed limitation, exact commands, measurements, and resource configuration. |
| Feature request | A concrete host use case, desired behavior, acceptance criteria, and alternatives. |
| Enhancement or maintenance | Current limitation, bounded scope, preserved invariants, and completion criteria. |

Use an issue title such as `storage: duplicate key survives a rejected batch`.
Include the crate version or commit, OS/architecture, toolchain, build profile,
Cargo features, and relevant `DatabaseConfig` values when reporting behavior.
Distinguish an in-memory database from persistent storage and state the
configured residency mode (`Auto`, `Materialized`, or `OutOfCore`). Include the
observed mode when available. Unknown details can be marked as unknown.

For query issues, include schema/index setup, a small fixture, query parameters,
and expected rows or errors. For persistence issues, include transaction,
checkpoint, crash/cancellation, and reopen order, plus whether a write was
acknowledged. A suspected root cause is helpful but is not required to report a
bug. Use synthetic fixtures and sanitize logs; do not upload real user databases
or credentials. Preserve minimized fuzz inputs, seeds, and replay commands.

Use the same sections when filing through `gh` or an API; the web forms cannot
enforce the content of those submissions. Maintainers assign severity from
observed impact rather than from the amount of code involved.

## Preparing a pull request

1. Fork the repository if needed and create a branch from current `main`.
   Every PR targets `main` directly. Keep sequential work in one coherent PR,
   or land each independent piece before starting its dependent PR.
2. Link an issue for behavior changes. Use `Issue Number: close #123` only when
   this PR completes that issue's acceptance criteria; use `ref #123` for
   partial or related work. Docs-only and mechanical maintenance may use
   `Issue Number: None` with a one-line explanation.
3. Keep changes focused and fill in the [PR template](.github/PULL_REQUEST_TEMPLATE.md).
   Explain the problem, mechanism, important tradeoffs, tests, and compatibility
   impact. Large public API or persistent-format proposals should be discussed
   in the issue before implementation.
4. Use `type(scope): concise change` for PR titles and commit subjects, for
   example `fix(storage): reject duplicate keys before WAL append` or
   `docs(contributing): describe recovery test evidence`. Common types are
   `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, and `chore`.
   Use the affected component as the scope, such as `api`, `cypher`, `sql`,
   `optimizer`, `storage`, or `search`. Write repository content and PR/commit
   text in English.
5. Keep the PR description current when the implementation changes. Explain
   unresolved risks and link any agreed follow-up; do not mark work complete
   merely because one test suite passes.

## HawDB design boundaries

- Keep `hawdb` as the host-facing embedded facade. Add crates only for a clear
  ownership, dependency, compile-time isolation, or reuse boundary. CLI tools
  should remain thin developer/fixture/preflight wrappers over library APIs.
- Express graph behavior through readable parameterized Cypher, and relational
  behavior through SQL. Reserve typed APIs for reusable kernel contracts such
  as transactions, recovery, bounded retrieval, and maintenance; follow the
  [query-first API contract](docs/specs/QUERY_FIRST_PUBLIC_API_SPEC.md).
- Connect indexing, optimizer, and storage features to an active workload.
  Keep limits and fallback behavior explicit. Report incomplete or sampled
  evidence accurately, and preserve fail-closed contracts where completion is
  required.
- Describe API and persistent-format changes separately. Follow the current
  greenfield schema and Mem release/full-verification rules in [AGENTS.md](AGENTS.md).
  Development database recreation is not permission to destroy authoritative
  legacy or Cloud data. A HawDB crate release does not authorize inclusion or
  activation in stable Mem artifacts.

## Validation and evidence

Use the toolchain pinned in [rust-toolchain.toml](rust-toolchain.toml), locked
dependencies, and the default Bazel configuration. The [README](README.md#building)
documents the build and feature surfaces. Register new tests in the appropriate
existing Cargo/Bazel target; a source file alone does not prove test discovery.

Choose coverage by the behavior changed:

| Change | Relevant coverage |
| --- | --- |
| Bug fix | Reproduction that fails before the fix; assert the observable result or invariant through the affected path. |
| Parser, query, or optimizer | Query results as well as plans; parameters, NULL/type boundaries, errors, and relevant differential cases. |
| Statistics or cache policy | Refresh/invalidation boundaries, schema and snapshot changes, exact versus sampled/incomplete evidence, and unchanged query results. |
| Storage, constraints, or transactions | Successful and rejected operations, multi-record batches, unchanged state on rejection, and relevant WAL/checkpoint/reopen/crash boundaries. |
| Resource or performance optimization | Same workload before/after; small and bulk/skewed cases, limits, and both benefits and regressions. |
| Public API, features, or dependencies | Host-facing integration and affected default/minimal/optional-feature builds. |
| Documentation or templates only | Syntax, links, referenced commands/targets, and consistency with repository policy; explain why runtime tests are unnecessary. |

For a new fast path, test the path-selection boundary and fallback behavior.
For a resource cap, ensure the fixture actually reaches the cap and asserts its
defined failure or fallback behavior. For delta validation, compare with the
full/reference path from a valid starting state. Reuse existing test helpers
and fuzz oracles where possible. Apply recovery and concurrency checks where
the change affects those boundaries rather than adding unrelated test matrices.

Examples of focused commands (select the targets relevant to your change):

```console
bazel test //crates/storage:hawdb_storage_tests
bazel test //:hawdb_unit_fast_tests --test_arg=plan_cache
cargo test --locked -p hawdb-storage
cargo check --locked -p hawdb --no-default-features
```

With Rust Bazel tests, pass a libtest name filter using `--test_arg` and inspect
the executed/filtered test counts. A successful command that selected zero
tests is not coverage. Cargo checks do not replace the affected Bazel tests.

Routine local code verification includes the repository's fuzz regression
command, also available as `make fuzz-test`:

```console
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

Fuzz remains local-only; do not add default, dedicated, or scheduled CI fuzz
jobs. Use [the fuzz guide](crates/fuzz/README.md) for targeted campaigns and
reproduction artifacts. If a required check cannot run, record it as not run,
with the blocker and remaining risk, for the maintainer to assess. For a
documentation-only change, record documentation validation instead of claiming
runtime or fuzz coverage. After finishing with a temporary worktree, run
`bazel clean` there before removing it.

In the PR, record the tested commit (and any uncommitted test probes), exact
commands, features/profile, environment, outcome, and useful counts or logs.
Distinguish local tests, CI, source inspection, and planned checks. Include the
seed and replay command for fuzz findings. Benchmarks should name both revisions,
data shape, batch size, repetitions, cache state, durability settings, and
CPU/memory/I/O measurements as applicable; debug timings and operation counts
are supporting evidence, not interchangeable with release workload measurements.

## Review and merge

Reviewers explain findings with the trigger, violated invariant or measurable
impact, relevant source path, and a focused correction or validation request.
Inspect bounded analogous paths when they share the same cause. Separate
merge-blocking correctness, recovery, compatibility, or demonstrated resource
regressions from optional refactors and additional coverage suggestions.

Maintainers may accept an explicit tradeoff or defer a non-blocking improvement.
Record that decision and its scope in the review, and track material follow-up
work in an issue. A suggestion is not automatically a merge blocker, and an
approval does not mean all suggested improvements were implemented.

First-time external contributions may wait for a trusted maintainer's
`/ok-to-test` authorization. Do not weaken CI trust controls to start checks.
Source review, CI authorization, approval, required checks, and merge are
separate states. Follow the repository's current Prow/Tide checks and labels;
an approval does not waive required checks or establish that the PR has merged.
Recheck the current head after updates and resolve review threads only once the
change or agreed disposition actually addresses them.

For CLI submissions, prepare the body as a Markdown file and use
`gh issue create --body-file ...` or `gh pr create --base main --body-file ...`.
Check remote results with `gh pr checks <number> | cat`.
