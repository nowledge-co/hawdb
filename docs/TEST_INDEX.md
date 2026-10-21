# Test ownership and known slow paths

Use this index to select an existing regression owner and understand what a
slow target is checking. Target definitions and CI job configuration remain
authoritative; this is a map of test families, not an exhaustive list of every
Rust test function. Update it when changing a family's scope, build profile,
execution lane, fixture size, isolation or known failure mode.

## Execution lanes

| Lane / entry point | Problems covered | Execution policy |
| --- | --- | --- |
| Prow root: `//:hawdb_unit_tests`, `//:hawdb_system_sql_loom_tests`, `//:hawdb_linux_ci_smoke_tests`, CLI/shadow and package-parity targets | Embedded facade behavior, concurrent publication, recovery, public consumers, compatibility protocols and benchmark evidence | Required PR and mainline regression coverage. Fuzz is excluded. |
| Prow crates: `//:hawdb_presubmit_crate_tests` | Internal component contracts and exact Search feature profiles | Required checked-profile component coverage (the release-only observer target runs in the native lane). Each package owns `:presubmit_tests`; manual-only targets must not accidentally enter implicit suite discovery. |
| Prow dense analytics: `//:hawdb_unit_analytics_rss_tests` | Persistent graph identity admission, edge streaming and bounded incremental RSS in a fresh process | Separate exclusive target; dense fixture construction is outside the measured child's RSS baseline. |
| Prow storage crash recovery: `//:hawdb_storage_crash_recovery_tests` | Subprocess termination at persistence boundaries, complete transaction recovery, endpoint integrity and artifact generations | Dedicated job binds evidence to source/toolchain/features. Process termination does not alone establish power-loss safety. |
| Native bindings: `//bindings/ffi:hawdb_ffi_tests`, `//bindings/go:hawdb_go_tests`, `//bindings/python/...` | C ABI retention, host ownership/errors, Go integration/race checks and Python behavior | Binding-specific PR/mainline workflows; Windows contracts and scheduled qualification are distinct receipts. See workflow definitions for current triggers. |
| Weekly platform and daily Windows abandonment qualification | Supported platform/feature combinations and the complete Windows abandonment/GC fault matrix | Scheduled or explicit workflows. They are not fuzz time budgets. |
| Local Shiro and MVCC tools | Generated SQL/Cypher result relations, parser/storage campaigns and optimistic transaction-history checking | Manual only. Never include the tools in normal or dedicated CI, scheduled workflows or production dependencies. |

## Root regression owners

| Target / family | What it detects | Notes |
| --- | --- | --- |
| `//:hawdb_unit_general_tests` | Embedded API, schema/query and general facade regressions | Shared unit executable; partition script assigns each discovered case an owner. |
| `//:hawdb_unit_concurrent_tests` | Transaction locks, read observation, commit admission and concurrent publication races | Known long target: [#972](https://github.com/nowledge-co/hawdb/issues/972). Preserve barriers and observations; a deadline is not a substitute for a deterministic schedule. |
| `//:hawdb_unit_analytics_core_general_tests` | Graph algorithms, projected artifacts, identity and generation behavior | Separate from isolated dense RSS ownership. |
| `//:hawdb_unit_analytics_rss_tests` | Dense persistent graph algorithms with bounded reader RSS and streamed adjacency | 128 nodes / 16,256 relationships. Known cost: [#1027](https://github.com/nowledge-co/hawdb/issues/1027). Uses the same repository test profile as every other root partition. |
| `//:hawdb_unit_recovery_tests`, `//:hawdb_unit_storage_crash_matrix_tests` | Transaction persistence, checkpoint/compaction and recovery boundary regressions | Dedicated crash-job evidence has additional identity and completeness checks. |
| `//:hawdb_unit_row_page_compaction_{materialized,out_of_core}_tests` | Physical row-page reclamation while pinned readers retain old generations; correct reopen/scrub in both residency modes | Full 16-table churn fixture and physical-byte assertions. Repository-wide checked test profile. Known long target: [#889](https://github.com/nowledge-co/hawdb/issues/889). |
| `//:hawdb_unit_row_page_compaction_admission_tests` | Budget refusal, retryability, failpoints and admission release around compaction | Ordinary compilation; separate from the two full convergence cases. |
| `//:hawdb_mvcc_joined_reads{,_minimal}_tests` | Joined-read dependencies, absent/filtered parents, off-page children, key bindings and sticky errors | Full and minimal feature contracts; fixed scenario matrix rather than timed fuzz. |
| `//:hawdb_mvcc_projection_publication{,_minimal}_tests` | Projection publication versus concurrent reads and transaction validation | Preserve publication barriers and complete observation evidence. |
| `//:hawdb_mvcc_staging_session{,_minimal}_tests` | Fixed-E graph/SQL mapping, complete-E phantom/delete/content guards, disjoint-write rejection, append/projection isolation, cancellation and reopen | Root CI; one-item batches and full-rebuild equality. Final artifact construction is blocking. See [source-coherent catch-up](specs/SOURCE_COHERENT_SEARCH_CATCH_UP_SPEC.md). |
| `//:hawdb_mvcc_full_source{,_minimal}_tests`, `//:hawdb_mvcc_baseline_comparison{,_minimal}_tests` | Exhaustive pinned source enumeration, duplicate IDs, full set/content comparison and full-rebuild reference | Root CI; small deterministic hot/cold fixtures. Complete-E verification adds a full-source pass and conservatively rejects every post-E write. |
| `//:hawdb_branch_project_open{,_minimal}_tests` and root branch lifecycle units | Existing-project open, recovery, leases and resource ownership | Includes V2 project selectors, exactly one retained metadata directory handle, FD saturation/export retry, pending-child recovery refusal at four descriptors followed by completion at six, and a mid-operation cold branch refusal at 32 descriptors with unchanged catalog/no child, restored FD counters and successful higher-budget retry. Native FD scaling runs in an isolated subprocess and verifies return to baseline after closure. Persistent fixture cost is distinct from randomized exploration. |
| `//:hawdb_execution_request_contract_tests`, process-memory/search-consumer contracts | Request budgets, refusal behavior, projection generation publication and host resource policy | Preserve exact error and readiness assertions. |
| Registered stationary Consumer unit family (`cargo test -p hawdb --all-features --lib api::search_projection_consumer::`) and `//:hawdb_search_projection_consumer{,_minimal}_tests` | Native creation/catch-up/reopen, immutable old pins, completed receipts, revocation, disposable-cache recovery, transient IO retry and allocation capacity | Includes 260 actual catch-ups with an old pin, 256 unresolved rows with typed exhaustion, and transient retry through both direct open and pending-publication `open_or_create` startup recovery without reinitialization. The long capacity fixture suspends automatic checkpoint publication; checkpoint concurrency remains covered separately. Root checked-profile lane plus Unix native binding targets; positive `registered_stationary` and `_1043_` selections execute in the Windows native contract lane. [Protocol, limits and remaining gates](CONSUMER_STATIONARY_PUBLICATION.md). |
| Storage source-sync and captured-WAL units | Pre-effect FD refusal versus uncertain durability failure | Source-barrier refusal preserves the live handle and retries; captured WAL append prepays native validation handles before writing even when a competing owner fills remaining capacity. The latter also executes explicitly in the native Windows contract lane. |
| Default/minimal/graph-text consumer, host allocator and portable tests | Public feature boundaries, allocator ownership, in-memory portability and excluded native dependencies | Do not replace exact minimal-feature compilation with a default-feature run. |
| `//:hawdb_system_sql_loom_tests` | Bounded modeled system-table publication interleavings | Model exploration; not a wall-clock fuzz campaign. |
| Package parity, query-limit inventory and root-partition script tests | Missing package ownership, unbudgeted query routes, duplicate or omitted test partition selection | Keep these structural checks when moving targets. |

Brace notation describes several concrete labels; expand it before invoking
Bazel. A family description does not imply every leaf belongs to every lane.

## Component regression owners

| Component / `:presubmit_tests` family | Problems covered |
| --- | --- |
| Core, expression, SQL/SQL-syntax, Cypher and DDL | Typed values, expression semantics, parsing/binding and schema constraints. |
| Optimizer, optimizer-{graph,relational,predicate,vector}, plan-{core,cache,cypher}, explain | Rewrite legality, plan construction, cache/publication concurrency and observable plans. |
| Executor and query/query-policy | Operator execution, result delivery, cancellation and budget/refusal behavior. Allocation probes use the repository profile; compare measurements only within the same profile. |
| Storage, integrity, cascades and artifact/bootstrap | WAL/checkpoint, encoded artifacts, indexes, corruption/refusal and startup ownership. Projection capture covers 65,537-node commits, oversized-preimage gaps, cold node/adjacency/relationship/spill FD exhaustion and retry, nested resource/cancellation refusals versus physical corruption across canonical readers, and cancellation across grouped replay work. |
| Search | Unit behavior plus nine explicit feature profiles; public streamed-document and source-policy lifecycle fixtures have independent owners below. |
| Analytics and vector-projection | Streaming algorithms, projected artifact IO and vector representation behavior. |
| QoS, cgroup, telemetry, resource-profile and runtime-tokio | Resource detection/admission, metrics and synchronous/async runtime boundaries. |
| Readiness, evidence, route-ownership, nowledge-contracts, compat and qualification | Typed readiness, complete evidence identity, compatibility and host integration contracts. |
| `crates/fuzz-contracts` | Production readiness contract values only. Its fast unit tests are not the Shiro generator/campaign. |

## Search feature and lifecycle matrix

See [SEARCH_FEATURE_TESTS.md](SEARCH_FEATURE_TESTS.md) for feature-specific
expectations. The concrete profile set is default, acl, minimal, text_only,
vector_only, background_only, text_vector, text_background and vector_background.

| Target pattern | What it detects | Cost and known issues |
| --- | --- | --- |
| `//crates/search:hawdb_search_tests` and `hawdb_search_<profile>_tests` | Search unit, persistence/recovery and feature capability behavior | Large lifecycle fixtures include 128-MiB sources. Repository test profile; the full matrix still runs. Existing deadlines remain unchanged. |
| `hawdb_search_source_policy_tests` and `hawdb_search_<profile>_source_policy_tests` | Inherited source/token admission policy through build, replacement, compaction, deletion and reopen; valid over-default-budget reader bodies | Million-token boundaries intentionally remain large. Five FTS profiles previously exceeded 900s: [#1034](https://github.com/nowledge-co/hawdb/issues/1034). Non-FTS decomposition: [#872](https://github.com/nowledge-co/hawdb/issues/872). |
| `hawdb_search_streamed_lifecycle_tests` and FTS profile variants | 32-MiB streamed bodies through projection update, replay, compaction/retraction and budget refusal | Five complete FTS feature profiles. Reuse the same feature-profile library; compilation is uniform across the repo. |
| Allocation-specific integration targets | Identifier, descriptor, segment, analyzer and related allocation accounting | Uses the same repository profile; preserve measurement assumptions and compare allocation evidence after profile changes. |

## CI benchmark smoke owners

`//:hawdb_linux_ci_smoke_tests` includes correctness/protocol checks and
benchmark evidence. The following benchmark groups remain `exclusive`, so
Bazel executes them sequentially after ordinary tests in the invocation.
Do not remove isolation merely to reduce wall time: relative latency and RSS
measurements can become unreliable under competing fixtures.

| Target / group | What it checks | Current execution |
| --- | --- | --- |
| `optimizer_summary`, `optimizer_group_1`, `optimizer_group_3` smoke targets | Optimizer/report evidence plus join, aggregate, lookup, vectorization and storage/search benchmark protocols | Existing complete workloads through the repository test profile. |
| `hawdb_linux_ci_benchmark_<name>_smoke_test` | index_restart_cost, integrity_checksum, relational_index_access, relational_index_page_cache, relational_monotonic_append and relational_oltp_mix | Separate targets keep the original finite deadlines and exclusive isolation. |
| `hawdb_linux_ci_wal_group_commit_smoke_test` | Concurrent/single-writer commit behavior, group-sync counts, paired latency policy evidence and complete reopen/recovery | Four paired workloads, 11 rounds each. Fixed work, not a requested run duration. |
| Fixture, storage recovery, background maintenance, previous-wrapper and migration-gate smokes | Protocol shape, recovery, compatibility evidence and fail-closed readiness | Existing semantic gates are preserved. |

There are no CI-specific optimized benchmark wrappers or target-local
optimization flags. Benchmarks used by test smokes follow the same checked test
profile as other targets. To explicitly verify release behavior, use
`bazel test --config=release ...` or `bazel run --config=release ...`. The legacy
`hawdb_bench_relational_index_access_release` label is a compatibility alias;
it no longer changes compilation mode by itself.

## Repository-wide compilation profiles

Cargo loads `.cargo/config.toml` from the repository hierarchy, covering the root
workspace, independent tools, bindings and the parser experiment. Run Cargo from
within the repository (including when using `--manifest-path`); callers outside
this directory hierarchy must explicitly supply the config. Published library
consumers still own their host application's profiles. External-consumer
measurements explicitly copy this config into their temporary host and record
its digest, so moving the host outside the repo does not silently lose the
selected release policy.

| Setting | Cargo dev | Cargo test / default Bazel | Cargo release/bench; explicit Bazel release verification |
| --- | --- | --- | --- |
| Optimization | 1 | 1 | 3 |
| Debug information | 0 | 0 | 0 |
| Codegen units | 256 | 16 | 1 |
| Cross-crate LTO | Disabled | Disabled | Thin |
| Debug assertions | Enabled | Enabled | Disabled |
| Integer overflow checks | Enabled | Enabled | Disabled |
| Symbol stripping | None | None | None |

Cargo incremental compilation remains enabled for dev/test and disabled for
release/bench. Bazel uses its action cache rather than Cargo incremental state.
Host build tools and platform linker/ABI requirements follow their toolchains;
they are not special optimization profiles for individual regression targets.
Bazel is primarily a test runner; release is an explicit regression-verification
profile, not a production artifact publishing path.

The ignored executor regression
`observer::query::tests::query_observer_registration_without_debug_assertions`
is explicitly selected by `//crates/executor:hawdb_query_observer_release_tests`
under `--config=release` in the required Linux native-bindings job. Its target
is manual so the ordinary checked crate suite cannot run it in the wrong mode.
Its assertion remains intact; moving its execution does not waive coverage.
Tide gates on the `Bazel native bindings (ubuntu-24.04)` context: the current
[context policy](https://github.com/nowledge-co/test-infra/blob/5a7b3995d384ed4b3f3ba576fe6d06cd700f27e6/config/prow/cluster/config.yaml#L108)
marks only Windows native contracts optional for HawDB and does not skip unknown
contexts. Preserve this merge-gate ownership when changing workflow or Tide policy.

The policy follows the distinction illustrated by [TiKV's Cargo profiles](https://github.com/tikv/tikv/blob/master/Cargo.toml):
cheap development/test code generation with assertions, versus explicit optimized
release code generation. TiKV's dev/test optimization is 0; HawDB chooses 1 to
reduce measured unoptimized-loop overhead without the compile cost of level 2.
No per-package optimization exceptions are copied from TiKV.

## Recent slow-run evidence and interpretation

Complete PR #1145 BEP on 2026-10-21 recorded:

| Lane | Bazel elapsed | Notable executed targets |
| --- | --- | --- |
| [Root](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1145/pull-skein-bazel-test-root/2112766656664047616) | 1565.6s | Compaction 445.2/444.2s; WAL benchmark 376.1s; optimizer group 1 229.1s. Exclusive benchmark execution adds about 864s sequentially. |
| [Crates](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1145/pull-skein-bazel-test-crates/2112766656974426112) | About 1218s | FTS source-policy 637-655s; streamed lifecycle 433-455s; heavy unit profiles 420-590s. |
| [Dense analytics](https://prow.nowledge.co/view/gs/ci-cd-503413-prow-public/artifacts/native/pr-logs/pull/nowledge-co_hawdb/1145/pull-skein-bazel-test-analytics-dense/2112766656806653952) | 798.0s | Isolated RSS target 551.6s; TestRunner consumed about 551 CPU seconds, indicating actual computation rather than a timed sleep. |

The initial opt-level=2 experiment on macOS arm64 changed complete materialized
compaction from 71.947s to 16.756s and default source-policy from 89.424s to
11.275s. Its first optimized invocation cost about 171s versus 111s for the
baseline due to new compile actions. That target-specific implementation was
removed after adopting repository-wide profiles; those numbers are exploratory
evidence, not results of the final level-1 profile. Obtain final-head BEP to
judge execution savings together with cold-build cost.

The final uniform level-1 profile completed the same materialized compaction
case in 21.991s and default source-policy fixture in 15.920s. Both targets
executed, with local compilation concurrency limited to four after observing
host memory pressure. The first level-1 invocation cost about 126s including
new compilation versus about 111s for the original baseline; this remains
local evidence, not a remote CI speedup claim.

Bazel `long`/`eternal` timeouts are maximum allowed execution times. They do not
make tests run for that duration. Cache-hit BEP can retain the original result's
recorded test duration: check cached-result counts before interpreting it as
new execution. Compare exact source, runner, features, fixture size, execution
versus cache hits, cold build cost and required target inventory.

## Local fuzz and reproductions

- Shiro: `make shiro-fuzz` runs one worker continuously until interrupted or an
  oracle fails; `SHIRO_SECONDS=30` adds a local time budget. Explicit `--cases`
  and `--case-index` support finite verification/replay. See
  [tools/shiro](../tools/shiro/README.md).
- MVCC: `make mvcc-fuzz` defaults to 30 seconds of local transaction histories.
  Generate editable cases, replay failures and shrink them before introducing a
  focused deterministic regression. See [tools/mvcc-fuzz](../tools/mvcc-fuzz/README.md).
- Local deterministic fuzz verification:
  `bazel test //tools/shiro:shiro_tests //tools/shiro:shiro_cli_tests //:hawdb_linux_ci_fuzz_smoke_test`.
  Historical `//crates/fuzz:*` labels are compatibility wrappers only.
- A fuzz discovery belongs in CI as a small confirmed regression, not by adding
  the continuous tool or arbitrary duration-based exploration to CI.
