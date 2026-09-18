# Root implementation extraction plan

## Scope and baseline

The embedded `hawdb` crate remains the host integration facade. Move implementations
into existing owning crates when dependencies already point inward; introduce a
new crate only for a demonstrated ownership or dependency-isolation boundary.
Do not change public facade paths, data formats, query semantics, release policy,
resource defaults, or the production control plane as part of extraction.

Baseline: `ec457ecd80c9a0470b76258d0803d5519ecfa8ec`. The `src/` tree contains
299,362 lines, including tests, developer binaries and large compatibility
fixtures. These are source-size observations, not production-code or compile-time
measurements. Representative mixed files are `api/mod.rs` (22,095),
`nowledge_mem.rs` (17,649), `store.rs` (12,850), `mem_integration_readiness.rs`
(12,382), `relational_sql.rs` (6,140), and `blackbox.rs` (1,635).

Reproduce the size inventory from that revision with:

```sh
rg --files src | xargs wc -l | sort -nr | head -30
```

The root already contains thin compatibility facades for syntax, plans,
optimization, analytics, search, QoS, and evidence. File size alone is not a
reason to move the remaining orchestration or to hide it behind another facade.

## Ordered delivery slices

| Slice | Owning boundary and approach | Entry condition | Exit and verification |
| --- | --- | --- | --- |
| Redacted blackbox evidence | Move typed reports, JSON adapters and artifact summarization into `hawdb-evidence::blackbox`; retain both historical root paths. | Only core errors, integrity, serialization and ordinary file I/O are required. | Original tests execute in the owner; root type/function compatibility, redaction, protocol, readiness and CLI consumers remain covered. |
| Relational statement compilation | Move RowPage DDL/DML lowering into `hawdb-relational`, beside strict-append compilation and scalar binding. Keep concrete transaction execution and RETURNING materialization in the root. | Compiler consumes SQL IR, parameters and `RelationalState`, not `Database` or `GraphStore`. | Owner tests cover binding, schema validation and transaction output; root transaction, SQL, recovery and differential fuzz coverage remains. |
| Remaining executor kernels | Inventory `src/executor` against existing store, memory and observer contracts; move one kernel group at a time. | The group needs no concrete store orchestration or reverse dependency on `hawdb`. | Rows, errors, memory admission, cancellation and streaming behavior agree; preserve features and fuzz. New public execution contracts remain separately reviewed. |
| Concrete store ownership | Extract inward contracts for checkpoint/statistics orchestration and transaction-private relational views before moving implementation. | WAL, recovery, snapshot identity and view lifetimes have a clear owner; storage imports no root, executor or search implementation. | Recovery/crash, rollback, lease, pinned-view and resource-bound tests retain the same lifecycle. No whole-store move just to reduce root LOC. |
| Integration and compatibility | Separate protocol models and static route/query inventory from database-running probes and host report assembly. Prefer existing evidence/readiness/route-ownership crates where ownership fits. | Extraction does not pull `Database`, `GraphStore` and the compatibility runner into a lower crate. | Full fail-closed readiness/cutover checks and fixtures remain; deleting a fixture or probe is not extraction. |

These are independent reviewable changes, not one all-at-once subsystem rewrite.
The first two slices need no new public facade contract. Later slices require
source-level dependency checks before selecting each move. Moving the complete
relational runtime or concrete store now would reproduce root dependency fan-out;
leaving everything in the root would miss already-established ownership seams.

## Shared verification gates

- Move unit tests with their implementation; retain database/host integration
  tests at the facade. A lower root test count must be explained by an equal
  owner-crate increase, never dropped coverage.
- Keep Cargo manifests/locks and Bazel dependency graphs aligned. Use default
  Bazel configuration; preserve feature forwarding and conditional imports.
- Check root public paths and concrete type identity, not just owner compilation.
- Compare mechanical portions with the baseline. Keep behavior fixes separate.
- Run focused owner/facade tests and the complete routine local fuzz surface:

```sh
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests \
  //:hawdb_linux_ci_fuzz_smoke_test
```

Do not infer performance improvement from moved lines. Compilation, local tests,
remote CI, independent review, and merge remain distinct delivery receipts.
