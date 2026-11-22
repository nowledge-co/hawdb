# Embedded MVCC concurrency

`HawDBConcurrentEmbedded` is a cloneable synchronous facade over one canonical
`HawDBEmbedded` owner. The C ABI uses this facade. `HawDBTokioEmbedded` shares
the same owner implementation and published-read protocol.

## Ownership

- Cypher planning, SQL template preparation and read snapshot acquisition use
  published immutable state. Ordinary reads do not wait for the mutable database
  owner after the first publication has been initialized.
- The publication mutex only protects a shared pointer. Query planning,
  execution, resource admission and host callbacks run after releasing it.
- Ordinary writers use the existing concurrent transaction kernel: private
  staging can overlap, semantic locks protect conflicting writes, and commit,
  WAL durability and publication remain ordered by the existing coordinator.
- SQL catalog/context commands and explicit host administration use exclusive
  administration. A context replacement invalidates the cached publication and
  coordinator. Prepared work cannot silently execute against a replacement.
- C ABI autocommit calls share a transaction-state read gate. Explicit transaction
  calls, backup, scrub and dump export take its exclusive side, preserving
  `transaction_open`/`no_transaction` errors and preventing an explicit transaction
  from starting in the middle of an autocommit call. Ordinary readers and writers
  share the gate and can execute concurrently.
- Ready checkpoint handoffs are adopted with a nonblocking owner acquisition;
  an occupied commit owner never prevents a cached snapshot read.
- Readiness/poison and open-user-transaction checks remain fail closed. Retained
  cursors keep their own snapshot/resource ownership after releasing the facade.
- All clones share one runtime governor. Planning reservations are retained
  through admission and released before a refused preparation is rebuilt.
  Execution permits remain alive through commit and construction of returned
  results. Retained cursors account for their independent result lifetime.
- Administrative replacement of an embedded handle or its database rebinds new
  ordinary, storage and retained work to the facade's original governor. Existing
  cursors keep their original snapshot and accounting, including cursors created
  before a replacement was adopted. Replacement does not create a second budget
  for facade work or change the Tokio scheduler's governor. The owner's retained
  governor candidate is fixed before the first retained read, so configuring an
  escaped database in another host cannot redirect future facade charges.
  When `EmbeddedGraph` adopts a database, future retained reads use a separate
  binding to the graph governor. Snapshots and cursors captured before that
  transfer continue to charge and refund their original governor, even if the
  snapshot opens its first retained cursor after the transfer. Ordinary governor
  reconfiguration does not reset an already bound retained allowance.
- A published snapshot pins schema and data, not SQL time. Each autocommit SQL
  execution and newly opened read transaction captures its own clock; repeated
  statements and internal forks within one read transaction keep its start time.
- Cold persistent admission refreshes SQL planning metadata with the admitted
  branch's template cache; read-only traffic does not retain the disabled
  pre-admission cache.

The synchronous facade exposes `QueryRequest` and `SqlQueryRequest` admission
methods. Administrative callbacks must not reenter the facade or wait for work
that needs its owner. Cold initialization and context replacement can still
require owner acquisition. This is an in-process concurrency contract; it does
not introduce multi-process writing or change the persistence format, commit
durability, or C ABI signatures. The SQL FFI path now uses facade admission too.

## Manual performance comparison

From the repository root:

```console
cargo run --locked --example embedded_mvcc_bench -j 2 -- 3 100 4096
```

Arguments are rounds, operations per worker and fixture row count. Both modes
use the same in-memory fixture, governor resources, admitted Cypher requests and
repository-wide profile. The control retains the previous whole-call
`Mutex<HawDBEmbedded>` path. The candidate uses the shared MVCC facade. Each
round alternates control/candidate order and covers one/four workers with
read-only and 90% filtered-count reads/10% disjoint point updates. Every sample checks
the exact final count of acknowledged updates. JSON lines report throughput
and read/write p50/p95/p99 latency, including contention and admission costs.

Report compilation/setup separately. Checked-profile local results do not prove
release-profile, disk-sync, FFI JSON conversion, remote CI or production speedup.
For release measurements use the same command with `--release`; do not introduce
per-target optimization settings. Deterministic held-owner regressions establish
independent progress rather than depending on benchmark timing thresholds.

The [2026-11-22 local comparison](benchmarks/embedded_mvcc_macos_2026_11_22/README.md)
records all samples, source/binary identity and both gains and regressions.
