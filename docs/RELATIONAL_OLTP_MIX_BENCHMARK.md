# Relational OLTP Mix Benchmark

`cargo bench --bench relational_oltp_mix` records the TP-shaped baseline the
columnar canonical contract must not regress
([`specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md`](specs/COLUMNAR_CANONICAL_AND_PROJECTION_SPEC.md)
§10: point read/write p99 parity, scans strictly better, and the same mix
re-run under a concurrent background analytics job within 2× of the quiet
baseline).

## Shape

The real Nowledge Mem content-store schema (`content_documents`,
`thread_messages`, the order and space secondary indexes), 200 threads ×
250 messages = 50,000 rows loaded in batched transactions, checkpointed, and
reopened so reads pay published-artifact costs. The mixed phase runs 4,000
operations from a deterministic generator: 70% primary-key point reads, 10%
ordered `LIMIT 50` pagination, 15% single-message insert transactions, 5%
content updates. Every mutation commits its own transaction because
per-request durability is the workload's truth. The scan/aggregate class
(space `COUNT(*)`, per-thread `GROUP BY` rollup) is sampled separately.

The rollup deliberately cannot yet take its production form — the current
aggregate subset rejects `ORDER BY`, so the "top threads by message count"
query is expressed without the ordering. Closing that subset gap is phase 4
scope of the columnar contract.

## Baseline — current row-oriented engine

Recorded 2026-08-12 on the pre-columnar engine (branch
`feat/columnar-canonical-redesign`, macOS/arm64, release profile), 50,000
rows:

| Operation class | ops | p50 | p95 | p99 |
| --- | --- | --- | --- | --- |
| Point read (PK) | 2,781 | 16 µs | 50 µs | 64 µs |
| Page read (`ORDER BY … LIMIT 50`) | 408 | 346 µs | 421 µs | 459 µs |
| Insert (own transaction, fsync) | 598 | 5.16 ms | 6.35 ms | 7.65 ms |
| Update (own transaction, fsync) | 213 | 7.95 ms | ~13 ms | 13.2 ms |
| Space `COUNT(*)` | 21 | 1.65 ms | 1.76 ms | 1.76 ms |
| Thread rollup (`GROUP BY` over 50k rows) | 21 | 43.8 ms | 45.2 ms | 45.2 ms |

Reading the baseline against the contract's promises:

- Point reads and mutations are the parity target: mutations are
  fsync-bound, which is why the columnar write path must not add work to
  the commit path (§5.1).
- The 43.8 ms rollup against 16 µs point reads is the AP tail the columnar
  representation exists to remove: the rollup touches every row's full
  record today, while a columnar scan reads two chunks per group and the
  space count collapses to group metadata (§3.2).

Numbers are single-machine medians for trend tracking, not representative
Mem-replica qualification; production admission still follows
`PRODUCTION_READINESS_SPEC.md`.
