# HawDB MVCC fuzz CLI

A standalone, local developer executable inspired by Fucci's conflict-directed
generation and independent oracle. It is a binary-only, unpublished Cargo
package with its own workspace and lockfile. The root library does not depend
on it, and the root package excludes this directory from its published archive.
It is not a test target or an automated CI campaign.

From the repository root:

```sh
cargo run --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  run --seed 42 --cases 1000 --log-directory target/mvcc-fuzz --print-report

# Routine local development: stop between cases after about 30 seconds.
cargo run --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  run --seconds 30 --explain

# Print a generated case, edit it, then inspect every step.
cargo run --quiet --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  generate --seed 42 --index 5 > /tmp/example.mvcc
cargo run --quiet --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  replay /tmp/example.mvcc --explain

cargo run --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  replay path/to/failure.json --print-report

cargo run --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  shrink path/to/failure.json --attempts 256 --print-report

bazel run //tools/mvcc-fuzz:hawdb_mvcc_fuzz -- run --seed 42 --cases 1000
```

Success is silent by default. Every completed execution writes `report.json` under a
fresh directory inside `--log-directory`. A mismatch writes the complete case
and observations to `failure.json` and stops with exit status 1. Replay writes
`replay.json`; shrink writes `reduced.json`. Invalid arguments/cases and I/O
errors return status 2. Use absolute paths for Bazel-run inputs and output
directories when artifacts must be accessible outside Bazel's run directory.
Successful shrinking still returns 1 because the reduced case reproduces the
mismatch. Existing artifacts are never overwritten. Reports identify the
runtime source checkout HEAD; they are not build attestations and can describe
an uncommitted tree.
Each saved case also has an editable `.mvcc` file and a readable `.txt` trace.
`--explain` prints SQL, bound parameters, expected/actual results and the first
failure's step; campaign output summarizes all eight families and commit orders.
`--print-report` instead prints the machine-readable JSON summary. These flags
are mutually exclusive. `generate` writes only the text case to stdout.
With `--seconds`, time starts after compilation; the tool finishes the current
bounded case before stopping. Without an explicit `--cases`, the time-bounded
campaign has a safety cap of one million cases.
Campaign reports also count each of the eight families in the order below,
split by which transaction reaches its commit/rollback first (`[0, 1]`).

## Writing a case

No JSON schedule bookkeeping is needed. Each `T1`/`T2` line is the next
scheduled operation; both snapshots are implicitly captured before the first
line. The oracle computes expected rows and commit outcomes independently:

```text
# T2 must conflict even though T1 restores the original value.
ROW 1 10

T1 UPDATE 1 99
T1 UPDATE 1 10
T2 UPDATE 1 20
T1 COMMIT
T2 READ
T2 COMMIT
```

Supported commands are `ROW <id> <value>`, `T1 READ`, `T1 UPDATE <id> <value>`,
`T1 INSERT <id> <value>`, `T1 DELETE <id>`,
`T1 BROAD_UPDATE <minimum-value> <replacement-value>`, `T1 COMMIT` and
`T1 ROLLBACK`, with the same operations for `T2`. Fixtures precede transaction
steps; each transaction must have exactly one terminal. Keywords are case
insensitive, values are signed 64-bit integers, and `#` introduces a comment.
Optional `SEED` and `INDEX` metadata preserve generator provenance. Syntax errors include line numbers. Arbitrary SQL is intentionally outside this
small operation grammar so the reference interpreter stays independent.

Ready-to-edit examples are in `cases/`. For example:

```sh
cargo run --quiet --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  replay tools/mvcc-fuzz/cases/same-key.mvcc --explain
```

## Scope and oracle

Both optimistic transactions capture their snapshots before any scheduled
operation. A generated schedule preserves each transaction's program order
while interleaving SQL reads, mutations and commit/rollback. This deliberately
controls statement order; it does not schedule machine instructions, group
sync, simultaneous commit threads or internal lock races.

Eight families cycle through same-key updates, disjoint keys, net-zero writes,
absent-key deletion versus insertion, delete/reinsert, broad-versus-narrow
writers, duplicate concurrent inserts and rollback. Seeds vary fixture values,
keys and interleavings, including both commit directions. Reads after each
mutation check the private snapshot; independent autocommit reads check visible
data after every step; a retained read transaction checks the original snapshot.

The reference interpreter uses typed generated operations and full committed
intent history, never the production collector, SQL evaluator or VersionIndex.
Absent/no-op writes retain primary-key intent. A non-primary-key predicate
write is conservatively broad. A stale writer conflicts when an earlier
successful commit overlaps its footprint or either writer is broad. Read-only
commits and rollback publish no intents. Full typed `(id, value)` rows are
compared, including rejection of duplicate primary keys or unexpected columns.
No general Serializable or read-set-validation claim follows: SI write skew
is outside the failure criterion.

Cases are versioned JSON containing `initial`, two `transactions` and a
`schedule` of transaction IDs (0 or 1). Each scheduled ID consumes the next
operation, or the commit/rollback after its final operation. Replay accepts
either a bare case or an artifact containing `case`. Input is limited to 16 MiB,
64 initial rows and 128 schedule entries. Programs must be valid in their own
private snapshot; malformed inputs are rejected before execution.

Reduction deletes operations and corresponding schedule entries, removes
fixture rows, and simplifies values to zero. Only valid candidates reproducing
the same semantic failure class are accepted, within an attempt budget. Engine
errors and panics are retained as failures but are not automatically reduced.
The reduced result is locally minimal only under the attempted transformations;
the tool does not promise a globally minimal reproducer.

Not yet covered in optimistic mode: pessimistic locks/deadlocks (see
pessimistic mode below), three or more transactions, graph/append writes,
constraints beyond the primary key, publication dependency tokens, persistent
reopen, crash/power-loss recovery, checkpoint/pruning races, or resource
rejection classification. Keep those separate from the first campaign's
acceptance claims.

## Pessimistic mode

`--mode pessimistic` (for `run` and `generate`; saved cases carry
`MODE PESSIMISTIC`) runs the same one-table schema plus one `Record` graph node
per fixture row under two pessimistic `ConcurrentDatabase` transactions. Each
transaction runs on its own worker thread, so a statement that waits for a
lock is observable. The scheduler sends each step and either requires an answer
within 10 seconds or, when the model predicts a wait, requires no answer within
200 ms. A schedule entry for a waiting transaction is deferred until the wait
ends, preserving each transaction's program order. The lock timeout is 30
seconds, so a timeout is a harness error rather than an expected outcome.

```sh
cargo run --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  run --mode pessimistic --seconds 30 --explain
cargo run --quiet --locked --manifest-path tools/mvcc-fuzz/Cargo.toml -- \
  replay tools/mvcc-fuzz/cases/pessimistic-upgrade-deadlock.mvcc --explain
make mvcc-fuzz MVCC_FUZZ_ARGS="--mode pessimistic"
```

Additional commands: `T1 READ_FOR_SHARE`, `T1 READ_FOR_UPDATE` (whole-table
`SELECT ... FOR SHARE|UPDATE`), `T1 GRAPH_READ` and `T1 GRAPH_UPDATE <id>
<value>` (Cypher `SET` of one `Record` node), with the same forms for `T2`.

The independent model encodes the documented pessimistic contract with the
lock spans of the SQL lock derivation for this schema: plain reads take no
lock; locking reads take a shared or exclusive full primary-key range;
primary-key `UPDATE`, `DELETE` and `INSERT` take an exclusive point; a non-key
predicate write takes an exclusive table lock; a Cypher property write of an
existing node takes an exclusive node lock. A request waits for a conflicting
lock held by, or a conflicting request queued by, the other transaction. The
waiter whose new wait closes a cycle is the deadlock victim and is aborted. A
newly granted, previously uncovered SQL lock refreshes a stale snapshot when
the transaction has no successful statement yet and otherwise aborts with the
retryable serialization error; a graph statement aborts on any stale new lock.
Commit rebases the successful writes onto current visible state.

Every step checks the statement or terminal outcome class (rows, success,
duplicate primary key, serialization abort, deadlock victim, call on an
aborted transaction), visible table rows and graph nodes through autocommit
reads, and the pinned initial snapshot. Eleven families cycle through same-key
waits, disjoint keys, duplicate inserts, table versus point writers, a shared
upgrade deadlock, a locking range versus insert, delete versus delete/insert,
rollback release, same and disjoint graph nodes, and a graph/SQL deadlock. An
optional leading plain read decides whether a later wait refreshes or aborts.

Pessimistic mode does not cover lock timeouts, three or more transactions,
relationship or label locks, uniqueness constraints on graph labels, Strict
Append, persistent reopen or crash recovery, and it does not claim
Serializable isolation.

## Development

The root workspace's formatting and lint checks do not discover this independent
package. Check it explicitly:

```sh
cargo fmt --manifest-path tools/mvcc-fuzz/Cargo.toml -- --check
cargo clippy --locked --manifest-path tools/mvcc-fuzz/Cargo.toml --all-targets -- -D warnings
```

The Bazel target is a manual `rust_binary` using existing dependencies; it is
not registered in test suites or CI. Root workspace membership, release publishing
and library features remain independent of this tool.
