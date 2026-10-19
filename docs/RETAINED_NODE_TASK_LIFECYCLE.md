# Task-bound retained node pulls

Issue Number: ref #1081, ref #987

## Rust host contract

`Database::query_with_params_retained_node_context` returns a
`RetainedNodeTaskCursor<'a>` borrowing a `RuntimeTaskContext`. It uses the same
indexed single-node physical plan and original immutable row owners as the
context-free API. The wrapper exposes no context-free pull or inner-cursor
escape. Every open pull checks cancellation/deadline before delivering a row
or publishing empty/post-delivery EOF. Construction checks the task before
runtime access or parsing, after parsing, before admission and after capture.
An already stopped task preserves its exact stop cause even for malformed
Cypher. Planning remains synchronous and is not interruptible inside each
parser/optimizer operation.

The seek property must have a node property unique constraint in the query's
catalog, for example `CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE`. An equality
index alone is not sufficient: without that constraint, construction returns
`UnsupportedPlan` before retained-source access or admission, even when the
lookup currently has zero or one match or requests `LIMIT 0`. A second match
under a unique constraint is `Execution(StorageIntegrity(..))`, as is an index
entry pointing to a missing row. No cursor or partial node is published.
Consumed read snapshots use their own catalog for this eligibility check.

The cursor does not clone the context, cancellation ancestry or I/O controller.
Its Rust borrow requires the host to keep the context alive through cursor use.
There is no background worker, prefetch or reserved foreground task held during
host idle time. Host-owned task/admission lifetime remains the host's contract.
Exported `RetainedNodeView` owners can outlive both cursor and context: their
independent query/runtime leases continue charging original row/catalog data
and foreign metadata until their final release.

An existing `RuntimeMemoryReservation` supplies the retained ledger's query limit.
The result account uses the smaller of configured query memory and admitted
result memory, including cursor control, source ownership and duplicate view
metadata. Every charge must fit both limits. Typed refusals identify the actual
configuration option or runtime reservation and distinguish account from query
exhaustion. Zero reservations fail, and smaller admissions are never raised
to a configured floor. Runtime retained reservations remain independently
required; an existing host reservation does not exempt these owners from
runtime admission. Conservative overlap is allowed.

## Consumed read snapshots

`DatabaseReadTransaction::into_retained_node_query` consumes a materialized
snapshot and returns `RetainedNodeSnapshotCursor`. Eligibility, indexed lookup,
row/label capture and admission use the same constructor as direct database
node delivery, with the snapshot's own catalog and indexes. A later live index,
property change or deletion cannot change the snapshot's result or turn a scan
in that older snapshot into an eligible index plan.

The wrapper owns a clone of the snapshot's optional task context and exposes
no inner cursor or context-free pull. It checkpoints before parsing, around
capture and on every open pull/EOF. Configured/admitted query and result limits
remain the same minima, and its additional wrapper metadata is pre-admitted.
Backpressure keeps the task bound; completion, terminal failure and explicit
close release the owned context. Caller-owned context/controller workspace is
not covered by this retained ledger.

Consumption drops the read transaction's reader/file-retirement pin, catalog,
relationship/index directories and planning caches after construction. The
returned cursor/view keeps only the chosen original immutable row, its original
catalog label owners, source health and independent quota leases. This does not
undo the cost of creating the read snapshot. Views can survive cursor, task and
database closure without cloning their property payload or label names.
Snapshot queries do not publish into the live database's statement recorder,
matching ordinary read-transaction observability; direct database cursors retain
their existing success/failure observation contract.
A direct database cursor closed or dropped after one delivery, before successful
EOF, records a failed statement with `Closed` rather than a completed statement.

Regressions compare original row/label addresses across a live mutation, check
immediate reader-pin release and exclusion of an unrelated large row, exercise
cancellation before delivery and at delivered/empty EOF after the caller drops
its context, preserve zero/undersized reservation refusals and completed EOF,
and refuse an old snapshot's scan after a live index is added.

## State and ownership argument

Let `S` be cursor status, `V` its initial source view and `E` exported views.
Assume the existing cancellation/deadline checkpoint and retained admission
primitives satisfy their contracts, immutable row/catalog owners preserve their
allocations, and the observer respects its synchronous callback contract.

1. Construction checkpoints the task before runtime access/parsing, before
   admission and after capture. A pre-existing stop refuses without parsing or
   binding the retained governor. Failure unwinds provisional owners and all
   leases without delivering `E`.
2. An open pull checks the borrowed task. On failure, `fail(Stopped(reason))`
   changes `S` to Failed, publishes Failed to the shared source, drops `V` and
   the health barrier, and records one failed statement. Subsequent pulls return
   the stored cause before any task/source work. Close cannot convert Failed
   into Closed or Completed.
3. Existing `E` still owns its original immutable allocations and independent
   leases. Dropping `V`, cursor, context or database cannot revoke its borrowed
   data. Its provisional outcome becomes Failed; readable bytes are not proof
   of a successful statement.
4. Successful EOF remains gated on the task/source checks and successful
   observer return. Completed repeated EOF stays Completed even if the task is
   cancelled afterward. Cancellation concurrent with a checkpoint can race;
   this is checkpoint-based cooperative cancellation, not retroactive revocation
   or an atomic transaction with the host's cancellation operation.
5. Every retained charge uses the constructor's ledger/account limits, including
   later independent view retention. The configured/admitted minimum cannot
   be widened by a duplicate view. Last-owner release returns retained quota;
   query/result admission does not prove a bound for parser/optimizer workspace,
   caller context/controller, foreign allocators or whole-process RSS.

These are source-level invariants and an induction over constructor/pull/close/
release transitions, not a machine-checked proof. Regressions cover cancelled
construction (including malformed Cypher), expired construction deadlines and
post-capture pulls, cancellation before delivery and at delivered/empty EOF,
exact sticky causes, one failed observation, original
payload addresses after all task/database owners drop, zero/undersized query
and result reservations, duplicate metadata refusal, completed EOF stability,
and final zero quotas. Runtime execution belongs to exact-head platform CI.

## Remaining scope

This is Rust task-bound point delivery, including consumed read snapshots.
Foreign C/Go/Python task binding, interruptible planning, Arrow node layouts,
SIMD, graph/FTS, multi-row/wide workloads and total-memory qualification remain
open. Existing context-free APIs remain independent of a host task. Performance
qualification, especially write/load timing on the current machine, is deferred;
no performance gain is claimed.
