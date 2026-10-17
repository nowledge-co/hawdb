# Borrowed query cursor safety

`HawDBBorrowedQueryCursor.tla` models a serving cursor over an immutable view
whose producer may close independently. A task may survive its runtime permit;
the task's continued existence does not authorize new serving work. Each cursor
borrows the serving permit, and each I/O scope ends before its cursor closes.

## State and invariants

Each of two single-use query IDs transitions from `new` to `active`, then
`taskOnly`, then `closed`. The immutable view is assumed to remain alive.
Producer closure is independent of every query transition. At most one modeled
I/O scope is active, matching the native fixture's one-slot request.

- `CursorBorrowed`: each open cursor's query remains active.
- `IoBorrowed`: each active I/O scope belongs to an open cursor and active query.
- `NoCancelledDelivery`: cancellation before the modeled completion checkpoint
  prevents delivery, including cancellation after I/O begins.

These invariants follow inductively: `OpenCursor` requires active execution;
`ClosePermit` requires no cursor; `BeginIo` requires a cursor; `CloseCursor`
requires no I/O; and `CompleteIo` checks cancellation before delivery. Other
actions preserve these conditions. Cancellation after successful delivery is
outside that completed operation; the model does not retract past results.

## Implementation correspondence and assumptions

| Model boundary | Rust boundary |
| --- | --- |
| `OpenCursor` | `Terms::cursor_with_query_context` rejects an admitted file when execution is absent, before reserving/allocating its buffer. |
| Cursor lifetime | `Cursor<'a>` borrows `Terms` and `QueryContext<'a>`; the context borrows `RuntimePermit`. Safe Rust cannot drop that permit while using the cursor. |
| `BeginIo` / `CompleteIo` | Shared `build_memory::read::query::read_at` acquires an I/O guard from the current task, uses `File::with_admitted_read`, then rechecks cancellation before accepting the result. Mutation ranges delegate to it. |
| Cancellation on buffered data | `Cursor::next` checks the current task even when no physical read is necessary. |
| Independent producer closure | `TermFile` retains memory receipts and the file, with no producer task/permit; admitted decoded `Terms` retain no writer `BuildMemory`. |

This correspondence assumes the internal constructor pairs the task, accounts
and borrowed permit from the same admitted operation. The model does not prove
that pairing or Rust's borrow checker. Legacy optional contexts remain supported
for ordinary readers; this model describes the admitted mutation-term path.
The complete governed reader and target-validation propagation remain unfinished.

## Executed finite checks

On macOS arm64, TLC generated 790 states and found 234 distinct states with an
empty queue and no invariant failure. All four registered Bazel checks passed.
The three negative controls each exited 12 at its intended invariant:

| Control | Broken rule | Invariant | Generated / distinct states |
| --- | --- | --- | --- |
| `ReuseTask` | Create a cursor after the permit closes using its surviving task. | `CursorBorrowed` | 304 / 132 |
| `DropBorrow` | Close execution while a cursor still borrows it. | `CursorBorrowed` | 160 / 91 |
| `SkipCheckpoint` | Deliver I/O after cancellation without the completion checkpoint. | `NoCancelledDelivery` | 571 / 222 |

Module/config bytes, logs, actual exit codes and toolchain evidence were
materialized and hash-checked for all four checks. Native tests separately cover
task-only refusal, fresh execution after producer closure, cloned term custody
after parent destruction, cancellation and full measured-heap reclamation after
all fixture/project owners close. The fixture explicitly owns its project;
the general test helper retains another project in TLS until thread exit.

This is a finite safety check of execution custody. It proves no fairness,
cancellation wall-time bound, cumulative physical I/O bound, cache/registry heap
coverage, file immutability, GC source custody, MVCC conflict validation,
activation or power-loss property. Memory and native-descriptor ownership have
separate bounded models and still require complete native closure qualification.
