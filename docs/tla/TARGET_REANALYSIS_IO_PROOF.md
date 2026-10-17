# One-slot target reanalysis

`HawDBTargetReanalysisIo` checks a narrow execution protocol for a captured
document body and a stored-term cursor sharing one query I/O slot. Capturing a
source must release its I/O permit before reanalysis. A body read must release
its permit before an emitted term can cause another physical read.

## Safety argument

`CaptureEnd` returns the capture slot before entering the body/term loop.
`BodyEnd` returns its slot before entering the term callback, and `TermEnd`
returns its slot before the next body read. Digest completion and abort release
the source and cursor before execution can close. By induction over these
transitions, the next physical read never waits for an I/O slot retained by its
own enclosing operation (`NoSelfWait`). Active sources, cursors and I/O retain
execution (`ExecutionCovered`); terminal states retain none of these resources
(`TerminalReleased`).

The body/term loop allows interleavings beyond the current implementation,
which finishes document analysis before visiting the reduced terms. The two
symbolic chunks are a finite test instance, not a bound on document size or a
complete proof for arbitrary native execution.

## Source correspondence

| Protocol boundary | Implementation |
| --- | --- |
| Shared constructor operation | `validate_targets_with_admission` borrows one `ReadAdmission`; the admitted branch constructs no default task or independent ledger. |
| Bounded document probe | `probe_document_id_with_admission` reserves decode space and a range buffer; `read_file_range` validates the full object and reads the selected range under one counted scope. |
| Capture slot return | `Source::capture_with_read_admission` returns from `with_read_scope` before reanalysis starts. `payload_length` comes from the selected segment manifest. |
| Body/term slot return | `QueryFileRange::read` acquires a slot for a bounded read and releases it before returning. Source body readers and mutation cursors share this implementation. |
| Borrowed source execution | `Source<'execution>` carries the borrowed query context. The admitted source cannot outlive its permit. It is an operation-local intermediate, not a retained immutable view. |
| CJK worker lifetime | `source_retraction` uses the same task and ledger; `analyzer_workspace::run` joins the borrowed worker before returning. |
| Failure/abort | Ordinary Rust destruction releases I/O and memory. Typed query I/O errors survive UTF-8 streaming, and cancellation is rechecked at the admitted boundary. |

`ReadAdmission::io` now rejects a raw task without an I/O controller even if
the internal caller supplies a runtime permit separately. This is a partial
pairing check; matching a task and permit from the same operation remains an
internal constructor obligation. Legacy sources and ordinary reader entrypoints
retain their existing independent path.

## Executed evidence

TLC generated 41 states and found 32 distinct states with an empty queue and no
invariant failure. Both independently checked negative controls exited 12 at
`NoSelfWait`: keeping the capture slot generated 9 states / 8 distinct; keeping
the body slot generated 15 / 12. All three registered Bazel checks passed, and
the module, configurations, logs, actual exits and toolchain files were
materialized and hash-checked (21 files).

The native focused campaign passed 20 cases, including one-slot target
validation, current-task budget refusal, cancellation during selected physical
capture, unbound-task refusal before allocating a 2 MiB range, typed streaming refusal,
and checksum-valid wrong length/digest/term
retractions that preserve the published manifest. The CJK test completes capture
before reserving competing root capacity. It proves worker admission fails
before a thread starts, then releases competition and compares every emitted
term with the stored cursor. Worker TLS observation proves join and that stack
capacity remains charged through exit; all query/host/I/O grants are refunded.

The finite model excludes external wait queues, cache-opening locks, storage
latency, disk budgets and bounded cleanup. It does not prove global deadlock
freedom, fairness or wall-time progress. These native fixtures use ordinary
files, not a fully cold managed Search closure. Cache/registry heap ownership,
GC roots, cumulative physical work, the complete root factory, C-ready/E
validation, atomic activation, durability and actual Mem integration remain
open. Full regression and feature qualification are separate evidence.
