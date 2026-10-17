# Resumable private mutation ordering

This refines the [external ordering component](SEARCH_UNORDERED_MUTATIONS_PROOF.md)
of #231/#232/#1043. It does not complete the artifact-owning C..E session,
stage-bound coverage/receipt authority, actual Mem migration, incremental final
index construction, or the original recovery/resource/performance gates.

## Ownership and public protocol

`queue_upsert` and `queue_delete` admit one input without sort/merge filesystem
I/O. Structural measurement and owned-document construction precede the sort
quantum; document/header configuration and the shared task ledger bound their
separate admission. A full buffer retains one admitted pending input. The caller
must advance until `ready_for_input` before offering another input; busy/closed
intake refuses permanently instead of silently discarding an operation.
`finish_input` closes intake, including while that pending input is retained.

`advance_ordering` admits work, logical read bytes and logical spill bytes for
one quantum. Work charges state transitions and the maximum byte comparisons
of heap/frontier/reduction keys. Frame validation (including the reader's
previous-key order check), exact body comparison and encoding are separately
bounded by their admitted read/write extents and existing
header/metadata/record limits. All cumulative counters remain live across
quanta; yielding resets only the step counters. A complete atomic record or
comparison must fit a whole quantum. Otherwise the operation refuses rather
than repeatedly yielding with zero progress. This is a record-granular state
machine; it does not split one body comparison into arbitrary byte quanta.

`ordering_complete` means a complete private sorted/reduced run exists. It is
not a Search generation, source coverage proof or publication watermark.
`prepare_publication` subsequently drives any remaining ordering and performs
base retraction reads, ordered-spool transfer, index construction and artifact
sealing. Those final phases remain blocking and outside the sort quantum.
The existing blocking upsert/delete helpers drive the same continuation states.

## Source-linked transition argument

- `unordered/heap.rs` replaces a whole-buffer sort with a min-heap continuation.
  During heap construction, previously built child subtrees are heaps. A sift
  retains its root and selected child across yields. Extraction moves the
  minimum to the suffix, preserving a descending sorted suffix whose keys are
  no greater than remaining heap keys. Swaps preserve the multiset. Completion
  therefore leaves descending storage; popping produces ascending run order.
  Comparison admission precedes every string comparison. No caller can mutate
  the private buffer while that continuation is active.
- `unordered/ordering.rs` retains buffer-write position, binary-carry level,
  one merge, seal scan position, reduction winner/current input and private
  output. Every I/O or metadata transition first checks its complete operation
  against the remaining quantum. Yield preserves that transition's state.
  Closing intake does not replace the pending input or reset the carry level.
- `unordered/run.rs::Merge` opens both inputs/output in separate transitions.
  Each reader inspects a bounded prefix, retains its full-record read cost,
  then validates/owns the record only after body admission. Prefix inspection
  survives a yield. The ordinary decoder independently validates the frame.
  Loaded frontiers remain owned until an admitted comparison selects one and
  its complete copy succeeds. Exhaustion checks both input count/digests.
  Output flush and both consumed-file removals precede returning the new run.
- Final reduction separates ID classification, exact equal-ID comparison and
  output copy. A winner and next input remain owned across budget boundaries.
  The reduced run is returned only after complete input drain, output flush
  and input-file removal. This adds one private reduced-run write/read pass;
  cumulative spill/read admission includes it. It trades I/O for an explicit
  resumable boundary before the still-blocking final artifact build.
- `Control::allow` checks feasibility before charging work or executing the
  transition. Actual read/write helpers enforce the admitted step bounds again.
  A defensive preflight mismatch refuses before an oversized read/write.
  Cumulative work, visited records and bytes never reset/refund on yield.
  Public steps set failure sticky before entry, including cancellation/panic.
  Continuation files/entries drop before the enclosing stage cleanup owner.

These are bounds on admitted logical work/I/O, not wall-clock scheduling,
physical device bytes, page-cache/RSS or arbitrary syscall duration. BufWriter
may physically flush bytes admitted in a previous quantum; the fixed admitted
buffer remains owned until flush/close. Path encoding and filesystem metadata
are governed by existing task/path/descriptor admission, not numeric I/O byte
counters. Cleanup failures retain the existing stage-cleanup contract. There
is no new claim of power-loss safety or durable private-work resumption.

## Finite model and negative controls

`HawDBSearchSortQuantum` refines pairwise merge into open, peek, load, choose,
copy, flush and input-removal phases. Four distinct record identities have two
business keys; each input is independently sorted. Varying record width and
work/read/write caps exercises both infeasibility and successful deferral.
Loaded indices must equal output plus retained frontiers, without duplicate
ownership. A full cost oracle checks that all logical work/I/O is charged;
done requires the exact complete merge, output flush and both removals.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9, pinned jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`:
**2,118 generated / 2,076 distinct states**, depth 56, no invariant failure.
Eight controls skip an input on yield, discard a retained prefix, reset totals,
refund reads, omit comparison work, admit too late, return a run early, or
revive a failed attempt. Each returns exit 12 for its exact named invariant.
CompletionWitness and DeferredCompletionWitness also return exit 12, ensuring
successful merge and successful merge after deferral are reachable. Parser or
incomplete-transition errors are not counted as evidence; final checks use the
fully parenthesized transition definitions.

The model abstracts exact encodings/CRC, concrete heap and merge loops, Rust
aliasing, filesystem behavior, arbitrary widths/record counts and cancellation
inside a syscall. It is a selected finite refinement, not a mechanized proof of
all Rust code. The earlier membership/reduction model remains complementary;
neither proves actual mapping completeness or a batch coverage token.

## Native regressions and remaining composition

The heap regression checks all 120 permutations of five prefix-related keys
under four work quanta (480 cases), using independent standard ordering.
The native queue/advance regression feeds 145 repeated upsert/retract intents
with buffer sizes 1/4/128 and **8 work / 127 read / 127 spill** per quantum.
It checks actual report deltas, progress, full final document equivalence,
closed pending intake and invisible old-head preservation. Other regressions
cover infeasible quanta, cumulative work exhaustion, busy/closed intake,
cancellation while buffered, merged, reducing, input-removing and fully ordered,
ledger/stage cleanup, CRC collisions, corrupt runs and competing publication.

The public owner Graph/SQL test now uses queue/advance/close, retaining old-head
visibility throughout final reduction. Hot/cold fixtures allow an actual
disjoint foreground commit and reject relevant writes before activation.
Temporary namespaces reserve a directory and include an atomic process-local
sequence, avoiding timestamp-only parallel fixture collisions.

Artifact construction and activation still need bounded continuation and the
complete source/binding/base/receipt composition in the
[batch specification](../specs/MVCC_SEARCH_BATCH_COVERAGE_SPEC.md). Legacy
partial-prefix catch-up remains unchanged; #1043 and #231/#232 stay open.
