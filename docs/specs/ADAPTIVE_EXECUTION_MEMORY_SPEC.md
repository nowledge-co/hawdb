# Adaptive execution memory and filtered traversal

## Scope and invariants

This work improves existing embedded execution paths. It does not change the
spill record format, canonical storage, snapshot identity, query ordering,
permissions, or hard admission. A soft target never revokes a runtime permit,
result, or allocation. Ordinary expression sorting retains its existing evaluator
and budget checks.

The embedded `hawdb` facade re-exports RuntimeMemoryTarget. Added report fields
are observable Rust API additions: downstream code constructing report structs
with literals must initialize the adaptive counter, even though serialized
reports only gain a field.

## Encoded sort merge

Non-expression Column, Property and Id keys are selected from a validated spill
record. The reader admits the encoded payload before allocation and validates
all fields, nesting, identifiers, UTF-8 and trailing bytes. Key preparation
preflights selected owned values before allocating. It preserves node/property
fallback, missing values, signed ID conversion and stable input ordinals.
Intermediate merges copy the encoded record without building a Binding. The
final merge decodes only emitted rows; OFFSET rows remain encoded. This removes
whole-row materialization from intermediate passes, while final output still
requires an admitted transition from encoded to owned data.

Run-local bounds describe maximum encoded payload and retained keys. They are
in-memory observations from run generation, not persisted metadata or guesses
from file lengths. Groups try 8, 4 and 2 inputs. Key and payload allowances are
reserved from the existing query root before a wider merge. Reader buffers,
writer buffers, heap/reader storage and descriptor capacity are also admitted.
A conservative maximum or a capacity refusal can reduce fan-in; the two-way
floor checks actual allocations. Corruption and I/O failures are propagated.
Inputs are removed only after a replacement has been flushed. Failed output
runs and reservations are released through their owners.

Encoded multi-way groups execute in bounded, independently admitted waves.
The sum of keys, encoded heads and IO buffers in a planned wave stays within
the blocking work-area budget. Buffers can use the remaining allowance before
another group is admitted, preserving useful IO sizes with fewer concurrent
groups. All groups also share one IO account. The actual-allocation two-way
fallback retains its original separate hard key/payload admission and a bounded
IO allowance; conservative maxima do not make an otherwise viable pair fail.
Buffer sizes can shrink below 4 KiB to preserve existing tiny-budget queries.
A universal IO floor and direct final multi-way output remain follow-up work
in [#1315](https://github.com/nowledge-co/hawdb/issues/1315):
the former needs an explicit low-budget fallback, and the latter needs joint
admission of all final heads plus decoded output.
Memory and captured descriptor inventories remain owned until every dispatched
sibling joins. Capacity pressure reduces wave size before adding merge passes.
Expression sorting retains serial pair merges; other blocking codecs retain
their existing shared scheduler. Reduced pass count alone is not evidence of
improved CPU throughput or concurrency. Measure both resources and latency.

## Cooperative work areas

RuntimeTaskContext carries a RuntimeMemoryTarget independently of allocation
and I/O controllers. Governor-bound tasks and children observe host-updated
process policy and resource pressure; no thread samples RSS on their behalf.
Elevated pressure requests half the configured work area and critical pressure
requests one quarter. A current process sample that pauses admission requests
one quarter; a stale or missing sample requests half unless resource pressure
is already critical. New admission still fails closed for unavailable samples. Relief must remain observed for 250 ms before a target expands; the
process policy retains its separate configured recovery headroom.

Sort, buffered hash aggregate, hash distinct and admitted hash join sample
the target on their first mutation-safe check and every 64 checks thereafter.
The cached value belongs to each operator; reset/spill does not restart its
cadence. Checks without growth (join probes) also advance sampling. This bounds
shared-policy locking while allowing at most 63 intervening safe checks before
observing a host update. Hard overflow and allocation admission remain immediate. They keep a progress floor bounded by the
hard operator limit: two incoming items plus two ordinary spill IO buffers.
Empty state never spills merely because of a soft target. TopN is exempt from
soft pressure: its offset-plus-limit heap keeps its cutoff and spills only for
hard memory capacity. Resetting that cutoff under sustained pressure would turn
otherwise rejected candidates into unbounded extra spill work. Pressure spills free
sort row-vector capacity. Hash join can spill its build table during probing;
already emitted probes are not replayed. Memory pressure can add spill work or
reach an existing spill quota; it never silently returns a partial answer.

## Filtered DiskANN feedback

Only traversal with an exact eligible domain on a sorted-ID pinned provider
uses execution feedback. Existing small-domain direct evaluation is unchanged.
After seeding, at least 32 additional visits and at most one eligible score per
eight visits indicate poor yield. Exact evaluation is considered only when the
eligible domain is no larger than four times those visits and a conservative
lookup/visit bound fits the remaining local work budget.

The switch happens at most once and before output. It reuses the same provider,
allowlist, beam and seen workspace, retains cumulative work charges, and avoids
rescoring already visited eligible records. Rejected vectors are never read.
Parent work limits, concurrent budget consumption, cancellation and provider
errors can still refuse completion; no partial result escapes. A Mem caller
must propagate the typed WorkUnits refusal as a failed/incomplete search, never
as an empty or partial successful result. Retrying requires explicit fresh
admission and a chosen larger work budget or smaller requested search scope;
the executor does not reset shared counters or silently retry. The public
search metrics expose `candidate_scan_adaptive_exact_scan_count`, summed across
segments and candidate scans. This is path evidence, not a new general recall
guarantee for the enclosing approximate search pipeline.

## Verification and measurements

Existing executor, core, QoS, vector-projection and Search CI owners cover the
ordinary unit/integration cases. Local-only differential entry points are
listed in TEST_INDEX. They include wide/nested rows, ordering ties, memory and
FD-driven 8/4/2-way selection, live host pressure, productive/poor vector
navigation, exact filtered ground truth, cancellation and work refusal.

`cargo run --locked --release -p hawdb-executor --example spill_resource_probe
-- <small|wide|many|concurrent>` is a manual resource probe. Copy the identical
probe to the baseline revision and run each scenario in fresh processes. It
uses streaming synthetic input, verifies stable output and payload preservation,
and reports global allocator counts/bytes, peak live heap, query-ledger peak,
cumulative spill writes and elapsed time. Obtain process peak RSS externally,
for example with `/usr/bin/time -v` on Linux. Add `--governed` after the
scenario to share one governor and process policy across all query workers.
Each query acquires a foreground permit, binds memory/IO controls and uses an
admitted query ledger. The host supplies a fixed healthy RSS sample (60-second
validity); this measures governed execution, not sampler cost or pressure latency. Fixtures have no durable user data.
For these complete-output scans every written run is consumed once, so logical
spill bytes read equal logical bytes written; neither is physical device IO.

Use identical toolchains/profiles, repeated alternating baseline/candidate
runs, and record filesystem/cache state. The concurrent case uses four queries.
Keep raw logs outside Git, and put revision-bound results and limitations in
the PR or one concise benchmark report. Heap growth, process RSS and ledger
charges are different measurements and must not be substituted for each other.
