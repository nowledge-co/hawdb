# Pinned full projection input: conditional refinement

This is the exhaustive input prerequisite for a verified projection baseline.
It does not certify a baseline, a mapper's output, a stage, or complete C..E
coverage. Those obligations and the original #231/#232 acceptance criteria
remain open. A changefeed cursor at or above a retained-history floor cannot
establish that an existing Search head contains the complete baseline.

## Source map and contract

`crates/storage/src/store/projection_full_source.rs` owns the immutable
`GraphStore::snapshot_for_read`, the canonical cold row reader when required,
the table directory, and private continuation. `src/api/dependency_read/full_source.rs`
captures it from the same prepared read used by callback queries. The facade
records a Database dependency: concurrent writes may proceed, but any intervening
source commit conservatively invalidates subsequent publication using this
baseline read token. This does not claim dependency-scoped baseline validation.

The ordered input is:

1. Every allocated graph node candidate in `0..next_node_id` at capture,
   including deleted IDs and allocation holes. Same-snapshot parameterized
   queries determine existence and graph mapping semantics.
2. Every canonical relational table, including empty tables and engine-owned
   `hawdb_schema_migrations`, followed by its strictly ordered primary keys.
3. One explicit End carrying the captured source epoch.

A host contract must name its intentional table exclusions. Input enumeration
does not silently exclude a table because the current mapper does not use it.
Relationship effects, preimages, ownership and namespaces still require a
complete mapping/capture audit; enumerating node candidates is not that audit.

## Enumeration and admission argument

For graph candidates, a successful visitor advances the private candidate by
exactly one. Missing/deleted candidates are charged too. The captured allocation
frontier prevents later nodes entering the input. Sparse ID spaces can require
substantial work; total-budget exhaustion refuses rather than skipping holes or
claiming completion.

For a table, metadata is acknowledged before its keys. Materialized rows use
`RelationalRowPages::first_key_after`: binary page selection and an exclusive
B-tree key range. Cold rows use the pinned canonical row-page reader's exclusive
range with no requested payload or hydration fields. A metadata-only store
without its canonical reader refuses. Both paths measure the borrowed key before
cloning it, reject oversized keys, and compare each new key with the last
acknowledged key. At exhaustion the emitted count must equal the pinned table
row count. Ordering plus the source reader's complete range semantics and pinned
count establish enumeration; count equality alone would not establish it.

Before allocating the cursor/table directory, capture admits an estimate of
cursor and GraphStore inline sizes, table names, and two maximum-size key slots.
Primary keys must have 1..32 components. A seek precharges a virtual work cost
derived from pinned row count and PK width. The measured pending key survives a
step-byte yield, so resuming does not repeat that seek solely because delivery
did not fit the previous frame. Its delivery charges item/input bytes before
the callback. The last-key position advances only after successful delivery.
Total work/item/byte counters never reset between frames. An atomic unit larger
than a step refuses with StepTooSmall; an exhausted total refuses with
BudgetExceeded. Partial frame capacity yields while retaining continuation.

These are logical input/metadata limits. They do not bound all retained COW
pages, source snapshot RSS, allocator overhead, callback output, query work,
elapsed time, or physical I/O. A cold per-key range call may revalidate pages,
rebuild its bounded overlay frontier, or revisit a suffix. Per-seek row-reader
limits and a virtual seek charge do not establish a cumulative physical-work
bound. A stateful physical cursor/fuel contract and cold/RSS qualification remain
required for the complete session. Equal counters across byte frame sizes prove
logical seek reuse only, not equal underlying physical visits.

## Failure, cancellation and custody

Each step marks itself failed before advancing or invoking a callback. Only a
successful step clears that flag. Callback errors, panics and cancellation
therefore cannot yield resumable successful progress. The tracked callback read
helper also preserves sticky query errors even if a host ignores the query's
Result. Callback queries share the session's task cancellation/deadline context.
Source usability and cancellation are checked during steps, after the End
callback, and at finish. Storage-integrity errors poison the shared source
through the existing storage error path. Failure after a cold callback but
before its reader returns cannot deliver a pending key.

Finish consumes the cursor and requires acknowledged End, a usable source and
an uncancelled task. Its result is read dependencies/input usage, not output
authority. Partial callbacks may have produced private output; this API neither
rolls that output back nor authorizes making it visible.

A future baseline/session must own the mapper continuation and admitted stage,
enumerate/map/reduce the expected document set, and compare the full held Search
head. It must establish source-owner uniqueness and bind source instance,
capture/mapping contract, target binding/incarnation, base selector and durable
operation receipt. End or caller-supplied counts cannot substitute for that
verification. Artifact lifecycle and atomic source-validation/activation are
separate protocol obligations.

## Finite model

`HawDBProjectionFullSource.tla` models two node candidates (one hole), two
tables (one empty), two keys, and End. A concurrent source mutation changes a
key without changing row count. Capture fixes the source version; private
pending-key state, varying step capacities, cumulative budgets, metadata
admission, callback errors and cancellation (including End) are explicit.

The positive configuration explored **16,487 generated / 6,085 distinct states**,
depth **28**, with no invariant violation. Twelve individual faults each exit
with TLC's invariant-violation status 12: EarlyEnd, SkipTable, DropKey, RepeatKey,
ReadLive, ResetTotals, ChargeLate, ForgetPending, IgnoreError, IgnoreCancel,
SkipMetadataAdmission, and SkipEndAck. A thirteenth ProgressWitness control exits
12 on `NoProgressWitness`, establishing a reachable successful multi-step path.
This is not a temporal liveness or starvation proof. Each negative control must
fail on its named invariant; parse errors/timeouts do not count as evidence.

The run used TLC 2.19 / TLA+ tools 1.7.4, Java 21.0.9, jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The registered Bazel action is `//docs/tla:HawDBProjectionFullSource_check`;
negative configurations are registered in `mutants/mutants.txt`.

This is a finite abstraction and conditional source argument, not a
machine-checked refinement proof of Rust or of storage snapshots, query
semantics, CRC validation, power loss, mapper coverage or physical cost.

## Native evidence

`tests/mvcc_full_source.rs` has six tests, passing under all-feature Cargo
execution (6/6, 29.83 seconds), covering:

- Hot/cold persistent snapshots held across later writes and checkpoint; all
  expected table/key inputs, graph holes, old values, explicit End, and stale
  publication refusal.
- Premature finish, callback refusal, ignored query errors and sticky failure.
- Metadata and oversized-key refusal, including charged graph holes.
- Identical ordered inputs and cumulative logical usage across byte yields,
  on hot and cold sources.
- Cancellation before capture, during visitation, in and after End; panic
  cannot produce reusable progress or successful finish.
- 520 composite-primary-key rows crossing page boundaries, deletion/update and
  a cold live overlay, compared with the ordinary full SQL result oracle.

The oracle explicitly includes engine migration rows rather than treating them
as application-table rows. Initial development assertions made that incorrect
assumption and failed; this was a fixture expectation error, not a demonstrated
snapshot defect. No old-revision behavioral baseline or performance improvement
is claimed for the newly introduced API. Default/minimal Bazel integration,
storage/executor regressions and local fuzz are additional delivery checks.
The final Bazel matrix passes **133/133 targets** (90 executed / 43 cached),
including all required local fuzz entrypoints and seventeen registered model
checks. The full-source default/minimal actions each pass **6/6**, in
19.46 / 25.08 seconds. Invocation:
`eb57c7bf-83d0-4dea-a102-41d40b51c761`; command elapsed time 3732.332 seconds,
critical path 436.61 seconds. These debug test timings are not a performance
qualification. Formatting, strict workspace/all-target/all-feature Clippy and
all prek hooks pass on the final Rust source. Broader acceptance remains open.
