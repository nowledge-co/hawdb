# Independent full-source baseline comparison

This implements complete historical set/content comparison for #231/#232/#1043.
It does not yet mint the versioned baseline/capture/mapping/binding/external-vector
or durable operation receipts required for activation. Actual Mem migration,
native C..E coverage activation, incremental
final artifacts, power-loss composition, physical-cost/RSS and writer qualification
remain open in [the acceptance ledger](../specs/MVCC_VERSIONED_PUBLICATION_SPEC.md).

## Source and expected-output custody

`Database::begin_projection_baseline_comparison` consumes real Search governor
admission, creates a private uniquely keyed expected stream, then captures the
full native source using the stage's exact bound task context. The existing
reader's watermark must equal that captured canonical epoch C. Caller-supplied
build epochs, unknown watermarks and lagged heads refuse before mapping. HawDB
does not expose arbitrary historic reads: current E data cannot verify an old C
head merely because the caller supplied its numeric cursor.

`ProjectionBaselineComparisonSession` owns the full-source cursor, fixed mapper,
shared cumulative calls/query/row/payload admission and invisible sort stage.
One source step delivers at most one borrowed input. The cursor enumerates graph
allocation candidates including holes, every canonical relational table including
empty/engine tables, ordered primary keys, and explicit End. Each successful
callback returns zero or one document. Table boundaries and End cannot emit a
document. No mutable source/stage, caller output vector, ordinal or completion
counter escapes. Backpressure advances ordering before another input; only
acknowledged source End closes intake. Ignored read errors, callback panic,
cancellation, source/cumulative admission or ordering failure poison the session.

This full mapping contract needs no parent fanout loop: canonical child keys are
independently enumerated. The host must classify every table explicitly, including
operational exclusions, and map each native source to its canonical document or
None. Mapping/business semantics remain trusted; source-complete enumeration does
not mechanically prove an arbitrary Rust function's business correctness.

The source visits each native node/key once and each callback can emit at most
one document. Therefore different outputs with the same document ID represent
distinct native inputs under this contract. `require_unique_upserts`, installed
before input, rejects duplicate IDs even for byte-identical payloads and across
spill boundaries. Ordinary same-E delta reduction keeps its existing identical
upsert coalescing. Full enumeration must choose one canonical producer for mirrored
Graph/SQL documents rather than silently merge ambiguous owners. A producer that
needs multiple documents per native input needs a separately qualified contract;
this API does not expand generic parent fanout during a full scan.

## Exact complete comparison

After ordering, fallible admission completes before transferring the stage into
`SearchOutOfCoreBaselineComparison`. On refusal, the poisoned writer remains in
the source owner's slot, retaining its actual permit until cleanup. The additional
comparison header shares the same memory ledger; a bounded reusable body scratch
file is reserved in the private stage before transfer.

Each native comparison step visits at most one final unique expected document or
explicit validated End. It locates that ID in the held immutable Search reader,
streams its verified stored body/header using the existing admitted Source path,
and compares the complete canonical encoding to the expected spool record using
fixed admitted buffers. Title, body, metadata and original embedding participate;
length/CRC equality never substitutes for byte equality. The existing Source
capture validates the full selected compressed segment envelope/checksums and
bounded header/UTF-8 body. Original payload is compared, not approximate ANN data.

Every expected ID must exist and match. At End, the native compared count must
also equal the reader's complete visible document count. Thus an extra retained
base document refuses even when every emitted expected document matches. No delta
is applied to the base to construct the comparison oracle. The old generation,
watermark and bodies remain unchanged, and no replacement artifact is built.

The set argument assumes the existing validated reader closure: without mutation
runs, manifest/content counts agree; with runs, `mutation_run::validate_closure`
reconstructs the exact visible count and digest and rejects repeated/out-of-closure
retractions, while target validation checks their claimed immutable versions.
Given a unique expected set X and valid actual set Y, exact membership/content
for all x in X establishes X is a subset of Y; equal cardinalities then establish
X=Y. Count-only, digest-only and retained-base-union comparisons are insufficient.
Corrupt/uncertain source artifacts fail closed and the active source evidence is
retained; only private comparison scratch is cleaned up.

## Historical identity and resources

Successful finish returns `ProjectionMatchedBaseline`, holding source identity/
history dependencies, the exact held reader generation, mapper and native report.
It has no publication trait or candidate conversion. The full C data snapshot
closes only after source and comparison End; private stage cleanup retires the
permit. The borrowed reader retains its original generation/pins. A newer live
canonical epoch or a newer Search head may exist by then: this result describes
the captured historical C and held head, not authority to publish over either.
The actual source-C mapper, unique-owner/namespace, capture history, schema,
binding/incarnation and independent vector versions still need a sealed contract
before an activation receipt may consume this result. Comparing vector bytes
prevents silent lexical-only acceptance; it does not certify an external authority.

A historical baseline-C observation must not be revalidated as an unchanged full
Database read at current E: C..E is the later change interval. Future native intake
must check its source/base/contract identity, complete capture availability and
final-E dependencies. Conversely final activation cannot silently weaken those
E checks. An unknown/lagged base needs a genuine full replacement or independent
verified baseline, not current hydration of an old cursor. A private C baseline
plus C..E overlay may require further native composition; ordinary unordered
same-E reduction cannot safely mix differing C and E upserts as identical inputs.

Mapping reservations never reset between source inputs, including empty results
and table/End callbacks. Ordering retains its existing cumulative work/read/spill
accounting. Stored-source comparison has fixed per-lookup compressed/expanded
ceilings and cumulative conservative reservations, precharged before lookup;
expanded input is checked before full decompression. Source headers, decode buffers,
comparison scratch, sort state and pending records share the admitted build ledger.
The reusable body file is bounded by the expanded ceiling; successful comparisons
remove it, and errors leave it private for stage cleanup.

These counters are not complete physical CPU/I/O/RSS or latency bounds. Routing
may probe multiple lexical indexes and is governed by the reader's existing
limits; its CPU/index reads are not covered by the new segment reservations.
Each lookup may reread/decompress a segment; this is not a linear-I/O merge scan.
Base routing, source snapshots/reader residency, host document construction and
canonical query work retain additional obligations. The total reservations are
conservative per-lookup credits, not measured bytes or a numeric wall-clock bound.
A genuine conservative full-C replacement is now implemented separately in
[the full rebuild path](PROJECTION_FULL_REBUILD_PROOF.md). Ongoing C..E
composition and size-independent final activation remain unimplemented.

## Database-scope dependency subsumption

The full source already records Database at C. `GraphReadDependencies::record`
now avoids retaining additional precise identities once that scope is admitted.
Otherwise a large full scan could accumulate one redundant dependency per row.
A previously refused capture remains refused; neither admission nor error state
is reset. Existing precise scopes collected before Database remain bounded by
their original cap. Unsupported query shapes, query/input budgets and statement
execution are unchanged.

Conditional safety argument: canonical commits increase epoch and every current
stamp is at most the current epoch. Database validation requires current epoch C
and the same canonical instance. Every precise identity read from pinned C is
therefore unchanged under that stronger condition. Omitting redundant precise
keys cannot make any later-epoch publication valid: the Database check rejects
it first. Prior refusal is checked before dependency validation. Historical
comparison itself never invokes current-E publication through that token.
The source regression admits 1,000 distinct keys into an already admitted one-slot
Database capture without byte growth, rejects a subsequent unrelated write by the
Database identity, and preserves an earlier precise-budget refusal.

## Finite model and controls

`HawDBProjectionBaselineComparison.tla` models two canonical owners (or empty
input), full mapping/End, unique ownership, independent expected output, exact
historical comparison, conservative lookup admission, failure/cancellation,
source/permit lifetime including failed ownership, competing source writes and a changed live Search head.
The positive model checks **4,104 generated / 2,496 distinct states**, depth **13**.
Fourteen fault controls and three witnesses all return exact expected TLC exit 12:
SkipSource, DeltaAsExpected, DedupOwners, CountOnly, IgnoreVectors, IgnoreCount,
ReadLive, IgnoreMapperError, IgnoreEpoch, ResetLookups, PublishMatch,
ReleasePinEarly, ReleasePermitEarly, IgnoreCancel, ProgressWitness,
ConcurrentWitness and HeldHeadWitness. Source mapping semantics and reader/codec
closure correctness are assumptions. This is not a Rust refinement, complete
host contract, physical-cost, crash/power-loss or fairness proof.

TLC 2.19 / TLA+ tools 1.7.4, Java 21.0.9; jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
Registered action: `//docs/tla:HawDBProjectionBaselineComparison_check`.
All seventeen controls are registered in `mutants/mutants.txt`.

## Native evidence

The initial corrected facade qualification passes **7/7** all-feature tests
(37.75s). It covers hot/cold C mapping across later canonical writes/checkpoint;
extra/missing/body/vector mismatch; identical duplicate owners; unknown empty
tables; ignored query errors, panic, forbidden boundary output, cumulative
mapping/comparison refusal, cancellation and premature finish; unknown/lagged or
caller-supplied epoch refusal; empty full sources and old held Search heads after
another publication; and 523 documents across cold key pages using just one
Database dependency slot. Every rejected attempt preserves the old active head
and releases actual governor counters after cleanup.

The exact native Search baseline selection passes **6/6** (1.89s), including
visible mutation-closure comparison and actual permit retention under forced
handoff memory-admission failure. The storage subsumption regression passes
**1/1**. Earlier fixture errors (unsorted extra base input, unqualified unbounded
DELETE, too-small cold seek quantum and wrong test-only API use) are excluded
from successful qualification and are not engine-defect evidence. Existing
callback/delta APIs and their default duplicate policy remain unchanged.

The dedicated default/minimal Bazel baseline actions pass **7/7 each**
(53.6s / 47.4s actions; 3/3 targets with the positive model cached). A private
comparison result cannot enter source-validated Search publication; the facade
contains a compile-fail contract for that boundary. The nonpublication compile-fail doctest passes 1/1 (0.14s), and final Rust
format plus workspace/all-target/all-feature strict Clippy pass. Full-suite
qualification remains separate from these focused receipts.

The final frozen-input matrix passes **140/140 targets** (96 executed / 44 cached),
2,288.725s elapsed / 391.84s critical path; invocation
`85e5267d-60e9-4d29-9e9b-b1887b24fdc3`. Runtime, tests, model and build inputs
remained at tree `54d70a18997d5470ac830cc3f713e0172ba07e94`; only this result
prose changed after terminal success. Both final baseline profiles pass **7/7**
(37.87s / 37.55s native). Search passes **858 / 20 existing ignored**, Storage
**1,168 / 29 existing ignored**, publication **64/64 each**, staging **12/12 each**,
JOIN **8/8 each**, owner **5/5 each**, and full-source **6/6 each**. Both graph
residency campaigns, twenty registered models, evidence checks and required local
fuzz pass. The matrix adds both baseline profiles and its model to the preceding
137-target command with unchanged deadlines and `--local_test_jobs=1 --jobs=4`.
A direct final-model check confirms 4,104 generated / 2,496 distinct states, depth
13 after strengthening failed-owner resource invariants. Final format and strict
workspace/all-target/all-feature Clippy pass. These local correctness receipts
do not qualify writer scaling, physical-cost/RSS or power-loss behavior.
