# Search Mutation Segment Contract

## Status

Implementation contract for the remaining work in issue #291 after append-only
publication and bounded leveled compaction. Mutation-run encoding, integrity
inspection, shared serving visibility and a continuation writer exist. Ordinary
new-ID updates still use the append path; updates to visible IDs publish a
target-bound mutation run. Cleanup can validate and retain those artifacts.

Closure validation also resolves each retraction to its exact immutable content
version, hydrates that record under the existing limits, and reconstructs its
digest, weighted lexical length and distinct terms with the selected analyzer.
An internally checksummed run with a nonexistent target or fabricated
contribution is rejected before the reader serves the closure.

## Read implementation

Validated runs now feed a shared target-bound predicate in text scoring, scalar
vector scoring, metadata candidates and hydration. Query-term corpus statistics
subtract exact retractions with checked arithmetic and atomic rejection. The
metadata candidate file also provides live vector ordinals before RaBitQ search.
For mutation closures, logical IDs are resolved before global approximate top-K
retention so equal scores do not depend on physical layer order. The updated
[proof boundary](tla/SEARCH_MUTATION_PUBLICATION_PROOF.md) describes these kernels
and their assumptions.

Internal differential fixtures exercise these paths after validation. Aggregate
run admission and typed, observable budget fallback are implemented in the read
path. The continuation writer prepares target-bound runs for clean readers as
well as existing closures and publishes replacements or delete-only manifests.
Compaction rewrites the mutation run atomically with the selected range. A
range containing the complete target closure materializes only visible
documents and removes those entries; a partial closure retains entries targeting
segments outside the range in a new run. Unaffected ranges retain their
existing runs and may compact. Sustained qualification is still required.

## Goal

A checkpoint that changes `K` logical search documents must write search
artifacts proportional to `K`, without rebuilding an unrelated corpus-sized
generation. Reads must return exactly the same document IDs and scores as a
single segment containing the current logical document set.

The contract applies to text, scalar vector, and RaBitQ serving. It preserves
publish-last recovery, reader pinning, task cancellation, and host-owned QoS
admission.

## Why append-only segments are insufficient

An appended content segment can represent a new document, but it cannot
represent deletion or replacement by itself:

- the old content remains selectable by text, scalar vector, and RaBitQ reads;
- its document length and term DF remain in corpus-wide BM25 statistics;
- a repeated ID currently fails the duplicate-result invariant;
- rebuilding an old content artifact to remove one document is proportional to
  the old artifact, not to the checkpoint delta.

The current `DocumentsDigest` is deliberately reversible, so manifest identity
can add and remove document contributions exactly. The missing contract is the
relationship between an old content segment and the mutation that supersedes a
specific document in that segment.

## Logical model

### Content segment

A content segment is an immutable, independently selectable artifact closure:

- documents whose UTF-8 `document_id` values are strictly increasing within the
  content artifact and its descriptor ranges;
- descriptor, payload, metadata, vector, lexical, and optional RaBitQ
  artifacts;
- a stable `segment_id` and publication generation;
- document count and reversible document-set digest contribution;
- per-segment lexical statistics and vector ordinal mapping.

Content-only append and existing rewrite paths retain the globally ordered,
non-overlapping range requirement from #696. Mutation closures allow overlap
between content artifacts: a replacement has the same ID as its
immutable predecessor. Its uniqueness invariant is one *visible* version per
logical ID, not disjoint physical ranges. Routing and compaction use the shared
target-bound visibility predicate for these overlapping artifacts.

The initial import must publish bounded content segments at the same granularity
as incremental appends. A manifest entry that owns a corpus-sized lexical or
vector artifact is not a valid mutation target: replacing one ID would still
rewrite that full artifact.

### Mutation run

A mutation run is an immutable, checksummed artifact published with one
checkpoint. Entries are sorted by `(document_id, target_segment_id)` and contain:

- `document_id`;
- the exact `target_segment_id` that supplied the visible previous version;
- `operation` (`delete` or `replace`);
- the previous document's reversible digest contribution;
- its lexical document length and unique query-term contribution.

A replace checkpoint also publishes a new content segment with the replacement
document. A delete checkpoint may publish only a mutation run. The mutation run
is part of the active manifest closure and is retained by reader pins and
generation cleanup exactly like content artifacts.

The target segment ID is required. A global set of deleted IDs would hide both
the old document and its replacement. A mutation hides an ID only when the
candidate came from its recorded target segment.

### Visibility

For a candidate `(segment_id, document_id)`, the document is visible if no
active mutation run contains an entry with both values. A replacement document
therefore remains visible in its newly published segment, while its predecessor
does not.

Serving evaluates this predicate before retaining or ranking a candidate:

- text scoring applies it before inserting a score into the cross-segment map;
- scalar vector scanning applies it before cosine similarity and score storage;
- RaBitQ turns it into a segment-ordinal allowlist before approximate search.

If a bounded RaBitQ allowlist cannot be constructed within the admitted query
budget, `Preferred` mode falls back to exact scalar serving with an observable
reason. `Required` mode fails closed; it never returns an under-filled page.

Hydration, metadata filtering, total-hit accounting, and candidate reports use
the same predicate. No route may independently implement visibility.

## Exact lexical statistics

Each content segment contributes its local document count, total document
length, and query-term document frequencies. Each active mutation run subtracts
the contribution of the document it made invisible. The resulting statistics
are used for all segment-local BM25 scoring:

```
live_statistics = sum(content_statistics) - sum(mutation_retractions)
```

A mutation records retractions only for the version that was visible at its
publication generation. This prevents double subtraction across repeated
replacements. Underflow, missing target metadata, an analyzer digest mismatch,
or an incomplete retraction artifact is a read or publication error, never a
best-effort score adjustment.

The manifest document count and digest follow the same rule. Publication
subtracts the previous contribution for every mutation target, then adds the
replacement contribution when present. Both values must agree with a complete
logical reconstruction in qualification tests.

`mutation_run::validate_targets` checks target existence and exact contributions
after `validate_closure` checks membership, unique target pairs and aggregate
identity. It selects the named content artifact, finds the bounded descriptor
range, probes the lexical ID mapping and hydrates only the selected document.
It does not route by logical ID across artifacts, which could select a newer
version. Reanalysis uses the artifact reader's source, term and token limits.
One hydrated target and its reconstructed terms are retained at a time. Several
targets in the same range currently repeat range I/O; this is not yet a
sustained-update performance qualification. The encoded run limit also remains
per file. `max_mutation_working_bytes` separately admits the aggregate run
buffers, decoded ownership and closure-validation indexes (default 128 MiB).
Each decode must fit alongside all previously retained runs. This counts
requested capacities under the pinned allocator-facing collection behavior,
not process RSS; content artifacts and one-target hydration/analysis retain
their independent limits.

In `Preferred` mode, only a typed compressed-search resource-budget error
restarts exact scalar scoring with the same visibility, candidate set and task
context. Reports include `compressed_vector_budget_exceeded`; `Required`
propagates the error. Cancellation, invalid input and corruption errors do not
trigger this fallback. Mapped projection files must remain immutable while a
reader is alive; open validates checksums, and explicit deep verification can
revalidate mapped payloads.

## Publication and recovery

Mutation preparation has four ordered stages:

1. Resolve every changed ID to its currently visible content segment and read
   only the bounded source records needed for the mutation.
2. Build the new content segment, if any, and the mutation run in a private
   stage directory under the caller task context.
3. Verify artifact lengths, checksums, analyzer and embedding identity,
   retractions, and the resulting count and digest.
4. Acquire the existing publication lease, recheck the active generation, then
   publish one manifest that references the complete new closure.

No mutation artifact is selectable before the manifest replacement. A cancelled,
failed, or stale preparation deletes its private stage and leaves the active
manifest unchanged. Existing readers retain their complete old closure until
their pins are released. If a process stops after an artifact is durable but
before manifest replacement, recovery selects the last complete generation and
ignores the orphan; the mutation recovery regression exercises that boundary.

## TLA+ verification boundary

[`HawDBSearchMutationPublication`](tla/HawDBSearchMutationPublication.tla) and
its [proof boundary](tla/SEARCH_MUTATION_PUBLICATION_PROOF.md) now specify the
publication protocol for a finite repeated-replacement/delete/compaction
workload. Registered checks cover target binding, stale preparation rejection,
publish-last durability, pinned closures and orphan-free complete compaction;
negative controls verify those checks detect their intended failures.

This is a bounded protocol model, not a proof of arbitrary histories, exact
lexical retractions or whole-process resource behavior. The current Rust serving
paths implement the shared visibility/statistics contract covered by the
fixtures below; the model remains a conditional refinement boundary and must be
extended as later lifecycle states land.

## Compaction

Content and mutation runs compact as one logical closure. A compaction that
selects a target content segment materializes only visible documents into the
replacement content segment. Entries targeting selected segments are absorbed;
entries targeting segments outside the range are serialized into a replacement
mutation run in the same manifest publication. This keeps every surviving
target bound to an unchanged segment without widening the selected byte range.

Leveled selection remains bounded by the existing input-byte policy. If the
visibility closure would exceed the selected budget, the run is deferred rather
than widening the operation or silently retaining a partial result. QoS
admission and cancellation use the scheduled compaction API introduced by #704.
Selection borrows retractions without copying them before QoS admission.
Preparation charges the retained entries, IDs, and term capacities to the build
memory ledger and keeps that reservation until the writer releases them.
Publication checks the rewritten run against the reader's aggregate reopen
budget before installing artifacts: combining individually admitted runs can
increase the transient decode requirement. Failure preserves the active manifest.

## Delivery order

1. Replace corpus-sized initial artifact ownership with independently published
   bounded content segments, and add exact target-record lookup evidence.
2. Add mutation-run encoding, manifest closure validation, publish-last
   recovery, pin-aware cleanup, and corruption tests.
3. Install mutation runs in `prepare_delta` and make the text path use shared
   visibility plus retracted corpus statistics.
4. Apply the same predicate to hydration and scalar vector reads; add RaBitQ
   allowlist/fallback behavior.
5. Absorb complete visibility closures and rewrite partial mutation closures
   during compaction, then qualify sustained append/update/delete workloads and
   write amplification.

Each delivery remains a separate reviewable change. Later cuts must not expose
mutation artifacts to serving before the shared visibility and statistic
contracts are complete.

## Verification matrix

The current bounded checkpoint regression is
`mutation_delete_publication_reuses_content_and_repeated_delete_is_a_noop`.
It retains two immutable content segments, publishes a delete-only mutation,
and compares the new manifest plus mutation-run bytes with the pre-existing
content closure. The test requires zero new document, metadata, vector, or
lexical artifact bytes and requires the published bytes to stay below the
existing closure size. This is a deterministic structural guard for `K`-sized
updates; it is not the representative tens-of-GB benchmark or a process-RSS
qualification.

The reproducible benchmark `search_mutation` supplies that measurement hook. It
builds a complete immutable generation, applies `K` delete mutations through
`prepare_delta`, reopens the result, and emits JSON containing the full-build
bytes, mutation-checkpoint bytes, ratio, elapsed time, source bytes read,
hydrated-document count, and process-memory samples. Set
`HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS` and
`HAWDB_SEARCH_MUTATION_BENCH_TOUCHES` to scale the fixture; for example:

```sh
HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS=200000 \
HAWDB_SEARCH_MUTATION_BENCH_TOUCHES=100 \
cargo bench --locked --bench search_mutation
```

The benchmark is an evidence generator, not a release qualification by itself:
the resulting JSON must be recorded against the host-selected production corpus
and paired with sustained RSS and crash-recovery runs.

Set `HAWDB_SEARCH_MUTATION_BENCH_ROUNDS` to run the sustained mode. It first
creates 32 small append segments, then repeats replacement plus new-ID append,
reopen, and bounded compaction for the requested number of rounds. The JSON
contains one record per round with checkpoint bytes, source hydration, whether
compaction published, document count, and process-memory deltas. For example:

```sh
HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS=200000 \
HAWDB_SEARCH_MUTATION_BENCH_TOUCHES=100 \
HAWDB_SEARCH_MUTATION_BENCH_ROUNDS=8 \
cargo bench --locked --bench search_mutation
```

A smoke run with 200 documents, 10 initial deletes, and three sustained rounds
published compaction on all three rounds, grew the logical count from 222 to
225, and kept each round's source hydration to one replacement document. This
is a deterministic lifecycle and RSS sampling harness; it does not establish a
production RSS limit or replace host power-loss testing.

At the issue's corpus-shaped scale, a release run with 334,844 documents, 100
initial deletes, and two sustained rounds wrote 68,285,256 bytes for the full
generation and 65,673 bytes for the initial mutation checkpoint. Both sustained
rounds published compaction, each checkpoint wrote 38,347 and 38,511 bytes,
hydrated one replacement document, and read 1,131 source-segment bytes. The
logical count advanced from 334,776 to 334,778. The initial checkpoint took
0.52 s; the two sustained checkpoints took 0.91 s and 0.90 s. This is still a
synthetic-content baseline and does not close production RSS or host power-loss
qualification.

The current corpus-shaped run uses the issue #291 scale (334,844 documents and
100 deletes) with the deterministic fixture above. On the local release build it
reported 68,285,256 bytes for the full generation and 34,733 bytes for the
mutation checkpoint (0.0509%), with 109,100 source-segment bytes read and 100
documents hydrated. The full build took 21.6 s and the mutation checkpoint took
0.68 s; the process RSS sample grew by 53,805,056 bytes. This is a useful
write-amplification baseline at the measured corpus size, but it remains a
synthetic-content run and does not close the sustained-RSS or crash-recovery
qualification gates.

- Differential: build the same logical corpus as one segment and as append,
  delete, and replace mutation runs; assert identical IDs, ordering, total hits,
  text scores, scalar vector scores, and hybrid results.
- Lifecycle: old pinned readers retain the old document while a reopened reader
  sees the replacement or deletion; stale publication and every cancellation
  checkpoint preserve the active manifest.
- Integrity: corrupt every mutation file class, remove a referenced artifact,
  or alter a retraction count; open and publication fail closed.
- Resource bounds: assert `K`-proportional new artifact bytes, admitted build
  memory, query memory, and scheduler accounting over sustained mutation and
  compaction workloads. The delete-only publication regression also compares
  the new manifest/run bytes with the complete pre-existing content closure
  and proves that no content artifact is rewritten.
- Qualification: record before/after checkpoint bytes and write amplification
  against the host-selected production corpus, then run the existing full
  read-equivalence and recovery gates.

## Non-goals

This is not an LSM for primary graph or relational storage, a background thread
inside HawDB, a partial-result fallback, or a compatibility migration for old
development-only HawDB manifests.


## Incremental mutation writer

The delta path selects mutation publication for a reader that already owns a
validated closure, or for a clean reader when an update targets a currently
visible document. New IDs on a clean reader remain append-only. The public
constructor validates and serves mutation closures; compaction absorbs selected
targets and atomically rewrites references to outside segments.

Preparation resolves each unique requested ID against current visibility. It
reads one bounded descriptor payload range at a time with the operation's
hydration admission, reconstructs exact retractions with an admitted analyzer,
and retains only changed IDs/term sets. Repeated targets in one range currently
repeat I/O. New content contains only upserts. A delete of an absent ID produces
no retraction, and an upsert of a previously deleted ID is a fresh visible version.

Publication encodes the run under its per-file limit and rechecks aggregate
reopen capacity for old-run decode prefixes and the new run before installing
anything. It preserves old runs, subtracts newly retracted count/digest once,
and adds new content contributions. Delete-only publication installs no empty
content artifact: its root-level written bytes comprise only the new run and
manifest. Empty build artifacts are still created in the temporary stage and
discarded; this has not been optimized or qualified. A repeated absent deletion
can publish just a new manifest (including source-epoch progress).

The existing generation lease/CAS and manifest-last commit boundary apply.
Cancellation, stale-generation rejection and budget failure leave the old
manifest unchanged. Mutation-aware compaction supports complete and partial
closures; unaffected ranges retain existing runs while compacting. Sustained
RSS/write-amplification and host power-loss qualification remain explicit
unfinished requirements in issue #291.
