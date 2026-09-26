# Search Mutation Segment Contract

## Status

Implementation contract for the remaining work in issue #291 after append-only
publication and bounded leveled compaction. Mutation-run encoding and integrity
inspection exist, but production writers do not emit runs. Public readers reject
nonempty mutation closures until shared serving visibility and retracted
statistics are implemented. Cleanup can validate and retain those artifacts
without exposing a query handle.

Closure validation also resolves each retraction to its exact immutable content
version, hydrates that record under the existing limits, and reconstructs its
digest, weighted lexical length and distinct terms with the selected analyzer.
An internally checksummed run with a nonexistent target or fabricated
contribution is rejected. This is preparation for serving, not its completion.

## Read implementation in progress

Validated runs now feed a shared target-bound predicate in text scoring, scalar
vector scoring, metadata candidates and hydration. Query-term corpus statistics
subtract exact retractions with checked arithmetic and atomic rejection. The
metadata candidate file also provides live vector ordinals before RaBitQ search.
For mutation closures, logical IDs are resolved before global approximate top-K
retention so equal scores do not depend on physical layer order. The updated
[proof boundary](tla/SEARCH_MUTATION_PUBLICATION_PROOF.md) describes these kernels
and their assumptions.

Internal differential fixtures exercise these paths after validation while the
public constructor still rejects mutation closures. Aggregate run admission and
typed, observable budget fallback are implemented in the guarded read path.
Writer installation, mutation-aware compaction and sustained qualification are
still required. This work does not
make mutation serving publicly available.

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
non-overlapping range requirement from #696. A future mutation closure must
allow overlap between content artifacts: a replacement has the same ID as its
immutable predecessor. Its uniqueness invariant is one *visible* version per
logical ID, not disjoint physical ranges. Routing and compaction must be adapted
before that closure is admitted to serving or to existing update paths.

The initial import must publish bounded content segments at the same granularity
as incremental appends. A manifest entry that owns a corpus-sized lexical or
vector artifact is not a valid mutation target: replacing one ID would still
rewrite that full artifact.

### Mutation run

A mutation run is an immutable, checksummed artifact published with one
checkpoint. Entries are sorted by document ID and contain:

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
their pins are released.

## TLA+ verification boundary

[`HawDBSearchMutationPublication`](tla/HawDBSearchMutationPublication.tla) and
its [proof boundary](tla/SEARCH_MUTATION_PUBLICATION_PROOF.md) now specify the
publication protocol for a finite repeated-replacement/delete/compaction
workload. Registered checks cover target binding, stale preparation rejection,
publish-last durability, pinned closures and orphan-free complete compaction;
negative controls verify those checks detect their intended failures.

This is a bounded protocol model, not a proof of the future Rust writer, arbitrary
histories, exact lexical retractions or all serving paths. Extend its refinement
mapping and tests with subsequent deliveries. Mutation runs must not become
selectable for query serving merely because artifact integrity or these model
checks pass. Shared visibility, statistics and all affected serving paths must
also be complete before removing the reader's capability guard.

## Compaction

Content and mutation runs compact as one logical closure. A compaction that
selects a target content segment must also select every active mutation entry
that targets it. It materializes only visible documents into the replacement
content segment and drops the corresponding mutation entries. A mutation run
may be removed only when every target it contains has been materialized or is
otherwise no longer active.

Leveled selection remains bounded by the existing input-byte policy. If the
visibility closure would exceed the selected budget, the run is deferred rather
than widening the operation or silently retaining a partial result. QoS
admission and cancellation use the scheduled compaction API introduced by #704.

## Delivery order

1. Replace corpus-sized initial artifact ownership with independently published
   bounded content segments, and add exact target-record lookup evidence.
2. Add mutation-run encoding, manifest closure validation, publish-last
   recovery, pin-aware cleanup, and corruption tests.
3. Install mutation runs in `prepare_delta` and make the text path use shared
   visibility plus retracted corpus statistics.
4. Apply the same predicate to hydration and scalar vector reads; add RaBitQ
   allowlist/fallback behavior.
5. Make compaction absorb visibility closures, then qualify sustained
   append/update/delete workloads and write amplification.

Each delivery remains a separate reviewable change. Later cuts must not expose
mutation artifacts to serving before the shared visibility and statistic
contracts are complete.

## Verification matrix

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
  compaction workloads.
- Qualification: record before/after checkpoint bytes and write amplification
  against the host-selected production corpus, then run the existing full
  read-equivalence and recovery gates.

## Non-goals

This is not an LSM for primary graph or relational storage, a background thread
inside HawDB, a partial-result fallback, or a compatibility migration for old
development-only HawDB manifests.
