# Search Mutation Segment Contract

## Status

Proposed implementation contract for issue #291. This document covers the
remaining delete and replacement path after append-only publication and bounded
leveled compaction. It is intentionally specific about the persistent and
query invariants before those paths change.

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
  segment, with each segment range strictly after the preceding active segment
  range in manifest order; replacements must preserve this invariant from #696;
- descriptor, payload, metadata, vector, lexical, and optional RaBitQ
  artifacts;
- a stable `segment_id` and publication generation;
- document count and reversible document-set digest contribution;
- per-segment lexical statistics and vector ordinal mapping.

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

The TLA+ model is deliberately deferred to delivery 2, when mutation runs first
become persistent manifest artifacts. That delivery must add a model and
registered configuration before it can proceed to delivery 3. The model must
cover target binding, stale preparation rejection, publish-last recovery, and
reader pins retaining a complete selected closure; its mutation and compaction
actions must also prove that an active target cannot be orphaned. Mutation runs
must not become selectable or serve requests before that model and its configured
checks pass.

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
