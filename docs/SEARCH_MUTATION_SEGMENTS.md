# Search Mutation Segment Contract

## Status

Implementation contract for issue #291's incremental publication and bounded
leveled compaction. Mutation-run encoding, integrity
inspection, shared serving visibility and a continuation writer exist. Ordinary
new-ID updates still use the append path; updates to visible IDs publish a
target-bound mutation run. Cleanup can validate and retain those artifacts.

Closure validation also resolves each retraction to its exact immutable content
version, stages that record under reader-local limits, and reconstructs its
digest, weighted lexical length and distinct terms with the selected analyzer.
The [streamed lifecycle](STREAMED_DOCUMENT_LIFECYCLE.md) uses the external
frequency reducer and retains immutable term-file ranges, so this validation
does not require the complete old body or contribution set in memory.
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
existing runs and may compact. The source-bound resource qualification below
covers sustained writes and actual bounded merges.

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

`SearchOutOfCoreGenerationBuildOptions` applies the same input ownership policy
to initial imports and incremental content publications: `max_content_documents`
defaults to 8,192 and `max_content_artifact_bytes` defaults to 64 MiB. The byte
target includes the descriptor, all three payload files, layout, complete lexical
artifact and manifest, and optional RaBitQ artifact. Compaction outputs use the
separate typed compaction input policy. Payload descriptor ranges retain their
own existing limits. The document count is a hard per-owner bound. The byte
target is a split threshold: an indivisible one-document owner may exceed it.
Record, lexical source/token, compressed/uncompressed segment, operation memory,
and whole-publication limits remain hard admission ceilings. Increasing source
limits for large bodies does not require increasing this split target.

Initial import and incremental append/replacement capture new bodies once in
their immutable spool. Each content owner
is built from a bounded spool range under the same operation memory ledger,
task and descriptor admission. Oversized artifact candidates split into smaller
ranges until they fit the byte target or contain one document. Global metadata-field
and embedding identity remain consistent across the partitions, including fields
absent from a particular partition. Private prefix manifests remain in the
writer's stage. After every partition and its dependencies validate, one final
manifest publishes the complete dataset in the real root. Cancellation or
admission failure before that boundary retains the previous complete dataset;
an absent previous selector remains absent. A lost response after the completed
durability barriers may leave the full batch committed.

An incremental batch joins its new owners to the captured active manifest once,
retaining old content and mutation runs and adding one target-bound retraction
run for the complete batch. Intermediate prefixes never expose partial edits
or deletes. Old bodies are neither copied nor scanned for this final join.
Host catch-up batches may exceed the per-owner document count without stalling
at that boundary; operation count, memory, source and whole-publication admission
still apply to the batch. Build reports include the new mutation run's bytes
and the complete final selector, and retain the final logical count/digest.

Compaction admission is independent of the split target and still counts every
complete dependency. A one-document exception is observable through its owner
document count and artifact lengths; it does not silently enlarge compaction
budgets. Hosts admitting such large documents must configure enough compaction
input admission to merge the selected owners. An insufficient policy reports
bounded no-progress instead of an unbounded rewrite.

Partition stages remain flat siblings under the real project root. A deferred
partition deletion stays registered with `retry_staging_cleanup` for that root,
including after publication. Build reports conservatively retain any observed
partition cleanup debt until the host inspects or retries the registered stages.

Build reports aggregate only the final content dependencies and final manifest,
excluding intermediate private manifests. `published_content_segments` reports
the new owners; the singular `rabitq_source_digest` is absent when zero or
multiple new RaBitQ artifacts were produced. The completed fresh three-cell
qualification below covers this initial ownership path. Earlier runs of the
single-owner builder remain separate evidence.

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

`SearchOutOfCoreReader::refresh` compares the complete checksummed durable
manifest with its pinned identity before reopening. An unchanged head keeps the
reader after checking that its referenced paths retain their admitted lengths.
A newer head shares previously validated descriptor/layout metadata and payload,
lexical and vector handles for content artifacts whose complete references are
unchanged. It still
validates new artifact references, run checksums and the aggregate visible
closure, but can reuse target reanalysis when the run reference
and every referenced content artifact are identical to the previously validated
closure. Global embedding model, version and dimension must also match before
any closure reuse. Fresh opens and changed run or target identities revalidate
the targets.
The typed refresh report counts opened/shared content artifacts and validated
and reused retractions. This cache
belongs to the reader and uses its existing closure; it adds no process-wide
cache or persistent trust record. Artifacts must remain immutable while pinned.
Regressed heads or changed identities at the same generation fail closed, and
the Mem maintenance adapter detaches its serving reader after a refresh error.

Incremental and compaction writers also retain an operation-local view of the
validated input closure for post-publication cleanup discovery. The operation
ledger admits copied manifest references and segment-handle slots before their
allocation; descriptor/layout metadata and immutable files remain shared. The
view survives dropping the original reader and is released with the writer.
Cleanup rereads the durable checksummed head, validates new files and runs, and
checks the complete closure under the inherited reader limits. Reuse requires
the same content/run references and global embedding identity as reader refresh.
Regressed or changed same-generation heads and missing referenced files fail
validation, retain artifacts and request cleanup retry. Ordinary cleanup without
an input view and ordinary fresh opens still validate the entire closure.
This removes a second resident copy of corpus metadata during writer cleanup;
manifest/run decoding and complete closure checks still depend on history size.

A [same-snapshot macOS diagnostic](benchmarks/search_incremental_cleanup_macos_2026_10_08.json)
records one changed-document publication against a complete 327,749-document,
384-dimensional snapshot. Writer-local reuse reduces the measured lifetime RSS
peak from 283,525,120 to 152,453,120 bytes with identical 47,302 published artifact
bytes under the same 256 MiB reservation. The old result exceeds admission.
The report retains source hashes, the typed prototype, configuration, process
counters and measurement limits. This diagnostic remains separate from the
completed fresh full-build and sustained qualification below.

Immutable artifact reads validate the exact manifest-bound length against the
opened file before reserving one encoded buffer. Bounded reads, EOF and checksum
checks still reject truncation, growth and replacement inconsistencies. Mutable
markers without a bound length retain progressive allocation. Lexical manifest
block decoding moves validated records from temporary nodes into one exact
final directory; the existing decode admission covers their overlap and the
serialized format is unchanged. These paths avoid geometric reallocations of
corpus-sized buffers, which retained resident pages in the measured macOS runs.

The [allocation and cold-open diagnostic](benchmarks/search_bound_artifact_reads_macos_2026_10_08.json)
retains genuine failing allocation regressions and all original measurements.
The same complete immutable base with 32 cold seed publications and a K10
replacement reached 691,470,336 bytes originally, 337,264,640 with exact file
buffers alone, and 157,319,168 after both corrections. Fresh construction is
excluded from that diagnostic. The original full 20 GiB sustained run failed
its 256 MiB RSS budget despite completing all 128 updates and merges. The
completed fresh qualification below covers the corrected, bounded initial
ownership implementation and retains these earlier failures.

In `Preferred` mode, only a typed compressed-search resource-budget error
restarts exact scalar scoring with the same visibility, candidate set and task
context. Reports include `compressed_vector_budget_exceeded`; `Required`
propagates the error. Cancellation, invalid input and corruption errors do not
trigger this fallback. Mapped projection files must remain immutable while a
reader is alive; open validates checksums, and explicit deep verification can
revalidate mapped payloads.

## Publication and recovery

Writer admission uses the canonical ancestry barrier delivered in #900 before
creating its private stage. Existing directories left by an interrupted attempt
still require synchronization. A search root may be a symlink outside its
registered project; the canonical ancestry walk then continues to the filesystem
mount boundary rather than rejecting that root. A mounted filesystem's own
persistence remains a platform assumption. This qualification reuses main's
implementation rather than adding a second stage-level barrier.

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

The default shared project FD budget is 1024. It does not pre-open handles.
On native Unix, project acquisition raises the process soft descriptor limit
when needed to cover the budget plus 64 handles for the host, preserving the
hard limit and any already higher soft limit. An insufficient effective limit
returns a typed `FileDescriptorError::OsLimit` with the requested allowance and
observed soft/hard limits. `FileDescriptorMetrics::os_soft_limit` reports the
current native Unix soft limit; it is unavailable on other platforms.
A host retaining old snapshots
must budget their handles alongside the new generation and temporary publication
files. Compaction reduces the active owner's fan-out; old readers keep their
complete immutable closures until released, so merging does not immediately
release every old handle.

Normal selection merges adjacent same-level owners. If no normal selection is
eligible and the active owner count reaches `crisis_segment_count` (default 16),
one attempt selects the smallest complete input among bounded adjacent pairs,
including pairs at different levels. The output promotes from the highest
selected level without exceeding the configured top level or demoting an
existing higher level. The hard input limit remains 256 MiB by default and
includes lexical artifacts and any rewritten mutation closure. Reader, writer,
output-artifact and operation-memory limits remain independent.

Hosts run attempts through the existing scheduled background API. A busy
scheduler defers before staging; cancellation or a budget failure preserves the
active generation. One call performs at most one merge, rather than synchronously
draining the complete history on a foreground query or checkpoint. This does
not start a worker automatically or increase the maintenance memory reservation.

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
Input admission and reports include the complete lexical artifact as well as
its manifest. Selecting any target also counts all input mutation-run artifact
bytes, because the surviving target closure is rewritten in that publication.
Selection borrows retractions without copying them before QoS admission.
Preparation charges the retained entries, IDs, and term capacities to the build
memory ledger and keeps that reservation until the writer releases them.
Publication checks the rewritten run against the reader's aggregate reopen
budget before installing artifacts: combining individually admitted runs can
increase the transient decode requirement. Failure preserves the active manifest.

## Delivery order

1. Replace corpus-sized initial artifact ownership with bounded content segments
   selected by one complete final publication, and add exact target-record lookup
   evidence.
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

`api::tests::power_loss::search_projection` runs the real search writer and reader
under storage's Unix `PowerLossModel`. It validates native IO coverage, then
materializes and reopens loss of all uncovered operations, complete/reversed
persistence, isolated pending operations, and prefix/suffix torn manifest
temporary writes. Append and bounded compaction are observed after the last
manifest temporary write; all three publication kinds are observed immediately
before/after active manifest rename. Mutation's post-commit discovery writes
validation scratch, so its single retained observation uses the exact rename
path rather than the last write under the component. Every selected manifest
must be byte-identical to the complete
old or new selector; document hydration, text/vector/hybrid IDs and scores, and
metadata filters must match that generation. Acknowledged publication must
survive loss of all remaining uncovered operations. Initial one/two-level roots
and retries of existing unsynchronized roots cover namespace ancestry, including
admission denial and startup with only one descriptor available.

Run the bounded qualification through its existing native CI owner:

```console
bash scripts/cargo-test-required.sh --locked -p hawdb --all-features --lib \
  api::tests::power_loss::search_projection:: -- --nocapture --test-threads=1
```

The existing Linux/macOS `api::tests::power_loss::` CI discovery and execution
include this module; default Bazel targets do not enable its `test-support` gate.
The model assumes completed POSIX file/directory synchronization and atomic
same-directory rename. These finite fixtures do not qualify native Windows
namespace durability, physical storage hardware, sustained RSS, or the
representative tens-of-GB workload required by #291.

The current bounded checkpoint regression is
`mutation_delete_publication_reuses_content_and_repeated_delete_is_a_noop`.
It retains two immutable content segments, publishes a delete-only mutation,
and compares the new manifest plus mutation-run bytes with the pre-existing
content closure. The test requires zero new document, metadata, vector, or
lexical artifact bytes and requires the published bytes to stay below the
existing closure size. This is a deterministic structural guard for `K`-sized
updates; it is not the representative tens-of-GB benchmark or a process-RSS
qualification.

For an uncompacted-history comparison, set
`HAWDB_SEARCH_MUTATION_BENCH_COMPACTION_EVERY=0` and run the same release fixture
with `HAWDB_SEARCH_MUTATION_BENCH_REUSE_VALIDATION=0` (fresh-open reference) and
`=1` (pinned refresh). Each round reports refresh time and the cached path's
validated/reused target counts, alongside artifact bytes, count assertions and
RSS. The default remains compaction every round; both modes use the same writer
and integrity validation, and disabling compaction is measurement configuration
for this developer benchmark, not a production maintenance policy.
Uncompacted histories retain more immutable files. Record the OS descriptor
limit and `HAWDB_SEARCH_MUTATION_BENCH_OPEN_FILES` separately; the latter selects
the fixture's finite project descriptor admission (default 1024, matching the
library default). Descriptor
exhaustion remains a failed measurement and must be retained alongside any run
using a larger explicit descriptor admission. The memory budget is independent.

The benchmark sets a 64 MiB uncompressed segment limit through typed build
options. Its 128-document range cap may bind first; neither limit bounds retained
metadata across ranges. The operation memory admission remains 256 MiB by
default. Override it with
`HAWDB_SEARCH_MUTATION_BENCH_SEGMENT_BYTES` and record the emitted limit alongside
the memory and descriptor budgets. A segment byte limit is not an operation
memory limit; the memory ledger still rejects work that exceeds its admission.

`HAWDB_SEARCH_MUTATION_BENCH_LEXICAL_BUILD_MEMORY_BYTES` selects the typed lexical
build batch admission (default 32 MiB). Record it independently from the complete
operation reservation. The qualification configuration uses 8 MiB batches with
an unchanged 256 MiB operation reservation; allocator and reader overhead still
require actual RSS measurements. A 327680-document, 512-byte-body diagnostic
using the earlier 32 MiB batch and copied metadata completed its ledger checks
but reached 321568768 resident bytes, exceeding that reservation. Retain that
failed RSS evidence alongside the shared-metadata, 8 MiB diagnostic; the latter
writes identical full/replacement/delete artifact bytes and peaks at 173064192
bytes. This diagnostic is not the large-body sustained vector qualification.

Set `HAWDB_SEARCH_MUTATION_BENCH_VECTOR_DIMENSIONS` to include deterministic
ordinal/column-hashed embeddings (default zero means no embeddings). The JSON
records the dimension, vector count, dense vector payload bytes and RaBitQ
artifact bytes so a text-only scale run cannot be presented as vector scale
evidence. Use the same dimensions and admission in both comparison modes.

Completed document-range descriptors are encoded into a private spool as they
are produced. The builder releases their owned ID and metadata summaries before
starting the next range. Final descriptor output validates the spool length and
checksum, streams the existing V3 grammar, and synchronizes the complete output
before publication. The descriptor's cumulative admission and encoded-file limit
remain enforced; the spool does not weaken the operation memory ledger. Layout
metadata remains separately admitted. The many-range regression completes 500
ranges for 1,000 documents under a 16 MiB operation budget and verifies every
persisted ID summary. The pre-streaming implementation fails this exact fixture
at document 310 after retaining 155 completed ranges.

The benchmark replaces K existing documents with changed text and embeddings
before measuring its K-document delete checkpoint. It records replacement
artifact, dense-vector and RaBitQ bytes, source reads, elapsed time and RSS
separately. Sustained rounds then replace one existing document and insert one
interleaved ID, with their configured compaction cadence. Compare both K phases
at fixed corpus size; a delete-only measurement cannot establish vector rewrite
costs.

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

## Source-bound resource qualification

The [source-bound report and raw results](benchmarks/search_initial_ownership_macos_2026_10_08/report.json)
record fresh macOS ARM64 runs at commit
`13aa6304450b8e1d9df46675f69bc634ade099f4`. They use pinned Rust 1.97.1 and
complete 5 GiB/20 GiB body-shaped corpora, with 65,536-byte bodies,
384-dimensional ordinal/column-hashed embeddings, and 32 cold seed segments.
Each cell performs its initial build, `K` changed-text/vector replacements,
`K` deletions, then 128 sustained rounds with two upserts and one actual
compaction per round. All three fresh cells complete successfully; all 1,902
source hashes, the committed head, clean worktree, and release benchmark agree
before and after execution.

| Logical corpus | K | Replacement checkpoint bytes | Delete checkpoint bytes | Lifetime peak RSS (MiB) | Maximum steady RSS (MiB) | Actual merges |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 5 GiB | 10 | 96,406 | 48,073 | 96.46875 | 86.71875 | 128 |
| 20 GiB | 10 | 127,635 | 79,390 | 174.921875 | 168.09375 | 128 |
| 20 GiB | 100 | 570,223 | 110,926 | 177.734375 | 170.828125 | 128 |

Every peak/steady sample is available and within the original 256 MiB complete
writer budget. Other unchanged limits are 8 MiB lexical build memory,
64 MiB segment bytes, 1,024 admitted project descriptors, and an OS descriptor
limit of 4,096. Validation reuse is enabled in every cell. At fixed K10, a
fourfold corpus increase grows initial artifacts 4.00008 times but replacement
checkpoint bytes 1.32393 times. At fixed 20 GiB, tenfold K grows changed dense
vector payload 10.2063 times. These are measured fixture ratios, not a claim
that every possible input has identical compression or metadata overhead.

The matched historical `8cbe16f8` K10 checkpoint emits 1,630,468,181 artifact
bytes for the same 327,680 base documents, 32 seed rows, body length, embedding
dimension and replacements. The qualified implementation emits 127,635 bytes,
a before/after artifact-byte ratio of 12,774.46. The historical initialization
builds seed rows together; persistent formats, dependencies and admission APIs
differ. This does not isolate throughput, RSS or device-write improvements.

Forced per-round compaction records checkpoint/merge artifact totals of
7,631,114/949,101,190 bytes, 11,408,495/2,907,016,737 bytes, and
11,515,923/2,906,121,477 bytes for the three cells. Corresponding artifact-byte
write amplification is 55.7198, 169.9681 and 169.9222, including all 384 actual
merges. These totals are not device writes. Host/cache state is uncontrolled;
elapsed times of 6,563.76, 16,401.31 and 11,318.13 seconds are supporting
observations, not controlled throughput measurements.

Reproduce the cells from the qualified commit with a separate temporary root:

```sh
scale_root=$(mktemp -d)
ulimit -n 4096
for scale_shape in 81920:10 327680:10 327680:100; do
  scale_documents="${scale_shape%:*}"
  scale_touches="${scale_shape#*:}"
  TMPDIR="$scale_root" \
    HAWDB_SEARCH_MUTATION_BENCH_DOCUMENTS="$scale_documents" \
    HAWDB_SEARCH_MUTATION_BENCH_TOUCHES="$scale_touches" \
    HAWDB_SEARCH_MUTATION_BENCH_CONTENT_BYTES=65536 \
    HAWDB_SEARCH_MUTATION_BENCH_ROUNDS=128 \
    HAWDB_SEARCH_MUTATION_BENCH_MEMORY_BYTES=268435456 \
    HAWDB_SEARCH_MUTATION_BENCH_SEGMENT_BYTES=67108864 \
    HAWDB_SEARCH_MUTATION_BENCH_LEXICAL_BUILD_MEMORY_BYTES=8388608 \
    HAWDB_SEARCH_MUTATION_BENCH_OPEN_FILES=1024 \
    HAWDB_SEARCH_MUTATION_BENCH_VECTOR_DIMENSIONS=384 \
    HAWDB_SEARCH_MUTATION_BENCH_COMPACTION_EVERY=1 \
    HAWDB_SEARCH_MUTATION_BENCH_REUSE_VALIDATION=1 \
    cargo bench --locked --bench search_mutation
done
```

Retain the complete benchmark JSON and process counters for each cell. The
complete local source-bound qualification export retains command logs, raw
results and hashes. The published report includes all three raw results and
complete benchmark stdout/stderr with their hashes. Native recovery evidence
includes 848 actual fault-image plans across 16 cut families, including private initial
prefixes and final real-root selection. The unchanged bounded protocol model
does not explicitly model adaptive initial partitions. Completed POSIX
synchronization/atomic rename assumptions, finite differential coverage and
hardware qualification limits remain explicit; these runs do not prove a
universal ANN or hardware power-loss guarantee.

The report retains the complete local root-suite failures: the post-scale macOS run passes 26/29 targets, with three original-deadline timeouts that
also occur on clean main. Their full isolated executions and the current
Linux 53-target root suite pass without extending deadlines. This supports a
nonblocking disposition for this repair, without proving macOS whole-suite
stability or a host cause. The post-scale complete fuzz command executes and
passes all 96 targets freshly; its earlier 95/96 outcome remains recorded.
All four required Linux checks pass at the qualified source, and platform CI
passes 12/12 jobs. Documentation publication preserves every non-documentation
source, test, benchmark, dependency and build file byte-for-byte; final-head
CI and independent review remain separate delivery gates.

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
closures; unaffected ranges retain existing runs while compacting. The
source-bound qualification above records complete sustained RSS and artifact
write amplification. Fault-image recovery evidence assumes the documented
platform synchronization contract; hardware power-loss qualification is not
provided by these software runs.
