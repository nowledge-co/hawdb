# Large Document Search Lifecycle Specification

## Status and scope

Status: proposed for review. This document specifies the remaining delivery for
[issue #392](https://github.com/nowledge-co/hawdb/issues/392); it does not claim
that the proposed APIs, default changes, or qualification have shipped.
Normative clauses below describe the proposed target contract. Existing public
contracts remain in force until their implementation changes are reviewed.

The additive streaming implementation and its retained default guards are
tracked in the [delivery matrix](../STREAMED_DOCUMENT_LIFECYCLE.md). That matrix
distinguishes implemented entrypoints from remaining qualification gates.

The recommended target is a document body larger than its admitted operation
memory, processed without truncation or a change to search semantics. This is
stronger than admitting a larger owned `SearchDocument`. Source size, minimum
analyzer working units, and materialized result size are separate capabilities.

The scope includes initial indexing, append, replacement, deletion, checkpoint,
compaction, reopen, exact lexical scoring, hybrid visibility, and selected-body
reading through the embedded Rust facade. Embeddings are supplied by the host;
model inference and model context limits are outside this contract.

This is a derived search-storage contract. It does not enlarge SQL `BYTEA`,
canonical WAL records, attachment upload limits, or Mem's canonical content
schema. A host can supply a pinned external source without first storing its
whole body in one HawDB row. Mem integration, release activation, full-data
verification, and legacy retention retain their independent gates.

## Current implementation and remaining boundaries

The following audit uses main revision
`f8c92970dd595e704dfde0a3caf9b0e9e7a0dec8`. Older descriptions in #392 predate
several implemented prerequisites.

| Boundary | Implemented foundation | Remaining obligation |
| --- | --- | --- |
| Input | `push(SearchDocument)` admits owned capacities; encoded spool writes use bounded scratch | The caller and spool decoder still materialize the body; add a streaming source path |
| Analysis | Incremental token emission, admitted identifier state, joined opaque analyzer workspace | Preserve state across input chunks and bound the largest indivisible analyzer unit before allocation |
| Frequencies | Document-local sorted spill and exact field-aware reduction | Feed it from streamed fields without reconstructing the body or final term map |
| Artifact construction | Shared leases, streamed encoders, immutable publication | Segment construction still retains `AdmittedDocument` values; remove the body-sized resident requirement |
| Updates | Governed preparation, mutation segments, target-bound retractions | Source conversion and old-version hydration/reanalysis still materialize documents and contribution state |
| Reading | Positioned, streaming segment decompression | Encoded document lines, decoded bodies, and owned public results remain document-sized |
| Host resources | `SearchGenerationAdmission` retains a governor permit through active work and immediate cleanup | Select bounded execution buffers from one shared allowance and qualify constrained operation |

Supporting contracts are [generation ownership](../SEARCH_GENERATION_CONTEXT.md),
[document frequency spill](../DOCUMENT_FREQUENCY_SPILL.md),
[analyzer ownership](../SEARCH_ANALYZER_WORKSPACE.md),
[token ownership](../SEARCH_TOKEN_OWNERSHIP.md),
[delta ownership](../SEARCH_DELTA_OWNERSHIP.md),
[selected hydration](../STREAMING_SEARCH_HYDRATION.md), and
[mutation segments](../SEARCH_MUTATION_SEGMENTS.md).

Current defaults include a 4 MiB lexical source limit, a 16 MiB encoded input
record limit, a 1,000,000 weighted-token document limit, and a 64 MiB materialized
hydration limit. They constrain different representations. Increasing one does
not establish support through the other boundaries.

## Required invariants

1. One source document remains one logical document, regardless of physical
   chunks, spill runs, memory profile, or segment layout.
2. Field selection and weights, identifier normalization, aliases, stopwords,
   CJK segmentation, term identity, tf, df, document length, matching counts,
   BM25 scores, and tie ordering MUST match the reference analyzer and scorer.
3. Every operation MUST reserve owned working capacity before allocation and
   hold that reservation until its final owner releases the capacity. A host
   permit MUST cover active work and immediate failure cleanup. Deferred cleanup
   MUST release worker/I/O admission and native handles, retain separately
   accounted ownership metadata and disk debt, and admit fresh retry workspace.
4. Input, staged artifacts, and publication MUST bind the same document and
   source version. A failed document MUST NOT become a partially indexed one.
5. Old readers MUST retain their complete generation through replacement,
   deletion, compaction, and cleanup. Resource pressure cannot revoke a pin.
6. Reader-local admission MUST remain authoritative. An artifact MUST NOT raise
   a reader's memory, term, decompression, or result limits.
7. No route may silently fall back to whole-body materialization when a
   streaming operation exceeds its budget. Failure is explicit and bounded.

## Capability and limit model

Separate limits into three groups:

| Group | Examples | May adapt with memory pressure? |
| --- | --- | --- |
| Input capabilities | Logical body bytes, encoded bytes, header/metadata size, token/count representation, host source safeguards | No; selected explicitly for the operation |
| Search semantics | Analyzer and embedding identity, field weights, finite term policy | No |
| Execution resources | Resident bytes, queue bytes, buffer sizes, workers, merge fan-in, spill space, I/O work | Yes, at safe ownership boundaries |

The target MUST NOT reject a document merely because its body exceeds 4 MiB.
It MAY reject a document that exceeds an explicitly selected input capability,
the finite term policy, or an indivisible working unit that cannot be admitted.
No replacement universal document-size constant is selected by this proposal.

Logical and encoded lengths use checked counters independently from `usize`
allocation sizes. Existing integer representation limits, including document
length and term-frequency counters, remain explicit admission boundaries. They
MUST NOT saturate into apparently successful search results. The current token
safety policy requires a separate reviewed selection for large-document profiles;
it MUST NOT be silently disabled to make a size benchmark pass.

The support claim MUST identify body size, largest identifier/analyzer unit,
term policy, token count, metadata/embedding size, operation budget, spill budget,
and supported lifecycle paths. Body size alone is not a capacity specification.

Source bytes and weighted-token limits form one host-selected
`SearchLexicalSourcePolicy`. The reader carries that policy into compaction,
mutation reanalysis and later writes, including calls with default build options.
Reopening still requires the host to supply the intended policy; persisted
artifacts cannot select or widen reader admission.

## Embedded source and read contracts

The recommended API direction is additive, using the existing writer, reader,
governor, task context, and publication protocol. Names and exact Rust signatures
remain review items; no new crate, runtime, or route-specific facade is needed.

| Proposed operation | Inputs and ownership | Result contract |
| --- | --- | --- |
| Stage a streamed document | Bounded owned header plus one-shot `Read` body, declared byte length, optional expected digest, and source provenance | Writer-owned immutable staged bytes; no caller body reference retained after the call |
| Prepare streamed mutations | Generation-bound mutation builder accepting ordered upsert sources and delete identities incrementally | One prepared update; no resident `Vec` of all document bodies |
| Open a verified body | Pinned reader, exact document/content-version identity, read limits, and task context | A bounded reader over completely validated staged output |

The header contains the document ID, title, metadata, and optional embedding.
These fields remain bounded resident units in the first implementation. A large
metadata value or title MUST be rejected before allocation exceeds its selected
limit; it MUST NOT be relabeled as a body or silently omitted from analysis.

A one-shot body is preferred over repeatedly invoking a host reader. The engine
captures it once into an owned stage, then uses that immutable source for
analysis and encoding. This avoids observing different bytes on later passes.
The host MUST keep a canonical source snapshot/version pinned through capture
when it claims canonical provenance. An engine-computed digest identifies the
captured bytes; it does not prove that a concurrently changing upstream source
was a consistent snapshot. Expected length/digest mismatches reject the stage.

Reads, EOF, UTF-8 validation, checksum accumulation, and staging writes MUST use
bounded buffers. Invalid UTF-8, short or excess input, I/O errors, cancellation,
and deadline expiry poison the affected candidate before publication. A length
declaration MUST NOT cause a body-sized allocation. The synchronous call applies
backpressure by ceasing reads; any asynchronous adapter needs byte-based queue
admission under the same permit. A blocking host `Read` is only cooperatively
cancellable between calls; no preemption guarantee is implied.

Existing owned input/output APIs remain supported wrappers with an explicit
resident-size requirement. They MUST fail on their own admission limits rather
than silently truncate content or change return types. A caller asking for an
owned 128 MiB body must admit that output even if indexing used less memory.

## Capture, encoding, and physical layout

The implementation SHOULD first preserve existing generation and document wire
grammars by streaming their fields. In particular, a hex-encoded line is not
permission to allocate an entire line: length/checksum validation, hex decoding,
UTF-8 checking, and copying can proceed incrementally. Record and segment byte
limits remain finite input/I/O policies, separate from working-buffer limits.

Before exposing a staged source to later passes, the capture MUST validate all
declared fields, lengths, counts, ordering, checksums, and EOF. Staging is private
and immutable after sealing. A counted file handle or verified owned path keeps
its resource and cleanup ownership for every pass; reopening an arbitrary host
path is not a substitute for source identity.

Content segment construction MUST stream body bytes into its compressor and
checksum sinks. Metadata and vector builders consume only the admitted header;
they MUST NOT keep an empty-looking `SearchDocument` that falsely represents
complete source content. Descriptor construction and document digests MUST use
the same full logical input as the reference path.

If current framing prevents bounded validation or repeated version-specific
reads, propose a generation-format revision separately, with explicit byte
layout, bounds, feature dispatch, and recovery tests. This specification does
not preapprove a new format. No active generation is rewritten in place, and no
canonical graph/relational WAL payload is added for derived search content.

## Exact analysis across physical chunks

Chunk boundaries MUST be invisible to the analyzer. A streaming field cursor
retains incomplete UTF-8 bytes, the unfinished raw identifier, and the previous
identifier part needed for adjacent-identifier phrases. Delimiters follow the
existing analyzer, including underscores and empty-delimiter behavior. Field
identity and token occurrence ordering persist until the complete field ends.

The initial strategy processes complete raw identifiers with the existing
analyzer. It admits identifier buffer growth and overlapping normalization,
deduplication, alias, CJK, and opaque native workspace before each allocation or
opaque call. The maximum resident analyzer unit is therefore input-dependent
and MUST be reported independently of total document size.

Continuous Chinese/Jieba runs MUST NOT be split using an assumed fixed overlap.
An oversized unit returns a classified minimum-working-set admission error,
with observed unit bytes and available budget. It does not skip the unit,
truncate tokens, replace the analyzer, or retry by allocating outside admission.
A future exact external-memory analyzer needs its own semantic proof; a new
approximate/chunked analyzer needs a separate identity and product decision.

The existing document frequency reducer MUST receive the original field and
occurrence sequence across every chunk. Unique-in-field phrases are deduplicated
against earlier ordinary occurrences too. Partial tf values are summed; df is
contributed once per document/term. Title weighting is preserved without a
duplicated token collection. Final document length and postings are emitted
through bounded reduction passes, never a reconstructed document-wide map.

## Resource ownership and adaptation

All active stages share the governor permit and the operation's existing memory
accounts. At every point, admitted live capacity MUST satisfy:

```text
input + queues + analyzer + frequency_state + encoding + codec_workspace
    + descriptors + publication + cleanup + reserved_progress <= operation_budget
```

This bounds modeled Rust capacities and qualified native allowances, not process
RSS. Host source buffers, shared dictionaries, allocator metadata, OS page cache,
and other excluded owners MUST be listed and measured separately. Producer
buffers and downstream result owners need their own host admission; ownership
handoffs cannot create an uncharged interval or double-book released capacity.

Reserve merge, decode, publication, and cleanup progress before filling data
buffers. Spill counters distinguish live disk occupancy from cumulative bytes
written; deleting a run does not refund cumulative write amplification. Source
stages, verified output stages, mutation runs, and merge overlap all consume
explicit disk quotas. FD use remains within the project's shared descriptor
budget, including cleanup and old-reader retention.

The [project descriptor contract](BRANCHING_STORAGE_SPEC.md#file-descriptor-budgets-and-branch-residency)
governs capture, analysis, encoding, validation, publication, and cleanup. Stage
ownership pins immutable bytes, not necessarily an open FD for the whole
pipeline. Idle handles may close while the stage remains protected; reopening
MUST revalidate the same identity through counted I/O. Active handles and
directory entries keep their permits until their native resources close.

Stage 1 MUST inventory simultaneous handles and nested temporary acquisitions,
including durability helpers and directory iterators. Reserve or borrow the
existing operation quota before a multi-step publication boundary; a helper
MUST NOT unexpectedly reacquire capacity already reserved for its caller. All
fallible validation/mapping needed to return a usable artifact belongs before
publication. A later synchronization failure still means uncertain publication.
Embedded feature/build configurations MUST select counted I/O; an uncounted
standalone backend cannot qualify this resource contract.

Project-budget and OS-limit rejection MUST retain their typed resource cause
through every wrapper. They MUST NOT be reclassified as corruption, poison an
otherwise valid reader, or authorize discarding valid artifacts. If cleanup is
denied mid-sequence, close releasable handles and retain the remaining private
artifacts with explicit cleanup ownership and disk accounting. Preserve the
primary operation error and expose cleanup denial separately; after a successful
commit, report cleanup pending without turning the commit into a failure.
Best-effort `Drop` cannot claim successful deletion: the explicit cleanup path
MUST report retained work and permit bounded retry after capacity returns. No
uncounted fallback, implicit budget increase, or busy retry is allowed.

Idle cleanup debt MUST NOT retain a completed operation's governor permit or
project FD domain. Its path and ownership metadata MUST remain admitted by both
the originating governor and its attached process-memory policy, in addition to
the allocation ledger. A separate memory-only reservation can conservatively
overlap active admission and MUST be acquired before private directory creation;
failed admission leaves no new stage. Shared active owners retain their work
resources until they finish. An active retry reacquires workspace and the current
descriptor domain. Bounded opportunistic
retry is part of subsequent same-root stage creation, including admitted
background compaction. Permanent failures retain evidence. The current fixed
registry's cross-root exhaustion boundary and safe post-crash ownership discovery
remain explicit #392 qualification work, not permission to forget retained debt.

Stage 1 prototypes MUST reproduce real exhaustion at scan, unlink, nested
publication, and reopen boundaries with a competing owner holding capacity.
Verify preserved published bytes/pins, typed denial, retained cleanup debt,
bounded counters, and successful retry after release. Error-constructor-only
tests do not qualify these paths. Track the related descriptor work in
[#829](https://github.com/nowledge-co/hawdb/pull/829),
[#839](https://github.com/nowledge-co/hawdb/pull/839), and
[#841](https://github.com/nowledge-co/hawdb/pull/841); their coverage is separate
from this proposed large-document qualification.

Profiles use one shared ceiling rather than one ceiling per worker:

| Profile intent | Physical behavior |
| --- | --- |
| Constrained | One active document, small buffers, low merge fan-in, earlier spill |
| Balanced | Larger buffers only from admitted capacity; bounded worker and queue counts |
| Throughput | Additional workers only after shared admission and foreground-latency qualification |

No numeric profile defaults are selected here. Pressure feedback reuses existing
host/process policy. Shrinking occurs between documents, flushes, or merge
passes; live buffers and opaque calls retain their leases until safe release.
Hysteresis prevents oscillation. Admission denial leads to bounded defer/retry
or a typed failure, never busy retry or another independent pressure controller.

## Replacement, deletion, and maintenance

Mutation preparation MUST resolve the exact visible content version from the
pinned generation, not hydrate whichever version currently has the same logical
ID. Replacement stages the new source and retracts the old version atomically.
Repeated replacement, delete, restore, and missing-ID behavior retain the existing
mutation contract and corpus statistics.

Old-version contribution extraction MUST stream validated source through the
same analyzer and external frequency reducer. Retractions and their encoding
MUST be spillable; retaining one full old body or its complete term set is not
a large-document delete path. Closure validation on reopen MUST use the same
bounded strategy, so successful deletion cannot create an unreopenable artifact.

The persistent in-memory mini-delta remains a bounded fast path. A document that
cannot fit it MUST take a governed, staged segment-update path or return an
explicit unsupported/admission error before changing state. Silent projection
invalidation followed by whole-corpus materialization does not satisfy this
contract. Which existing `SearchIndex` mutation entrypoints acquire this path
must be recorded in the delivery matrix before removing their source guard.

Checkpoint and compaction MUST merge selected immutable versions without
materializing complete bodies or term maps. Work selection, intermediate disk,
and retained generations remain budgeted. A single selected document may exceed
the operation's RAM, but not its explicit source, I/O, or disk-work limits.
Maintenance denial cannot lose acknowledged canonical writes or falsely advance
the projection's complete-through watermark.

## Reopen, queries, and result delivery

Reopen MUST validate document/version references, frame metadata, analyzer and
embedding identity, lengths, and required capabilities under reader-local
limits. It MUST NOT require the original writer's full build reservation or
materialize the body merely to discover its size. Lower-memory readers may
accept the same artifacts while selecting smaller execution buffers.

Lexical, vector, and hybrid candidate evaluation use postings and metadata
without loading full bodies. The facade needs an explicit candidate/metadata or
bounded-preview result mode for large-document search; an existing API that
returns complete `SearchHit` bodies retains its full-output admission contract.
Preview bounds MUST be part of the request/result contract, with source offsets,
and MUST NOT be presented as complete content or alter ranking.

For a given lexical, vector, or hybrid query strategy, all result modes MUST
consume one shared candidate/filter/scoring/top-k implementation and the same
generation-pinned ordered candidate result. Modes select only the downstream
metadata, preview, or full-body materializer; they MUST NOT duplicate scoring,
rerank, drop hits, or refill top-k to satisfy a payload budget. Admission failure
remains explicit. Tests MUST compare IDs, scores, matching counts, version
visibility, and tie order across successful modes, including ACL/metadata
filters and replacements. Full-body admission denial on a large result is a
separate expected outcome, not a different ranked result.

For streamed full-content reading, the recommended first delivery verifies the
entire required source segment while writing only the selected body into an
admitted temporary output. It then returns a sealed reader. Corruption in an
unselected suffix still prevents successful output admission. Returned readers
retain their pin, disk accounting, and cleanup owner until drop.

An arbitrary external sink cannot roll back bytes already consumed. Therefore
no fallible scan may expose unverified source prefixes as successful output.
Even a verified reader can encounter later consumer/I/O errors: callers must
observe successful EOF/completion before treating a transfer as complete. A
transactional sink or independently authenticated chunk format is a separate
API decision, not an implicit guarantee of this proposal.

## Publication, cancellation, and recovery

The candidate lifecycle is:

```text
admitted -> capturing -> source_sealed -> artifacts_prepared -> validated
         -> publication_fence -> published -> cleanup_or_retention
```

Every pre-publication failure leaves the previous active generation unchanged.
No partial document, postings, retraction, source watermark, or manifest may be
published. The existing publication lease, expected-generation check, identity
checks, durability ordering, and manifest-last commit fence remain authoritative.

Cancellation/deadlines propagate through input, analysis, spill, reanalysis,
encoding, validation, and cleanup admission. Opaque calls are checked before and
after execution; their duration is reported separately. Analyzer workers join,
including TLS destruction, before releasing workspace and host admission.

Cancellation after a completed publication fence cannot undo the commit.
Ambiguous post-rename errors retain evidence; a lost response is not proof of
rollback. Failed cleanup retains accounted artifacts for an explicit bounded
retry. Process interruption, torn writes, lost unsynchronized writes, and
reordered persistence are separate qualification cases. Model results MUST state
their platform assumptions and MUST NOT imply physical power-loss certification.

Formal qualification is required before enabling the new publication lifecycle.
Stage 1 MUST map these transitions to the
[existing TLA+ practice](../tla/README.md), reusing or extending
[search mutation publication](../tla/HawDBSearchMutationPublication.tla) and
[validated result delivery](../tla/HawDBValidatedResultDelivery.tla) where their
abstractions fit. The former models durable-closure selection and pinned
reclamation; the latter models validation before observable delivery. Neither
currently proves streamed capture, FD denial, deferred cleanup, or ambiguous
post-rename outcomes for this lifecycle.

Before Stage 2 enables publication, a bounded model and Rust transition mapping
MUST cover sealing/validation, stale publication, cancellation on both sides of
the fence, uncertain publication/recovery, reader pins, and cleanup denial/retry
with retained ownership. Check no partial active closure, no reclamation of
protected bytes, no premature result exposure, and no release of live resource
ownership. Negative controls MUST violate each corresponding safety property;
reachability witnesses MUST exercise both a committed-but-unacknowledged result
and cleanup that succeeds after resource pressure clears. Stage 3 extends this
evidence to replacement, deletion, and compaction.

Model receipts MUST identify source/configuration, state bounds, assumptions,
invariants, and counterexamples. Eventual cleanup requires explicit fairness
and resource-availability assumptions; indefinite admission denial permits
retained debt. Atomic file/selector abstractions do not prove torn-write or
write-reordering behavior: any abstracted persistence boundary requires separate
fault-injection/refinement evidence under the declared platform assumptions.
This proposal adds the modeling gate, not a new model or a claim of verification.

## Diagnostics

Typed reports SHOULD expose the selected path and phase; logical/encoded source
bytes; maximum input/analyzer unit; operation and progress reservation peaks;
buffer/queue/worker peaks; live and cumulative spill bytes; merge passes;
hydration and reanalysis bytes; cancellation checkpoints; generation identity;
and cleanup deferrals. Process RSS/page faults are independent measurements.

Errors must distinguish source/record/term/token capability, minimum analyzer
working set, memory admission, spill quota, FD admission, source identity,
corruption, stale publication, cancellation, and unsupported result capability.
Use the existing error taxonomy where applicable; new public variants require
review. Diagnostic payloads MUST NOT include source text, tokens, private paths,
or document identifiers unless the host explicitly requests permitted detail.

## Verification and release gates

Qualification MUST exercise the public embedded path, not only private helpers.
Every receipt binds source revision, target/features, input hash/generator seed,
analyzer/embedding identity, resource policies, and actual selected test counts.

| Dimension | Required evidence |
| --- | --- |
| Size | 4 MiB minus/equal/plus one, 8/32/128 MiB bodies, encoded-record boundaries, and at least one body larger than the total operation reservation |
| Shape | Repeated words, high distinct-term count, long identifiers, uninterrupted Chinese, mixed Unicode, aliases, stopwords, metadata, and empty fields |
| Physical chunks | Every boundary of small multibyte fixtures, randomized short reads, splits within UTF-8/identifiers/phrases, and multiple buffer profiles |
| Semantics | Complete term/tf/df/length/statistics equality; exact matching counts, BM25 scores, ordering, ties, and hybrid version visibility; shared candidate results across metadata/preview/full-body modes |
| Lifecycle | Initial import, append, repeated replacement, delete/restore, old reader overlap, checkpoint, compaction, low-memory reopen, and body transfer |
| Resources | Exact/one-short admission, concurrent operations sharing one governor, slow consumers, native allocation bounds, disk/FD denial and retry, and retained cleanup debt at actual mid-sequence exhaustion |
| Failure | Short/excess/invalid input, source mismatch, partial I/O, corruption including late tails, cancellation, deadlines, unwind, and interrupted publication |
| Formal protocol | Bounded lifecycle model, Rust transition mapping, negative controls, and reachability witnesses for ambiguous publication and deferred cleanup; stated persistence/fairness assumptions |

The 8/32/128 MiB points are qualification cases, not new product ceilings.
Separate admitted continuous-CJK cases from intentional minimum-unit rejection.
Whitespace-only size fixtures isolate admission boundaries but do not qualify
analysis or high-cardinality spill. Default/minimal/text/vector/ACL feature
behavior and supported native platforms must be explicit.

Use a frozen independent analyzer/scorer on feasible complete inputs and an
independent streaming/count oracle for larger fixtures. Negative controls MUST
detect chunk-state reset, lost partial tf, duplicated df, source-version mixing,
uncharged body/queue allocation, partial publication, and hidden materializing
fallback. Allocation measurements include source producers and result consumers
separately; preallocating a large fixture outside the measurement window cannot
prove larger-than-memory input support.

Retain exact/one-short and semantic regressions in ordinary suites. Seeded fuzz
campaigns remain local Bazel targets, not new CI jobs. Implementation changes
require formatting, strict Clippy, affected feature tests, and the routine fuzz
command from `AGENTS.md`; do not weaken budgets, corpus, assertions, or timeouts.

Measure throughput, foreground p95/p99, peak ledger/RSS, spill/write amplification,
and cancellation response on repeated matched workloads before choosing defaults.
The original complete, identical-corpus acceptance for
[#206](https://github.com/nowledge-co/hawdb/issues/206) remains independent;
synthetic large-document fixtures do not replace it or production Mem readiness.

## Delivery stages and review decisions

| Stage | Entry condition and bounded scope | Exit condition |
| --- | --- | --- |
| 1. Contracts and proof | Review source/result ownership, analyzer minimum units, existing count limits, format feasibility, and formal-model scope | Minimal streaming capture/analyzer/read prototypes demonstrate bounded ownership and exact chunk semantics; descriptor exhaustion/cleanup proof, shared scoring boundary, and lifecycle transition mapping are recorded; no default changes |
| 2. Build and read | Approved additive interfaces, existing shared ledger, and checked publication/result-delivery model before enabling publication | Public initial-build and verified-body paths process a body larger than their reservation, including corruption/cancellation/cleanup and result-mode equivalence |
| 3. Mutable lifecycle | Stage 2 plus existing target-bound mutation protocol and extended model/refinement evidence | Replace/delete/reopen/checkpoint/compaction and the supported `SearchIndex` paths pass the same memory and semantic gates |
| 4. Profiles and default transition | Complete supported-path evidence and host/resource measurements | Review finite input policies, token limits, and reader defaults together; remove the 4 MiB-only rejection only on qualified paths |

Each implementation slice targets `main` directly and references #392. Completed
prerequisites are reused, not rebuilt. A stage may land with the existing default
guard intact; neither its merge nor this specification closes the parent issue.

Decisions requested before implementation:

- Accept the recommended larger-than-operation-memory target and one-shot input
  capture; alternatively, explicitly limit the first delivery to owned inputs.
- Accept unchanged analyzer semantics with an explicit minimum-unit rejection
  for an unadmittable identifier/Jieba run.
- Review additive streamed mutation and verified-body/candidate result contracts,
  including temporary-disk costs and transfer completion semantics.
- Prefer existing wire grammars; require a separate layout review if the bounded
  proof cannot preserve them. Select default capability/profile values only
  after lifecycle qualification, not in this specification-only change.
