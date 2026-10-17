# Consumer-bound installed activation: proposed protocol

This is a design and finite-model result for the next native C-ready/E session.
The current implementation does **not** refine the positive model. The
[native operation-custody primitive](CONSUMER_OPERATION_CUSTODY_PROOF.md) implements
only target exclusion and clean-checkpoint admission. No consumer activation API
or persistent head format is implemented here. The
[acceptance ledger](../specs/MVCC_VERSIONED_PUBLICATION_SPEC.md) remains binding.

## Why the existing prepared writer cannot be the consumer activation path

[SearchIndex opening](../../crates/search/src/lib.rs) loads a complete resident
snapshot and compares it with the immutable manifest. The registered path then
validates descriptor/lexical artifacts and standalone resident vector identity.
[ConsumerProjection](../../crates/search/src/consumer.rs) retains a root lease
for its lifetime, permits mutation only inside apply, and obtains its checkpoint
receipt by hashing the resident snapshot. The [facade registry acknowledgement](../../src/api/search_projection_consumer/mod.rs)
binds source epoch, registration/projection/checkpoint IDs and snapshot digest.

The generic [prepared generation](../../crates/search/src/out_of_core/generation_writer/prepared.rs)
publishes immutable artifacts and a manifest. It does not write the registered
resident snapshot or its checkpoint receipt. Removing its registered-root check
would leave D's resident snapshot beside E's manifest, which reopening rejects.
Keep that rejection. Retaining the resident checkpoint format for the new path
would require full-N serialization after E capture and full-N reload, conflicting
with the intended incremental and memory bounds.

The new path needs a generation-backed consumer read view, a small selector and
its operation receipt, plus matching registry/recovery/GC behavior. This can be
an additive typed library path; it must not silently make existing resident
SearchIndex methods return empty data for an out-of-core consumer. Host reads
must use a genuine immutable view with the required query/hydration capabilities.
No new crate is justified merely by this format boundary.

## Lifecycle and custody

| Phase | Authority and work | Required outcome |
| --- | --- | --- |
| Admission | Validate a completed target checkpoint, source/target identities and mapping revision; reserve one target operation slot. | Refuse dirty, uncertain or competing work before taking candidate ownership. |
| C-ready | Pin C and complete its owner/document mapping, immutable owner indexes and large artifacts under one actual governor. | C remains private; the original active D remains the expected target base. |
| Capture/close E | Capture E and its eager read scopes, qualify C..E continuity, consume native owner End and all reverse fanout. | One stage owns complete final-E mapping and vector authority; no host Done flag substitutes. |
| Invisible install | Build only affected E artifacts from C, encode the complete immutable manifest and metadata, install/verify/synchronize their closure and prepare the reader. | All large I/O, validation and data-structure construction stay outside SourceOwner. |
| Fenced selection | Under SourceOwner, revalidate source instance/schema/scopes, binding/target incarnation, target base and source durability; atomically select the prepared view/receipt and exchange the immutable read view. | Release SourceOwner after that visibility point; return an owned pending-durability token retaining target operation custody. |
| Head barrier | Outside SourceOwner, finish the selected head's durability barrier while retaining the target operation slot. | No acknowledgement precedes the barrier; source writers may run while it completes. |
| Return/dispose | Return the new immutable read view and old/candidate cleanup custody on both success and rejection. | Release SourceOwner before destroying large readers, stages or reservations. |

C-ready must finish the full-N body build, owner association indexes and large
artifact work before E capture. The current manifest is a flat segment vector;
serializing/copying all C descriptors after E can also widen the legitimate
post-E conflict window even when performed outside SourceOwner. Native design
needs structurally shared immutable metadata or measured, admitted descriptor
work that still satisfies the original large-workload writer gates. Moving a
full-N rebuild outside the fence does not make it an incremental E path.

The target's lifetime lease excludes external owners. It does not serialize
its own apply/checkpoint/publish-directory/cleanup operations while a preparation
token is outside SourceOwner. Use one engine-owned operation custody capability;
all competing internal operations must return typed Busy/deferral before mutation.
Do not clone a lease and assume exclusion. Foreground canonical commits remain
independent: a deferred Search maintenance operation must not convert an already
committed source write into an ambiguous application failure.

The completed receipt must be distinguished from dirty legacy state. In the
current code checkpoint_uuid changes before fallible checkpoint I/O, while the
previous receipt can remain. An arbitrary in-memory binding is not a completed
checkpoint. A directory move or unregister/rebind is also a target-incarnation
change, even if its visible path/name later returns to its old value. Require an
engine-owned non-reuse contract for incarnations, with exhaustion refusing;
neither path equality, a content hash nor Arc identity alone supplies that contract.
Persisted identity/recovery and process-local proof lifetimes need distinct rules.

## Candidate-directory layout for native consumer implementation

The [native root binding check](NATIVE_PUBLICATION_ROOT_IDENTITY_PROOF.md)
strengthens acquisition of the existing lease. It does not capture a logical
target incarnation or bind worker output to source C/E; the native constructor
and fenced selection still need that context.

The [dependency-install backend split](PRIVATE_GENERATION_INSTALLATION_PROOF.md)
is an internal seam only. Existing wrappers still immediately select their
component head under the caller's publication exclusion; they do not construct
the candidate-directory or consumer-selector lifecycle described below.

Use an engine-owned candidate directory as the artifact namespace for the new
generation-backed consumer format. Keep D's consumer selector in the registered
root unchanged throughout C construction and C..E closure. A private component
manifest inside the candidate directory can describe C, then E; neither manifest
selects C through the consumer root. The final small consumer selector references
only the complete E view and its receipt. Its expected target remains the original
D, independently of the private component manifest's generation.

This layout keeps existing segment manifests' bare filenames relative to one
component directory. It avoids scattering an unselected C's aliases into D's root,
where ordinary generation cleanup conservatively preserves future generations.
The private C builder and E updater can use the existing immutable component
machinery without bypassing registered-root rejection: only their engine-owned
private component directory is unregistered. That directory is not a new public
production integration point or a caller-supplied alternate target. The native
constructor must derive it from trusted consumer operation/root authority and
validate source, binding and target incarnations before selection.

Directory ownership is still substantive work. The candidate owns all C/E
artifacts, owner/association indexes and cleanup admission. Temporary writer
stages must remain flat siblings with independently retained cleanup owners;
propagate the existing internal staging-root policy through E updates rather
than leaving nested directories that the current flat stage remover cannot
reclaim. Retain C's complete artifact closure until the native E input is closed;
a borrowed reader or open file handle alone does not protect path-based validation
from component cleanup. A rejected attempt transfers all remaining directory and
sibling-stage debt to bounded cleanup custody before releasing its reservations.

Selection transfers the directory from preparation custody to the committed
view/receipt roots. An uncertain selector result keeps it in unresolved custody,
never ordinary stage Drop. Recovery and receipt retirement must enumerate these
roots and charge or refuse unresolved on-disk debt before admitting more work;
a process-local cleanup registry alone cannot bound or identify orphaned output
after a crash. Root selector names must be validated engine-generated directory
identities, not arbitrary paths. The directory and all referenced dependencies
must satisfy their namespace/data barriers before a durable selector can cover
them. This changes the consumer format and needs its own recovery qualification.

Opening the private component avoids accidentally reading D's active head, but
does not by itself govern descriptor decoding or reader residency. Complete that
admission before C-ready, retain the same governor through E and transfer live
view accounting explicitly on success. Post-E flat-manifest copying/encoding
remains subject to the original large-workload gates. No private-directory
constructor, consumer selector, governed reader, durable cleanup ledger or native
C..E authority is delivered by the current preparation-only token. The finite
activation model abstracts file identities and does not prove this directory
layout's physical implementation.

## Reader ownership and admission refinement

The native reader foundation groups each immutable segment's descriptor, layout,
lexical reader, optional vector projection and three payload handles inside one
`Shared<SegmentReaderContents>`. Refresh and cleanup reuse clone that shared
owner; they borrow its children. A future segment capacity owner must be the
last field of these contents, rather than a field of the outer reader. Otherwise
an escaped segment clone could outlive the wrapper that refunded its metadata.
Search's `Shared` alias now reuses the internal `hawdb_core::SharedAllocation`
primitive, also used by vector projection. It frees its Arc allocation before
dropping extracted contents and exposes neither weak references nor raw Arcs. This representation is implemented;
a segment retained-memory owner and a fully admitted reader constructor are not.

The required resource invariant is:

```
for every live payload allocation a:
    exists an unreleased reservation r from the originating governor
    such that a is covered by r
and total live admitted capacity <= that governor's admitted memory
```

The inductive ownership argument has four cases. Reserve before allocation;
failed reserve creates no payload. Clone only increments ownership and does not
refund capacity. Dropping a nonfinal reference preserves the same payload and
reservation. The final drop first frees shared allocation metadata and payloads,
then releases the reservation. Wrapper moves and rejected activation must obey
the same cases. The reservation is `RuntimeRetainedMemory`, obtained from the
existing operation permit before constructing retained state. It does not retain
execution slots. An operation's working ceiling overlaps this extra reservation
until work finishes; failure to fit that overlap must refuse privately. A local
`QueryMemoryLease` alone cannot establish the originating-governor invariant.
This is a refinement obligation, not a TLC-checked native lifetime theorem.

Root-manifest preflight must cover both `Vec<SegmentManifest>` and
`Vec<MutationRunManifest>`, including input rejected only after deserialization.
The schema-aware capacity uses the larger owned record size, two vectors and
per-record validation-set capacity in the existing checked JSON bound. The
bound counts opened containers, so it also covers serde's compact sequence
encoding of a struct. Passing zero record size does not: a 65-record malformed
segment fixture requested 85,214 bytes with only 27,444 admitted on the retained
pre-fix allocator run. Checksum or filename rejection happens too late to avoid
that allocation. Streaming checksum verification avoids a second serialized-body
buffer. The three partitioned/discovery call sites and the append/mutation/
compaction publication branches now pre-admit this decoding. Publication keeps
the decode lease alive after moving the vectors into the final manifest.

The bound depends on the fixed schema, pinned serde/Rust vector-growth behavior
and conservative B-tree node allowances. It is not a generic bound for arrays
of scalar values, an allocator-independent theorem, or an RSS measurement.
Regressions measure requested live allocation peaks for compact segment and
mutation records, assert refusal at one byte below the observed admission peak,
and assert full ledger release. These tests complement the argument; they do
not qualify descriptor/layout/lexical/vector decoding, cumulative visibility,
FD retention, same-governor handoff, physical recovery or C-ready as a whole.

The partitioned metadata join now returns one private `Prepared` owner containing
its merged manifest and input/decode/growth/reference leases. Reservations are
attached before decoding or growing the payload they cover. The manifest is the
first field, and only an immutable accessor escapes to the installer. Thus both
success and error destruction free the joined payload before its leases; the
installer also transfers the original staged-input lease rather than relying on
an outer local's lifetime. The pre-fix native regression left 1,792 requested
bytes live after result destruction while the extra admission was already zero.
The repaired regression exercises both ordinary drop and caller unwind, including
unchanged active-head bytes. This proves that bounded join's observed lifetime
boundary; it does not establish the still-unimplemented retained-governor reader.

### Vector payload verification before mapping

The native vector opener now verifies the held file before creating its mmap.
`artifact/stream_verify.rs` reads each validated contiguous segment as its ID,
scale, offset and code columns, using one 8 KiB buffer. It does not allocate a
segment-sized buffer or scan mapped payload pages to establish integrity.

The loop argument maintains consumed-prefix segment/aggregate CRCs, the last
observed ID, and the least pending `(row, field)` header violation. Choosing the
least pair preserves the mapped verifier's row order even though columns arrive
separately. A segment checksum failure precedes that segment's header errors;
the aggregate checksum follows every valid segment. Complete success therefore
retains both checksum layers, global ID ordering, finite/nonnegative scales and
finite offsets. Only that success reaches the map call. The validated manifest
and payload/footer boundary are premises; this is a source argument rather than
a mechanized Rust refinement theorem.

An independent mapped-verifier oracle compares both supported bit widths across
valid data, signed zero, checksum failures and deliberately conflicting header
errors. A short-read source verifies the 8 KiB request bound, one initial seek
and exact consumption of the payload before the footer. The pre-fix native
corruption regression reached the map hook before failing checksum validation.

This bounds the payload verification buffer, not all opening or serving memory.
Manifest parsing/ownership, same-governor handoff, cancellation/I/O admission,
query-time mapped-page residency and native file-root/GC custody remain separate
obligations. It does not establish a complete governed reader or C-ready.

### Vector metadata admission and retained ownership

`FileProjection<Memory>` now shares its complete immutable path, manifest and
mapping, with two concrete, non-cloneable capacity owners as the last fields.
Clones increment the shared reference count without copying metadata or admitting
new backing. `SharedAllocation` frees the control allocation before dropping
contents, and contents free all payloads before either capacity owner. Writer
completion supplies the final display path at construction; it no longer mutates
a shared projection after opening the temporary file.

The internal admitted constructor consumes an already admitted immutable file.
Its display path is metadata, not file-binding authority. All footer, manifest,
payload and map operations use that same handle. Path/allocation capacity precedes
the path copy; encoded capacity precedes the manifest Vec. A borrowed JSON pass
counts containers and the largest token. The fixed schema has one vector of
struct records: final capacity/string bounds are retained; vector-growth overlap,
escaped-token scratch and diagnostics stay transient. Both map and sequence
record encodings count, including data refused by later validation. I/O guards
span each physical read; interrupted reads recheck cancellation before retrying.

The governed generation writer implements this bridge using its existing build
ledger, task and governor. Successful receipts own `QueryMemoryLease` followed
by `RuntimeRetainedMemory`, with conservative coverage for the three fixed ledger
accounts. They contain no execution permit. This currently charges that fixed
metadata allowance per receipt; a full reader can amortize it only while preserving
shared ledger lifetime. Allocation-tracked native tests check encoded/record
refusal before large allocation, complete refunds, allocation-free clones, fresh
query admission after producer closure, and concurrent final-clone disposal.
The [retained-capacity protocol model](SHARED_READER_CAPACITY_PROOF.md) checks
separate query refund/account destruction, shared producer/reader ledger
lifetimes and four negative controls. It assumes the Arc extraction contract
and symbolic capacity bounds; it is not a Rust or allocator refinement proof.
Returned diagnostic objects use the existing error API and do not carry retained
admission. Their lifetime after an operation ends is outside this component.

This implements the vector component, not the complete governed reader. Ordinary
reader opening still uses the unaccounted legacy entry until descriptor, layout,
lexical, visibility and root ownership are all integrated. Neither the generic
admission interface nor its receipts grant source completeness, native cleanup,
C-ready or activation authority. All original mapped-page residency, physical
I/O/RSS, actual Mem, incarnation/receipt/recovery and writer/fairness gates remain.

## Durable metadata and retry contract to implement

One immutable view record must identify the source E and native binding/target
incarnation, source durability prerequisites, the complete preinstalled artifact
manifest, and an operation identity/result receipt. A small durable selector
must choose that view/receipt together. The full manifest must be precomputed
and immutable; the existing active_identity decoder and manifest rewrite are
content-dependent and cannot remain inside the short activation fence.

Artifact synchronization must cover file contents, installed dependency names
and required directory barriers before the head barrier can acknowledge them.
Source durability must cover E before the target selects E. The selected token
then completes the target head barrier outside SourceOwner, retaining the root
lease and operation slot. Releasing the source fence after atomic selection does
not undo the already-validated linearization point when later source writes run.
It does not permit target mutation or registry adoption under a different binding.
Only the view exchange occurs under the source fence; completion returns that
operation's historical result, rather than reinserting it into a later binding.
Source durability must cover E before the target makes E durably authoritative.
A relaxed source mode does not authorize a durable projection beyond recoverable
source. If the head barrier cannot establish a definite outcome, return typed uncertain
commit custody and retain roots/evidence; do not report rollback or an empty
consumer. The model includes an uncertain return and disposal outside the source
fence. Native registry/in-memory-view composition still needs implementation.
A lost response can leave the complete new view committed; cancellation
cannot roll it back after selection's completed durability barrier.

A current-head receipt alone is insufficient for repeated operations: after
publishing operation 2, operation 1's lost response may still be retried. Native
implementation needs a bounded retained receipt ledger, admission refusal before
its capacity is exhausted, and an explicit retirement/retry horizon. Removing a
receipt must not permit an expired operation ID to be re-executed. One possible
contract uses durably reserved, non-reused operation ordinals and a durable
retired-through floor; another needs an equally strong argument. Retirement and
GC must preserve the previous durable selector until the retirement barrier
completes. The model below checks only one operation. A separate
[bounded receipt-ledger proposal and model](CONSUMER_RECEIPT_LEDGER_DESIGN.md)
checks multiple operations and explicit retirement. Neither model implements the
ledger or proves their native composition; those remain prerequisites for
bounded durable idempotency.

Recovery validates the selected complete view, binding, source coverage and
receipt. Missing/inconsistent dependencies or an uncertain selector fail closed,
retaining evidence. They never authorize an empty ready consumer or deletion
of an earlier completed generation. Unselected installed candidates remain
private garbage only after their operation custody is released and a validated
root proves they are not referenced.

## Finite protocol model and correspondence limits

[HawDBConsumerInstalledActivation](HawDBConsumerInstalledActivation.tla) models
one consumer operation, C-ready and native input closure as premises, two source
epochs, a binding change-away-and-back sequence, installed files, source/data/head
barriers, cancellation, a response that may be lost, cleanup, detected uncertainty,
a crash losing unsynchronized state, and recovery/retry. The root tuple update
is atomic in this model; implementing it is an obligation, not an existing fact.

The direct pinned TLC positive run checked 30,712 generated / 7,017 distinct
states at complete depth 22. Thirteen isolated faults and four reachability
witnesses produced the expected named invariant counterexamples, 18 checks total:

| Fault or witness | Expected invariant counterexample |
| --- | --- |
| Bypass operation exclusion; release slot early | SlotExclusion |
| Trust a dirty legacy checkpoint | CleanAdmission |
| Decode the full manifest or drop heavy custody inside the fence | NoHeavyUnderFence |
| Compare only the repeated visible binding value; drop the source fence | ValidSelection |
| Publish the view and receipt separately | TupleCoherent |
| Omit data or source synchronization | DurableOrdered |
| Acknowledge before synchronizing the head | AckOrdered |
| Reclaim files with uncertain publication | UncertainRetains |
| Treat invalid recovery as an empty ready view | RecoveryFailClosed |
| Lost response followed by receipt retry | NoLostResponseWitness |
| Accept an unrelated post-E source write | NoUnrelatedWitness |
| Recover the selected complete consumer view | NoReopenWitness |
| Admit a source writer between selection and the head barrier | NoBarrierWriterWitness |

A witness is a deliberately false invariant used to prove reachability, not a
failure of the positive protocol or a liveness/fairness proof. Tool jar SHA-256:
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
The corresponding 18 selected Bazel targets also passed. Retained action
evidence matches the current module/configuration bytes and pinned tool hash.

The source guard is an abstract relevant-domain stamp. Actual C/E closure,
physical owner identity, source/schema continuity, closed mapping, parent
preimages/fanout, final document uniqueness and independently sourced vectors
remain separate native proof obligations. Expected-head CAS and external-writer
exclusion also require the native target authority; the model assumes the retained
root lease and does not simulate an external publisher. Unknown state represents
detected uncertainty, not a filesystem byte-level corruption model. Synchronization
abstracts successful barriers, without proving storage/device behavior. The heavy
work counter expresses a protocol placement rule; it is not an RSS, I/O, lock-time
or throughput measurement. All original actual-Mem, power-loss, resource,
1/4/8-writer latency/fairness and group-fsync gates remain open.

The first draft had an indentation parse error; it was corrected before the
positive and fault runs above. Its adverse log is retained. No invariant was
removed to obtain the positive result. Model guard refinement made detected
uncertainty an explicit selection refusal and made cleanup obey the actual slot
state, rather than allowing the early-release fault to bypass cleanup independently.
