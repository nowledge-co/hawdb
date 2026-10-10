# Zero-Copy Columnar Interchange, Graph, and FTS Specification

## Status and scope

Status: exploratory design proposal, not an active production contract. No new
API, retained-batch producer, SIMD backend, or cross-language zero-copy path is
qualified by this document. Publishing it does not change existing APIs or
impose new requirements on ordinary binding changes.

Related measurement work is tracked in
[issue #976](https://github.com/nowledge-co/hawdb/issues/976). Its benchmark and
bulk-boundary acceptance criteria remain open. That issue excludes execution
engine changes; the producer-layout, SIMD, graph, and FTS work considered here
requires separately scoped implementation issues and workload evidence.

MUST, MUST NOT, SHALL, SHOULD, and MAY below express prospective acceptance
criteria for an implementation explicitly adopting a named capability from
this proposal; SHALL has the same strength as MUST. They do not override the
active contracts in the specification index. Adoption requires an implementation
review identifying its eligible plans, types, adapters, and qualification
evidence; unadopted capabilities remain proposals.

For adopted capabilities, HawDB SHALL own its in-memory batch representation,
bounded pull protocol, applicable kernels, and resource lifetimes. Compatible
columns SHALL interoperate through the Arrow C Data / C Stream interfaces and
Python PyCapsules without
requiring an Arrow SDK in the engine. Native consumers MAY use the smaller
HawDB batch interface, including its explicit row selection.

The target is **zero payload copies**, not fewer copies or a faster serializer.
JSON, base64, Arrow IPC, per-row language objects, and an intermediate owned
`Vec<Value>` MUST NOT be required by the strict result-delivery path.

The core proposal covers embedded Rust, C ABI, purego Go, and Python consumers.
Numeric SIMD, graph identities/frontiers/path views, and FTS candidates/content
views are independent extensions, not prerequisites for a scalar host-boundary
pilot or for closing #976. This proposal does not authorize a durable-format
change, a second canonical column store, a new graph algorithm, or approximate
search behavior. Existing release, query, durability, and recovery contracts
remain authoritative.

Related contracts:

- [Vectorized morsel execution](VECTORIZED_MORSEL_EXECUTION_SPEC.md).
- [Row-page storage and demand-paged indexes](ROW_PAGE_AND_DEMAND_PAGED_INDEX_SPEC.md).
- [Embedded runtime](EMBEDDED_RUNTIME_SPEC.md).
- [Public API boundary](QUERY_FIRST_PUBLIC_API_SPEC.md).
- [Compact lexical postings](LEXICAL_COMPACT_POSTINGS_SPEC.md).
- [Large-document lifecycle](LARGE_DOCUMENT_LIFECYCLE_SPEC.md).
- [Selected-document hydration](../STREAMING_SEARCH_HYDRATION.md).

## Existing foundations and gaps

The executor has typed `ColumnVector` values, validity bitmaps, adaptive
`Selection::All/Bitmap/Indices`, shared-column projection, and numeric kernels
in its restricted numeric fragment. Eligibility is the existing one-label
`SeqNodeScan -> PropertyCompare -> Project -> optional Limit` shape with an
integer/float literal comparison and a public numeric property descriptor;
see the vectorized morsel contract. These building blocks do not establish a
general columnar result producer for ordinary queries.

`crates/executor/src/numeric/lending.rs` defines a private `pub(super)`
`LendingBatchCursor` that lends `NumericNodeBatch`. Its borrowed view expires
at the next mutable cursor step, allowing producer buffer reuse. This lifetime
MUST NOT be exported unchanged to a retaining foreign runtime. The admitted,
immutable retained-slot producer proposed below is new implementation work,
not an existing general cursor that only needs an ABI wrapper.

Collected `QueryRows` already owns an `Arc<QueryRowStorage>` containing flat
`Vec<Value>` storage, and `QueryRows::value` returns an allocation-free
`ValueRef`. This can reduce binding-side clones without making the storage
columnar. Python currently clones values into eager dictionaries; the Go C ABI
uses JSON and an owned buffer copy. Neither boundary is strict zero-copy.

Current UTF-8 columns use `Arc<[String]>`, booleans use byte-per-value storage,
and dynamic columns use Rust `Value` objects. Standard Arrow UTF-8 offsets and
packed booleans cannot directly share these layouts. Arrow view layouts do not
remove the need to construct their inline values/prefixes. Typed loops also do
not prove that an explicit SIMD backend executes.

| Current result shape | Gap before strict delivery can be claimed |
| --- | --- |
| Numeric fragment's integers, floats, and node identities | Qualify a retained producer, owner/account transfer, and each host adapter; no foreign zero-copy path exists today |
| String/binary application results such as titles and thread IDs | Add a qualified producer-native layout or owned native spans; current collected rows are not a columnar producer |
| Byte booleans or noncontiguous selected results | Native layout/selection views may have their own capability; packed/dense Arrow export requires a compatible producer or explicit materialization |
| Lists, maps, and mixed dynamic values | Define and qualify immutable recursive views and schema/type mapping; the initial strict pilot does not support them |

An initial numeric pilot therefore cannot serve most Mem-shaped results and
MUST NOT be presented as completing #976 or providing general query support.

The implementation SHALL extend existing executor and facade ownership.
Creating another crate, replacing the durable row layout, or exposing an
internal crate as the production integration surface is not a prerequisite.

## Measurement gate and API choices

Before selecting a production boundary change, record the #976 baseline for
Python and Go against an engine-only Rust lower bound at a frozen revision.
Use all five cases (`select`, `point`, `fill`, `fill_bulk`, and `wide`), sizes
`1e3`, `1e5`, and `1e6`, fixed-seed data, and in-memory and warm file-backed
runs. State the release build, durability setting, CPU/toolchain, consumer work,
checksums, wall time, conversion profile, allocations, and peak RSS. Include
representative strings, lists, and maps instead of only numeric-friendly rows.
This document supplies no such baseline or measured end-to-end speedup.

The comparisons should distinguish reduced per-call/per-value overhead from
payload copying. One bounded conversion per batch may outperform the existing
JSON/dictionary paths; zero-copy is not assumed to dominate point queries or
to justify changing executor layouts. A narrow zero-copy pilot may establish
feasibility, but does not replace representative workload measurements.

Keep the following independently named choices available:

- Existing owned result APIs continue to serve their supported types and plans.
  Binding-only improvements may convert from `ValueRef` without cloning values
  and expose lazy tuple rows with shared column names. Lazy host conversion
  over collected `QueryRows` still retains its full result allocation; it is
  not bounded execution streaming.
- An explicitly materializing batch/`to_arrow` API may gather values, pack
  booleans, and copy UTF-8 once per admitted batch. A compact binary FFI path is
  another measurement-driven option. Both must report conversion costs and
  preserve value semantics; recursive/mixed values require explicit schema
  mappings and type-parity tests rather than an assumed universal Arrow mapping.
  Batching an already collected result must charge the retained full result,
  not just its current exported batch.
- A separately requested strict API preserves existing payload allocations or
  refuses incompatible plans/layouts. Its refusal does not remove support from
  the ordinary APIs and cannot trigger a copying fallback. Producer-native
  UTF-8, boolean, and recursive-layout changes require their own evidence and
  implementation review before widening eligibility.

The lifetime and admission analysis below can inform both batch APIs; its
no-copy invariants apply only to an explicitly adopted strict capability.

## Zero-copy definition and invariants

### Claimed boundaries

Every support claim MUST name its boundary:

| Boundary | Required claim |
| --- | --- |
| Storage or encoded search artifact to first execution representation | Source reuse only when layout, validation, and lifetime permit it; decoding, decompression, and reconstruction MUST be reported |
| Existing immutable execution buffers through pass-through projection, selection, and result delivery | Same underlying payload allocation; zero payload copies |
| Rust owner through native C ABI, Go views, or Python views | Same underlying payload allocation; zero payload copies |
| Compatible native batch to Arrow C Data / PyCapsule | Zero payload copies, or an explicit refusal |
| Conversion to Go rows, Python objects, IPC, or a dense gathered result | Explicit materialization outside the strict contract |

A strict result-delivery claim MUST NOT be advertised as an entire query doing
no copies when source decoding or materialization occurred. A request that also
requires source reuse MUST reject a plan requiring those transformations.
New arithmetic values, scores, frontier entries, and selection masks may be
produced directly in admitted buffers; generating a result is not permission
to duplicate an existing result during handoff. Building an export buffer from
already materialized rows counts as conversion, regardless of which module
performs it.

### Hard invariants

1. **ZC-1: Buffer identity.** Pass-through values, validity, offsets, and
   existing selection buffers MUST preserve allocation identity, generation,
   and the applicable byte range through delivery. Slices may change offsets;
   they MUST NOT move the payload.
2. **ZC-2: No serialization.** Strict delivery MUST NOT encode/decode JSON,
   base64, Arrow IPC, or another payload serialization format.
3. **ZC-3: No staging copy.** Strict delivery MUST NOT clone a payload, gather
   selected values, rebuild strings, repack booleans, or create row objects to
   make a foreign view. Moving ownership of an existing buffer is permitted.
4. **ZC-4: Selection preservation.** Filtering, projection, and limit MUST
   retain column storage. A noncontiguous selection MUST remain explicit;
   compacting its payload is a different, materializing operation.
5. **ZC-5: Immutable lifetime.** All foreign-visible buffers MUST remain
   immutable and live until the final view releases its owner. Cursor advance,
   close, cancellation, checkpoint, and compaction MUST NOT invalidate them.
6. **ZC-6: Bounded ownership.** Each retained allocation, descriptor, native
   resource, and handle MUST remain admitted until its final owner releases it.
   Zero-copy MUST NOT create unbounded pinned memory.
7. **ZC-7: Copy-free dispatch.** Scalar and SIMD backends MUST consume the same
   source buffers. Alignment, dispatch, or tail handling MUST NOT introduce a
   hidden payload copy.
8. **ZC-8: Explicit refusal.** An incompatible layout or consumer request MUST
   return a typed error. Strict delivery MUST NOT silently select the existing
   row/JSON path, even if that path is faster for a small result.
9. **ZC-9: Proof.** Pointer/range identity, copy accounting, semantic parity,
   and lifetime/resource tests are all required. A throughput benchmark alone
   does not establish zero-copy.

Schema metadata, owner handles, and address/length descriptors may be allocated
under admission. Metadata MUST NOT hide copies of existing value bytes: adding
string prefixes or inline string payloads during a layout conversion counts
as copied payload. Newly generated selection metadata and its allocation are
reported separately; zero-copy does not mean zero allocation.

## Native batch representation

### Schema, buffers, and views

A batch consists of an immutable typed schema, physical row count, ordered
column descriptors, logical selection, and an owner graph. Logical selected
row count MUST be distinct from physical row count. Nullability MUST be
explicit, including empty and all-null results; the first non-null row MUST
NOT decide a stream's schema.

Target physical representations, enabled only with a qualified producer:

| Values | Representation and ownership |
| --- | --- |
| Signed/unsigned identifiers and integers | Contiguous, correctly aligned fixed-width values; signedness and logical role remain explicit |
| Floating point | Contiguous values preserving original IEEE bit patterns |
| Boolean | Producer-native packed bits plus independent validity; existing byte booleans cannot be repacked in the exporter |
| UUID | Producer-native 16-byte values in RFC 4122/network byte order, compatible with `arrow.uuid` |
| UTF-8 / binary | Owned offsets plus immutable payload buffers, or native borrowed span descriptors retaining every referenced buffer |
| Selection | All, contiguous range, dense bitmap, or ordered sparse indices |

The first scalar feasibility slice may qualify only integers, floating point,
node identities, validity, and selection. UTF-8, binary, packed booleans, and
UUIDs are later capability slices rather than mandatory changes for that pilot.
Capability reporting MUST name both the producer and adapter; an available
descriptor type alone does not imply query-result eligibility.

All-valid columns MUST omit a validity allocation. Empty bytes, empty UTF-8,
and null MUST remain distinct. Every range, offset, multiplication, alignment,
UTF-8 boundary, and buffer length MUST be checked before publishing a view.
Native-owned fixed-width buffers SHOULD use 64-byte alignment when measurements
justify it; correctly aligned existing buffers MUST NOT be copied merely to
meet that preference.

Borrowed spans SHALL identify an owner, offset, and length instead of carrying
a Rust `String`, `Vec`, or enum layout across the ABI. Noncontiguous string
buffers MUST NOT be concatenated by a strict exporter. Already compatible
Arrow Utf8View/BinaryView buffers may be shared; manufacturing their inline
values or prefixes from existing payload is a conversion and requires refusal.

Recursive or mixed `List/Map/Value` columns are not implicitly supported by the
initial foreign ABI. They require a separately qualified immutable child/view
representation. Until then, the strict API MUST reject them; encoding JSON,
guessing a homogeneous type, or stringifying the value is forbidden.

### Selection and operators

Projection SHALL share column descriptors/owners. Filtering SHALL produce only
an admitted selection and share the input columns. Limit/offset SHALL preserve
logical order and represent the selected range or subsequence without copying
values. Selection indices refer to physical rows and MUST be validated against
the batch length. Payload retained by an empty/small view still counts at its
full retained capacity.

Sorting, joins, and distinct MAY produce indices or directly generated result
buffers when an existing qualified operator can do so. This document does not
extend plan eligibility. An operator that requires row materialization cannot
be activated in the strict path by hiding that step before export.

The native API SHALL preserve selection semantics for Go and Python. The Arrow
adapter MUST NOT misrepresent physical rows as the selected result. A standard
Arrow export is eligible for all rows, an order-preserving contiguous slice,
or an already compatible representation such as a qualified dictionary view.
An arbitrary bitmap/indices selection MUST otherwise return
`SelectionRequiresMaterialization`. Passing a selection alongside physical
Arrow arrays is permitted only through a distinctly named native view API,
never as an ordinary filtered RecordBatch.

## Consumer-driven pull and memory admission

### Pull state machine

Execution SHALL remain consumer-driven. A typed facade cursor exposes schema,
`next_batch`, terminal completion/error, and close. Exact Rust/C/Python/Go
method names are implementation review items, not existing API claims.

```text
Open -> next_batch -> reserve -> produce -> seal -> leased batch
                |                                  |
                +-> Backpressure <-----------------+
                |                    release -> capacity becomes reusable
                +-> Exhausted / Failed / Closed
```

No scan, decode, expansion, hydration, or scheduling of the next result batch
may occur merely because the host has not requested another batch. Default
prefetch and read-ahead queues SHALL be zero. A blocking operator may read
additional input during a demanded call only under its existing admitted
blocking/spill contract; a pull API does not make a sort or top-k free.

One cursor MUST serialize concurrent `next_batch` calls. Source and result
order MUST remain unchanged by backpressure, CPU backend, or batch size.
Known schema/layout/plan incompatibility MUST be rejected before the first
batch. A later execution or integrity error MUST remain observable, rather
than appear as successful EOF. Delivered batches remain provisional until
successful terminal completion, following the existing query contract.

### Leases and bounded slots

The internal lending cursor remains valid only until its next step. A foreign
batch instead owns a lease over a sealed immutable producer slot. Sealing MUST
move/retain the existing allocation; it MUST NOT clone it. A leased slot MUST
NOT have its payload mutated, cleared, resized, reallocated, or returned to a
mutable pool until the final reference releases it. Moving an owner handle or
descriptor without moving its underlying allocation remains permitted.

The default cursor SHALL admit at most two distinct batch slots: enough for a
consumer-held batch and the next demanded handoff, without prefetch. This also
allows a Python iteration assignment to replace its previous batch safely.
Lower byte budgets may support only one slot and require explicit release
before the next pull. Callers may select stricter/larger limits through typed
configuration only when the same ledger admits them.

Two slots are a count ceiling, not a requirement to allocate two maximum-size
batches eagerly. Idle reusable capacities remain charged. Each demanded batch
SHALL size itself from available bytes as well as requested rows; a single
indivisible value exceeding the allowance MUST fail before an oversized
allocation, rather than be truncated or split into a different logical value.

Limits MUST include maximum outstanding batches, retained bytes, view-handle
metadata, and per-batch rows and bytes. A row limit alone is insufficient for
large strings, high-degree graph frontiers, or document bodies. Per-call demand
MUST be bounded by these limits and by cumulative query row/payload budgets.
An exhausted resource budget MUST NOT be presented as EOF or an implicit
query `LIMIT`.

When retained views consume the allowance, `next_batch` MUST return recoverable
`Backpressure` before advancing its source. Releasing views and retrying MUST
resume at the same logical position. A synchronous call MUST NOT wait for the
same caller to release a view, and MUST NOT copy or revoke that view to make
space. Other execution budget failures remain explicit terminal failures;
they MUST NOT become a partial successful result.

### Accounting and cleanup

The query ledger and embedded runtime resource owner MUST admit actual retained
capacities before allocation. Transfer to a foreign owner SHALL transfer the
charge without an unaccounted gap. The charge MUST outlive cursor/database
closure when views do. Shared payload is charged once per allocation; every
owner/descriptor/handle is separately charged. A tiny slice of a large arena
is charged for the arena that it keeps alive, not merely the visible bytes.

Batch reservations MUST include validity, selection, offsets/spans, allocator
padding, pool slots, and producer work, plus any applicable SIMD scratch, graph
blocking state, and search decoder/top-k working state. External result leases
must also remain covered by the embedded retained-result allowance. A producer
MUST NOT double its pool to bypass a retained-byte limit.

The database/host SHALL enforce an aggregate retained-result allowance across
all cursors and language adapters; opening more cursors MUST NOT multiply the
available memory. Within each applicable allowance, the sum of unique retained
buffer capacities, metadata, scratch, decoder state, and blocking state MUST
fit before an allocation or ownership transfer is accepted. Conservative
charges MUST include opaque native workspace and foreign wrapper overhead.
An engine ledger is not a bound on the entire Go/Python runtime's RSS; process
measurements and baseline runtime memory remain separate qualification data.

Repeated retains, memoryviews, and capsule exports share payload charges but
consume admitted handle/descriptor capacity. Collecting every batch into a
Python list, Arrow table, or Go collection MUST hit the same limits rather than
silently enlarge them. Backpressure in a C Stream adapter MUST be surfaced as
an explicit resource error to its consumer, never as successful end-of-stream;
native pull consumers retain the recoverable retry contract.

Closing/cancelling a cursor stops further pulls and releases producer scratch
and unleased slots. Already leased immutable batches remain readable. Their
owners retain only necessary buffers, resource charges, and native code/module
lifetimes; a batch MUST NOT accidentally retain a mutable database transaction
or an entire search/graph cache. Existing storage page-pin wave limits remain
in force. Foreign retention cannot extend a lending page pin indefinitely;
an incompatible retention request must fail.

## Embedded facade and foreign interfaces

The root `hawdb` facade SHALL own the reusable batch-query contract. AST/plan
shape and producer capabilities SHALL determine eligibility before execution.
The interface is a general query-result and lifetime contract, not a typed API
for each application route. Existing owned result APIs remain separate explicit
materialization surfaces; they MUST NOT serve as an unnoticed strict fallback.

### Native C ABI and Go

The native ABI SHALL use versioned, size-checked `repr(C)` descriptors with
fixed-width discriminants and opaque owner handles. It MUST describe buffer
ranges, layout, physical length, selection, and nullability without exposing
Rust object layouts. Caller outputs SHALL be initialized to an empty/released
state before fallible operations. Panics MUST NOT unwind across the ABI.

Retain/release and pull operations SHALL be available as exported functions,
so the Go host can keep its purego, `CGO_ENABLED=0` loading model. Native batch
delivery MUST NOT depend on the cgo-only Arrow Go C Data importer. A later Go
Arrow adapter requires independent zero-copy/lifetime qualification.

Go typed column/view objects MUST retain their native owner and module lease.
The strict path MUST NOT call `bytes.Clone`, `C.GoBytes`, base64/JSON decoding,
or construct `[][]any`. Raw slices or pointers borrowed from native memory have
an explicit lease lifetime; no convenience method may conceal that releasing
the owner invalidates them. High-level views SHALL reject access after explicit
release. GC/finalizers may prevent leaks but MUST NOT be the only way to release
capacity and unblock the next pull.

### Python

Python SHALL expose owner-bearing batches/columns and a pull cursor through the
existing PyO3 extension. Native fixed-width and binary buffers SHOULD support
read-only buffer exports. A memoryview's exporter MUST retain its buffer owner
through `bf_getbuffer`/`bf_releasebuffer`; nullable columns expose validity
separately. Arbitrary nullable/string columns MUST NOT be advertised as a
zero-copy ordinary NumPy ndarray.

Compatible objects SHALL implement `__arrow_c_array__` and/or
`__arrow_c_stream__` using the standard named PyCapsules. Every export request
creates an independently owned descriptor/capsule, sharing the same payload.
Capsules are consumed once; an unconsumed capsule's destructor releases its
descriptor owner. Successfully moved ownership MUST NOT be released a second
time. Creating/exporting a capsule MUST NOT advance the pull cursor or gather
a selection.

The extension MUST NOT require PyArrow merely to import HawDB or expose its
native zero-copy interface. Optional consumer integration MUST be tested using
public protocol APIs. Requesting a schema cast, writable view, unsupported
layout, or dense filtered Arrow result MUST fail explicitly if it requires a
payload copy. Converting to Python dict/list/str/bytes is a separately requested
materialization API; `QueryResult::from_output` is not an implementation of the
strict interface.

Arrow release callbacks SHALL follow the standard move/release rules and hold
the Rust owner until the final consumer release. A stream release MUST NOT
invalidate arrays already handed to its consumer. Nonzero offsets, empty
arrays, child ownership, and early/error cleanup require explicit tests.

## SIMD execution contract

This is an independent proposed execution extension. It needs a separately
scoped issue, kernel baseline, and end-to-end workload evidence before adoption;
scalar strict delivery and ordinary binding improvements do not depend on it.

The target backends SHALL be scalar, x86_64 AVX2, and AArch64 NEON, selected by
runtime CPU feature detection under target-specific compilation. Unsupported
CPUs/targets SHALL use the scalar backend over the same shared buffers. Global
`target-cpu=native`, a Bazel configuration change, and AVX-512 are not required.
Qualification MUST identify the backend that actually executed.

Initial kernels target numeric comparisons, selection/validity bitmap logic,
counts, and qualified sorted identifier/ordinal operations. Sparse selections
may favor scalar or indexed access; a SIMD preference MUST NOT trigger dense
gathering of the input. Tail handling MUST NOT read beyond valid buffers or
assume padding that an external owner did not provide.

All backends MUST preserve the row/scalar reference semantics, including null
exclusion, integer/float coercion, `f64::total_cmp`, signed zero, infinities,
NaN bit patterns, and selected-row order. `BIGINT SUM` MUST preserve checked
overflow behavior, including an overflowing intermediate selected-row prefix;
reassociation that only checks the final sum is not equivalent. SIMD speed
claims require per-kernel differential tests and measured end-to-end benefit.

## Graph-specific optimization and correctness

This is an independent proposed graph extension, not part of #976's acceptance.
Adoption requires a separately scoped issue and evidence identifying an active
query workload and the relevant expansion/hydration cost. Existing graph
semantics and resource contracts remain authoritative until then.

### Identity, adjacency, and frontier

Graph execution SHOULD carry compact node/relationship identities and parent
ordinals instead of repeating complete node/property/binding objects. Node and
relationship identities retain separate logical roles and signedness; an
executor-private locator MUST NOT escape as a query value.

Eligible expansion SHALL use bounded adjacency cursors over the existing
immutable/live merge. It MUST preserve `(neighbor_id, relationship_id)` order,
relationship type/direction, snapshot visibility, and parallel-edge semantics.
A high-degree node MUST NOT create a degree-sized query buffer. Expansion MUST
stop before reading the next adjacency entry after downstream cancellation or
limit, subject to the existing executor semantics.

Frontiers SHALL use admitted typed buffers/selection plus parent references.
Filters and projections reuse frontier buffers. New parent/edge references
are directly generated operator output, not reconstructed row maps. SIMD
intersection or membership tests may be used only when their identity ordering
and input shape are proven; they cannot change traversal order, duplicate
paths, cycle handling, or shortest-path semantics.

### Property hydration and paths

The required-property set SHALL remain derived from the query/plan. Properties
SHOULD be accessed only when needed by predicates or surviving projections;
host-side graph joins or scans MUST NOT replace query execution. Identifier-only
and qualified exact-count plans MUST NOT hydrate properties or enumerate edges
solely to create a result representation.

A retained path view MAY reference an admitted immutable parent/edge arena.
It MUST NOT clone a `Vec<NodeId>` for every path during delivery. Logical path
order, relationship identities, depth, and the owner of each referenced arena
remain explicit. A standard Arrow list export that requires constructing a
new child payload MUST refuse; native path views and explicit path
materialization remain distinguishable interfaces.

Whole-query CSR/CSC construction, global neighbor sorting, and persistent
graph-layout changes are outside this contract. Existing derived graph
projections MAY be reused only with their generation and resource contracts.
Benchmarks MUST include hub nodes, reverse/type-constrained expansion, parallel
edges, cycles, tiny result limits, cancellation, and large property payloads.

## FTS-specific optimization and correctness

This is an independent proposed search extension, not part of #976's acceptance.
Adoption requires a separately scoped issue and profiles separating candidate
ranking, identity resolution, decoding, and host delivery. Existing search
semantics and resource contracts remain authoritative until then.

### Candidate and postings flow

FTS SHOULD carry generation-bound document ordinals, score values, visibility
references, and selection through ranking. It MUST NOT copy canonical ID
strings, titles, metadata, or bodies for every scored candidate. Ordinals are
not stable node IDs; lexical and vector ordinal spaces MUST NOT be conflated,
and no ordinal may resolve against another generation.

Existing per-block dictionaries, encoded-run skipping, and exact block-max
pruning SHALL remain authoritative. A zero-copy result change MUST NOT expand
all postings into a query-sized array. Compressed/delta postings require
bounded decoding when their layout cannot be used directly; decoded writes
MUST be measured separately from result handoff. SIMD intersections and
membership filters operate on compatible admitted blocks, preserving the
scalar reference and exact visibility semantics.

Canonical IDs SHALL be resolved only when needed for visible survivors or
exact tie ordering. Using ordinals for a tie is permitted only with proven
order equivalence within the pinned scope. Visibility/retractions and required
filters MUST apply before final top-k semantics; filtering invisible winners
after truncation cannot silently return an incomplete result.

Top-k state, score buffers, decoder workspace, and dictionary/ordinal references
MUST fit the same admitted operation budget. Pulling fewer output rows does
not excuse an unbounded candidate map. Scoring MUST preserve reference BM25
statistics, floating-point results, and tie order; SIMD reassociation,
approximate pruning, or changed analyzer behavior is not authorized.

### Selected content and integrity

Score/ID-only queries MUST NOT hydrate bodies. Selected-body access remains a
separate bounded demand. A body/title/snippet view may share already verified
immutable content only when its owner and full retained capacity are admitted.
It MUST NOT reference a reusable decompression or document-line scratch buffer.

Compressed content and existing encoded document lines cannot be relabeled as
direct UTF-8 views. Decompression, decoding, snippet construction, normalization,
or highlight insertion MUST be reported as source construction/materialization;
a request requiring source reuse MUST return `CopyRequired` for such a path.
Bounded streaming readers remain useful but are not zero-copy body views.

The complete applicable integrity checks MUST finish before publishing a body
view. A valid selected document followed by a corrupt segment suffix MUST NOT
escape as a successful verified body. Generation retention must survive
replacement, deletion, compaction, and cleanup without revoking live views.
Strict ownership MUST retain only required content/identity resources, not an
entire reader cache or all decoded documents.

Benchmarks MUST cover common CJK terms, selective terms, dense/sparse candidates,
top-k ties, retractions, mixed pinned generations, cold/warm reads, large bodies,
corrupt tails, and a consumer that stops before requesting hydration.

## Errors, observability, and qualification

Required distinct error categories are `UnsupportedPlan`, `UnsupportedLayout`,
`UnsupportedType`, `CopyRequired`, `SelectionRequiresMaterialization`,
recoverable `Backpressure`, `WorkingUnitTooLarge`, terminal
budget/integrity/execution errors, and use-after-release at checked view APIs.
Names are target categories, not claims
that variants currently exist. Errors MUST preserve existing engine evidence.

Profiles SHALL report:

- selected API/plan/layout and actually executed SIMD backend;
- source bytes decoded/decompressed and first-representation materialization;
- payload bytes copied by projection/selection, layout conversion, result
  handoff, and explicitly requested language-object materialization;
- selection/descriptor/scratch allocation bytes and per-account peaks;
- current/peak retained capacities, outstanding batches/view handles, pool
  slots, pin/generation resources, and backpressure events;
- rows/candidates/adjacency entries requested, visited, selected, and emitted;
- terminal completion/error and total query/result resource charges.

Strict projection/selection forwarding, export conversion, and result handoff
copy counters MUST be zero. Counters MUST cover operations in adapters as well
as the engine; moving a copy into a constructor or omitting native allocations
from language allocation statistics cannot satisfy the contract.

Qualification is scoped to the plans, types, and adapters actually adopted.
The core gates apply to each strict delivery claim; extension gates apply only
when that extension is implemented or advertised. A scalar numeric pilot does
not require SIMD, graph, or FTS implementation. No pilot can claim support for
types or application queries outside its recorded capability set.

| Gate | Required evidence |
| --- | --- |
| Buffer identity | Allocation ID/generation/range equality from producer through projected/selected Rust, Go, Python, and eligible Arrow views; address equality alone is insufficient when allocations can be recycled |
| Lifetime | Views retained across next pull, cursor/database close, cancellation, and GC; add graph publication or search replacement/compaction when the claimed producer uses those resources; exact final release without double-free or module unload |
| Pull and memory | No speculative next batch; slow/stopped consumer; two-slot, byte, handle, and aggregate multi-cursor caps; small view retaining a large arena; oversized values; collection of all batches; recoverable backpressure without source advancement or same-thread deadlock |
| Layout and refusal | Null/empty/all-null, maximum integers, offsets/alignment, unsupported types/layouts, sparse selection Arrow export, writable/cast requests, and malformed descriptors; add NUL bytes and UUID parity when those types are supported |
| SIMD extension | Forced scalar/AVX2/NEON parity on their supported platforms, tails, nullable/sparse input, NaN/signed zero, and intermediate sum overflow; no unsupported instruction execution |
| Graph extension | Identity/order/duplicate/path parity, bounded hub expansion, property laziness, cancellation and pin release |
| FTS extension | Exact candidates/scores/ties/visibility, ordinal generation fencing, no unnecessary ID/body hydration, and corrupt-tail refusal |
| Performance | Same consumer work and checksums; rows/bytes, latency/throughput, copy bytes, allocations, ledger peaks, native RSS, cold/warm source work, and retained resource counts |

Measurements SHALL compare current owned boundaries and each implemented
candidate separately, including materializing batches and eligible strict
scalar/Arrow paths. Add SIMD comparisons only when implemented. Small point
results and large variable-width batches are required for a claim of general
host-workload benefit; a numeric-only pilot records its narrower coverage and
unsupported workloads. A Go IPC decode microbenchmark cannot establish Python,
full-query, graph, FTS, SIMD, or zero-copy support. Memory boundedness and
zero-copy are acceptance gates even when throughput improves.

## Delivery order

1. Record #976's representative baseline and isolate conversion/crossing cost.
   Exit only with reproducible Rust/Python/Go results and value parity. Choose
   ordinary binding improvements from that evidence; the zero-copy proposal is
   not a commitment to rewrite engine layouts.
2. Independently evaluate binding-only clone removal, lazy tuples, and explicit
   materializing batch/Arrow or binary FFI paths. Exit each adopted path with
   measured benefit, complete type semantics, and retained-memory accounting.
   These improvements may satisfy #976 without implementing strict delivery.
3. If separately scoped evidence justifies a strict capability, implement and
   qualify a narrow scalar retained-batch producer, native owner/account
   transfer, and Go/Python views. Start from eligible numeric execution shapes;
   do not describe the current lending cursor as a general retained producer.
   Exit only with cross-language identity, refusal, lifetime, bounded slots,
   recoverable backpressure, and purego evidence. Add Arrow export only for
   layouts/selections whose no-copy compatibility is independently qualified.
4. Evaluate producer-native UTF-8/boolean/recursive representations and SIMD,
   graph, and FTS extensions as separate measured workstreams. They are not a
   combined prerequisite or an automatic follow-up to #976. Each adopted slice
   must pass its relevant gates; unsupported strict slices keep explicit refusal.

Every implementation stage SHALL use default repository Bazel configuration
and the required formatting, strict Clippy, target-specific, and local fuzz
checks. This design document supplies target acceptance criteria, not passing
test or shipping evidence.

## External interface references

- [Arrow columnar layouts](https://arrow.apache.org/docs/format/Columnar.html).
- [Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html).
- [Arrow C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html).
- [Arrow PyCapsule Interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html).
- [Python buffer protocol](https://docs.python.org/3/c-api/buffer.html).
- [Rust CPU intrinsics and feature detection](https://doc.rust-lang.org/std/arch/index.html).
