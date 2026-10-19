# Experimental retained numeric delivery

Historical evidence engines include label-owner parent `ca347097`, prototype `60899139`,
and native-node probes based on `da02e302` plus the candidate source/module hashes
recorded below. This design document combines multiple prototype stages; its
measurements do not implicitly describe the current checkout.

Regenerate with: `PYTHONPATH="$PWD/bazel-bin/bindings/benchmarks/retained_boundary.runfiles/_main/bindings/python/python" uv run --no-project --python 3.14.6 --with pyarrow==26.0.0 python bindings/benchmarks/retained_boundary.py --rows 10000 --backend memory --mode all --samples 7`.

For a new numeric/Arrow group, first build `//bindings/benchmarks:retained_boundary` and
`//bindings/ffi:hawdb_ffi` with `bazel build -c opt` at the selected source. This
command does not regenerate the separate label-owner or native-node probes: use their
archived drivers linked in the corresponding sections. Preserve their recorded source
and module hashes.

This implements ownership building blocks and an experimental native cursor for
[issue #987](https://github.com/nowledge-co/hawdb/issues/987), under the
[columnar interchange proposal](specs/ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md).
Whole-operation source/planning qualification, complete C/Go resource
qualification, Python/Arrow resource and platform qualification
remain incomplete. Native C Data/C Stream, Python capsules and a scoped optional
PyArrow consumer path now have layout and lifetime evidence. The complete
proposal and #987 remain unfinished.
Production hosts continue
to use the root `hawdb` facade; this internal executor module is not a new
integration surface.

## Point lookups, row reads, and columnar delivery

Storage layout, execution layout, and host delivery are separate choices.
This capability does not replace canonical rows with Arrow columns. It builds
numeric execution buffers from materialized node properties and preserves those
buffers through delivery. That first representation is construction work, not
storage-to-host zero-copy. The current retained producer accepts the restricted
scan/filter/project fragment; it does not qualify indexed point lookup or
persistent row-page reuse. A one-row result or `LIMIT 1` does not prove a seek.

For an indexed point lookup returning many fields, keep the query runtime's
index-to-required-row-property path. Reading one row across many separately
stored columns can touch more pages/cache lines, while constructing a one-row
record batch adds schema, column, validity and owner overhead. Columnar random
access itself is valid; the concern is locality and fixed cost, not inability to
address a row. A covering access path that already owns the requested immutable
column buffers may still make a column view efficient.

| Workload | Delivery direction to measure | Important cost |
| --- | --- | --- |
| Indexed point lookup, many fields or recursive values | Owned row or the experimental single-match retained node view; no forced column transpose | Seek and required-property decoding, per-call overhead, row-owner lifetime |
| A few scalar fields already in compatible column buffers | Native scalar or column views; Arrow only when the consumer needs it | Descriptor/handle cost and whether the consumer converts back to rows |
| Many rows, narrow numeric projection, vectorized host work | Bounded retained column batches and compatible Arrow export | Batch admission, selection compatibility, amortized crossing cost |
| Graph expansion or FTS top-k followed by hydration | Bound candidate identities first, then hydrate requested fields | Candidate/page budget, adjacency/posting access, large-document pins |

The experimental indexed node view now supplies owner-bearing references into
original immutable row/catalog allocations, with independently admitted native
and C/Go/Python views. See [native delivery](#experimental-native-indexed-full-node-delivery),
[Python ownership](PYTHON_RETAINED_NODE_OWNERSHIP.md), and
[C/Go views](GO_RETAINED_NODE_VIEWS.md). Its single-match materialized index plan
refuses scans, multi-match seeks and hydration; this does not make the numeric
scan producer source-reusing. A strict row view must not transpose owned rows
into Arrow to claim zero-copy. Compressed or
encoded row pages still need decoding unless their original representation is
directly consumable. Referenced pages, strings and large-document content must
remain charged at their full retained capacity, even for a tiny visible span;
source/planning workspace cannot be omitted from a whole-operation bound.
The indexed pilot is implemented; broader row/span eligibility and whole-operation
qualification remain open.

Choose the requested representation explicitly. Strict column requests retain
their refusal semantics; they do not silently switch to a copying row path.
Do not introduce a second canonical column store or change durable layout just
to support Arrow interchange. Graph and FTS continue to use bounded query-runtime
access paths rather than host-side scans or eager whole-document hydration.

Qualification must compare actual indexed hit/miss reads against scans, varying
row count, projected width, scalar/UTF-8/recursive types, backend/residency and
warm/cold cache. Report the physical access path, pages/bytes decoded, call and
conversion time, p50/p95 latency, allocations, pinned capacities and peak RSS.
Measure row, retained-column and Arrow consumers independently, including the
case that converts Arrow back into language row objects. Existing ordinary
boundary measurements do not supply this comparison or a crossover threshold.

### Review invariants

The follow-up preserves these local ownership/state obligations:

- A cached borrowed descriptor is read only while its batch owner lock is held
  and the owner is open. Close needs the exclusive lock; therefore payload
  release cannot overlap a scalar read. Copied Go batch values share both the
  owner and cache synchronization. Independent retains reserve their own cache
  capacity before publishing a new owner.
- Python cursor source advancement, Python batch delivery, close and Arrow
  adoption are mutually exclusive. A waiter detaches from the interpreter;
  an executing pull can reattach without that waiter holding the GIL. A failed
  batch allocation is marked terminal before another operation can take the
  cursor. Independently delivered payload owners survive cursor closure.
- After native source advancement, caught registration/delivery failure implies
  a terminal cursor state. Subsequent pulls return that failure, not another
  successful batch or EOF. A simple Closed refusal is not a failure transition.
- A sparse Arrow batch is never gathered. Late refusal preserves earlier
  immutable arrays and prevents successful terminal confirmation. Arrow export
  publishes no native-word validity bitmap on an incompatible-endian host.

These are source-level arguments backed by regressions, not a machine-checked
proof of Rust/Python/Go execution. They assume consumers obey lease/release
contracts and exclude process abort, allocator abort and foreign-memory misuse.

## Shared admission

`RuntimePermit::reserve_retained_result` reserves a buffer-owner group and its
first view before allocation. `RuntimeRetainedResult::try_retain` reserves
another independently owned view without charging the payload again. Both
operations share the originating governor, including across governor clones
and permits. Each buffer owner stays charged until its last view drops; view
metadata is charged separately and drops with that view. Tokens retain neither
CPU/I/O slots nor a database/query handle after the work permit closes.

Charges include the producer's retained capacities, caller descriptors and
native token/Arc overhead. The existing runtime memory and host process-memory
policy also apply, including when retaining after the original permit closes.
Admission failures roll back temporary reservations. Reservations overlap an
active work permit conservatively rather than silently transferring its whole
query budget.

`RuntimeRetainedResultError` preserves retryable capacity exhaustion separately
from a request larger than its total allowance, runtime admission failure,
and arithmetic overflow. Releasing another view allows retry without revoking
an existing view. The aggregate snapshot reports bytes, owner groups, handles,
peaks and backpressure. An owner group may contain several native buffers;
`buffer_owners` is not a count of individual buffer allocations.

The new typed `RuntimeGovernorConfig::retained_result_handle_limit` defaults to
1,024 for shared hosts and 256 for mobile hosts. These caps apply only to the
new reservation API. Result byte allowances remain 16 MiB and 2 MiB,
respectively, and ordinary query behavior is unchanged. Adding the public
configuration field requires updating exhaustive struct literals; constructors
or struct update syntax remain usable. A handle cap does not replace byte
admission or the proposal's separate per-cursor slot limit.

## Numeric ownership handoff

### Retained property extraction

The retained numeric producer validates and appends the same borrowed property
lookup. It does not traverse the node property map again when constructing the
first numeric buffer. Type mismatch poisons the producer before advancing its
value, identity, validity, or row-count state; the producer cannot seal a partial
result. This does not change source ownership or turn construction into source
reuse.

[Isolated extraction controls](../bindings/benchmarks/evidence/README.md#retained-property-lookup-controls-20261012)
preserve 54 raw observations, frozen binary identities, source hashes, the
standalone harness, and its measurement scope. Nine alternating paired rounds
observed median extraction speedups of 1.46x, 1.80x, and 1.93x for one immutable
integer node with 1, 16, and 128 properties, respectively. Query-ledger peak
remained 13,216 bytes and returned to zero after release. Another compiler
process competed for CPU; these observations are not clean-machine latency
qualification. Planning, storage I/O, selection, cursor and foreign delivery,
null/float timings, and full-query performance are excluded. The result does
not establish comprehensive performance acceptance, SIMD, or strict source
reuse.

For a borrowed immutable `NodeRecord`, both property lookups in the previous
producer resolve to the same `Option<&Value>`. Reusing the first reference
therefore supplies the same input to the unchanged value-buffer match. The
schema guard still runs before that match, and neither the guard nor the lookup
mutates the node. Accepted inputs perform the same value/identity/validity
appends; rejected inputs return before those appends. Existing admission and
sealing rules remain in force. This is a source-level equivalence argument,
not a machine-checked whole-query proof.


`numeric::retained` constructs the first typed integer/float representation
from borrowed node properties under both a query ledger and the shared runtime
reservation. It stages no owned rows or `Vec<Value>`. Optional unsigned node
identities, a lazy validity bitmap, and ordered selection indices are prepaid.
All-valid batches allocate no validity bitmap. The capacity calculation
includes owner layout and conservative per-buffer padding; it is not an RSS
or allocator-overhead measurement.

Sealing moves the existing Vec allocations into an immutable Arc-owned storage
structure. It does not convert them to `Arc<[T]>`, gather selected values, or
clone their payload. Each buffer has a module-scoped allocation identity and
generation; tests verify the identities and payload, mask, identity-column and
selection addresses before/after sealing and retaining. Fresh producers receive
fresh IDs even if allocator addresses are recycled. Generation is currently
zero because these immutable allocations are never reused as mutable slots.

Physical row count remains distinct from the ordered selected indices. A
selection limit truncates indices while retaining the complete underlying
allocation. Float bits and the existing numeric comparison semantics are
preserved, including signed zero and NaN total ordering. Invalid scalar types
or capacity overrun poison the builder so callers cannot seal a partial result.
Schema-mismatch errors do not stringify a potentially large corrupt value.

`QueryMemoryLease::split_off` divides an existing admitted charge between
shared storage and a view without an extra reservation or a temporary release.
The exact-budget test proves sealing works when the ledger is already full.
Retaining another view admits runtime and query metadata before sharing storage;
failed admission leaves the original view usable. Payload destruction precedes
release of its query and runtime capacity charges.

`source_constructed_bytes` discloses numeric/identity first-representation work.
It is not a claim that source access or the whole query performs no copies.
There is no qualified source-reuse capability.

## Root cursor and bounded pulls

`Database::query_with_params_retained` and
`DatabaseReadTransaction::into_retained_query` expose the experimental contract
through the root facade. Eligibility comes from the existing optimized physical
plan and public numeric property descriptor. Supported plans have one label,
one integer/float comparison and projections of that same property or `id(n)`,
with optional SKIP/LIMIT. Repeated property projections share their payload.
For example, declare `CREATE NODE TABLE Item` and
`CREATE PROPERTY ON NODE TABLE Item(score) TYPE INT` before querying
`MATCH (n:Item) WHERE n.score >= $min RETURN n.score, id(n)` with a parameter map.
An undeclared property, another projection or plan shape refuses explicitly.
Property columns are nullable Int64/Float64; identity columns are non-null UInt64
with an explicit node-identity role. This identity schema does not advertise
ordinary row-value signed-integer or graph-frontier qualification.

The default is two distinct live payload slots, zero prefetch, at most 1,024
inspected source records and 1 MiB admitted capacity per pull. Database batch
configuration can restrict these maxima further. Unrelated-label records count
toward the inspected-record bound; a filtered-empty batch is not EOF. The
materialized source resumes by ordered node ID over the original immutable
copy-on-write pages. Persistent canonical/overlay sources and source-reuse
requests return `CopyRequired`.

The direct database entrypoint borrows the existing catalog and planner during
creation instead of cloning a complete read transaction. The cursor keeps only
the shared node directory/pages, fail-closed poison signals and optional task
context. Consuming a read transaction releases its catalog, relationships,
indexes, query cache, file leases and reader pin before returning the cursor.
These heap-only node pages survive database destruction and later writes use
copy-on-write publication. No persistent canonical pages are decoded by this
path. Shared storage poison signals still fail subsequent source access closed.

Before capturing the source, creation admits a conservative bound on its entire
materialized node directory, pages, B-tree nodes, labels, property containers,
and recursively owned string/binary/list/map capacities. Unrequested properties
and unrelated labels are included. Both the query ledger and aggregate runtime
memory policy hold source leases until close, completion, failure or
cancellation. Numeric batches and foreign views do not retain these leases.
Overflow or excessive value nesting refuses admission without publishing a
cursor. Canonical/out-of-core sources remain unsupported.

The bound uses the pinned Rust 1.97.1 B-tree layout. A cold preflight borrows all
records without cloning source values. The immutable page directory owns its
capacity cache, so snapshots created before the first preflight also reuse the
same initialized bound. Cloning a map shares the directory and does not copy a
cache. A writer detaching a shared directory starts with an empty cache; all
mutable directory/page access invalidates the writer's cache. Ordinary writes
do not recalculate capacity. The directory's cache metadata is included in its
retained capacity. Admission conservatively charges each cursor's full source
bound, even when cursors share
pages. This can refuse a narrow projection over a large row store, or multiple
cursors over one shared source, earlier than an allocation-deduplicated policy.
The cold walk checks cancellation before and after creation, not between rows.

The source admission argument has four invariants:

1. For a nonempty pinned-toolchain B-tree with `n` entries and `t` nodes,
   the root has at least one entry and every other node at least five, so
   `t <= 1 + floor((n - 1) / 5)`. An empty tree is charged for one root.
   The per-node bound includes eleven inline pairs, twelve child pointers and
   header/alignment padding for the concrete source types.
2. Container allocations and recursively owned buffer capacities are added
   with checked arithmetic. Shared pages appear once in a source directory;
   charging the entire source separately to each cursor conservatively bounds
   the union of allocations retained by admitted cursors.
3. A directory and its cached bound have the same immutable generation.
   Snapshot cloning shares that pair, including before cache initialization.
   `Arc::make_mut` either obtains an exclusive directory or clones its page
   references into a new directory with an empty cache. `DerefMut` invalidates
   the cache before mutable directory/page access. An older snapshot retains
   its unchanged directory and bound. Thus no published bound can describe a
   different source generation.
4. Both memory reservations precede source capture. Failed construction drops
   provisional leases. Terminal release destroys source references before its
   query/runtime charges; produced result owners retain neither source nor
   source charges.

These are source-level arguments with mutation, refusal and lifetime regressions,
not a machine-checked proof of allocator/RSS or whole-query boundedness.

Arrow export itself does not change WAL or commit processing. Source retention
can affect concurrent writes: while a cursor holds an old page, modifying that
page requires COW publication; slow consumers extend that retention window.
The capacity cache adds metadata to the page directory and invalidation before
mutable access. Sharing it removes per-map cache copies and duplicate cold walks
across snapshots of one directory; it does not eliminate COW page retention.
Qualification must therefore measure ordinary writes and writes while a slow
cursor remains open separately, with
identical durability and workload. A read-boundary speedup does not establish
write neutrality or qualify these shared storage changes.
The scoped [write controls](../bindings/benchmarks/RETAINED_WRITE_RESULTS.md)
preserve measured slow-reader costs and unfavorable inter-producer observations;
they do not complete the ordinary-write gate.

Pulling is serialized by Rust's mutable cursor borrow. Slot/byte/handle pressure
returns a retryable error before source advancement, without waiting for a
same-thread consumer to release its own view. A larger explicitly configured
slot count remains subject to the shared byte/handle allowance. View retention
does not consume an additional distinct slot. The slot releases after final
payload destruction, even if the original batch wrapper dropped earlier.
The control owner and first batch require at least two shared handles. A
one-handle configuration fails with `WorkingUnitTooLarge` before creating the
control owner or binding the governor instead of returning permanently
unresolvable backpressure. The host can correct that rejected configuration;
once a valid governor binds, replacement cannot multiply its allowance.

SKIP/LIMIT expose a range of the selection allocation without gathering values.
Provenance includes allocation ID, generation, retained capacity and visible
byte range; a small selection still charges its full owner. Result row and
selected-payload limits apply cumulatively and caller options cannot raise the
database limits. Every batch remains provisional until successful terminal
completion. Terminal errors repeat as errors, never EOF; status changes remain
visible through earlier independently retained batches. Cursor close and
cancellation release the source without revoking produced values. Dropping the
database leaves the cursor's immutable heap source available for further pulls.

The database and all its read snapshots share one retained governor binding,
chosen at the first eligible cursor. Configure the governor before that first
cursor; later ordinary-governor replacement does not create a second retained
allowance. Native batches retain schema/status and admitted payload owners,
without retaining the database, source iterator or read pin. CPU permits are
released between pulls. Profiles retain inspected/constructed/selected work even
when a later delivery budget fails; emitted rows/bytes include successful
deliveries only. Profiles also report original source cardinality and currently
pinned rows/pages without scanning records; completion, failure and close drop
the current source counts to zero. Directory capacity counts only the shared
directory allocation, excluding node-page and record payload capacities.
`source_pinned_capacity_bytes` reports the admitted live source bound and becomes
zero at terminal release. `source_preflight_rows` reports records actually
inspected by a cold capacity walk; a cache hit reports zero. `query_peak_bytes`
includes source leases and remains a historical peak after their release.

Adapters can set additional cursor metadata and a minimum usable shared-handle
count through RetainedQueryOptions. next_batch_with_metadata includes foreign
owner/descriptor capacity in row sizing and reserves it before source work.
The ordinary Rust next_batch path passes zero additional metadata. Overflow or
an impossible working unit fails without inspecting source records.

## Experimental native Arrow C Data export

`RetainedQueryBatch::export_arrow` exports an independently owned numeric record
batch through the standard `ArrowSchema` and `ArrowArray` layouts. The Rust
`RetainedArrowExport` releases both descriptors on drop. `into_raw` transfers
their release obligations to a C Data consumer, which must follow the standard
move/release rules and keep producer code loaded. This is a root library API;
it does not yet add a C ABI frontend or a Go Arrow consumer interface.
The Rust export also exposes shared completion/error status and the original
allocation identity/generation/capacity with its selected value byte range.

Only an empty or order-preserving contiguous selected range is eligible.
The native validity words require a little-endian host for Arrow's byte-wise
LSB bitmap order. Array and stream export reject other endianness with
`UnsupportedLayout`, without swapping payload or adopting/advancing a cursor.
Sparse/reordered selections return `SelectionRequiresMaterialization` before
constructing descriptors. Each child uses its original values/validity pointers
and a checked physical-row offset; repeated projections share values. No
selected values are gathered. Schema keeps nullable Int64/Float64 and non-null
UInt64 identity roles, with `hawdb:role=node_identity` field metadata. All-valid
columns have no validity buffer. Empty/all-null selection keeps declared types.

Schema, names, metadata, child descriptors, pointer directories and array
owners are admitted before allocation. Each descriptor has its own shared
handle/metadata charge. Result payload is retained once through a shared batch
owner, at its full capacity. Impossible handle configurations fail explicitly;
temporary exhaustion rolls back partial schema/array construction and permits
retry. Schema-only exports retain schema/control metadata without retaining a
result slot or numeric payload.

Release callbacks locate ownership through `private_data`, never the original
descriptor address. Moving a child and marking its source released keeps that
child valid after immediate parent release. Parent release walks live children,
then destroys its own directories. Cursor/database close does not revoke the
independent arrays; final schema/array release returns all native charges.

Native regression tests cover pointer/range identity, nonzero offsets, child
movement, schema-only slot release, partial admission rollback, nullable IEEE
bits, empty/all-null schema, sparse refusal and exact final native release.
This is descriptor identity/lifetime evidence, not platform, allocator/RSS or
complete source/planning qualification.

## Experimental native Arrow C Stream and Python capsules

`RetainedArrowStream::try_take_cursor` admits the stream before adopting a root
cursor; a failed admission leaves the original cursor and source position
unchanged. `into_cursor` can recover that cursor if a foreign wrapper allocation
fails. The consuming `into_arrow_stream` convenience method drops its consumed
cursor when export fails. Construction, schema export and observations
never pull or prefetch. Each demanded array reserves all child descriptors and
handles before native source work; a mutex serializes concurrent pulls.

Retryable pressure returns a nonzero stream error with an initialized released
output, never successful EOF. Releasing arrays permits retry at the same source
position. Terminal failures repeat and remain visible after schema requests.
Arrow errno 12 covers both retryable pressure and terminal capacity/budget
failures; the diagnostic distinguishes them. An errno alone is not a retry
classification, and a generic consumer need not support retry after an error.
Use the native typed cursor when a consumer needs reliable pressure handling.

Contiguity is data-dependent within each demanded batch. A stream can deliver
earlier contiguous arrays and then fail with errno 22 and
`SelectionRequiresMaterialization` on an interleaved selection. Earlier arrays
remain readable but do not constitute a complete result. The failure remains
terminal across repeated pulls and schema requests; it is never EOF. Creation
checks known plan/schema/platform incompatibility, but does not pre-scan the
source to predict future selection. Native selection-aware batches remain
available for such queries. There is no gather, hidden copy, prefetch or
whole-result preflight added to make the stream appear universally eligible.
The prepaid stream diagnostic workspace avoids allocating at a full allowance;
oversized diagnostics produce an explicit error. A demand that confirms the
immutable source is exhausted needs no additional descriptor, byte or payload
slot. Cancellation and source poison checks still apply. Final delivered arrays
remain provisional until successful terminal confirmation.

Python batches implement `__arrow_c_array__` and `__arrow_c_schema__`; cursors
implement `__arrow_c_stream__` and `__arrow_c_schema__`. Each named capsule is
consumed once and owns an independently admitted descriptor, sharing the
existing payload. Successful stream export adopts the cursor, so the original
Python cursor reports closed and cannot pull again. Requested-schema arguments
currently refuse with `CopyRequired`; sparse selections refuse with
`SelectionRequiresMaterialization`. There is no implicit gather or cast.

Unconsumed capsules release their descriptors at destruction; moved descriptors
have null release callbacks and are not released twice. Every independently
moved array/schema child keeps the code module until its own final release.
Python owners have individual module refcount/GC edges; shared native owners
are external GC roots. Native release on a detached thread may safely defer the
final Python decref until the next attached PyO3 entry. Arrays survive stream,
Python parent and database closure. Schema-only capsules keep metadata without
keeping a numeric payload slot. Python capsule allocation failure restores an
adopted cursor without advancing its source.

Optional tests use public `pyarrow.record_batch`, `pyarrow.schema` and
`RecordBatchReader.from_stream`. PyArrow 26.0.0 on macOS ARM64/CPython 3.11
preserves original value addresses, nonzero offsets, repeated projection,
identity metadata, IEEE bits and tiny-slice lifetime. Tests also cover empty
schema, sparse/cast refusals, explicit pressure/retry and stream/database close.
These are scoped consumer layout/lifetime checks, not complete copy/allocator,
RSS, performance or platform qualification. PyArrow remains optional and is
absent from the engine, package dependencies and default test environment.

## Experimental C views

The C ABI now exposes checked version-1 numeric cursor, batch, schema, column
and buffer descriptors. Result buffers move from the root producer without
JSON encoding or payload gathering. Independent column/batch retains share
native allocation identities and ranges and admit their wrapper/descriptor
capacity before allocation. Handle lookup validates module namespace and a
never-recycled ID, rather than dereferencing opaque caller addresses.

The registry allocates one prepaid node per live handle, with no unused
container capacity. Payload destruction occurs outside its lookup lock. A
cursor has its own pull mutex; unrelated cursors do not share that execution
lock. Outputs initialize before fallible calls, and panics are contained at
the ABI. A producer failure remains visible through previous readable views.

Borrowed column descriptors need no new owner and remain tied to the parent
batch. Their descriptor set is prepaid in the demanded batch. Independently
owned columns remain readable after parent/database closure. C creation needs
three shared handles for a minimally readable owned column protocol; a lower
limit refuses before binding. Foreign resource qualification and Arrow still
need their own evidence. See the
[C contract](../bindings/ffi/README.md#experimental-retained-numeric-abi).

## Experimental pure-Go views

The 64-bit purego adapter calls the versioned C descriptors with cgo disabled.
`DB.QueryRetained` returns a serialized pull cursor, independently releasable
batches/columns and explicit typed outcomes. Batch scalar getters use the
prepaid borrowed descriptor and require no new handle. Column getters read the
selected native positions directly, with no gathering or mutable slice export.
Borrowed descriptors are cached once per accessed column and batch owner,
including copies of that Go batch value. Independent batch retains prepay their
own descriptor capacity. Repeated scalar reads take owner/cache locks but do
not repeat the foreign call or global registry lookup. This is not a general
bulk-performance claim. A failed native release preserves explicit Close retry
and its finalizer; an already absent handle cannot be retried as a live owner.
An owner lock covers scalar reads and Close; released access refuses. Independent
retains share allocation identity/ranges and root admission. Owners keep only
their native handle and Library mapping, not a Go DB or mutable transaction.
The library already stays mapped for the process lifetime.

SchemaCopy explicitly copies schema metadata; result payload is not JSON,
base64, IPC or owned Go rows. Finalizers provide a leak safety net, while
explicit Close releases capacity for retry. The adapter keeps provisional
state and Backpressure distinct from successful EOF. See the
[Go interface and example](../bindings/go/README.md).

## Experimental Python buffers

`Database.execute_retained` calls the same embedded root API using immutable
`RetainedOptions`. Its cursor pulls serially without prefetch; batches and
numeric/selection/validity exporters have explicit Close and independently
admitted retains. Native `bf_getbuffer` reserves a fresh root lease before
allocating its descriptor/shape owner. `bf_releasebuffer` drops that lease
without depending on the original exporter's current open state. A memoryview
therefore stays readable after exporter, batch, cursor and database closure.
Cursor pull, close, observation and stream adoption share one native mutex.
Waiting for it releases the GIL, while a pull keeps the mutex through Python
batch allocation and delivery. Concurrent close waits for an in-flight pull;
it does not raise PyO3's mutable-borrow error. A delivered batch keeps its own
lease after close. Terminal failures do not turn into skipped rows on retry.

Physical values, ordered u32 selection and optional native u64 validity words
are separate read-only buffers. Selection is never gathered. Formats are q/d/Q
for values, I for selection and Q for validity words; bit r uses word r/64,
least-significant-bit r%64. Exporting writable data or requesting a dtype change
refuses before transferring ownership. Source/plans/types outside the recorded
numeric capability refuse without an owned-result fallback. PyArrow is not an
import dependency; the separate capsule protocols are described above.

Each owner retains the native Python module and participates in GC traversal.
Python objects use their actual type basicsize plus conservative padding;
native buffer leases additionally reserve descriptor/shape and opaque consumer
overhead. Repeated native buffer acquisition consumes shared handles. Derived
memoryviews share CPython's existing managed lease; their opaque host allocations
still need complete accounting/RSS qualification. A tiny slice keeps the full
native payload charged. This is not a bound on arbitrary interpreter objects.

Closed high-level owners refuse access, while existing memoryviews keep their
independent leases. Backpressure is a typed retryable exception before source
advancement, not None/StopIteration. Lower cumulative row limits produce sticky
terminal errors, visible through earlier live exporters. Native producer or
foreign batch-allocation failure aborts delivery and releases the source;
native root emission and successful Python delivery are reported separately.
`value_copy`, schema/profile/resource/provenance copy methods explicitly build
small Python objects outside the strict payload view. See the
[Python interface](../bindings/python/README.md#experimental-retained-numeric-buffers).

This is not yet a qualified whole-operation memory bound. Parsing and optimizer
workspace still need a complete admission/capacity audit. The cursor pins all
materialized node pages, including unrelated labels, under the conservative
source admission described above. Allocation-deduplicated source accounting,
cold-walk cancellation latency, allocator/RSS and complete foreign workspace
qualification remain open. Avoiding a full read-snapshot clone and reporting
source counts does not resolve these remaining gates. Current counters cover
numeric source construction, selection
and native payload handoff; they do not establish a complete allocator/RSS or
complete foreign-adapter copy profile. Use the ordinary APIs for unsupported workloads;
the experimental retained API never silently materializes those workloads.

## Evidence and remaining work

The executor Cargo unit suite passes 323 tests with 16 existing ignored local
campaigns. New tests cover allocation-preserving handoff, sparse selection,
nullable masks across word boundaries, empty/all-null batches, integer bounds,
float bits after source destruction, exact query budget, aggregate byte and
handle exhaustion, release/retry, sticky failures and final charge release.
The QoS suite passes 92 tests, including concurrent shared-handle admission and
process-policy failures after work-permit closure. These are building-block
tests, not full query or cross-language qualification.

Native cursor regressions additionally cover two-slot and shared-handle pressure,
retry without source advancement, snapshot publication/close, cancellation and
immediate read-pin release, source survival after database destruction,
SKIP/LIMIT range identity, nullable float IEEE bits, fixed empty
schemas, terminal cumulative budgets and bounded sparse-label source work.
Explicitly increasing slots from two to four cannot bypass shared handles;
larger cursor row/payload options cannot bypass database limits.

Remaining #987 work includes whole-operation source/planning qualification, complete
copy/resource/performance profiles, complete C/Go resource and platform
qualification, Python buffer/opaque-consumer resource and platform
qualification, and complete Arrow resource/platform qualification.
The representative large-size
baseline and measured bulk boundary in #976 also remain incomplete. UTF-8,
binary, packed boolean, UUID, recursive layouts, SIMD, graph and FTS extensions
require their own implementation and evidence; this foundation qualifies none
of those capabilities.

## Bounded point-page ownership proof

This historical proof covers the earlier generic page owner. The resident
point primitive below now retains an individual row instead.

The storage regression
`independent_point_page_preserves_values_without_retaining_other_pages`
constructs an independent owner of one existing COW node page. It checks that
the node and string addresses remain unchanged, the full page capacity remains
included in the existing capacity estimate, and unrelated pages and the original
directory can be released. Updating and deleting the live node must leave the
old page unchanged; dropping the final page owner must release it.

This is an executable ownership proof, not a production query lease. It does
not change the retained cursor's complete-source admission or implement source
reuse, allocation/generation provenance, poison propagation, aggregate admission,
or host views. This historical page proof required admitting the complete page;
charging only the selected fields would omit unrelated records pinned by that
page. It must also account for COW replacement memory while a host retains the
old page. Ordinary wide point reads remain on the owned path until those
contracts and comprehensive performance are qualified.


## Admitted resident point-row primitive

`GraphStore::materialized_node_point_read` is a root-internal ownership
primitive for resident nodes. Out-of-core or canonical-base stores return
`Unsupported`; absent live rows and tombstones return `Missing`. The preflight
borrows the exact row and checks its complete retained capacity, including
nested values and spare buffers, before acquiring or transferring the caller's
permit. Only then does it capture the row and two fail-closed poison signals.
The row drops before the permit. Future borrows check the poison signals.

Resident node COW pages contain immutable row owners. Detaching a page clones
row references; mutating a selected row clones that row only when shared.
The map preserves borrowed NodeRecord access, and snapshot-capacity caches
are invalidated before mutable access. Point preflight walks the selected row
without an eager write-time capacity cache and retains no page or directory.

This still requires ordinary point-query and host performance qualification.
Caller policies must cover source ownership, output materialization and COW
replacement generations. It does not provide typed contiguous Arrow arrays,
a host buffer handoff, or whole-process memory admission. Persistence encoding
and the embedded facade remain unchanged.

### Concrete point allocation ownership

`materialized_node_point_read_typed` retains the caller's concrete allocation
permit rather than erasing it into a boxed trait object. The point source keeps
that type's automatic `Send`/`Sync` properties; the existing boxed entry point
and standalone allocation trait remain unchanged. Both entry points use the
same preflight/capture path and validate the complete row reservation before
and after retained-state transfer. A short permit, failed transfer or shrinking
permit releases the allocation without capturing a source row.

The typed ownership regression moves a source to another thread after COW
mutation and store destruction, checks the original record address, then drops
the source and observes permit release. This establishes the native typed-owner
mechanics, not Python/Arrow callback qualification or runtime/process admission.
The caller must supply a real retained query/runtime allocation policy before
host activation; no unsafe Send/Sync implementation or query routing is added.

### Row ownership invariants

For a selected immutable row `R`, the checked capacity estimate includes its
Arc/record allocation, labels, keys, and recursively owned buffer capacities.
Overflow or excessive nesting refuses admission. Preflight borrows `R`; a row
Arc is cloned only after the permit is sufficient both before and after
retained-state transfer. Refusal therefore does not extend payload lifetime.

COW pages hold row Arcs. Detaching a page clones handles; `Arc::make_mut` gives
a writer a distinct row when a reader retains the original. The retained row
and its existing buffers preserve their addresses, and untouched rows remain
shared. A point source retains no page or directory. Its row and poison signals
are declared before its permit, so payload destruction precedes permit release.

Point bounds are computed from the current row on adoption, with no eager
write-time row-capacity cache. Mutable access invalidates the full-source
capacity cache before mutation; detached snapshot generations keep independent
bounds. The B-tree allocation estimate relies on the pinned Rust layout.
Allocator fragmentation, process residency, host handoff, and crash safety
require separate qualification; these ownership invariants do not establish
those broader properties.


## Full-node projection and symbol ownership

Ordinary `RETURN n` is not just a node-record view. The expression runtime
clones the property map and overwrites its `_id` and `labels` entries with
synthetic identity and catalog-derived label names. Retaining a row Arc alone
therefore does not qualify source reuse for this logical result. A leased
projection must preserve those shadowing semantics and must not silently
substitute raw properties or label IDs for the existing logical fields.

The catalog now stores immutable label owners internally. `Label` and the
borrowed `labels()` / `label_name()` interfaces remain unchanged, as do catalog
replay/import and persistence encodings. Snapshot cloning shares the original
label strings; label replacement publishes a new owner. The internal
`retain_label_name_with_admission` preflights the full Arc/Label allocation and
string capacity, calls host admission, validates the returned byte allowance,
then captures only that label owner. Missing IDs skip admission, and typed host
refusals remain unchanged. The label drops before its allocation token.

The private name-lookup directory also shares the same immutable label owner.
Its keys compare and borrow by name, preserving string lookup and ordering
without allocating another name. Same-name import replaces the directory key
so it shares the newly published owner. Different-name import retains the
existing alias lookup behavior. This removes duplicate symbol-name allocation;
it does not establish a whole-query performance improvement.

For a label owner `L`, let `B(L)` be the checked inline Label size, Arc header,
and string capacity under the pinned Rust layout. Capture occurs only after
`permit.bytes() >= B(L)`. Thus refusal acquires no additional label owner, a
retained name preserves its source allocation after catalog replacement/drop,
and unrelated symbols are not retained by this lease. Container/adapter
metadata, allocator fragmentation, and process residency are separate bounds.
Callers remain responsible for a real query/runtime policy; a claimed byte
allowance alone is not a whole-process admission mechanism.

Full-node query activation still requires one coherent admitted owner for the
row, every referenced symbol, virtual-field descriptors, and independent host
views. Shared source capacity must stay charged until the last view is released,
including after cursor close/cancellation. Result-budget accounting must include
source payload retained by a delivered result, not just descriptor metadata.
Admission must precede row/symbol capture and must preserve retryable versus
working-unit refusal. A generation nonce does not replace pointer/range proof.

This symbol primitive does not enable retained full-node queries or construct
Arrow layouts. Native row views and Arrow compatibility require separate
qualification; a B-tree label set and recursive `Value` map are not contiguous
Arrow arrays. Property-only projection, complete-node projection, host handoff,
performance, and whole-operation memory remain explicit open gates.

### Label ownership component cost

A frozen standalone probe compiled the actual `hawdb-core` sources with Rust
1.97.1 in release mode. Parent `ca347097` was compared first against the initial
label-owner revision `60899139`, then against the shared-name-directory
correction. The probe exercised catalog creation, catalog clone/drop, and the
ordinary owned-name projection component. Names were prepared before timing;
creation still included symbol lookup, allocation, insertion, and catalog drop.
Owned-name projection consumed 100,000 label names per round and still copied
their strings on both sides. All outputs had matching expected checksums.

The first 432-observation experiment exposed a regression: clone improved
1.27-1.83x, but creation time increased approximately 11-19%. A correction trial
overlapped local fuzz compilation and is not qualification evidence. Both
experiments remain separate from the repeat below.

After local compilation and fuzz processes finished, the frozen original
parent and corrected binaries ran in B/C/C/B order twice. Each process ran nine
rounds per case with the first discarded: 864 raw observations, 768 measured,
32 measured samples per side/case. Median ratios below are baseline time divided
by candidate time; ratios below one are regressions.

| Symbols | Name bytes | Creation | Clone/drop | Ordinary owned-name projection |
| --- | --- | --- | --- | --- |
| 100 | 16 | 1.068x | 3.345x | 1.074x |
| 100 | 1,024 | 1.223x | 13.901x | 0.976x |
| 10,000 | 16 | 1.082x | 3.680x | 0.984x |
| 10,000 | 1,024 | 1.391x | 18.300x | 1.196x |

The correction removes the creation regression in these measured fixtures;
two owned-name projection medians still regress. This sequential local probe
has no randomized confidence interval or system-wide workload isolation. It
measures neither complete queries nor graph mutations, host delivery, resident
memory, tail latency, durability, or source-to-host zero copy. The component
ratios must not be multiplied by earlier point-source ratios or substituted
for the complete performance gate.

### Experimental native indexed full-node delivery

`Database::query_with_params_retained_node` prepares the parameterized Cypher
query through the ordinary optimizer and accepts one complete-node projection
over an actual equality index seek. A redundant matching equality filter and
an outer zero-offset limit are supported. Other plans, null lookup values,
canonical/out-of-core storage, and multiple matching nodes refuse explicitly;
`LIMIT 1` does not justify silently dropping additional seek matches. The
cursor exposes `retained_indexed_node_view` as its access path.

One aggregate runtime result reservation and query-ledger lease cover the full
selected row, all referenced immutable label-name backing capacities, and
native owner/descriptor capacity before source owners are captured. Label
capacity is observed by ID without scanning the catalog. Additional views
reserve their own runtime handle and query-memory charge before cloning the
source owner. Source destruction precedes charge release, including after
cursor EOF/close, source mutation, or database drop. Missing seeks capture no
source and reserve no retained result owner. Byte/handle backpressure retains
the original view and does not advance the pull.

`RetainedNodeView::field` returns borrowed property values or the synthetic
identity/label-name views. `_id` and `labels` shadow raw properties exactly as
ordinary node projection does; the property iterator omits those shadowed
entries. Logical payload limits exclude shadowed raw properties, but backing
capacity includes them because the row owner retains those allocations. A
small logical result can therefore legitimately refuse a large physical
working unit. No property map, label string, or Arrow array is constructed for
the delivered native payload.

The native result remains provisional until successful EOF. Existing views
keep their immutable payload after close; subsequent borrowed access still
checks source integrity flags. The context-free entrypoint remains independent
of a host task. The [task-bound Rust cursor](RETAINED_NODE_TASK_LIFECYCLE.md)
borrows the host context, checks open pulls and EOF for cancellation/deadline,
and honors existing query/result reservations. [Python](PYTHON_RETAINED_NODE_VIEWS.md)
and [C/Go](GO_RETAINED_NODE_VIEWS.md) adapters provide independently owned
recursive views for this same narrow plan. Consumed materialized read snapshots
use the same node owner/admission path and keep their owned task bound through
EOF; consumption releases unrelated snapshot directories/caches and reader pins.
Foreign task binding, multiple-node streaming and an Arrow node representation
remain open. Planning/index-key workspace, ledger and
allocator residency, process-wide memory, execution/RSS profiling,
performance, tail latency, and durability remain independent qualification
gates. Passing the native ownership regressions does not establish the complete
performance gate or authorize replacing ordinary host query delivery.

#### Native query delivery diagnostic

A local release probe compared the ordinary owned Rust API with the native
indexed full-node API in the same candidate source. Both paths prepared the
same parameterized equality query, warmed up every ID, and consumed every
logical field with matching checksums. Each fixture used nine alternating
rounds per path, discarded the first, and retained eight measured samples.
The 54 raw observations and 48 measured observations cover 128, 1,000, and
10,000 resident nodes with a 273-byte text attribute and shadowed reserved
properties. No JSON conversion was inside either timed path.

| Nodes | Owned median ns/query | Native median ns/query | Owned/native |
| --- | --- | --- | --- |
| 128 | 18,080 | 10,868 | 1.664x |
| 1,000 | 27,157 | 10,545 | 2.575x |
| 10,000 | 129,891 | 10,434 | 12.449x |

This repeat froze parent `da02e302` plus the candidate source; the native module
SHA-256 was `e6f476c034f3eb9331e397bbf96fb0932df8636232bf308bd63a56925856b252`.
It is a same-source API diagnostic, not a before/after revision comparison.
The owned API runs ordinary observers, whereas the native API currently uses
its own profile and retained-result admission. These entrypoint differences
prevent attributing the entire ratio to avoided payload copying. No Python/Go,
Arrow, file/WAL, mixed/concurrent workload, tail latency, or RSS was measured.
The earlier prototype observations remain separate. Raw observations and the
standalone driver are preserved in
[`native_indexed_node_controls_20261012.json`](../bindings/benchmarks/evidence/README.md#native-indexed-node-controls-20261012).
Neither run satisfies the complete performance gate.

#### Terminal observation and admitted cursor control

The native cursor shares the ordinary statement-summary, successful slow-log,
and telemetry recorder. The recorder accepts row counts and typed error display
references, without constructing an owned `QueryOutput`. Native success is
recorded exactly once at EOF, including empty results; close records an unfinished
statement failure. Constructor failures are recorded after successful parsing,
as in the ordinary query path. Retryable view backpressure does not finish the
statement. Telemetry receives actual native output payload bytes on successful
completion; unavailable operator/RSS samples are not synthesized.

Query text and cursor recording state have independent runtime retained-memory
and query-ledger reservations before their allocation/capture. The selected row
and recording control use the same query account, and their combined capacity
must fit the working-unit limit. Empty cursors retain no row/result owner but
still admit their recording control. EOF/close releases that control; immutable
exported views retain their own row/result charges. The recorder can finish after
the database is dropped. An unwind guard marks cursor/views failed if a completion
callback panics; it never publishes successful EOF before callbacks return.

The guarded repeat uses the same full-field/all-ID fixtures and round policy as
the prior diagnostic, with statement-summary execution/row/error counts checked
outside each warmup and timing interval. Its native module SHA-256 was
`7a7365635313d3bf54240250971ffb03a1efd9f8968ef3ac8b0f4e631c282efd`.

| Nodes | Owned median ns/query | Native median ns/query | Owned/native |
| --- | --- | --- | --- |
| 128 | 24,417 | 16,935 | 1.442x |
| 1,000 | 25,808 | 11,700 | 2.206x |
| 10,000 | 130,370 | 12,196 | 10.690x |

[Raw observations and driver](../bindings/benchmarks/evidence/README.md#native-observed-node-controls-20261012)
remain a same-source API diagnostic. Shared statement recording removes a known
missing-observation difference, but execution/RSS profiling and retained-result
admission still differ. No before/after revision improvement or complete
performance gate is established. Python/Go, Arrow, graph/FTS, writes, mixed/large
workloads, memory residency, tail latency and durability still require evidence.
