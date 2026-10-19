# Experimental retained node views for C and Go

Issue Number: ref #1081, ref #987

## Workload and layout

`DB.QueryRetainedNode` forwards parameterized Cypher to the embedded retained
node runtime. The eligible physical plan is a materialized, single-match index
seek returning one complete node, optionally with the exact predicate and a
zero-offset limit. The seek property must also have a unique constraint, such
as `CREATE CONSTRAINT ON :Item(id) ASSERT UNIQUE`. Without it, the lookup returns
`RetainedUnsupportedPlan` before retained-source access or admission, regardless
of the current match count. A second match under a unique constraint reports
storage integrity failure through `RetainedExecutionError` and publishes no
cursor or partial row. An index existing in the catalog is insufficient: a tiny
label can cost-select a scan. Unsupported plans,
hydrated/out-of-core rows, open user transactions, and writable requests refuse
explicitly. There is no JSON result fallback or copied Arrow packing.

The node remains row-oriented. Its immutable native row owns properties and
nested containers; immutable catalog owners supply label text. Point/full-row
reads do not first construct column arrays. The numeric batch and Arrow APIs
remain separate contracts for suitable column workloads. This node API does
not implement Arrow list offsets, heterogeneous Arrow arrays, SIMD kernels,
graph traversal, FTS candidate delivery, or multi-row scans.

## Interface

The additive C ABI uses `HawdbRetainedQueryV1` and the existing cursor, state,
and release operations. New symbols are:

- `hawdb_retained_node_query` and `hawdb_retained_node_next`;
- `hawdb_retained_node_field` and `hawdb_retained_node_child`;
- `hawdb_retained_node_retain`.

Each successful node/value operation returns `HawdbRetainedNodeV1` with an
independently releasable namespace/id. The descriptor contains its kind, child
count, readonly original data range, and borrowed UTF-8 name. The name is the
query column for the root, a field key for node/map children, or empty for list
items. Node order is `_id`, `labels`, then sorted unshadowed properties; map order
is native key order and list order is index order. Missing fields and invalid
indices return `InvalidColumn`, with a zeroed output and no published handle.
NULL and containers have no data buffer. UTF-8, binary, UUID and label leaves
expose original bytes. Integers/identities, floats and bools expose original
native-endian storage; identities interpret the original 64 bits as signed.
Bool occupies one byte. These ranges are not Arrow allocation descriptors and
do not claim an invented allocation id or capacity for each nested leaf.

Go exposes `RetainedNodeCursor.Next/State/Close` and
`RetainedNodeValue.Field/Child/Retain/Kind/ChildCount/ByteLen/State/Close`.
`ByteAt`, `Int64`, `Float64` and `Bool` load from immutable original storage
under the owner read lock. No mutable slice, raw pointer or unleased string
escapes. `NameCopy` explicitly copies metadata. There is no implicit foreign
node dictionary or payload conversion. Scalar return values and caller-created
buffers are consumer work, not borrowed payload owners.

The Go adapter requires a 64-bit host and compatible optional symbols. Older
libraries remain usable for ordinary queries; requesting the missing capability
returns `capability_unavailable`. Query text and parameters still cross as UTF-8
and JSON input. Input normalization/encoding is not claimed to be zero copy.

## Active pull, retention and memory

There is no prefetch. Successful EOF makes provisional values final after a source-health barrier; unfinished
close marks them Closed. Published owners remain readable after cursor, parent
and database closure, and preserve the old generation across later writes.
Readers should close each independently owned child promptly. Copying a Go
wrapper shares one close state; `Retain` admits a distinct owner.

The native runtime admits complete row/catalog source capacity and control
metadata. A lightweight health owner keeps only two shared poison signals,
without a node directory, row payload or file lease. Its conservative shared
atomic/header capacity is included in control admission; point owners also
admit those signals because exported payload can outlive successful EOF. Child/retain operations reserve registry/object/descriptor capacity,
a 512-byte foreign-wrapper allowance, and the complete index-path capacity
before allocating the path or cloning the native view. The root/control options
include the same allowance. Foreign cursor control has a separate shared-memory
reservation retained until cursor destruction, including after empty EOF. Its
conservative observation/control overlap is admitted before allocation. Each payload view consumes a shared runtime handle;
backpressure publishes no partial owner and preserves the parent and its quota.
The path stores indices, without cloning property keys or values. Keeping a
small leaf alive still pins the complete originating row/catalog owners.

These reservations cover the stated native owners and bounded adapter allowance,
not exact Go allocator size classes, stack frames, finalizer queues, input JSON,
planning/index-key workspace, whole-operation peak memory, or whole-process RSS.
A slow consumer can pin old generations and induce copy-on-write on later writes.
Active pull alone does not bound host retention; shared limits and timely Close
remain necessary. Database default budgets and commit durability are unchanged.

## Ownership argument and assumptions

Let a published handle own an immutable native view and an index path. Its
resolved descriptor borrows only that view's row, catalog labels, column name,
or static synthetic names. The following is a source-level inductive argument,
not a machine-checked proof or a sanitizer qualification:

1. Root delivery obtains an admitted native view before allocating its registry
   owner. The empty path resolves that original node.
2. Child delivery validates the current immutable path/index, admits all new
   metadata, then copies only the index path and retains the same source owner.
   Therefore each child can resolve its original key/data independently of its
   ancestors, with no payload duplication.
3. Retain preserves the path and source allocations under independent admission.
   Allocation failure/unwind releases the tentative native view. Invalid input
   and refused admission leave output empty and publish nothing.
4. Registry handles are namespaced and monotonically allocated without reuse;
   stale/foreign handles cannot dereference caller-supplied addresses. Temporary
   lookup references keep the object alive during each C operation.
5. C callers must keep the library mapped and synchronize all raw pointer reads
   against release. Go mappings live for the process lifetime; each scalar/byte
   read holds the same owner read lock that Close excludes. Go finalizers are a
   safety net, while explicit Close is the resource control path.
6. Each open pull, including empty or post-delivery EOF, checks both source
   poison signals before completion. Detected integrity/apply failure becomes
   sticky Failed, records one failed statement, and cannot publish successful
   EOF. The barrier is a checkpoint of monotonic flags; faults arising after
   its check do not retroactively invalidate an already completed result.
7. Root handoff errors or registration panic abort the native cursor and preserve
   a sticky terminal failure. A later pull cannot convert that failure into EOF
   success. Existing state/release operations also recognize node owners.
8. Immutable row/catalog generation ownership means subsequent writes publish a
   different generation when required. Removing one registry handle cannot free
   payload retained by another independently admitted view.

Assumptions include valid caller buffers and synchronized raw C reads, compatible
native ABI layouts, immutable native source ownership, and successful host
wrapper construction. Fatal allocator exhaustion, module unload outside the Go
contract, and foreign misuse are not recovered by the panic boundary.

## Verification scope

C ABI regressions cover nested binary/labels, pointer identity, old payload across
write and parent/database release, EOF/unfinished-close state, final retained
quota zero, handle admission rollback, stale/foreign handles, invalid fields,
missing seeks and sticky registration panic. Go regressions cover memory/file
stores, updated-row readback, native scalar values, recursive ownership, layout
widths, strict refusals, and concurrent reads/Close. They are registered in the
existing native-bindings CI, including its `TestRetained` race filter.

Ordinary regression execution is delegated to CI. Local mandatory formatting,
strict workspace and standalone FFI Clippy, prek hooks, and the default fuzz
command remain required. Local Go test compilation checks discovery without
executing regressions. Performance qualification, especially write timings on
the current loaded machine, is deferred; no throughput/latency gain is claimed.
Broader Arrow/SIMD, graph/FTS, task/transaction lifecycle, whole-operation memory,
and comprehensive workload qualification remain open.
