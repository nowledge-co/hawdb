# Experimental Python retained node views

`Database.execute_retained_node` uses the native indexed complete-node cursor.
It accepts an equality-index point plan returning one node variable, with a
unique constraint on the seek property, for example
`CREATE CONSTRAINT ON :Document(id) ASSERT UNIQUE`. Without the constraint,
construction raises `RetainedError` with `kind="unsupported_plan"` before
retained-source access or admission, regardless of the current match count.
A second match under a unique constraint is a storage integrity failure exposed
with `kind="execution"`; no cursor or partial row is published. An existing
index alone is insufficient: the optimizer may choose a scan for a tiny label,
which refuses explicitly. The API does not force index selection or secretly
materialize an owned fallback. Missing
matches complete with no row owner. Scans, computed node
projections, and layouts requiring hydration refuse explicitly. An open user
transaction is rejected by the embedded facade, as for numeric retained queries.

```python
cursor = db.execute_retained_node(
    "MATCH (n:Document {id: $id}) RETURN n", {"id": 42},
)
node = cursor.next_node()
title = node.field("title")
payload = memoryview(title)
assert payload.readonly
assert cursor.next_node() is None
title.close()
node.close()
cursor.close()
# payload still owns the original UTF-8 bytes until its final release.
payload.release()
```

The result is a native owner, not a Python node dictionary. `field(name)` returns
an independently admitted child view; missing names return `None`. Nested maps
support `field`, and nested lists support `item(index)`. Paths contain only
indices into the immutable source, without copying property names or payload.
Map lookup currently walks ordered entries; this is not an optimized wide-map
lookup contract. The synthetic `_id` and `labels` shadow raw properties just as
ordinary complete-node results do. Label items borrow catalog-owned UTF-8 names.

UTF-8, binary, UUID and label views expose a read-only one-dimensional `B` buffer.
Integer, float, boolean and synthetic identity fields expose one-element native
`q`, `d`, `?` and `q` buffers over original scalar storage. Identity storage is
unsigned internally; its identical bit pattern yields the ordinary signed
logical identity when consumed as `q`. Null fields expose their kind instead of
pretending to contain a scalar buffer.
Its bytes reference the original source allocation. This representation is not
a Python Unicode object or an Arrow string array. `bytes(view)` and decoding
are explicit consumer copies. `scalar_copy()` constructs ordinary Python values
only for null, boolean, integer, float and synthetic identity fields; it refuses
variable-width/container values rather than materializing them implicitly.
Containers do not pretend to provide contiguous Arrow buffers.

## Ownership and accounting

Before constructing a Python child descriptor, the native row owner admits its
path capacity and conservative Python object overhead. Each buffer acquisition
admits a separate lease, shape/stride storage, `Py_buffer` and memoryview
overhead before allocating the lease. Closing the exporter cannot revoke a
successful buffer acquisition. The row, labels and all nested values remain
immutable across source updates, database closure and cursor completion.
The final buffer release returns its view handle and source capacity.

Every open pull, including empty/post-delivery EOF, checks the retained
source-health signals. A detected poisoned source fails terminally instead of
marking provisional results Completed; existing immutable buffer leases remain
mapped. The health owner pins no rows or directories.

The cursor admits Python control metadata even for an empty result. A separate
shared-memory control reservation remains until the native cursor is destroyed,
including after EOF; `cursor.close()` drops that native owner. Observation and
control overlap is included in admission. Failure to
allocate the Python root view aborts foreign delivery; successful EOF must not
mark a failed handoff complete. Cursor close records unfinished failure and
preserves already exported bytes. Independently retained owners keep the native
extension rooted; each Python owner contributes its own GC edge.

These charges cover this protocol's conservative descriptors and retained source
capacity, not exact Python allocator residency or whole-process RSS. Parameter
conversion and explicit scalar copies remain host work. Consumers can retain
views indefinitely subject to shared handle/byte admission; active pull alone
does not bound the number of retained old row generations. Such readers can
force COW payload copies on writes.

## Qualification status

The native regression checks additional metadata admission, overflow rollback,
failed handoff status and final quota release. Python regressions use actual
`PyObject_GetBuffer` and `memoryview` consumers, compare pointer addresses,
exercise embedded updates, EOF/close/database lifetime, nested maps/lists,
binary/label values and explicit refusal paths. CI runs these in the existing
native binding suite on memory and file fixtures; a green result must be read
back at the delivered SHA before runtime qualification is claimed.

This API does not establish complete source-to-host qualification or the PR's
comprehensive performance gate. Go node views, Arrow-compatible layouts,
read-transaction/task integration, planning/RSS parity, wide/nested traversal
performance, graph/FTS, persistent writes, mixed workloads, tails and power-loss
qualification remain open. Ordinary Go point improvements at older revisions
cannot be attributed to this adapter.
