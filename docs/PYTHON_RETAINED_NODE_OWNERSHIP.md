## Safety argument and assumptions

The invariant is that every successfully exported pointer has an independently
admitted `NativeView` in its `NodeBufferLease`, referring to the same immutable
source row and catalog symbol owner used to resolve that pointer. Child paths
contain indices only; they do not become part of the exported pointer lifetime.

1. Root delivery moves the admitted native view into the Python root. Child
   construction first reserves its descriptor bytes and handle, then allocates
   the path and retains that same source. Failed admission allocates no child.
2. Buffer resolution runs while the Python exporter is borrowed. Before that
   borrow is released, a separate admitted native view is placed in a stable
   boxed lease. `buf` references original storage; `shape` and `strides` reference
   that lease. Successful publication transfers the lease to `Py_buffer.internal`
   and transfers an exporter reference to `Py_buffer.obj`.
3. Close/drop of a root, child, cursor or database cannot remove the lease's
   source owner. Source updates use COW when this immutable row remains shared;
   label symbols retain their immutable owner. Neither operation relocates the
   exported storage. EOF/failed/closed status changes do not revoke pointers.
4. The matching release callback clears `internal` and destroys that one lease.
   Its native view releases its handle/metadata and eventually the source charge
   when the final owner disappears. CPython releases `obj`; the callback must
   not decrement it a second time.

The induction starts at the admitted root and is preserved by child retention,
buffer export, close, mutation and final release. Its assumptions are the native
row/symbol immutability contract, valid CPython buffer acquisition/release calls,
and the consumer respecting read-only storage. This is a source-level argument,
not model-checker, sanitizer or whole-process memory qualification. Payload COW
on writes and arbitrary caller-held view counts remain performance obligations.
