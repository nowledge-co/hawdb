# Retained numeric ownership foundation

This implements ownership and admission building blocks for
[issue #987](https://github.com/nowledge-co/hawdb/issues/987), under the
[columnar interchange proposal](specs/ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md).
It does not expose a strict query cursor, language view, or Arrow export.
The complete proposal and #987 remain unfinished. Production hosts continue
to use the root `hawdb` facade; this internal executor module is not a new
integration surface.

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

## Evidence and remaining work

The executor Cargo unit suite passes 323 tests with 16 existing ignored local
campaigns. New tests cover allocation-preserving handoff, sparse selection,
nullable masks across word boundaries, empty/all-null batches, integer bounds,
float bits after source destruction, exact query budget, aggregate byte and
handle exhaustion, release/retry, sticky failures and final charge release.
The QoS suite passes 92 tests, including concurrent shared-handle admission and
process-policy failures after work-permit closure. These are building-block
tests, not full query or cross-language qualification.

Remaining #987 work includes a root-facade eligible-plan contract, bounded
resumable demand source, default two-slot/no-prefetch cursor, typed refusal and
terminal/provisional state, cumulative result limits, range/alignment descriptors
and complete copy/resource profiles, C/purego/Python read-only owners, module
lifetime, and compatible Arrow export/refusal. The representative large-size
baseline and measured bulk boundary in #976 also remain incomplete. UTF-8,
binary, packed boolean, UUID, recursive layouts, SIMD, graph and FTS extensions
require their own implementation and evidence; this foundation qualifies none
of those capabilities.
