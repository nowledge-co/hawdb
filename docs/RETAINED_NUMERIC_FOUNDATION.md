# Experimental retained numeric delivery

This implements ownership building blocks and an experimental native cursor for
[issue #987](https://github.com/nowledge-co/hawdb/issues/987), under the
[columnar interchange proposal](specs/ZERO_COPY_COLUMNAR_INTERCHANGE_SPEC.md).
Source snapshot/planning workspace qualification, language views and Arrow
export remain incomplete. The complete proposal and #987 remain unfinished.
Production hosts continue
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
visible through earlier independently retained batches. Cursor/database close
and cancellation release the source without revoking produced values.

The database and all its read snapshots share one retained governor binding,
chosen at the first eligible cursor. Configure the governor before that first
cursor; later ordinary-governor replacement does not create a second retained
allowance. Native batches retain schema/status and admitted payload owners,
without retaining the database, source iterator or read pin. CPU permits are
released between pulls. Profiles retain inspected/constructed/selected work even
when a later delivery budget fails; emitted rows/bytes include successful
deliveries only.

This is not yet a qualified whole-operation memory bound. Root cursor creation
still uses the existing read-snapshot and optimizer paths, whose catalog,
statistics, source-pin and planning workspace allocations need a complete
admission/capacity audit. The direct database entrypoint avoids cloning unrelated
slow-query and statement-summary history, but that does not resolve the remaining
workspace gate. Current counters cover numeric source construction, selection
and native payload handoff; they do not establish a complete allocator/RSS or
foreign-adapter copy profile. Use the ordinary APIs for unsupported workloads;
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
read-pin release, SKIP/LIMIT range identity, nullable float IEEE bits, fixed empty
schemas, terminal cumulative budgets and bounded sparse-label source work.
Explicitly increasing slots from two to four cannot bypass shared handles;
larger cursor row/payload options cannot bypass database limits.

Remaining #987 work includes source/planning workspace admission, complete
copy/resource/performance profiles, C/purego/Python read-only owners, module
lifetime, and compatible Arrow export/refusal. The representative large-size
baseline and measured bulk boundary in #976 also remain incomplete. UTF-8,
binary, packed boolean, UUID, recursive layouts, SIMD, graph and FTS extensions
require their own implementation and evidence; this foundation qualifies none
of those capabilities.
