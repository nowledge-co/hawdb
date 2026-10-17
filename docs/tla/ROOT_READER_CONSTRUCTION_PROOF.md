# Private root construction: conditional resource argument

The private `out_of_core::admitted_root` constructor connects root metadata,
content segments, mutation runs and target validation to one borrowed admission.
This document gives a structural argument about those owners. It is not a new
TLC result or an end-to-end heap, GC, source-incarnation or MVCC proof.

## Ownership invariants

For each newly retained payload family `P`, construction requires its query
reservation `Q` and host reservation `H` before expanding owned metadata:

`Live(P) => Held(Q) /\ Held(H)`.

For temporary encoded buffers, native-path scratch and validation sets, the
working grant and borrowed execution remain live until their destruction.
Shared children additionally satisfy `Refs(child) > 0 => Live(child)` under the
assumptions in [shared reader capacity](SHARED_READER_CAPACITY_PROOF.md).
The guarded read boundary uses the native lease protocol documented in
[admitted native custody](ADMITTED_NATIVE_LEASE_PROOF.md); serving intermediates
use [borrowed cursors](BORROWED_QUERY_CURSOR_PROOF.md) and the
[one-slot target protocol](TARGET_REANALYSIS_IO_PROOF.md).

The root-specific obligations are:

| Payload/work | Admission boundary and lifetime |
| --- | --- |
| Root path, configuration and analyzer | `root_bytes` counts the incoming buffers' actual capacities, including spare alias-vector/string capacity and stopword node allowances. Host inputs already exist before the call; this does not bound their allocation by the host. The constructor admits them before retaining a usable result and performs no configuration/analyzer clone. |
| Root head bytes | `read_unbound` holds I/O and native custody while learning length, rejecting an oversized cold object, allocating the admitted encoded buffer and reading in at most 8 KiB calls. Held-file reads survive path replacement. |
| Manifest strings/vectors | `decode_capacity` preflights received bytes, including compact record arrays, before serde expansion. Its guard also covers transient validation sets. Parse and canonical checksum boundaries preserve known cancellation. |
| Segment/run vectors | One retained receipt precedes exact-capacity vector allocation. Each successfully opened child separately owns its descriptor/layout/lexical/vector or mutation/term receipts. |
| Closure validation indexes | Checked declared counts reserve working space before active-segment and retracted-target sets are built; task checks occur while counting and validating records. Logical mutation limits remain additional ceilings. |
| Target hydration/reanalysis | The same admission reaches probes, compressed capture, body/term reads and CJK worker join; no independent default task is created on this branch. |

This argument assumes the pinned collection/serde capacity bounds and correct
internal pairing of task, accounts and permit. Arithmetic and capacity failures
fail before ownership expansion; temporary and partially constructed values
unwind normally. The legacy branch retains its existing error classification.

## Return, clone and failure argument

`OpenedRoot` declares its reader before its three root/manifest/collection
receipts. Rust destruction therefore drops its owned metadata and child handles
before returning those grants. A cloned segment or term file retains its own
shared payload receipt after the parent disappears. The common receipt drops
the query/account reference before the host reservation. It stores no execution
permit, and the native producer-close test observes zero active CPU/task/I/O
slots while the private result and an independent child remain alive.

Failure at any constructor boundary drops the initialized prefix. No root is
returned after a failed decode, child open, closure check or target validation.
The private wrapper has no `Deref` or public serving/refresh accessor, and
`into_ordinary` refuses an admitted result. The ordinary public constructor uses
the legacy branch; callers cannot acquire an admitted serving capability through
this unfinished constructor. Admitted reuse is also refused until source-root
custody is implemented.

## Native evidence and limits

The focused native tests cover a complete mutation closure built through the
actual writer, Chinese target reanalysis with observed worker join, producer
closure, independent shared children, incoming analyzer capacity refusal before
path lookup, project-authority refusal and the public-conversion fence. A
checksum-valid compact manifest remains below the 1 MiB encoded limit but its
ordinary buffer/decode path exceeds 2 MiB; admitted construction refuses before
that owned expansion. Separate head-read cases cover held-file replacement,
size refusal and cancellation before a large buffer allocation.

The allocation experiment initially retained 48 bytes after query/view close.
An allocation backtrace identified macOS native condvar initialization in
`GovernorIoWavePermit::drop`. The actual governor owns it; closing that owner
returns measured live allocation to zero. The test includes that destruction
without warming the path or weakening its zero/peak assertions.

Admission borrows an existing project instead of constructing a new global
descriptor/cache domain. Canonical containment rejects an unrelated supplied
project, but does not prove binding identity or prevent namespace ABA. Tests use
native files with existing project owners. They do not qualify a complete cold
managed closure or establish project cache/registry heap admission. Governor
infrastructure, allocator overhead and process RSS are separate from requested
reader payload capacity.

Required remaining work includes incarnation-bound GC roots shared by each
independent child, cleanup selection/unlink linearization, cache/registry owners,
cumulative physical work, parser/cleanup/open-lock latency, fresh serving and
refresh contexts and the final typed production entrypoint. Closed C-ready/E
mapping, source-coherent bounded C..E coverage, atomic activation/durable receipts,
actual Mem integration and all original power-loss/writer/fairness/fsync gates
remain open. Full regression and feature qualification are separate evidence.
