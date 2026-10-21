# Source-coherent Search catch-up

Tracking: [#1043](https://github.com/nowledge-co/hawdb/issues/1043).
This is an additive embedded-library preparation path based on main, independent
of the stationary Consumer implementation in #1039. It does not enable Mem
Search ACK, replace registered Consumer recovery, or close #1092/#1106.

## Source and publication boundaries

One `ProjectionStagingSession` captures the complete C..E change interval and
one immutable source snapshot E. Its private ordinal can pause inside a commit.
Every visited item, including repeated keys, consumes cumulative admission;
input selection does not clone or repeatedly scan a retained transaction prefix.
Graph and SQL mapping use the same tracked E read context. Mapping failures,
ignored read errors, budget exhaustion and cancellation poison the session.

The mapper is a fixed trusted host function. It must propagate incomplete work,
use the supplied source reads, and represent all relevant graph and relational
effects. Schema/opaque capture gaps require rebuilding rather than skipping
input. Native cold preimage capture runs before WAL append and checkpoint replay
uses that same collector with its cooperative work context. Append tables and
named graph projections are outside this source domain: tracked SQL refuses
append tables and tracked Cypher refuses named-projection algorithms before
execution. `Append` and `ProjectGraph` WAL records therefore leave the canonical
changefeed and its resume floor unchanged. Extending supported tracked reads to
these domains requires a matching capture and full-enumeration contract first.

Mapping and external ordering retain one admitted stage, bounded continuation,
memory ledger and cumulative read/spill/sort counters. Repeated identical E
upserts coalesce; conflicting content refuses. E upserts supersede retractions.
This reduction alone cannot certify host mapping or a historical baseline.

An intermediate B < E is private progress only. `ProjectionStagedInputs` is
opaque and cannot enter the Search publication API. Successful enumeration of
End does not by itself make an arbitrary candidate publishable.

## Complete-E verification

`verify_complete_source` consumes a closed stage and retains its original E
without recapturing a live source. An exhaustive native source cursor and fixed
zero/one-document mapper produce independent expected E documents. The same
sorter admits them and rejects duplicate IDs, including identical duplicates.
This full mapper defines the trusted projection contract; it must not depend on
unversioned external state.

The comparison streams the reduced delta and unique expected documents:

- An expected ID with an upsert must have identical encoded content.
- An expected ID without an upsert must exactly match the held base document.
- A deleted ID must be absent from the expected set. An upsert missing from the
  expected set refuses.
- The expected count must equal the base count plus actual insertions minus
  actual deletions. Unique membership plus cardinality detects untouched extra
  base documents and missing retractions.

Both streams validate their recorded extent and digest. Only the original
compared mutation run is restored for artifact construction. The resulting
`ProjectionCoveredUpdate` privately owns that candidate and its dependencies;
callers cannot exchange a token, candidate or dependency set between sessions.

`Database::publish_projection_covered_update` rechecks the source instance,
dependencies, exact candidate/source E, observed source durability and expected
Search selector under publication exclusion. Rejection preserves the old head.
A configured synchronous policy or recovered WAL is not itself an observed
durability barrier. Explicit synchronization does not weaken future commits.

## Compatibility and API scope

After reopening a persistent database with an uncheckpointed WAL tail,
`publish_prepared_projection` and the default prepared Search publication fence
can return `SourceNotDurable`, including with `SyncOnEveryWrite`. Recovery proves
which transactions are present, not that this process observed a completed
synchronization barrier. Hosts must checkpoint successfully or use the explicit
`ProjectionSourceDurability::Synchronize` publication variant after reopen.

The generic `publish_prepared_search` fence validates identity, dependencies,
durability and the expected selector; it proves nothing about document content.
Only `ProjectionCoveredUpdate` binds the complete-E comparison to the candidate
that is published. Intermediate staging/output inspection/baseline comparison
and raw cursor APIs are hidden from the public documentation until there is a
production consumer. Their visibility for qualification tests is not a stable
production integration contract.

## Capture and spill budgets

The canonical source commit path has no fixed operation-count capture cap.
Large admitted transactions do not lose their changefeed solely because they
contain more than 32K creates or traverse more than 65K adjacency entries.
Checkpoint replay checks cooperative capture work in groups of at most 1,024
internal metadata/adjacency work items and again before returning; cold readers
retain their own I/O admission. This grouping bounds logical work between checks,
not elapsed time, and does not cap transaction size.

Capture still has a 64 MiB cumulative metadata/preimage allocation budget,
a 64 KiB allocation ceiling per node preimage, and a nested-batch depth ceiling
of 64. Large IDs, high fanout or other bulk imports can exceed those byte/depth
limits. A successful source commit then installs an explicit gap at its epoch,
clears earlier retained changes, and makes older consumers return `BehindFloor`.
It never emits a truncated delta. Hosts should split bulk imports when suitable,
or finish importing and perform one complete rebuild at the final source epoch.
A resource/I/O refusal before WAL append instead returns an error; typed FD and
resource-budget failures preserve the handle for retry after capacity returns.
Canonical, adjacency, property-spill, property-projection and descriptor readers
classify nested resource/cancellation causes before latching physical-failure
poison. Stable-identity I/O maps these causes to its existing admission error
before converting diagnostics to text. Physical failures and corruption retain
their fail-closed behavior.

External sorting charges spill writes and disk reservations cumulatively across
all merge passes. Deleting an input run does not refund either reservation. With
R initial runs, non-coalescing inputs can require about input bytes times
`(1 + ceil(log2(R)))`, plus framing and the independent full-source verification
stream. One million inputs with a 1,024-record buffer produce roughly 977 runs
and about 11 passes' worth of write accounting. The default 4 GiB spill limit is
not permission to ingest 4 GiB of raw input. Hosts must budget the merge
amplification, verification and final artifact build, or select larger admitted
buffers to reduce the number of runs. Exhaustion refuses the private stage.

## Costs and remaining gates

This conservative implementation performs O(full source) extra verification and
records a Database dependency. **Every post-E source commit refuses activation,
including disjoint writes.** Fixed-E reads remain stable while writers continue,
but this fallback does not deliver the design's disjoint-write progress goal.
Do not remove that barrier without a complete source-enumeration/mapping proof.

Input, mapping, sorting and comparison have bounded logical work and resource
admission. Final artifact construction and publication remain blocking; these
limits are not physical I/O, RSS or elapsed-time guarantees. The full-source
mapper currently supports zero or one document per native input, not arbitrary
resumable fanout. Private work is discarded on cancellation/restart and rebuilt
from the last selected base. A committed E head survives a lost caller response;
this does not supply an idempotent registered-Consumer operation receipt.

Hosts must hold their binding authority through activation. Versioned host
binding/incarnation, durable coverage receipts, external vectors, optimized
owner reduction, short activation and actual Mem integration remain open.
The legacy API's #1108 prefix refusal remains intact and is not routed through
this new path automatically. Its earlier mixed-snapshot bug is historical;
current main rejects the incoherent prefix.

The [COW evolution design](MVCC_COW_EVOLUTION_SPEC.md) retains COW values,
ordinary snapshot retirement and persistent Consumer registration as separate
lifetimes. [Neumann et al., SIGMOD 2015](https://15721.courses.cs.cmu.edu/spring2016/papers/p677-neumann.pdf)
informs complete dependency validation and safe reclamation. Its undo-based
value representation and main-memory measurements do not mandate a HawDB layout
change or establish WAL/power-loss safety. Optional serializability is not a
prerequisite for this Search path.

## Verification ownership

The default/minimal `hawdb_mvcc_staging_session` targets include
`tests/support/projection_coverage.rs`: one-item update/delete and ABA,
hot/cold and relational equality with full rebuild, repeated keys/rename,
empty intervals, wrong content, phantom upserts, deletion of surviving documents,
omitted deletes, duplicate IDs and exact budget refusals, disjoint post-E writes,
source/base mismatch and cancellation/reopen. Append and named-projection writes
preserve capture and full-rebuild equality through WAL/checkpoint reopen.
`hawdb_mvcc_full_source` and `hawdb_mvcc_baseline_comparison` qualify enumeration,
comparison and the independent full-rebuild reference. All belong to root CI.
Storage/Search component tests cover 65,537-node bulk capture, oversized-preimage
gaps, cold node/adjacency/relationship/spill FD exhaustion and retry, nested
resource/cancellation refusal versus physical corruption, durability barriers
and private preparation.
See [the test index](../TEST_INDEX.md) for execution lanes.
Reopen tests are not fault-image or power-loss qualification.
