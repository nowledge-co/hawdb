# Source-coherent Search catch-up

Tracking: [#1043](https://github.com/nowledge-co/hawdb/issues/1043).
This is an additive embedded-library preparation path based on main, independent
of the stationary Consumer implementation in #1039. It does not enable Mem
Search ACK, replace registered Consumer recovery, or close #1092/#1106.
The [registered Consumer facade](../REGISTERED_CONSUMER_CATCH_UP.md) now connects
this comparison to stationary publication and durable Consumer recovery. Its
token retains the compared full-E stream, exact base receipt and source proof;
it invokes no mapper again while constructing the registered generation.

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
A resource/I/O refusal before WAL append instead returns an error; native FD and
resource-budget failures preserve the handle for retry after capacity returns.
Internal change capture and registered catch-up steps always use non-strict
project descriptor admission, including when foreground mode is strict. This
retains accounting without weakening preimage, memory, result or physical-I/O
limits; see [descriptor capacity](../FILE_DESCRIPTOR_CAPACITY.md).
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

Independent verification MUST retain complete-source enumeration and exact
comparison with the selected base. Database scope retains a conservative
Database dependency and refuses every post-E canonical commit. Explicit Domains
scope captures the declared label/table dependencies, including absence; a
post-E write outside those dependencies MAY coexist with publication. Reads
outside the declared scope MUST fail. The existing scoped validation and
retained native source pin remain authoritative.

A `SearchProjectionDefinition` binds logical name, nonzero host semantic version,
canonical scope, initializer, delta mapper and complete mapper. Its derived
native Consumer ID MUST distinguish different versions/scopes and graph/table
namespaces. Domain order and duplicates MUST NOT change identity. Hosts MUST bump
the version after changing mapper/initializer semantics. Native target binding,
exact content comparison, original-owner recovery and existing durability fences
MUST NOT be weakened by this convenience contract. It introduces no new durable
publication state transition; the existing model's Consumer identity is simply
the derived ID.

Scoped graph enumeration MUST use one pinned base/delta/tombstone view. It MUST
not probe historical NodeId holes, duplicate overlapping labels, resurrect a
captured deletion, or see later source writes/checkpoints. Cold segment pruning
MUST use a complete label synopsis including propertyless nodes; Bloom false
positives are permitted and false negatives are forbidden. Each descriptor,
record and label-comparison decision consumes cumulative admission before cursor
acceptance. Cancellation, corruption or exhausted limits cannot yield a
complete-source token. Physical readers retain their independent resource
admission. Canonical descriptor values use version 2; unsupported development
formats fail closed without deleting or replacing their evidence.

Final generation construction remains complete and bounded by its existing
resident/generation limits. Split preparation runs outside the original write
owner and returns to it for dependency/durability validation and selection.
These bounds do not guarantee RSS, elapsed time or starvation freedom. No
unchanged Search segment sharing or durable private-stage continuation is added.
The full mapper supports zero or one document per native input, not arbitrary
resumable fanout. A committed E survives a lost response through the original
registered operation; it does not create an independent host mapping certificate.
Actual Mem activation, external vectors and platform power-loss qualification
retain their separate gates. The legacy #1108 prefix refusal remains intact.

The [COW evolution design](MVCC_COW_EVOLUTION_SPEC.md) retains COW values,
ordinary snapshot retirement and persistent Consumer registration as separate
lifetimes. [Neumann et al., SIGMOD 2015](https://15721.courses.cs.cmu.edu/spring2016/papers/p677-neumann.pdf)
informs complete dependency validation and safe reclamation. Its undo-based
value representation and main-memory measurements do not mandate a HawDB layout
change or establish WAL/power-loss safety. Optional serializability is not a
prerequisite for this Search path.

## Enumeration refinement argument (#1222)

Let `B` be the captured canonical base, `D` the captured resident/delta map,
`T` the captured node tombstones and `L` the selected label set. The emitted node
IDs are precisely `matching(B, L) \ (keys(D) union T)` union
`matching(D, L) \ T`. Base and delta domains in this expression are disjoint;
overlapping labels are tested as membership and do not multiply IDs. The pinned
read view fixes all four sets through later commits/checkpoints.

For every node in a canonical segment, the writer inserts every label into that
segment's synopsis, including nodes with no properties. Therefore membership in
`L` implies at least one positive Bloom test; a negative segment decision cannot
remove a matching node. Positive false matches add admitted work only. The
resumable descriptor/record cursor accepts a position only after admission;
refusal/cancellation invalidates completion rather than skipping the position.
Version/digest/framing failures retain storage evidence and cannot mint coverage.

This refines the existing complete-source abstraction; it adds no WAL,
checkpoint/head, native selection or owner transition. The publication/custody
models (`HawDBVersionedProjectionPublication`, `HawDBConsumerIncarnationCustody`)
and source-segment publication model retain their state relations. Their
publication/durability obligations remain required. The implementation evidence
is the independent sparse/multilabel/base-delta reference and exact full-rebuild
integration comparison, not an assertion that process reopen proves power loss.

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
gaps, cold node/adjacency/relationship/spill capture under strict foreground FD pressure, nested
resource/cancellation refusal versus physical corruption, durability barriers
and private preparation.
See [the test index](../TEST_INDEX.md) for execution lanes.
Reopen tests are not fault-image or power-loss qualification.
