# Admitted projected point seeks

Issue Number: ref [#992](https://github.com/nowledge-co/hawdb/issues/992)

The boundary benchmark's point case executes N distinct parameterized Cypher
queries against N nodes. Before the point fix, its plan selected `IndexNodeSeek`,
but the materialized storage callback cloned all labeled nodes before checking
the property. Improving host serialization cannot remove that engine cost.

## Selected path

`NodeProjectionScanExec` uses the existing borrowed scalar-index iterator only
when all three conditions hold:

1. The pattern resolves to one exact label in the current catalog snapshot.
2. The access contains exactly one property value and the same catalog declares
   a scalar property index for that label/property.
3. The store is materialized. An out-of-core store's heap index is incomplete.

For each indexed candidate, execution checks cancellation, admits the selected
node's estimated temporary allocation, and then reads only required properties.
The temporary reservation remains alive through residual evaluation and output
batch insertion. An error, cancellation or stop drops the reservation and ends
the iterator. After integrating main's admitted graph reads, the reservation also
covers the temporary binding and variable/key overhead using the same admission
function as the other projected scan paths. Output/batch accounting remains in
force independently; both reservations overlap while projection owns its result.

No index is added, result allowance raised, durability changed, or persistent
representation replaced. Multi-value, union, undeclared-index and out-of-core
accesses retain main's admitted read paths, including before-copy source permits
and admitted union deduplication. Readers without the optional borrowed index
capability use those paths and must refuse if they cannot support admission;
GraphStore supplies the actual indexed iterator. No unadmitted legacy fallback
is introduced by the merge.

## Correctness argument

These are source-level proof obligations, with differential regressions. They
are not a machine-checked proof of the entire storage engine.

Let G be the immutable store/catalog snapshot, L the exact label, p the property,
v the bound value, and S the required property set. Define

```text
C(G,L,p,v) = { n.id | n is live in G, L is in n.labels, n[p] == v }
P_S(n) = (n.id, n.labels, n.properties restricted to S)
```

The existing scalar-index invariant is `I_G[L,p,v] = C(G,L,p,v)` for declared
indexes. Creation backfills existing nodes; creation, property mutation and
deletion maintain membership. Recovery and read-snapshot publication retain a
matching catalog/index generation. The eligibility guard is essential: an
undeclared index and an empty declared index are different states.

The previous singleton property callback enumerates live nodes in NodeId order,
retains exactly C, and projects their required properties. The new iterator
enumerates I in the same NodeId order and obtains P_S for each member. Index
completeness therefore gives equal candidates, projections and order. The
unchanged residual, projection and batch code gives equal successful results,
including duplicate keys, missing keys, explicit nulls and label isolation.
Scalar index equality and residual Cypher equality remain separate; the
residual must still run when present.

Before constructing P_S, `projected_node_owned_admitted` computes its existing
conservative allocation estimate from that same immutable record and calls
admission. Rejection propagates before the clone. The lease's lexical lifetime
covers the selected temporary and its projection into independently accounted
output. This newly rejects oversized selected source rows even when a later
residual would discard them; allocating first would violate the admission
contract. An empty indexed match creates no projected source row.

The index contains a set of ids, so duplicates cannot repeat one node. Iterator
termination does not fetch or clone another projected record. Store/catalog
borrows exclude mutations during this operation. The existing snapshot API
retains the old index membership across later live updates/deletes.

## Work and layout

For N labeled nodes, K matching ids and W properties per node, the previous
memory path visits and clones N complete nodes per query. The indexed path
performs one index lookup, K node lookups and K selected-property reads. It
does not construct a vector containing all index candidates.

A point result is naturally a selected row. Columnar delivery is independently
useful for batches and numeric/vector kernels. This optimization continues to
return the ordinary owned row contract: it is not storage-to-host zero-copy.
The retained numeric producer, Arrow buffer identity and the broader source
and foreign-memory admission requirements remain separate acceptance gates.

## Verification

- Differential execution compares singleton access with a label-scan oracle
  before index creation, after backfill, after updates and after deletion.
  It covers duplicate keys, numeric types, UTF-8/NUL keys, nulls, misses,
  residuals, label isolation and large unrequested properties.
- A low source budget refuses a selected large field before residual evaluation;
  a miss still returns an empty result. No budget is weakened.
- Public parameterized queries cover cache rebinding, duplicate ordering, LIMIT
  and read snapshots pinned across later updates/deletes.
- The ordinary benchmark retains all N point calls, identical fixtures,
  checksums, result order, warmups and default durability/resource settings.
  Native allocation profiling uses separate instrumented binaries and must not
  be mixed with ordinary latency measurements.

The complete user read-performance gate also requires the representative host
matrix and retained/Arrow qualification. Write cases remain regression controls;
they do not require Arrow to improve commit performance. A faster point result
alone cannot mark PR #986 or issues #976/#987 complete. Measurements and retained
limits are recorded in [POINT_RESULTS.md](../bindings/benchmarks/POINT_RESULTS.md).

## Canonical record reuse

Canonical projected point admission previously located and framed the same
record twice: once to estimate the selected allocation and once to decode it.
The private `with_node_input` callback now keeps one immutable segment read
alive across both phases. `CanonicalNodeInput::decode_selected` measures the
selected properties, calls admission, then constructs the owned projection
from that same payload. Both the direct point read and `ProjectionAdmission`
use this internal contract; no public API or persistent format changes.

For an immutable canonical generation G and record R, the old path computes
`A_S(R)` and, on admission, later decodes `P_S(R)`. The new path computes the
same `A_S(R)` and `P_S(R)` while the record borrow remains alive. Successful
values and allocation charges are therefore equal under the existing selected
codec and immutable-generation invariants. A missing record invokes no callback.
An admission refusal constructs no projection, retains its original typed
HawDB error, and does not poison the reader. Physical corruption still passes
through the existing fail-closed error handling. Live overlays and tombstones
retain their earlier precedence. This is a source-level equivalence argument,
not a machine-checked durability proof.

The source cache lease now overlaps admission and selected decoding. It is
released when the callback exits, including refusal; it must not be confused
with the caller's independently accounted owned-result reservation. Reusing a
record does not remove selected-value allocations, property preflight/decoding
walks, or necessarily repeated property-spill reads. It does not establish
zero-copy delivery or a whole-process memory bound.

The frozen [canonical point controls](../bindings/benchmarks/evidence/README.md#canonical-projected-point-controls-20261012)
compare the `9bbffbcf` parent with the recorded uncommitted production patch.
They checkpoint and reopen 256 nodes with zero live delta, project one or 16
integer properties at three record positions, and alternate producer order
across nine paired rounds. The longer matrix contains 108 raw process results
at 20,000 calls per result. Paired median speedups range from 1.21 to 1.69;
all 54 longer pairs improve. The original 1,000-call matrix, including its
negative pairs, is preserved rather than discarded. Selected admission bytes
match; measured cache accesses fall from four to two per call, with no cache
misses or residual pins in these warm controls. Cache accesses are not physical
IO counts. These measurements exclude query planning/index seek, cold latency,
consumer/drop work, write throughput, foreign delivery and Arrow qualification.
They do not satisfy the comprehensive PR performance gate.

Regression coverage rejects canonical point admission and retries successfully
with the same charge and selected values, preserves a typed execution refusal,
checks missing records never call admission, and excludes an unrequested
128-KiB property from the selected allocation.
