# Admitted projected point seeks

Issue Number: ref [#992](https://github.com/nowledge-co/hawdb/issues/992)

The boundary benchmark's point case executes N distinct parameterized Cypher
queries against N nodes. Its plan already selects `IndexNodeSeek`, but the
materialized storage callback clones all labeled nodes before checking the
property. Improving host serialization cannot remove that engine cost.

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
the iterator. The existing output/batch accounting remains in force.

No index is added, result allowance raised, durability changed, or persistent
representation replaced. Multi-value, union, undeclared-index and out-of-core
accesses retain their existing read paths. Readers without the optional borrowed
index/admitted-projection capability also retain their original path; GraphStore
supplies the actual indexed iterator.

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
