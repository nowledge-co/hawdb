# Costed relational join implementations

CSG-CMP chooses a physical implementation while comparing join alternatives.
The embedded planner no longer diverts an eligible single join away from cost
enumeration merely because the syntax-order plan supports hash or index merge.

## Binding and execution contract

The SQL binder prepares typed implementation candidates for each two-binding
operator. Each candidate retains its operator ID, complete predicate IDs,
oriented bindings, standalone accesses, equality keys and selectivity. Hash and
merge candidates compete with probe and bounded materialized plans inside
`best_csg_cmp_plan`; selected algorithms are not replaced after enumeration.

- Hash candidates require a standalone full-scan build input and at least one
  equality key. All residual predicates are still evaluated by the existing
  hash kernel. INNER may swap inputs; LEFT preserves its original right binding.
- Merge candidates require compatible forward index accesses and the existing
  materialized or shadow index-read mode. Transaction-workspace index reads do
  not advertise that capability. Keys from another operator cannot be reused.
- Current specialized kernels accept two relation inputs. They can form a
  subtree of a reordered multi-table query, but are not advertised for an
  arbitrary composite input. Other joins retain probe or bounded materialization.
- Hash output does not claim input ordering: grace partitioning may change it.
  Requested ordering must be satisfied by another valid plan or a later sort.
- The selected typed contract is matched back to its prepared SQL implementation
  before constructing a physical node. Unknown contracts fail closed.

The existing embedded facade, storage format, persistence versions and execution
budgets are unchanged. Internal optimizer types gain a capability-aware entry
point; existing entry points still enumerate without specialized candidates.

## Cost and planning budget

One canonical estimator is used by enumeration and physical operator profiles.
Probe costs still multiply right-access work by outer cardinality. Generic
materialization still charges Cartesian row-pair work. Hash charges one build,
one probe per left row and estimated matching-pair work; merge charges a pass
over each ordered input and matching-pair work. Hash includes an additional
build insertion per right row. These are planning work units, not calibrated
wall-clock or I/O predictions.

Equality output estimates use existing fresh NDV/complete unique-key metadata
and the documented fallback. Planning does not scan rows to invent statistics.
LEFT cardinality retains its outer floor. Arithmetic saturates as before.

Distinct prepared implementations share `max_expressions` with the logical memo.
Preparation checks the limit before inserting another candidate; memo construction
reserves those slots and reports total expression usage on success or exhaustion.
Budget exhaustion follows the existing reported fallback chain. No default
budget was raised to make the larger search space pass.

## Verification

The public SQL regressions require both CSG-CMP reordering and an actual HashJoin
or MergeJoin in a three-table profile, compare complete ordered results/schema
with syntax order. The unindexed hash fixture also checks fewer visited rows;
the merge fixture proves costed reachability, not a measured speedup over indexed
probes. A separate forced-spill case
checks grace-hash evidence, exact result multiplicity and cleanup without making
an unrelated external sort consume the same tiny test budget.

A 96-case independent nested-loop oracle covers NULL keys, duplicate keys,
residual predicates, INNER/LEFT and indexed/unindexed inputs. Other tests cover
candidate legality, ordering, exact expression limits, fallback, component costs,
cancellation and subsequent reader reuse. Existing syntax-only kernel tests
remain, including cancellation after admission and hot-partition spill.

Required local verification includes optimizer/root tests and:

```sh
bazel test --nocache_test_results //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```

The fuzz suite contains the SQL join-rewrite differential campaign. Fuzz remains
local-only and is not added to routine or dedicated CI jobs. No benchmark-based
production latency claim is made by this change.
