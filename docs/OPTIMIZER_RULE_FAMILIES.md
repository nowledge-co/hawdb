# Optimizer family ownership

[Issue #502](https://github.com/nowledge-co/hawdb/issues/502) separates domain
planning helpers from the generic Cascades framework. The embedded `hawdb`
library remains the host integration API. `hawdb-optimizer` retains its prior
public modules, exported constructors and concrete type identities through
re-exports; existing hosts need no import migration.

| Owner | Contract | Direct first-party dependencies |
| --- | --- | --- |
| `hawdb-cascades` | Memo, rules, stages, cost/properties, diagnostics and optimizer context/config | None |
| `hawdb-plan-core` | Vector logical/physical IR and resource profile | None |
| `hawdb-optimizer-graph` | Graph catalog, cardinality, rewrites, lowering and typed graph traces | Cascades, core, Cypher, expression, plan-cypher, optimizer-vector |
| `hawdb-optimizer-relational` | Access costing, join enumeration, planning diagnostics and sargability | Cascades, expression; core for tests |
| `hawdb-optimizer-predicate` | Search predicate normalization, parsing and pushdown | Cascades |
| `hawdb-optimizer-vector` | Adaptive vector backend selection and bounded plan construction | Cascades, plan-core |
| `hawdb-optimizer` | Compatibility facade | Cascades and the four families |

Predicate parsing retains its existing `serde_json` dependency. The other new
owners introduce no third-party dependency. Context/config has one definition in
Cascades; the graph trace retains typed `PhysicalOperatorId`/`PhysicalPlanKind`
and re-exports that same config. Query-family names, defaults and interpretation
are preserved. `hawdb-plan-cypher` re-exports the shared vector types so executor,
planner and existing host signatures retain their original identity.

The dependency boundary serves existing consumers:

- Search uses predicate/vector helpers and Cascades context directly. Its
  dependency closure excludes the compatibility facade, graph optimizer and
  relational optimizer. Its executor still legitimately uses Cypher plan IR.
- Vector helpers depend only on Cascades and the parser-free shared vector IR.
- The relational toolkit has no Cypher/graph optimizer or graph-plan dependency.
- Plan-cache's test-only optimizer dependency points to the graph owner.
- Root SQL planning imports the relational owner; the root separately keeps the
  complete optimizer compatibility facade for existing public paths.

These are dependency-direction claims, not measured compilation or binary-size
improvements. No planning algorithm, score, cardinality heuristic, selected
plan, trace event, cache policy, budget, error, schema, durability or release
activation behavior changes as part of this extraction.

## Coverage and build contracts

Unit tests move with their implementations. Graph planner golden tests and their
unchanged fixture contents move into `optimizer-graph/tests/testdata/`, with
compile-time inputs declared on the consuming Bazel test. Generic context tests
move into Cascades and the vector IR test moves into plan-core. Database/host
integration tests remain at the embedded facade.
The existing graph cardinality differential campaign also checks relational
join fallback estimates. Its test-only dependency points to the relational
owner; graph's production library has no relational dependency.

Every owner has Cargo and Bazel library/test targets and belongs to the canonical
crate presubmit suite. Original `//crates/optimizer` golden and local fuzz labels
remain aliases to the owning tests. Their full case selection, ignored-campaign
admission and local-only policy are preserved. `hawdb-optimizer`'s presubmit
suite includes all four owner suites, so the historical aggregate still covers
the moved tests.

Validate owner tests, unchanged golden plans, facade/root consumers, the search
feature matrix, portable/minimal consumers and the complete documented local
fuzz surface. Strict pinned workspace check, formatting and Clippy remain
required. Compare resolved third-party package versions before/after; only
first-party dependency edges and workspace packages should move in Cargo.lock.

## Separate algorithm work

The existing graph stage sequence and relational join algorithms are retained.
Issue #502's optional Phase 3 proposes driving the graph stages through a real
`OptimizationPipeline`; it remains separate work with deterministic stage/event
compatibility as its acceptance boundary. Converging the relational toolkit and
SQL runtime into a memo-driven optimizer is the explicitly separate Phase 4
architectural decision. Neither is inferred from this crate split.
