# Shortest-path discovery and materialization

Issue: [#222](https://github.com/nowledge-co/hawdb/issues/222).

`ShortestPathExec` discovers paths one BFS level at a time. It does not keep
complete path vectors in its frontier. The implementation is private to the
existing executor crate; query syntax, public APIs, and storage formats stay
unchanged.

This operator serves only the original endpoint-id shape
`MATCH p = (a)-[:T* ALL SHORTEST lo..hi]-(b) WHERE a.id = ... AND b.id = ... RETURN ...`.
Every other shortest-path query uses the correlated MATCH step described in
[General shortestPath and allShortestPaths](#general-shortestpath-and-allshortestpaths).

## Search semantics

For ordinary shortest paths (`min_hops <= 1`), the index keeps one discovery
state per node. An edge is retained only when it reaches the next BFS depth.
This shares equal-length prefixes/suffixes and gives expected O(V + E) discovery
work and state, excluding storage adjacency ordering and materialized output.
Each level finishes before path materialization begins.

For a larger minimum hop count, an earlier arrival cannot suppress a longer
qualifying path. The index therefore uses `(node, depth)` states. Enumeration
rejects repeated nodes, preserving the existing **node-simple path** semantics.
A target reached only through cyclic walks does not terminate the search; the
next level may contain a valid simple path. No positive simple path returns to
its source or exceeds the graph's node count minus one edges.

The lower-bounded case can retain O(D * (V + E)) layered state and may enumerate
many invalid walks when checking simplicity. It is bounded by memory, maximum
depth, and cancellation, but is not claimed to have linear total complexity.
This is necessary to preserve the existing minimum-hop contract rather than
silently returning only unconstrained shortest paths.

## Ordering and multiplicity

The path DAG stores forward edges in the existing ordered-adjacency sequence.
Parallel relationships remain separate edges, including duplicate node paths.
Undirected traversal retains outgoing-before-incoming order.

Reverse reachability marks states that lead to the candidate target depth.
An iterative forward traversal then emits paths in the same prefix order as
full-path BFS at that depth. Enumerating backward through parent lists would
change that order and potentially change the LIMIT prefix. No recursive call
stack or hash-map iteration determines result ordering.

## Resource and cancellation boundaries

- Discovery, index growth, enumeration scratch, and materialized paths share
  the existing blocking-operator and query-root memory account.
- Vector accounting uses capacity, including simultaneous old/new allocations
  during growth. The hash index has a conservative allowance for bucket slack,
  control bytes, alignment, and rehash overlap; this is tracked memory, not RSS.
- The result limit bounds path materialization, not the size of the complete
  target-level DAG. An unlimited result can still exceed the output budget.
  Budget failures return an error, never a partial shortest-path result.
- Path payloads and the outer result allocation remain charged until their
  owning allocations are dropped during binding conversion. Binding capacity
  is admitted before allocation; overlapping search/output state remains under
  the shared account.
- Checkpoints run at BFS level boundaries, for each expanded state/neighbor,
  while marking reachability, and during iterative materialization. Cancellation,
  storage errors, and unwinding release query leases with their owning search.
- The existing diagnostic input count now measures expanded discovery states,
  not the number of full path prefixes previously popped from the queue.

## Verification

The root tests include a 9-layer, width-4 graph with 262,144 shortest paths.
Its seven-path query must fit the same 64 KiB budget that rejects the former
full-path frontier. A seeded independent edge-list/full-path-BFS oracle compares
5,184 ordered results across directions, multiplicity, visibility filters,
minimum/maximum hops, and result limits. Explicit tests cover shortcuts,
cyclic-only candidate depths, empty limits, and memory failures.

Executor unit tests inject cancellation after every BFS level and storage
failure/panic after allocation. Materialization cancellation is injected after
one output path exists. Further checks cover output-budget rejection and
transient allocation overlap. Existing query ACL and accounted-stream tests
remain integration gates.

```sh
cargo test -p hawdb-executor
cargo test -p hawdb --lib
cargo test -p hawdb --lib --features acl shortest_path
cargo clippy -p hawdb-executor --all-targets --all-features -- -D warnings
bazel test //crates/executor:hawdb_executor_tests //:hawdb_unit_tests
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

Fuzz remains local verification, not a default or dedicated CI job.

## General shortestPath and allShortestPaths

### Supported Cypher

```cypher
MATCH (p1:Person {id: $a}), (p2:Person {id: $b})
OPTIONAL MATCH path = shortestPath((p1)-[:KNOWS*]-(p2))
RETURN CASE path IS NULL WHEN true THEN -1 ELSE length(path) END AS len
```

- Pattern forms: `shortestPath(...)`, `allShortestPaths(...)`, and the
  selector forms `[:T* ANY SHORTEST lo..hi]` / `[:T* ALL SHORTEST lo..hi]`.
  The path variable is optional.
- One relationship step, typed or untyped, outgoing, incoming or undirected.
  The minimum is 0 or 1. The maximum may be omitted (`*`, `*1..`, `*0..`).
  An omitted maximum is capped at `MAX_SHORTEST_PATH_HOPS` (256, the
  bounded-expansion recursion limit). An explicit maximum above 256 is a
  semantic error. If an unbounded search reaches 256 hops while both
  frontiers can still grow, the query fails instead of reporting no path.
- Relationship variables and relationship property maps are rejected; use
  `relationships(path)`.
- Both endpoints must be bound before the search. An endpoint qualifies if an
  earlier MATCH or WITH bound it, if the same clause binds it through another
  pattern, or if it has an inline property map or appears in the clause's
  WHERE predicate. Anonymous or otherwise unconstrained endpoints are a
  semantic error. The check is syntactic: a weak predicate such as
  `a.age > 0` still admits every matching node, and the search then runs once
  per endpoint pair, each bounded separately by `blocking_operator_bytes`. The
  total cost is roughly (endpoint pairs) x (reachable subgraph per search), so
  constrain endpoints as tightly as possible.
- WHERE conjuncts of the same clause that read a shortest-path variable are a
  semantic error. Neo4j applies them during the search (it returns the
  shortest path that satisfies them); evaluating them after selection would
  silently differ. Filter explicitly after the search with
  `WITH p WHERE ...`.
- The clause may be `MATCH` or `OPTIONAL MATCH` anywhere in a pipeline, with
  WITH, ORDER BY, SKIP and LIMIT before or after it. In OPTIONAL MATCH,
  missing paths keep the row and bind the path and any new endpoints to NULL.
- Path functions: `length(p)`, `nodes(p)`, `relationships(p)` and
  `properties(nodes(p), 'key')`, in RETURN, WITH, CASE, WHERE and ORDER BY.
  `p IS NULL` and `p IS NOT NULL` work in RETURN, WITH, CASE and WHERE. A NULL
  path yields NULL from every function.
- Returning `p` itself yields a map `{length, nodes, relationships}`. Its
  nodes and relationships use the same maps as `RETURN n` and `RETURN r`.
  Paths can be passed through WITH.

### Semantics

- `shortestPath` returns at most one path per endpoint pair.
  `allShortestPaths` returns every shortest path. Paths through parallel
  relationships are distinct.
- Equal endpoints yield a zero-length path for minimum 0 and no path for
  minimum 1.
- Relationships are unique within a path. Relationships bound by other
  patterns of the same MATCH clause are not traversed, and the next pattern
  in that clause cannot reuse the path's relationships.
- Tie-break: the search is deterministic for a given snapshot. `shortestPath`
  returns the first meeting node found in ordered-adjacency
  (relationship-id) order, joined with the first-discovered parent chains on
  both sides. `allShortestPaths` emits paths grouped by meeting node in
  discovery order, then by source-side and target-side parent order. Use ORDER
  BY when a particular order is required.

### Planning

The binder lowers each shortest-path pattern into `Node` steps for its
endpoints, followed by a `GraphMatchStep::ShortestPath` step. The step binds
the path variable as a value column. A required MATCH whose other patterns
are single nodes is split. A first GraphMatch binds the endpoints under the
WHERE conjuncts that do not read a path variable, so normalization can use
node scans, filters and indexes. A second GraphMatch then runs the search.
OPTIONAL MATCH, and clauses with relationship patterns, stay in one program
to preserve whole-clause NULL semantics and relationship uniqueness.
Path functions lower to `ProjectionExpression::Path`. Fingerprints, EXPLAIN,
plan-cache parameter marker analysis and physical rebinding, and the
determinism classifier cover both nodes. Like other GraphMatch pipelines,
these plans are cached per exact parameter value.

### Execution

`traversal::shortest_path_bfs` runs a bidirectional, level-synchronous BFS.
Each side has a visited index and parent links, not complete path prefixes.
The smaller frontier expands first; ties expand from the source. The first
level that reaches the other side fixes the shortest length. All meeting
nodes of that level sit at the same position on every shortest path, so each
path is enumerated exactly once. `shortestPath` keeps only the first parent
link and stops at the first meeting. `allShortestPaths` keeps every link to
the previous level. It enumerates paths lazily with an iterative stack, so a
downstream LIMIT or stop stops enumeration.

The visited indexes, parent links, meeting list and enumeration scratch are
charged before allocation. They go to an `OperatorMemoryTracker` on the
GraphMatch blocking-state account (`blocking_operator_bytes`), under the query
root. Exceeding the budget fails the query. Adjacency reads use the existing
admitted ordered-adjacency path. Materialized path nodes and relationships are
admitted through the GraphMatch source account. Runtime checkpoints run per
level, per expanded node, per adjacent relationship and per enumerated chain
step.

### Deviations from Neo4j

- Paths are returned as maps, not as a distinct path type.
- Unbounded patterns are capped at 256 hops, and reaching the cap is an error.
- Minimums above 1 are only accepted by the original `ALL SHORTEST` endpoint-id
  shape. Predicates over the path inside the search (`all(r IN
  relationships(p) WHERE ...)`) are not supported; WHERE conjuncts that read
  the path in the same clause are rejected rather than applied after the
  search (use `WITH p WHERE ...`).
- Because paths are maps `{length, nodes, relationships}`, path functions
  accept any map-valued column with those keys (for example a path carried
  through `WITH` or `collect`/`UNWIND`); other values fail at run time.
- Endpoints must be constrained as described above. Neo4j would also run the
  search for every pair of label-scanned endpoints.

### Verification

```sh
cargo test --locked -p hawdb-executor --lib shortest_path_bfs
cargo test --locked -p hawdb-cypher shortest
cargo test --locked -p hawdb-plan-cypher shortest
cargo test --locked -p hawdb --lib api::tests::shortest_paths
```
