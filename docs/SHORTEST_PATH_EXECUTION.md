# Shortest-path discovery and materialization

Issue: [#222](https://github.com/nowledge-co/skein/issues/222).

`ShortestPathExec` discovers paths one BFS level at a time. It does not keep
complete path vectors in its frontier. The implementation is private to the
existing executor crate; query syntax, public APIs, and storage formats stay
unchanged.

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
cargo test -p skein-executor
cargo test -p skein --lib
cargo test -p skein --lib --features acl shortest_path
cargo clippy -p skein-executor --all-targets --all-features -- -D warnings
bazel test //crates/executor:skein_executor_tests //:skein_unit_tests
bazel test //crates/fuzz:skein_fuzz_tests //crates/fuzz:skein_fuzz_cli_tests //:skein_linux_ci_fuzz_smoke_test
```

Fuzz remains local verification, not a default or dedicated CI job.
