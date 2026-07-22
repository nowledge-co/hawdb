# MVP Task Plan

## Objective

Build a runnable embedded graph database slice that proves the first Skein
pipeline:

```text
schema -> parser -> AST classifier -> planner -> optimizer/direct lowering -> executor -> demo
```

The MVP should execute a small Cypher subset end to end:

```cypher
CREATE (:Memory {id: 1, title: 'Graph foundations'})
CREATE (:Memory {id: 1})-[:MENTIONS]->(:Entity {id: 10})
MATCH (m:Memory) WHERE m.id = 1 RETURN m.title AS title
MATCH (m:Memory)-[:MENTIONS]->(e:Entity) RETURN m.id AS memory, e.id AS entity
```

## Scope

In scope:

- schema catalog for node labels and relationship types
- durable property graph store for nodes and relationships
- WAL replay and checkpoint recovery
- outgoing/incoming adjacency index rebuild
- parser for `CREATE` node statements
- parser for `CREATE` one-hop relationship pattern statements
- parser for `MATCH` single-node patterns with optional property equality
  predicates
- parser for `MATCH` one-hop outgoing relationship patterns
- parser for `RETURN` property projections with optional aliases
- logical plan builder
- AST-shaped fast-path classifier for simple statement families
- Cascades-style memo skeleton
- physical plan implementation rules for scan, expand, filter, project, and
  create
- executor for the physical operators above
- mutation-only transaction facade with commit and rollback
- rebuildable in-memory property equality index
- `IndexNodeSeek` implementation for simple label plus equality predicates
- deterministic explain output
- CLI demo through `cargo run`
- unit tests covering parser, query execution, optimizer trace, WAL replay,
  checkpoint recovery, torn WAL tails, relationship adjacency, atomic batch WAL
  replay, transaction commit/rollback, and property index rebuild

Out of scope for this task:

- snapshot read transactions and MVCC isolation
- persistent property index descriptors and cost-based index selection
- full openCypher compatibility
- Nowledge wrapper compatibility layer

## Deliverables

- `Cargo.toml`
- `src/schema.rs`
- `src/store.rs`
- `src/cypher.rs`
- `src/planner.rs`
- `src/optimizer.rs`
- `src/executor.rs`
- `src/api.rs`
- `src/main.rs`
- `docs/STORAGE.md`
- parser, execution, and optimizer trace tests

## Validation

Run:

```bash
cargo fmt --check
cargo test
cargo run
```

Expected demo output includes a physical plan containing:

```text
ProjectExec
IndexNodeSeek
```

and a result row containing:

```text
Graph foundations
```

Use `explain_query` or `skein explain-json` for route visibility. Simple schema
and `CREATE` statements can report `search_mode = fast_path`; predicate-bearing
`MATCH` queries should continue to report `search_mode = memo` and expose
implementation-rule events such as indexed node seeks.

## Next Tasks

1. Add snapshot read transactions and MVCC isolation.
2. Add persistent index descriptors and cost-based index selection.
3. Add sparse/dense adjacency storage with copy-on-write segments.
4. Add CSR/CSC projection generation as rebuildable checkpoint artifacts.
