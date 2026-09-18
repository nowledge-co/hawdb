# MVP Task Plan

## Objective

Build a runnable embedded graph database slice that proves the first HawDB
pipeline:

```text
schema -> parser -> planner -> Cascades optimizer -> executor -> demo
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
- Cascades-style memo skeleton
- physical plan implementation rules for scan, expand, filter, project, and
  create
- executor for the physical operators above
- transaction-private COW Cypher workspace with read-your-own-writes, grouped
  commit, and rollback
- rebuildable in-memory property equality index
- `IndexNodeSeek` implementation for simple label plus equality predicates
- deterministic explain output
- CLI demo through `cargo run`
- unit tests covering parser, query execution, optimizer trace, WAL replay,
  checkpoint recovery, torn WAL tails, relationship adjacency, atomic batch WAL
  replay, transaction commit/rollback, and property index rebuild

Out of scope for this task:

- row-version chains and fine-grained write-write conflict detection
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

## Next Tasks

1. Extend the existing PostgreSQL primary-key point/range lock inference to
   graph indexed predicates only after record-property write footprints and
   per-key version validation can be proven together.
2. Add persistent index descriptors and cost-based index selection.
3. Add sparse/dense adjacency storage with copy-on-write segments.
4. Add CSR/CSC projection generation as rebuildable checkpoint artifacts.
