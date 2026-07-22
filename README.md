# Skein

Skein is an embedded Rust graph database intended for the Nowledge local graph
data plane. It uses Cypher as its query language and a Cascades-style optimizer
for deterministic, explainable planning.

## Usage Direction

Prefer parameterized Cypher for new application code:

```rust
use skein::{Database, Value};
use std::collections::BTreeMap;

let mut db = Database::new();
db.query("CREATE INDEX ON :Memory(id)")?;

let mut parameters = BTreeMap::new();
parameters.insert("id".to_string(), Value::Int(1));
db.query_with_params(
    "CREATE (:Memory {id: $id, title: 'Graph foundations'})",
    &parameters,
)?;

let output = db.query_with_params(
    "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title",
    &parameters,
)?;
```

Typed knowledge APIs remain as compatibility facades for existing Nowledge
integration points. New graph behavior should first be expressed as
parameterized Cypher unless a bounded facade is needed for migration parity,
snapshot ownership, or a stable legacy adapter shape.

Use `explain_query` or `skein explain-json` to inspect plan selection. Simple
AST-shaped statements such as schema DDL and basic `CREATE` statements can use
the `fast_path` search mode; graph reads and predicate-bearing `MATCH` queries
continue through the memo optimizer.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current design.
The staged implementation and compatibility gates are tracked in
[docs/EMBEDDED_DEVELOPMENT_PLAN.md](docs/EMBEDDED_DEVELOPMENT_PLAN.md).
