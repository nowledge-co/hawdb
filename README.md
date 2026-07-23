# Skein

Skein is an embedded Rust graph database intended for the Nowledge local graph
data plane. It uses Cypher as its query language and a Cascades-style optimizer
for deterministic, explainable planning.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current design.
The staged implementation and compatibility gates are tracked in
[docs/EMBEDDED_DEVELOPMENT_PLAN.md](docs/EMBEDDED_DEVELOPMENT_PLAN.md).
The active Nowledge Mem replacement backlog is tracked in [TODO.md](TODO.md).
