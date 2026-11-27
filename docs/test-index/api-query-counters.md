# Cypher write statement counters (query counters)

Owner: api::tests::query_counters; hawdb_core::query_counters::tests
Lane: `//:hawdb_unit_general_tests` (existing required native general lane); `//crates/core:hawdb_core_tests`
Replay: `cargo test --locked -p hawdb --lib query_counters`; `cargo test --locked -p hawdb-core --lib query_counters`

`QueryOutput::counters` (`QueryCounters`, in the spirit of Neo4j
`SummaryCounters`) counts the write operations a Cypher statement executed,
not how much stored data changed. `api::tests::query_counters` runs every case
on an in-memory `Database::new()` and on a durable out-of-core database whose
seed was checkpointed into the canonical base, with identical expectations:
same-value `SET` counts 1; one property set twice counts 2 (constant and
row-evaluated `SET`, and an `UNWIND … MATCH … SET` whose rows hit the same
node); `SET n.p = null` counts; `CREATE` counts nodes, labels, relationships
and each given property but not declared `DEFAULT`s; `DETACH DELETE` counts
the relationships it deletes; a matching `MERGE` counts only `ON MATCH SET`,
a creating one the pattern and `ON CREATE SET` properties, also for a batch
row that matches the node an earlier row created; re-creating an index or
constraint counts 0; `SET` on a missing node and reads count nothing; each
explicit-transaction statement (also on `ConcurrentDatabase`) reports only its
own counters; failed statements return errors. `SET n += {}`, `SET n:L` and
`REMOVE` are not parsed yet: the test pins their parse errors and checks that
the mutation layer's empty assignment list matches a node with
`properties_set == 0`. No new lane, profile or timeout.
