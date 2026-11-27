# Read-only opens make zero filesystem mutations (#2029)

Owner: api::tests::read_only_zero_writes
Lane: `//:hawdb_unit_general_tests` (existing required native general lane)
Replay: `cargo test --locked -p hawdb --lib read_only_zero_writes`

Read-only opens make zero filesystem mutations under the database root
([#2029](https://github.com/nowledge-co/hawdb/issues/2029)), owned by
`api::tests::read_only_zero_writes` in the existing `//:hawdb_unit_general_tests`
owner. A recursive snapshot (paths, sizes, mtimes) must be identical after a
`read_only` open, Cypher reads, `information_schema.tables` and `COUNT(*)`, a
refused write and a second open, for a checkpointed branch project and one with
an unreplayed WAL tail, in materialized and out-of-core modes, and through
`EmbeddedGraph` in `GraphMode::ShadowReadOnly`. The WAL tail replays in memory
for graph and relational reads (relational rows advance the checkpoint view as
in-memory live batches); `RecoveryMode::AutoRepairTornTail` refuses with
`HawDBError::ReadOnly`. On Unix a `chmod -R a-w` copy, with and without a WAL
tail, serves the same reads (skipped for euid 0). Full-text, vector and hybrid
`search_candidates` plus `text_search`/`vector_search` Cypher through an
`EmbeddedStore` with a persistent search projection stay snapshot-identical
(also on a write-protected copy). A `test-support` switch
(`DiscardReadOnlyRowReplay`) drops the in-memory row view: relational scans,
counts, point and secondary-index reads then refuse with `ReadOnly`, never
checkpoint-only rows, while graph reads still see the tail. A branch no writer
admitted (no artifact directory, sealed WAL intervals in its root) and a branch
whose artifact directory was removed are served from the immutable objects:
checkpoint artifacts are bound in memory without installing aliases and sealed
WAL intervals replay in place, creating nothing. Replay with
`cargo test --locked -p hawdb --lib read_only_zero_writes`. No new lane, profile
or timeout.
