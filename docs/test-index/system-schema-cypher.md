# Registry migrations with Cypher graph schema DDL (#2014)

Owner: api::tests::system_schema_cypher; hawdb-relational system_schema::tests::sql_only_migration_checksum_is_unchanged_and_cypher_is_bound
Lane: `//:hawdb_unit_general_tests` (existing required native general lane)
Replay: `cargo test --locked -p hawdb --lib -- api::tests::system_schema_cypher::`; `cargo test --locked -p hawdb-relational --lib system_schema`

Registry migrations with Cypher graph schema DDL (#2014) are owned by
`api::tests::system_schema_cypher` in `//:hawdb_unit_general_tests` (the
existing required native general lane) and by
`system_schema::tests::sql_only_migration_checksum_is_unchanged_and_cypher_is_bound`
in the `hawdb-relational` unit tests. They cover a `CREATE INDEX ON :Memory(id)`
migration that backfills existing nodes (resident and out of core), is recorded once, is not re-applied on
reopen and is used by lookups and SET; two owners whose Cypher unique
constraint fails publish neither owner, table nor index; data-writing, read and
`CHECKPOINT` Cypher is refused before any write; a changed applied Cypher
statement is checksum drift; an existing identical index is reused and an
existing range index is kept next to the new equality index; on an out-of-core
database the first SET after the migration seeks the index without reopening
and MERGE does not scan the label; and an SQL-only checksum equals the value
computed on main before the change. Replay with `cargo test --locked -p hawdb --lib --
api::tests::system_schema_cypher::` and `cargo test --locked -p hawdb-relational
--lib system_schema`. No lane, profile or persistent format changes.
