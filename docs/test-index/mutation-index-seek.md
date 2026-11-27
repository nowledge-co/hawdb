# Write-path target lookup through property indexes (#2016)

Owner: api::tests::mutation_index_seek
Lane: `//:hawdb_unit_general_tests` (existing required native general lane)
Replay: `cargo test --locked -p hawdb --lib -- api::tests::mutation_index_seek::`

Write-path target lookup through property indexes (#2016) is owned by
`api::tests::mutation_index_seek` in `//:hawdb_unit_general_tests` (the
existing required native general lane). It runs node and relationship writes
(same-run create, re-key and delete, extra conjuncts, `IN`, duplicate and
integer keys, relationship create/MERGE, re-target and re-source, node MERGE,
`DETACH DELETE`) against an indexed and an unindexed database in both residency
modes, with and without checkpoint, autocommit and transactional, and requires
byte-identical graphs before and after reopen. It also checks that an index
created without reopening, or staged in the same transaction, still finds
committed nodes, and that the affected-row cap stops a high-fanout indexed key
at the first excess match (at most limit + 1 examined index hits, same error as
the scan). Replay with `cargo test --locked -p hawdb --lib --
api::tests::mutation_index_seek::`. No lane, profile or persistent format
changes.
