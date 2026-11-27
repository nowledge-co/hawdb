# Maintained logical record bytes (#2032)

Owner: api::tests::logical_record_bytes; store::tests::checkpoint_temporary_space_admission_matches_full_walk_estimate
Lane: `//:hawdb_unit_general_tests` (existing required native general lane); `//crates/storage:hawdb_storage_tests`
Replay: `cargo test --locked -p hawdb --lib logical_record_bytes`; `cargo test --locked -p hawdb-storage --lib checkpoint_temporary_space_admission_matches_full_walk_estimate`

Maintained logical record bytes ([#2032](https://github.com/nowledge-co/hawdb/issues/2032))
are owned by `api::tests::logical_record_bytes` in the existing
`//:hawdb_unit_general_tests` owner and by
`store::tests::checkpoint_temporary_space_admission_matches_full_walk_estimate`
in `//crates/storage:hawdb_storage_tests`. A 50,000-node resident store runs 256
indexed autocommit `SET`s with zero full record walks (the reference walk counts
each call); the maintained estimate equals the full walk after mixed node and
relationship creates, updates, property removals and deletes with large
strings, after a rolled-back transaction, and after drop-without-checkpoint
reopen, in materialized and out-of-core modes. The storage case pins identical
free-space-reserve decisions at the walked threshold and an end-to-end WAL
refusal/admission. Replay with `cargo test --locked -p hawdb --lib
logical_record_bytes` and `cargo test --locked -p hawdb-storage --lib
checkpoint_temporary_space_admission_matches_full_walk_estimate`. No new lane,
profile or timeout.
