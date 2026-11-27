# Out-of-core index projection coverage (#2019)

Owner: api::tests::ooc_index_projection; api::tests::mutation_index_seek
Lane: `//:hawdb_unit_general_tests` (existing required native general lane)
Replay: `cargo test --locked -p hawdb --lib ooc_index_projection`; `cargo test --locked -p hawdb --lib mutation_index_seek`

Out-of-core index projection coverage ([#2019](https://github.com/nowledge-co/hawdb/issues/2019))
is owned by `api::tests::ooc_index_projection` in the existing
`//:hawdb_unit_general_tests` owner. After `CREATE INDEX` on a checkpointed
out-of-core store (one-hour age trigger) the automatic checkpoint builds the
persistent projection by itself; `StorageResidencyReport::unserved_graph_property_indexes`
names the index before and is empty after, also across reopen, and `SET` by the
key counts a `mutation_target_index_seeks`. Without background maintenance an
uncovered index makes writes scan the canonical base by property
(`mutation_target_property_scans`, never `mutation_target_label_scans`), stop
at the first match over `max_mutation_affected_rows` with the scan's error, and
an explicit checkpoint switches them to seeks. Lookups and `SET`s by the key
equal an unindexed twin's before and after the projection exists, across base,
delta-created, re-keyed, deleted and delta-updated nodes; a canonical node
materialized by a write to another property stays findable through the index,
for `MATCH … SET` and for `MERGE` (which before the fix created a duplicate
node). A projection build refused by its budget (forced through
`GraphStore::force_property_projection_budget_refusals`) is reported in
`persistent_property_projection_budget_refusals`, and a later automatic
checkpoint driven by the age trigger still builds it.
Resident stores report no unserved indexes. `api::tests::mutation_index_seek`
now requires zero label scans for every indexed case. No new lane, profile or
timeout.
