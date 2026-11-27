# Resident optimizer-statistics refresh policy (#2017)

Owner: api::tests::statistics_refresh_policy
Lane: `//:hawdb_unit_general_tests` (existing required native general lane)
Replay: `cargo test --locked -p hawdb --lib statistics_refresh_policy`

Resident optimizer-statistics refresh policy ([#2017](https://github.com/nowledge-co/hawdb/issues/2017))
is owned by `api::tests::statistics_refresh_policy` in the existing
`//:hawdb_unit_general_tests` owner. Eight finite fixtures assert per-caller
full-recompute counters from `Database::optimizer_statistics_maintenance_report`:
crossing 999 -> 1,001 committed nodes recomputes once at commit and never again
at planning; non-durable growth from 500 to 50,500 nodes refreshes at commit
with exact label counts; 256 indexed `SET`s in one explicit transaction add no
planning or commit recompute and count as dirty only at commit; a durable
resident store leaves an over-threshold commit to the background statistics
task, which resets lag; stale and forced-refresh statistics return identical
rows. Under the default `LocalQosPolicy` the background refresh runs for a
50,000-record durable resident store and a 3,000-record out-of-core store
(charge clamped to one per-task slot), reporting write-guard hold time; an
inadmissible refresh memory budget is skipped without waiting, counted in
`background_refresh_skipped`, and a later pass refreshes. Replay with `cargo test --locked -p hawdb --lib statistics_refresh_policy`
or the owning Bazel target with `--test_arg=statistics_refresh_policy`. No new
lane, profile or timeout.
