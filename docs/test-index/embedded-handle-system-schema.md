# Runtime system schema registry apply through the embedded handle (Mem index rollout)

Owner: embedded_store::tests::system_schema_tests
Lane: `//:hawdb_unit_general_tests` (existing required native general lane)
Replay: `cargo test --locked -p hawdb --lib -- embedded_store::tests::system_schema_tests::`

`EmbeddedStoreHandle::apply_system_schema_registry_context` (and the plural
`apply_system_schema_registries_context`) apply a `SystemSchemaRegistry` to an
already-open store from a host maintenance thread. The family pins: a Cypher
`CREATE INDEX` migration on a checkpointed out-of-core durable store applies
versions 1 and 2, runs the post-schema checkpoint (`checkpointed`) and leaves
`StorageResidencyReport::unserved_graph_property_indexes` empty, also across
reopen; re-applying is a no-op (nothing applied, no checkpoint, same commit
epoch). Resident durable and in-memory stores apply without a checkpoint. A
read-only handle returns the report when nothing is pending and
`HawDBError::ReadOnly { operation: "apply_system_schema_registry" }` when a
migration is pending, writing nothing (reopened writable, it is still pending).
On a one-CPU governor a held CPU slot makes the default context refuse at once
and a bounded admission wait end with `RuntimeAdmissionRefused` carrying the
time queued; cancelled, expired and queued-past-deadline contexts return
`RuntimeTaskStopped`; nothing is applied in either case. A call queued for
admission holds neither the store guard nor the exclusive database lease (a
writer on another thread takes the write access and commits meanwhile). Two
threads applying the same registry apply it exactly once; the other skips it
or gets the immediate, typed `BranchBusy { resource: "system schema apply in
progress" }` (never a lock-timeout `Execution` error), and a call made while
another holds the per-handle apply guard is refused with it without queueing. Readers on other threads keep succeeding
with the same result while the registry applies. A migration that fails at execution
(a duplicate `CREATE TABLE` in a later migration) rolls back the whole batch:
no table, no index, no record, same commit epoch, handle usable, and a corrected
registry later applies every migration. Background maintenance is disabled in
the fixtures so only the apply's own checkpoint can build the projection. No
sleeps beyond 30 ms admission deadlines; no new lane, profile or timeout.
