# System Schema Upgrade Contract

## Scope

HawDB treats engine-owned metadata and application-owned durable tables as
versioned system schemas. An embedding application registers its schema during
database open; the embedded handle is not returned until the registered schema
is current. Ordinary user tables remain outside this contract.

The first intended application schema is Nowledge Content Store. Its owner and
table definitions remain adapter-owned and are deliberately not compiled into
HawDB.

## Upgrade Protocol

The protocol follows the useful local subset of TiDB bootstrap upgrades:

1. Each owner has an append-only migration list, contiguous from version 1.
2. A migration identity includes owner, version, name, and the ordered SQL
   statement bytes. HawDB persists its SHA-256 checksum. A migration may also
   carry Cypher graph schema DDL (`SystemSchemaMigration::with_cypher`): index
   and constraint creation only, applied after the migration's SQL in the same
   transaction. A non-empty Cypher section is hashed after a
   `hawdb-system-schema-cypher-v1` tag, so SQL-only checksums are unchanged
   and a Cypher statement never hashes like SQL of the same text. Any other
   Cypher (data writes, reads, transaction control, `CHECKPOINT`) is refused
   before any write. Creating an index that already exists with the same
   label, property and kind is a no-op and the migration is still recorded; an
   index of another kind is a separate index. On an out-of-core handle the
   apply checkpoints after the commit when a registry index is not yet served
   by the persistent projection (#2019), so the host's first write seeks it. A
   failed checkpoint is returned as an error after the migration is recorded;
   the next apply retries it.
3. Existing migration identities and checksums are verified before any write.
4. All pending DDL and migration records for one owner are staged in one mixed
   graph/relational transaction and published by one WAL commit.
5. The current version is therefore visible only after every pending statement
   succeeds. A failed statement publishes neither schema objects nor version.
6. A database newer than the running binary, a changed applied migration, a
   non-contiguous registry, or a drifted registry table fails closed.
7. Read-only open succeeds only when no registered migration is pending.
   Otherwise it returns `HawDBError::SystemSchemaUpgradeRequired` with the
   owner and its installed and expected versions, and does not poison a
   handle. C ABI kind and Python `kind`: `system_schema_upgrade_required`.
8. Durable `Database::open` applies the engine-owned registry before returning.
   An embedding host registers its application-owned registries in open
   options, and those upgrades also finish before the handle becomes visible.
10. `Database::apply_system_schema_registries` verifies several owners'
    registries before any write, then stages every owner's pending suffix in
    one transaction and one WAL commit. Either every owner reaches its current
    version or none advances. Embedded open applies its registered owners
    this way.

A commit that creates or alters a relational table requires a canonical row
checkpoint before an out-of-core read may serve the changed tables. The
row-page base and its live overlay bind a fixed table set with schema digests;
a new or changed table, even an empty one, cannot be served from them. The WAL
alone already makes the schema durable: crash recovery replays it and
checkpoints before the writable handle returns. When every table a commit
rewrites is empty (new tables, and indexes on them), rows are resident and
relational indexes are materialized, the checkpoint waits for the next
ordinary one or for the first row written to those tables, so N such owners
cost no checkpoint and their first use costs one (#1942). Otherwise the
commit runs a synchronous checkpoint before returning, and committing N owners
together pays one checkpoint instead of N.
9. Ordinary SQL may read `hawdb_schema_migrations` for diagnostics but cannot
   create, alter, index, insert, update, or delete it. The `hawdb.engine` owner
   is reserved.

Schema commits advance the database-wide durable commit epoch because graph
and relational state share one WAL. A pure relational commit MUST NOT create
an empty search-projection mutation. Projection lag is measured against the
latest retained projection-relevant mutation or the changefeed resume floor,
not against unrelated schema epochs; otherwise an automatic schema upgrade
would make an unchanged search projection spuriously stale.

HawDB Lightning includes the engine registry in its relational stream. An
in-memory source that has not durably bootstrapped the registry derives that
export state without mutating its live commit epoch. Initial import accepts a
target containing only the verified engine bootstrap, but rejects a stream
that omits or drifts the engine registry.

Logical dump import ([LOGICAL_DUMP.md](../LOGICAL_DUMP.md#system-schema-registries))
restores application records into its fresh, unpublished target through the
engine's registry insert, never through a statement from the dump. It validates
the table definition, the engine's records and the shape of every other record
before it writes, keeps the target's own engine records, and verifies the
restored records before publication. Import runs no migration. A host that
passes its registries gets rule 3 before any write; otherwise rule 3 applies
when the host registers them on open.

`ALTER TABLE ... ADD COLUMN` is durable through the same WAL transaction. It
rewrites immutable relational rows with the declared default (or `NULL`) and
is admitted against row and full resident-rewrite byte limits. Adding a
`NOT NULL` column without a default to a non-empty table is rejected. `IF NOT
EXISTS` and inline key, unique, or foreign-key constraints are rejected so a
migration cannot hide schema drift.

TiDB also coordinates bootstrap ownership between distributed nodes. HawDB
does not copy that mechanism: its embedded durable writer already owns the
exclusive database open, so a second bootstrap election would add no safety.

## Application Registration

Application schema definitions remain in the owning adapter. HawDB provides
`SystemSchemaRegistry`, `SystemSchemaMigration`, and
`Database::apply_system_schema_registry` (one owner) and
`Database::apply_system_schema_registries` (several owners, one commit); it does
not compile Nowledge table definitions into the default database facade.
`EmbeddedStoreOpenOptions` accepts one registry per owner so an embedded runtime
applies them together before becoming available to the host.

Route-specific behavior remains parameterized SQL. Schema registration is a
typed API because it coordinates ordered DDL, checksum validation, WAL commit,
and open failure; it is not a typed replacement for business queries.

`Database::system_schema_status` reports `owner`, `installed_version`,
`expected_version` and `pending` for the engine and every given registry,
without writing, on read-only and writable handles. Validation is the same as
for an upgrade, so a drifted or future schema is an error, not a status.
`engine_system_schema_expected_version` returns the version this build's engine
registry ends at, for a database that a read-only open refused.

## Data Migration Boundary

Schema upgrade and SQLite data migration are separate state machines. Schema
upgrade must stay small and deterministic. The Nowledge adapter owns SQLite's
online backup, immutable snapshot identity, stable keyset pages, row and byte
budgets, and the import cursor. Each data page and cursor update is committed
in the same HawDB transaction. The source SQLite file remains unchanged.

Completing the immutable Content Store snapshot is not live-cutover evidence.
Cutover remains fail-closed until the host atomically fences legacy writers,
proves the source high-water mark, and switches write ownership. Count, hash,
ordering, anchor reachability, differential query, crash, and
representative-load gates remain required before SQLite can be decommissioned.

## Verification

The bounded `HawDBSystemSchemaUpgrade.tla` model checks ordered suffix staging,
atomic durable and visible schema/registry versions, fail-closed validation,
read-only and failed-DDL behavior, crash recovery, and the HawDB Lightning
registry boundary. The model and its TLC configuration are executed by the
`//docs/tla:HawDBSystemSchemaUpgrade_check` Bazel target and included in the
`//docs/tla:storage_models` suite. `scripts/check-storage-tla.sh` repeats the
bounded check only when producing retained release evidence.

Focused HawDB tests must prove:

- fresh version 1 bootstrap and version 2 upgrade;
- several owners publish one commit and, for empty tables, no checkpoint,
  fail together, and recover whole or absent after a crash or power loss at
  each publication step, including the deferred checkpoint;
- restart idempotency without an extra commit;
- checksum and future-version rejection;
- failed DDL does not publish a migration version;
- a Cypher graph index migration backfills existing nodes, applies once and is
  used by reads and writes; a failing Cypher statement publishes no owner;
  non-DDL Cypher is refused; SQL-only checksums match a golden value;
- read-only open rejects pending upgrades;
- ordinary SQL and application migrations cannot modify the engine registry;
- logical dump import restores application records, installed and pending,
  and refuses forged or corrupt records before writing;
- pure schema commits do not create search mutations or projection work.

The Mem adapter must separately prove:

- a completed Content Store snapshot cannot bypass the live-write fence;
- SQLite page rollback replays the same primary keys without duplicates;
- an oversized source row fails rather than returning a partial page.
