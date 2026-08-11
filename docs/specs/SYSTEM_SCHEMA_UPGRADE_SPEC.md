# System Schema Upgrade Contract

## Scope

Skein treats engine-owned metadata and application-owned durable tables as
versioned system schemas. An embedding application registers its schema during
database open; the embedded handle is not returned until the registered schema
is current. Ordinary user tables remain outside this contract.

The first intended application schema is Nowledge Content Store. Its owner and
table definitions remain adapter-owned and are deliberately not compiled into
Skein.

## Upgrade Protocol

The protocol follows the useful local subset of TiDB bootstrap upgrades:

1. Each owner has an append-only migration list, contiguous from version 1.
2. A migration identity includes owner, version, name, and the ordered SQL
   statement bytes. Skein persists its SHA-256 checksum.
3. Existing migration identities and checksums are verified before any write.
4. All pending DDL and migration records for one owner are staged in one mixed
   graph/relational transaction and published by one WAL commit.
5. The current version is therefore visible only after every pending statement
   succeeds. A failed statement publishes neither schema objects nor version.
6. A database newer than the running binary, a changed applied migration, a
   non-contiguous registry, or a drifted registry table fails closed.
7. Read-only open succeeds only when no registered migration is pending.
8. Durable `Database::open` applies the engine-owned registry before returning.
   An embedding host registers its application-owned registries in open
   options, and those upgrades also finish before the handle becomes visible.
9. Ordinary SQL may read `skein_schema_migrations` for diagnostics but cannot
   create, alter, index, insert, update, or delete it. The `skein.engine` owner
   is reserved.

Schema commits advance the database-wide durable commit epoch because graph
and relational state share one WAL. A pure relational commit MUST NOT create
an empty search-projection mutation. Projection lag is measured against the
latest retained projection-relevant mutation or the changefeed resume floor,
not against unrelated schema epochs; otherwise an automatic schema upgrade
would make an unchanged search projection spuriously stale.

Skein Lightning includes the engine registry in its relational stream. An
in-memory source that has not durably bootstrapped the registry derives that
export state without mutating its live commit epoch. Initial import accepts a
target containing only the verified engine bootstrap, but rejects a stream
that omits or drifts the engine registry.

`ALTER TABLE ... ADD COLUMN` is durable through the same WAL transaction. It
rewrites immutable relational rows with the declared default (or `NULL`) and
is admitted against row and full resident-rewrite byte limits. Adding a
`NOT NULL` column without a default to a non-empty table is rejected. `IF NOT
EXISTS` and inline key, unique, or foreign-key constraints are rejected so a
migration cannot hide schema drift.

TiDB also coordinates bootstrap ownership between distributed nodes. Skein
does not copy that mechanism: its embedded durable writer already owns the
exclusive database open, so a second bootstrap election would add no safety.

## Application Registration

Application schema definitions remain in the owning adapter. Skein provides
`SystemSchemaRegistry`, `SystemSchemaMigration`, and
`Database::apply_system_schema_registry`; it does not compile Nowledge table
definitions into the default database facade. `NowledgeMemOpenOptions` accepts
one registry per owner so an embedded runtime applies them before becoming
available to the host.

Route-specific behavior remains parameterized SQL. Schema registration is a
typed API because it coordinates ordered DDL, checksum validation, WAL commit,
and open failure; it is not a typed replacement for business queries.

## Data Migration Boundary

Schema upgrade and SQLite data migration are separate state machines. Schema
upgrade must stay small and deterministic. The Nowledge adapter owns SQLite's
online backup, immutable snapshot identity, stable keyset pages, row and byte
budgets, and the import cursor. Each data page and cursor update is committed
in the same Skein transaction. The source SQLite file remains unchanged.

Completing the immutable Content Store snapshot is not live-cutover evidence.
Cutover remains fail-closed until the host atomically fences legacy writers,
proves the source high-water mark, and switches write ownership. Count, hash,
ordering, anchor reachability, differential query, crash, and
representative-load gates remain required before SQLite can be decommissioned.

## Verification

The bounded `SkeinSystemSchemaUpgrade.tla` model checks ordered suffix staging,
atomic durable and visible schema/registry versions, fail-closed validation,
read-only and failed-DDL behavior, crash recovery, and the Skein Lightning
registry boundary. The model and its TLC configuration are executed by
`scripts/check-storage-tla.sh`.

Focused Skein tests must prove:

- fresh version 1 bootstrap and version 2 upgrade;
- restart idempotency without an extra commit;
- checksum and future-version rejection;
- failed DDL does not publish a migration version;
- read-only open rejects pending upgrades;
- ordinary SQL and application migrations cannot modify the engine registry;
- pure schema commits do not create search mutations or projection work.

The Mem adapter must separately prove:

- a completed Content Store snapshot cannot bypass the live-write fence;
- SQLite page rollback replays the same primary keys without duplicates;
- an oversized source row fails rather than returning a partial page.
