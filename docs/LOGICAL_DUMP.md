# Logical Dump and Persistent Format Upgrades

HawDB's on-disk format is still in development. A release may change it
without a migration (AGENTS.md: no compatibility migrations for development
formats), and the new reader then refuses the old files with a typed
`UnsupportedStorageFormat` error (see [STORAGE.md](STORAGE.md#reader-compatibility-refusals)).
An embedding application that keeps a user's only copy of its data in HawDB
needs a supported way across such a change. The engine provides two things
for that:

1. **A persistent format version** that summarizes every on-disk codec, so a
   host can tell before an upgrade whether the new library will open its
   databases. It is exposed as `hawdb::PERSISTENT_FORMAT_VERSION` /
   `hawdb::persistent_format_version()` in Rust and `hawdb_format_version()`
   in the C ABI. Each release's notes say whether it changed.
2. **A logical dump**: a versioned directory of documented JSON files that is
   independent of the physical format. Every later HawDB release reads every
   dump format version up to its own. Export with the old library, import
   with the new one.

No in-place upgrader exists, and none is promised. The dump format is the one
compatibility surface the engine maintains.

## Upgrading an embedded database

Before replacing the HawDB library a host ships:

1. Compare `hawdb_format_version()` of the running library with the format
   version of the new release. Release notes carry a line
   `Persistent format changed: yes|no (...)` that names both versions.
2. If they are equal, replace the library. The new one opens the database
   in place.
3. If they differ, use the **old** library to export every database it owns
   (`hawdb_export` / `Database::export_logical_dump`) into a new directory.
   Keep the original database until the next step succeeds. Optionally check
   the dump with `hawdb_verify_dump` / `hawdb::verify_logical_dump`; its
   `manifest_sha256` equals the export report's.
4. Use the **new** library to import each dump into a new path
   (`hawdb_import` / `Database::import_logical_dump`), then point the
   application at the imported database. Delete the old database and the
   dump only after the imported database opens and the import report's
   counts match the export report's.

If a host skips the check and the new library opens an old database, the open
fails with `unsupported_storage_format` naming the format and both versions
(for example `unsupported storage manifest version 0; this reader supports
version 1`). The open writes nothing and the old data stays intact, so the host
can still go back to the old library for step 3.

The export must run with the library that wrote the database, because only
that library can read it. A host that may skip releases must therefore export
before every update whose format version differs from the running one. It
does not have to support every intermediate version.

## Persistent format version

`PERSISTENT_FORMAT_VERSION` (`crates/storage/src/persistent_format.rs`) is a
single integer. Two builds with the same value read each other's databases; a
change that makes an existing database unreadable bumps it. It is not written
into the database: the individual codecs keep their own version fields and
refusals, and the integer is the summary hosts compare.

`docs/persistent-format.lock` records the version together with every
magic/version/format constant declared in the crates that write persistent
files (`crates/storage`, `crates/search`, `crates/vector-projection`,
`crates/relational`). `scripts/check-persistent-format.py` runs in the
`Persistent Format` workflow. CI fails until the lock is regenerated, so every
codec change shows reviewers whether the version was bumped. The check is a
tripwire, not a proof: a layout change that alters no declared constant must
still bump the version by hand.

On each published release the same workflow compares the version at the
release tag with the previous release tag and appends
`Persistent format changed: yes|no (...)` to the release notes. A previous
release that predates the version reports `unknown`.

Version 1 is the format written by the first release that carries this
constant. Earlier releases make no statement.

## Dump format, version 1

A dump is a directory:

```text
<dump>/
  manifest.json                     written last; its presence marks a complete dump
  schema.json                       relational and graph schema definitions
  data/table-<TTTTT>-<PPPPP>.jsonl  rows of the T-th table in schema order, part P
  data/nodes-00000-<PPPPP>.jsonl    graph nodes, part P
  data/relationships-00000-<PPPPP>.jsonl
```

Data files are JSON Lines (one UTF-8 JSON value per `\n`-terminated line) and
start a new part every `file_records` records (default 100,000). Readers must
take file names from the manifest, never by listing the directory.

### `manifest.json`

```json
{
  "format": "hawdb-logical-dump",
  "dump_format_version": 1,
  "created_by": { "hawdb_version": "0.31.0", "persistent_format_version": 1 },
  "source": { "branch": "main", "commit_epoch": 42 },
  "schema": { "path": "schema.json", "kind": "schema", "records": 0, "bytes": 1234, "sha256": "…" },
  "files": [
    { "path": "data/table-00000-00000.jsonl", "kind": "table_rows", "table": "events",
      "records": 1000, "bytes": 81234, "sha256": "…" },
    { "path": "data/nodes-00000-00000.jsonl", "kind": "nodes", "records": 10, "bytes": 900, "sha256": "…" },
    { "path": "data/relationships-00000-00000.jsonl", "kind": "relationships", "records": 9, "bytes": 700, "sha256": "…" }
  ],
  "counts": { "tables": 1, "table_rows": 1000, "nodes": 10, "relationships": 9 }
}
```

- `sha256` is the lowercase hex SHA-256 of the file's bytes and `bytes` its
  length. `records` is the number of JSON Lines records.
- `files` lists the row files of each table in table order and part order,
  then node files, then relationship files. Import loads them in that order.
- `source.branch` names the exported branch when the database has a branch
  catalog. `source.commit_epoch` is the snapshot the dump reflects.

### `schema.json`

```json
{
  "tables": [
    {
      "name": "events",
      "columns": [
        { "name": "id", "type": "bigint", "nullable": false },
        { "name": "at", "type": "timestamptz(6)", "nullable": false,
          "default": { "current_timestamp": { "zoned": true, "precision": 6 } } },
        { "name": "token", "type": "uuid", "nullable": true, "default": "uuidv7" },
        { "name": "state", "type": "text", "nullable": true, "default": { "literal": "new" } }
      ],
      "primary_key": ["id"],
      "unique": [["token"]],
      "foreign_keys": [
        { "columns": ["thread"], "references_table": "threads", "references_columns": ["id"],
          "on_delete": "cascade", "on_update": "no_action" }
      ],
      "indexes": [{ "name": "events_at", "columns": ["at"], "unique": false }]
    }
  ],
  "graph": {
    "node_labels": ["Memory"],
    "relationship_types": ["MENTIONS"],
    "tables": [{ "kind": "node", "name": "Memory", "state": "public" }],
    "properties": [{ "table_kind": "node", "table": "Memory", "name": "id", "type": "int",
                     "nullable": false, "state": "public" }],
    "indexes": [{ "label": "Memory", "properties": ["id"], "kind": "equality" }],
    "constraints": [{ "kind": "node_unique", "subject": "Memory", "property": "id" }]
  }
}
```

- Tables are ordered so that every foreign key's target precedes its source.
- Column `type` is one of `boolean`, `bigint`, `double precision`,
  `numeric(p,s)`, `text`, `bytea`, `uuid`, `vector(n)`, `timestamp(p)`,
  `timestamptz(p)`. A `default` is `{"literal": <value>}`, `"uuidv7"`, or
  `{"current_timestamp": {"zoned": bool, "precision": p}}` (`zoned: false` is
  `LOCALTIMESTAMP`).
- Referential actions are `no_action`, `restrict` or `cascade`.
- Graph index `kind` is `equality`, `range`, `full_text` or `composite`.
  Constraint `kind` is `node_unique`, `node_exists`, `relationship_unique` or
  `relationship_exists`. Schema `state` is `delete_only`, `write_only`,
  `backfill`, `validating`, `public` or `gc`. Property `type` is `any`,
  `bool`, `int`, `float`, `string`, `text`, `list` or `vector(n)`.
- Index and constraint *definitions* are dumped, never their physical state.
  Import rebuilds them over the imported data, which also re-validates every
  constraint.
- A graph property may carry a `default`, the creation-time default declared
  by `CREATE PROPERTY ... DEFAULT ...`: `{"literal": <value>}` (a non-null
  value in the encoding below that the property `type` accepts) or
  `"current_timestamp"` (`DEFAULT current_timestamp()`, allowed on `int` and
  `any` properties). The field is omitted when a property has none.
  Import re-declares the default, so a node or relationship created
  afterwards without the property gets it, exactly as in the source.
  Restored records are not new records: they keep exactly the properties
  they had, so a record that lacked a defaulted property still lacks it and
  an explicit NULL stays NULL. A property in `backfill` resumes its backfill
  in the imported database with the same literal value. A
  `current_timestamp()` default still in `backfill` would be re-resolved to
  a different instant, so export refuses it (see Coverage) and `verify`
  refuses a dump declaring one; `verify` also refuses a null default, a
  literal the property type rejects, and an unknown default kind.
- A table may list `search_indexes`, the definitions of BM25 SQL search
  indexes over its `text` columns:
  `[{"name": "events_text", "column": "text", "analyzer": "cjk_ngram"}]`.
  The field is omitted when a table has none. An index name shares the
  relation namespace, so it never equals a table name or another search
  index's name; `verify` refuses a dump that breaks this or names a column
  that is not `text`. The postings are never dumped. A build that has no SQL
  search indexes refuses to import a dump listing one, with `Semantic`,
  before writing anything, rather than importing the table without its index.

### Data records

- **Table rows:** a JSON array of the row's values in the table's column order.
- **Nodes:** `{"id": <u64>, "label": "<label>", "properties": {"name": <value>, ...}}`.
- **Relationships:** `{"id": <u64>, "type": "<type>", "source": <node id>,
  "target": <node id>, "properties": {...}}`.

Node and relationship ids are preserved by import, so ids a host stored
elsewhere stay valid.

### Values

Every value has exactly one encoding, and every JSON object is a single-key
tag, so a reader never has to guess a type:

| Value | Encoding |
|---|---|
| null, boolean, string | JSON `null`, `true`/`false`, string |
| 64-bit integer | JSON integer, exact over the full `i64` range |
| float | `{"$float": <number>}`, or `"NaN"`, `"Infinity"`, `"-Infinity"` |
| decimal | `{"$decimal": "<exact decimal text>"}` |
| binary | `{"$binary": "<standard padded base64>"}` |
| UUID | `{"$uuid": "<canonical text>"}` |
| vector | `{"$vector": [<float32 coordinates>]}` |
| temporal | `{"$date"|"$localtime"|"$localdatetime"|"$datetime"|"$duration": "<ISO 8601>"}` |
| list | JSON array |
| map | `{"$map": {"key": <value>, ...}}` |

Relational `timestamp(p)` values appear as `$localdatetime` and
`timestamptz(p)` values as `$datetime`. The column type restores the exact
stored value on import.

### Forward-compatibility rules

These rules are what lets every later release read a version 1 dump:

- `format` must be `hawdb-logical-dump`. `dump_format_version` is an integer.
  A reader accepts every version from 1 to its own. A newer version is
  refused with `UnsupportedStorageFormat` (`format: "logical dump"`, naming
  both versions) before anything is written.
- Readers ignore object fields they do not know. Writers add only optional
  fields within a version.
- A new kind of data (a new file `kind`, schema section entry, column or
  property type, value tag or schema state) requires a new
  `dump_format_version`. Readers refuse unknown kinds, tags and spellings
  instead of skipping them, so a dump is never imported partially.
- The spellings above never change meaning. A later version may add
  spellings but not remove or reinterpret these.

## Consistency and failure behaviour

- **Export** reads one pinned read snapshot
  (`DatabaseReadTransaction::export_logical_dump`). Writers that commit while
  it runs are not in the dump. Relational tables are read in primary-key
  keyset pages of `page_rows` rows (default 512). A page that exceeds the read
  payload budget is halved, down to one row, before export refuses. A table
  without a primary key is read in offset pages, which cost quadratic work in
  its row count. Each table's exported row count is checked against
  `COUNT(*)` at the same snapshot, and the graph's counts against the
  snapshot's statistics.
- Export writes into a sibling staging directory
  (`.<name>.hawdb-export-<pid>-<nonce>-<n>`), synchronizes every file and
  directory, and moves the stage onto the destination last (see
  [Publication](#publication)). A failed export removes the stage it created
  and leaves nothing at the destination.
- **Import** reads `manifest.json` and `schema.json` (each at most 64 MiB),
  checks the format and version, and validates the manifest's paths, kinds,
  tables and counts before it creates anything. It builds the database in a
  sibling staging directory with `SyncOnCheckpoint` durability. It verifies
  every data file's length, record count and SHA-256 as it streams it.
  Afterwards it compares each table's `COUNT(*)` and the graph counts with the
  manifest, checkpoints, closes the database and moves the stage onto the
  destination. Every failure before that move publishes nothing.
- Both refuse an existing destination and require its parent directory to
  exist.
- A process crash or power loss during either operation can leave a
  `.<name>.hawdb-export-*` or `.<name>.hawdb-import-*` directory beside the
  destination. It is never adopted or resumed. Run the operation again
  (staging names are unique) and delete leftovers when no operation is
  running.
- `verify_logical_dump` (`hawdb_verify_dump`, `library.verifyDump`) checks
  a dump the way import does (format version, paths, counts, and every
  file's length, record count and SHA-256) without building anything or
  writing a byte.
- Import keeps the importing engine's own rows in the engine migration
  registry (`hawdb_schema_migrations`, owner `hawdb.engine`). Only the
  application owners' migration records come from the dump.
- Memory stays bounded by `page_rows`. Graph import commits a batch of
  `page_rows` records at a time and checkpoints every
  `import_checkpoint_records` records (default 200,000).

## Publication

The last step of both operations moves the complete staging directory onto
the destination with one no-replace filesystem operation:
`renameat2(RENAME_NOREPLACE)` on Linux, `renamex_np(RENAME_EXCL)` on macOS and
`MoveFileExW` without `MOVEFILE_REPLACE_EXISTING` on Windows. An empty
directory, populated directory, file or symlink that another process creates
at the destination at any moment before the move is never replaced: the call
fails with `storage`, removes its own stage and publishes nothing. There is no
check-then-rename fallback. A platform or filesystem without such a primitive
(for example one returning `EINVAL` or `ENOTSUP` for the flag) fails the same
way, with nothing published.

### Unknown publication outcome

After the move the destination holds the complete result, but its name is
durable only once the parent directory is synchronized. A failure from the
move onwards, for example a failing parent-directory sync, cannot honestly be
reported as "nothing published", so it is a distinct error:
`HawDBError::PublishOutcomeUnknown { destination, message }`, C ABI and Bun
kind `publish_outcome_unknown`. Every other error kind means nothing was
published. On `publish_outcome_unknown`:

1. **Keep the destination.** It is never a partial result, and the operation
   does not remove it. Do not delete it as a failed attempt.
2. Call `confirm_logical_dump_publication(destination)`
   (`hawdb_confirm_dump_publication`, `library.confirmDumpPublication`). It
   checks that the destination is a directory and synchronizes it and its
   parent. It is idempotent: repeat it until it succeeds; calling it after a
   successful operation changes nothing.
3. Check the content: an export with `verify_logical_dump(destination)`, an
   import by opening it (and scrubbing it for full assurance).
4. If the destination is missing (a crash undid the move), the publication
   did not survive. Run the operation again; its stale stage is neither
   adopted nor removed, as above.

A retried export or import against the same destination refuses with
`storage` because the destination exists, so a host never publishes twice by
retrying.

## Coverage

Format version 1 covers the selected branch's:

- relational tables: every column type, nullability, defaults, primary keys,
  unique constraints, foreign keys including self-references, secondary
  indexes, and rows;
- the property graph: labels, relationship types, typed node and
  relationship tables and properties with their schema states and
  creation-time defaults, nodes,
  relationships, properties, and the definitions of equality, range,
  full-text and composite property indexes and of unique/exists constraints.

Export refuses, with a `Semantic` error naming the object, when the snapshot
contains state that format 1 cannot represent. Nothing is written in that
case:

- strict append tables (`storage_mode = 'strict_append'`);
- materialized views;
- persisted projected-graph definitions;
- nodes with zero or several labels;
- a `current_timestamp()` graph property default whose backfill has not
  finished (run schema maintenance until the property is `public`, then
  export);
- foreign-key cycles between different tables.

Not exported:

- **Other branches.** A dump holds one branch. Export each branch from a
  database opened on it. Import creates a database with a single `main`
  branch.
- **Rebuildable derived state**: search and vector projection generations,
  projected-graph artifacts, statistics and caches. Their definitions belong
  to the host (`rebuild_search_projection`, `resolve_vector_index`). Rebuild
  them after import.
- Physical identity: storage generations, commit epochs, WAL history and
  database instance identity start fresh in the imported database.

Each refused kind gets format support in a later `dump_format_version`,
tracked under issue #1655.

## API

| Rust | C ABI | Bun |
|---|---|---|
| `hawdb::PERSISTENT_FORMAT_VERSION`, `persistent_format_version()` | `uint32_t hawdb_format_version(void)` | `library.formatVersion()` |
| `hawdb::LOGICAL_DUMP_FORMAT_VERSION` | `uint32_t hawdb_dump_format_version(void)` | `library.dumpFormatVersion()` |
| `Database::export_logical_dump(dest)`, `DatabaseReadTransaction::export_logical_dump(dest, &options)`, `HawDBEmbedded::export_logical_dump(dest)` | `bool hawdb_export(db, dest, dest_len, report_out, err_out)` | `database.export(dest)` |
| `Database::import_logical_dump(src, dest)`, `HawDBEmbedded::import_logical_dump(src, dest)` | `bool hawdb_import(src, src_len, dest, dest_len, report_out, err_out)` | `library.import(src, dest)` |
| `hawdb::verify_logical_dump(path)` | `bool hawdb_verify_dump(path, path_len, report_out, err_out)` | `library.verifyDump(path)` |
| `hawdb::confirm_logical_dump_publication(dest)` | `bool hawdb_confirm_dump_publication(dest, dest_len, err_out)` | `library.confirmDumpPublication(dest)` |

The C ABI reports are the Rust `LogicalDumpExportReport`,
`LogicalDumpImportReport` and `LogicalDumpVerifyReport` serialized field for
field as JSON, and errors use the usual `{"kind", "message"}` envelope:

- `semantic` for an unrepresentable kind of data;
- `transaction_open` when a handle with an open transaction exports;
- `unsupported_storage_format` for a dump from a newer release;
- `storage_integrity` for a damaged or incomplete dump;
- `storage` for an existing destination or an I/O failure;
- `publish_outcome_unknown` when the complete result may already be at the
  destination ([Unknown publication outcome](#unknown-publication-outcome)).
