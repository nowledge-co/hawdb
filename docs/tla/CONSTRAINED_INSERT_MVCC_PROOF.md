# MVCC footprints for pure constrained inserts

The Mem content-store contract contains unique document ownership, unique chunk
ordinals and foreign keys from messages/chunks/anchors to documents (see
`crates/qualification/fixtures/nowledge_content_store/content_store_schema_v1.sql`).
Previously any such table forced a Database barrier, so even different documents
or different child rows sharing a stable parent could not commit from one epoch.

This extension qualifies only explicit `RelationalWrite::Insert` with
`RelationalInsertMode::Error` on constrained or referenced tables. Replace,
primary-key deletion and other destructive work on those tables remain broad.
Unconstrained explicit replacements/deletes retain their existing row footprints.
Constrained predicate replay, UPSERT, DDL and opaque relational WAL remain broad.
Unconstrained complete-key predicates have a separate
[replay refinement](PRIMARY_KEY_PREDICATE_MVCC_PROOF.md). This does
not narrow arbitrary constraint/cascade work or introduce serializable reads.

## Identity mapping and validation

For table t, primary projection P, and each non-primary unique definition i with
name N(i) and column projection U(i), the insert footprint is

    {(RelationalRow, t, P(row))}
    union {(RelationalIndex, t, N(i), U(i,row)) : U(i,row) has no NULL component}.

A transaction records the union over every input row and statement, not only
its final net changes. Column positions preserve schema order. Constraint names
come from `relational_unique_index_name(ordinal)` and declared unique-index
names are used verbatim, matching `required_index_definitions`. Declared names
cannot use the engine's reserved namespace. Table and index name are separate
identity fields, so different tables/definitions cannot alias just because their
values match. DDL remains a broad barrier when those definitions change.

Values are cloned into the existing `RelationalKey`; there is no alternative
text encoding or equality relation. Authoritative row validation requires the
exact declared scalar type, so a successfully staged raw value is the same
value used by row/index key construction. Invalid row/key shapes retain the
existing broad fallback and staging diagnostics. Any NULL component exempts a
secondary unique key exactly as in authoritative unique-index construction;
stamping a shared NULL sentinel would falsely serialize otherwise valid rows.
Primary keys retain their usual non-NULL requirement.

Two successful pure inserts can conflict only by sharing a primary identity or
a non-NULL unique identity. A prior later-epoch insert of either identity leaves
a tested stamp. Conversely, each newer tested identity denotes one such overlap.
Thus validation rejects those stale inserts, while distinct primary and unique
identities do not cause a version conflict. Within-transaction duplicates and
violations that existed before the captured epoch remain ordinary constraint
errors; this change does not bypass canonical constraint staging.

Validation runs before staging against current canonical rows. A newly committed
unique value is therefore reported as a typed retryable `relational_index`
conflict instead of a later ordinary duplicate-key error. The complete mixed
footprint is checked again before WAL. Index publication, canonical constraint
checks and durable atomicity are unchanged; no stale private index root is
installed into the live database.

## Foreign-key dependency boundary

A pure Error-mode insert does not remove or modify an existing referenced row,
so it cannot cause ON DELETE/UPDATE cascades. A valid private insert's foreign
key either refers to a row in its snapshot or to a row inserted earlier in the
same transaction. Two inserts may read the same parent without conflicting;
no `ForeignKey` write stamp is emitted for that read.

This is safe because every operation that could invalidate that parent or its
referenced unique value remains broad:

1. Tables with outgoing foreign keys or non-primary uniqueness accept the
   narrow path only for Error-mode inserts. Replace/delete and mixed sequences
   return a Database barrier during classification, before any relational keys
   are recorded.
2. A single catalog scan checks incoming references too. If a touched table has
   any non-pure-insert operation, an incoming edge makes the transaction broad.
   The per-table `insert_only` flag is intersected over all its operations, so
   an earlier insert cannot hide a later destructive write, or vice versa.
3. Predicate/UPSERT/DDL/opaque paths already remain broad. Schema changes also
   prevent an old transaction from silently validating under different foreign
   keys or index definitions.
4. If destructive work commits first, its newer Database stamp rejects the
   old child insert. If the child insert commits first, the broad writer's
   current-epoch check rejects its stale workspace, even though the child did
   not stamp the parent. These are the two existing broad-barrier directions.

The new insert path therefore does not need to serialize shared parent reads.
A future narrowing of destructive foreign-key or referenced-unique work must
replace this barrier argument with complete dependency handling; simply emitting
that writer's own primary keys would invalidate this proof. Canonical constraint
checks remain mandatory, including for invalid fresh writes.

## Admission and publication

Classification deduplicates touched-table metadata, retaining primary positions
and the names/positions of unique definitions. It does not construct metadata
for non-unique/FK support indexes. Incoming-edge work remains one catalog pass.
Every additional unique identity uses the existing VersionWriteSet count and
estimated-byte limits and the current/retained-root admissions. Table/index/key
payload is charged by the existing `VersionKey::RelationalIndex` weight.
Collection failure discards private preparation before canonical data, epoch or
WAL publication. Extra constraint identities can therefore reach admission
limits sooner; quotas have not been enlarged. Temporary classification/key
allocation and total process RSS are not bounded by this payload estimate.

## Finite model and negative controls

`HawDBConstrainedInsertIntent.tla` models two transactions, two primary keys,
two non-NULL unique values plus NULL, and one shared parent. Inserts write a row
and optionally a unique identity; parent deletion uses a broad barrier and
atomically removes its children. An independent full-history oracle compares
primary equality, non-NULL unique equality and broad effects directly, rather
than consulting the collected identity sets.

The model checks validator equivalence (including false conflicts), preservation
of canonical uniqueness/FKs, constraints after successful validation and
first-committer-wins. It completed with **2,000 generated / 1,640 distinct states**,
depth 5 and an empty queue. The Bazel TLC action and test actually executed.
Three registered mutants must violate `ValidationMatchesHistory`: omit unique
stamps, stamp NULL as an ordinary unique value, or stamp a shared parent read as
a write. Separate named witnesses reach disjoint shared-parent commits, two NULL
inserts and a later parent deletion/cascade.

```sh
bazel test //docs/tla:HawDBConstrainedInsertIntent_check
scripts/check-storage-tla.sh --check-mutants
```

For a witness, append its `INVARIANT` to a copy of the positive cfg and require
that exact named violation: `NoDisjointWitness`, `NoNullWitness`, or
`NoCascadeWitness`. The finite instance does not cover arbitrary FK networks,
physical index encoding, allocation, WAL failure or pin lifetimes. The source
argument extends the identity projection to multiple definitions/tables; the
existing MVCC, intent, budget and group-commit models retain their own separate
boundaries. This is not a composed machine-checked Rust proof.

## Executable evidence

Three configurations use contract-shaped parent ownership and child ordinal
constraints with a declared unique token index: Materialized, OutOfCore with
materialized relational indexes, and OutOfCore with Authoritative relational
indexes. Each fixture checkpoints and reopens before concurrent writers start;
the third explicitly asserts canonical metadata-only relational residency. They check two
multi-table disjoint commits sharing one parent; compound, declared-index and
parent-owner unique conflicts; multiple NULL keys; unchanged old snapshots;
checkpoint retention; byte-identical rejected WAL; atomic absence of the losing
transaction's otherwise disjoint parent row; exact reopen rows/epoch; and new
unique conflicts after restart followed by a second reopen. Parent deletion and
child insertion are tested in both commit orders with cascade-capable metadata.

Collector tests check constraint/index namespaces, NULL exclusion, both orders
of mixed insert/destructive work, count/byte admission and incoming dependencies
on otherwise unconstrained tables. A long declared index name makes the new
unique identities exhaust the default 16 MiB write-set estimate while ordinary
row payload stays small; rejection preserves canonical rows, epoch and stamps.

Restoring the old collector reproduces the false Database conflict for disjoint
inserts. Omitting unique identities instead loses the typed retryable unique
conflict, which the integration test rejects. Both source probes are temporary
and restored before positive validation. Predicate/UPSERT/destructive constraint
footprints, broader resource qualification and single-stream latency remain
separate acceptance work for #231/#232.
