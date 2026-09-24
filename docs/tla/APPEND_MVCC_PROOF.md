# Strict-append MVCC footprint

Typed strict-append writes now participate in MVCC through `AppendTable(name)`.
Distinct tables can commit from one snapshot without invalidating each other.
Every write to the same table still conflicts, even across different partitions.
This is deliberately table-granular because generated sequence allocation is
shared by all partitions of a table. It is not partition-level concurrency or
serializable read-set validation.

## Footprint completeness

For table t, let R(t) contain its rows, partition ordering watermarks, and its
optional generated sequence watermark. Distinct names select disjoint entries
in `AppendState`'s schema, row and watermark maps. `AppendTableSchema` has no
cross-table foreign-key or unique-key contract. Define the write footprint F:

- Append(t, rows) and AppendGenerated(t, rows): F = {AppendTable(t)};
- CreateTable(t, schema): F = {Schema};
- An opaque WalOp::Append without typed preparation: F = {Database};
- A transaction's footprint is the union of its operations' footprints.

Represent an overlap by either an equal identity or either side containing a
Schema/Database barrier. For any two supported non-DDL append operations whose
mutable resources overlap, both operate on the same t, hence share
AppendTable(t). Any schema change overlaps all concurrent writers through the
barrier. Union preserves this implication for arbitrary finite transactions:
if a pair of operations conflicts, their identity witnesses are included in
the two transaction unions. This is a sufficient conflict footprint, not a
minimal one; caller-provided rows in disjoint partitions remain conservative.

Under the version-index theorem, the first commit stamps every member of F at
its commit epoch. A later transaction with an older read epoch and an overlapping
F fails before WAL append. The existing two-direction barrier check additionally
rejects a broad writer after a narrow commit and a narrow writer after a broad
commit. For a footprint containing Schema plus table identities, the broad
writer check dominates: if the current epoch is not newer than the base, every
other stamp is also no newer. Thus that shape refines the broad-only abstraction
in `HawDBMvccValidation`, rather than needing a new weaker validation rule.

## Source correspondence and allocation

`commit_prepared_mutation_ops` starts one bounded VersionWriteSet from final
graph operations. Relational staging adds its conservative Database identity.
`collect_append_version_writes` extends that same set from the materialized
AppendTransaction which is passed directly to `encode_append_wal_batch`.
Generated writes become ordinary rows during preparation but retain their
exact table name, so the transformation preserves F. Collection does not decode
or retain an extra copy of the WAL payload. A count-limit failure occurs before
WAL append and canonical publication; partial local collection is discarded.
The generic recursive WAL collector still maps opaque append records to Database.
New AppendWrite variants require extending the exhaustive match.

Preparation runs against the current canonical `self.append_state`, under the
commit sequencer, not against the transaction's old private append snapshot.
For distinct t and u, applying a prepared mutation to R(t) preserves current
R(u). This matters as much as conflict validation: publishing an entire stale
snapshot would lose the other table's rows despite disjoint identity sets.
The integration tests assert both payloads and order keys after reopen.

For generated order, let N(t) be the canonical next value. Preparation computes
a prospective interval from N(t), but does not mutate canonical state. Validation
failure drops that preparation without publishing its rows or watermark. A retry
therefore uses the current canonical N(t), with no gap caused by the failed
attempt. A successful interval advances only t's watermark. Projecting the
serialized WAL onto any fixed t yields the one-table history checked by
`HawDBGeneratedAppendOrder`: operations on other tables stutter in that projection.
Induction over commits preserves the conjunction of the per-table allocation
invariants. A complete global WAL prefix projects to a prefix for each table;
atomic batch recovery is still required, as in `HawDBAppendMixedTransaction`.
These are conditional refinement arguments, not a combined machine-checked
Rust/resource/filesystem proof.

## Verification and limits

Run the existing per-key, generated-order and mixed-publication models:

```sh
bazel test //docs/tla:HawDBMvccValidation_check //docs/tla:HawDBGeneratedAppendOrder_check //docs/tla:HawDBAppendMixedTransaction_check
scripts/check-storage-tla.sh --check-mutants
```

The per-key model's two ordinary keys can denote two AppendTable identities;
its full-history oracle, broad barriers, pinning, pruning and restart arguments
apply unchanged. The generated-order model remains explicitly one-table; its
single scalar allocator is not a global allocator for all tables. Neither
model proves complete source collection on its own: the footprint induction
above and executable cases supply that mapping.

Tests cover caller-provided and generated order, distinct tables, same-table
conflicts across different partitions, unchanged WAL on rejection, pinned
checkpoint cleanup, exact rows/keys after reopen, mixed-table atomic rejection,
Schema barriers in both commit orders, and independence from graph writes.
The collector test checks deduplication, an actual entry-limit rejection and the
opaque-record fallback. Restoring the old Database footprint must fail the
same-snapshot disjoint-table regression.

No WAL format, persisted version format or public API changes. Historical COW
maps can retain version metadata; the new String-bearing table identities use
the existing entry cap and page-weight accounting, not a new global byte bound.
Relational identities, generated-order partition concurrency, optimistic read
skew and whole-transaction fairness remain separate work.
