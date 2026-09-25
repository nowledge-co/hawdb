# Point updates that preserve constraint keys

The Mem content contract has document ownership uniqueness and foreign keys from
messages/chunks/anchors to documents. Updating a message's content or a chunk's
text does not change those keys, but previously every update to these tables
used a Database barrier. The classifier now admits complete-primary-key UPDATE
when every assigned column is outside the union of:

- primary-key columns;
- columns of every UNIQUE constraint and declared unique index;
- local columns of every outgoing foreign key.

This is a syntactic assignment test, independent of current row values. Even
`SET owner = owner` remains broad when owner is constrained. The point predicate
must still satisfy the [necessary-equality proof](PRIMARY_KEY_PREDICATE_MVCC_PROOF.md).
Changing constraint keys, primary-key assignments, constrained DELETE/Replace,
UPSERT and predicates without a complete structural key remain broad. This is a
sufficient eligibility rule, not a claim of maximal conflict precision.

## Preservation argument

Let C(t) be that union for table t. For any row r and an admitted assignment list
A, every assigned column lies outside C(t). Therefore projection of the result
onto C(t) equals projection of r, irrespective of the values computed by A.
This follows componentwise: the mutation changes only assignment targets, and
no target belongs to C(t). It holds for composite/overlapping definitions and
for all intermediate steps of repeated updates. Row absence is also preserved:
UPDATE neither creates a missing row nor deletes an existing row.

Consequently the sets of primary and unique values, every outgoing FK tuple and
the existence of all referenced rows are unchanged by such an update. Incoming
FKs must target a primary or unique key: canonical relational schema validation
checks `unique_index_definition(&foreign_key.referenced_columns)` and rejects
non-unique targets. Preserving all primary/unique columns therefore preserves
incoming targets too. If the schema language later allows a different reference
target or new cross-row constraint, this eligibility argument must be extended
before that feature can use this path.

A stable target row implies stable residual evaluation and row-local assignment
inputs at canonical replay, by the point-predicate proof. Since its constraint
projections and row existence are unchanged, publishing the update cannot
invalidate constraints on another row. Non-unique postings may change, but
existing canonical staging rebuilds them using primary identities rather than
installing a stale private index root. The update claims its primary row identity
only; it does not introduce false conflicts by stamping unchanged unique values
or a shared FK parent read. Another write to that row still rejects by the normal
first-committer-wins rule, including absent-row and false-residual intents.

This composes with pure Error-mode inserts. Inserts claim new primary and
non-NULL unique identities; updates preserve their existing values. Inserts
cannot steal an existing row's unique value from a valid snapshot. A previously
missing parent needed by an update cannot silently appear in private execution;
private staging still validates FK existence. Any operation that removes or
changes a reference target remains broad, so both existing barrier directions
protect readers of that reference. No constraint check is bypassed.

## Transaction classification and publication

`RelationalVersionTable::preserves_constraint_keys` is the AND of eligibility
across every operation on a touched table. Pure inserts and these point updates
can keep it true; Delete/Replace and key-changing updates clear it. Local
constraints cause immediate broad fallback for a non-preserving operation, and
the incoming-reference scan also rejects any touched non-preserving table.
Classification finishes before relational keys are inserted. Thus the first
safe statement cannot hide a later unsafe statement, nor can the reverse order
restore eligibility. Other unsupported operations return Database immediately.

The existing entry/payload admissions, history pinning, validation-before-WAL,
canonical constraint staging, durability and recovery protocols are unchanged.
Per-key identity does not remove staging scans or promise bounded total process
memory. Range writes, general constraint-changing footprints, unrestricted retry
fairness and current-head performance qualification remain separate work.

## Finite projection model

`HawDBConstraintPreservingUpdate.tla` enumerates old and replacement values for
five column roles (primary, UNIQUE constraint, declared unique index, FK and
ordinary body), plus every nonempty assignment-target subset. Two values per
column yield 31,744 input cases. An explicit check step gives **63,488 distinct /
63,488 generated states**, depth 2 and an empty queue. The model independently
compares the primary/unique/FK projection before and after every admitted update.
Ignoring unique columns or FK columns must violate `ConstraintProjectionPreserved`.
Named witnesses reach a real admitted body change and a rejected projection
change, excluding a vacuous all-reject classifier.

This model proves a finite assignment/projection lemma, not transaction liveness,
a composite SQL parser proof or a composed durability theorem. The general
componentwise argument above handles arbitrary schema lists and values. The
point-predicate, intent and MVCC models retain their separate validation scope.

```sh
bazel test //docs/tla:HawDBConstraintPreservingUpdate_check
scripts/check-storage-tla.sh --check-mutants
cargo test --locked -p hawdb --lib relational_mvcc_constraint_preserving_updates
```

## Executable boundary and recovery checks

One integration test uses 11 scenarios in each of Materialized,
OutOfCore/materialized-index and OutOfCore/Authoritative configurations. It
checkpoints/reopens before writers and explicitly asserts metadata-only row
residency for the Authoritative case. Parent ownership uniqueness, child compound
uniqueness, a declared unique index, outgoing FK and incoming FK to a non-primary
unique column are present together.

The scenarios cover disjoint multi-table body updates, same-row atomic rejection,
both insert/update orders in one table, and both commit orders against writes to
UNIQUE, declared-index and FK columns. Each broad transaction mixes a body update
with a constraint assignment, exercising both statement orders on the same table.
They assert typed conflict identity,
unchanged rejected WAL and rows, exact successful row values/epochs, stable old
snapshots across checkpoint, exact reopen state and new same-row update conflicts
after restart. Reinstating the old broad constrained-update classification makes
the disjoint case fail; that source probe is restored before positive checks.
