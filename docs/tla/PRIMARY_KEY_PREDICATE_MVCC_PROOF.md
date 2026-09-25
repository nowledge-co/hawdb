# Complete primary-key predicates and MVCC replay

SQL UPDATE and DELETE compile to predicate operations even for `WHERE id = $1`.
Treating every predicate as a Database barrier consequently rejects unrelated
point updates. `relational_predicate_primary_key` derives a row identity from the
bound predicate, without executing a query or inspecting its current matches.
The original operation is still staged/replayed; neither its WAL encoding nor
its statement result contract changes.

## Necessary equalities

Traverse only AND nodes from the root. An equality leaf reached by that traversal
is necessary for the entire predicate to be TRUE. Never traverse OR or NOT.
Other leaves and residual subtrees contribute no binding. Require one consistent,
non-NULL equality with the exact schema scalar type for every primary column;
assemble the key in declared primary-key order. Repeated identical equalities
are accepted; contradictory equalities, missing components and invalid types
use the broad fallback. Thus syntactically different conjunction order cannot
produce different key identities.

The structural argument is induction on the traversed AND tree. A TRUE leaf
`column = value` implies that equality, and a TRUE conjunction implies TRUE in
both children. Every accepted primary component therefore has its specified
value in every matching row. The uniqueness of a complete primary key makes
the matching set a subset of the singleton extracted key, independently of
other rows, insertion order or the current snapshot. Ignored residuals can
reduce that set but cannot enlarge it. This also holds with SQL three-valued
logic: only TRUE matches WHERE, and TRUE AND requires two TRUE operands.

For example `(id = 1 OR id = 2)` remains broad; `id = 1 AND (body = 'a' OR
body = 'b')` may qualify. A component hidden only inside OR/NOT never supplies a
necessary binding. This is a sufficient syntactic proof, not a complete theorem
prover for equivalent SQL predicates.

## Stable replay and intent retention

Qualified updates must not assign any primary-key column, including a syntactic
self-assignment. DELETE uses a tombstone intent; UPDATE uses a live intent. Both
claim the extracted identity even when the row is absent or a residual is false.
A successful concurrent insert/change/delete at that key therefore rejects the
stale operation before replay, including a write that would turn a false residual
into TRUE. No matching-row scan or net-difference capture can replace this intent.

Suppose validation succeeds. Existing per-key MVCC guarantees that the target row
has not been written since capture, and both Database/Schema barrier directions
exclude an untracked destructive or schema change. Other rows cannot match the
predicate. The target's presence and value therefore agree with the captured
state; the row-local residual and assignments see the same inputs at replay.
For several statements, induct on statement order: earlier successful qualified
operations apply the same row-local changes in the workspace and canonical
replay. The collector retains the union of their input identities, including
net-zero effects. Existing explicit-key insert/delete operations compose by the
same argument. Any unsupported operation makes the entire transaction broad.

DELETE still excludes secondary uniqueness and outgoing/incoming FK dependencies.
UPDATE can now admit constrained/referenced tables when every assigned column
preserves all primary/unique/FK projections; see the
[constraint-preserving refinement](CONSTRAINT_PRESERVING_UPDATE_MVCC_PROOF.md).
Key-changing work on those tables remains broad. Pure constrained inserts retain
their separate eligibility and proof. Non-unique postings are rebuilt against
current canonical state; stale private index roots are not installed.

This proves row-local replay under existing staging/admission success. It does
not remove the stager's scans, guarantee identical resource costs after unrelated
inserts, prove general range/UPSERT/constraint footprints, or provide serializable
read-set validation. The classifier's temporary traversal stack and key clones
are not total-RSS admission; stored identities retain existing entry/byte caps.

## Finite model and executable evidence

`HawDBPrimaryKeyPredicate.tla` enumerates two keys, two non-NULL values and absent
rows, with equality atoms, AND/OR/NOT and an additional outer AND layer. It checks
that every selected row lies inside an extracted singleton and that preserving
claimed rows preserves predicate selection across arbitrary changes elsewhere.
It enumerates **14,904 initial cases / 29,808 distinct and generated states**,
with an empty queue and depth 2. One explicit check step per case lets the
negative-control verifier require a transition invariant violation. This is
finite expression/state enumeration, not a concurrent commit-state model. The existing
`HawDBRelationalWriteIntent` and MVCC models separately cover stamp validation;
the source argument above connects their footprint premise to predicate replay.
NULL and composite ordering are covered by the source argument and Rust tests,
not by this non-NULL, single-component finite instance.

Two registered negative controls extract a key from only one OR branch or omit
an absent-row intent. They must violate `KeyBound` and `ReplayStable`, respectively.
Named reachability checks `NoResidualWitness` and `NoDisjointWitness` demonstrate
that the enumeration contains changing predicate results and unrelated-row
changes with a stable selected target.

```sh
bazel test //docs/tla:HawDBPrimaryKeyPredicate_check
scripts/check-storage-tla.sh --check-mutants
cargo test --locked -p hawdb --lib relational_mvcc_primary_key_predicates
cargo test --locked -p hawdb-storage relational_mvcc_primary_key_predicate
```

The integration test uses composite primary keys in Materialized,
OutOfCore/materialized-index and OutOfCore/Authoritative configurations. It
checkpoints/reopens and asserts metadata-only residency before the latter's
writers start. Disjoint updates, delete/update, absent update/insert and
insert/absent-delete, false residual intents and residual OR are checked with
old snapshots, exact rows/epochs, checkpoint, unchanged WAL on conflict and
reopen. The collector test checks repeated/conflicting equalities, OR/NOT,
NULL/type mismatches, range/partial keys, schema key ordering and primary-key
assignment fallback. Temporarily restoring broad predicate classification makes
the disjoint integration case fail with a Database conflict; the probe is restored
before final positive checks.
