# UNWIND bootstrap acceptance

This records the HawDB-side contract for
[issue #298](https://github.com/nowledge-co/hawdb/issues/298). It does not complete
the issue's Mem importer migration or its representative performance acceptance.

## Transaction boundary and WAL

Compare these two implementations of a page within one `DatabaseTransaction`:

- execute one parameterized `MERGE` for each row, then commit;
- execute one `UNWIND $rows AS row MERGE ... ON CREATE SET ...`, then commit.

Both stage graph operations privately and publish through the same transaction
commit. The first implementation does **not** append a durable transaction for
every statement. Therefore N-to-1 statement reduction alone does not imply
N-to-1 WAL append reduction for the existing Mem bootstrap path. Autocommit
per-row queries are a different baseline.

`unwind_transaction_matches_sequential_merges_and_wal_for_repeated_keys` compares
eight deterministic 32-row pages, each containing eleven keys, repeated keys,
an existing destination node, NULL names and list properties. It checks:

- equal staged and recovered query results;
- preservation of the existing node and first-creation semantics;
- no WAL byte changes before commit and one commit-epoch increment per page;
- byte-identical committed WAL files between the two transaction strategies.

This is evidence about persisted output, not a syscall-count measurement or a
performance benchmark. It does not assert a universal byte-identity contract
across storage formats or configurations.

## Ordered MERGE equivalence proof

Scope: a finite ordered page with valid, unique destination keys; each row
provides a key and `ON CREATE` values. Multiple source rows may repeat a key.
The assignments do not overwrite the match key. There are no `ON MATCH`
assignments, intervening reads or concurrent writes within the private
transaction. Both executions and their final commit must satisfy their
respective resource limits. This proof does not assert equal acceptance under
different statement-level budgets or equivalence to the importer conflict check.

Let S be the initial map from keys to nodes. Let R be the ordered source rows.
Define F(S, r) to preserve S when r's key exists; otherwise it adds exactly one
node with the row's key and creation values, using the next node identifier.
The sequential reference computes the left fold of F over R.

For the batch implementation, after resolving k rows let P_k be its pending
nodes and O_k its pending WAL operations. The invariant is that applying O_k
to the initial state yields the reference fold over the first k rows, including
node identifiers, and P_k contains exactly the newly created nodes in order.

Base: k = 0 has empty pending nodes and operations and the original allocator.
For the next row there are three exhaustive cases:

1. Its key exists in the initial state. Both paths preserve that node.
2. Its key was created by a prior row. The batch's pending-node lookup finds
   the same node the sequential path now reads; both preserve it.
3. Its key is absent from both. Both allocate the same next identifier and
   emit the same create operation and values.

Each case preserves the invariant. Induction proves equal final graph state
and ordered create operations for every finite admitted page. Repeated keys
with differing creation values retain the first creation, rather than the last
row's values. `commit_mutations_internal` in
`crates/storage/src/store/graph_commit.rs` implements the existing/pending/new
cases; `materialize_unwind_mutations` in
`crates/executor/src/mutation/preflight.rs` preserves input order.

This is a source-linked deductive proof, not a machine-checked proof of Rust.
The tests exercise the implementation against an independently executed
sequential reference; they do not replace the induction or storage recovery
tests.

## Rejection and statement savepoints

Input admission and row resolution finish before staging. The transaction
executor takes a statement savepoint before materialization and restores it on
error. Thus a rejected batch returns to the pre-statement state, which may
already contain earlier accepted statements. It does not roll back those
earlier statements or append a durable prefix of the rejected batch.

`unwind_transaction_rejection_preserves_prior_statement_and_wal` stages a prior
node, rejects an over-budget batch, observes only the prior node and unchanged
WAL, then commits a valid retry and checks recovery. The existing
`public_unwind_mutation_batches_direct_and_transactional_bootstrap_rows` also
covers a valid row followed by an invalid row and a successful later retry.

## Remaining Mem acceptance

The inspected Mem Entity importer calls `apply_node` per row within one
transaction. That helper first checks destination fields: equal existing data
is accepted, differing data is rejected. Plain `MERGE ... ON CREATE SET` alone
would silently preserve a conflicting existing node, so it is not an equivalent
replacement for the helper. A migration must retain this check, source schema
and identity validation, row/payload/time budgets, and atomic cursor publication.

Before closing #298, migrate at least one real page importer, compare conflict
and resume behavior against its current implementation, and measure release
wall-clock and actual statement/WAL counts on representative pages. Include
fresh, already-imported and conflicting destinations. Report unchanged WAL
counts honestly when both variants already commit once per page. No production
cutover or Mem release-policy change is part of this acceptance work.
