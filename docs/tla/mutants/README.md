# Storage and Query Memory TLA+ Negative Controls

These configurations enable one deliberately unsafe transition in a storage
or query-memory model. They are not production specifications and must never enter the
positive `storage_models` suite. `scripts/check-storage-tla.sh --check-mutants`
runs each configuration and succeeds only when TLC reports the invariant named
in `mutants.txt`.

The negative controls prove that the bounded models exercise these failures:

- visibility before WAL durability;
- duplicate or out-of-order partition appends;
- partial batch recovery;
- manifest publication before segment durability;
- reclamation of a reader-pinned generation;
- partial publication of a mixed graph/row/append transaction;
- child admission that exceeds the query root;
- releasing a child reservation while a lease still owns its capacity.

The per-key MVCC controls additionally reject skipped validation, either missing
broad-barrier direction, premature history cleanup, ignored retained source
snapshots, index reset with live transactions, unsafe current-epoch compaction,
and publication before sync. See
[the model proof and scope](../MVCC_VALIDATION_PROOF.md).

The governed-writer progress control rejects younger admission past an older
full-capacity writer. Its proof also records a separate temporal starvation
counterexample when the safety assertion is omitted; see
[the progress proof](../GOVERNED_WRITER_PROGRESS_PROOF.md).

The constrained-insert controls check omission of unique stamps and spurious
conflicts from NULL unique values or shared foreign-key parent reads. See the
[constrained-insert proof](../CONSTRAINED_INSERT_MVCC_PROOF.md).
