# Append Storage TLA+ Negative Controls

These configurations enable one deliberately unsafe transition in an append
storage model. They are not production specifications and must never enter the
positive `storage_models` suite. `scripts/check-storage-tla.sh --check-mutants`
runs each configuration and succeeds only when TLC reports the invariant named
in `mutants.txt`.

The negative controls prove that the bounded models exercise these failures:

- visibility before WAL durability;
- duplicate or out-of-order partition appends;
- partial batch recovery;
- manifest publication before segment durability;
- reclamation of a reader-pinned generation;
- partial publication of a mixed graph/row/append transaction.
