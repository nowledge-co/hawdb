# Incremental mutation-result admission

This change was measured against PR #986 parent `72f4efa0`. It does not
change mutation limits, public APIs, WAL bytes, transaction publication, or
durability. The full check immediately before prepared mutation publication
remains in place.

## Equivalence argument

Let `w(r)` be the existing saturating sum of result-column name lengths and
`estimated_value_bytes` within row `r`. Let `S(R)` be the saturating sum of
`w(r)` for every row in the result prefix `R`.

The tracker starts at `(checked_rows, payload_bytes) = (0, 0)`. After each
successful check its invariant is `(len(R), S(R))`. Mutation preparation only
appends owned rows; it neither removes nor changes previously appended rows.
For an extension `R ++ D`, nonnegative saturating addition is associative,
so `S(R ++ D) = saturating_add(S(R), S(D))`. The tracker computes exactly that
value by visiting only the unchecked suffix. It updates its state only after
all checks pass. A preparation branch that skips the outer check leaves its
rows in the unchecked suffix for the next check or the final full check.

Operation count, affected-row count, result-row count, and payload checks run
in the same order as before, with unchanged errors and thresholds. Rejection
still precedes WAL append and publication. The final complete scan guards
other callers and the prepared transaction boundary independently of tracker
state.

For `n` one-row mutations, the old outer checks visit `n(n+1)/2` rows. The new
outer checks visit each appended row once, followed by the existing final
linear scan. Tracker state is two scalar fields; it adds no result owner,
payload buffer, or heap allocation. This is a logical admission equivalence,
not a whole-process memory bound or an end-to-end zero-copy claim.

## Evidence and limits

The parent ordinary producer was sampled with macOS `sample`. Its 10k file
fixture completed with checksum parity. The separate 100k fixture was refused
by the unchanged 16 MiB query-result budget; its sampled preparation stacks
can locate CPU work but cannot qualify a successful 100k query or its latency.
The sampling window includes startup and all observed phases and is not a
phase-isolated CPU profile.

The prefix regression compares incremental and complete checks across nested
values, count limits, exact payload thresholds, and one-byte-under thresholds.
The graph regression checks refusal before state publication and an exact-limit
retry with preserved node IDs. Compilation does not establish runtime results;
those require explicit current-source CI coverage after delivery.

The ordinary paired producer control covers bulk, point, and select at 1k and
10k on memory and file backends, without allocator instrumentation or weakened
budgets. It is a parent/prototype ARM64 Cargo control, not complete Linux,
foreign-language, graph/FTS, sequential-write, wide-result, memory or tail
qualification. Retain every negative outcome and do not infer comprehensive
acceptance from a bulk improvement.

The recorded ordinary control contains 240 raw outcomes (216 measured, 108 alternating pairs). Write-boundary median speedups for 1k memory/file are 1.07948 / 1.25546, and for 10k memory/file 2.60379 / 2.55005. Point memory elapsed controls regressed to 0.95935 / 0.97505; select elapsed controls include 0.97988 (1k memory), 0.96892 (10k memory), and 0.97549 (10k file). These negatives remain acceptance failures; no noise explanation is assumed. Full raw records and source identities are in [archived incremental_mutation_limit_controls_20261012.json](../bindings/benchmarks/evidence/README.md#incremental-mutation-limit-controls-20261012).
