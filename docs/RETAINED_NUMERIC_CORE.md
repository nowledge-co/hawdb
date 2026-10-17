# Experimental retained numeric cursor

This is the storage, executor and Rust ownership portion of
[issue #987](https://github.com/nowledge-co/hawdb/issues/987). The host integration
surface remains the root `hawdb` crate. It introduces no crate or durable format.

`Database::query_with_params_retained`, the corresponding session and embedded
methods, and `DatabaseReadTransaction::into_retained_query` expose serialized,
demand-driven numeric batches. Eligibility follows the existing single-label
numeric scan/filter/project plan and catalog types. Unsupported plans, layouts,
types and strict source-reuse requests fail explicitly. The ordinary owned query
APIs remain available.

The producer constructs numeric values, validity, identities and selection in
admitted execution buffers. Sealing transfers their allocations into immutable
owners. Allocation identity, generation, byte range and pointer provenance are
observable through the batch contract. This preserves produced allocations;
it does not expose canonical persistent pages directly.

## Ownership and admission

Materialized sources capture an immutable COW node map and read-health state.
They do not clone the entire store or retain unrelated indexes, relationships,
mutable transactions or file leases. Conservative capacity preflight covers the
captured source, including recursive values and cold record inspection. Invalid,
overflowing or excessively nested capacity estimates refuse before publication.
The catalog removes an imported label's previous name key before replacing it.

One retained runtime is shared by a database and its read snapshots. The runtime
governor accounts for unique buffer owners, source capacity and independent view
handles. Reopening cursors cannot create fresh allowances. A retained view stays
charged after its cursor or database closes and releases when its final owner
drops. Result admission is separate from ordinary work permits.

Pulls do no speculative prefetch. The default two-slot bound, byte ceilings and
handle ceilings apply before source advancement. Recoverable backpressure leaves
the next source position available for retry after a view is released. Oversized
units and terminal execution, cancellation or result-budget failures are explicit.
Previously returned batches are provisional until successful completion; repeated
pulls after failure must preserve that failure. Closing a cursor releases source
pins without revoking independently retained batches.

Session `max_result_rows = 0` means unlimited. Host `None` and `Some(0)` likewise
supply no row ceiling. Positive session/host ceilings and explicit per-cursor
limits combine restrictively; they cannot raise one another. Payload and memory
budgets remain independent. See [result limits](READ_RESULT_LIMITS.md).

## Qualification scope

Unit and integration coverage checks value/order parity, integer and IEEE
boundaries, null validity, duplicate-column identity, source snapshots, exact
release, cancellation, backpressure, runtime accounting and session-limit capture.
The manual `retained_source_capacity` benchmark reports cold preflight work and
source capacity independently of cursor creation and consumption timing. It
accepts 1–10,000 rows and either memory or a fresh file path, preserving the
power-loss-safe default durability.

Foreign bindings, Arrow export and indexed node views are separate subsequent
deliveries. Full-operation planning/foreign workspace accounting and the final
performance, RSS, conversion, allocation and tail-latency matrix remain open in
[#986](https://github.com/nowledge-co/hawdb/pull/986). This foundation alone does
not satisfy that PR's complete acceptance or justify a general zero-copy or
whole-operation bounded-memory claim.
