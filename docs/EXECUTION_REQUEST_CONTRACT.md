# Embedded execution request contract

The host-facing entrypoints are `hawdb::executor::execute_with_request` and
`execute_with_request_consumer`. `ExecutionRequest` borrows a physical plan,
parameters, memory configuration, and optional task context. `ExecutionResources`
borrows the catalog, execution store, and host external-read operator. No helper
process, environment variable, internal-crate dependency, or global context is
needed. The facade also exports the external-read report and enum types needed
to construct a `VectorSeedExecutionOutput`.

## Compatibility policy

The fifteen older `execute_with_*` overloads remain deprecated compatibility
wrappers. Existing production and benchmark callers use the request contract;
the existing deprecated-API test module deliberately continues exercising all
fifteen wrappers. Deprecation is source-level guidance, not permission to remove
these entrypoints in an unrelated change. The short `execute` helper remains.

Parameters, output limits, explicit memory limits, task cancellation/deadline,
external reads, row order, and deterministic profile fields retain their existing
meanings. Defaults omitted by a wrapper are reconstructed before comparison.
The parity matrix covers all fifteen overloads and compares complete profiles
except process-wide RSS/page-fault samples, which naturally vary between calls.
External wrappers additionally compare the host's observed embedding, filters,
context presence, working/result budgets, and parallelism.

One deliberate distinction must not be hidden by a blanket equivalence claim:
legacy consumer overloads with **both output limits absent** deliver rows as they
are produced. The request consumer always retains output until execution and
admission finish, even without explicit output limits. Consequently a late
execution/admission error may expose a prefix through the old unbounded API but
never through the new request consumer. The latter may require more result
memory. No incremental-delivery switch is exposed as the default convenience API.
The regression `unbounded_legacy_delivery_remains_incremental_but_requests_validate_first`
checks this boundary with a successful first row and a later division-by-zero.

## Delivery and accounting proof

Let accepted rows be the sequence A, its payload charge P, its result-memory
charge M, and observable callback rows be E. During collection the public request
consumer uses `DeferredUntilValidated`. Before accepting row r,
`QueryOutputAccumulator::emit` checks the next row count, next payload total, and
result-account reservation. Only successful admission appends r to A. Thus,
by induction on accepted rows, A is an input prefix and all configured limits
hold. E remains empty because this branch never invokes the callback.

`execute_profiled_consumer` calls `finish_delivery` only after the entire plan
execution succeeds. This transition establishes validation of the complete
input before the first callback. A collection, source, output-limit, or memory
error returns through `?`, dropping A and its lease without calling the consumer.
This proves no unvalidated prefix escapes. The materializing API returns its
row builder only on success, so its private accumulation does not expose partial
results either.

After validation, `finish_delivery` drains A in order, checking the task context
before and after each callback. By induction on drain iterations, E is an ordered
prefix of A. Callback failure, panic, or cancellation can leave that validated
prefix observable; callbacks may perform irreversible side effects, so these
APIs do **not** promise callback rollback. No further callback runs after the
error propagates. On success all deferred charges reset before profiling. Error
or unwind drops the remaining buffer and RAII lease. The root/local admission
invariant is independently covered by `HawDBQueryMemoryLedger`.

The materialized profile's completion charge is sampled while the accumulator
still owns its result reservation. Returned `QueryRows` do not carry a query
ledger lease; the execution scope releases it after sampling. Therefore a
nonzero materialized completion counter is evidence of admitted ownership at
that snapshot, not a promise of ongoing host-result accounting. A successful
request consumer has a zero completion charge after validated delivery. Neither
counter bounds process RSS or memory subsequently retained by host callbacks.

## Model and evidence boundaries

`HawDBValidatedResultDelivery.tla` models collection, whole-input validation,
ordered delivery, late admission failure, source failure, cancellation, callback
failure, and terminal reservation release. Its finite instance varies row,
payload, and memory caps independently across three unequal-sized rows. It
checks `NoUnvalidatedDelivery`, `OrderedPrefix`, `ValidationCoversWholeInput`,
`TerminalReleasesResult`, and `SuccessIsComplete`. The early-callback mutant must
violate `NoUnvalidatedDelivery`. This complements, rather than replaces, the
existing memory-ledger model; it does not model OS allocations or an arbitrary
query operator's semantics.

TLC exhaustively checks this configured finite instance. The induction above
covers arbitrary finite row sequences subject to the stated implementation
premises. Neither is a machine-checked refinement proof of Rust. Revisit both
when changing accumulator modes, admission order, callback timing, or profiling.

`tests/execution_request_contract.rs` imports only the public `hawdb` facade.
It checks independent row oracles across cardinalities, parameter payloads,
batch sizes, exact/insufficient result limits, cancellation, deadlines, memory
denial, callback failure, resource reuse, and successful host vector reads with
profile evidence. `src/executor/tests/request_contract.rs` compares all fifteen
compatibility overloads, including error text, visible callback prefixes and
engine accounting. Both are included by existing Cargo and Bazel targets.
