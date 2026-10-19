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

## Resource refusal metadata

Memory-ledger, operator-tracker and spill-quota admission failures expose
`HawDBError::ResourceBudgetExceeded`. Rust, FFI JSON, and Python preserve the
resource kind, refusing scope, owner, memory class, additional request, current
usage, effective budget, and `budget_option`. Rejected reservations do not charge
the account. Query-level ownership transfers report their net additional charge.

Sort, TopN, Distinct and aggregation retain their blocking configuration origin
for tracker and oversized-item refusals. Derived single-item merge allowances
report the actual smaller limit (for example, one third of the blocking budget
for partial aggregation), with the whole item as `requested` and zero `used`.
Tracker-growth refusals report the additional charge and current tracker usage.
Spill decode reports usage before releasing its temporary decoded state on error.
Counter overflow is a refusal even when the configured limit is `usize::MAX`.
Generic tracker constructors identify their explicit argument, rather than
inventing a connection to the database configuration. The public
`merge_sort_runs(memory_budget)` entrypoint uses the explicit
`OperatorMemoryTracker::with_account(budget_bytes)` origin for its merge-state
tracker; configured Sort/TopN paths retain their blocking configuration origin.
Corrupt spill records and
invalid ownership transfers remain ordinary execution errors.

When a typed resource refusal escapes a contextual morsel worker or its ordered
consumer, the scheduler signals `RuntimeCancellationReason::ResourceExhausted`
before joining its in-flight workers. Workers checking that same context stop
cooperatively; the scheduler returns the original resource error and its metadata,
not the secondary cancellation error. Memory, spill, descriptor exhaustion, read
budgets, and graph expansion limits participate. Invalid resource configuration
and ordinary callback errors do not signal resource exhaustion. A reservation
refusal handled inside an operator (for example, by spilling) does not cancel it.
Typed blocking-operator refusals now participate in this sibling cancellation;
spill decisions made through `would_exceed` before charging remain handled pressure.
The Tokio operation wrapper also preserves an operation error after a resource
stop; an operation that returns success after that signal is still rejected.
Explicit user cancellation and deadlines keep their existing precedence.

The numeric parallel pipeline uses a child task context, including for raw calls
without a host context, so sibling failure does not poison a reusable caller
context. Direct scheduler users should likewise pass a child if the parent is
reused. `cancel_for_resource_exhaustion` preserves the first local cancellation
cause and wakes descendant waiters; generic cancellation telemetry includes it
in the cancellation count. Generic `map_ordered` APIs still materialize every
callback result; they do not interpret a nested `Result`. Callbacks that do not
check a context cannot be interrupted while executing. Ordered streaming may
already have delivered a validated prefix before failure; it never reports that
prefix as a successful complete query.

Classify errors by `resource` and `scope`. `budget_option` identifies the effective
boundary's origin; it is an extensible diagnostic string, not a closed enum or
an instruction to raise a setting automatically. The explicit
`new_with_budget_option` and `account_with_budget_option` constructors also accept
caller-defined static strings. Hosts must tolerate unknown origins while retaining
the typed classification and numeric fields. These are the current built-in
origins:

| `budget_option` | Effective boundary |
| --- | --- |
| `ExecutionMemoryConfig.query_memory_bytes` | Configured query root, source account, retrieval pipeline, or result-memory limit. |
| `RuntimeTaskContext.memory_reservation.memory_bytes` | Runtime-admitted query root; increasing configuration does not widen this grant. |
| `RuntimeTaskContext.memory_reservation.result_bytes` | Runtime-admitted result-memory limit when it is no larger than the configured query-memory limit. |
| `ExecutionMemoryConfig.blocking_operator_bytes` | Configured blocking-state limit or a derived operator/merge-item allowance; `budget` reports the effective limit. |
| `ExecutionMemoryConfig.batch_payload_bytes` | Output batch allowance; a single expansion row exceeding it is an operator-local per-row refusal. |
| `OperatorMemoryTracker::new(budget_bytes)` | Explicit unaccounted operator limit. |
| `OperatorMemoryTracker::with_account(budget_bytes)` | Explicit tracker limit, independent of its backing account limit. |
| `QueryMemoryLedger::new(budget_bytes)` | Explicit query-root constructor argument. |
| `QueryMemoryLedger::account(budget_bytes)` | Explicit account constructor argument, including derived limits. |
| `KnowledgeRetrievalPipelineBudget::new(query_memory_budget)` | Explicit retrieval pipeline root and account allowance. |
| `QueryMemoryAccount::sub_account(budget)` | Child allowance reserved in its parent before dispatch. |
| `QueryMemoryAccount::sibling(budget_bytes)` | Explicit sibling account allowance. |
| `ExecutionMemoryConfig.max_spill_bytes` | Cumulative bytes written by one spill operator. |
| `ExecutionMemoryConfig.max_spill_runs` | Cumulative runs created by one spill operator. |
| `ExecutionMemoryConfig.max_total_spill_bytes` | Shared live spill bytes, including pending reservations. |
| `ExecutionMemoryConfig.max_total_spill_runs` | Shared live spill runs. |
| `ExecutionMemoryConfig.min_spill_free_bytes` | Filesystem reserve to preserve; `budget` reports probed available space minus this reserve. |

Result memory uses the smaller of the configured query-memory limit and the
runtime result grant. Equal limits report the runtime grant: raising only the
configuration cannot widen it. The retrieval graph-read allowance comes from
`DatabaseConfig.execution_memory.query_memory_bytes`; its output payload limit
is a separate `ReadBudgetExceeded` boundary.

Shared limits may have been tightened by another query using the same spill
directory. Filesystem `used` counts reservations not reflected in the space
probe, rather than all occupied disk space. Raising the minimum reserve reduces
usable space. `used + requested` can overflow `u64`; hosts must not rely on an
unchecked sum to interpret the refusal.

Existing descriptor errors retain their separate type. Operator-local checks
that have not been converted may still return `Execution`; this metadata contract
does not claim that every execution limit uses the new variant.

An expansion row that cannot fit an empty output batch reports `scope: Operator`,
`requested` equal to that row's retained bytes, and `used: 0`. The boundary is the
row itself, independent of any rows already buffered. There is no separate
per-item scope; the message identifies this per-row ceiling explicitly. Releasing
other account ownership or retrying the unchanged row cannot satisfy it. Raising
`batch_payload_bytes` still does not widen a smaller query or runtime grant.

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
