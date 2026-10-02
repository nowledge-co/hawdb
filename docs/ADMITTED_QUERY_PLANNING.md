# Admitted query planning

The synchronous `HawDBEmbedded` admitted query methods and all
`HawDBTokioEmbedded` query methods acquire a cheap `Control` permit before
parsing, optimizing, or capturing a planner snapshot. Raw `Database` APIs remain
available to hosts that provide their own admission boundary.

## Two admitted phases

1. Planning reserves one CPU slot and an estimated `1 MiB + source length` of
   memory. The estimate uses string length only, without parsing or traversing
   caller-owned parameter values. Tokio preparation runs on the existing bounded
   blocking lane. It captures an immutable catalog/store snapshot under the
   embedded mutex, then releases that mutex before parsing or optimizing.
2. Planning returns only a fixed-size admission descriptor. The prepared AST,
   optimized query, and reader pin are dropped before the planning permit is
   released. The execution queue retains the original caller input, not a
   separately allocated prepared plan. No task waits for an execution grant while
   holding a planning grant.
3. Execution admission uses the physical plan's CPU, I/O, memory, and result
   requirements, with at least one CPU slot and the planning memory estimate.
   Execution prepares again under that grant; Tokio reuses the shared template
   cache. This adds one parse compared with retaining a prepared query across
   admission, but keeps the reservation lifecycle explicit without a general
   permit-reconfiguration API.

These are governor reservations, not allocator/RSS measurements or hard parser
allocation limits. Persistent database-owned caches retain their existing entry
limits and lifetime. The default planning priority is foreground; an explicit
Tokio work request supplies its own planning priority. Statement-level hints
take effect after the admitted parse, when constructing execution admission.

## Host-owned process RSS feedback

Hosts can attach a shared `ProcessMemoryPolicy` with
`HawDBEmbedded::open_with_process_memory_policy(options, policy.clone())`.
Pass the resulting handle to `HawDBTokioEmbedded::from_owned` or `from_borrowed`
to preserve that policy in the Tokio entrypoints. Existing openers retain their
previous admission behavior. The additive opener leaves public options, the
library work-memory ceiling, and the persistent format unchanged.

The host chooses a process-wide resident limit independently of each instance's
`RuntimeGovernorConfig::memory_budget_bytes`. Clone one policy for every instance
that shares this limit; separate policies would each spend the same RSS headroom.
Both planning and execution must satisfy the library, host/cgroup, and process
policy bounds. Work already admitted keeps its permit when RSS rises. A successful
reservation is not an allocator cap and cannot prevent unrelated host allocations
or underestimated active work from exceeding the process limit.
The policy gates admitted query methods. Opening the database and raw `Database`
calls retain their existing host-managed resource boundaries.

```rust,ignore
let policy = ProcessMemoryPolicy::from_current_process(
    ProcessMemoryPolicyConfig::new(NonZeroU64::new(process_limit_bytes).unwrap())
        .with_recovery_headroom(8 * 1024 * 1024)
        .with_sample_max_age(Duration::from_secs(5)),
)?;
let embedded = HawDBEmbedded::open_with_process_memory_policy(options, policy.clone())?;
let runtime_config = TokioRuntimeConfig::from_governor(embedded.runtime_governor());
let embedded = HawDBTokioEmbedded::from_owned(embedded, runtime_config)?;
// Refresh from the host's existing timer, for example once per second.
policy.refresh_from_host()?;
```

Sampling remains owned by the host. Refresh at a bounded cadence shorter than
`sample_max_age`; neither the opener nor query admission starts a polling thread.
`refresh_runtime_resources` and Tokio's `resource_refresh_interval` refresh host
and cgroup resources, not RSS. They preserve caller-pinned resource snapshots;
explicit `update_runtime_resources` still applies while retaining the pin.
RSS policy updates remain independent of resource pinning and wake queued Tokio
work even when periodic resource detection is pinned off.

Current RSS, rather than lifetime peak RSS, consumes the shared process headroom.
Reservations cover estimated growth not yet observed in RSS. Positive observed
growth consumes outstanding reservation charges in admission order, avoiding a
second charge for the same growth. Releasing a permit removes only its remaining
unobserved charge; measured resident memory stays charged until a later sample.
This is a conservative feedback convention, not ownership attribution for every
allocation in the host process.

Missing, unsupported, failed, or stale samples fail closed for new work with a
retryable `MemoryPressure` error. RSS saturation returns retryable
`MemorySaturated`; a request above the stable resident limit is nonretryable.
After saturation, a fresh sample must show the configured recovery headroom before
admission resumes. Synchronous queries return the admission error before mutation.
Tokio waits for policy notification, cancellation, or its deadline; it does not
discard active permits or spin while waiting for a sample.

The public consumer tests cover both default and minimal profiles, shared
instances, resource pinning, hysteresis, missing/unsupported/stale samples,
owned/borrowed Tokio recovery, and cancellation before mutation. On Linux, macOS,
and Windows, a subprocess touches and releases a bounded 64 MiB anonymous mapping
and checks current-RSS-driven rejection and recovery through actual synchronous
and Tokio queries without cgroups. It emits `hawdb-process-rss-native-v1` with the
observed byte counts. The local-only state-machine campaign compares 32 seeded,
512-step sequences with an independent interval-debt oracle.

```sh
cargo test --locked -p hawdb --test process_memory_policy_contract -- --nocapture
cargo test --locked -p hawdb --no-default-features --test process_memory_policy_contract
cargo test --locked -p hawdb-fuzz --lib process_memory_state_machine_matches_interval_oracle -- --nocapture
bazel test //:hawdb_process_memory_policy_contract_tests \
  //:hawdb_process_memory_policy_contract_minimal_tests \
  //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests \
  //:hawdb_linux_ci_fuzz_smoke_test
```

## Snapshot and freshness boundaries

Planner snapshots own reader pins, so checkpoints and cleanup cannot invalidate
their physical generations. The plan cache and optimizer statistics-generation
state share ownership across snapshots. Statistics invalidation does not reset
the generation counter: an in-flight old planner must not publish a template
under a key that a newer statistics publication can reuse. Read transactions
share the planning cache handle without locking it while capturing the read view.
Freshness retains the full published identity captured with the pin, including
checkpoint generation and epoch. It must not reconstruct that identity from
the read-only store snapshot, which deliberately omits the durable writer handle.

Tokio read execution captures its planning view and read transaction together,
then plans and executes outside the embedded mutex. Mutation execution plans
outside that mutex, reacquires it, and validates the published view, configuration,
system variables, and optimizer schema before execution. A stale mutation is
replanned outside the mutex, at most three times. Repeated changes return an
execution error asking the caller to retry; no mutation is applied by those
failed attempts. A changed admission descriptor also fails closed and asks for
retry rather than executing with a stale reservation.

Control statements remain classified by the existing parser and physical-plan
rules. Streaming still rejects mutations and unsupported materializing statements.
Cancellation and deadlines are checked before admission and after snapshot
capture. The blocking operation owns its permit until it finishes, even when
the awaiting future is dropped. Successful committed writes keep the existing
acknowledgement semantics.

## Observable changes and verification

One successful query now normally records two admissions and completions:
`Control` planning followed by execution. A stream rejected as a mutation records
only the planning phase. A cancelled or saturated request that never passes the
planning gate does not parse or optimize.

Regressions cover saturated synchronous and all three Tokio entrypoints using
malformed queries, planning without the writer mutex, warm template reuse,
old/new schema isolation, generation invalidation, reader-pin release, mutation
freshness, and ordered planning/execution telemetry. Existing cancellation,
streaming backpressure, result budgets, and custom mutation classification tests
remain enabled.

```sh
cargo clippy -p hawdb --all-features --all-targets -- -D warnings
cargo test -p hawdb --features tokio-runtime --lib embedded
cargo test -p hawdb --features tokio-runtime --lib api::query_runtime
cargo check -p hawdb --no-default-features
bazel test //:hawdb_unit_tests //:hawdb_system_sql_loom_tests \
  //crates/qos:hawdb_qos_tests //crates/qos:hawdb_qos_loom_tests \
  //crates/runtime-tokio:hawdb_runtime_tokio_tests \
  //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests \
  //:hawdb_linux_ci_fuzz_smoke_test --nocache_test_results
```

Fuzz remains local-only. This change introduces no runtime, backend, dependency,
persistent-format version, or `io_uring` integration.
