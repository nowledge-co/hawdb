# Consumer preparation, publication, and read retention

This delivery addresses the implementation gaps tracked in #232 and #1136,
following the physical reclamation work in #867. The earlier negative
[publication qualification](CONSUMER_PUBLICATION_QUALIFICATION.md) remains valid
for its recorded runtime. It is not evidence that the new path passes a gate.

## Declared complete input domains

`begin_search_projection_consumer_catch_up_in` accepts a `ProjectionSourceScope`.
`Database` preserves conservative whole-source validation. `Domains` enumerates
only the named graph labels and relational tables, including absence and empty
tables. Label membership is checked against the same pinned source as hydration;
allocation holes and excluded candidates still consume bounded enumeration work.
A node with multiple selected labels is visited once. The full mapper cannot
replace the complete comparison with a sampled or partially applied delta.

The stable Consumer ID identifies the scope and deterministic mapper version.
Changing the projection definition requires changing that ID. Tracked mapper
queries can add dependencies outside the enumeration domain. Registering the
scope before delta hydration lets the existing query collectors prove when a
label/table dependency subsumes physical identities. Adding a narrower scope
after hydration cannot erase already captured dependencies.

Unrelated commits can advance after E without invalidating this proof. Relevant
updates, deletes, insertions, membership changes, schema changes, a different
source instance, source rewind, expired registration, and an unusable retained
feed boundary remain refusals. Continuously changing a relevant domain can still
invalidate an attempt; this API does not certify arbitrary stale derived data.

Once C..E has been fully compared, reservation checks changefeed retention at E,
not at the old durable C. C may have left the live feed while its already captured
coverage remains owned. This exception requires the opaque completed token and
preserves every binding, expiry and source-dependency check. E itself must still
be at or above the current resume floor so the newly published Consumer can
continue. Ordinary lifecycle/open/capture paths still require their durable C.

## Construction outside the database owner

The typed path has three stages:

1. `prepare_search_projection_consumer_covered_update` validates complete-E
   coverage, admits resources, and reserves the successor using the original
   captured native parent. Its returned token borrows the Consumer exclusively
   and does not borrow the database.
2. `ProjectionConsumerPreparation::prepare` performs full generation construction,
   artifact verification, compatibility snapshot loading, and selector preparation.
   Call it after releasing the database owner's write guard.
3. `publish_search_projection_consumer_prepared_update` validates source
   dependencies and native reservation/target identity, selects the prepared
   generation, completes the durability barrier, and mirrors the authoritative
   native receipt into the registry. Return its `ProjectionConsumerPublication`
   from the owner callback and retire it outside the guard.

```rust
let reserved = owner.with_write(|db| {
    Ok(db.prepare_search_projection_consumer_covered_update(
        &mut consumer,
        covered,
        ProjectionSourceDurability::RequireKnownDurable,
    ))
})??;
let prepared = reserved.prepare();
let published = owner.with_write(|db| {
    Ok(db.publish_search_projection_consumer_prepared_update(prepared))
})??;
let report = published.into_report();
```

The compatibility `publish_search_projection_consumer_covered_update` method
composes these stages synchronously and retains its blocking behavior. Hosts
must use the split path to shorten their outer ownership interval.

A pending native operation deliberately hides its registration from ordinary
status reads. Final publication uses the retained operation's source and target
fences, then reconstructs the disposable registry cache from native authority.
A registry refresh during preparation cannot discard the borrowed operation.
Dropping either a reserved or prepared token transfers original native recovery
custody back to the Consumer. Resolve it with
`resolve_search_projection_consumer_publication` before another attempt.
Cancellation, pre-selection rejection, and a lost successful reply retain their
existing distinct resolution paths. No pathname adoption or durability-policy
relaxation is introduced.

## Embedded handle integration (#1171)

`NowledgeMemEmbeddedStoreHandle::open_or_create_search_projection_consumer`
returns a `NowledgeMemSearchProjectionConsumer` bound to that handle's original
publication owner. Its lifecycle keeps the source open even after other handle
clones close. The returned staging session borrows that lifecycle; private
progress can be paused and resumed with one-operation steps. Dropping the session
discards private progress. After process restart, reacquire the same stable ID
and begin again at the durable completed C; no private prefix is restart progress.

Use `begin_catch_up(config, mapper, scope)`, bounded `advance`, and
`verify_complete_source`/coverage steps as for the database protocol above. The
host supplies `ProjectionMappingFunction` and `ProjectionBaselineMappingFunction`;
tracked `query_sql_with_params` provides relational hydration against the same E.
These callbacks retain the existing query/output/payload/continuation budgets.
The host does not implement snapshots, coverage authority or publication.

`prepare_covered_update` reserves under the embedded write guard and constructs
after releasing it. Its token's `publish` method reacquires only the original
handle, performs fenced selection, refreshes the handle's canonical read view,
and retires old residency outside the guard. The database protocol reuses the
configured governor; the facade does not take a second background permit.
`resolve_publication`, `renew`, and `cleanup` preserve the native lifecycle and
lost-response contracts. Explicit logical removal uses the original handle
method `unregister_search_projection_consumer(id)`; an old runtime Consumer is
not a revocation authority for a new incarnation. Immutable `consumer()` plus
`read_permit()` expose the registered query/pinning surface.

This is an explicit registered projection target. It neither adopts an existing
legacy path nor silently redirects store-level Search queries. To migrate, create
a new registered target from the initializer's complete pinned snapshot, retain
the old target until verified, and direct query users to the verified Consumer.
The stable ID includes the mapper/scope version. Bootstrap remains a blocking
operation; this change bounds subsequent private catch-up, not initial import.

Full-residency and incremental maintenance compatibility APIs continue to accept
a complete interval that fits their supplied batch budgets. A short prefix from
a newer source is still rejected without changing the durable projection. Those
APIs do not promise resumable bounded progress; use the registered lifecycle for
that capability. Increasing limits or retrying the same refused prefix is not a
replacement for the new protocol. Positive Windows publication is qualified by
the existing supported-platform CI selection, separately from Linux evidence.

## Read data and conflict history

Read-only snapshots retain canonical data generations without registering a
conflict-history watermark. Writable workspaces, savepoints, and live dependency
collectors continue to register their validation epoch. Consequently a long
ordinary reader no longer forces unrelated conflict stamps to remain resident.

A version index records the epoch below which complete conflict history has
been discarded. A writable workspace created later from an old read-only view
is conservatively rejected if its base predates that floor. Registering an old
pin after reclamation cannot restore missing conflict evidence. Existing write
workspaces inherit their original protection floor across private statements
and savepoint restoration; delete/reinsert conflicts remain detectable.

Dependency collectors still protect history while collecting unknown future
reads. This change does not claim arbitrary scope-specific reclamation for an
unfinished collector or a long writable transaction.

## Qualification

The [current qualification report](CONSUMER_PROGRESS_QUALIFICATION.md) records
the failed strict writer gate and remaining matrix/platform work. This design
document is not an acceptance receipt.

The focused regressions cover materialized/out-of-core source domains, repeated
watermark advancement after real writer commits, relevant graph/SQL/schema/source
refusals, reserved/prepared drops, native fault recovery, durable reopen, long-read
history reclamation, stale upgrades, and savepoint delete/reinsert conflicts.

The Consumer benchmark compares disabled, scoped blocking, and detached modes
on the same binary. Both enabled modes build the same immutable documents and
perform 16 successful concurrent-phase publications plus 32 quiet publications.
All three modes perform the same 16 maintenance-trigger commits; each enabled
attempt must advance to a new E even after foreground writers drain. The finite
measurement explicitly retains `foreground writes + quiet publications + 64`
changefeed entries (the byte limit remains 64 MiB), so source-window expiration
does not terminate a slow control. Production defaults and work-step budgets are
unchanged. The earlier default-window failure is retained as adverse evidence. The harness
checks the exact foreground counter values before quiet-phase writes. It records
both foreground completion and whole-phase elapsed time, and does not count
post-drain publications as simultaneous progress. Native build intervals record
real foreground commit counts. Publication callback timing is
a lower bound on outer-owner hold time, not the selector mutex's duration.

An opt-in manual `--sustained true` detached workload extends publication attempts
through the foreground writer phase, with a 4,096-attempt ceiling. It uses the
same fixed write count and performs ordinary blocking checkpoints after every
16,384 completed foreground writes, inside the publication owner. This exercises
maintenance at the existing live-row ceiling rather than increasing that ceiling.
Every checkpoint count and elapsed interval is retained. Caller-triggered
checkpoint admission is separate from the query/native-preparation governor;
whole-process memory and timing limits must be declared by the measurement host.

The sustained retention-entry reservation includes the declared maximum trigger
writes: `foreground writes + quiet publications + 4096 + 64`; the 64 MiB byte
ceiling is unchanged. The default 16-publication workload and its reservation
remain unchanged. Source epochs must equal all actual foreground and trigger
commits, and exact final counters and documents remain mandatory. Qualification
requires at least two successful post-E advances with active writers and no
publication-cap exit while writers remain active. Cap hits, resource refusals,
checkpoint failures and incomplete datasets are retained as unmet evidence.
A finite fixed-work run is not a steady-state or starvation-freedom proof.

The separate original single-stream gate keeps the a703cc0f baseline, identical
writer harness, 40 paired runs plus a labelled warm-up, six primary commit
p50/p95 endpoints, ranks 12/29, and zero tolerated increase. Neither throughput
nor an inconclusive interval substitutes for that gate. Results and acceptance
dispositions must be attached after measurement; issue closure requires the
complete acceptance criteria, including evidence outside these focused changes.

## Acceptance ledger

The issue state is not a substitute for checking each delivered contract. The
following audit uses the final heads of already merged PRs. Required native
Linux/macOS/Windows contracts and Prow build, crate, root, analytics and crash
checks succeeded on those heads. Optional/manual skipped jobs are not passes.
Those results do not qualify the new source in this delivery.

| Package | Delivered implementation and evidence | Remaining boundary |
| --- | --- | --- |
| A / [#1137](https://github.com/nowledge-co/hawdb/issues/1137) | [#1144](https://github.com/nowledge-co/hawdb/pull/1144), head `429748c2234849fbcb79e727761cb8effcc1d3ec`, merge `b38787125fe34d494295103d31967f84b87e9c5a`: nine retention regressions, mixed graph/SQL/schema snapshots, savepoints, allocation/RSS receipt and local fuzz. See [retention qualification](MVCC_SNAPSHOT_RETENTION_QUALIFICATION.md). | This delivery changes ordinary-reader history ownership and adds stale-upgrade fencing; it needs its own supported-platform checks. It does not remove the original #232 latency/fairness gates. |
| B / [#1138](https://github.com/nowledge-co/hawdb/issues/1138) | [#1145](https://github.com/nowledge-co/hawdb/pull/1145), head `e89c08f01156d9ad8b0b6a255e05875b93173be8`, merge `ba45e0909833766ea8b785ca47a39a09d7ae8b42`: stable-ID open-or-create, real reopen, no query-created registrations, typed invalid/expired/rewind/retention failures, capacity-two lifecycle and real 64/65 codec boundary. See `src/api/search_projection_consumer/tests/lifecycle.rs`. | Source-coherent catch-up and positive stationary Windows publication retain their separate #1043/#1092/#1106 acceptance. A lifecycle wrapper or native refusal does not satisfy them. Current Windows CI selects positive creation/catch-up/reopen and `registered_consumer` integration tests; the new path still requires that CI result. |
| C / [#1139](https://github.com/nowledge-co/hawdb/issues/1139) | [#1146](https://github.com/nowledge-co/hawdb/pull/1146), head `90fd29f47b35d09f85fee230ab7e7e33c1024826`, merge `fb76b6fd0df7090033a831e72daabd00026cfbbd`: real transaction-owned complete dependencies, sticky refusal and bounded ownership. The [source-linked shape matrix](specs/TRANSACTION_READ_DEPENDENCIES.md) covers exact/domain/conservative/refused query shapes. | Conservative false conflicts and unsupported external inputs remain explicit contracts. This work does not replace that complete collector with scope declarations alone. |
| D / [#1140](https://github.com/nowledge-co/hawdb/issues/1140) | Same #1146 head and merge: opt-in serializable validation before WAL, optimistic/pessimistic and grouped/ungrouped paths, fourteen process interruptions, twenty bounded schedules, default/minimal configurations and same-work SI/optional measurements. See `src/api/tests/concurrent_transactions/serializable.rs`. | Finite schedules and process interruption do not prove power-loss behavior or starvation freedom. Optional serializability is not a new gate for #232, as specified by #1136. |

The parent [#1136](https://github.com/nowledge-co/hawdb/issues/1136) remains a
contract with independently reviewable acceptance, not a documentation-only
closure task. The physical generation lifecycle from
[#1162](https://github.com/nowledge-co/hawdb/pull/1162) remains the #867 foundation:
reader/candidate/native-operation/durable-branch roots and retryable failed
deletion evidence are not replaced by conflict-history reclamation.

For [#232](https://github.com/nowledge-co/hawdb/issues/232), the new source addresses
post-E unrelated commits, detached construction and read-history retention.
Joint progress, callback hold times, foreground tail latency, the original strict
single-stream gate, group-sync amortization, and sustained large/small admission
fairness are separate acceptance rows. A favorable result for one cannot close
the others. The qualification report records actual results and unresolved rows;
this ledger does not automatically close any issue.

## Architecture follow-through (#1222)

The versioned `SearchProjectionDefinition` binds bootstrap and both mapper
paths to one canonical scope/ID. The embedded defined wrapper keeps the original
Consumer owner and uses the existing split preparation/publication handoff.
Scoped complete-source enumeration now skips ID holes and prunes cold segments
using label summaries; exact document-set/content comparison remains mandatory.
See [the scan and publication controls](benchmarks/architecture_2026_10_24/README.md)
for the measured source and the decision to retain complete Search generation
construction. No performance gate or platform qualification is waived.
