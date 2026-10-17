# Versioned projection publication: argument and model boundary

This model supports the [publication specification](../specs/MVCC_VERSIONED_PUBLICATION_SPEC.md)
for #231/#232. It specifies the missing source-validation/publication protocol;
it does not claim that the native embedded path already implements it.

## Independent safety specification

A captured job has a source instance, monotonic target incarnation, epoch,
immutable rows and point/scan dependencies. `IndexValid` consults prunable
stamps. `OracleValid` independently consults unpruned stamps, complete scan
membership, exact identities and the actual projected row contents. Publication
records a permanent error flag if that independent oracle rejects it. Later
writes need not preserve equality between the active projection and live Graph.

The projected domain contains `a` and `b`; `b` begins absent. `u` is unrelated.
A scan records membership as well as existing row dependencies. Broad writes
stamp a database barrier, and schema writes stamp a schema barrier. These are
abstract assumed collectors, not a proof of AST/plan extraction. Read-domain
summaries are distinct from writer conflict intents in the implementation plan.

## Inductive argument

Assume complete collectors, monotonically allocated stamps within an instance,
atomic snapshot/pin registration, exclusion of relevant writes and replacement
from validation through selector commit, and successful durability barriers
with the platform guarantees required by the storage implementation.

1. Capture preserves immutable input and registers its history/resource roots
   before preparation escapes. Private build and flush actions do not expose a
   selector. A lease prevents reclamation of the prepared durable parts.
2. A relevant intervening mutation leaves a stamp newer than the captured epoch.
   Safe pruning cannot remove that stamp while the capture pin survives. A
   missing row requires its domain membership stamp; row stamps alone cannot
   detect insertion into an initially absent scan result. Broad and schema
   barriers cover otherwise unqualified changes.
3. Instance identity rejects a replacement even with equal epochs and values.
   Monotonic incarnation rejects A-to-B-to-A rebinding. A disjoint write touches
   neither the captured read keys nor its barriers, so it can commit during
   preparation without invalidating publication.
4. Validation checks all dependencies and the source durability prefix while
   acquiring the publication fence. Every transition capable of invalidating
   the proof obeys that fence. Thus the proof remains valid when the selector
   becomes durable, rather than only at the instant validation returned.
5. Selector synchronization requires all artifact parts to be durable. The
   source durability prefix covers the captured epoch, including when ordinary
   Graph commits use explicit relaxed synchronization. Visible output and its
   watermark derive from the complete durable selector.
6. Cancellation before selector commit releases private ownership. After commit,
   cancellation cannot report rollback. Crash restores the durable Graph prefix
   and durable selector, invalidates runtime identities and drops transient
   ownership. A lost response is resolved from the already committed generation.
   Cleanup cannot remove parts reachable from the durable selector.

These arguments concern the abstract transitions. TLC checks the configured
finite instance; neither this text nor TLC is a machine-checked unbounded
refinement proof of the Rust implementation.

## Invariants

| Invariant | Obligation |
| --- | --- |
| `TypeOK` | State values stay in the finite configured domains |
| `NoStalePublication` | Every selector commit passes the independent validity oracle |
| `ArtifactsBeforeSelector` | A durable selector has every durable artifact part |
| `DurableBeforeVisible` | Visibility and acknowledgement require a durable selector |
| `DurableSourceBeforeProjection` | The selector's source epoch survives source recovery |
| `ReceiptHasCompleteGeneration` | An advanced watermark has the complete committed generation |
| `StagingRetained` | A preparation lease retains its flushed candidate parts |
| `ResourcesUntilDecision` | An undecided live attempt retains source/resource ownership |
| `NoCommittedCancellation` | A durable commit cannot become a cancellation result |

## Component mapping and native refinement still required

| Model transition/state | Existing component or remaining obligation |
| --- | --- |
| Capture and pin | `GraphStore::capture_read_dependencies` and `Database::begin_prepared_read` register a canonical-tip history pin and retain exact source identities |
| Prunable stamps | `crates/storage/src/version.rs`; `ReadNodeDomain` summaries and a read-only fallback barrier share the pinned index/admission lifetime |
| Source write/barrier | `graph_commit.rs` validates destination intents before `extend_read_invalidation_stamps`, admits the union before WAL, then installs it atomically |
| Private artifacts and selector | `projection_generation.rs` candidate, sealed-token GC lease and synchronized head publication; Search activation composition remains to implement |
| Catalog head CAS | `ProjectionGenerationStore::publish` checks expected head and watermark; its lock does not exclude Graph writes or host target replacement |
| Publication fence | `Database::publish_prepared_projection` retains its exclusive borrow through source validation and catalog publication; full embedded host-binding/cursor authority remains to integrate |
| Source durability | Known durable source epoch must cover capture; explicit typed synchronization covers an uncovered relaxed/recovered prefix, with no active group |
| Crash and retry | Protocol abstraction; native torn-write, reordered-write, lost-unsynced-write and lost-response fault qualification remains required |

The initial native refinement is deliberately bounded. In
`src/api/dependency_read.rs`, qualified node scan/seek/count leaves collect their
label domain before execution. Unary filters/projections/aggregates/sorts/limits
operate on those inputs; filters are also checked recursively for hidden
relationship-existence reads. Unsupported operators and adjacency predicates
fail closed. An optimizer-folded empty plan records an observable database epoch
dependency. Ignoring a statement or dependency-budget failure cannot produce a
usable source token. Qualified declared scalar-index EQ/IN predicates now use exact keys even when
the physical access is a scan; composite/range and other unsupported shapes retain
the conservative domain. The [indexed-read argument and controls](INDEXED_PROJECTION_READ_VALIDATION_PROOF.md)
cover that source refinement and the capture-to-query activation gap.

Every node create stamps its declared label and the all-node domain; property
changes and deletion use pre-commit labels or same-batch created labels. A cold
identity whose labels are unavailable leaves the independent read barrier.
Legacy direct and schema paths retain their existing broad stamps. These
summaries are collected only while a prepared source is registered, so capture
from an older ordinary snapshot is forbidden. Canonical capture and summary
activation happen under the same owner borrow before any later writer can run.
The registration pins all history newer than that capture through validation.
Private writer workspaces do not build canonical read summaries. These facts
establish the finite model's assumed no-gap stamp lifetime for this native path;
they do not prove other query families or every cold-storage transition.

`SealedProjectionGeneration` retains an `Arc` lease through its last clone.
Sealing transfers `open_writers` protection to the pin under the catalog mutex,
and GC consults both roots. Independently opened handles for the canonical
catalog path share a weakly registered mutex/pin state, including read-only
readers. This is process-local serialization, not multi-process write support.
Different catalog tokens cannot authorize a new publication.

Native regressions cover exact/absent point dependencies at the kernel, empty
predicate results, relevant data/schema changes, independent stale writers,
budget refusal before WAL with reopen, and source-instance replacement. The
external facade consumer pauses preparation in a worker thread while the main
thread commits a disjoint write, then publishes the prepared generation. It also
checks head CAS, statement failure, source watermark mismatch, known-prefix refusal, explicit relaxed-source
synchronization and recovery of an already selected committed receipt after reopen.
Those reopen checks are not power-loss qualification. Receipt resolution after
subsequent head replacement, host target ABA and multi-batch progress remain
separate obligations.

See the [source durability argument and controls](PROJECTION_SOURCE_DURABILITY_PROOF.md)
for the separate refinement of durable-prefix knowledge across replay, file and
namespace barriers.

## Local finite evidence

The initial main safety configuration passed with 12,808,706 generated states,
2,660,220 distinct states and depth 20. Bounds are `MaxEpoch = 2`, three rows,
two artifact parts, one prepared job, up to two target rebinds and one crash.
The run used repository-pinned TLA+ tools 1.7.4, SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`,
and local Azul Java 21.0.9 (x86_64 through Rosetta). This is direct local TLC
evidence, distinct from a registered Bazel action or CI result.

The registered Bazel safety action and evidence-script test also passed. Its TLC
log records the same generated/distinct state counts and search depth 21 with
automatic workers (the direct two-worker run reported depth 20). Parallel TLC
search-depth statistics are not a separate protocol bound.

All twelve fault controls below exited with TLC invariant-violation status 12
and the named invariant. Both reachability controls also exited 12 with their
intentionally false assertions. They are witnesses, not safety failures.

| Configuration suffix | Expected violation |
| --- | --- |
| `SkipInstance` | `NoStalePublication` |
| `IgnoreBindingIncarnation` | `NoStalePublication` |
| `OmitScope` | `NoStalePublication` |
| `SkipBarrier` | `NoStalePublication` |
| `DropFence` | `NoStalePublication` |
| `PrunePinned` | `NoStalePublication` |
| `SkipSourceDurability` | `DurableSourceBeforeProjection` |
| `SkipArtifactDurability` | `ArtifactsBeforeSelector` |
| `PublishEarly` | `DurableBeforeVisible` |
| `EarlyWatermark` | `ReceiptHasCompleteGeneration` |
| `ReclaimStaged` | `StagingRetained` |
| `CancelCommitted` | `NoCommittedCancellation` |
| `DisjointWitness` | `NoDisjointPublication` |
| `LostResponseWitness` | `NoRecoveredLostResponse` |

Reproduce the registered safety action with:

```sh
bazel test //docs/tla:HawDBVersionedProjectionPublication_check --test_output=errors
```

For direct TLC, run from `docs/tla` with the pinned jar and a usable Java runtime:

```sh
java -Xmx2g -cp /path/to/tla2tools.jar tlc2.TLC -workers 2 \
  -metadir /tmp/versioned-publication-safety \
  -config HawDBVersionedProjectionPublication.cfg HawDBVersionedProjectionPublication
```

For a control, substitute its full `mutants/HawDBVersionedProjectionPublication<Suffix>.cfg`
and use a separate metadata directory. Check the exact invariant name as well
as the nonzero status; syntax errors or runtime failures are not successful
negative controls. The checked-in `mutants/mutants.txt` records those expectations.

## Limits and composition

The model abstracts completed file/directory barriers atomically. It does not
model filesystem bytes, failed syscalls, cross-process readers, arbitrary query
plans, multiple simultaneous preparations, incremental batch composition,
constraints, writer queues, numeric RSS, unbounded retry or fairness. The two
witnesses establish reachability only. Existing MVCC validation, retained-history,
WAL/group, branch/GC and scheduling models remain separate obligations.

Native source-linked collector completeness, deterministic interleavings,
paused-preparation writer progress, cancellation/cleanup ownership, fault images
and 1/4/8-writer latency/throughput qualification are still required. Keep both
issues open until the complete acceptance ledger is satisfied.

## Main owner composition regression

After incorporating main `cfbea1b` (#1032), the external consumer
`tests/mvcc_publication_owner.rs` composes the real public
DatabasePublicationOwner with PreparedRead and sealed Search activation in both
default and minimal profiles. Preparation releases capture ownership, records a
host-owned graph Binding dependency and pauses on a channel. A competing thread
must commit through the same owner and report completion before preparation is
released; starting a writer thread or observing a try-lock is insufficient.
Delayed graph hydration still reads the captured value. Disjoint writes permit
activation at the captured epoch; relevant writes, graph binding ABA despite an
equal latest guard value, and replacement by a same-epoch database reject before
selector/watermark visibility and reclaim the candidate stage.

This is composition evidence for the reusable owner/engine APIs. The binding is
a tracked graph row in this fixture; it does not qualify arbitrary external host
binding storage, native bounded catch-up coverage or the actual Mem outer-owner
handoff. Search activation still performs artifact I/O under publication
exclusion. Those acceptance gates remain open.
