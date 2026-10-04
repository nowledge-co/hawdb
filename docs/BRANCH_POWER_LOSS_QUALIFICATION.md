# Branch power-loss qualification

This records deterministic implementation evidence for #820, namespace fixes
for #834, and descriptor evidence for #819. It is a bounded qualification
matrix under stated synchronization assumptions, not a physical-device
certification or a Rust-to-TLA refinement proof.

## Reproduce and test discovery

Use the pinned Rust toolchain and locked dependencies:

```sh
cargo test --locked -p hawdb-storage --all-features
cargo test --locked -p hawdb --all-features --lib api::tests::power_loss:: -- --nocapture
cargo test --locked -p hawdb --all-features --test branch_project_open
cargo test --locked -p hawdb --all-features --lib api::branch_lifecycle::tests::
```

The facade matrix is registered in the existing Cargo library test target and
requires `test-support` (included by `--all-features`). The recorder is Unix-only.
The existing Bazel storage unit target includes its recorder/image-engine tests
under `cfg(test)`. The ordinary Bazel facade target does not enable the storage
recorder; run the explicit Cargo matrix rather than claiming its filtered Bazel
invocation executes fault cases. Default/minimal project-open integration tests
have existing Bazel targets. Routine local fuzz remains local-only:

```sh
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```

## What the recorder checks

Attach before database handles open in an empty, already durable anchor
on an existing mount. Descendant project installations inherit the recorder,
so a new project and its intermediate parents are captured within that anchor.
The anchor and mount themselves remain external synchronization assumptions.
Resource domains remain separate; this inheritance is test instrumentation only.

Successful native creates, opens, writes (including short/vectored writes),
truncations, shared inode identities, directory installation, hard links,
renames, removals, and file/directory synchronization drive the model. Capture
audits the actual visible tree and rejects uncaptured mutations. Memory and
operation limits reject before native mutation. Observation points retain one
image immediately before or after the named actual operation; later barriers
or handle closure cannot strengthen that image. No callback executes inside
storage locks. A final audited capture checks the complete fixture's IO coverage.
Acknowledgment capture requires the host to exclude concurrent operations.

Completed file barriers preserve bytes and length; completed directory barriers
preserve direct names. Synchronizing a child directory alone does not install
its name in its parent. Uncovered operations may disappear, persist in selected
order, or persist only a selected byte range. Images preserve hard-link identity
and are materialized in isolated directories. Recovery uses ordinary
`Database::open_with_config`, metadata SQL, and deferred branch admission.

## Executable matrix

| Path | Cuts and oracle |
| --- | --- |
| Initial project installation | Before/after actual new project directory creation within a durable anchor; lost/persisted unacknowledged installation remains uncommitted. Completed schema/data commits preserve both newly created parents and exact main UUID. |
| Legacy bootstrap | Before/after the final selector replacement; legacy committed graph data and durably reserved main/project identities survive both lost and retained uncovered changes. |
| Synchronous mixed transaction | SQL table schema, relational row, and graph data survive loss of every uncovered operation after acknowledgment. |
| Relaxed mixed transaction | Its successful checkpoint covers the complete schema/SQL/graph transaction. |
| Relaxed private WAL | Two acknowledged graph/schema transactions; whole loss, every eligible half-torn write, isolated writes, and reverse write order yield complete transaction prefixes or fail closed with WAL evidence retained. |
| Checkpoint/head | Both durability policies, before/after head replacement; completed prefix survives, with exact schema/data and allowed relaxed-tail loss before completion. |
| Source seal/private rotation | Both policies and before/after seven actual boundaries: successor creation/write/file sync, sealed-WAL installation, sealed-root installation, source-head rename, and its directory barrier. Each cut recovers lost/persisted changes; completed seal acknowledgment covers the whole prefix. |
| Create reservation/completion | Both policies, before/after each catalog replacement; durable receipts retain one UUID/outcome across retries. A reservation that never persisted has no acknowledged identity; a missing pre-publication head may produce the same UUID's terminal abort. |
| Logical deletion | Both policies, before/after Deleting and Deleted catalog replacement; repeat the original UUID/revision request, preserve one tombstone, and recover the unleased descendant. |
| GC | Acknowledged deletion plus actual orphan unlink before its name barrier; an unleased modified descendant retains inherited graph data and its own schema/SQL state after parent cleanup. |
| Admission | Before/after actual checkpoint hard-link alias installation; exact committed schema/data survive replay without copying a parent dataset. |
| Configurations | `Auto`, `Materialized`, and `OutOfCore` crossed with all four existing relational index modes; indexed queries, child DDL/DML/checkpoint, nested fork, parent deletion and reopen under a 32-FD domain. |

Checkpoint/head, create/delete catalog and GC cuts additionally replay
uncovered operations in reverse order and each operation in isolation. The
oracle still retains every completed file/directory barrier; these schedules
never weaken an acknowledged prefix. The executed log records actual pending
operation and generated plan counts so a single-operation cut is not mislabeled
as a nontrivial reordering case.

`Authoritative` currently requires a canonical binding and rejects schema-changing
transactions. The configuration matrix checks that rejection leaves the epoch
unchanged, publishes DDL through the existing `Shadow` configuration/checkpoint
path, then reopens in `Authoritative` for indexed DML, forks and crash recovery.
This qualification does not add or imply new authoritative DDL support.

Storage regressions separately cover first immutable-store ancestors, failed
ancestor-barrier retry, reuse after uncertain final-name publication, and complete
but unsynchronized pending head/WAL recovery. Removing first-publication ancestor
barriers, reuse's final barrier, or either pending-recovery file/directory barrier
makes the corresponding real recovery regression fail. Negative controls use
isolated native targets to prevent executable replacement by another worktree.

## Descriptors, jobs and GC schedules

The project-open integration suite exercises 64 unopened branches, repeated
creation retries/listing and reopen: no retained runtime/cache/WAL FD per record,
zero leaked reservations, a finite 32-FD high-water cap, and unchanged native
process descriptor count in an isolated child. A 12-FD fixture proves typed
rejection before a create can change the catalog. Existing tests cover shared
immutable-cache identities, idle eviction, native-open failures, reader snapshots,
source-preserving failed selection, and repeated switching.

Consumer staging creation and explicit/destructor cleanup use the counted
storage traversal. A four-descriptor regression retains the entire stage when
its domain is full, then removes it with one slot available without leaking
permits. A source-backed initialization regression actually fills its 32-FD
domain, propagates the typed rejection, retries successfully, and reopens the
published consumer. Initialization releases its publication lease before
cleanup, allowing that returned slot to clean the stage. Search's optional
post-publication cleanup separately defers both a full-budget scan and an
unlink blocked by a still-open iterator, then retries without changing the
published generation.

The combined production-path descriptor audit resolves Rust import aliases and
reviews native ingress and handle/permit lifetimes in the facade, storage,
search, and embedded vector-projection sources. Low-level opens and clones
acquire permits before native handles; handles close before permits return.
Iterator entries retain their shared permit, and immutable logical references
retain identity without requiring resident descriptors. External query spill
and export operations carry their source project context. Developer input,
test-support image materialization, host telemetry, and standalone projection
without `storage-io` are outside this branch-engine domain. This finite source
audit supplements small-budget regressions; it is not a compiler completeness
proof or an exact process-wide descriptor census. Derived projection completion
and error classification are delivered in #839; derived-recovery and pruned
range error preservation are delivered in #841. Each PR targets `main` directly.

Pending/running/failed host jobs block `USE BRANCH` and retain the current runtime;
completed jobs keep their outcome and monotonically increasing IDs across
selection. Read snapshots retain their runtime lease. Catalog-backed GC holds
metadata serialization through inspection and sweep, and defers all reclamation
if any branch lease is live. It does not speculate that an unknown owner has no
pins. The fault matrix exercises that conservative implementation. Independent
job-owned roots and finer concurrent mark/revalidate/sweep schedules remain
separate #819/#820 work; this PR adds neither capability.

## Model mapping and limits

The unchanged [lifecycle model](tla/BRANCH_LIFECYCLE_PROOF.md) represents complete
schema/data closures. Runtime file/name barriers provide implementation evidence
for its `SyncObject`, `ReadyHasDurableHead`, `SyncAcknowledgementsDurable`, and
`BranchStateAtomic` obligations. Create/delete receipts correspond to its durable
lifecycle states. The IO model separates inode contents from directory names;
abstract atomic closure publication alone cannot prove that distinction.

The model assumes reliable completed POSIX synchronization and atomic
same-directory rename. Native execution validates the implementation against
those assumptions; it does not certify a filesystem, controller or drive and
does not power-cycle the host. Windows now requests counted write-capable
directory-handle flushes and propagates failures; recorder attachment still
rejects Windows. API success, cross-target Clippy or ordinary Windows recovery
cannot establish the Windows ancestor-name barrier's semantics. See the
[namespace assumptions](BRANCH_NAMESPACE_DURABILITY.md). Symlinks and
custom native open flags are rejected by capture. IO outside the recorder must
be added before claiming coverage of a new path. The finite named matrix is not
exhaustive IO-interleaving exploration; physical devices, Windows namespace
persistence, independent job roots and fine-grained concurrent GC remain explicit
gaps. Do not close the umbrella issues solely on this matrix's passing result.
