# Branch lifecycle model and refinement boundary

The normative contract is
[`BRANCHING_STORAGE_SPEC.md`](../specs/BRANCHING_STORAGE_SPEC.md), tracked by
[#774](https://github.com/nowledge-co/hawdb/issues/774). This is an executable
design model for the P0 storage contract. The implementation has direct
head admission, branch-local checkpoint/seal publication, and SQL session
selection. The links below describe those implementation boundaries; they do
not establish a Rust-to-TLA refinement proof or physical power-loss qualification.

## State and abstraction

`HawDBBranchLifecycle` has a bootstrap branch (`0`), child (`1`), and
grandchild (`2`). The finite root set represents whole immutable closures:
each root atomically pairs a logical data set with its schema and the required
checkpoint/WAL artifacts. The model can create `main -> child -> grandchild`,
publish one update at each level, and retain a child after its parent becomes a
durable tombstone.

`s.state`, `s.head`, `s.durableHead`, `s.base`, and `s.capturedSource` represent
the durable catalog/head lifecycle and immutable lineage. A branch has no
expiry state or deadline. A prepared create records its exact source root before
the child head is installed; recovery either completes that installed create or
preserves its terminal aborted tombstone. The model does not reuse an ID.

`s.flushed` means files reached the operating system. `s.durable` means their
required storage barrier completed. `s.head` is the currently visible root and
`s.durableHead` is the recovery root. Branch `1` represents an explicitly
selected `SyncOnCheckpoint` context; the other branches represent the default
`SyncOnEveryWrite` context. A synchronous acknowledgement requires the complete
closure in `s.durable`. A relaxed acknowledgement requires a complete OS flush,
then `PowerLoss` may restore the older durable head and record that loss. A
checkpoint seals the relaxed visible head into `s.durableHead`. These are ghost
state distinctions, not proposed persisted fields.

`s.localOpen`, `s.foreignOpen`, and `s.foreignPin` represent runtime leases.
They do not affect whether a ready branch exists. `Select` chooses a ready,
unleased branch; `SelectBusy` records a failed target admission and preserves
the source context. One branch may have only one writer, while separately leased
branches can be open at the same time. A foreign lease pins the exact root it
admitted, rather than following a later current-head change.

The Rust catalog subprotocol now has direct counterparts to `BeginDelete` and
`FinalizeDelete`: `begin_delete_file` persists the `Deleting` state while
holding the metadata lease, and `finish_delete_file` persists the tombstone
only from that state. `Database::delete_branch` uses a temporary branch-lock
admission probe before `BeginDelete` and releases it before finalization.

`GraphStore::admit_branch_from_head` is source-level evidence for the
admission portion of this model. It validates a ready UUID/revision, acquires
the target lease without holding catalog metadata serialization, recovers from
the target's immutable root and private WAL, validates the sealed successor
prefix before replaying its append-only suffix, then revalidates the same
catalog identity before exposing the runtime. The target lease stays held
through failed-admission cleanup or for the admitted runtime's lifetime.

`GraphStore::seal_admitted_branch` checks the exact current commit epoch, seals
the append-only WAL suffix, and publishes the next head. Checkpoint publication
also selects a new root/private WAL for that UUID; ordinary transactions leave
the selector unchanged. Catalog `source_commit_epoch` remains creation lineage,
while child reservation and publication validate the current selected source
head. Direct admission preserves mutable WAL dependencies outside disposable
runtime materialization and finalizes relational recovery only after replaying
all sealed generations and the private suffix.

`Database::open` admits a published project's selector and catalog before
loading its default branch's data. The first data operation reserves target
resources, recovers the exact ready UUID/revision under its lease, completes
required schema/row readiness, and then publishes the runtime. Checkpoint
artifacts are mounted as verified hard-link aliases; mount validation uses
transient handles without populating the immutable-handle cache. A mutable open
detaches an alias before changing shared bytes. Admission does not copy the
source container or the checkpoint dataset; hard-link installation requires
the project runtime and object store to reside on the same filesystem.

SQL `CREATE BRANCH`, `DROP BRANCH`, and `USE BRANCH` use the storage lifecycle
contracts. `USE` validates a candidate before replacing the selected store,
schema, identity, optimizer, planning caches, reader pins, and projection
consumers together. It rejects explicit transaction and shared-session
selection. Failed admission leaves the source runtime available.

The project descriptor domain has a finite default limit of 1024, configurable
through `DatabaseConfig::max_open_files`. Ownership locks, mutable WAL handles,
cached immutable handles, and temporary recovery/publication operations share
that domain. Target capacity is reserved before recovery; unopened logical
artifact aliases retain no native handle. This bounds one project's charged
descriptors, not all host libraries or independent projects in the process;
an OS-level descriptor rejection remains possible and is reported explicitly.
Native Unix project acquisition observes the soft limit and caps effective
admission below it by 64 host handles. Configured and effective limits are
reported separately; shared-domain conflicts compare the configured value.
Insufficient minimum capacity returns a typed OS-limit error. Observation does
not change process limits. The OS-cap calculation and immutable cache's LRU
order are implementation policies outside the descriptor-count conservation
argument, which uses the installed effective limit.

Catalog-backed GC serializes metadata through sweep and conservatively defers
all reclamation while any branch lease remains active. The admitted store and
read snapshots retain that lease, protecting unpublished candidate closures
and older reader roots. The returned report exposes this deferral.

These are source-level links to the catalog, admission, publication, and session
state machines, not a full Rust-to-TLA refinement. Runtime isolation and
descriptor tests are separate implementation evidence; the model does not
prove the Rust resource budget (#819). Deterministic physical power-loss runtime
qualification (#820) remains required; #778 still owns fine-grained physical
cleanup and pin-aware reclamation. Process-kill/reopen tests and successful
model checking do not substitute for torn-write, write-reordering, and lost
unsynchronized-write qualification.

`s.candidate` is an unpublished root and `s.armed` records a candidate whose
complete closure is already durable. `StageCandidateClosure` models an
unreferenced, fully persisted DDL/DML artifact that a later publication can
adopt. This makes the mark-then-recheck GC requirement concrete: an object
marked before it is armed by a later publication must still be retained at
sweep time. `Protected` includes ready/deleting heads, recovery heads, creating
bases, leases, and publication candidates. Parent deletion may proceed while a
descendant survives because the descendant has its own immutable root.

## Implementation fault evidence

The [branch power-loss matrix](../BRANCH_POWER_LOSS_QUALIFICATION.md) captures
actual production IO and recovers isolated crash images through ordinary
project opening. It distinguishes inode bytes from parent-directory names,
including namespace retry and uncertain pending-create recovery. Its named
bootstrap, WAL/seal, head/checkpoint, catalog, admission and GC cuts support the
abstract complete-closure obligations below; they do not establish a refinement
proof or exhaustive physical interleaving coverage. The model and its finite
configuration are unchanged by those implementation corrections, so previous
positive/mutant/witness results remain results for that exact abstraction.
Windows ancestor-directory persistence and physical-device behavior remain
explicit qualification gaps.

## Safety argument

TLC exhaustively checks the configured finite instance. The invariants encode
these implementation obligations:

| Invariant | Obligation |
| --- | --- |
| `DurableHeadsComplete` | Every recovery head retains its full durable closure. |
| `VisibleHeadsFlushed` | A visible head never references objects absent from the OS-flushed closure. |
| `CreateSourcesRetained` | Creating, ready, and deleting records retain their captured base. |
| `LineageImmutable` | Parent advancement or deletion cannot rewrite a child's captured source. |
| `BranchStateAtomic` | A selected root exposes its matching schema and data together. |
| `ParentImmutableUnderDescendantWrites` | Child/grandchild publication cannot replace main's head. |
| `DeletedHasNoLease` and `SingleWriterPerBranch` | Logical deletion waits for runtime ownership release; a branch never has two writers. |
| `SourcePreservedOnBusy` | A failed `USE BRANCH` admission does not discard the current selection. |
| `SyncAcknowledgementsDurable` | Default-policy acknowledgements survive a crash. |
| `ReadyHasDurableHead` | A ready branch always has a recoverable root. |
| `ArmedCandidatesComplete` | A sweep cannot reclaim an artifact once an in-flight publication has claimed it. |
| `ForeignPinsFlushed` | A live foreign owner retains its admitted root until its own process closes or power loss ends every lease. |

`ProcessCrash` discards candidates and local leases while preserving another
process's simulated foreign lease and its pinned root. `PowerLoss` ends every
lease, resets each visible head to its durable head, and removes non-durable OS
flushes. `Recover` leaves a deleting branch retained when a foreign owner is
still live, then finishes deletion once that owner is absent. This models the
contract's distinction between a dead local process, a still-live foreign owner,
and a machine restart.

## Negative controls and reachable paths

All configurations are registered in `mutants/mutants.txt`. The runner rejects
a passing mutant and verifies the named invariant, so parser errors and
unrelated failures do not count as evidence.

| Configuration suffix | Injected defect | Required counterexample |
| --- | --- | --- |
| `PublishEarly` | Publish a visible root before its full flush. | `VisibleHeadsFlushed` |
| `MutateParent` | Let a descendant write replace main's head. | `ParentImmutableUnderDescendantWrites` |
| `ForgetCreating` | Omit a creating branch's source from GC protection. | `CreateSourcesRetained` |
| `StaleSweep` | Sweep from an old mark without rechecking later candidates. | `ArmedCandidatesComplete` |
| `DeleteLeased` | Finalize a branch while an owner still holds its lease. | `DeletedHasNoLease` |
| `AllowSharedWriter` | Admit a second writer for one branch. | `SingleWriterPerBranch` |
| `AllowUnsyncedFork` | Capture a relaxed source that has no durable recovery root. | `CreateSourcesRetained` |
| `SplitSchemaData` | Publish a data root without its matching schema. | `BranchStateAtomic` |

Seven safe-guarded configurations assert deliberately false invariants to show
the required paths are reachable:

| Configuration suffix | Reachable trace |
| --- | --- |
| `CreateRecoveryWitness` | Install a child head, process-crash, then recover the same ready child. |
| `AbortedCreateWitness` | Process-crash before child-head installation and retain a terminal aborted tombstone. |
| `DeleteRecoveryWitness` | Start delete, process-crash, then recover a durable deleted tombstone. |
| `ConcurrentBranchWritersWitness` | Hold writers for different branch UUIDs concurrently. |
| `NestedForkWitness` | Create a grandchild from a ready child. |
| `DeleteWithLiveDescendantWitness` | Delete a parent record while its child's immutable root remains ready. |
| `RelaxedAcknowledgementLossWitness` | Acknowledge a flushed relaxed write, lose power, and recover the earlier durable head. |

## Reproduction and recorded result

Use the repository's pinned TLA+ Tools/JDK through Bazel:

```console
bazel test //docs/tla:HawDBBranchLifecycle_check --jobs=1 --test_output=errors
scripts/check-storage-tla.sh --check-mutants
```

The standalone mutant runner accepts `TLA2TOOLS_JAR`, `TLA_JAVA`, and
`TLA_WORK_ROOT` when the host has no Java runtime. These are verification
inputs only; they are not HawDB production configuration. The normal Bazel
action retains module/config snapshots, tool hashes, Java version, and full TLC
log in `bazel-bin/docs/tla/HawDBBranchLifecycle_check.run.tlc-evidence/`.

On 2026-09-30, TLC 2.19 with the repository's remote JDK 21 completed the
positive configuration without an error: 40,016,092 generated states,
4,285,426 distinct states, maximum depth 38, and an empty queue. The full
registered mutant suite rejected every control, including the fifteen branch
controls above. These counts identify this model run; they are not an
acceptance threshold or evidence that the Rust implementation has the same
properties.

## Limits and runtime acceptance

The instance has one identity at each lineage depth, a fixed set of four
closures, no sibling, and one logical update per branch. It abstracts catalog
and head serialization, cross-process locks, object decoding, checksum
validation, WAL byte framing, OS/file-system barriers, torn writes, write
reordering, descriptor budgets, and physical descriptor counts. The separate
`HawDBImmutableRootBindings` model checks that multiple physical paths may map
to one immutable object without losing a recovery binding. This model also does
not model SQL parsing, transaction-scoped selection rejection, complete
active-WAL replay, or the Rust file-descriptor budget.

The model establishes safety only. It makes no fairness, latency, eventual-GC,
or starvation claim. In particular, it assumes a successful sync survives
power loss and a selected root is atomically old or new. Runtime work must
fault-inject every WAL, catalog, head, checkpoint, and GC publication boundary;
exercise both durability modes; prove complete schema/data recovery; test
cross-process leases and selection failure; bound open descriptors across many
branches; and verify no branch creation copies or imports parent logical data.
