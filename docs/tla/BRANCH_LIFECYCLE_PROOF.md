# Branch lifecycle model and refinement boundary

The normative contract is
[`BRANCHING_STORAGE_SPEC.md`](../specs/BRANCHING_STORAGE_SPEC.md), tracked by
[#774](https://github.com/nowledge-co/hawdb/issues/774). This is an executable
design model for planned P0 storage. It does not claim that runtime branch
selection or a Rust-to-TLA refinement proof exists. The storage layer has a
direct head-admission kernel, but SQL session selection and branch-local head
publication remain separate implementation work.

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

`GraphStore::admit_branch_from_head` is source-level evidence for the
admission portion of this model. It validates a ready UUID/revision, acquires
the target lease without holding catalog metadata serialization, recovers from
the target's immutable root and private WAL, validates the sealed successor
prefix before replaying its append-only suffix, then revalidates the same
catalog identity before exposing the runtime. This linkage does not establish a full
Rust-to-TLA refinement, nor does it implement SQL `USE BRANCH`, DDL/DML head
publication, or physical reclamation.

`s.candidate` is an unpublished root and `s.armed` records a candidate whose
complete closure is already durable. `StageCandidateClosure` models an
unreferenced, fully persisted DDL/DML artifact that a later publication can
adopt. This makes the mark-then-recheck GC requirement concrete: an object
marked before it is armed by a later publication must still be retained at
sweep time. `Protected` includes ready/deleting heads, recovery heads, creating
bases, leases, and publication candidates. Parent deletion may proceed while a
descendant survives because the descendant has its own immutable root.

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
