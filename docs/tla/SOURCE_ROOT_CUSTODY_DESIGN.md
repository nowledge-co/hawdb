# Source-root custody for governed immutable readers

Status: proposed protocol with a finite safety model. Native refinement is
**not implemented**. The admitted root remains private and refuses public
serving/conversion. This document does not qualify legacy readers, source-root
incarnation, actual Mem activation or complete MVCC readiness.

## Ownership and publication protocol

A source-root authority must hold the native directory incarnation and perform
child opens/unlinks relative to that authority. A pathname recheck before normal
path I/O cannot protect the interval between validation and use. Unix and Windows
implementations both need native tests for rename/replacement and typed descriptor
admission. The existing short publication lease has a different lifetime from a
reader: retaining it for an entire view would block subsequent publication.

While holding short publication exclusion, capture the manifest closure and
install per-artifact registrations before allowing GC to select or unlink those
artifacts. Then release publication exclusion. Each shared immutable segment or
mutation term-file payload owns its registration until its final user and any
in-flight read close. Closing the producer or parent view cannot release a
child's sources. Reusing one segment must retain only its artifact family, not
all artifacts from the old manifest. The root directory authority itself does
not pin every generation.

Both native cleanup entrypoints must consult the same registrations:

- `generation_cleanup::SearchProjectionCleanupState::run_with_remover` handles
  discovered and queued retry candidates.
- `generation_cleanup::once::PreparedCleanup::run_with_remover` handles the
  one-pass writer cleanup.

Selection and unlink must share exclusion against new registration. Existing
Arc clones keep the registration present; they cannot resurrect a zero-owner
payload after unlink selection. Final removal can proceed independently once
all users close. The directory authority must also bind cleanup to the captured
root after namespace replacement, so cleanup cannot delete a different root.

Registration nodes, shared payload/control blocks and native directory handles
require admission before allocation/open and receipts through actual destruction.
A receipt embedded in a removed BTreeMap value does not necessarily cover the
remaining tree capacity. Weak registry control blocks can outlive payload guards.
These resource obligations remain separate from the finite model below.

## Native ownership lifetime that must be separated

The target contract in `MVCC_VERSIONED_PUBLICATION_SPEC.md` excludes
multi-process writing. This model has one coordinating process; it does not
implement cross-process reader registrations or distributed GC. The native
ownership/exclusion mechanism must enforce that scope for the entire lifetime
of retained views, including after their producer closes.

Current `consumer::operation::Authority` owns a
`SearchProjectionPublishLease`; `ConsumerProjection::open` acquires that native
lease before constructing its authority. Keeping an Arc to this authority in a
new reader would keep the publication lock held after the consumer closes.
Reopening a new consumer for the same root would then fail at native acquisition,
even when the old operation-reference counter is zero. Merely avoiding a busy
operation reference therefore does not solve independent-view lifetime.

The refinement needs distinct root resource/namespace custody and short
publication admission. Do not implement reader registration by cloning the
current consumer authority or retaining its publication lease. Same-process
producer reopening must coordinate with existing reader registrations for the
same captured root, while retaining native ownership against unsupported peers.
Directory/resource identity must survive producer closure independently of that
producer's operation generation. Registration lookup and last-owner removal
also need real metadata admission and allocation lifetimes; a new uncharged weak
registry is insufficient.

Add a native case that closes the producer while a child remains usable, opens
a successor producer for the same root, publishes a newer generation and runs
both cleanup paths, then reads the old child again. Also test root replacement,
final child closure and subsequent reclamation. This is a required future native
case, not evidence already obtained. The finite `RetainBusy` control catches one
abstract form of this exclusion error; it does not certify the current native
lease implementation or its process-ownership mechanism.

## Finite safety model

[HawDBSourceRootCustody](HawDBSourceRootCustody.tla) models two physical roots,
three artifact identities, one producer/parent, an independent child and an
in-flight read. One publication makes the selected child source reclaimable
without its registration. Namespace replacement does not change captured native
authority. Separate selection/unlink actions share publication exclusion. Both
cleanup kinds are independently selectable. The current and preceding artifacts
are abstracted; the model is not a recovery-format or durability specification.

The positive standalone graph has 220 generated / 66 distinct states, depth 10,
with an empty remaining queue. The registered Bazel checks must replay the final
module/configurations before this design is delivered. Nine independent mutants
exercise the required boundaries:

| Control | Violated invariant |
| --- | --- |
| DropOnParent | SourcesLive |
| ReleaseDuringRead | SourcesLive |
| IgnoreOncePins | SourcesLive |
| IgnoreRetryPins | SourcesLive |
| PathForUnlink | ForeignRootUntouched |
| PathForRead | ReadUsesCapturedRoot |
| RetainBusy | ReaderDoesNotExcludeWriter |
| PinWholeManifest | ChildPinIsExact |
| UnlockBeforePin | SourcesLive |

`ReaderDoesNotExcludeWriter` forbids retaining the reader's publication-busy
reference after installation. It is a structural safety condition, **not** a
proof of starvation freedom, bounded native waits or writer latency. Exact child
pins forbid retaining unrelated old-manifest sources after parent closure; this
does not establish physical reclamation latency or allocator bounds.

## Required native refinement and remaining gates

Current native cleanup uses generation sets and path-based unlink. It does not
implement this live reader registry. Current admitted construction checks project
containment, which does not prove source incarnation or namespace authority.
Before exposing governed serving, implement the counted native directory
capability, map manifest/artifact identities and shared child ownership to real
registrations, and connect both cleanup paths. Native controls must reproduce
stale-root replacement, final-drop/read races, producer close, segment reuse,
last-owner removal, cancellation and publication concurrent with retained views.

The finite IDs assume physical root authority and immutable artifact identities;
no dev/ino, pathname, hash or Arc equality is itself a proof of those assumptions.
The model omits crashes, persistence barriers, GC replay, torn/reordered writes,
managed cache/registry allocation, descriptor eviction, physical read budgets and
liveness. Those require native implementation/evidence and composed recovery
arguments. Closed C-ready/E mapping, complete owner/parent/fanout dependencies,
durable activation receipts, actual Mem integration and the original writer,
fairness, RSS, I/O and group-fsync qualification remain open.
