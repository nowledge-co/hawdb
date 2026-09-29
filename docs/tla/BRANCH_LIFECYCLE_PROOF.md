# Branch lifecycle model and refinement boundary

The normative contract is
[`BRANCHING_STORAGE_SPEC.md`](../specs/BRANCHING_STORAGE_SPEC.md), tracked by
[#776](https://github.com/nowledge-co/hawdb/issues/776). This is an executable
design model for planned P0 storage. No runtime branch implementation or
Rust-to-TLA refinement proof is claimed.

## Superseded design evidence

The 2026-09-29 branch contract removes TTL/automatic expiry and requires writable
branch-local DDL, multi-level forks, and default power-loss-safe commits with
explicit relaxed durability. Branch metadata remains synchronously durable. The
model and recorded results below describe the earlier design and do not qualify
the revised contract. Retain them as historical evidence; revise the model and
rerun its checks before claiming current coverage. Runtime power-loss fault
injection remains necessary even after abstract model checks pass.

## State and abstraction

`HawDBBranchLifecycle` has parent `0`, child `1`, and four immutable root
closures. Root `0` references the original checkpoint; root `1` references a
replacement parent checkpoint. Root `2` shares checkpoint `0` plus a child's
WAL, while root `3` shares checkpoint `1` plus a child's WAL. `Values(root)`
records the corresponding logical data, independently of the selected head.
The child can capture either parent root, then keep it while the parent moves.

`s.state`, `s.head`, `s.base` and `s.durable` stand for durable catalog state,
head selectors, captured lineage and successfully persisted objects.
`s.expected` is a ghost logical result updated only by that branch's own
publication; it is not reconstructed from the current head by the invariant.
This lets a child publication that changes the parent's head violate isolation.
`s.capturedSource` independently records the create-time source for the lineage
invariant. Neither ghost is a proposed on-disk field.

`s.pin` represents one open writer/reader owner per branch and its retained
snapshot. Both entries can be populated simultaneously. `s.candidate` models a
publication guard, and `s.marked` models a potentially stale GC candidate set.
The old reader pin remains unchanged across head publication. `s.online`
separates a crash from recovery; recovery-witness booleans only record whether
specific transitions have occurred.

`PrepareCreate`, `InstallChildHead` and `PublishCreate` are separate durable
steps. Crash before head installation leads to an aborted tombstone; crash
after installation recovers the same branch to `Ready`. Deletion first makes
the branch non-openable and retains its roots, then retires it after lease and
publication release. An already prepared write can finish during deletion.

Each `PersistObject` action adds one complete durable object. Intermediate
closure subsets survive a crash but cannot be selected. Atomic selector
replacement and complete object sync are the model's filesystem assumptions,
not an assertion that a sequence of Rust filesystem calls is atomic.

## Safety argument

The following induction explains the positive transition system. TLC checks
its finite configured instance rather than arbitrary-size instances.

| Invariant | Preservation argument / implementation obligation |
| --- | --- |
| `SelectedRootsComplete` | Bootstrap is complete; child head installation checks its base; head publication requires every candidate object durable. Sweep excludes all selected roots and recovery never combines partial candidates with a selected head. |
| `PinnedRootsRetained` | Open pins the complete current head. Publish preserves the old pin. Sweep excludes pins, and close/crash explicitly releases them. A real cross-process collector must not release another process's lease. |
| `CreateSourceRetained` | Prepare captures the complete source atomically and makes it a GC root before parent advancement. The base remains protected through creating/ready/expired/deleting; only retirement releases it. |
| `LineageImmutable` | Prepare sets base and captured source once. Every later action preserves both, regardless of parent changes or deletion. |
| `BranchIsolation` | A new child's expected data equals its captured source. A branch publication updates that branch's complete head and expected data only. Crash/recovery and administrative actions change no logical data. |
| `ParentImmutableUnderChildWrites` | Parent publication selects only roots 0/1. Child publication may select roots 2/3 but cannot replace the parent's selector. |
| `DeletedHasNoLease` | Finalize requires no lease and no candidate; crashed owners are dropped before delete recovery. Logical retirement never revokes a live lease. |
| `OpenAdmission` | Only Ready branches admit new handles; expiry preserves existing ones. The wall-clock deadline check is abstracted by the Expire transition. |
| `ReadyHasRoot` | Create only becomes Ready after head installation; recovery only completes a create with an installed head. |

GC's mark phase alone grants no deletion authority. `Sweep` checks protection
again, so an orphan object reused by a new candidate/head after mark remains
safe. Production refinement requires a shared reachability serialization span,
lease-aware pins and conservative treatment of unreadable metadata; a stale
cached set or per-directory generation cutoff is insufficient.

## Negative controls and reachable paths

All configurations are registered in `mutants/mutants.txt`. Each contains
exactly one invariant, and the runner requires the named violation, rejecting
parser failures, timeouts, unrelated errors, and unexpectedly passing models.
The six defect controls each change one constant:

| Configuration suffix | Injected defect | Required counterexample |
| --- | --- | --- |
| `PublishEarly` | Select a candidate before all object syncs. | `SelectedRootsComplete` |
| `MutateParent` | A child write overwrites the parent's head. | `ParentImmutableUnderChildWrites` |
| `ForgetCreating` | GC omits the pending create's base root. | `CreateSourceRetained` |
| `StaleSweep` | Sweep trusts an old mark without current-reference validation. | `SelectedRootsComplete` |
| `DeleteLeased` | Finalize deletion before the live owner releases its lease. | `DeletedHasNoLease` |
| `OpenExpired` | Expired branches admit new handles. | `OpenAdmission` |

Six witness configurations retain all safe guards and assert a deliberately
false invariant. Their counterexamples show that the interesting scenarios are
reachable, rather than making a safety check vacuously pass:

| Configuration suffix | Required reachable trace |
| --- | --- |
| `CreateRecoveryWitness` | Prepare create -> install head -> crash -> recover Ready. |
| `AbortedCreateWitness` | Prepare create -> crash before head -> recover terminal-aborted Deleted. |
| `DeleteRecoveryWitness` | Publish create -> begin delete -> crash -> recover Deleted. |
| `ConcurrentWritersWitness` | Publish create -> open both parent and child simultaneously. |
| `ExpiredHandleWitness` | Publish create -> open child -> expire while handle remains open. |
| `IndependentWritesWitness` | Both branches publish distinct logical writes with their own heads. |

These expected counterexamples are separate from the positive `storage_models`
suite. They do not establish liveness under an unfair scheduler.

## Reproduction and retained evidence

Use the default repository Bazel configuration and its pinned TLA+ Tools/JDK:

```console
bazel test //docs/tla:HawDBBranchLifecycle_check --jobs=1 --test_output=errors
bazel test //:storage_tla_evidence_script_test //docs/tla/tests:rule_contract_tests --jobs=1 --test_output=errors
scripts/check-storage-tla.sh --check-mutants
```

The standalone mutant runner accepts `TLA2TOOLS_JAR`, `TLA_JAVA`, and
`TLA_WORK_ROOT` to reuse the same pinned artifacts and retain full logs in an
isolated directory. These are developer verification inputs, not production
branch controls. The positive Bazel action retains module/config snapshots,
tool hashes and the full log in
`bazel-bin/docs/tla/HawDBBranchLifecycle_check.run.tlc-evidence/`.
The suite manifest is `docs/tla/storage_models.bzl`, so the normal model gate
and complete-evidence collector include this model without a separate CI job.

The initial positive configuration explored 93,764 generated / 16,493 distinct
states, maximum depth 26, with the queue exhausted and no invariant violation
(TLA+ Tools 1.7.4, TLC 2.19, remote JDK 21, macOS arm64). Counts are evidence for
these model bytes, not an acceptance threshold or evidence of Rust behavior.
Current-run control outcomes and source revision belong in the PR validation
receipt; do not substitute these historical counts for checking changed bytes.

## Limits and runtime acceptance

There is one child identity, no reuse of that ID, at most one logical update
per branch, and no sibling/grandchild in this finite instance. Object contents
and closure functions are immutable by construction. Hash collisions, filesystem
rename/directory-sync failures, corrupt metadata, WAL byte framing, cross-host
behavior, and native path/case rules are not modeled. Atomic create preparation
abstracts metadata serialization and the expected-source-revision check.
Name validation/reuse, idempotency fingerprints and receipts, bounded resource
admission, and pagination are specified but require implementation tests.

The model represents expiry state, not UTC arithmetic or clock rollback.
Crash releases both modeled in-process leases; a real crash cannot release a
different process's handles. The collector is not tested against an arbitrary
number of concurrent processes by this model. There are no fairness assumptions,
eventual-completion claims, p99 bounds, or byte-cost claims.

Runtime issues must supply the specification's fault-injection, query-isolation,
reopen, cross-process lease, object-closure and local fuzz coverage. In
particular, verify all canonical graph/relational/append/overflow dependencies,
not just a checkpoint's top-level file, and observe absence of logical copying
during branch creation. Those checks remain necessary after this design model
passes.
