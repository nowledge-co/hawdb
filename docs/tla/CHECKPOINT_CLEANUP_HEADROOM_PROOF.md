# Checkpoint cleanup capacity admission

## Reproduced failure and boundary

Main `b3f00e6d7a7e55e8ac3435d6f01514c9af70bf44` can admit an ordinary
writer during `Releasing` after its delta has crossed the capacity reserve.
A test-only admission trace from the existing retiring-owner regression records
`delta=13662`, `max=16384`, `capacity=true`, `group=false`, then the next mutation
is refused at 14,842 estimated bytes. The 16-KiB limit and all deadlines are
unchanged. The preceding trace includes both Preparing and Releasing admissions;
this is a missed phase predicate, not evidence that every checkpoint error has
this cause. The temporary trace instrumentation is not part of the fix.

The frontend control mutex already serializes admission/debt observation with
publication. Preparing waits for checkpoint capacity, while Discarding and
Releasing can still own the memory or physical-generation admission needed by
the next checkpoint. These two cleanup phases must use the same capacity wait.
Below the reserve, ordinary writes continue while cleanup remains paused. At
capacity, the caller waits with the existing deadline and receives the existing
retryable `BranchBusy` outcome if progress does not arrive. No pressure threshold,
deadline, durability default, WAL encoding or publication boundary changes.
Group-commit exemptions and the existing Retiring deferral protocol are retained.
`BranchRuntimeCell::get_mut` and `peek_mut` obtain this control boundary before
acquiring the runtime write guard. Condition-variable waits release the control
mutex, so the capacity wait does not retain either the frontend runtime writer
or the control mutex needed by checkpoint progress.

## Finite argument and assumptions

`HawDBCheckpointCleanupHeadroom.tla` isolates this admission gate with one
foreground writer and inactive group commit. `used` is the logical delta charge,
not physical RSS. CapacityAt is below DeferAt by an explicit reserve, and the
modeled write estimate fits that reserve. An admitted writer holds the control
boundary through its atomic write decision; cleanup can progress while it waits.
Preparation can reduce the retained delta, and cleanup releases the admission
needed to start another checkpoint. A finite tick budget abstracts the existing
capacity deadline; it does not measure wall-clock time or model every wakeup.

At admission in either cleanup phase, the fixed predicate requires
`used < CapacityAt`. Since `WriteEstimate <= DeferAt - CapacityAt`, adding the
modeled estimate stays strictly below DeferAt. Thus cleanup does not admit a
reserved-size write only to reject it at the pressure boundary. At or above
CapacityAt the caller waits or returns Busy without making a canonical write.
This is a conditional admission argument for the cleanup phases, not a guarantee
that an arbitrary large user transaction fits or that the entire owner is live.
Idle/Retiring scheduling, physical storage progress, byte-estimator completeness,
writer fairness, source/target rebinding and durability are separate protocols.

The finite configuration uses CapacityAt=3, DeferAt=5, WriteEstimate=2. TLC checks
**1,139 generated / 508 distinct states**, depth **9**. Ignoring either Discarding
or Releasing's capacity fence yields `NoCleanupCapacityBypass` with exit 12.
Two witnesses also exit 12 for their named negated properties: a small writer
commits while cleanup remains active, and a waiting writer commits after cleanup
and checkpoint progress. The witnesses show reachability, not scheduler fairness
or eventual progress under permanent resource denial.

TLC 2.19 / TLA+ tools 1.7.4, Java 21.0.9; pinned jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`.
Each run uses an isolated Java temporary directory. Both faults and both witnesses
return the expected invariant and exact exit 12; parse errors and timeouts do
not count as controls. The positive action and all four controls are registered
in the storage-model/evidence manifests.

## Native linkage and validation scope

The native change widens the existing capacity-deadline and mandatory-fence
predicates in `Control::lock_frontend` to include Discarding and Releasing.
`preparation_needs_capacity` and its thresholds remain the same. No returned
mutation error is parsed or retried, so the change cannot re-execute a partially
completed host callback or assume cross-statement rollback.

`cleanup_capacity_waits_before_admitting_another_writer` controls each cleanup
phase with a capacity-limited debt snapshot. A single observation channel reports
either the actual wait phase or completed admission; the test releases the phase
and joins the caller before asserting. With the original source this fails
because admission completes during Discarding; with the fixed source it passes.
It does not rely on a sleep to infer that another thread ran.
`cleanup_with_capacity_still_admits_a_writer_before_resource_release` verifies the
complementary below-reserve boundary in both cleanup phases. Existing real-owner
pressure, paused-prefix and cleanup tests remain the required integration checks.
The conditional model does not replace those tests or the complete MVCC writer,
resource, lost-response and composed power-loss qualifications.

Native qualification retains the original failing admission test (0/1 on the
unchanged predicate) separately from the passing fix. Main plus the cleanup patch
passes all five preparation/backoff tests (69.73s) and all 17 headroom tests
(89.76s), including both previously observed small-delta pressure failures.
After combining the identical cleanup implementation with the owned full rebuild,
`bazel test //:hawdb_unit_general_tests --test_arg=api::automatic_checkpoint
--test_arg=--test-threads=2 --repo_env=CARGO_BAZEL_ISOLATED=false
--local_test_jobs=1 --jobs=4 --test_output=errors` passes **84/84** native cases
(491.11s), on runtime input tree `abd422eea144ed9d9d264f977532ec8d22dedfca`.
This selects automatic-checkpoint cases from the general partition; it is not a
passing claim for every general-partition test or for the concurrent partition.
These debug verification times are not writer-throughput or latency comparisons.
