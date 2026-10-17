# Search prepared publication: argument and finite boundary

`HawDBSearchPreparedPublication` checks the Search backend lifecycle needed by
#231/#232: build an invisible candidate without retaining publication exclusion,
then reacquire that exclusion and reject a changed base before installing any
real-root dependency. It supplements the graph dependency/source-durability
models; it does not replace them or prove complete Mem owner integration.

## Conditional safety argument

Assume one supported Search root, cooperating library publishers and consumer
registration using its publication lease, immutable committed generations,
manifest-last durable replacement, and no unmodeled filesystem corruption.

1. Capture the active selector and next generation under the root publication
   lease. Release the lease before building content. Distinct writers own unique
   private stages, so prospective generation names may coincide without touching
   each other's output or the real selector. Partition prefix manifests remain
   inside the parent stage and are never real-root visibility steps.
2. The prepared token retains the writer, content metadata, stage registration,
   cleanup ticket, task context, descriptor domain and admitted memory/disk state.
   Governed candidates retain the same `Arc<RuntimePermit>` through build memory
   and stage ownership after their outer wrapper is consumed. Queued candidates
   retain no publication lease. Stage cleanup cannot collect a live registration.
3. Activation acquires the same lease as ordinary publishers and consumer binding.
   It checks the captured selector before real-root artifact installation. A
   different valid generation returns a conflict; disappearance/regression fails
   integrity checks. The existing standalone full-replacement repair of a damaged
   selector retains its exact bounded raw bytes and rechecks them. Different
   malformed selectors must not collapse to one absent/unknown base. This repair
   is not permission to ignore corruption in authoritative Graph recovery.
4. The lease remains held across dependency installation and final selector
   replacement. While held, another publisher or cooperating binding operation
   cannot invalidate the check. Complete installed dependencies precede the
   durable selector. Rejection and pre-commit cancellation expose no candidate.
   Cancellation after the selector's successful commit cannot undo that commit.
5. A selected generation remains a durable GC root after token cleanup. Failed
   attempts may leave unreachable real-root dependencies for conservative cleanup;
   they may not replace active dependencies after a stale-base rejection.
6. `Database::publish_prepared_search` accepts only the closed, sealed-trait Search
   candidate types. Under one exclusive database borrow it checks canonical
   instance, captured dependencies, exact source watermark and observed durability
   before consuming the candidate. It also refuses a source watermark below
   the captured active watermark. An unchanged relevant read set does not imply
   that a progress watermark may move backward. Damaged bases cannot establish
   that floor and fail closed on this source-validated path; standalone full
   replacement retains its existing explicit repair behavior. The facade shares
   the existing catalog publication's source validator. It does not expose `validate -> bool` plus an arbitrary
   caller callback. Host binding/receipt predicates still need their outer fence.

## Model and source mapping

The model has two one-shot publishers, two private parts per candidate, two
possible generations, a valid/absent or one of two damaged selector identities,
consumer registration, three possible source epochs, backend/checked activation
modes, a root publication owner, per-generation installed parts,
private-stage collection, active-artifact collection, cancellation, terminal
process crash and a lost-response receipt state. It checks:

- `NoStaleActivation`: selected work matches the exact captured selector and
  consumer exclusion at the commit point;
- `CompleteActiveClosure`: every selected part belongs to the selected attempt,
  even when competing attempts used the same prospective file names;
- `PrivateStageRetained`: ready/installing tokens retain all staged parts;
- `NoFalseRollback`: a committed attempt cannot be marked cancelled;
- `NoPreparationLease`: expensive build does not own publication exclusion;
- `NoCheckedWatermarkRegression`: source-validated activation never lowers the
  target's current source watermark. The model permits an older still-valid
  source proof, independently of the prospective Search base capture. Standalone
  backend replacement is a separate mode and has no new watermark policy.

| Model action | Native seam |
| --- | --- |
| Capture / Build | `generation_writer.rs::prepare_publication_with_artifacts`; scoped capture lease followed by artifact construction or `initial::prepare` |
| Acquire / Reject | `prepared.rs::SearchOutOfCorePreparedGeneration::publish` and `CapturedBase::validate` |
| Install / Select | unchanged `publication.rs::publish_generation` and `initial/install.rs::publish` under the activation lease |
| StageGC / Cancel | owned `StageDirectory` active registration and deferred cleanup ticket; task cancellation checks |
| Return / Resolve / Crash | committed selector durability abstraction; native generation-reader reopen and existing lost-response paths, not a new persistent operation receipt |
| Source validation | `src/api/dependency_read.rs::validate_prepared_projection_source` followed by a sealed Search activation under the same database borrow; source correctness is covered by the companion models, not assumed from the Search token |

TLC 2.19 / pinned tla2tools 1.7.4 checks **1,394,361 generated / 488,676 distinct
states**, depth 22, with both publishers and `MaxGeneration = 2`. No fairness or
unbounded liveness theorem is claimed. Nine fault configurations independently
skip base validation, release the activation fence, select before complete
installation, collect a pinned private stage, collect an active closure, cancel
a committed attempt, conflate damaged selector identities, or hold the lease
while building, or omit the checked watermark floor. Each returns status 12 with its exact named invariant violation.
The tenth configuration intentionally violates `NoConcurrentProgress`: it is a
reachability probe showing one publisher commits while the other's build remains
paused, not an expected safety failure in the correct model.

Native regressions synchronize a competing publisher's completed commit inside
the paused artifact callback before preparation resumes. They cover invisible
single/partitioned candidates, stale single/partitioned/incremental rejection,
complete hydration and source epoch after reopen, cancellation/drop cleanup,
retained governor admission, and damaged-selector byte comparison. Facade
consumers exercise actual Search activation after disjoint Graph writes, related
Graph rejection, explicit relaxed-source synchronization, foreign instance and
missing/mismatched/regressing watermark and damaged-base refusals in default
and minimal profiles.

## Refinement limits and remaining delivery

The model treats durable file installation and selector replacement as abstract
operations. It does not prove Rust memory safety, complete checksum coverage,
byte-level torn/reordered writes, stable host binding incarnations, exact batch
receipt coverage, numeric RSS or fair scheduling. Terminal crash preserves the
abstract completed barriers; process reopen tests are not power-loss proofs.
Incremental reused closures are abstracted as two complete parts; existing
incremental/partition failure regressions are additional evidence, not an
unbounded proof of closure composition.

This API builds indexes and partition content outside the Search publication
lease, and permits caller-controlled queuing before activation. **Activation
still verifies/installs artifacts and runs cleanup under its lease**, including
possible fallback copies and checksums. Its lock-held I/O has not yet been made
independent of artifact size or measured. The actual Mem catch-up path still
holds its outer owner through the legacy operation; it is not migrated here.

The facade requires an exact captured epoch. A bounded changefeed batch can have
a lower covered watermark, so it still needs the specified batch/receipt and host
binding protocol before adopting this path. Search tokens are consumed and are
not durable operation receipts; lost-response recovery still reads the selected
complete generation. Graph/Search remain separate durable domains with no
cross-store rollback. These obligations and the full performance/retention gates
remain in the versioned-publication acceptance ledger.
