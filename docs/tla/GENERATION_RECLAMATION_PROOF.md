# Checkpoint Generation Reclamation

`HawDBGenerationReclamation.tla` is the bounded safety model for immutable
checkpoint generations and the retention policy implemented by
`DurableStore::reclaim_old_generations`. The model now covers both:

- `current-and-previous` (the default): the selected generation and its
  immediate predecessor remain available;
- `current-only`: only the selected generation remains, subject to reader pins.

The model is a safety oracle. It does not assume fairness, and therefore does
not prove eventual reclamation, bounded reclamation latency, or a particular
free-space outcome.

## Model actions

| Model action | Production path |
|---|---|
| `BeginRead` / `EndRead` | `Database::begin_read_transaction` and reader-pin release |
| `BeginCheckpoint` / `PersistGeneration` | candidate preparation and durable artifact publication |
| `PublishGeneration` | `DurableStore::publish_checkpoint_manifest` |
| `Reclaim` | `DurableStore::reclaim_old_generations` |
| `Crash` | process loss with the selected durable generation intact |

`retentionPolicy` is selected nondeterministically in `Init` and is unchanged
by every transition. TLC therefore explores both configured policies in one
finite run. `RetentionFloor` maps to the generation set retained by
`try_reclaim_old_generations`: `currentGeneration - 1` for the default policy
and `currentGeneration` for `CurrentOnly`.

## Checked invariants

| Invariant | Obligation |
|---|---|
| `ManifestGenerationAvailable` | The selected generation is never reclaimed. |
| `PinnedReadersRemainAvailable` | Every active reader's generation remains readable. |
| `PreviousGenerationRetained` | Under the default policy, the immediate predecessor remains available; `CurrentOnly` is exempt. |
| `RetentionPolicyRespected` | The retained set agrees with the selected policy. |
| `PublishedOnlyAfterDurable` | A generation becomes selectable only after it is durable. |

The safety argument is:

1. A generation becomes current only after `PersistGeneration` has made it
   available and `PublishGeneration` has selected it.
2. `Reclaim` is enabled only while the writer is idle and no reader exists.
   Reader generation pins are therefore never removed by this coarse model.
3. The retention floor always includes the selected generation and, under the
   default policy, its immediate predecessor. `CurrentOnly` removes only
   generations strictly below the selected one.
4. `ManifestGenerationAvailable` and `PinnedReadersRemainAvailable` are
   inductive over every transition, including `Crash`.

## Parked automatic source

The implementation adds one refinement not represented as a separate model
action: under `CurrentOnly`, a manual checkpoint drops the automatic owner's
parked source before publication so its stale immutable-root pin cannot survive
the post-publication object sweep. This is safe for three reasons:

- the manual checkpoint first suspends the automatic owner and waits for its
  phase to be idle, so the parked source cannot be concurrently preparing or
  publishing;
- the live runtime still owns its current `branch_root_pin`; the sweep runs
  only after the new branch root is selected, when the superseded root is no
  longer the runtime's serving root;
- every live reader, prepared candidate, and branch creation base registers its
  own root pin, which the object-store mark retains independently of the parked
  automatic source.

`HawDBBranchLifecycle.tla` owns the general root-pin and mark/sweep
obligations (`ArmedCandidatesComplete`, `CreateSourcesRetained`,
`ForeignPinsFlushed`). The current-only implementation regression additionally
bounds the project's unique-inode footprint across repeated checkpoints, so a
retained superseded immutable closure fails the test.

## Limits

The model abstracts checksums, atomic rename, directory barriers, physical
crash reordering, and the object-store alias/mark implementation. It does not
prove Rust refinement, reclamation liveness, WAL replay, or power-loss
durability. Those remain covered by the implementation, power-loss
qualification, and the branch lifecycle model.

## Reproduction

```bash
bazel test //docs/tla:HawDBGenerationReclamation_check --test_output=errors
```

The checked finite instance uses two readers and generations `0..4`; covering
both retention policies produced 3,106 generated and 804 distinct states, no
errors, and a complete depth-21 state graph.
