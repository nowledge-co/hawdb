# Projection source durability: knowledge and recovery boundary

`HawDBProjectionSourceDurability` refines the source-barrier assumption in
[versioned publication](VERSIONED_PROJECTION_PUBLICATION_PROOF.md). It separates
OS-flushed complete transactions, file-data durability, WAL-name durability and
the prefix whose completed barriers the current process can actually establish.
The model does not prove byte-level filesystem recovery or the complete embedded
Search publication path.

## Independent specification and inductive argument

Let `E` be the applied complete source epoch, `C` its durable checkpoint epoch,
`D` the synchronized WAL-data prefix, `N` WAL-name durability and `K` the runtime's
known durable prefix. The independent recovery oracle is `R = D` if `N`, otherwise
`R = C`. The obligations are `K <= R`, known namespace implies actual namespace,
and every durable projection watermark is at most `R`.

- Initially `K = C`. Replaying complete records into OS-visible state establishes
  `E`, not their hardware durability. A process reopen resets knowledge to `C`
  and clears namespace knowledge even when future writes select synchronous mode.
- A synchronous commit synchronizes the complete file prefix and any namespace
  not already known durable before advancing `K`. An existing WAL filename is
  insufficient evidence: it may have been created by a previous relaxed process.
- Relaxed or grouped commits can advance `E` without advancing `K`. Successful
  shared group flush completes file and required directory barriers before `K`
  advances. A successful checkpoint publishes its complete closure and establishes
  the new `C` as a known floor.
- Explicit source synchronization owns the canonical source from file sync through
  required directory sync and knowledge publication. No writer can insert a newer
  unsynchronized suffix between those steps. It may cover a prefix later than the
  captured epoch, but does not change the policy for subsequent host writes.
- A failed barrier does not advance `K` and poisons further publication in that
  runtime. A complete uncertain WAL suffix is retained for recovery, not treated
  as a rolled-back operation. Reopen replays complete records and conservatively
  re-establishes knowledge; it does not reinterpret an error as data loss.
- Publication checks dependencies and rejects active groups before considering
  source synchronization. It requires `captured <= K`, or explicitly synchronizes
  an uncovered prefix while retaining the owner. Power loss recovers `R`, which
  therefore still covers every published projection watermark.

These are arguments about the abstract transitions, conditional on completed
barriers and the existing canonical WAL/checkpoint closure. TLC checks a finite
instance, not an unbounded machine-checked refinement of Rust or storage hardware.

## Native mapping

| Model | Source |
| --- | --- |
| `known` and namespace knowledge | `DurableStore::known_durable_commit_epoch` and `wal_namespace_known_durable`, initialized from the checkpoint and false respectively |
| Reopen | `DurableStore` admission plus `graph_recovery.rs`; replay advances the applied WAL epoch without certifying its durability |
| Synchronous commit | `durable/wal.rs::finish_wal_append`; successful complete append advances known epoch after both required barriers |
| Group flush | `finish_wal_sync_group`; no early knowledge from grouped append |
| Checkpoint | `durable/checkpoint.rs` installs known floor and namespace only with the selected checkpoint manifest/closure |
| Explicit sync | `DurableStore::synchronize_source_prefix`; existing file only, exact expected WAL length, file sync and required directory sync |
| Poison on failure | `GraphStore::synchronize_source_prefix`; kernel regression injects file-sync and directory-sync failures separately |
| Publication guard | `Database::publish_prepared_projection_with_source_durability`, exact source/dependencies before `ProjectionSourceDurability` handling |
| Default policy | `RequireKnownDurable`; explicit `Synchronize` affects the current operation's source prefix and leaves subsequent commit policy intact |

The source method never recreates a missing WAL. The canonical database borrow,
existing directory lease and WAL generation ownership exclude compliant concurrent
replacement/writes during synchronization. Arbitrary external file replacement,
torn frames and reordered dependency writes require the separate fault-image
qualification; path existence and matching length alone do not prove those cases.

## Finite evidence and controls

With `MaxEpoch = 2`, one process reopen and one power loss, the main configuration
checked **12,978 generated / 3,965 distinct states**, depth 18, with no errors.
It used repository-pinned TLA+ tools 1.7.4, SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`,
with Azul Java 21.0.9 on macOS arm64.

All seven fault controls returned TLC invariant-violation status 12 with the
expected invariant. The eighth configuration is an intentionally false assertion
that witnesses successful relaxed publication after explicit synchronization.

| Suffix | Expected violation |
| --- | --- |
| `TrustRecoveredPolicy` | `KnownPrefixSound` |
| `TrustExistingWalName` | `KnownPrefixSound` |
| `SkipDirectoryBarrier` | `KnownPrefixSound` |
| `EarlyGroupKnowledge` | `KnownPrefixSound` |
| `AdvanceOnSyncFailure` | `KnownPrefixSound` |
| `ReleaseSyncOwner` | `KnownPrefixSound` |
| `PublishFromWrittenEpoch` | `DurableProjectionSource` |
| `RelaxedWitness` | `NoRelaxedPublication` |

The registered safety action is:

```sh
bazel test //docs/tla:HawDBProjectionSourceDurability_check --test_output=errors
```

For direct TLC, run from `docs/tla` with the pinned jar and use distinct metadata
directories for the main configuration and each `mutants/` configuration. Verify
the exact invariant and exit status; parser/runtime failures are not controls.
The manifest `mutants/mutants.txt` records all expectations.

## Native tests and remaining qualification

Storage regressions check relaxed commits before/after an explicit barrier,
checkpoint coverage, synchronous/group coverage, replay under a future synchronous
policy, missing WAL refusal, failure before file sync and before directory sync,
and the first synchronous write after reopening an existing relaxed WAL name.
An uncertain complete WAL suffix remains replayable after the failed response.
Facade consumers check explicit relaxed publication, unchanged future write
policy, reopened-source refusal until synchronization, checkpoint-based publication
and dependency-conflict rejection before any source synchronization.

The main publication model still carries the independent source-validity,
artifact-closure, GC, cancellation and lost-response obligations. This focused
model abstracts complete transaction prefixes and completed file/directory barriers;
it has no byte layouts, torn writes, failed checkpoint internals, filesystem
mount failure, multiple writer processes, query collectors or numeric I/O bounds.
Failure injections and process reopen are not power-loss proof. Compose the native
fault-image campaigns and full #231/#232 workload/retention gates before completion.
