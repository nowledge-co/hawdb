# Graph bundle carry: invariant argument and verification boundary

Issue: https://github.com/nowledge-co/hawdb/issues/1641.

The executable specification is `tla/HawDBGraphBundleCarry.tla`. TLC explores
its complete configured finite state space, with four physical generations and
three commits. This note gives a transition-by-transition invariant argument;
it is not a TLAPS theorem, a mechanically checked Rust refinement, or a proof
that a physical device implements the modeled synchronization assumptions.

## State and assertions

Each root stores checkpoint C, graph G, serialized graph source E, physical
base coverage S and abstract graph content version V. A separate replay tip
and graph state describe the candidate suffix; live and reader states have
their own visible tips. Graph content versions are logical history values,
not literal serialized E. Fresh bundles set E=S, while carry preserves old E.

The main assertions are:

1. Every nonempty root has `0 < G <= C` and `V <= E <= S`.
2. Graph bytes at G represent the graph state captured at S. Row-only commits
   may advance S without changing those bytes or E.
3. The graph-change classification fence covers every real graph change.
   Recovery may raise it conservatively to S; this does not advance E.
4. A candidate replay tip covers its base and contains the complete graph
   prefix at that tip. Catch-up preserves C/G/E/S and invalidates synchronization.
5. Selection requires a synchronized replay tip equal to the writer tip.
   Live graph state then matches that tip, while physical S remains unchanged.
6. Each reader retains its own visible graph state/tip and its physical G.
   Later writer commits and checkpoints cannot change that snapshot.
7. Current, previous, candidate and reader graph roots remain allocated.
   Row/checkpoint roots and graph roots have separate reclamation sets.

## Preservation by each transition

`Init` contains only a sentinel empty root. `Prepare` cannot reuse it: first
publication allocates a positive, fresh G. Every fresh generation is unique
and graph contents are immutable after allocation.

`Write` appends one complete logical commit to the abstract history. A graph
write updates the graph version and classification fence; a relational write
leaves both unchanged. Existing base roots, candidate prefix and readers keep
their captured history. The concrete classification contract combines graph
WAL operations and Catalog equality; unlogged low-level Catalog edits are
additionally exercised by a Rust fence mutation, rather than modeled as WAL.

`Prepare` captures the current logical state as a physical base. Reuse requires
an existing checkpoint and a graph fence no newer than its base S. Hence the
graph state at the new S equals the retained graph state. A genuine later graph
change forces fresh G, including after selecting an older base with a newer
suffix. Fresh output is pinned by the candidate until selection or discard.

`CatchUp` extends only the candidate logical tip/graph state. It never retargets
the immutable base and clears synchronization. Foreground writes after `Sync`
leave an older sealed prefix; the tip-equality guard prevents selecting it until
catch-up and a new synchronization complete.

`Select` transfers the candidate physical root to current and current to
previous. Its complete graph suffix becomes live state. Crucially, it preserves
the writer's graph fence: base S does not become the later visible tip. The next
preparation therefore rebuilds after a graph suffix rather than reusing stale G.

`Pin` captures both physical dependencies and the logical delta at the current
visible tip. `Write` and `Select` leave those reader values unchanged; history
entries at past tips are immutable. `Release` removes only that reader's roots.

`RecoverFence` represents the classification result after recovery: the greater
of base S and the latest replayed graph change. Actual lost/torn/reordered-byte
choices belong to the companion checkpoint model and physical recorder tests.

`Discard` drops candidate ownership without changing current/previous/reader
dependencies. `ReclaimGraph` excludes every retained G, while `ReclaimRow`
excludes every retained C. Retaining a graph therefore neither authorizes its
removal nor unnecessarily retains an unrelated old row descriptor at the same
integer generation.

## Controls that establish useful coverage

The ordinary configuration checks every assertion. Separate configurations
must produce their named invariant counterexamples:

| Control | Required observation |
| --- | --- |
| Dirty reuse | Base graph contents no longer match captured source |
| Omit current G | Current graph dependency is reclaimed |
| Omit reader G | A held reader loses its physical graph |
| Drop graph suffix | A carried nonempty base selects an incomplete live graph |
| Lose graph fence | Selecting a carried graph suffix forgets a later graph change |
| Forget reader delta | A reader of a carried nonempty base loses its newer graph state |
| Suffix selection witness | A carried nonempty base is actually selected with a newer graph suffix |

The last control is a reachability witness, not a faulty implementation. It
prevents a vacuous pass that never exercises base S < visible tip. Three commits
allow the concrete sequence: graph at 1, relational carry base at 2, graph suffix
at 3. The fourth generation permits subsequent rewrite and reader-safe cleanup.

## Mapping to Rust and complementary evidence

`DurableManifest` owns C/G/E binding validation. `reusable_graph_bundle` and the
checkpoint Catalog snapshot implement eligibility; `graph_change_commit_epoch`
provides classification. Preparation/adoption and candidate catch-up retain
physical base identity independently from the private WAL tip.
`PublishedReadView`/`PhysicalGenerationPins` protect C/G, and detached read
snapshots own their visible deltas. Reclamation filters only graph-family paths
through G; explicit branch-WAL pins remain a separate contract.

`HawDBAutomaticCheckpoint` checks complete transactions, suffix publication,
barriers, cancellation and crash choices. `HawDBImmutableRootBindings` checks
full physical-path closure when references share content. Their assumptions
must hold together with the carry model; separate green runs do not constitute
a machine-checked composition. Rust mutation guards connect Catalog/graph
classification and reader G to these abstractions. Physical recorder images
exercise actual checksum-bound closure and head publication/recovery paths.

The concrete closure planner also compares an already-bound physical path's
streamed content reference with the selected sealed root before publishing it
again. This enforces the model's immutable allocated graph contents when a
generation is carried. Identity-preserving byte corruption is not a new model
transition: the seven `api::tests::graph_bundle_integrity` probes exercise its
fail-closed implementation boundary, including unchanged head selection and
shared-handle invalidation. Admission/resource refusals remain distinct from
proven content-binding mismatches. Candidate bootstrap keeps private failures
isolated; after an integrity failure it rechecks selected-root bindings before
allowing existing readers to continue. A mismatch or incomplete diagnostic
invalidates shared readers. Successful verification isolates a private failure.
This diagnostic is failure-only and uses checkpoint work accounting.

This evidence excludes unbounded scheduler liveness, concrete OS/device
behavior, memory/descriptor accounting proofs, hash collision resistance,
arithmetic overflow and performance. Those remain governed by their owning
contracts and targeted executable qualification. No throughput or SF1-size
claim follows from the formal result.
