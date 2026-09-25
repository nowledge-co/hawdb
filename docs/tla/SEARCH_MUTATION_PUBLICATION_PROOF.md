# Search mutation publication: proof boundary

## Runtime boundary

`SearchOutOfCoreReader::open_with_lexical_policies` validates the complete
artifact closure and rejects any nonempty mutation-run list before constructing
a public reader. `load_artifact_closure` is private and returns integrity data,
not a query handle. Cleanup uses that helper, so rejecting an unsupported serving
format does not prevent discovery and retention of its artifacts.

All public reader constructors delegate to the guarded constructor. Therefore
any successfully constructed reader has an empty mutation-run list. Its existing
text, vector and hydration paths cannot silently ignore a mutation. Previously a
valid one-document deletion reported `document_count == 0` while hydration still
returned that document. The regression rejects that public open, preserves
validation errors for corrupt/over-budget runs, and checks that cleanup succeeds
and an already-open reader retains its previous snapshot.

This is an interim capability boundary, not implementation of mutation queries.
Remove it only when all read paths share target-bound visibility and exact
retracted statistics, with differential and recovery coverage. Production
writers currently publish empty mutation lists; development fixtures with
nonempty lists now fail open explicitly instead of returning inconsistent data.

## Exact target contribution validation

The runtime now adds `mutation_run::validate_targets` after structural closure
validation. The following algebraic argument establishes the retraction
precondition; it is not a machine-checked Rust refinement or a BM25 query proof.

Let `C` be the finite set of physical versions keyed by `(segment_id, id)`. For
an analyzer `A`, define each version's contribution as

```
F_A(v) = (1, weighted_length_A(v), t ↦ 1[t ∈ distinct_terms_A(v)])
D(v)   = DocumentsDigest(encode_search_document_line(v))
```

Let `R` be the finite list of run entries. Structural validation rejects any
entry outside the active segment set or any repeated target pair. Target
validation requires a lexical ID probe and exactly one hydrated record from
that named content artifact, then compares the recorded length, sorted distinct
terms and digest to `F_A(v)` and `D(v)`. Hydration checks the selected payload
range's integrity. Analyzer identity was checked when opening both the content
and run artifacts. The same admitted analyzer implementation used for lexical
delta construction computes weighted lengths and term deduplication here.

**Claim.** If these checks accept, every run contribution equals that of a
unique physical version, and subtracting them from the physical corpus leaves
exactly the contribution of the unretracted versions.

**Proof.** Existence follows from exact-artifact lookup and hydration; uniqueness
of the mapping follows from rejection of duplicate target pairs. Exactness
follows from component-wise equality with the reconstruction (ordered unique
term lists represent the term indicator function). For the empty prefix,
`sum(C) - 0 = sum(C)`. Suppose the result holds for the first `j` entries. Entry
`j+1` names a version still in the remainder, since its target is distinct from
the previous targets. Subtracting its equal contribution removes exactly that
summand. Induction gives

```
sum(v ∈ C, F_A(v)) - sum(r ∈ R, F_A(target(r)))
    = sum(v ∈ C \ targets(R), F_A(v)).
```

All integer components are nonnegative because each subtracted summand is
present. The corresponding digest identity holds in `Z/(2^64)`, using the
existing reversible modular sum; digest equality alone is not collision-free
content authentication. This proof assumes valid immutable content artifacts
and deterministic analysis. It does not prove a writer selected the previously
visible version, that replacement operations contain their new version, or
that query code applies the identity. Those obligations remain guarded.

The implementation processes one target at a time. By induction on the loop,
no prior hydrated target survives into the next iteration. Source hydration and
analysis retain their existing byte/token limits. This bounds the additional
source-record lifetime independently of `|R|`; it does not establish a global
RSS bound for retained run JSON or eliminate repeated range reads.

Runtime evidence:

- `out_of_core_mutation_retractions_must_match_physical_targets` recomputes the
  outer checksums and aggregate manifest after forging IDs, digest, length or
  term lists. Missing IDs outside and inside a descriptor range, another
  existing ID, and fabricated contributions must still fail; cleanup must
  retain the run and report a discovery failure.
- `out_of_core_mutation_retraction_binds_same_id_to_exact_content_version`
  distinguishes two physical versions with the same ID. Only the matching
  target version is accepted.
- `out_of_core_mutation_retraction_uses_weighted_analyzer_contributions` checks
  hand-calculated weighted lengths, unique terms and a stopword policy.
- `out_of_core_mutation_target_validation_obeys_hydration_budget` requires a
  target larger than the configured hydration budget to fail.
- The existing closure test still proves valid deletion inspection, pin-aware
  retention and public reader rejection. The finite publication model below is
  unchanged and does not model these payload-level checks.

## Protocol invariants

For content versions `C` and mutation entries `R`, define

```
visible(C, R) = {v in C | no r in R matches both v.id and v.segment}
closure(C, R) = content artifact identities union mutation artifact identities
```

The publication protocol requires:

1. Every mutation target still belongs to the selected content set.
2. A selected closure is fully durable before its selector becomes active.
3. A prepared publication is committed only if its base generation is still
   active. The check and selector replacement share the publication lease.
4. Cleanup retains the union of active, pinned and in-flight durable closures.
5. Compaction materializes visible versions from its complete selected closure,
   preserves their logical values, and removes the absorbed mutation entries.
   If that closure exceeds the admitted budget, compaction defers.

Initially the content-only closure satisfies these properties. A replacement
adds one fresh version and a mutation bound to its predecessor's segment. The
old version becomes invisible while the replacement remains visible; deleting
adds only the predecessor-bound mutation. Binding a later replacement to the
currently visible version prevents retracting the same predecessor twice.
A global ID mask would hide both versions and violate this argument.

Preparation does not change selection. Flushing adds durable files; the guarded
selector replacement switches to an entire candidate closure. A stale candidate
is discarded, preventing it from resurrecting a version removed by a concurrent
publication. A crash before replacement leaves the old selector; a crash after
replacement leaves the complete new closure. This assumes atomic durable
selector replacement and truthful successful file durability, rather than
proving those primitives from filesystem behavior.

Pinning adds the selected closure to the protected union. Cleanup removes only
files outside that union; releasing a pin may shrink it but cannot remove the
active closure. During complete-closure compaction, each visible logical value
is copied to fresh content and the absorbed entries are removed. Thus logical
values are unchanged and no retained entry targets removed content. Partial
selection requires a closure-expansion/budget algorithm not implemented here.

## Executable finite model

`HawDBSearchMutationPublication` checks the above publication rules on a fixed
finite workload: two documents, one reader, generations 0–5, two consecutive
replacements of document `a`, deletion, complete-closure compaction, and a
competing deletion of `b`. Payload values are separate from segment identities,
so remapping a segment during compaction cannot satisfy the oracle by changing
the document value. A prepared replacement can race the competing publication;
crash/discard, pin/unpin and per-file reclamation interleave at each stage.

The configured invariants are `TypeOK`, `ActiveClosureDurable`,
`PinnedClosureRetained`, `TargetsRemainBound`, `VisibilityMatchesLogical` and
`StaleNeverPublished`. TLC explores the complete finite safety graph; no fairness
or eventual-completion claim is made. The fixed candidate contents and expected
logical sets are explicit test oracles, not an implementation of a general
mutation planner. In particular, this does not prove arbitrary histories,
partial compaction, exact BM25 statistics, payload checksums, admission limits,
RSS, byte-level crash recovery, or Rust refinement.

Five registered mutants independently remove target-segment binding, the base
CAS, flush-before-publish, pin retention, or compaction target closure. Each must
violate its named invariant. Two additional intentionally false reachability
controls prove the repeated-replacement and compaction states are reachable.
A syntax error or unrelated failure does not count as a successful control.

## Relationship to existing proofs

The append publication model covers content-only checkpoint/WAL ordering and
pin retention. It does not imply mutation visibility: adding a tombstone changes
both the logical live set and the artifact closure. This model adds that bounded
protocol obligation, while the runtime gate prevents treating it as completed
serving support. The Source sidecar model remains about graph-epoch binding and
is not a substitute for this search mutation contract.

Issue #291 still requires writer integration, exact retracted statistics,
shared visibility across all query/hydration paths, RaBitQ handling, bounded
visibility-closure compaction and sustained workload qualification. Model
success alone does not authorize enabling any of those paths.

## Verification

```sh
bazel test //docs/tla:HawDBSearchMutationPublication_check
bash scripts/check-storage-tla.sh --check-mutants
bash scripts/check-storage-tla.test.sh
```

The positive configured graph has 3,267 generated states, 629 distinct states,
zero states left on the queue and depth 22. Record the tested source revision,
negative-control outcomes and Rust regression results in the PR; counts are
specific to this configuration.
