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

## Shared predicate and staged statistics implementation

The in-progress read implementation retains validated runs in
`MutationVisibility`. For one run, strictly ordered unique document IDs make
binary search return its unique matching ID exactly when that ID is present.
Testing the returned entry's target segment, then taking the disjunction over
runs, is therefore equivalent to existence of the exact physical target pair.
Negating that disjunction implements `visible(C, R)` below. It deliberately does
not negate existence of the logical ID alone: repeated replacements may retract
versions in segments 7 and 8 while leaving the same ID in segment 9 visible.

`LexicalCorpusStatistics::retract_documents` implements the subtraction identity
above for the bounded query-term map. It rejects malformed term sets and checked
count, length or DF underflow; after each subtraction it also requires every
queried DF to be at most the remaining document count, and an empty corpus to
have zero length. Query terms absent from a version contribute zero. By the
distinct-target induction above, valid physical retractions satisfy these
conditions in any order. The implementation mutates a clone and assigns it back
only after the entire sequence succeeds. Thus rejection at any prefix preserves
all original statistics, including byte-read evidence. Content-only queries skip
this staging allocation.

`corpus_retractions_match_rebuilding_every_subset` checks all subsets of a
three-version corpus, including a zero-token document, in both forward and
reverse order against a fresh sum of the remaining contributions.
`corpus_retractions_are_exact_and_atomic` exercises valid subtraction to an
empty corpus and failures after a valid prefix. The repeated-replacement
visibility test distinguishes physical versions with the same ID.

An internal guarded-reader fixture compares deletion and replacement reads
against a rebuilt one-segment corpus, including text scores, scalar vector and
hybrid results, metadata filters and hydration. It also exercises a RaBitQ
allowlist with a hidden predecessor. This fixture intentionally bypasses only
the public capability guard after full artifact validation. It does not prove
that all serving obligations are complete: writer publication and
mutation-aware compaction remain unfinished. The public guard
is still mandatory, and these tests do not authorize removing it.

## Layout-independent RaBitQ candidate ordering

Mutation closures can overlap physical ID ranges. Therefore `(layer, ordinal)`
is not a logical tie-breaker: with a one-candidate budget, a live `z` in an older
layer would beat replacement `a` in a later layer at the same approximate score,
whereas the ID-ordered merged projection selects `a`.

For mutation closures, `retain_logical_projection_hits` maps each layer's local
candidates to visible logical IDs before global heap retention. Missing,
duplicate, unordered or hidden ordinal mappings fail. `LayeredProjectionHit`
orders equal scores by ascending logical ID. Content-only closures preserve
physical ordering and avoid this metadata I/O. This argument assumes immutable
valid artifacts and one visible version per logical ID; establishing that
writer invariant and enabling the reader are separate remaining obligations.

For a fixed total candidate order, each layer's local top K contains every
candidate from that layer that could belong to the global top K: an excluded
candidate already has K better candidates in its own layer. Within an artifact,
ordinal order agrees with ID order, so local tie ordering restricts the global
logical ordering consistently. It is therefore sufficient to retain the best K
from the union of local top K lists. The bounded heap maintains this by induction
on candidate insertion: insert while under capacity, otherwise discard a
candidate no better than the worst retained one, or replace that worst one.
Resolving IDs only after truncation would not satisfy this proof.

ID capacities are charged before modifying the heap, including credit for an
evicted ID. An admission failure leaves the heap and byte count unchanged and
propagates as an error rather than returning partial candidates. Subsequent
projection scans subtract retained ID bytes from their working budget. The
allowlist is dropped before mapping, and retained tie IDs are released before
raw rerank creates its own ID collector. Metadata payload decoding retains its
separate existing range/decoder limits; this is not an aggregate RSS proof.

`mutation_rabitq_equal_scores_match_merged_logical_id_order` exercises the
one-candidate overlapping replacement case against a rebuilt projection. The
negative control restores physical tie selection and must choose the wrong ID.
`logical_candidate_ties_ignore_layer_order_and_reject_over_budget_atomically`
checks heap selection and unchanged state on ID-budget failure. The top-K
argument concerns a shared approximate scoring order, not equality of ANN and
exact nearest-neighbor recall or a proof of numeric kernels.

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


## Aggregate run admission and failure-specific fallback

Let `L` be `max_mutation_working_bytes`, `R_i` the retained charge after `i`
runs, `B_i` the input capacity, `D_i` the decode preflight bound, and `I_i`
the reserved target-index charge. Initialization admits the outer run vector
and active-segment index. Before reading, the loader checks the previous
retained charge plus twice the encoded length and read scratch. Before serde,
it requires `R_i + B_i + D_i + I_i <= L`. Successful decode measures owned
capacities `O_i`, requires `O_i <= D_i`, and retains only `O_i + I_i`.
Thus, assuming the preflight bounds below, induction gives `R_i <= L` at every
prefix and admission of each transient decode alongside prior ownership.
A rejected admission does not change the counter. Streaming body checksums
avoid a second full serialized body allocation.

The schema-specific scanner counts objects, arrays and scalar strings outside
quoted/escaped content. Scalar strings matter because retraction terms are a
`Vec<String>`, not a vector of JSON objects. Three times the element-header
counts conservatively cover pinned geometric growth with old/new allocation
overlap; array minima cover small allocations. Encoded input length covers
owned string payloads, while eight times the longest token plus fixed scratch
covers token decoding/error workspace. These are implementation-dependent
bounds for the pinned Rust/serde behavior, not a language-level allocator or
RSS theorem. Validation sets reserve the existing conservative per-entry
allowance. Content mappings, target hydration/analysis, allocator metadata and
OS residency are outside this run-capacity claim.

`mutation_decode_preflight_covers_scalar_terms_and_escaped_strings` checks
tracked peak allocations for small through 16,385-term arrays and escaped
Unicode/long strings. `mutation_working_budget_rejects_combined_runs_before_second_decode`
checks a one-byte boundary and two individually admissible runs that cannot
coexist; failed admission preserves the counter. The public-loader fixture
checks wiring and the zero-run exemption. These finite tests supplement the
conditional induction; they do not prove all serde inputs or whole-process RSS.

For vector fallback, partition compressed failures into typed `Budget` and
`Failure`. Native `ResourceBudgetExceeded` maps to the former; other native
errors and ordinary `HawDBError` conversions map to the latter, regardless of
message text. `Required` propagates either partition. `Preferred` discards the
compressed attempt only for `Budget`, then invokes the same scalar path as
`Disabled` with identical visibility, candidates, limits and task context.
Consequently, if that scalar invocation succeeds its result is the exact scalar
result; otherwise the query fails without publishing partial candidates.
Accumulated I/O remains recorded, and the report marks the fallback explicitly.
The admitted-byte receipt conservatively records the available cap, not a
measured allocation peak. Cancellation checkpoints still apply to the retry.

The guarded mutation fixture compares this retry against `Disabled`, checks
that `Required` fails at the allowlist/block budget boundary, and verifies
cancellation fails and dimension mismatch retains its distinct existing report. Corruption is checked on reopen,
after dropping immutable mappings. A separate typed-error test includes
misleading budget text in corruption, invalid-vector, unsupported-kernel and
I/O errors; none is classified as a resource fallback. These arguments do not
authorize modifying mapped files or removing the public mutation-reader guard.


## Composition with lexical block-max pruning

Physical posting bounds remain valid after hiding versions: the maximum term
frequency of a subset cannot exceed the original block maximum. Both scoring
and its upper bound use the same retracted live-corpus DF, count and average
length. For valid positive live DF, IDF is nonnegative; BM25 increases with
term frequency and decreases with document length. Therefore evaluating the
physical maximum TF at length zero still bounds every visible posting, even
when retractions change IDF and average length. Hidden records never enter the
collector, so its floor is formed only from live candidates. Existing strict
skip/tie rules thus preserve the live retained window. A term with live DF zero
has no visible contributor and its stream can be omitted entirely.

The out-of-core hybrid path composes pruning with the target-bound visibility
predicate; text mode keeps exhaustive counting. The regression
`block_max_pruning_with_retractions_matches_rebuilt_live_corpus` removes a
high-scoring rare hit and common postings, compares retracted statistics and
pruned scores to an independently rebuilt live corpus, and requires a positive
skipped-block count. This extends the pruning argument to mutation visibility;
it does not establish sustained performance or arbitrary floating-point error
bounds beyond the existing scorer's assumptions.
