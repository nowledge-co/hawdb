# Search mutation publication: proof boundary

## Runtime boundary

`SearchOutOfCoreReader::open_with_lexical_policies` validates the complete
artifact closure before constructing a public reader. The validated reader now
shares target-bound visibility, retracted lexical statistics, metadata
candidate filtering, hydration and vector allowlists across every serving path.
Malformed, over-budget, or target-invalid mutation artifacts still fail closed;
cleanup can inspect and retain the same closure without exposing a partial one.

A successfully constructed reader may therefore contain a nonempty mutation-run
list, but every query path resolves `(content_segment_id, document_id)` before
returning a logical version. A delete contributes no visible document and a
replacement leaves only its fresh version visible. The public reader fixture
compares these results with a rebuilt one-segment corpus, including text,
scalar-vector, hybrid, metadata and hydration behavior.

This is a serving contract for validated immutable closures. Compaction remains
bounded: a selected range absorbs entries targeting its segments and publishes
the surviving outside-target entries as a replacement run. A complete target
closure therefore removes its run; a partial closure keeps the outside targets
without widening the selected range. Sustained workload, power-loss recovery
and whole-process RSS qualification remain separate gates.

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
that query code applies the identity. Those obligations are established by the shared serving-path differential
fixtures; writer selection and compaction closure are proved separately below.

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
  retention and public reader serving of a validated closure. The finite publication model below is
  unchanged and does not model these payload-level checks.

## Shared predicate and staged statistics implementation

The in-progress read implementation retains validated runs in
`MutationVisibility`. For one run, entries are ordered by
`(document_id, target_segment_id)`; a binary-search lower bound followed by a
bounded equal-ID scan finds every physical target for that logical ID. Testing
the target segment across that range, then taking the disjunction over runs, is
therefore equivalent to existence of the exact physical target pair.
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

The public reader fixture compares deletion and replacement reads
against a rebuilt one-segment corpus, including text scores, scalar vector and
hybrid results, metadata filters and hydration. It also exercises a RaBitQ
allowlist with a hidden predecessor. The fixture uses the same closure
validation as the public constructor. Complete target closures are absorbed by
compaction; partial closures rewrite the outside-target entries atomically, and
sustained resource qualification remains unfinished.

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
5. Compaction materializes visible versions from its selected closure, preserves
   their logical values, removes entries targeting selected segments, and
   republishes entries targeting outside segments. If the selected artifacts
   exceed the admitted budget, compaction defers.

Initially the content-only closure satisfies these properties. A replacement
adds one fresh version and a mutation bound to its predecessor's segment. The
old version becomes invisible while the replacement remains visible; deleting
adds only the predecessor-bound mutation. Binding a later replacement to the
currently visible version prevents retracting the same predecessor twice.
A global ID mask would hide both versions and violate this argument.

Preparation does not change selection. Flushing adds durable files; the active
selector replacement switches to an entire candidate closure. A stale candidate
is discarded, preventing it from resurrecting a version removed by a concurrent
publication. A crash before replacement leaves the old selector; a crash after
replacement leaves the complete new closure. This assumes atomic durable
selector replacement and truthful successful file durability, rather than
proving those primitives from filesystem behavior.

For a partial closure, let `S` be the selected content-segment IDs and split
the active entries into `R_S = {r | r.target_segment ∈ S}` and
`R_o = R \\ R_S`. The compaction writer emits exactly
`V_S = {v ∈ C | v.segment ∈ S and visible(v, R)}`. Publication replaces `S`
with one fresh segment containing `V_S`, removes `R_S`, and serializes `R_o`
as the new mutation run. Every entry in `R_o` still targets an unchanged
active segment, so its `(segment, document_id)` binding is preserved. The
logical result is therefore

```
(C \\ S ∪ V_S, R_o) = (C, R) \\ {v ∈ S | not visible(v, R)}
```

with the same visible IDs, document contributions and retracted statistics.
The staged count and reversible digest are checked against `V_S` before the
manifest CAS. This proves the partial-closure rewrite under valid immutable
artifacts; it does not prove filesystem durability beyond the existing
publish-last contract.

The byte-level recovery regression additionally writes a durable mutation run,
then replaces the active selector with an invalid/torn value before reopening
through the writer. Recovery chooses the last generation-specific manifest;
the orphaned run remains unreferenced and cannot hide the committed document.
This establishes the state-machine boundary for a failed selector publish, but
does not model arbitrary filesystem reorderings or claim power-loss durability
for an unverified storage device.

Pinning adds the selected closure to the protected union. Cleanup removes only
files outside that union; releasing a pin may shrink it but cannot remove the
active closure. During compaction, each visible logical value is copied to
fresh content, entries targeting selected segments are removed, and entries
targeting outside segments are republished. Thus logical values are unchanged
and no retained entry targets removed content. The selected artifact budget
still bounds the rewrite; it does not require closure expansion.

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

Issue #291 still requires crash/power-loss qualification across the real
publication paths and sustained workload write-amplification/RSS qualification.
Model success alone does not authorize enabling any of those paths.

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

The mutation serving fixture compares this retry against `Disabled`, checks
that `Required` fails at the allowlist/block budget boundary, and verifies
cancellation fails and dimension mismatch retains its distinct existing report.
Corruption is checked on reopen, after dropping immutable mappings. A separate
typed-error test includes misleading budget text in corruption, invalid-vector,
unsupported-kernel and I/O errors; none is classified as a resource fallback.


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


## Incremental append preserves old retractions and vector identity

For a validated closure `(C, R)` and an appended segment `N` whose IDs exceed
the physical maximum, append publishes `(C ∪ {N}, R)`. Existing run references
are moved unchanged into the new manifest. Since no entry of `R` targets `N`,
`visible(C ∪ {N}, R) = visible(C, R) ∪ documents(N)`. The sets are disjoint by
the append precondition; logical count and additive digest are therefore the
previous logical values plus the new segment contributions. Appending must not
subtract old retractions again or discard them. The public fixture
`mutation_append_preserves_retractions_and_logical_identity` executes real
preparation/publication/cleanup, reopens the validated closure, compares run
bytes and checks the old document stays absent without old-content hydration
during preparation. The complete-closure compaction fixture additionally proves
that the selected visible corpus reopens without stale mutation targets.

Incremental staging also inherits the active embedding dimension, including a
dimension with no model name. A vectorless new segment does not imply that the
remaining active segments are vectorless. Retaining that dimension preserves
old RaBitQ identity checks for append and partial compaction; any incoming
incompatible vector still fails the writer's existing dimension validation.
Tests cover vectorless append and vectorless compaction with an unselected
vector-bearing segment. The linked [cleanup proof](../SEARCH_CLEANUP_OWNERSHIP.md)
now additionally treats failed closure discovery as unknown retention rather
than evidence for deleting old generations.


## Guarded mutation continuation publication

Let `V = visible(C, R)`, `U` be the delta's unique upsert IDs, and `D` its unique
delete IDs, with `U ∩ D = ∅` (validated before staging). Exact visible-version
lookup yields `T`, one physical target for each ID in `(U ∪ D) ∩ ids(V)`.
All `T` targets are distinct and none is already hidden by `R`. Publish a run
for `T`, retaining its operation tags, and a new content segment `N` containing
exactly `U` if `U` is nonempty. Then

`visible(C ∪ {N}, R ∪ T) = (V \ targets(T)) ∪ documents(N)`.

The right-hand sets are disjoint by lookup and input uniqueness. Consequently
`count' = count - |T| + |U|`, and the additive document digest subtracts each
verified target contribution exactly once then adds the new segment digest.
Missing deletes contribute neither a target nor a subtraction. Repeated
replacements target the newest visible segment; previously hidden same-ID
versions cannot be selected. Deleting all visible versions yields count/digest
zero while retaining immutable physical artifacts until future compaction.

The continuation writer uses the operation ledger for entry slots, ID copies,
analyzer map/terms, output term vectors, controlled hydration and encoded run
bytes. It keeps target records transient and output retractions owned until
publication/drop. The stored per-run read/decode peak bound lets preparation
replay old-run prefix admission with the next closure's header/index counts;
new-run admission includes all prior retained ownership. Thus a new outer run
slot or content-segment index cannot silently invalidate a formerly admitted
old decode prefix. This is requested-capacity admission under the earlier
pinned decoder assumptions, not a whole-operation RSS proof. Legacy active
manifest decoding and all prior limits retain their existing boundaries.

At finish, the existing publication lease checks the expected active generation
before generation allocation. Retractions are encoded and admitted, content
and run files are installed and verified, and only then is the active manifest
durably replaced. Any earlier error leaves the active closure unchanged; staged
or orphan files cannot become selectable through that unchanged manifest.
There is no new claim about an OS error after rename but before directory sync:
that boundary follows the existing storage contract. The finite TLA+ protocol
model is unchanged; this argument maps the incremental mutation path to its
publish-last/CAS steps, not to a machine-checked Rust refinement.

Fixtures cover delete-only byte accounting and unchanged content references,
missing/repeated deletes, competing prepared writers, replacement/revival
against a rebuilt lexical corpus, repeated replacement and deletion to empty.
Budget and cancellation fixtures compare active-manifest bytes before/after
failure. These do not prove power-loss recovery or bounded sustained load.
The branch is reachable for validated mutation readers and clean readers whose
updates target a visible document. New-ID appends remain on the append path.
Complete target closures can be absorbed by compaction; partial target closures
are rewritten into a bounded outside-target run. The remaining power-loss and
sustained O(K) qualification gates are open.

The sustained lifecycle regression
`mutation_sustained_replacements_append_and_compaction_reopen_each_round`
provides the corresponding finite induction check. At the start of round `i`,
the reopened manifest has one visible version for every logical ID and a valid
immutable closure. The delta targets one visible version and appends one fresh
ID; the guarded-publication equation above therefore yields exactly one fewer
old version plus the two requested output versions. Compaction either leaves
the closure unchanged or replaces an adjacent same-level range with a digest-
preserving segment, while rewriting any outside-target mutation entries. A
successful reopen re-establishes the induction hypothesis for round `i+1`.
The test executes four rounds and checks document counts, replacement and
append hydration after every reopen. This is a finite state-machine witness,
not a proof of arbitrary-length RSS stability; the benchmark and production
corpus qualification remain separate.

`mutation_compaction_publication_failure_preserves_the_active_closure` covers
the failure edge of the same transition. It exhausts the admitted published-
byte budget after the compaction candidate has been staged, then checks byte-
for-byte manifest identity, successful reopen of the old closure, retained
mutation visibility, and removal of the private stage directory. Thus the
publication transition is observationally atomic at this boundary: either the
new manifest is committed with its complete closure, or the old closure stays
selected. Filesystem power loss between individual durability calls still
requires the platform fault-injection qualification.

The test-only `GenerationIo::replace` failpoint strengthens this boundary
without adding a production control plane. The
`compaction_replace_faults_never_publish_a_partial_closure` regression injects
an interruption at each artifact rename, including the active-manifest rename.
For every reachable boundary, the old manifest remains byte-identical, reopen
selects exactly the prior document set, and the private stage is gone;
unreachable failpoint numbers are treated as no-ops. This enumerates the
publication edges in the current implementation and provides executable
evidence for the publish-last invariant. The companion
`compaction_process_abort_never_publishes_a_partial_closure` test runs the
same compaction in a child process and terminates it at the first rename; the
parent then reopens the old closure and observes the orphaned private stage.
That models process loss, while an actual host power cut and filesystem
reordering still belong to the platform qualification described above.
