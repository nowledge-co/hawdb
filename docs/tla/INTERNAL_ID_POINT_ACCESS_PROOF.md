# Internal-ID root access: conditional refinement

This removes unrelated full-node ownership from qualified ID roots needed by
prepared Search hydration. It does not issue batch coverage, certify a baseline,
or implement the artifact-owning C..E session. The complete acceptance ledger in
`docs/specs/MVCC_VERSIONED_PUBLICATION_SPEC.md` remains in force.

## Source map and semantic argument

`executor::scan::ids` extracts one necessary `IdEq`/`IdIn` leaf for the current
variable through AND only, at depth at most 32. It admits at most 1,024
nonnegative integer entries. Unsupported types, negative values, OR/NOT roots,
other variables and larger bounds retain existing execution. Eligibility is
checked before creating the sorted candidate directory. Candidate capacity is
reserved before allocation; the permit survives all point reads. Sorting and
deduplication preserve the ascending-ID, once-per-node order of ordinary scans.
The input list, parser/plan, and storage cache keep their existing accounting.

The elementary implication is: if P is a conjunction containing `id(n) IN S`
(or equality as a singleton), then every row where P is true has id(n) in S.
Induct on the number of enclosing AND nodes. No analogous implication holds for
OR, NOT, a different variable, or an absorbed access predicate. The implementation
keeps the original residual in its original executor; it does not rewrite the
predicate to TRUE or infer a complete result from a visited-key count.

For a pinned source G, let V be visible nodes matching the label pattern, U the
sorted unique candidate IDs, and R the original residual. A scan returns the
ordered sequence of n in V with R(n) true. By the implication above it is exactly
the sequence obtained by visiting U, skipping missing/tombstoned or wrong-label
nodes and evaluating R unchanged. Repetition in the input list does not multiply
source entities. Intersecting additional ID predicates still happens through R.
The type/NULL fallback preserves the evaluator's three-valued semantics instead
of imposing a new coercion rule. Unsupported input is not an empty-result proof.

Three source owners use this rule:

- `scan/owned.rs` uses a necessary ID in its already qualified storage filter.
  `batch/transforms.rs` only derives that filter when its predicate refers to the
  scan variable. The complete original predicate still runs on each binding.
- `scan.rs` uses it only for LabelScan projection access. Property/index access
  can carry a condition removed from the residual; bypassing it would be wrong.
  Projected point reads copy only the required properties.
- `graph_match.rs` uses the clause's predicate for an unbound node pattern.
  Existing bound/import/null handling, pattern tests, final predicate, expansion
  and the whole-clause optional boundary remain in their original owners.

Native full point reads use `node_with_allocation`. Projected reads use
`projected_node_owned_in_labels_admitted`; resident labels and cold encoded
headers reject wrong-label IDs before projected Value ownership. Both use the
same immutable store/reader for preflight and decode, preserving overlays and
tombstones. A custom reader's default label-aware projected method retains its
admitted post-read filter; it does not promise native before-copy label pruning.
Unsupported admitted point reads still fail closed. The old projected method
retains its signature and delegates with no label restriction.

The key permit precedes the key Vec. A source permit precedes clone/decode and
survives residual evaluation, recursion and the consumer callback. Existing
output owners admit row copies independently. Admission refusal, cancellation,
consumer Stop and consumer error unwind the same owners. A failed attempt must
not be retried by silently scanning instead. Existing IdEq/IdIn pruning reports
make selection visible; they report logical candidates, not physical I/O or RSS.

This argument is conditional on complete point lookup, label preflight, source
pinning and the existing allocation contract. Canonical block validation and
cache fills can still read/retain encoded data beyond the selected record.
Adjacency, output expressions, cross-statement work, report retention and the
whole C..E artifact session need their own governance. This is not a total
physical-I/O, allocator/RSS, elapsed-time or fairness proof.

## Finite state check and non-vacuity

`HawDBInternalIdPointAccess` independently computes the expected ordered result
from visible/label/residual/access sets and three-valued ID membership. It models
short repeated inputs, a NULL representative outside the key domain, AND/OR/NOT,
a foreign variable, absorbed indexed access, key admission, source admission,
copy, consumer ownership, release, cancellation and normal completion. One key
has an infeasible source payload; a successful point path can skip that unrelated
key. Refusal is allowed instead of partial successful completion.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9; jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`:
**596,480 generated / 546,720 distinct states**, depth 8, no invariant failure.
Ten controls incorrectly extract through OR/NOT or a foreign variable, bypass
indexed access, omit label filtering/residuals/deduplication, skip key/source
admission or release the source grant before consumption. Each returns TLC
exit 12 for its exact registered invariant. A separate exit-12 witness reaches
successful point progress while an unrelated infeasible source key is present.
Parser/runtime errors are not successful controls.

The finite model abstracts actual bytes, codecs, capacity rounding, native
bindings, dependencies and physical I/O. Its two-key instances do not prove
arbitrary cardinalities; the set/ordering argument above and native regressions
supply the corresponding conditional refinement, not a mechanized Rust proof.

## Native evidence

`internal_id_roots_avoid_unrelated_payloads_in_resident_and_cold_sources` uses
real GraphStore resident and checkpoint-open OutOfCore sources. Unrelated and
wrong-label nodes have 1 MiB + 137 byte bodies. Ordinary, projected and generic
MATCH roots select two small nodes with repeated/unsorted IDs and an absent ID.
All return IDs [1,2] within a 64 KiB query budget, report IdIn pruning, make no
body-sized allocations, release ledger grants and stay within the ledger cap.
This is an allocation observation around execution, not an OS RSS measurement.

`internal_id_root_access_matches_full_scan_boolean_and_type_semantics` compares
all three access paths against a generic full-root enumerator using double NOT
(which preserves three-valued truth but prevents necessary-ID extraction).
Thirty-six predicate/list combinations cover repeated/missing/wrong-label IDs,
empty sets, NULL, Float, negative values, a 1,025-entry fallback, intersections,
OR/NOT and foreign variables. Another case ensures an absorbed property access
cannot be bypassed by an ID residual. The oracle uses the original evaluator
with independent full enumeration; it is not a separate expression evaluator.

`prepared_hydration_uses_internal_id_roots_without_owning_unrelated_bodies`
exercises the actual prepared parameterized hydration helper with a large
unrelated Memory, a small selected Memory and optional HAS_LABEL mapping, in
Materialized and checkpoint-open OutOfCore modes under a 128 KiB query budget.
The result is the single expected Search row; duplicate/missing roots do not
advance a watermark or force whole-database dependency validation.

The pre-change source is `26135ddc7c83e221124ac13b0925bdff8f8606c1`.
Attaching only these two test files to that source produces two failing tests:
the allocation regression reports resident generic MATCH plus all three cold
root paths exceeding their budgets on an unrelated body; actual hydration fails
for cold node-only input. The independent semantic-oracle test passes. The
detached baseline uses its own Bazel output directory. Final broad-test outcomes
are recorded in the PR; no detached-worktree executable substitutes for current
source checks.
