# Scoped graph read collection

This extends eager E guards to qualified physical-ID roots and single-hop
queries. It supplies bounded retained dependencies for a future complete C..E
session. It does not authorize interval coverage, fanout, a mapper, a stage,
a target incarnation or a watermark, and does not itself change the capture contract.

## Source correspondence and conditional argument

The normal [plan collector](../../src/api/dependency_read.rs) still selects
supported shapes and rejects hidden adjacency predicates/unqualified operators.
The [graph collector](../../src/api/dependency_read/graph_match.rs) validates all
root ID values and the existing root-count cap even when a scope dominates the
physical keys. A root restricted to label L may use an already-held complete
L domain; an unlabeled root requires the all-node domain. A qualified single
hop can omit its adjacency/relationship/neighbor scan only if the complete root
label, target label and relationship type domains are held. Unlabeled targets
and untyped relationships require the respective all-domain guards. Existing
Database-equality captures also dominate these keys. Missing protection retains
the prior precise collector and its work/entry/byte refusal; it does not silently
claim a closed read. Previously captured keys are not deleted.

Given the existing supported-shape qualification, a pure single-hop query is a
function of the scoped root nodes, target nodes, relationship identities and
properties, parameters, and stable schema/catalog interpretation. Unchanged
complete domains therefore imply unchanged query source state, including
negative root IDs, filtered neighbors, empty results and LIMIT pages. This is
stronger than guarding only returned rows. Result shaping, direction, predicates
and execution remain in the ordinary query runtime. A scope does not waive
query result/runtime/storage limits or integrity checks.

Read scopes keep their shared Database, Schema and ReadBarrier checks. Domain
lookup refuses to subsume after sticky refusal. Lookup borrows an ordered key
family, starting at its None key and stopping at the next variant; it does not
clone input names or scan later per-node keys. VersionKey's derived enum order
makes each variant's keys contiguous and orders None before Some. This is a
logical lookup/retention argument, not physical RSS, I/O, or latency evidence.
The generic kernel record method still does not erase arbitrary physical keys:
only a query collector with the source-shape premise may omit them.

## Finite abstraction

[HawDBScopedGraphRead](HawDBScopedGraphRead.tla) represents root, target and
relationship domains, wildcard source shapes, correct/global/partial/wrong-name
held scopes, sticky prior refusal, two bounded source writes, failed metadata
precision via a barrier, schema change and validation. Its result oracle retains
all required source classes. Equality of this over-approximation suffices for a
pure query's source equality; it is not an implementation of Cypher evaluation.
The scope-only capture models the saturated budgets used by native tests. The
fallback precise collector is an existing separate premise, not proved here.

The direct local TLC run uses the same pinned tool jar SHA-256
936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88.
The positive model checked 56,640 generated / 55,040 distinct states at complete
depth 5. Nine faults (missing root/target/type, wrong names, named scope mistaken
for wildcard, ignored refusal/barrier/schema, whole-epoch fallback) and one
success reachability witness produced their expected invariant counterexamples.
All eleven checks also passed in the final selected Bazel matrix. The model
remains a finite conditional abstraction, not mechanized Rust refinement.

## Native evidence and remaining work

The first default seven-test integration run passed, then passed again after
strengthening existing-owner/edge fixture assertions. It includes both present
and absent physical IDs under one label key, complete hops under three keys,
six missing/wrong/wildcard negative cases at saturated entry budgets, and eight
hot/cold interleavings. Endpoint and relationship property changes after E reject
publication; unrelated node-kind writes allow it. Deliberately empty candidates
exercise source validation only and confer no output-completeness authority.

The final allocation-free lookup draft passed default 7/7 (23.73 seconds) and
minimal 7/7 (16.26 seconds). These timings are qualification observations, not
benchmarks. Strict locked workspace/all-target/all-feature Clippy also passed on this draft.
The final fixed-source 189-target Bazel matrix passed, including all 72 selected
formal checks and 98 required local fuzz targets. Default/minimal scope profiles
passed 7/7 each (18.55 / 16.97 seconds); owner-feed profiles also passed 7/7 each.
Full Cargo Storage passed 1,953 tests with zero failures and 30 existing ignored
tests, plus one compile-fail doctest. Separate Bazel read-dependency selection
passed 32/32 with 1,951 filtered. Formatting, strict workspace Clippy and all-file
prek passed. Exact scope, source tree and retained evidence are recorded in the
[endpoint component qualification](OWNER_ENDPOINT_CAPTURE_PROOF.md). Integration
with the owned C..E session remains pending. The separate [V2 endpoint capture](OWNER_ENDPOINT_CAPTURE_PROOF.md) addresses
relationship effects in the interval; complete fanout/mapping custody and all
original binding/vector/recovery/performance gates remain open.
