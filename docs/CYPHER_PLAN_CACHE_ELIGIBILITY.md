# Cypher pipeline plan-cache eligibility

Clause-pipeline migration (#158) must preserve the cache policy of the legacy
statement shapes. Previously `statement_uses_plan_cache` admitted every Pipeline,
including mutations and vector procedures that bypass the cache in their legacy
representation. A public `CALL vector_search(...) WITH ... WITH ... RETURN ...`
query already reaches this path without enabling the broader MATCH migration.
This is an eligibility/accounting defect; it is not evidence of stale results.

## Policy and source-linked proof

Let a pipeline be the finite clause sequence P = [c1, ..., cn]. Define A(c):

- true for MATCH (including OPTIONAL MATCH), WITH and RETURN;
- true for CALL GraphAlgorithm, preserving the existing PageRank/Louvain policy;
- false for CALL VectorSearch and CALL ProjectGraph;
- false for UNWIND, CREATE, MERGE, SET and DELETE (including DETACH DELETE).

UNWIND is conservatively excluded to preserve the legacy UnwindMutation policy;
this does not assert that every UNWIND query mutates data. Adding a general
read-only UNWIND caching contract is separate work.

Define E(P) = (n > 0) AND (for all i in 1..n, A(ci)). The Pipeline arm in
`src/api/plan_cache.rs::statement_uses_plan_cache` implements exactly E with a
nonempty guard and `Iterator::all`. The clause and procedure matches are
exhaustive, so adding a new enum variant requires an explicit classification.

For the iteration, let b0 = true and bk = b(k-1) AND A(ck). Induction on k gives
bk = the conjunction of A(ci) for 1 <= i <= k: the base is the empty conjunction;
the induction step adds exactly the next predicate. Short-circuit evaluation
preserves this result because false AND x = false. The separate nonempty guard
rejects n = 0. Therefore the implementation returns true iff E(P). Inserting
any excluded clause at any position forces E to false. Inserting or reordering
allowed clauses preserves eligibility for a nonempty sequence, but does not
establish that the sequence is a valid Cypher query.

Both the database and read-transaction planning entry points in `src/api/mod.rs`
use this predicate when optimizer search is Auto. False selects
`Bypass(StatementNotCacheable)`. The bypass path in `optimized_query_plan_for`
records a bypass and returns without cache lookup/admission; the cache-use path
alone inserts the optimized plan. Non-Auto optimizer directives retain their
own bypass reason. Planning/binding errors can return before a bypass is counted.

This proves the classification and its admission consequence under the existing
call-site control flow. It is a deductive source argument, not a machine-checked
proof of Rust, parameter substitution, optimizer equivalence or execution.
Semantic binding remains responsible for clause order, scope and procedure
restrictions. Legacy statement arms and the existing CypherQuery wrapper policy
are unchanged. Cache keys, parameterization, invalidation and budgets are unchanged.

## Regression evidence

`pipeline_plan_cache_preserves_legacy_eligibility` compares eleven parsed query
shapes in legacy, pipeline and CypherQuery-wrapped pipeline form, covering graph
algorithms, mutations, UNWIND and both excluded procedures.
`pipeline_plan_cache_checks_every_clause` inserts eight excluded clause fixtures
at all six positions of a read pipeline (48 cases), checks the allowed
MATCH/OPTIONAL MATCH/WITH/RETURN sequence, and rejects the empty sequence.
The synthetic insertion fixtures test classification independently of binding.

The public explain regression calls a vector procedure with two WITH clauses
twice: both calls bypass and leave entries, admissions, hits and misses at zero.
The public read regression proves that ordinary two-WITH queries remain cached:
for two parameter values it checks miss then hit and the exact executed row.
This pipeline currently uses exact parameter variants, so changing the value
creates a separate entry rather than asserting cross-value template reuse.

The pre-fix predicate admits mutations, an inserted UNWIND and the public vector
pipeline; the corresponding regressions fail their eligibility/bypass assertions.
These tests constrain migration policy without enabling default MATCH routing or
claiming the rest of #158 is complete.
