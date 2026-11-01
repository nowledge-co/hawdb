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
The public read regression proves that ordinary two-WITH queries remain cached
and return the exact executed rows for each parameter value. These statements
are normalized like every other read (#1425), so a parameter used only as a
node-property key is a template slot: a new value reuses the entry, and the
multi-WITH regressions check the rows after each such hit.

The pre-fix predicate admits mutations, an inserted UNWIND and the public vector
pipeline; the corresponding regressions fail their eligibility/bypass assertions.
These tests constrain migration policy without enabling default MATCH routing or
claiming the rest of #158 is complete.

## Physical retention and parameterized CASE

Each graph physical-plan cache has two typed `DatabaseConfig` limits:
`max_physical_plan_cache_bytes` defaults to 16 MiB and
`max_physical_plan_cache_entry_bytes` defaults to 8 MiB. Zero disables admission;
`max_plan_cache_entries` remains an additional independent entry-count limit.
These limits apply independently to each live cache, including private read
and transaction caches. Total retention grows with the number of live handles;
it is outside `query_memory_bytes` and is not an aggregate database or
process-memory limit. Shared cache accounting is tracked separately in
[issue #1371](https://github.com/nowledge-co/hawdb/issues/1371).

Weighted admission charges requested heap reachable from the complete key,
physical IR and optimizer trace, including container spare capacity, Arc control
blocks and shared payloads, and the cache table's capacity high-water mark.
Sharing is charged in full per reference. The single-entry limit also includes
the inline key/value layout. Checked overflow or opaque, potentially growing
owners refuse admission and execute the freshly bound plan normally. Compiled
regex programs are opaque, so cached templates retain only their validated
pattern sources. Each execution compiles its own program before evaluating rows;
that program never populates the cached source. Regex patterns keep their exact
parameter guards and invalid patterns still fail before cache reuse. The counters and limits
are exposed by `plan_cache_stats()` and the derived `system.plan_cache` metrics
`max_bytes`, `max_entry_bytes`, `charged_bytes`, and `rejected_admissions`.
Admission decisions distinguish accepted and refused weighted entries. Accepted
admission can still be followed by LFU eviction; it does not promise a later hit.

Parameterized CASE marker trees now use physical template reuse under these
limits. Parameter classification and exact guards still determine safe reuse;
unsupported function-valued CASE branches can retain exact value variants.
Schema, statistics environment, optimizer settings, ACL policy and scoring shape
remain part of the key. Neither query result budgets nor cancellation checks
are changed. Oversized entries must not replace an existing hot entry.

Requested heap is not RSS: allocator rounding, fixed cache metadata, caller-owned
in-flight clones and concurrent misses are outside this bound. The separate
shared logical binding cache retains its existing byte limit. The relational
SQL sibling cache owns only parameter-neutral parsed SQL, never physical plans;
it remains count-limited and is not covered by either graph byte budget.
The native allocation oracles and replay owners are in [TEST_INDEX](TEST_INDEX.md).
