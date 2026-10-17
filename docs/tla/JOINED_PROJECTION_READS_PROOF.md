# Joined projection reads: conditional refinement

This qualifies the two SourceChunk query shapes in the audited Mem mapping at
`a78432d280609fedd94bc22c4c90da53867b1086`, through the existing embedded SQL
runtime. It does not move the actual host owner, construct complete C..E coverage,
or certify an imported baseline. The full MVCC acceptance ledger remains open.

## Shape and source map

`api/dependency_read/relational/joined.rs` accepts one canonical aliased INNER
JOIN whose ON clause is a conjunction of explicit child/parent column equalities
covering every parent primary-key component exactly once. Equality direction and
component order may differ. Aliases must be distinct and must not collide with
the other relation's table name. LEFT/multiple joins, OR/NOT/non-column ON,
partial/duplicate parent tuples and unknown columns refuse. The existing SQL
expression admission now also visits ON expressions. Catalogs, locking,
projection-bound sources and functions retain their restrictions.

The collector derives a necessary child scope from the existing complete-PK or
declared-index-prefix rules. For joined queries, only the explicit child alias
can supply a necessary equality; a parent predicate, OR or NOT cannot become a
child point lookup. Unsupported scalar/type or child scope qualification keeps a
whole-epoch dependency. SQL errors remain sticky rather than producing a token.

The complete parent tuple comes from either:

1. necessary child equalities under AND on every ON field (the document fanout
   page); or
2. a parameterized primary-key SELECT projecting only ON fields from the same
   pinned child snapshot (the exact chunk lookup).

The second path requires a complete child primary key; an insufficiently bound
prefix refuses instead of enumerating parents in host Rust. Identifier quoting
escapes embedded quotes; values remain parameters. Complete schema/key checks
precede query construction. The original JOIN, WHERE, projection, ordering and
LIMIT execute unchanged through the normal pinned SQL runtime. The collector
does not scan/filter/join data in a route-specific executor.

## Dependency argument

For a child point k, let F_E(k) be the child at the pinned epoch E. Its row stamp
protects existence, join fields and payload, including an absent child. If it
exists and all join fields are non-NULL, the complete typed parent tuple P is
fixed by F_E(k). Record the parent row identity even when the parent is absent
or a parent residual (such as owner_kind='source') filters it out. Otherwise a
later insert or ownership transition could change an empty result undetected.
No other child or parent can affect this point query. A missing child or NULL
join component yields no matches under ordinary equality; the child row stamp
alone protects that result against relevant changes.

For a fanout domain with a necessary binding of every child ON field to tuple
P, every candidate joins the same parent P. The complete child prefix plus P's
row identity is sufficient. The prefix covers all members and payload changes,
including off-page/absent matches and movement in/out. LIMIT/keyset residuals do
not reduce the dependency. Parent changes and child-domain changes both reject;
unrelated domains and parents can proceed. The argument holds independently of
physical join order, access choice or which rows happen to be returned.

All reads use the existing immutable transaction captured with dependency/history
registration before the first query. The hidden lookup cannot switch to live
state. The final token shares the original canonical instance and pins; the
existing exclusive publication boundary validates the entire dependency set.
Schema/database/read-summary barriers and WAL publication are unchanged. See
`RELATIONAL_INDEX_PREFIX_READS_PROOF.md` for the old/new/payload summary proof
and `RELATIONAL_PROJECTION_READ_VALIDATION_PROOF.md` for point identities.

NULL does not identify a parent. UUID input and projected values use canonical
SQL coercion. Floating parent/child identity domains retain whole-epoch fallback;
no new equivalence between SQL comparison and index total ordering is assumed.
Composite keys retain every typed component in schema order. A type mismatch
keeps a whole-epoch dependency after complete join-shape validation.

## Admission and limits

The join directory and parent tuple contain at most 32 components. Tuple/scalar
preflight admits at most 4 KiB before cloning; oversized static tuples fall back
conservatively. A dynamic lookup projects at most one row / 4 KiB, respecting any
stricter caller statement limit and using the same cancellation/deadline context.
It does not project unrelated body fields. An oversized dynamic key refuses,
even when the original JOIN might have returned no rows. Generated identifier
SQL is preflighted to 64 KiB before escaping/formatting; normal SQL lexical,
parameter, expression and dependency admission still applies. Errors in the
lookup, original JOIN or dependency insertion invalidate the whole prepared read.

This adds at most one bounded internal statement per qualified point JOIN.
Its work is additional to the original query and its ordinary execution report;
these limits are logical metadata/output bounds, not total physical I/O, query
work, allocator/RSS or latency bounds. Cold canonical validation/cache fills and
physical join planning still have their existing contracts. Cross-statement
cumulative work and bounded host fanout continuation remain required for the
artifact-owning session. No stable Mem inclusion/activation is authorized.

## Finite model and limits of refinement

`HawDBJoinedProjectionReads` models two children and two distinct complete parent
identities, NULL/absent children, absent/filtered/source parents, projected field
updates, point and first-page queries, capture-before-query and two atomic
commits. The oracle independently recomputes pinned/current output and complete
child/parent dependency history. Primary row stamps always publish; only prefix
summaries depend on active read tracking. The existing storage models supply
the assumed atomic commit, WAL, retained-history and barrier contracts. This
model does not repeat or replace their persistence qualification.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9, jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`:
**33,258 generated / 26,056 distinct states**, complete depth 6, positive exit 0.
Thirteen faulty controls and one disjoint-progress witness all return exact
expected invariant violations with TLC exit 12. They cover late tracking,
omitted/returned-only/current-only parents, absent-child omission, missing
old/new/payload group invalidation, narrowing to the returned page, omitted
parent field changes, wrong alias/parent identity, and ignored read errors.
Earlier syntax-error development runs are excluded.

Parent identities abstract whole typed tuples; SQL coercion, quoting, tuple
component validation and alias binding are source/native premises. Child payload
abstracts projected/residual/order changes within a complete root scope; the
model does not claim to implement SQL's complete ordering/type semantics.
Finite exploration and the conditional argument are not an unbounded proof of
Rust, a complete mapper, cross-store recovery, fairness or scaling.

## Native regressions

`tests/mvcc_joined_reads.rs` exercises point and actual keyset-page
shapes on hot/checkpointed OutOfCore sources, pinned parent changes before the
first query, disjoint parent commits, absent/filtered/deleted parent changes,
absence and NULL child joins, and off-page updates. Composite parent keys retain
all components and reversed ON operands; foreign/OR predicates stay broad.
Partial/duplicate keys, ambiguous aliases and unsupported ON shapes refuse.
Quoted canonical identifiers and 1 MiB unused body fields test the generated
lookup's semantics and projection. UUID parent keys and floating fallback,
dependency exhaustion and the separate 4 KiB dynamic guard limit are covered.

The pre-split Cargo probes pass all seven `joined_reads` tests and the separately
added exact audited SourceChunk statement test (eight tests in total). The latter
copies both SQL strings verbatim from the audited host revision and uses a
reduced synthetic projection schema; it does not certify the full Mem DDL/import
or mapper contract. Tests are discovered in both existing Bazel facade profiles.

A separate detached production baseline at
`436aa97d7e58227f7c3282fac2df790340019a3f` receives the exact final test file,
with the shared test fixture and test-only Bazel target definitions, without
production shims or API assertion changes. Bazel selects eight
tests: seven positive JOIN cases fail at the old unsupported-join boundary; the
error/admission negative control passes. The test attachment and logs are retained
locally; its independent Bazel output is cleaned and shut down after the run.
Command: `bazel test //:hawdb_mvcc_joined_reads_tests --test_output=errors`.

The initial combined 72-test default facade action reached its existing
300-second timeout under the full matrix; the minimal profile passed 72/72.
That adverse log is retained. JOIN tests now have dedicated default/minimal
Bazel actions (8 each); the original publication actions retain all 64 tests.
Both use `tests/support/mvcc_fixture.rs`. All 72 names and the exact eight JOIN
test bodies are preserved; no timeout, assertion or seed is reduced. This is
a test-action grouping change, not an engine performance measurement.

Final Bazel verification passes **130/130 targets** (4 executed / 126 cached).
Both original publication actions pass 64/64 and both dedicated JOIN actions
pass 8/8. Their action times are 216.3s / 241.5s (publication default/minimal)
and 291.5s / 192.2s (JOIN default/minimal), with all original 300s limits intact.
The unchanged storage/search/SQL/executor, both owner consumers, all required
local fuzz entrypoints, sixteen registered models and evidence-script checks are
included. These contention-sensitive debug timings are not a latency guarantee.
