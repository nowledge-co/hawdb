# Bounded MATCH binding and normalization

## Representable contract

The generic clause pipeline uses `GraphMatch` until a structural normalization
can express the same operation with conventional operators. Its bounded
traversal supplies endpoint nodes. It does not supply a scalar relationship
binding for a multi-hop path, per-edge property filtering, an untyped traversal,
or a non-outgoing traversal. Single-hop matching has those capabilities.

Let `H` mean both hop bounds equal one, `T` mean a nonempty relationship type,
`O` mean outgoing direction, `R` mean a relationship variable is requested and
`P` mean relationship properties are specified. The admitted relationship shape
is

```
A = H or (T and O and not R and not P).
```

Endpoint properties are a different operation: filtering the selected endpoint
remains representable and is not rejected by this predicate. Unused path aliases
also retain their existing scope behavior. ALL SHORTEST and mutation commands
have separate lowering contracts; this change does not add capabilities there.

## Why validation precedes execution and normalization

Previously `bind_read_clauses` admitted every relationship shape. `GraphMatch`
checked its bounded limitations only after finding a source node and resolving
the relationship type. Thus the same unsupported query could succeed on an
empty graph and fail during execution on a populated graph. Moreover, converting
a matching logical shape to `Expand` could bypass that runtime guard and expose
different behavior, such as returning one relationship value for a multi-hop
binding.

`validate_bounded_relationship` now runs for every relationship step before the
binder constructs the `GraphMatchStep::Expand`. It returns a semantic error for
an unrepresentable shape, independently of data, optimizer choice or whether
normalization is requested. It retains the existing one-hop error wording used
by the legacy statement binder.

## Deductive argument

For `H`, validation returns successfully without rejecting single-hop features.
For `not H`, its four ordered tests reject `P`, `not T`, `not O` and `R`.
Reaching success therefore entails `T and O and not R and not P`; conversely
those four facts make every rejection test false. This proves success iff `A`
for this shape check. Other binding checks may still reject the statement.

Induct over relationship steps within a MATCH clause. Before the first step no
unsupported step has been emitted. For the next step, failure exits binding
before returning a logical plan; success establishes `A` before emitting that
step. Consequently a successfully returned generic MATCH plan contains only
admitted relationship shapes. The clause loop applies the same reasoning to
later MATCH and OPTIONAL MATCH clauses, including clauses that import earlier
bindings.

Normalization receives a plan only after successful binding, so it cannot turn
one of these rejected shapes into an executable `Expand`. No assumption about
the number of source rows or existence of a relationship type enters this
argument. In particular, an empty graph cannot bypass it. This is a
source-linked proof of the private binder boundary, not a proof of every
traversal algorithm or validation of manually constructed logical plans.

## Regression evidence

Planner tests cover the three ranges `0..1`, `1..2` and `2..2`, each of the five
unsupported forms (relationship binding, relationship properties, no type,
incoming and undirected), both MATCH and OPTIONAL MATCH, and both raw and
normalized planning: 60 rejected planning calls. Positive cases retain all five
single-hop forms, outgoing typed bounded traversal and bounded endpoint-property
filtering: 14 successful planning calls.

The embedded regression uses the public two-WITH pipeline entrypoint on empty
and populated graphs. All five unsupported forms produce semantic errors in
both states. Outgoing typed bounded reads return the two expected endpoints;
single-hop relationship projection retains its expected weight. Removing the
validation call makes the regression fail because the empty graph returns a
successful empty result. The source guard is restored after that negative probe.

The existing frozen corpus remains unchanged. This fix does not switch the
default MATCH parser, remove the old AST or complete issue #158.

## Default-entrypoint migration gate

A temporary, uncommitted default-MATCH routing experiment on main
`01e7ac8a7c7efe97cb54b28691abeca36fe897f3` routed MATCH to `Statement::Pipeline`
and normalized its bound plan. It was reverted after characterization:

- All 1,427 frozen cases were examined. `mem-0344` and `mem-0361` became
  parseable; binding still rejected missing parameters or the unsupported
  multi-pattern mutation. These require deliberate parser/binder-stage coverage,
  not silent changes to rejection expectations.
- Golden differences remained exactly `probe-0040` and `probe-0042`, already
  described in [WITH semantics](CYPHER_WITH_SEMANTICS.md).
- The full embedded Cargo suite had 1,579 passes, 29 failures and four ignored
  cases. Failures included observable statement classification, plan-cache
  accounting for newly routed mutations, host fast-path/readiness classification,
  and unsupported bounded shapes reaching conventional execution. They are not
  all equivalent-plan differences and must not be dismissed by updating counts.
- Existing tests that inspect `Statement::MatchReturn` also need actual migrated
  AST assertions. Retaining or bypassing those tests alone cannot establish the
  default-pipeline contract.

Before default routing, preserve typed read/write cache eligibility and semantic
statement/fast-path reporting, requalify negative query behavior, migrate AST
assertions, and rerun the complete parser, planner, embedded and fuzz gates.
