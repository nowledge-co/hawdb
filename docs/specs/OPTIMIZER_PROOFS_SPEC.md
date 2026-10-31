# Bounded optimizer proofs and local qualification

Issue Number: close #1366

The graph optimizer derives semantic facts from the pinned schema and supported
logical operators. Statistics, NDV and observed cardinality never establish a
key, a non-NULL value or an equality. The optional rewrites remove proved
redundant work while retaining unsupported evaluation boundaries.

## Proof domain

The pass runs after existing logical normalization and before physical lowering.
It visits at most 2,048 logical nodes, with depth at most 128. Facts retain at most
64 scalar fields, bindings and equivalence entries, eight keys of at most eight
fields, and names/literal strings up to 128 bytes. Predicate inspection shares a
64-node budget and depth 16; supported scalar recursion also stops at depth 16.
Exhaustion loses facts and retains operators. One skipped budget event makes a
whole-plan traversal limit visible.

| Source/operator | Facts and limitations |
| --- | --- |
| Node scan | Internal entity identity is a key. Public declared BOOL, INT and STRING properties carry enforced types and nullability. Public non-null properties, active EXISTS constraints, or checked filters can discharge the NULL condition on UNIQUE. |
| Filter | Supported pure equality and non-null predicates add constants, non-NULL and equalities in matching enforced comparator domains. At most one row follows only from a completely fixed proved key. Opaque/mixed numeric domains receive no equality proof. |
| Projection | Pure scalar aliases preserve mapped facts and keys. All expressions read the input scope. Duplicate/oversized aliases and unsupported expressions invalidate proof safety. |
| Cartesian product and supported equality filter | A composite key retains both input identities. A unique property may identify its own component, but one side is never a stream key after fanout. Column/binding collisions fence proofs. |
| Distinct and aggregation | Complete output tuples/grouping tuples establish keys. Empty ungrouped aggregation stays intact. No inference from truncated output inventories is permitted. |
| Limit | Preserves facts and bounds output cardinality; it does not make an unsafe producer safe. |
| OPTIONAL, traversal, paths, opaque/external producers and mutation | Fences. The pass neither assumes facts nor descends to rewrite through these boundaries. |

Arithmetic, unsupported functions, volatile or otherwise opaque evaluators are
conservative safety barriers. This matters when removing a blocking operator
could let LIMIT skip a later error. Float and generic ANY properties retain
their operators unless another independent proof suffices.

Projection retains only explicitly mapped proof facts. An ORDER BY on an
unprojected source property therefore remains a conservative fallback, even if
the runtime still carries that graph binding. Projected constant aliases can
elide sorting.

## Optional rules

Each application records a structured `Applied` event. Disabled matching rules
record `Skipped`. Essential normalization, legality, capability, authorization,
memory admission and resource checks cannot be disabled.

| Rule identifier | Rewrite |
| --- | --- |
| `transformation:proof_redundant_ordering` | Remove constant or repeated equivalent sort keys; remove sorting when supported input order or a one-row bound proves it unnecessary. Every evaluated key and the producer must remain safe. |
| `transformation:proof_redundant_distinct` | Remove deduplication when the projected tuple contains a proved key or there can be at most one row. Nullable UNIQUE alone is insufficient. |
| `transformation:proof_singleton_group` | Replace nonempty unique grouping plus pure non-distinct COUNT(*) with projection and count 1, only after an already values-only producer. Generic projection preserves graph bindings while grouping clears them; ordinary graph-scoped grouping is retained. Other aggregates remain unchanged. |

Values-only column/literal projection transfers size their complete incoming batch from
borrowed values before allocating output. This prevents an otherwise redundant
group's replacement from demanding the full 16-MiB payload ceiling under a
2-MiB query budget. Oversized rows still fail against the configured ceiling.
Duplicate aliases acquire `#N` suffixes and retain the existing admission path.
The sizing fast path is bounded to 64 unique aliases of at most 128 bytes;
computed expressions, retained graph bindings and scoring annotations keep the
existing admission path. The preflight does not evaluate expressions twice or skip their errors.

Semantic cache identity separately records public table/property states and
EXISTS constraints for proof-sensitive logical shapes. Existing statistics
identity intentionally ignores lifecycle churn. An old reader can retain its
valid proofs and cannot publish them into the current schema's physical cache
identity. Unrelated point-query cache behavior is preserved.

## Embedded qualification API

`DatabaseReadTransaction::qualify_optimizer_query(QueryRequest, options, search)`
prepares and executes one ordinary read on the transaction's pinned snapshot.
It uses the regular parser, planner, capability/access checks, executor,
cancellation and output limits. Qualification bypasses physical cache admission
and has no global or environment control plane. Mutations/write procedures and
transaction/system controls are rejected.

`OptimizerQualificationOptions` can disable the three optional rules and attach
`OptimizerCostPerturbation::new(seed, percent)`, with percent in 0..=25. Stable
fingerprint hashing perturbs ranking among the existing legal ordered-access and
Sort/TopN alternatives. It does not perturb every join/index decision, change
search budgets, alter raw reported costs or admit illegal plans. Defaults do not
compute the extra ranking fingerprints.

The outer result covers preparation/planning failures. Successful preparation
returns `OptimizerQualificationOutput` with the actual selected trace, snapshot
epoch and an inner execution result, retaining plan evidence on runtime error.
Owned successful rows remain charged while held by the caller. `verify_rules`
accepts up to six required/forbidden assertions over structured Applied events;
mere diagnostic mentions, estimates and skipped rules do not satisfy them.

## Local Shiro workflow

Existing DQP now sends identical statement text and parameters through typed
search selection and records the same plan it executes. Both its normal
campaign coverage and the opt-in qualification report separate different-plan
agreement from same-plan agreement. Arbitrary matching errors remain failures.

`qualify_optimizer_read` loads a fixture through ordinary embedded APIs, pins one
reader, and compares the baseline with one to eight typed variants. Each variant
can carry up to six rule assertions. Typed rows, multiplicities, NULLs, declared
ordering, statement digest and snapshot epoch participate in qualification.
Reports retain exact configuration and execution errors; missing fingerprints
never count as alternative-plan coverage. Generous identical budgets support
semantic comparisons; plan-specific resource errors are surfaced as failures.

```console
cargo run --locked --manifest-path tools/shiro/Cargo.toml --bin shiro -- qualify --seed 7 --case-index 0 --disable-rule transformation:proof_redundant_distinct --forbid-rule transformation:proof_redundant_distinct --cost-seed 42 --cost-percent 25
```

The command emits replay JSON containing the original mutations, statement,
parameters and the qualification configuration. A generated untyped case may
legitimately select the same plan. Typed UNIQUE/NULL, OPTIONAL/fanout, runtime
error and near-tie bounded-sort fixtures independently exercise rule changes and
real executable alternatives. Shiro stays in its existing unpublished tool
workspace and local-only Bazel suites. No fuzz CI lane is added.

The manual `property_proofs` benchmark compares source revisions through ordinary
streamed reads, checks every output, and includes small/bulk, warm/forced-cold
planning, nullable/computed/point controls and unrelated schema metadata. It is
not a performance gate. Performance results belong under `docs/benchmarks/`
with procedure, aggregate results, provenance and limitations only.
