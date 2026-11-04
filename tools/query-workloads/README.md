# Query workloads

Private, unpublished query generation shared by developer tools. This independent
workspace depends on `hawdb-core` values, not the database facade, optimizer or
executor. Production crates must not depend on it. The root library package
excludes this directory.

The extracted Cypher AST/fixture generator and SQL shape generator retain their
original seed/index mapping. `tests/legacy.rs` checks a frozen inventory of 2,160
complete records from `612d010d4`: five seeds, 144 indices, Cypher and both SQL
index modes. Shiro continues to own execution, oracles, reduction and reporting.

`smith` adds an opt-in, type-directed recursive predicate generator. Integer,
text and nullable column scopes control expression construction; integer
arithmetic, boolean composition and comparisons recurse within a node/depth
budget. SELECT shapes cover inner/LEFT many-to-one joins, grouped/global
COUNT/SUM and ordered projection with LIMIT/OFFSET. Projection, join keys and
aggregate arguments remain constrained shapes; arbitrary subqueries, DML,
functions and recursive projection generation are outside this first subset.

The query root, relation/join/projection/order/limit skeleton and recursive
expression nodes all participate in conservative node/depth bounds. Relations
are a flat scope, not a recursive FROM tree. A depth below four disables joins.
The generator refuses an unrepresentable node budget instead of retrying until
it finds an easier query. Separate execution limits enforce row/payload budgets.

Both modes use the exact same fixture: two tables, eight bucket values, seven dimension keys,
nullable values, unmatched LEFT joins, duplicate non-key values and two secondary indexes. A template
uses one typed predicate; recursive mode expands typed predicates within the
same six shape families. ORDER BY uses a unique id tie-breaker. HawDB's current
aggregate subset does not support aggregate ORDER BY/LIMIT, so grouped output is
bounded by eight bucket values and compared as a bag. Output budgets below eight
use a global aggregate. See [query-lab](../query-lab/README.md) for the comparison
and frozen cost-baseline consumers.

The design learns from RisingWave's
[SQLsmith generator](https://github.com/risingwavelabs/risingwave/tree/34c424f2a037cd368ba19a229fc802ac1c800cbb/src/tests/sqlsmith):
type-directed recursion and explicit scopes are useful; permissive error skips
and stringified row comparison are not adopted. This is an original bounded
implementation, not a port of RisingWave's SQL grammar or execution harness.

Manual verification from the repository root:

```console
cargo test --locked --manifest-path tools/query-workloads/Cargo.toml
cargo fmt --manifest-path tools/query-workloads/Cargo.toml -- --check
cargo clippy --locked --manifest-path tools/query-workloads/Cargo.toml --all-targets -- -D warnings
```

The manual Bazel owners are `//tools/query-workloads:query_workloads_tests` and
`//tools/query-workloads:legacy_tests`, also selected by Shiro's local test suite.
