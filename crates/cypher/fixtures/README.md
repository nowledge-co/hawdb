# Cypher migration baseline

These fixtures freeze parser and logical planner behavior before the AST
restructuring in [issue #158](https://github.com/nowledge-co/hawdb/issues/158).
They do not freeze the AST's Debug representation, change query execution, or
qualify Mem activation. Existing owner and runtime tests remain in place.

`migration_corpus_v1.jsonl` contains 1,427 independently identified cases:

| Origin | Cases | Meaning |
| --- | ---: | --- |
| Mem runtime source | 973 | Static literals from the source inventory; callers include legacy Kuzu paths. |
| Legacy snapshot source | 64 | Queries against the immutable Kuzu bootstrap snapshot. |
| Mem test source | 6 | Test-only literals, including legacy schema setup. |
| HawDB owner test | 286 | Static inputs from parser tests and four selected runtime test modules. |
| HawDB bound test | 4 | Thread read queries with the actual parameters from existing owner tests. |
| Synthetic binding probe | 94 | Explicit artificial parameters for planner migration coverage; these are not observed Mem requests. |

All 1,043 Mem inventory rows at revision
`5cc46b559383c4a1d945ce7981c12d61fc630678` are retained, including rejected queries.
The inventory owner is `hawdb_evidence::query_inventory::scan_nowledge_query_inventory`.
Each row was matched unambiguously against independently decoded Rust literals
using Syn 2.0.118. The scanner's continuation/UTF-8 fix is included in the HawDB
baseline, `fd146ca3bc7df39d9656fb877aa74dcdba5ac9d1`.

The `query` field is the exact decoded literal, including whitespace and trailing
semicolons. `normalized_query` is retained for matching the existing inventory;
it must not replace the input under test. Source paths, physical line numbers,
function/constant names and the 94 Mem source SHA-256 hashes are recorded. The
manifest also records historical HawDB source hashes and examples for all 41
pre-migration Statement variants. Those variant names describe historical coverage
and are not assertions about the new AST representation.

There are 1,370 parser acceptances and 57 rejections. Of the 43 Mem rejections,
38 are Kuzu snapshot syntax, two are legacy schema setup tests, and three are
currently unsupported legacy Mem runtime queries (`memory_evolves.rs:413`,
`memory_evolves.rs:1168`, `repo.rs:2071`). The other 14 are intentional parser
negative tests. No rejected input is silently excluded.

Planner outcomes are recorded separately: 394 exact logical-plan goldens, 954
missing-parameter outcomes, two vector binding rejections, 20 statements requiring
session/runtime handling, and 57 parser rejections. A missing binding is not evidence that planning or
execution succeeded. Explicit binding probes exercise all 34 plannable historical
Statement variants; the remaining seven are session/runtime wrappers and controls.
This is not complete bound-parameter or execution coverage for every Mem call.

Eleven golden cases contain `CURRENT_TIMESTAMP()`. Each lists the exact typed
create/assignment slots produced by that expression. Tests check each generated
value against the interval around planning, then replace only those slots with
zero for comparison. Other integers, including values in the same time interval,
remain untouched. No clock injection, broad integer masking, source rewriting or
wall-clock snapshot is used.

The manifest names the static extraction exclusions. Assertion strings, partial
fragments and formatted/generated inputs remain covered by their existing tests.
The backtracking campaign and optimizer DDL combinations are not reduced to static
samples. Other runtime test modules are retained but were not exhaustively scanned
for this fixture. Keep execution, budgets, cache identity and recovery assertions
in their existing owners during migration.

Intentional behavior changes require review of the affected case and golden;
do not regenerate expected values automatically in a test. Preserve case IDs,
source text and historical provenance when adapting the harness to the new AST.

`migration_main_1550_scalar_regex_peg_baseline_v1.jsonl` records the 44 reviewed
statement/pipeline transitions introduced by issue #1550. The original
expression/predicate and diagnostic fixtures remain immutable: scalar-expression
left operands now parse as regex predicates, while binding and execution retain
responsibility for rejecting non-string patterns and evaluated operands.

`migration_main_1489_unbounded_var_length_peg_baseline_v1.jsonl` records the 2
reviewed statement/pipeline transitions introduced by issue #1489: an unbounded
variable-length relationship pattern (`*`, `*n..`) now parses. Execution
enumerates its trails and refuses one longer than the streaming depth limit.

`migration_main_1488_pattern_predicates_peg_baseline_v1.jsonl` records the 87
reviewed statement/pipeline transitions introduced by issue #1488. Each case was
already accepted and stays accepted with a new AST: a `WHERE` pattern predicate
whose source node is anonymous (`NOT ((:Item)-[:REL]->())`, `()-[:R*1]->()`)
used to become the one-hop existence predicate over that anonymous name, which
no row binds, so it was always false. It is now an existential subquery over the
whole pattern. The one-hop predicate is kept only from a bound, unlabeled source
to an anonymous target; named targets, labeled sources, multi-hop paths, type
disjunctions and relationship properties are part of the pattern. No frozen
case changes between rejection and acceptance.

Run the default owner and complete local fuzz surface:

```sh
bazel test //crates/cypher:presubmit_tests //crates/plan-cypher:presubmit_tests //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
```
