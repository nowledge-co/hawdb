# PEG migration qualification

PR #1379 remains incomplete. The preceding published head is
`3f9271bc738035fff73d6a2cc890398ecf238b76`. The combined runtime was tested before
commit at tree `893beda491ac25bf584bcfe05b31b593a80671eb`, based on that head. Independent Cypher outcomes use
`fd10958dd37b8878151aeb73ff0477d189b5db5e`. Runtime hashes in the
[combined evidence](benchmarks/cypher_peg_final_qualification_2026_11_03/README.md)
bind the measured candidate; results from an earlier head retain that scope.

The acceptance criteria are [#1375](https://github.com/nowledge-co/hawdb/issues/1375)
and [#1328](https://github.com/nowledge-co/hawdb/issues/1328). They require public
diagnostic invariants with reviewed transitions called out, rather than an
unqualified promise of identical strings for every malformed input. Owner
comment 6581188647 retains the diagnostic, allocation and throughput gates and
asks that the PR remain draft. Proposed dispositions do not constitute owner
acceptance or independent approval. The later rust-peg experiment request
supersedes the issue's original implementation proposal: SQL retains its owned
memoized engine, while Cypher uses compiled `peg = 0.8.6` without packrat
memoization. No workspace crate was split.

| Requirement | Evidence obtained | Remaining work |
| --- | --- | --- |
| Every supported SQL/Cypher entry uses PEG, without legacy fallback/dependency | Seven entry chains reach PEG; seven maintained locks and fifty manifests exclude sqlparser; lexical family dispatch does not retry another parser | Bind the source inventory to the final committed revision |
| AST/spans, rejection boundaries and query results | 21,944 independent Cypher outcomes unchanged; SQL/Decimal fixtures and prior API/plan-cache tests pass; combined candidate passes 255 Cypher tests, one ignored | Combined native/query/API checks pass; committed-head checks remain pending |
| Public diagnostic invariants and reviewed transitions | All 10,845 Parse errors pass independent byte/line/column/context recomputation; 187 further exact repairs, no lost reference matches; 2,336 remaining differences in 73 stable groups | All 73 source-mechanism reviews and full paired records are available; obtain required maintainer dispositions |
| Bounded matching and language nesting | Cypher retains 16 MiB/1M tokens/4M captured-rule work/262144 nodes-edges/32 nesting; rollback charges stay monotonic; SQL memo/depth tests remain | Combined default/minimal/WASM and 8/8 stack checks pass; admission-order dispositions remain pending |
| Existing SQL byte admission | SELECT/EXPLAIN and syntax entries retain 16 MiB; DDL/DML preserve large single literals/comments with token/work/capture/memo/expression guards | This is not a uniform finite byte cap for every statement family |
| Cargo/Bazel/locks/test ownership | No new crate, parser configuration or public AST type; existing package owners retain tests | Native strict checks pass; final documentation checks and relevant Bazel targets remain |
| Required local fuzz and MVCC | Published 3f head passes all 98 fresh single-attempt tests and terminal CI; baseline MVCC failure is reproduced and corrected in the candidate | Final combined-head fuzz/CI/MVCC, preserving the original failure evidence |
| Parser cost and throughput | Combined candidate uses 8-byte tokens; all 195 heap controls are nonincreasing against parent | Four wide and sixteen boolean metrics still regress against former main; new throughput attempt unqualified due to process-observation interruption; final E2E pending |

The [diagnostic index](benchmarks/cypher_peg_diagnostic_review_2026_11_03/README.md)
contains every remaining corpus index, paired examples and complete-rendered
hashes. The fifth independently sourced fixture checks all 187 distinct repair
cases. Another 50 vector-return controls preserve every acceptance/rejection
class and existing reference match, restore six exact controls, and retain
fourteen diagnostic differences; these overlapping controls are not added to
the main corpus count. Neither invariant checks nor exhaustive membership
constitutes acceptance of changed diagnostic wording, locations or precedence.

## Source review of matching and diagnostics

`Builder::step_limit`, `node_limit` and `nesting_limit` clamp test overrides to
production maxima. `enter` charges captured-rule work before matching;
`node`, `finish_projection` and `finish_order` guard logical nodes/edges.
`rewind` retains logical charges and fatal errors; `recognize_inner` checks fatal
admission before returning a tree. The combined work/depth guard uses one label,
so a precise counter cause cannot be inferred from that message alone. These
work units are not equivalent to the removed VM's instructions.

The byte guard is `frontend::check_input`; the lexer tests the exact one-million
token boundary before AST construction. Current regressions include
`peg_limits_fail_closed_and_keep_utf8_positions`,
`token_budget_checks_the_exact_boundary_before_ast_construction` and
`nesting_budget_includes_statement_wrappers`. The 8-byte token representation
covers full admitted lengths/offsets, EOF and UTF-8 boundaries, with a compile-time
size assertion for supported builds.

SQL's `peg/tests.rs` includes
`memoized_subtree_cannot_bypass_language_depth_admission`,
`independent_language_budgets_survive_memo_reuse` (asserting actual memo hits),
`repeated_memo_references_each_count_toward_the_edge_budget` and
`rule_aliases_and_cached_lookahead_keep_exact_logical_budgets`. Cypher has no memo
table; captured-rule charges and constant-cost terminal/action helpers are its
bounded alternative. SQL's `non_select_statement_bodies_preserve_independent_input_limits`
checks the intentionally retained large INSERT literal and DDL-comment behavior.

The new repairs keep `:=` atomic in PEG, preserve the first farthest failure,
report missing vector-return columns from matched projection spans, and retain
specific range/fulltext index diagnostics. Unterminated string tokens fail only
when a string production is admitted; invalid/dangling escapes retain their
original lowering-time errors. Two malformed project_graph controls retain a
precise string-token error: the removed parser incorrectly treated `[` as a
quote delimiter and reported unterminated string at EOF. Their source-based
review is separate from exact repairs and remains pending disposition.

## MVCC qualification failure and correction

The published PEG head's optimistic run passes, but pessimistic seed42/case147
fails. The exact saved input also fails on independently built current main
`870a6edd77fa0913dd8ff47bbb200d67a80cd364`: after a same-value point UPDATE commits,
a previously reading transaction's broad UPDATE is admitted instead of requiring
a snapshot retry. A new API regression fails before the correction.

Table-lock validation checked payload summaries, which do not advance for
same-value or absent-key write intents. The candidate also validates the existing
primary-key version range, retaining the metadata/stamp budgets and fail-closed
behavior. No recovery format or durability policy changes. Source-bound storage
7/API18 tests pass, including both residency modes and a table-guard work-cap
assertion. Exact replay and three 30-second campaigns pass: optimistic 56,234,
pessimistic 164 (all twelve families and both terminal directions), and range
34,570 (all ten families). These are prototype results with exact modified-file
hashes, not a claim that the published 3f head passed the failed gate.

## Preceding committed-head performance evidence

On 3f, all 98 required local fuzz labels pass fresh and once. BEP auditing verifies
graph halves of 218.180 and 153.312 seconds, a 63 ms gap and no overlap with other
suite tests. CI is terminal: 26 successes, eight configured skips, no failures.
The unchanged five-query/11-sample/20,000-iteration release parser benchmark in
eight counterbalanced processes improves equal-weighted throughput by 24.4622
percent; all five improve. Four paired gains average 24.4683 percent, with a
95 percent t interval of 22.7528 to 26.1838.

A separate 32-process public-library comparison against current main 870a uses
the unchanged 64-row/two-query fixture, normal warm cache, eleven samples and
2,000 calls. Full ordered results are checked outside timing. Point lookup's
paired gain is 3.2949 percent (95 percent t interval 2.6396..3.9503); aggregate
pagination is 0.4917 percent (-0.0322..1.0156, uncertain). Combined throughput
gains 1.2104 percent (0.7470..1.6738), with all sixteen pairs positive. Mean CPU
is 1.8554543125 seconds main / 1.8296183125 seconds PEG. No compiler/test/benchmark
interference was observed; other host activity was uncontrolled. These are local
experimental intervals and do not qualify the uncommitted combined candidate.


## Combined runtime qualification status

At the runtime tree above, exact formatting, strict workspace/all-target/all-feature
Clippy, full prek, query15/cache26, parser255+114+36, storage7/concurrent18,
portable14 and documented minimal WASM strict Clippy pass. The small-stack protocol
passes all eight checks: optimization levels1/3, cold/warm256/512 KiB, all21,944
inputs matching each build's own8 MiB reference. Subsequent review data and
documentation retain the same runtime hashes and repeat the required checks.

The throughput attempt stopped before timing: other compiler/test work was
observed, and a process observation exceeded the existing two-second guard.
No qualified timing receipt or samples were produced, and no guard was weakened.
This result cannot pass or fail the throughput gate. New committed-head fuzz,
CI, MVCC and current-main public E2E evidence must retain their actual source
scope; preceding 3f results are not substituted for them.
