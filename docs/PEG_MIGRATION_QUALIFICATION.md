# PEG migration qualification

This is an incomplete qualification record for PR #1379. The published runtime
under review is `a6c936c2a380e17768352c227e9299cca3dd8e0f`; the independent
Cypher reference is `fd10958dd37b8878151aeb73ff0477d189b5db5e`. The incremental
predicate/punctuation diagnostic and diagnostic-buffer repair is source-bound
to the hashes in the diagnostic index. Native/resource checks pass; final-head
fuzz, CI and public E2E evidence must be recorded separately after publication.

The acceptance criteria are [#1375](https://github.com/nowledge-co/hawdb/issues/1375)
and [#1328](https://github.com/nowledge-co/hawdb/issues/1328). Issue #1375 requires
preserving public diagnostics/invariants with reviewed transitions called out.
Its initial proposal to reuse the internal PEG predates the owner's subsequent
rust-peg experiment request; SQL retains the owned memoized engine, while Cypher
uses compiled `peg = 0.8.6` without packrat memoization. No workspace crate was
split. Owner comment 6581188647 retains the diagnostic, allocation and throughput
gates and asks that the PR remain draft. Proposed review dispositions below do
not constitute owner acceptance or independent approval.

| Requirement | Evidence obtained | Remaining work |
| --- | --- | --- |
| All supported SQL and Cypher entries use PEG; remove legacy dependency/drivers | Owned SQL family grammars and compiled Cypher grammar; parser/package tests; maintained Cargo/Bazel metadata | Rebind the prior entry-point inventory to this revision |
| Auditable AST, spans, semantic rejection boundaries and query results | Independent 21,944 Cypher outcomes; SQL and Decimal fixtures; 254 Cypher, 114 SQL, 36 syntax and 26 plan-cache tests | a6 CI26 success/6 skips; rebind the unpublished change to final API/CI evidence |
| Public diagnostic invariants and reviewed transitions | Prior repairs retain independent provenance; compact tokens preserve all 22,980 parent rendered outcomes; 1,590 further exact repairs; 2,523 remaining records indexed | Review all 83 transition groups, fix actual diagnostic defects, record proposed changes and obtain required dispositions |
| Byte/token/work/node/language-nesting bounds | Existing 16 MiB/1M/4M/262144/32 guards and monotonic accounting remain; byte-range tests and small-stack checks pass | Complete per-limit source/boundary qualification; explain work-unit differences and validation-order transitions |
| Memo admission and reuse where applicable | SQL keeps the memoized bounded engine; Cypher has no memo table or cached nesting path | SQL source tests and Cypher alternative are linked below; guard-precedence transitions still need dispositions |
| Default/minimal/WASM embedded facade | Final strict workspace Clippy, portable14 and documented WASM strict Clippy pass; public AST and facade signatures unchanged | Compilation evidence does not establish browser execution |
| Cargo/Bazel/maintained locks/test ownership | All required checks and documentation updated; no new workspace crate or production parser configuration | Verify final dependency and source closure |
| Local fuzz and MVCC checks | Published a6 revision: all98 fresh single-attempt required fuzz tests pass; BEP audit verifies exclusive graph intervals | Bind exact MVCC command/coverage and final CI outcomes |
| Parser cost and throughput | Native tokens24->12 bytes; all195 heap controls nonincreasing against parent; parser throughput+25.7665% in unchanged five-case experiment | Current-main public E2E32 comparison passes on a6 (combined +1.3199%, 95% interval 0.8455..1.7943); 5 wide and16 boolean metrics still regress against former main |

The [diagnostic index](benchmarks/cypher_peg_diagnostic_review_2026_11_03/README.md)
is exhaustive for membership, derived from the frozen transition index, complete fresh paired capture before the current performance experiment. The prior
4,113-record derived index was verified against all current parent outputs.
It contains triage buckets and complete paired examples, not completed source
reviews.

The resource counterexample list is checked in with
[compact-token measurements](benchmarks/cypher_peg_compact_tokens_2026_11_03/heap-summary.json).
These regressions remain explicit. Bounded admission, AST equivalence, good
throughput and passing fuzz are separate evidence; none authorizes silently
accepting the remaining diagnostic or allocation tradeoffs.


## Incremental diagnostic repair under qualification

The predicate default expectation is now `identifier`, with a PEG failure
production retaining the operand token after unary minus. Matched punctuation
terminals name their expected character. Required-character failures retain the
legacy cursor convention; list/map boundaries instead name both separator and
closing delimiter, without consuming an unexpected character. End-of-statement
failures take precedence over equal-position speculative call/property failures;
committed comma/closing-delimiter failures and farther failures retain priority.
This restores the independent COUNT/ID incomplete-call outcomes while preserving
the page-rank missing-comma regression.

The formatter writes directly into one exactly sized String, displaying escaped
near-context without an intermediate String and retaining byte/line/column
semantics. The multibyte-message regression covers this boundary explicitly.
The fourth provenance-bearing repair fixture checks1,548 distinct query/entry
pairs covering1,590 exact old outcomes. Complete accepted ASTs/spans and rejection
classes remain unchanged across21,944 records. All previously matching independent
messages remain unchanged. Another159 changed outputs were already incompatible;
they remain in the2,523-record review index, with full paired examples. These
facts do not accept the remaining transitions or allocation tradeoffs.


## Source review of admission mechanisms

The Cypher implementation in `parser/frontend/rust_peg.rs` clamps test overrides
to production maxima in `step_limit`, `node_limit` and `nesting_limit`. `enter`
charges captured-rule work before matching and checks value/structural depth;
`node`, `finish_projection` and `finish_order` enforce logical node/edge budgets.
`rewind` discards captures while retaining logical charges and fatal errors.
`recognize_inner` checks `fatal` before returning a successful tree. The combined
work/depth guard currently uses one internal expected-label string: classifying
a refusal's precise cause from that string alone would be unsound. This does not
establish equivalent work units to the removed VM.

The default byte guard is in `frontend.rs::check_input`; the lexer checks the
exact one-million-token boundary before AST construction. Current tests include
`peg_limits_fail_closed_and_keep_utf8_positions`,
`token_budget_checks_the_exact_boundary_before_ast_construction`, and
`nesting_budget_includes_statement_wrappers`. The first also proves that test
overrides cannot raise the production nesting bound.

SQL retains its separate memoized engine. `sql-syntax/src/peg/tests.rs` contains
`memoized_subtree_cannot_bypass_language_depth_admission`,
`independent_language_budgets_survive_memo_reuse` (asserting actual memo hits),
`repeated_memo_references_each_count_toward_the_edge_budget`, and
`rule_aliases_and_cached_lookahead_keep_exact_logical_budgets`. These tests run in
the existing syntax package owner. Cypher has no memo table, so no cached subtree
can bypass its guards; charged capture rules and constant-cost terminal/action
helpers are its bounded alternative. Source review of these mechanisms and
passing tests are evidence for bounds, not acceptance of every changed refusal
message or every old/new admission threshold.

## Incremental performance evidence

The unchanged five-query, 11-sample, 20,000-iteration default-release benchmark
ran in eight counterbalanced processes against the independently built former
parser (`fd10958dd37b8878151aeb73ff0477d189b5db5e`). All five queries improve.
Equal-weighted throughput improves 24.4622 percent; the four paired gains average
24.4683 percent (paired t 95% interval 22.7528..26.1838). This is a separate
experiment from the published a6 measurement. No compiler/test/benchmark
interference was observed; other host activity was uncontrolled. See the
[source-bound report](benchmarks/cypher_peg_diagnostic_review_2026_11_03/parser-timing.json).
The 195 heap controls do not increase against a6; five wide and sixteen boolean
metrics still regress against former main. No remaining diagnostic or allocation
tradeoff is accepted by these measurements.
