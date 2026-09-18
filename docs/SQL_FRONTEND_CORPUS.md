# SQL frontend differential corpus

The ordinary `hawdb-sql` tests run the same complete SQL text through
`prepare_postgres_sql` and `hawdb_sql_syntax::parse_postgres_statement`.
This originated as the corpus prerequisite for [#159](https://github.com/nowledge-co/hawdb/issues/159).
It records frontend migration gaps; production routing remains unchanged.
Creation-binder outcomes are maintained separately in `frontend_create_bindings_v1.json`.

## Inventory

`crates/sql/fixtures/frontend_corpus_v1.jsonl` contains 1,007 concrete cases.
Each has a stable ID, statement family, source path/symbol, adaptation, expected
outcome for each frontend, and an optional named divergence waiver.
`frontend_corpus_manifest_v1.json` pins source hashes, case membership, family
coverage and waiver scope. `frontend_source_inventory_v1.json` classifies all
187 SQL-prefixed Rust string literals found in the ten audited source files.

The audit starts at `1977888050c405adc873ebcccfec77c3352f4659`.
The first #157 CROSS JOIN slice added 13 cases without changing the original 981
outcomes. The HAVING/comma-FROM continuation adds another 13 cases and changes
only `case-0991` and `case-0994` from production rejection to parser acceptance.
Their SQL and IDs remain unchanged, and their provenance moves to the new
`having_from.rs` tests. The former still requires an execution-time ON scope
error, and the latter requires grouped-column validation; syntax acceptance alone
does not qualify execution. The `production-from-having` waiver is now unused and
removed. All other prior outcomes and waivers are retained. Existing source line
changes track test registration and the two moved inputs.

| Source | Concrete cases |
| --- | ---: |
| `crates/sql/src/tests.rs` | 31 |
| `crates/sql/src/tests/cross_join.rs` | 11 |
| `crates/sql/src/tests/having_from.rs` | 15 |
| `crates/sql/src/tests/clause_diagnostics.rs` | 7 |
| `crates/sql/src/parser/clause_tests.rs` | 291 |
| `crates/sql-syntax/tests/postgres_pgq.rs` | 38 |
| `crates/sql-syntax/tests/postgres_select_pgq.rs` | 29 |
| `crates/sql-syntax/tests/support/alias_boundaries.rs` | 506 |
| `crates/sql/src/pgq/tests.rs` | 16 |
| `crates/sql/src/pgq/lowering/tests.rs` | 7 |
| Frozen Content Store statement corpus | 39 |
| Frozen Content Store schema | 13 |
| Local ALTER/CREATE INDEX/DELETE counterparts | 4 |

The Rust inventory was extracted with `syn` string-literal decoding, including
macro token streams. Diagnostic messages and SQL fragments have explicit
exclusion reasons. Static parser inputs retain decoded Rust literal bytes;
format templates are expanded at their existing test arguments. In particular:

- All 32 ordinary `source_contract` inputs expand to nine statements each.
- The ordinary alias matrix expands seven keywords, four envelopes and two
  `AS` choices, including every negative boundary and all four legal alias names
  with/without a column list: 504 cases, plus two NATURAL JOIN regressions.
- Standalone `GRAPH_TABLE` helpers use the recorded
  `wrap_graph_table_as_from_item` adaptation: prefix `SELECT * FROM ` so both
  statement entry points receive identical complete statements.
- The larger ignored clause/alias campaigns remain separate local verification;
  the static inventory does not claim to enumerate their entire input space.
- The four local counterparts are `ALTER TABLE messages ADD COLUMN size BIGINT`,
  `ALTER TABLE`, `CREATE INDEX broken ON`, and `DELETE FROM`.

The frozen inputs remain owned by
`crates/qualification/fixtures/nowledge_content_store/`. Every named workload
statement must appear exactly once with byte-identical SQL and dense production
parameter positions matching its declared parameter count. Every nonempty
schema line must appear exactly once and prepare successfully. The current
schema stores one complete DDL statement per line; a format change requires
reviewing this extraction contract. The fixtures enter SQL Bazel targets only
as test compilation data.

## Outcomes and waivers

Production preparation includes lowering and dense-parameter validation. Owned
parsing is syntax-only. Each accepted entry records its parameter positions;
the owned inventory comes from lexical parameter tokens and is not a binding
or parameter-type assertion. Accepted families are checked against the returned
AST variant. Rejected outcomes pin the production `Parse`/`Semantic` class or
the owned `SyntaxErrorCode`, and owned error spans must be valid UTF-8 boundaries.
The unrelated error enums are not compared as if they shared a vocabulary.

All three owned statement families (`select`, `select_graph_table`, and
`create_property_graph`) require both owned acceptance and rejection cases.
Any accept/reject disagreement requires a waiver with the matching family and
direction, a reason, and an issue link. Unexpected outcomes, missing waivers,
waivers on converged cases, unknown waivers and unreferenced waivers fail.
The current matrix has 42 cases accepted by both frontends and 333 rejected by
both. The remaining 632 cases use these ten explicit waivers:

| Waiver | Cases | Recorded gap |
| --- | ---: | --- |
| `production-pgq` | 275 | Owned SQL/PGQ syntax is not routed through production preparation. |
| `production-expressions` | 119 | Production expression/predicate representation remains limited; see #156. |
| `production-column-alias-list` | 112 | Production lowering rejects table column alias lists. |
| `owned-ddl-dml` | 104 | Owned statement routing supports SELECT and CREATE PROPERTY GRAPH only. |
| `owned-alias-boundary` | 14 | Explicit keyword aliases have different acceptance contracts. |
| `owned-aggregate-filter` | 2 | Owned aggregate FILTER syntax is missing. |
| `owned-like` | 2 | Owned LIKE/ILIKE/ESCAPE shape is missing. |
| `owned-explain` | 1 | Owned EXPLAIN routing is missing. |
| `owned-empty-projection` | 1 | Production accepts the empty SELECT target list; owned syntax rejects it. |
| `prepare-dense-parameters` | 2 | Only production preparation validates dense parameter positions. |

These are observed contracts, not a claim of PostgreSQL semantic equivalence.
PGQ binding, result types, graph keys/nullability/endpoints, execution, and
catalog publication are outside this test's evidence. The read-only
creation binder has its own source-schema and bound-outcome verification; the
frontend acceptance counts do not substitute for that evidence.

## Maintaining the corpus

When a source hash changes, inspect its diff and re-inventory added, removed or
changed SQL inputs before updating the hash. Keep IDs stable for unchanged
cases. Decode Rust literals as Rust, rather than treating escapes as JSON or
Python escapes; expand the ordinary generated matrices completely. Record
fragments/diagnostics with exclusions and helper adaptations explicitly.

Add or update the JSONL case, its source membership, literal provenance and
counts together. Review each frontend outcome against the intended contract;
do not regenerate expected results by accepting all current parser output.
Existing single-frontend AST/binder tests remain the semantic oracles. Remove
a case's waiver when the two frontends converge and delete a waiver definition
when its last use disappears. A new waiver needs a specific explanation and
linked remaining work. Frozen workload SQL must be copied exactly from the
canonical fixture, not rewritten to fit a frontend.

## Verification

The canonical `//crates/sql:hawdb_sql_tests` target includes the ordinary corpus
and negative controls. The latter deliberately remove coverage, provenance or
waivers, retain stale/unreferenced waivers, alter frozen inputs/parameters and
change expected frontend errors to prove those checks reject the regression.

```sh
cargo test -p hawdb-sql -p hawdb-sql-syntax
cargo clippy -p hawdb-sql --all-targets -- -D warnings
bazel test //crates/sql:presubmit_tests //crates/sql-syntax:presubmit_tests
```

The local-only `hawdb_sql_frontend_corpus_fuzz_tests` target runs 4,096 seeded
case selections with whitespace and line/block-comment prefixes, preserving
the original SQL bytes and expected contracts. It is tagged `manual`, ignored
by ordinary Rust tests, and included in the existing local fuzz suite.

```sh
cargo test -p hawdb-sql tests::frontend_corpus::frontend_corpus_differential_campaign -- --exact --ignored
bazel test //crates/fuzz:hawdb_fuzz_tests //crates/fuzz:hawdb_fuzz_cli_tests //:hawdb_linux_ci_fuzz_smoke_test
bazel test //:hawdb_unit_fast_tests //:hawdb_unit_storage_crash_matrix_tests //:hawdb_storage_crash_recovery_tests
```
