# Owned SQL/PGQ token PEG

`hawdb-sql-syntax` owns the bounded token PEG used by the default relational
SELECT and EXPLAIN SELECT execution paths, including prepared statements and
plan-template cache misses. No feature, environment variable or host opt-in is
required. The syntax-only CREATE PROPERTY GRAPH and GRAPH_TABLE entry points
also use PEG. Branch lifecycle and paged SHOW commands use PEG as well.
Cypher uses the same engine with its own token grammar and
currently retains native clause terminals.

Parser routing is explicit by statement family:

| Family | Default path |
| --- | --- |
| SELECT, EXPLAIN [ANALYZE] SELECT | Owned token PEG, then relational IR lowering |
| CREATE PROPERTY GRAPH, GRAPH_TABLE syntax APIs | Owned token PEG; existing catalog/binder/execution admission still applies |
| Relational DDL/DML and materialized-view statements | Upstream `sqlparser` and existing lowering |
| Branch commands | Bounded SQL token PEG and branch AST transformation |

This is a SELECT-family promotion under [#1328](https://github.com/nowledge-co/hawdb/issues/1328),
not completion of every SQL statement family's migration. A PEG syntax,
semantic or resource error is returned directly; it never retries the upstream
parser. Accepting PGQ syntax does not activate unqualified PGQ execution.

## Design reference

DuckDB 2.0 makes its PEG frontend the default. Its
[parser documentation](https://duckdb.org/docs/preview/sql/peg_parser) describes
token matching, packrat memoization, source locations, suggestions and grammar
extensions. Its [implementation guide](https://github.com/duckdb/duckdb/blob/main/src/parser/peg/README.md)
separates grammar matching from AST transformation and describes named rule
composition and native matchers. Ordered choice makes rule order significant;
it does not guarantee that adding an alternative preserves the old language.

HawDB adopts that separation without copying DuckDB code or adding a parser
generator dependency. The bounded existing language does not yet justify a
textual grammar compiler, generated transformer framework, runtime plugin ABI,
custom tokenizer registration or catalog-aware completion. Those are separate
features, not consequences of using PEG.

## Grammar and transformation

`src/parser/frontend/grammar.rs` defines named rules using typed Rust
combinators: token/keyword, sequence, ordered choice, optional, repetition,
and positive/negative lookahead. Keywords match unquoted words case-insensitively.
The SQL grammar exports `Statement` and `FromItem` alternatives; PGQ prepends
its rules before immutable compilation. Compilation resolves named references
to numeric rule IDs, so matching does not hash rule names. In particular,
GRAPH_TABLE composes inside joins and FROM lists rather than retrying after a
SQL parse error.
A negative keyword guard prevents a broken GRAPH_TABLE prefix from becoming
an ordinary relation.

`src/peg.rs` builds a compact tree of named captures. SQL/PGQ matching has no
native callbacks: precedence levels, function arguments, graph paths,
property-graph definitions, aliases and clauses are PEG rules.
`src/parser/expression.rs` folds matched expression captures;
`src/parser/frontend/transform.rs` transforms only the selected tree into the
existing syntax AST. The former recursive expression and graph parsers have
been removed. Transformation does not select grammar alternatives or mutate
catalog/execution state.

The relational entry adds constant SELECT, implicit projection aliases, the
upstream-compatible explicit table-alias policy, empty projections, read-only
EXPLAIN, vector table-function arguments and nonblocking-lock syntax (rejected
by semantic admission). It shares clause, scalar and join grammar with PGQ.
LIKE/ILIKE/ESCAPE, function ALL and aggregate FILTER are syntax nodes; their
existing relational semantic restrictions remain authoritative. PGQ binders
reject the newly represented relational-only expressions until qualified.

`hawdb-sql/src/parser/owned.rs` converts selected expressions into the existing
semantic lowerer's AST types in memory; it never serializes or reparses SQL.
This preserves position-sensitive semantic restrictions and the shared
relational expression IR. Byte locations become character-based line/column
locations using line starts and sparse UTF-8 checkpoints, avoiding repeated
whole-line scans for wide statements. SQL statement, expression and table
function parsing is bounded by the lexer and PEG limits.

Grammar compilation rejects duplicate or unresolved names, direct/indirect
left recursion (including through nullable prefixes/lookahead), and nullable
repetition. Right recursion is permitted within the runtime depth bound.
Alternatives are attempted from the same token position; the first success
wins. A later sequence failure does not revisit an earlier successful choice.
The AST transformer and grammar form one internal contract and change together.

## Resources and diagnostics

The matcher runs continuations on an explicit work stack rather than the host
thread stack. This permits a graph pattern at its 128-level limit to contain an
expression at its independent 128-level limit without overflowing a Rust thread.
Admission still bounds internal expression frames, work, captures and memo
entries. Two language-depth domains carry subtree heights in memoized captures;
reusing a capture in a deeper context cannot bypass either limit. Depth refusal
retains its expression or graph error code. Grammar prefixes must match before
entering guarded content, so a rejected alternative does not consume language
nesting admission.

A parser invocation owns its memo table and capture arena. Successful and
failed named rules are cached by rule ID and token position; successful hits
reuse capture IDs instead of cloning syntax trees. Each rule's first memo entry
uses a direct indexed slot; other positions use a sparse hash table. Both tiers
share the same entry budget and retain successful and failed results. The direct
slots are bounded by the compiled grammar's 256-rule cap, not the input length.

Capture children are ranges in one edge arena, and native values occupy a
separate arena instead of enlarging every named node. A reusable scratch vector
collects children during matching. Failed expressions and lookahead roll back
only that scratch vector; nodes, edges and values remain append-only because
memoized results may reference them. The AST transformer reads child ranges
without cloning temporary child lists.

Token failures store positions and static expectations, rendering source text only for the final error; native
errors share their existing diagnostic. Cached diagnostics preserve
the farthest failure through later reuse. Lookahead discards its own speculative diagnostic. Optional/repeated
branches discard absent-prefix mismatches while preserving deeper failures. Ordinary syntax errors select the farthest byte
position, with the first expectation retained on ties. Expected-token sets and
interactive completion are not exposed yet.

The shared engine defaults to 4,000,000 matching work units, 262,144 memo
entries, 262,144 capture nodes and separately 262,144 capture edges, and 128
nested matcher expressions. SQL/PGQ uses an 8,192-frame internal matcher budget
because each language level spans multiple grammar rules; its independent
expression and graph language budgets remain exactly 128. The explicit work
stack enforces internal frame admission without recursive host-stack growth.
Work counts rule dispatch, combinator evaluation and
native consumed/scanned token ranges. Native terminals are atomic bounded
operations: their token-range charge is checked after they return; the lexer
input/token caps and their own nesting checks also apply. These are logical
bounds, not a byte-precise allocator admission guarantee or a wall-clock limit.
The existing full lexer limits remain 16 MiB input, 1,000,000 tokens and 64 nested
block comments; expression and graph language nesting each remain 128. The
first-token dispatch helper reads only the prefix and does not enforce the
whole-input byte cap. Complete owned SELECT/EXPLAIN and SQL/PGQ syntax parsing
still enforce that cap; legacy-routed DML/DDL bodies retain their existing
admission behavior. Numeric underscores must be surrounded by digits; malformed
separators fail lexically in both the relational and PGQ syntax entries.

`ParserLimitExceeded` identifies PEG work/storage/internal-depth refusal.
Expression and graph language-depth failures retain
`ExpressionNestingLimitExceeded` and `GraphPatternNestingLimitExceeded`.
Native resource errors retain their existing error codes. Resource failures are fatal across
choice, optional, repetition and lookahead, and are never treated as a failed
alternative. All state is per invocation, so rejected input cannot poison the
next parse. No syntax repairs are applied.

## Qualification and remaining boundary

Existing syntax and frontend differential fixtures remain the compatibility
oracle. Engine regressions include malformed nested alternatives that would
have exponential cost without failure memoization, cache reuse, source spans,
and exact budget boundaries. Source-bound local parser campaigns supplement
those cases. See [test ownership](TEST_INDEX.md#owned-sqlpgq-token-peg).

The local parser-only benchmark is reproducible with:

```console
cargo bench --locked -p hawdb-sql-syntax --bench frontend
cargo bench --locked -p hawdb-sql-syntax --bench frontend -- --shapes
```

The default mode mixes five short SQL/PGQ statement shapes; `--shapes` also
isolates minimal queries, malformed PGQ, 500-column projections, 64 joins and
a 4 MiB rejected word. It warms the grammar before emitting nine CSV samples
per workload. Exclude the first sample and interleave identical benchmark
sources against baseline and candidate revisions. Allocation measurements need
a separate instrumented probe; timings from that probe include its overhead.
Results and tested revisions belong in the PR, not a checked-in run log.

PEG still adds matching/capture overhead relative to handwritten parsing.
Further statement-family promotion still requires positive/negative parity,
executable lowering, query-result/plan coverage and resource measurements, as
required by the
[SQL/PGQ specification](specs/POSTGRES_SQL_PGQ_SPEC.md).

## Frozen migration baseline

The `frontend_peg_baseline_v1.jsonl` SQL fixture records 1,009 public parser
outcomes from main `0f611b6dec`. The Cypher
`migration_peg_baseline_v1.jsonl` fixture records statement and pipeline results
for all 1,427 inventory queries (2,854 records). Each accepted result has the
SHA-256 of its complete UTF-8 Rust Debug AST, including source spans. Rejections
record Parse or Semantic. These fixtures supplement semantic equality checks:
Cypher `AstNode` equality deliberately ignores spans. They must not be refreshed
from the candidate parser to hide migration differences. The original query
inventories remain the source of query text and provenance.
