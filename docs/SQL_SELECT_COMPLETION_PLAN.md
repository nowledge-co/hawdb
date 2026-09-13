# SQL SELECT completion for issue #157

Initial base: `523236934a50d9691ec8a88d5f3a2064450badcf` (explicit CROSS JOIN, PR #475).
This implementation follows the shared expression IR from PR #468. The goal is
execution of HAVING and multiple FROM items, not parser-only acceptance.

## Scope and decisions

Use the existing SELECT and join owners. Add `SelectStatement.having: Option<Expr>`
and `SqlJoin.on_scope_start: usize`. The latter is the zero-based first input
visible to ON (the base relation is input zero); the current join's right input
is the inclusive end. Existing single-FROM constructors use zero. Comma items
start a new scope. The metadata survives parsing and template caching and is
validated/qualified before join reordering. Generated physical-order AST joins
use zero after their references have been bound. This is a source adjustment for
complete struct literals, not a replacement of query entrypoints or row formats.

A separate FROM tree would also retain scopes, but would migrate every existing
flat-chain consumer or require a second SELECT representation. Scope metadata
retains the required distinction with fewer duplicated owners. Flattening without
scope metadata is incorrect and is excluded.

HAVING uses accounted aggregate states independently of output names. Existing
single-purpose aggregate fast paths must not bypass it. The generalized aggregate
path must preserve grouping, empty-input semantics, three-valued filtering,
parameter rebinding and memory/cancellation admission. Filtering precedes OFFSET,
LIMIT and output budgets. Validation happens before scanning, including empty
inputs. Existing non-HAVING query behavior stays unchanged.

## Execution stages

1. Preserve comma FROM scopes through parsing and schema binding. Inputs are the
   existing base tables/INNER/LEFT chain and actual catalog. Output is a qualified
   executable chain. Verify comma-vs-CROSS precedence, unqualified names, forward
   references, aliases, LEFT nesting, empty tables and parameter namespaces.
2. Bind and execute HAVING across grouped and implicit aggregate paths. Inputs
   are the shared Expr and current schema/parameters; output is admitted aggregate
   state and filtered groups. Verify hidden and FILTER/DISTINCT aggregates,
   grouped columns, unsupported/nested expressions, NULL, empty inputs and limits.
3. Reconcile the original issue examples with the existing unique output-column
   contract, preserve reviewed corpus cases and qualify owner/root/full local fuzz.
   PostgreSQL-compatible row values and name binding must be distinguished from
   the existing result-name limitation. Do not close the issue with an unhandled
   acceptance requirement. Keep pending public-result decisions explicit if a
   source-compatible implementation cannot satisfy them.

Entry: actual current sources and #157 description/comments inspected; local
edits, isolated Git delivery and default Bazel verification are authorized.
Force-fetch the same Bazel labels only if the known external cache is incomplete.
No configuration, timeout, workload, persisted format or mutation protocol changes.
Each stage needs a runnable owner/runtime proof before broad verification. Exit:
complete execution/corpus evidence, immutable source verification and reviewable
PR delivery; no agent approval or merge bypass. The draft #475 can be updated to
reflect the implemented HAVING/FROM scope once the continuation is qualified.
The duplicate-name result contract remains open as recorded in
[the result-column audit](SQL_RESULT_COLUMNS_CONTRACT_AUDIT.md).

PR #468 merged as `d16d1ee285b67a272bcc83fd8d55bed6a2040be9`. Delivery also
integrates the remote #475 main merge `a29947ef8b212b561d218d3e96bd45c55984f652`,
preserving the independent compat-owner extraction. Final qualification includes
that owner and does not substitute an earlier pre-integration result.
