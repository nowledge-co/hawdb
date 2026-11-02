# Mem query compatibility contracts

Consumer: <https://github.com/nowledge-co/mem/issues/9338>.
Engine owner: <https://github.com/nowledge-co/hawdb/issues/1503>.
Source query inventory: Mem `b1dda00faea5d454db5a1affc928906e5b858270`.

The finite `api::tests::mem_query_contract::mem_9338_*` fixtures execute the
consumer's C1-C5 Cypher shapes through `Database::query_with_params`, rather
than treating parser admission as execution evidence. They use synthetic data.

| Contract | Required behavior |
| --- | --- |
| C1 | `LOWER(regexp_replace(source, ' ', '_', 'g'))` groups normalized sources, excludes NULL sources, and applies aggregate ordering before LIMIT. |
| C2 | Nested `list_append` feeds read UNWIND with aliases, name and ID. NULL aliases use the query's COALESCE; DISTINCT retains each entity once. Appending a list retains it as one nested element. |
| C3 | `trim` works inside CASE/WHERE. NULL, empty and whitespace-only Space values select default; other Spaces, including padded nonempty values, do not broaden to default. |
| C4 | Independent property-map MATCH bindings accept literal/parameter SET values. Every binding must match before any assignment; assignment order is retained even when aliases select the same node. Missing/NULL endpoints yield no writes. One statement publishes through the existing atomic mutation/WAL path. |
| C5 | Thread first/last-message assignments retain minimum/maximum values when requested, keep existing properties for NULL incoming values, and replace metadata/update time only when non-NULL. Comparisons include the native temporal types. |

`list_append` uses the existing list builder and its memory/work accounting.
String scalars are a separate expression family whose children participate in
binding, retained-plan accounting, determinism and physical fingerprints.
Computed read UNWIND evaluates its expression for each input row and uses the
existing bounded row-unwind executor. Computed sources do not enable new
mutation-pipeline forms. Mutation plans continue to bypass the physical cache.

`trim` accepts one string, returns Unicode-whitespace-trimmed text, and
propagates NULL. `regexp_replace` accepts three string arguments and optional
flags: absent/empty replaces the first match; `g` replaces every match. Regex
syntax uses the existing Rust regex engine, not unrestricted PostgreSQL regex
syntax. Replacement supports literal text (including `$`), `\1` through `\9`,
`\&` for the whole match and `\\` for a literal backslash. Other flags/escapes,
invalid regexes, arity and type errors fail explicitly. NULL inputs propagate.
Output increments are admitted before allocation under the query account or
the existing unaccounted-expression ceiling; evaluated input bytes and output
work count against the list-expression work cap. Regex compilation retains the
regex library's compiler size limits; this is not a complete accounting model
for opaque regex-engine scratch memory or a throughput claim.

Multi-binding SET currently rejects relationships, WHERE predicates, repeated
binding names, RETURN, and property-dependent assignments. Constants do not
require materializing the MATCH Cartesian product. The storage command resolves
all selectors before preparing ordered writes. Existing mutation operation,
affected-row and output ceilings apply. Refusal discards the prepared batch;
transaction rollback and durable reopen remain owned by the existing kernel.
Temporal range preservation shares the kernel's comparable temporal ordering,
including offset tie-breaks for zoned datetimes representing the same instant.
No new WAL encoding, durability default or publication boundary is introduced.
The fixture does not establish additional power-loss qualification.

## SQL comparison scope

The SQL statements in the consumer issue express comparison semantics, not
current Mem production calls. This bounded delivery does not request a SQL
array API or unrestricted PostgreSQL compatibility:

| SQL form | Status in this delivery |
| --- | --- |
| Multi-row constant UPDATE with IN | Executed control; exact updated and untouched rows asserted. |
| `regexp_replace`, `TRIM` | Unsupported by SQL frontend; refusal retained as a separate control. |
| CASE SELECT and CASE UPDATE | Unsupported by SQL frontend; refusal retained as a separate control. |

## Verification and consumption

Root fixtures own complete post-state, rollback, missing bindings, alias order,
operation-budget refusal, string output-memory refusal, temporal comparisons,
WAL-backed close/reopen, read CASE/UNWIND, OPTIONAL MATCH aggregation and SQL
controls. Component tests retain expression and parser coverage. Replay:

```console
cargo test --locked -p hawdb --lib mem_9338 -- --test-threads=1
bazel test //:hawdb_unit_general_tests --test_arg=mem_9338
```

The engine implementation is not a final consumed Mem pin. The consumer issue
must remain open until Mem consumes a reviewed release/revision and records its
own executable compatibility coverage and terminal integration CI. This work
does not authorize CRDT changes, Legacy fallback, stable artifact inclusion,
storage-mode activation, ownership publication or removal of legacy stores.
