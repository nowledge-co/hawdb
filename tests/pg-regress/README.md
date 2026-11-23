# PostgreSQL regression tests

Run selected PostgreSQL regression queries directly against embedded HawDB with
`make pg-regress-hawdb`. This path requires no PostgreSQL server, libpq, DSN,
Python or subprocess. Both SQL and expected results come from the pinned
PostgreSQL repository; they are not generated from HawDB's current answers.

An optional live comparison CLI is also retained:

An independent, unpublished Rust test project following RisingWave's
[`src/tests/regress`](https://github.com/risingwavelabs/risingwave/tree/main/src/tests/regress)
layout. The CLI directly calls both the embedded HawDB API and the Rust
[`libpq`](https://crates.io/crates/libpq) client. No Python scheduler, hand-written
FFI, helper process or HawDB PostgreSQL protocol server is involved in comparison.

## Layout and provenance

- `src/main.rs`, `regress.rs`: CLI, serial session/step execution and JSON reports.
- `src/suite.rs`: typed manifest/schedule loading and upstream provenance checks.
- `src/compare.rs`: result/error comparison and negative controls.
- `src/embedded.rs`: the real embedded Rust transaction API adapter.
- `tests/upstream_sql.rs`: HawDB-only regression tests using `Database::query_sql`.
- `data/hawdb-expectations.json`: exact upstream output ranges and explicit
  scalar/error mappings for those tests; no hand-written expected result rows.
- `src/postgres.rs`: safe Rust libpq connections, execution and result decoding.
- `data/sql/`, `data/expected/`, `data/isolation/`: unmodified PostgreSQL inputs.
- `data/cases.json`: checksums, upstream paths, exact SQL/line ranges, explicit
  adaptations and unsupported cases. Extend this manifest to select more tests.
- `data/schedule`: one serial `test: GROUP` per line; not a parallel pg_regress
  scheduler. Groups run in schedule order, cases in manifest order.
- `POSTGRESQL-LICENSE`: upstream license.

Inputs are pinned to PostgreSQL `REL_18_0` commit
`3d6a828938a5fa0444275d3d2f67b64ec3199eb7`, version **180000**.
HawDB-only tests read the selected stock expected output directly. The optional
comparison CLI uses live PG 18.0 as its reference. Aligned-column padding is
preserved by a scoped whitespace attribute. A source edit requires a reviewed
checksum/provenance update.

The manifest is a selected subset. Unselected source statements are not coverage.
Separately named `adapted/` cases may attach explicit fixture-only `execute_sql`
to a source step. Both backends execute that same SQL; the report retains its
original/effective SQL and adaptation reason. Raw upstream failures remain.

### HawDB-only SQL selection

The additional selection keeps the upstream query text unchanged:

- `adapted/int8/comparison-0` through `comparison-11`: the six column-left
  comparisons against each of two integer constants, retaining all five
  upstream integer pairs and their duplicate values. Setup adds a synthetic
  primary key, explicit INSERT targets and numeric literals instead of string
  input conversion. The HawDB-only assertion removes only that exact additional
  key column before comparing all upstream columns and rows. These cases do not
  cover string parsing or PostgreSQL integer widths.
- `adapted/select_having/`: six ungrouped HAVING queries covering aggregate
  comparisons, zero/one-row constant predicates and two invalid ungrouped-column
  references. Negative cases map the original upstream ungrouped-column error
  to the exact typed HawDB semantic error; adjacent successful queries exercise
  the same setup and syntax. The optional CLI retains SQLSTATE `42803` mapping.
- `adapted/select_implicit/non-projected-order-key`: ordering by an input column
  omitted from the projection, preserving both result order and duplicate rows.
- `subselect/`: three stock queries for matching and nonmatching scalar
  membership on a FROM-less implicit row, plus two unchanged derived-table
  queries checking ordinary and extra-parenthesized subselects.
- `adapted/subselect/`: six stock queries for constant, uncorrelated, nested
  and correlated membership, row `NOT IN` with partial NULLs, and scalar
  `NOT IN` over an empty result with a nullable left operand. The eight-row
  fixture stores the unused `f3` column as BIGINT to preserve the integer
  INSERTs. The two NULL cases use ordinary CREATE TABLE inside their isolated
  schemas in place of CREATE TEMP TABLE.
- `adapted/subselect/exists-or-correlation`: unchanged correlated EXISTS inside
  OR over the original eleven logical rows. An ordinary table replaces the TEMP
  partitioned fixture, and explicit values expand `generate_series(0,10)` with
  integer `x/2` and `x+1`. Partition planning, ANALYZE and series execution are
  not covered. The six expected result rows come directly from upstream.
- `with/nested-cte-shadowing`: unchanged nested WITH/RECURSIVE query with a local
  `w6` shadowing an enclosing `w6`, checking the upstream one-row result.
- `with/recursive-sum`: the stock recursive CTE that sums the integers from 1
  through 100.

The `select_having` and `select_implicit` fixtures use the original, already
unique `a` column as a BIGINT
primary key, BIGINT for `b`, TEXT for CHAR columns and explicit INSERT targets.
All original values are retained. CHAR padding/width and integer-width semantics
are outside this adaptation. Existing raw int8 cases remain unchanged. The five
other `select_having.sql` queries are unselected because they need aggregate
ORDER BY, scalar LOWER, expression GROUP BY or arithmetic predicates. They are
not counted as passes.

Two additional selected source cases, `adapted/subselect/row-in-values` and
`row-not-in-values`, remain explicitly **UNSUPPORTED** in the live manifest.
Their unchanged `CAST(... AS int4)` currently produces HawDB's semantic error
`CAST requires a numeric target type`. They are excluded from the 33 passing
HawDB-only cases, and the queries are not rewritten to hide this limitation.
Their eight original rows remain recorded with the explicit integral-valued
`f3` fixture adaptation for later qualification.

Each expected-output range must echo the exact selected query and belong to a
checksum-verified source. Assertions preserve column names, value kinds,
duplicate counts and explicit ordering. The small expected-output decoder is
limited to these integer and known ASCII-word text fixtures. A blank cell means
NULL only in a column explicitly declared nullable and integer. Arbitrary text
whitespace and empty-string semantics are outside the selected output formats.
The manifest retains the source query and any ordering clause verbatim.

## Run locally

Run all 33 migrated cases on HawDB alone with the pinned Rust toolchain:

```bash
make pg-regress-hawdb
# Or directly:
cargo test --locked --manifest-path tests/pg-regress/Cargo.toml --no-default-features --test upstream_sql
```

Cargo also runs two existing checksum/selection controls imported from the suite
loader. Those controls are separate from the 33 migrated PostgreSQL cases.

### Optional live comparisons

Use the pinned Rust toolchain, PostgreSQL/libpq development headers and library,
libclang and pkg-config/pg_config for the native bindings. The private
`postgres-reference` default feature enables Rust `libpq`; it is never a library
workspace dependency. No Python runtime is required.

Supply a dedicated PostgreSQL **18.0** test instance. For example, start a local
`postgres:18.0` container yourself on a chosen loopback port. The runner does not
create containers or install a server. The libpq client may be another version,
but every connected server must report exactly 180000; later 18.x are refused.

Give the test role permission to create schemas. Every case uses a unique
`hawdb_pg18_*` schema, which is removed after all its sessions close. An abruptly
killed process may leave that named test schema for inspection. Do not point this
tool at an application database.

```bash
make pg-regress-list
export PGREGRESS_DSN='host=127.0.0.1 port=58432 dbname=postgres user=hawdb_test'
make pg-regress
make pg-regress PG_REGRESS_ARGS='--case adapted/isolation/ --report target/pg-regress/isolation.json'
make pg-regress PG_REGRESS_ARGS='--case adapted/int8/ --report target/pg-regress/int8.json'
make pg-regress PG_REGRESS_ARGS='--case adapted/select_ --report target/pg-regress/select.json'
```

Or run the single CLI directly:

```bash
cargo run --locked --manifest-path tests/pg-regress/Cargo.toml -- --help
cargo run --locked --manifest-path tests/pg-regress/Cargo.toml -- --case boolean/scalar
```

The DSN stays in the environment and is not written to reports. `--dsn-env NAME`
selects another variable. Reference connections have five-second connect,
three-second statement and 500-ms lock timeouts. Embedded API calls run
synchronously in-process; there is no separate hard-preempting worker timeout.
Timeouts, failed cleanup and incomplete runs never become successful cases.

The complete comparison CLI is built with Cargo. An additional internal
JSON-lines embedded API fixture remains available through the manual Bazel
binary `//tests/pg-regress:hawdb_pg_regress_worker`; the CLI does not launch it.
Both stay outside default CI/scheduled workflows, the library workspace and
published library archives. Native Windows execution is not locally qualified.

## Comparison contract

Compare named-column associations and value kinds, preserving NULL. The embedded
API exposes named rows, so projection-order equivalence is not claimed. PG
int2/int4/int8 map losslessly to HawDB's signed integer domain; width/OID metadata
is not equivalent. Float formatting is normalized through IEEE values; exact
decimal values are never converted to floats. Boolean/text/integer/NULL remain
separate. Ordered cases retain row order; unordered cases compare multisets,
preserving duplicate counts. DML affected-row counts are checked. Column names
are checked unless an individual step records an unnamed-label adaptation.
Empty-result value types cannot be checked through the embedded name-only schema.

- `PASS`: every selected statement agrees under the explicit mappings.
- `MISMATCH`: first unequal observation; remaining case statements are not run.
- `UNSUPPORTED`: manifest-declared scope exclusion and reason; never a pass.
- `INFRA_ERROR`: wrong server, bad provenance/selection, connection loss, timeout,
  unexpected panic or cleanup failure; incomplete runs cannot pass.

Exit codes: 0 if at least one case passes and none mismatch or fail infrastructure;
1 for mismatches or zero executed passes; 2 for infrastructure failure. Reports
retain revision, dirty-tree flag, executable/harness/input hashes, original SQL
and both observations. Reports are refreshed after each completed case and marked
complete only after the selected run ends. A killed process may leave an
incomplete report. Two arbitrary errors never agree: a negative case requires
an explicit PostgreSQL SQLSTATE/HawDB typed-error mapping.

Transactions map Repeatable Read to Snapshot Isolation and Serializable to
explicitly enabled Serializable validation. Both capture snapshots at the first
statement. Only nonblocking sequential schedules are supported. The selected
`simple-write-skew` permutations are a subset of upstream automatic interleavings.
PG SSI can reject at another step or choose another victim than HawDB's
commit-time validator. A strict stepwise mismatch is diagnostic, not necessarily
an isolation bug. Wait-aware isolationtester and allowed-outcome SSI validation
remain separate follow-ups; unsupported waits are not flattened into serial runs.

Result limits are 4096 reference rows and 1 MiB per reference cell, checked after
libpq receives results, not before network allocation. Use only bounded synthetic
fixtures. Source, library and process memory qualification remain distinct.

## Verification and initial baseline

```bash
cargo fmt --manifest-path tests/pg-regress/Cargo.toml -- --check
cargo clippy --locked --manifest-path tests/pg-regress/Cargo.toml --all-targets --all-features -- -D warnings
cargo test --locked --manifest-path tests/pg-regress/Cargo.toml --all-targets --all-features
```

Native controls change duplicate counts, row order, kinds, column associations,
affected counts and mapped errors, and corrupt a copied upstream input. Real
PG 18.0 comparisons and actual 18.6 refusal are separate evidence.

See [BASELINE.md](BASELINE.md). Raw fixtures intentionally expose current INT,
mandatory-primary-key and typed-Boolean-literal gaps, so the broad default run
exits 1. Do not silently exclude these failures. The separately named adapted
isolation selection spells out common fixture types and INSERT targets while
retaining the original transaction statements and ordering. Its integer balances
use BIGINT instead of DECIMAL; its passes do not claim native decimal semantics.
