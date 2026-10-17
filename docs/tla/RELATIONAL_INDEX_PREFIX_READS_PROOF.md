# Relational index-prefix reads: conditional refinement

This extends prepared single-table SQL reads needed by Mem Thread fanout. It
neither certifies an imported baseline nor constructs a complete C..E projection
session. The full acceptance ledger in
`docs/specs/MVCC_VERSIONED_PUBLICATION_SPEC.md` remains in force.

## Source map and argument

`api/dependency_read/relational/indexed.rs` derives a necessary leading tuple
from a declared index using the existing AND-only equality collector and SQL
scalar coercion. A complete primary key retains its existing row identity.
Index names, table names and typed tuples distinguish domains. No equality is
extracted through OR or NOT; residual range/keyset conditions, ordering and
LIMIT do not narrow the dependency. The scope includes every matching row,
including unreturned pages and an empty result. It is independent of whichever
physical access the SQL runtime chooses.

For predicate P with necessary non-NULL tuple T, P(r) implies prefix(r)=T.
Suppose a transaction changes the query result between capture C and validation
E. Some inserted, removed or modified row must formerly or currently match P,
or the schema/instance changed. In the row case its old or new index tuple has
T as a leading prefix. Stamping both memberships at every intervening committed
epoch therefore invalidates T, even for ABA and changes outside the first page.
Updating only a projected payload must also invalidate T. Membership deltas
alone omit an unchanged index key; captured new rows supply these payload
stamps. A delete supplies its old index membership. Schema and opaque writers
retain existing conservative schema/database barriers. Reopen invalidates the
process-local source identity rather than reconstructing old prepared tokens.

`storage/store/read_dependencies/relational_indexes.rs` consumes complete staged
index and row captures before WAL append. Incomplete/unknown captures or summary
admission exhaustion record ReadBarrier. A read using the new summary family
always validates that barrier as well as Schema and Database. Unsupported
floating, NULL, overflow-reference and oversized equality values retain the
whole-epoch fallback. In particular, no assumption equates floating SQL
comparison with the index key ordering (signed zero or NaN).

`graph_commit.rs` requests the existing bounded captures while any prepared
reader is active, starting at snapshot capture rather than first query. It
validates original writer destinations before adding read-only prefix stamps.
The new family is never a shared writer intent: disjoint primary-key writers
in one group remain eligible to commit. Stamps publish with the successful
staged canonical transaction and never install on a failed WAL append. A
rollback/sync failure can leave an uncertain complete disk transaction; recovery
still permits only the old or complete new canonical state, or a strict torn-tail
refusal with evidence. This component is not a new power-loss qualification.

Pins retain relevant conflict history. Publication revalidates the finished
read dependencies under the existing exclusive database publication authority;
there is no validation-to-install window introduced here. Admission or query
errors remain sticky and cannot produce a successful partial read token.

## Nullable unique index completeness

Unique indexes omit an entire key when any component is NULL. Consequently an
unbound nullable suffix makes a unique prefix unsuitable for answering a query:
(group='g', suffix=NULL) still matches group='g' but is absent from the index.
`RelationalTableSchema::index_prefix_covers_matches` requires every unbound
suffix to be NOT NULL for unique indexes. Necessary equality-bound components
are non-NULL. Base access, join access (both unique constraints and declared
indexes), and the prepared-read collector share this rule. An unsafe query
retains a full scan and conservative dependency. Nonunique indexes retain their
existing NULL membership. The change corrects a native wrong-result case rather
than changing SQL's NULL semantics.

## Bounds and remaining costs

Read tuples contain at most 32 components and 4 KiB of metadata/value ownership.
The collector visits at most 1,024 index descriptors / 64 KiB of index metadata,
in addition to existing SQL/parameter/expression limits. Summary generation
admits at most 65,536 visits and 64 KiB of cumulative key bytes before cloning;
repeated visits count even if the version map deduplicates them. Row column
searches consume the visit budget. Exhaustion stamps ReadBarrier, preserving
unrelated exact primary-key reads while conservatively refusing domain reuse.
Individual summary keys use borrowed value preflight and fixed 32-slot row
lookup storage. Existing version-history admission can reject a write before
WAL publication if the complete stamp set cannot be retained.

Requesting captures adds existing staging costs: up to the configured/default
index capture and row capture limits (defaults 100,000 entries / 8 MiB and
100,000 rows / 64 MiB). These limits and estimated bytes are not allocator/RSS
measurements. Captures may also enable existing optional WAL replay-access
metadata. This is not a total query, disk-I/O, WAL-byte, peak-RSS or latency
bound; cold block reads, SQL execution and final Search artifact construction
keep their existing contracts. Unsupported SQL joins remain refused by the
prepared facade, even though ordinary SQL JOIN correctness is fixed here.

## Finite model

`HawDBRelationalIndexPrefixReads` uses two row identities, two groups, two
ordinal/payload values, indexed and unindexed roots and at most two commits.
The independent oracle compares pinned/current query output and complete
old/new membership history. Queries include a group, composite tuple, bounded
page, OR and NOT. Capture may precede reads and writes. The model separates
writer row stamps from read prefix stamps, retained history, WAL success,
rollback, uncertain complete/torn outcomes and one recovery/instance replacement.
Idle pending proposal fields are canonicalized because no later transition
reads them. Unknown-capture choices before tracking or on unindexed roots are
collapsed: they cannot affect any narrower surviving read. Both initial root
orderings are included so an unrelated OR match can become the first result.
These are semantic state reductions, not removal of failure actions.

Invariants require no stale accepted dependency, no read-summary-induced writer
conflict, and no stamp visible before its canonical epoch. Thirteen faulty
configurations must violate their named invariant with TLC exit 12: late
tracking; omitting old/new/payload summaries; ignoring ReadBarrier/incomplete
capture; trusting an unindexed domain; narrowing OR, NOT or a page; pruning a
pinned summary; publishing before WAL; and validating writer summary keys.
Two additional controls require concrete disjoint-reader progress and stale-base
same-prefix disjoint-writer commits. Syntax errors, timeouts and unrelated
invariant failures do not count as successful controls.

TLC 2.19 / TLA+ 1.7.4, Java 21.0.9, jar SHA-256
`936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88`:
**7,374,096 generated / 4,769,128 distinct states**, complete depth 14,
positive exit 0. All 13 fault controls and both progress witnesses return exit
12 for the expected named invariant. Earlier development runs with a syntax
error, an unfinished positive run and an OR control timeout are excluded.

The bounded model and source argument are conditional evidence, not an
unbounded machine-checked refinement proof of Rust, SQL completeness, filesystem
synchronization, fairness or the full artifact/session protocol.

## Native regression and host applicability

`tests/mvcc_projection_publication.rs` covers hot/cold roots, capture before the
first query, 1 MiB payload-only related/disjoint updates, full composite prefixes,
insert/delete/move/ABA, absence, schema changes and off-page updates. It also
checks OR/NOT/unindexed/nonleading/NULL/floating/oversized fallback. Nullable
unique declared indexes and constraints have base and JOIN result checks on
hot/cold roots, plus complete non-NULL binding behavior.

Storage tests commit two same-group disjoint writers from one epoch, exhaust
summary admission while preserving an unrelated primary-key token, and exercise
partial-write/rollback/sync failures with live state/stamp and reopen assertions.

The audited Mem mapping at `a78432d280609fedd94bc22c4c90da53867b1086`
uses a Thread fanout page with necessary `thread_storage_id` equality and
(order_index, content_message_id) keyset ordering. A declared index with that
leading field can now yield a group dependency instead of Database. Qualified
SourceChunk point/page joins are covered by `JOINED_PROJECTION_READS_PROOF.md`.
Bounded fanout continuation, baseline/mapping verification and actual outer-owner
migration remain open. No host callback is automatically certified.

Local prefix-component evidence at `436aa97` (Rust 1.97.1, macOS arm64;
the later joined-read component extends the facade suite):

- The six `relational_prefix` Cargo facade tests pass. Both default/minimal
  Bazel facade consumers pass all 64 tests; both owner consumers pass all 5.
- Bazel storage passes 1,167 tests / 29 ignored, relational passes 216 / 9,
  and the separately executed allocation suite passes 108 / 0.
- A detached `e4e87d36ddfa470218638f7422a5665e75038dc3` baseline with
  test attachment only (new prefix-count introspection assertions removed)
  selects six tests: three pass and three fail. Two failures expose whole-epoch
  dependency fallback; the nullable JOIN returns one row instead of two.
  Its separate Bazel output avoids cross-worktree executable reuse. The fixture
  adaptation, failure logs and subsequent clean/shutdown are retained locally.
  The SQL correctness defect is tracked by #1045.

Commands: `cargo test --locked -p hawdb --test mvcc_projection_publication
relational_prefix -- --nocapture`; Bazel targets
`//:hawdb_mvcc_projection_publication_tests`,
`//:hawdb_mvcc_projection_publication_minimal_tests`,
`//:hawdb_mvcc_publication_owner_tests`,
`//:hawdb_mvcc_publication_owner_minimal_tests`,
`//crates/storage:hawdb_storage_tests`,
`//crates/relational:hawdb_relational_tests`, and
`//crates/executor:hawdb_projection_allocation_tests`. The same filtered Bazel
facade target reproduces the baseline failures. PR validation separately records
the full local fuzz matrix and any adverse timing outcome.
