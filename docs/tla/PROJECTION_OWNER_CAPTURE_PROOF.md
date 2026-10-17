# Versioned owner capture and recovery boundary

This component records source facts needed by the native C..E projection
session. It does not implement that session, qualify an arbitrary host mapper,
retain future events on behalf of C, or authorize Search publication.

## Contract and source correspondence

The existing built-in projection feed contains node upserts, old built-in
Search document IDs and relational primary keys. A Thread deletion can have
none of these built-in graph effects. V1 retains its physical node owner even
when its legacy document event is empty. It never encodes a deleted physical
node as a legacy upsert. Legacy materialized getters omit these empty document
events; legacy borrowed enumeration emits no extra items for them. The retained
log's count and byte status include the new internal events and payloads.

[The pre-WAL collector](../../crates/storage/src/store/search_projection_capture.rs)
uses one ordered owner map. Every node create, property mutation and deletion
records its owner. Existing HAS_LABEL fanout also records affected built-in
projected nodes. Existing typed relational key capture retains its independent
captured/rebuild contract. This is not complete capture of arbitrary
relationship-dependent host mappings; mapper qualification must refuse such a
claim. Graph-schema/opaque WAL families and exhausted metadata budgets still
stage a gap.

Creation records `Absent`. An identity change or deletion records the
transaction-start labels and old ID as `Absent`, `String`, or `Unsupported`.
Other effects use `NotRecorded`, requiring end-snapshot hydration. The
collector reads the unchanged source before the entire WAL batch, so a rename
followed by a delete retains the original parent identity. A later effect
cannot replace a known preimage with `NotRecorded`. Non-string identities must
not be coerced into strings by a subsequent mapping contract. In particular,
old Thread identity can support unfiltered enumeration of child keys; this
component does not yet perform that enumeration or prove its bounds.

An empty relational table creation need not advance this owner floor or emit
row-key input. Relational rewrites keep the existing captured/rebuild behavior.
A complete C..E mapping contract must independently bind the catalog/schema and
mapping versions; neither V1 input exhaustion nor this floor proves that every
canonical table was classified.

Additional map slots, preimage copies and both output vectors consume the
existing pre-WAL capture byte budget before their allocations. Repeated work
is charged even when its owner is already present. This is a conservative
logical metadata budget, not a measured RSS or allocator-governor guarantee.
The metadata bound can cause earlier rebuild refusals than the old capture.
There is no claim of zero throughput/latency regression.

[Immutable log pages](../../crates/storage/src/store/search_projection_change_log.rs)
share a captured wrapper and the original legacy `Arc` payload. Appending or
trimming live pages cannot change a captured snapshot. Prefix removal advances
the exclusive V1 floor, including removal of the entire log. A gap resets the
log with the gap's floor. Existing entry and byte retention limits apply to the
combined retained records; a C query pin does not pin future appends.

## Checkpoint and WAL boundary

[The extension codec](../../crates/storage/src/checkpoint/owner_capture.rs)
adds `projection_owner_capture 1 floor`, an epoch/count header for each retained
qualified transaction above the floor, and one encoded owner row per effect.
The zero-owner header is required for relational-only transactions. Per-row
checkpoint work units preserve cancellation checks; the already materialized
checkpoint text and the per-row JSON/hex codec are not new out-of-core or
allocator-admission guarantees.

[The durable writer](../../crates/storage/src/store/durable/checkpoint.rs)
appends the extension before computing the existing text checksum, compressed
artifact integrity, manifest bindings and publication barriers. There is no WAL
format or durability-policy change. WAL replay uses the same pre-transaction
collector against recovered canonical state. Successful canonical publication
installs the prepared facts; failed appends must not publish facts or floors.
A lost response can still recover a complete committed transaction under the
existing WAL contract.

A legacy checkpoint has no V1 marker. The actual
[recovery constructor](../../crates/storage/src/store/search_projection_change_log.rs)
keeps its canonical head and legacy history but starts V1 at the checkpoint
head, even if the old document feed advertised a smaller resume floor.
Subsequent WAL records can reconstruct qualified facts after that boundary.
Writing another checkpoint preserves the fence. This is a capability boundary,
not a schema migration or permission to discard a canonical database.

Unknown versions, duplicate headers, missing/count-mismatched rows, missing
transaction headers, out-of-order owners/labels, absent legacy-upsert owners,
and extension epochs outside the retained qualified interval fail closed.
Without a marker, extension rows are invalid. Old readers reject the new tags;
this development checkpoint extension is not backward-readable. Byte integrity
and manifest validation remain the protection against arbitrary artifact
rewrites; the semantic parser is not a cryptographic completeness proof.

## Finite model and native tests

[HawDBProjectionOwnerCapture](HawDBProjectionOwnerCapture.tla) starts from a
legacy checkpoint with an omitted owner effect, then explores up to three
canonical epochs, owner-only commits, failed WAL attempts, trim, snapshots,
new checkpoint writes, row loss and replay. `CompleteInterval` requires all
changed owners after any admitted resume point to remain available.
`CapturedInterval` applies the same condition to immutable captured state;
`NoUncommittedFacts` forbids speculative future records/floors.

TLC completed 1,017 generated / 258 distinct states, depth 11. Five mutations
(trusting the legacy floor, omitting owner-only events, forgetting the trim
floor, publishing before WAL, ignoring missing rows) each produce an invariant
violation. An additional negative witness reaches a usable upgraded interval,
so the finite result is not solely refusal. The seven Bazel targets passed;
TLC logs and tool hashes are retained by each target's `.run.tlc-evidence` output.
This is a finite protocol argument, not a verified Rust/TLA refinement or a
physical torn/reordered-write model.

[Four native owner tests](../../crates/storage/src/store/search_projection_capture/owner_tests.rs)
exercise owner-only Thread deletion across WAL/checkpoint reopen in both
Materialized and OutOfCore modes, pinned snapshots, original batch identities,
unsupported/absent IDs, ordinary property changes, trim and gap floors.
[Three codec/recovery tests](../../crates/storage/src/checkpoint/owner_capture.rs)
exercise old-format fencing through the actual decoded-log constructor,
subsequent new-format persistence, Unicode/control characters, empty owner
sets, missing/duplicate/out-of-order rows, unknown versions and preimages.
The old-format test is a parser/constructor test, not a complete old-format
on-disk reopen test. Existing WAL-failure tests now also compare owner facts and
floors. Existing borrowed-cursor tests check payload-pointer identity and COW
page sharing. A first wrapper implementation failed that pointer assertion;
sharing the original legacy `Arc` corrected it without weakening the test.

The full source-qualified bounded C..E cursor, mapping/version/instance and
incarnation binding, child fanout, external vector lineage, retention lease,
stage-bound coverage receipt, final-E read dependencies, short atomic activation,
lost-response receipt, actual Mem owner migration, physical power-loss fault
composition and 1/4/8-writer performance/fairness gates remain open. The old
conservative full-C publication fence and disabled Mem Search ACK remain.

## Qualification of the source-fact component

Runtime, tests and model inputs were frozen at tree
`ed5a86759a9c80eb952e9f8f94901f04cc04ad61`; subsequent changes here are result
prose and the explicit catalog-scope limitation above.

- `cargo test --locked -p hawdb-storage --lib --all-features -- --test-threads=4`:
  1,939 passed, zero failed, 30 existing ignored, 787.25 seconds.
- The 12 default/minimal embedded MVCC consumer targets and
  `//:storage_tla_evidence_script_test`: 13/13 passed (12 executed, one cached).
  These cover publication, JOIN, full source, comparison, staging and the public
  owner composition; they do not test the actual Mem owner migration.
- The three required local fuzz suites expanded to 98 targets: 98/98 passed
  (81 executed, 17 cached). Both residency halves covered 16 seeds / 992 state
  checks. Their native action times were 130.7 / 134.9 seconds. Wall time
  includes waiting behind another Bazel command; it is not a scaling benchmark.
- The seven source-capture TLC targets passed, including five fault controls and
  the upgrade witness. The selected total is 118 Bazel targets, not the entire
  workspace or all formal models.
- The owner-related all-feature Cargo selection passed 42 tests, including the
  seven new owner/codec regressions. Strict workspace/all-target/all-feature
  Clippy, workspace formatting and both `prek` hooks passed on the source bytes.

The initial complete Storage run passed 1,938 tests and failed one existing
missing-floor diagnostic assertion (30 existing ignored). The new extension
validator had returned a generic error before the original precise diagnostic.
The original diagnostic was restored without changing the test; the focused
case passed and the complete rerun above passed. Initial fuzz also passed
98/98 before that diagnostic correction and was rerun on the final source.
The original pointer-copy failure, both complete Storage runs, the two fuzz
runs, final integration logs/XML/cache status and all seven TLC evidence
outputs were retained separately. None of these results closes the remaining
full objective or grants host mapping/publication authority.
