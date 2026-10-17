# Mem projection effects and baseline authority

This source audit informs the native C..E session design in
[the batch coverage spec](MVCC_SEARCH_BATCH_COVERAGE_SPEC.md). It is not a
completed production-writer audit or permission to mint coverage receipts.
The remaining work below is required by #231/#232/#1043.

## Audited revisions

HawDB source is the #1039 branch with the pinned full-source input prerequisite.
Mem integration source was read at
[`bf0a527a0d48fd2e6ec38e839cf96555cc57c763`](https://github.com/nowledge-co/mem/tree/bf0a527a0d48fd2e6ec38e839cf96555cc57c763/integrations/hawdb/src).
Its `message_projection_changefeed.rs` is byte-identical to the previously
audited `a78432d280609fedd94bc22c4c90da53867b1086` version. These observations
do not establish the invariants of every application mutation route.

## Refresh against current Mem main

A read-only source snapshot at
[`cb850597a892efaf76720e4d9ba175277ff69a12`](https://github.com/nowledge-co/mem/tree/cb850597a892efaf76720e4d9ba175277ff69a12)
compares all 69 Rust files under `integrations/hawdb/src/` plus the server's
`hawdb_content_store.rs` with the pinned revision above using Git blob identities.
Of these 70 files, 60 are unchanged, seven changed and three were added. The
message changefeed, incremental Search mapper, message-source mapper and
bootstrap current-source comparison remain byte-identical. This confirms the
previous mapping/vector observations for those exact files; it does not establish
complete mutation-route coverage or remove the outer-owner integration requirement.

The source adds
[`graph_mirror_destination.rs`](https://github.com/nowledge-co/mem/blob/cb850597a892efaf76720e4d9ba175277ff69a12/integrations/hawdb/src/graph_mirror_destination.rs),
[`graph_mirror_evolves.rs`](https://github.com/nowledge-co/mem/blob/cb850597a892efaf76720e4d9ba175277ff69a12/integrations/hawdb/src/graph_mirror_evolves.rs)
and a graph-target schema contract. EVOLVES replay applies retained Memory
lifecycle/metadata post-state through a native transaction and has an independently
identified Search component. These writers belong in the required host-effect
inventory; a mirror/replay receipt is not a native complete Search baseline or
C..E activation receipt. Their complete effect, retry and ownership composition
has not been executed by this audit.

The refreshed
[`graph_source.rs`](https://github.com/nowledge-co/mem/blob/cb850597a892efaf76720e4d9ba175277ff69a12/integrations/hawdb/src/graph_source.rs)
and graph preparation explicitly exclude `GraphMirrorJournal`,
`GraphMirrorResolution` and `GraphMirrorResolvedIdentity` from canonical graph
pages. They are retained source-side coordination/resolution state, and pending
or orphan resolution blocks preparation. Named source tests cover these exclusions
and blocking outcomes; this refresh inspects their source rather than claiming
the tests ran here. Any new full-source contract must retain those names and
rationales, and distinguish these source-only tables from authoritative HawDB
projection source families.

The changed content schema helper exposes transaction-local catalog verification.
The server content-store diff only adds test-adapter dead-code expectations;
the two atomic mutation routes audited below retain their bodies. New graph
bootstrap/target-contract work still needs its own source-linked qualification.
No local Mem worktree was modified, and no host migration, readiness, ACK or
stable-release authorization follows from this read-only refresh.

## Effects that must reach the session

| Source | Current mapping/capture | Obligation before complete coverage |
| --- | --- | --- |
| Built-in graph documents | Native node changes hydrate at E; delete/ID-change capture retains built-in old document IDs. Label changes and HAS_LABEL edges fan out to projected neighbors. | Bind the exact mapping contract; verify all supported canonical entry points and cold/replay paths. |
| Thread metadata | Mem reads changed *live* Thread nodes, then pages `thread_messages` by `thread_storage_id`. | Deletion/ID replacement cannot rely on finding the old Thread at E. Capture old fanout identity or establish and test an enforced immutable identity / atomic child-update contract. |
| `thread_messages` | Text PK maps to `message:<content_message_id>`; body/metadata also read the owning Thread. Missing rows produce deletes. | Admit continuation and cumulative query/output cost; include absent/changed Thread dependencies; define the outcome of missing owners rather than silently skipping a group. |
| `content_chunks` | Text PK maps to `source_chunk:<chunk_id>`, joined to `content_documents` with `owner_kind='source'`. Missing/filtered rows produce deletes. | Protect parent membership and payload; preserve deletes when parent ownership changes; carry all child keys through bounded processing. |
| `content_documents` | Current source-owned documents fan out over chunk pages. A missing registered prefixed document is accepted without fanout; thread-owned documents have no chunk fanout. | Prove all affected chunks are captured on delete/ownership transitions, or capture old ownership/fanout identities. A parent key alone plus a current-only lookup is insufficient evidence. |
| `content_anchors`, `content_migration_state` | Explicitly ignored by this mapper after validating text PKs. | Keep these exclusions in the versioned host contract and exclusion tests; do not infer that every operational table is ignored. |
| Other relational tables, including engine registry | Existing callback refuses unknown tables. Full-source enumeration now exposes every table, including empty ones. | Explicit supported/excluded classification with rationale and tests for the actual App schema. Unknown tables must not disappear from baseline coverage. |
| Other graph relationship effects | Native built-in capture intentionally specializes HAS_LABEL and does not map arbitrary relationship-property changes. | Audit actual host query dependencies and declare/capture any additional effects. Arbitrary host mapping cannot inherit the built-in completeness claim. |
| External vector-bearing Search writes | Mem catches up the canonical feed, then applies an independently supplied vector delta. SourceChunk relational reconstruction explicitly emits `embedding: None`. | Model these writers as a separate versioned authority. Baseline and activation must bind their content/version and vector identity, or preserve/reapply them under a proven contract. |

The relevant host implementations are
[`message_projection_changefeed.rs`](https://github.com/nowledge-co/mem/blob/bf0a527a0d48fd2e6ec38e839cf96555cc57c763/integrations/hawdb/src/message_projection_changefeed.rs),
[`incremental_search_projection.rs`](https://github.com/nowledge-co/mem/blob/bf0a527a0d48fd2e6ec38e839cf96555cc57c763/integrations/hawdb/src/incremental_search_projection.rs),
and [`projection_bootstrap/current_source.rs`](https://github.com/nowledge-co/mem/blob/bf0a527a0d48fd2e6ec38e839cf96555cc57c763/integrations/hawdb/src/projection_bootstrap/current_source.rs).
The last compares actual native documents with immutable legacy source exports;
it explicitly does not confer a target incarnation or retained writer fence.
That comparison must not be reused as a native baseline receipt by matching
only its numeric graph epoch.

## Mutation paths checked after the mapping audit

[`hawdb_content_store.rs`](https://github.com/nowledge-co/mem/blob/bf0a527a0d48fd2e6ec38e839cf96555cc57c763/nmem-rs/crates/nmem-server/src/hawdb_content_store.rs)
provides a stronger contract for two named routes:

- `delete_full_thread` validates destination ownership, then deletes all matching
  `thread_messages`, the content document, graph Messages, ThreadIdentity and
  Thread in one `handle.with_transaction` call. It writes
  `content_thread_delete_receipts` in that same transaction. For this route,
  complete relational child-key capture can supply Message deletes without
  re-reading a deleted Thread; absence of a Thread preimage alone is not evidence
  that this route loses the affected document set. The receipt table is outside
  the current message mapper's explicit recognized/ignored set and needs an
  intentional operational exclusion with tests, rather than silent omission.
- `replace_source_chunks` upserts the parent, deletes all old chunk rows, inserts
  replacement chunks and updates the summary in one mixed transaction. A
  complete old/new child-PK intent union can cover this replacement. This does
  not prove that every parent deletion/ownership-change entry point has the
  same child-key coverage.

These are source-level observations at the pinned revision, not new executed
end-to-end qualifications. Generic direct graph mutations still require the
old fanout identity or an enforced contract. The remaining writer inventory
must distinguish such direct mutations from these atomic host routes; it must
not add unnecessary preimage machinery based on a hypothesized route bug.

## Ownership and baseline design requirements

A normalized document ID is not necessarily a unique source-owner identity.
Relational text PKs establish uniqueness within their tables, but graph business
ID uniqueness must be established from the actual schema/enforced writer path.
Two distinct owners producing the same body must not be accepted merely because
the unordered reducer coalesces identical records. The session needs either a
verified uniqueness contract or deterministic, explicitly specified ownership
reduction; otherwise it refuses. Namespace isolation must also cover legacy
graph Message and relational Message producers if both exist.

A baseline verification must enumerate the complete native input, run the
declared mapper, govern reduction and compare the complete resulting document
set with the held Search base. Canonical-only enumeration cannot establish
external vector equality. For such data, capture a versioned immutable external
source under the host authority and include it in the baseline/publication
dependency contract. Dropping vectors or comparing only lexical fields would
weaken the requested complete projection contract.

The resulting private receipt must bind canonical source instance/epoch,
capture contract, mapping contract, external source/vector versions where
applicable, target binding/incarnation, base selector, artifact/reduction identity
and durable operation identity. The session, rather than caller counters or raw
operation vectors, must retain custody of these values through sealing and
activation. Binding checks belong under the owner that serializes replacement
and external projection writes. A matching graph epoch alone cannot detect an
external vector write or an ABA target binding.

## Next implementation and evidence

Before native session authority is enabled, trace Thread deletion/identity
changes, content-document ownership/deletion, external vector writes and all
projection namespaces through the actual application mutation paths. Record
enforced restrictions as executable admission/schema invariants, not comments.
Add hot/cold/replay regressions proving the old and new affected document sets,
and extend the finite model with external-version and source-owner substitutions.
Then compose the owned full-source/feed cursors with mapper continuation,
governed unordered staging, full baseline verification and durable activation.

This audit identifies source-level completeness gaps; it does not claim a
production incident or a newly executed native reproduction for each row.
The legacy outer owner and unbounded per-callback fanout loops remain in place.
Their migration, cumulative physical-work/RSS bounds, power-loss composition,
and original enabled/disabled 1/4/8-writer qualification remain required.
