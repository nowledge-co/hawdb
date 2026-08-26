# Search Projection Changefeed Contract

## Scope

This contract defines the commit-ordered identity feed used to maintain an
external search projection from canonical graph and relational mutations. The
feed is part of the embedded Skein library surface. Search documents, lexical
segments, embeddings, ranks, and projection-owned watermarks remain derived
state owned by `SearchIndex`; they MUST NOT become canonical graph or
relational WAL payloads.

## Unified commit identity

Every retained change has one `commit_epoch` from the same transaction commit
that publishes its graph and relational effects. A change may contain:

- graph node identities to hydrate;
- graph document identities to delete;
- relational table and primary-key identities to hydrate or delete; or
- a fixed-size rebuild barrier when an exact relational identity set cannot be
  represented within its admission limits.

The relation identity capture MUST be derived from the staged transaction's
single before/after state. Predicate scans and conflict probes that do not
change a row MUST NOT be emitted. Primary-key-changing updates MUST emit both
the old and new keys. Identical replacement rows and missing-key deletes MUST
NOT create false changes.

The reserved `skein_schema_migrations` registry is engine control metadata,
not searchable application content. Its exact key changes MUST be omitted from
the external search feed so an otherwise graph-only database can use the
graph-only catch-up helper. Application table changes remain in the unified
feed even when the caller later maps a table to no search documents.

The relational WAL record MAY retain table and primary-key identities needed
to reconstruct this feed after restart. It MUST NOT retain a search document,
row payload copied for search, embedding, rank, token stream, SearchIndex
generation, or SearchIndex watermark. A relational WAL record that cannot fit
the admitted exact identity set MUST retain a fixed-size rebuild barrier
instead of rejecting an otherwise valid canonical transaction solely for the
derived projection.

## Bounded retention

`RelationalPrimaryKeyChangeCaptureLimits` bounds key count and encoded identity
bytes for one commit. Exceeding either limit produces
`RequiresRebuild(CaptureLimitExceeded)` atomically; the feed MUST NOT publish a
partial key set.

`DatabaseConfig::max_search_projection_change_log_entries` and
`DatabaseConfig::max_search_projection_change_log_bytes` jointly bound the
retained in-process feed. Trimming removes a whole oldest commit, advances the
resume floor through that commit, and never splits a commit. A caller whose
cursor is below the floor MUST rebuild instead of receiving a partial suffix.
The status API MUST expose retained entry bytes and the configured byte cap.

## Recovery

A checkpoint MUST encode the graph identities, relational identity capture or
rebuild barrier, and commit epoch symmetrically. Ordinary restart MUST reject
malformed, unordered, duplicate, or epoch-inconsistent entries. WAL replay
MUST reconstruct post-checkpoint changes from the relational WAL capture and
graph operations before serving the feed. A missing relational WAL capture,
full relational snapshot replacement, or multiple relational transactions in
one logical WAL batch MUST produce a rebuild barrier rather than silently
advance incremental freshness.

## Consumption and publication

`Database::build_search_projection_change_batch_after` returns only whole
commits and applies one operation budget across graph identities and distinct
relational primary keys. The batch may deduplicate the same identity across
commits because the consumer hydrates canonical current state. Its
`complete_through_commit_epoch` is therefore a lower-bound freshness cursor:
every relevant identity through that epoch has been delivered, while a
hydrated value may already reflect a later canonical commit.

The graph-only builder MUST fail when the selected range contains relational
changes. It MUST NOT publish a watermark that silently skips relational work.

`Database::apply_search_projection_change_batch` publishes the combined graph
and caller-derived relational delta. The caller MUST acknowledge the exact
number of primary-key identities in the batch. A count mismatch or a
caller-supplied source epoch MUST fail before mutating `SearchIndex`. The
combined delta and its complete-through epoch are applied together; the
watermark MUST NOT advance when relational work is incomplete or delta
validation fails.

`Database::catch_up_search_projection_with_relational` is the bounded host
integration loop for that unified path. It MUST select only whole commits, call
the host hydrator before projection mutation, apply graph and relational deltas
together, and checkpoint the persistent `SearchIndex` after every successful
batch. The operation-per-batch and batch-count limits MUST both be positive.
Hydrator failure, incomplete primary-key acknowledgement, an indivisible commit
larger than the operation limit, resume-floor expiry, or a retained rebuild
barrier MUST fail without publishing a new watermark. Restart resumes from the
last durable projection checkpoint. The existing graph-only catch-up helper
continues to reject a selected range containing relational changes.

`NowledgeMemEmbeddedStore` and `NowledgeMemEmbeddedStoreHandle` expose the same
typed contract. The host hydrator receives one pinned
`DatabaseReadTransaction` and the exact `SearchProjectionChangeBatch`; it does
not own batch selection, watermark publication, or checkpoint order. This is
the supported embedded library seam for application-owned SQL-to-search
mapping. It is not a CLI, helper-process, or route-specific control plane.

## Formal refinement

`SkeinProjectionChangefeed.tla` models the following obligations:

- a visible canonical commit is already durable;
- every relevant committed epoch has either an exact retained change or a
  rebuild barrier;
- resume starts at or above the retained floor and consumes whole commits;
- projection publication never exceeds the canonical epoch and requires all
  delivered identities to be processed;
- garbage collection removes only whole oldest changes and advances the floor;
- a cursor below the floor or a retained barrier cannot publish incrementally;
- search payload is outside the modeled canonical WAL state.

The model is part of `//docs/tla:storage_models` and is checked through the
repository's pinned `rules_tla` Bazel toolchain.
