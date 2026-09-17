# Checkpoint decode inward contract

Status: proposal for review. No code change yet.

## Goal

Split the root `GraphStore::load_checkpoint` (currently ~580 lines in
`src/store/graph_recovery.rs`) into a storage-neutral **parse** step and a
root-owned **apply** step, so the checkpoint text decoder can live in
`skein-storage` without depending on `GraphStore`.

This mirrors the already-landed checkpoint **encode** split (#627): encode is
`CheckpointImage -> String`, and this adds `String -> DecodedCheckpoint`.

## Constraints

- **Internal, not public.** `DecodedCheckpoint` and `parse_checkpoint` are
  `#[doc(hidden)]` / crate-private in `skein-storage`. They are not re-exported
  as a new host-facing API. Skein's public surface stays query-first
  (`Database::query`, `execute`, `explain`); this contract only serves the
  recovery reconstruction path.
- **Byte-identical.** Field names, field order, checksum semantics, and every
  `SkeinError::Storage` message must remain unchanged. The existing hex-recovery
  tests are the acceptance oracle.
- **Fail-closed.** Any malformed/duplicate/out-of-order line, checksum mismatch,
  or decoded-byte-limit violation fails the open, exactly as today.

## DecodedCheckpoint

```rust
// skein-storage::checkpoint (internal)
#[doc(hidden)]
pub struct DecodedCheckpoint {
    pub generation: u64,
    pub commit_epoch: u64,
    pub next_node_id: u64,
    pub next_rel_id: u64,
    pub search_projection_change_log_start_epoch: u64,
    pub search_projection_change_log_retained_bytes: u64,
    pub search_projection_graph_changes: Vec<SearchProjectionGraphChange>,
    pub search_projection_database_identity: Option<skein_core::Uuid>,
    pub initial_import_source_fingerprint: Option<String>,
    pub nodes: Vec<NodeRecord>,
    pub relationships: Vec<RelRecord>,
    pub projected_graphs: BTreeMap<String, ProjectedGraphDefinition>,
}
```

Notes:

- `nodes` / `relationships` use the existing `NodeRecord` / `RelRecord` types
  (already in `skein-storage`). Order is significant and must be preserved.
- `catalog` descriptors (labels, rel types, tables, properties, indexes,
  constraints) and `statistics` are **not** held in `DecodedCheckpoint`. They
  are written directly into `&mut Catalog` / `&mut GraphStatistics` parameters
  because those types are already storage-neutral (`skein-core`).

## Parse (moves to skein-storage)

```rust
#[doc(hidden)]
pub fn parse_checkpoint(
    text: &str,
    catalog: &mut Catalog,
    basic_statistics: &mut GraphStatistics,
    checkpoint_statistics: &mut GraphStatistics,
) -> Result<DecodedCheckpoint>
```

Responsibilities:

- Strip and verify the trailing checksum (`split_checkpoint_checksum`).
- Reject a missing/duplicate V1 header, storage version, generation, commit
  epoch, and statistics-completeness flag.
- Parse and validate search-projection changes (epoch ordering, ordered upsert /
  delete ids, relational primary-key change validation).
- Apply catalog descriptor lines via `Catalog::import_*` in the same order.
- Populate `basic_statistics` / `checkpoint_statistics` with the same field
  semantics as today.
- Collect node / relationship lines into `nodes` / `relationships` in order.
- Collect projected-graph lines into `projected_graphs`.
- Return `DecodedCheckpoint` carrying the store-specific scalars and collections.

## Apply (stays in root)

`GraphStore::load_checkpoint` becomes:

```rust
let state = parse_checkpoint(&body, catalog, &mut self.basic_statistics,
                             &mut self.checkpoint_statistics)?;
// ... existing generation/commit_epoch and relational-checkpoint verification ...
self.next_node_id = state.next_node_id;
self.next_rel_id = state.next_rel_id;
self.search_projection_change_log_start_epoch = state.search_projection_change_log_start_epoch;
self.search_projection_change_log_retained_bytes = state.search_projection_change_log_retained_bytes;
self.search_projection_graph_changes = state.search_projection_graph_changes;
self.search_projection_database_identity = state.search_projection_database_identity;
self.initial_import_source_fingerprint = state.initial_import_source_fingerprint;
for node in state.nodes {
    self.apply_create_node_with_labels(catalog, node.id, node.labels, node.properties);
}
for rel in state.relationships {
    self.apply_create_relationship(rel.id, rel.source, rel.target, rel.rel_type, rel.properties);
}
for (name, definition) in state.projected_graphs {
    self.register_projected_graph(name, definition)?;
}
```

The node/relationship application continues to use the existing
`apply_create_node_with_labels` / `apply_create_relationship` (root-only), so
adjacency reconstruction behavior is unchanged.

## Open questions for review

1. Whether `parse_label_set` (comma-separated label ids) should be part of the
   storage parse helper set or remain inline.
2. Whether the statistics `retain_supported_property_statistics` /
   `retain_valid_index_statistics_samples` post-pass stays in root (current
   proposal: yes, it stays in `load_checkpoint`).
3. Whether to name the struct `DecodedCheckpoint` or `CheckpointState` (the
   `CheckpointImage` encode-side type already uses the `Image` suffix).

## Verification gates

- `cargo test -p skein --lib checkpoint` (currently 101 passed) must remain green.
- `cargo test -p skein --lib store::` must remain green.
- The hex-recovery tests (`src/store/tests/hex_recovery_tests.rs`) are the
  byte-format acceptance oracle and must not change.
