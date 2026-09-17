# Graph engine inward contract

Status: proposal for review. No code change yet.

## Goal

Move the concrete graph engine (`GraphStore` + its `graph_*` / relational-row /
index-shadow modules, currently in `src/store*`) into `skein-storage`, keeping
`src` as the query-first facade (`Database`, `NowledgeMemGraph`, `SkeinEmbedded`).

`src` must not depend on the concrete store type; it consumes a storage-neutral
contract. The contract stays internal (`#[doc(hidden)]`), never a host API.

## Current layering (verified)

- `Database` (`src/api/mod.rs`) owns `store: GraphStore` directly and calls its
  methods (the graph engine exposes ~450 methods across `impl GraphStore` in
  `graph_read` / `graph_mutation` / `graph_commit` / `graph_apply` /
  `graph_checkpoint` / `graph_recovery` / `graph_indexes` /
  `graph_columnar_shadow` / `relational_row_pages` / `relational_index_shadow`).
- `GraphStore` fields are almost all `skein-storage` types
  (`CowSegmentedMap`, `RelationalState`, `AppendState`,
  `ColumnarShadowState`-adjacent readers, `Arc<dyn BackgroundWorkAdmission>`, …).
  The only root types are `durable: Option<DurableStore>` and three root state
  structs (`ColumnarShadowState`, `RelationalIndexShadowState`,
  `RelationalRowPageState`).

## Existing inward-contract pattern (verified)

The engine already implements storage-side traits through thin root adapters,
which is exactly the pattern to extend:

- `skein_analytics::ProjectionSource` (`crates/analytics`) — implemented in
  `src/analytics.rs` via `visit_projection_nodes` / `visit_projection_relationships`.
- `skein_search::SearchProjectionSource` — implemented in `src/search.rs`.
- `skein_system_sql::SystemSqlStore` (`crates/system-sql`) — implemented in
  `src/store.rs` (`commit_epoch`, `statistics`, `projected_graph_statuses`, …).
- Host seams are already abstracted: `skein_storage::BackgroundWorkAdmission`
  for the QoS governor, `TelemetrySink` for telemetry.

## Blockers

1. `GraphStore` owns `DurableStore` (root), whose checkpoint / manifest /
   publication orchestration still uses root helpers.
2. `GraphStore` owns three root state structs that are storage-neutral data but
   whose methods call `GraphStore` methods. They must move with the engine.
3. The facade and `nowledge_mem` call `GraphStore` methods directly instead of
   through a trait.

## Proposed inward contracts

1. **`GraphEngine` (storage-side, `#[doc(hidden)]`)** — the read/mutation/
   commit/statistics surface the facade consumes. `Database` holds
   `Arc<dyn GraphEngine>`-equivalent (or a generic `Store: GraphEngine`) instead
   of the concrete `GraphStore`.
2. **Checkpoint/statistics orchestration contract** — the interface the durable
   orchestration drives (`prepare_checkpoint`, `load_checkpoint`, statistics
   refresh), so the checkpoint orchestration can move next to the engine.
3. **Transaction-private relational view contract** — the interface the
   transaction layer uses to bind/unbind private relational read views, so view
   lifetimes have a clear owner inside storage.

Host seams (`BackgroundWorkAdmission`, telemetry, spill) stay traits the engine
consumes; they are already storage-side.

## Proposed phasing

1. Move the three storage-neutral state structs (`ColumnarShadowState`,
   `RelationalIndexShadowState`, `RelationalRowPageState`) into `skein-storage`
   with their private helpers, leaving `impl GraphStore` glue in root.
2. Introduce the `GraphEngine` trait in `skein-storage` and implement it for
   `GraphStore`; switch `Database` to generic-over-`GraphEngine` and remove
   direct concrete-store calls.
3. Move the durable/checkpoint orchestration behind the checkpoint contract.
4. Move `GraphStore` + `graph_*` modules into `skein-storage::graph`, leaving
   only the facade and adapters in `src`.

## Open questions for review

1. `GraphEngine` as one trait or split read/mutate/commit traits?
2. Keep `Database` generic (`Database<Store>`) or boxed (`Arc<dyn>`)?
3. Whether the durable orchestration moves before or after `GraphStore`.

## Verification

Each phase keeps the full `cargo test -p skein --lib` + `cargo test -p
skein-storage --lib` + bazel root suites green; no public facade change.
