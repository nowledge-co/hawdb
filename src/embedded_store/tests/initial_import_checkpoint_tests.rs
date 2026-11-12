// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Initial search-projection import with deferred durability
//! (`nowledge-co/hawdb#1707`).
//!
//! These tests pin the split between `apply_initial_import_projection_delta`
//! (resident only) and `checkpoint_initial_import_projection` (one full
//! rewrite): equivalence with per-page checkpoints, the crash/resume contract,
//! and the write volume of a geometric checkpoint schedule.

use super::*;
use crate::{SearchCheckpointReport, SearchDocument, SearchHit};

const IMPORT_SOURCE_EPOCH: u64 = 77;
const EMBEDDING_DIMENSION: usize = 8;
const QUERIES: [&str; 4] = [
    "alpha river",
    "copper lantern signal",
    "delta",
    "orbit mesa",
];
const VOCABULARY: [&str; 24] = [
    "alpha", "bravo", "copper", "delta", "ember", "fjord", "garnet", "harbor", "iris", "jade",
    "kestrel", "lantern", "meadow", "nectar", "orbit", "prism", "quartz", "river", "signal",
    "tundra", "umber", "vapor", "willow", "mesa",
];

struct ImportFixture {
    root: std::path::PathBuf,
    options: EmbeddedStoreOpenOptions,
}

impl ImportFixture {
    fn new(name: &str) -> Self {
        let root = unique_embedded_store_test_dir(name);
        let options = EmbeddedStoreOpenOptions::with_search_projection(
            root.join("graph"),
            root.join("search"),
            GraphMode::WritableCutover,
        );
        let (mut store, _) = EmbeddedStore::open_with_options(options.clone()).unwrap();
        store
            .graph_mut()
            .query("CREATE (:Memory {id: 'anchor', title: 'Import anchor'})")
            .unwrap();
        drop(store);
        Self { root, options }
    }

    fn open(&self) -> EmbeddedStore {
        EmbeddedStore::open_with_options(self.options.clone())
            .unwrap()
            .0
    }
}

impl Drop for ImportFixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn mix(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xff51_afd7_ed55_8ccd);
    value ^= value >> 33;
    value = value.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    value ^ (value >> 33)
}

fn import_row(index: usize, revision: u64) -> SearchProjectionRow {
    let seed = mix((index as u64) << 8 | revision);
    let words = 6 + (seed % 10) as usize;
    let body = (0..words)
        .map(|offset| VOCABULARY[(mix(seed.wrapping_add(offset as u64)) % 24) as usize])
        .collect::<Vec<_>>()
        .join(" ");
    let embedding = (0..EMBEDDING_DIMENSION)
        .map(|axis| (mix(seed ^ axis as u64) % 1000) as f32 / 1000.0)
        .collect();
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: format!("m{index:05}"),
        title: format!("{} {index}", VOCABULARY[index % 24]),
        body,
        embedding: Some(embedding),
        source_id: None,
        metadata: BTreeMap::from([("revision".to_string(), revision.to_string())]),
    }
}

/// Page `page` inserts `rows` new documents, rewrites one document of the
/// previous page, and deletes one document of the page before that, so replay
/// must be order-preserving and idempotent rather than insert-only.
fn import_page(page: usize, rows: usize) -> SearchProjectionDelta {
    let mut upserts = (page * rows..(page + 1) * rows)
        .map(|index| import_row(index, 0))
        .collect::<Vec<_>>();
    let mut deletes = Vec::new();
    if page >= 1 {
        upserts.push(import_row((page - 1) * rows, page as u64));
    }
    if page >= 2 {
        deletes.push(format!("memory:m{:05}", (page - 2) * rows + 1));
    }
    SearchProjectionDelta {
        upserts,
        deletes,
        max_operations: None,
        source_graph_commit_epoch: Some(IMPORT_SOURCE_EPOCH),
    }
}

fn hit_signature(hits: Vec<SearchHit>) -> Vec<(String, u64, u64, u64)> {
    hits.into_iter()
        .map(|hit| {
            (
                hit.id,
                hit.score.to_bits(),
                hit.text_score.to_bits(),
                hit.vector_score.to_bits(),
            )
        })
        .collect()
}

/// Observable projection state: every candidate document plus ranked text and
/// hybrid hits (ids and exact score bits) and the durable import provenance.
#[derive(Debug, PartialEq)]
struct ProjectionState {
    documents: Vec<Option<SearchDocument>>,
    hits: Vec<Vec<(String, u64, u64, u64)>>,
    import_source_graph_commit_epoch: Option<u64>,
}

fn projection_state(store: &EmbeddedStore, max_documents: usize) -> ProjectionState {
    let index = store.search_projection().unwrap().index();
    let documents = (0..max_documents)
        .map(|id| index.document(&format!("memory:m{id:05}")).cloned())
        .collect();
    // Rank with every retriever this feature profile compiles in.
    let probe = import_row(3, 0).embedding.unwrap();
    let mut modes = Vec::new();
    if cfg!(feature = "full-text-search") {
        modes.push((SearchMode::Text, None));
    }
    if cfg!(feature = "vector-search") {
        modes.push((SearchMode::Vector, Some(probe.as_slice())));
    }
    if cfg!(all(feature = "full-text-search", feature = "vector-search")) {
        modes.push((SearchMode::Hybrid, Some(probe.as_slice())));
    }
    let mut hits = Vec::new();
    for query in QUERIES {
        for (mode, embedding) in &modes {
            hits.push(hit_signature(
                index.search(query, *embedding, *mode, 20).unwrap(),
            ));
        }
    }
    ProjectionState {
        documents,
        hits,
        import_source_graph_commit_epoch: index
            .projection_freshness()
            .import_source_graph_commit_epoch,
    }
}

fn checkpoint_bytes(report: &SearchCheckpointReport) -> u64 {
    report
        .snapshot_compressed_bytes
        .saturating_add(report.projection_bytes_written)
}

/// Host-side schedule recommended in the docs: checkpoint when the
/// un-checkpointed input reached the size already checkpointed (doubling), but
/// never let the un-checkpointed gap exceed `cap_bytes`.
fn geometric_checkpoint_due(pending: u64, checkpointed: u64, cap_bytes: Option<u64>) -> bool {
    pending >= checkpointed || cap_bytes.is_some_and(|cap| pending >= cap)
}

#[derive(Debug, Default)]
struct ImportRun {
    checkpoints: Vec<SearchCheckpointReport>,
    /// Pages covered by each checkpoint, i.e. the host's durable cursor.
    checkpoint_cursors: Vec<usize>,
    peak_pending_bytes: u64,
}

impl ImportRun {
    fn total_bytes(&self) -> u64 {
        self.checkpoints.iter().map(checkpoint_bytes).sum()
    }

    fn final_bytes(&self) -> u64 {
        self.checkpoints.last().map_or(0, checkpoint_bytes)
    }
}

/// Imports pages `start..end`, checkpointing after page `p` when `due` says
/// so and always after the last page when `finish` is set.
fn import_pages(
    store: &mut EmbeddedStore,
    pages: std::ops::Range<usize>,
    rows: usize,
    run: &mut ImportRun,
    checkpointed_bytes: &mut u64,
    mut due: impl FnMut(usize, u64, u64) -> bool,
    finish: bool,
) {
    let end = pages.end;
    for page in pages {
        store
            .apply_initial_import_projection_delta(import_page(page, rows), IMPORT_SOURCE_EPOCH)
            .unwrap();
        let pending = store.initial_import_projection_pending().unwrap();
        assert!(pending.uncheckpointed_deltas >= 1);
        run.peak_pending_bytes = run
            .peak_pending_bytes
            .max(pending.uncheckpointed_input_bytes);
        if due(
            page,
            pending.uncheckpointed_input_bytes,
            *checkpointed_bytes,
        ) || (finish && page + 1 == end)
        {
            run.checkpoints
                .push(store.checkpoint_initial_import_projection().unwrap());
            run.checkpoint_cursors.push(page + 1);
            *checkpointed_bytes += pending.uncheckpointed_input_bytes;
            assert_eq!(
                store.initial_import_projection_pending(),
                Some(Default::default())
            );
        }
    }
}

#[test]
fn initial_import_1707_deferred_checkpoint_matches_per_page_checkpoint() {
    const PAGES: usize = 12;
    const ROWS: usize = 25;
    let max_documents = PAGES * ROWS;

    let per_page = ImportFixture::new("initial_import_1707_per_page");
    let mut store = per_page.open();
    let mut per_page_reports = Vec::new();
    for page in 0..PAGES {
        per_page_reports.push(
            store
                .apply_initial_import_projection_delta_and_checkpoint(
                    import_page(page, ROWS),
                    IMPORT_SOURCE_EPOCH,
                )
                .unwrap(),
        );
        assert_eq!(
            store.initial_import_projection_pending(),
            Some(Default::default())
        );
    }
    let expected = projection_state(&store, max_documents);
    drop(store);

    let deferred = ImportFixture::new("initial_import_1707_deferred");
    let mut store = deferred.open();
    let mut deferred_reports = Vec::new();
    for page in 0..PAGES {
        deferred_reports.push(
            store
                .apply_initial_import_projection_delta(import_page(page, ROWS), IMPORT_SOURCE_EPOCH)
                .unwrap(),
        );
    }
    let pending = store.initial_import_projection_pending().unwrap();
    assert_eq!(pending.uncheckpointed_deltas, PAGES as u64);
    assert!(pending.uncheckpointed_input_bytes > 0);
    // The resident state is already searchable before durability.
    assert_eq!(projection_state(&store, max_documents), expected);
    let report = store.checkpoint_initial_import_projection().unwrap();
    assert_eq!(report.document_count, expected_document_count(PAGES, ROWS));
    assert_eq!(per_page_reports, deferred_reports);
    assert_eq!(projection_state(&store, max_documents), expected);
    drop(store);

    // Both durable projections reopen to the same state.
    assert_eq!(projection_state(&deferred.open(), max_documents), expected);
    assert_eq!(projection_state(&per_page.open(), max_documents), expected);
}

fn expected_document_count(pages: usize, rows: usize) -> usize {
    pages * rows - pages.saturating_sub(2)
}

#[test]
fn initial_import_1707_crash_resumes_from_last_checkpoint_without_skips_or_duplicates() {
    const PAGES: usize = 24;
    const ROWS: usize = 10;
    let max_documents = PAGES * ROWS;

    // Uninterrupted reference run with the geometric schedule.
    let reference = ImportFixture::new("initial_import_1707_reference");
    let mut store = reference.open();
    let mut reference_run = ImportRun::default();
    let mut checkpointed = 0;
    import_pages(
        &mut store,
        0..PAGES,
        ROWS,
        &mut reference_run,
        &mut checkpointed,
        |_, pending, checkpointed| geometric_checkpoint_due(pending, checkpointed, None),
        true,
    );
    let final_state = projection_state(&store, max_documents);
    drop(store);
    // Doubling checkpoints at pages 1, 2, 4, 8, 16 (approximately; the input
    // estimate is per byte) plus the end-of-import checkpoint.
    let last_doubling_cursor =
        reference_run.checkpoint_cursors[reference_run.checkpoint_cursors.len() - 2];
    assert!(
        (PAGES / 2..PAGES - 2).contains(&last_doubling_cursor),
        "{:?}",
        reference_run.checkpoint_cursors
    );

    // Crash points: before any checkpoint, inside an early gap, and inside the
    // last and largest gap (after the final doubling checkpoint, before the
    // end-of-import checkpoint), which loses up to about half the import.
    for (crash_after_pages, label) in [
        (1, "before_first"),
        (3, "early_gap"),
        (PAGES - 1, "last_gap"),
    ] {
        let fixture = ImportFixture::new(&format!("initial_import_1707_crash_{label}"));
        let handle = EmbeddedStoreHandle::new(fixture.open()).unwrap();
        // Host-persisted resume state, written only after a checkpoint
        // returned `Ok`: the page cursor and the checkpointed input bytes
        // that seed the doubling schedule.
        let mut durable_cursor = 0usize;
        let mut persisted_checkpointed_bytes = 0u64;
        let mut durable_state = None;
        let mut checkpointed = 0u64;
        let mut pending_at_crash = 0;
        for page in 0..crash_after_pages {
            handle
                .apply_initial_import_projection_delta(import_page(page, ROWS), IMPORT_SOURCE_EPOCH)
                .unwrap();
            let pending = handle.initial_import_projection_pending().unwrap().unwrap();
            pending_at_crash = pending.uncheckpointed_deltas;
            // `before_first` never checkpoints: the crash precedes page 1's
            // durability.
            if label != "before_first"
                && geometric_checkpoint_due(pending.uncheckpointed_input_bytes, checkpointed, None)
            {
                handle.checkpoint_initial_import_projection().unwrap();
                checkpointed += pending.uncheckpointed_input_bytes;
                // The host advances its durable cursor only after the
                // checkpoint acknowledged success.
                durable_cursor = page + 1;
                persisted_checkpointed_bytes = checkpointed;
                let store = handle.read_store().unwrap();
                durable_state = Some(projection_state(&store, max_documents));
            }
        }
        assert!(pending_at_crash >= 1, "{label}: crash must lose pages");
        if label == "last_gap" {
            assert_eq!(durable_cursor, last_doubling_cursor);
            assert_eq!(
                pending_at_crash as usize,
                crash_after_pages - durable_cursor
            );
        }
        // Crash: drop the process state without a checkpoint.
        drop(handle);

        let mut store = fixture.open();
        let recovered = projection_state(&store, max_documents);
        match &durable_state {
            Some(state) => assert_eq!(&recovered, state, "{label}"),
            None => {
                assert_eq!(durable_cursor, 0);
                assert!(recovered.documents.iter().all(Option::is_none));
                assert_eq!(recovered.import_source_graph_commit_epoch, None);
                assert_eq!(
                    store.search_projection().unwrap().index().document_count(),
                    0
                );
            }
        }
        if durable_cursor > 0 {
            assert_eq!(
                recovered.import_source_graph_commit_epoch,
                Some(IMPORT_SOURCE_EPOCH)
            );
        }
        assert_eq!(
            store.initial_import_projection_pending(),
            Some(Default::default())
        );

        // Resume from the durable cursor. Replaying the last durable page as
        // well models a host that crashed after the checkpoint but before it
        // persisted its cursor; replay is idempotent.
        let replay_from = durable_cursor.saturating_sub(1);
        let mut run = ImportRun::default();
        // Seed the doubling schedule from the persisted count, not from 0.
        let mut checkpointed = persisted_checkpointed_bytes;
        import_pages(
            &mut store,
            replay_from..PAGES,
            ROWS,
            &mut run,
            &mut checkpointed,
            |_, pending, checkpointed| geometric_checkpoint_due(pending, checkpointed, None),
            true,
        );
        assert_eq!(
            projection_state(&store, max_documents),
            final_state,
            "{label}"
        );
        assert_eq!(
            store.search_projection().unwrap().index().document_count(),
            expected_document_count(PAGES, ROWS)
        );
        drop(store);
        assert_eq!(
            projection_state(&fixture.open(), max_documents),
            final_state,
            "{label} after reopen"
        );

        // A seeded resume needs at most one more full rewrite than the
        // uninterrupted schedule still had left; restarting the count at 0
        // instead re-checkpoints after 1, 2, 4, ... replayed pages.
        let remaining_reference = reference_run
            .checkpoint_cursors
            .iter()
            .filter(|cursor| **cursor > durable_cursor)
            .count();
        assert_eq!(
            run.checkpoints.len(),
            scheduled_checkpoints(replay_from..PAGES, ROWS, persisted_checkpointed_bytes),
            "{label}"
        );
        assert!(
            run.checkpoints.len() <= remaining_reference + 1,
            "{label}: {} resumed checkpoints, {remaining_reference} remaining in the reference",
            run.checkpoints.len()
        );
        if label == "last_gap" {
            let unseeded = scheduled_checkpoints(replay_from..PAGES, ROWS, 0);
            assert!(
                unseeded >= run.checkpoints.len() + 2,
                "unseeded resume {unseeded} vs seeded {}",
                run.checkpoints.len()
            );
        }
    }
}

/// Checkpoints the doubling schedule takes over `pages` (plus the final one)
/// when seeded with `checkpointed` input bytes. Mirrors `import_pages`.
fn scheduled_checkpoints(
    pages: std::ops::Range<usize>,
    rows: usize,
    mut checkpointed: u64,
) -> usize {
    let end = pages.end;
    let mut pending = 0u64;
    let mut checkpoints = 0;
    for page in pages {
        pending +=
            crate::embedded_store::search_projection_delta_bytes(&import_page(page, rows)) as u64;
        if geometric_checkpoint_due(pending, checkpointed, None) || page + 1 == end {
            checkpoints += 1;
            checkpointed += pending;
            pending = 0;
        }
    }
    checkpoints
}

#[test]
fn initial_import_1707_crash_inside_checkpoint_fails_closed_and_reimports() {
    // The resident checkpoint durably replaces the snapshot before it
    // publishes the generation manifest. Model a crash in that window by
    // pairing the next checkpoint's snapshot with the previous generation.
    const ROWS: usize = 10;
    let fixture = ImportFixture::new("initial_import_1707_crash_inside_checkpoint");
    let search = fixture.root.join("search");
    let torn = fixture.root.join("search_torn");
    let mut store = fixture.open();
    store
        .apply_initial_import_projection_delta(import_page(0, ROWS), IMPORT_SOURCE_EPOCH)
        .unwrap();
    store.checkpoint_initial_import_projection().unwrap();
    drop(store);
    std::fs::create_dir_all(&torn).unwrap();
    for entry in std::fs::read_dir(&search).unwrap() {
        let entry = entry.unwrap();
        assert!(entry.file_type().unwrap().is_file(), "{entry:?}");
        std::fs::copy(entry.path(), torn.join(entry.file_name())).unwrap();
    }
    let mut store = fixture.open();
    store
        .apply_initial_import_projection_delta(import_page(1, ROWS), IMPORT_SOURCE_EPOCH)
        .unwrap();
    store.checkpoint_initial_import_projection().unwrap();
    let expected = projection_state(&store, 2 * ROWS);
    drop(store);
    std::fs::copy(
        search.join("search_projection.hawdb"),
        torn.join("search_projection.hawdb"),
    )
    .unwrap();

    // Reopen refuses the mixed state instead of serving either generation.
    let mut options = fixture.options.clone();
    options.search_projection_path = Some(torn.clone());
    let error = EmbeddedStore::open_with_options(options.clone()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match the published immutable projection"),
        "{error}"
    );

    // Recovery for an initial import: discard the projection and re-import
    // from page 0 with the host cursor and checkpointed count reset.
    std::fs::remove_dir_all(&torn).unwrap();
    let (mut store, _) = EmbeddedStore::open_with_options(options).unwrap();
    for page in 0..2 {
        store
            .apply_initial_import_projection_delta(import_page(page, ROWS), IMPORT_SOURCE_EPOCH)
            .unwrap();
    }
    store.checkpoint_initial_import_projection().unwrap();
    assert_eq!(projection_state(&store, 2 * ROWS), expected);
}

fn run_schedule(
    name: &str,
    pages: usize,
    rows: usize,
    due: impl FnMut(usize, u64, u64) -> bool,
) -> ImportRun {
    let fixture = ImportFixture::new(name);
    let mut store = fixture.open();
    let mut run = ImportRun::default();
    let mut checkpointed = 0;
    import_pages(
        &mut store,
        0..pages,
        rows,
        &mut run,
        &mut checkpointed,
        due,
        true,
    );
    run
}

#[test]
fn initial_import_1707_geometric_schedule_bounds_write_amplification() {
    const ROWS: usize = 20;
    let mut geometric_ratios = Vec::new();
    for pages in [8usize, 32] {
        let per_page = run_schedule(
            &format!("initial_import_1707_write_per_page_{pages}"),
            pages,
            ROWS,
            |_, _, _| true,
        );
        let geometric = run_schedule(
            &format!("initial_import_1707_write_geometric_{pages}"),
            pages,
            ROWS,
            |_, pending, checkpointed| geometric_checkpoint_due(pending, checkpointed, None),
        );
        assert_eq!(per_page.checkpoints.len(), pages);
        // The last checkpoint of either schedule writes the same final index.
        let final_bytes = geometric.final_bytes();
        assert!(final_bytes > 0);
        assert_eq!(
            per_page.checkpoints.last().unwrap().document_count,
            geometric.checkpoints.last().unwrap().document_count
        );
        let log2_pages = usize::BITS - pages.leading_zeros();
        assert!(
            geometric.checkpoints.len() <= log2_pages as usize + 2,
            "pages={pages}: {} checkpoints at {:?}",
            geometric.checkpoints.len(),
            geometric.checkpoint_cursors
        );
        // Generations are published once per checkpoint and stay monotonic.
        assert!(geometric
            .checkpoints
            .windows(2)
            .all(|pair| pair[0].projection_generation < pair[1].projection_generation));
        let ratio = geometric.total_bytes() as f64 / final_bytes as f64;
        assert!(
            ratio <= 3.0,
            "pages={pages}: geometric wrote {} bytes for a {final_bytes}-byte index",
            geometric.total_bytes()
        );
        geometric_ratios.push(ratio);
        // Per-page checkpointing is quadratic: about pages/2 full rewrites.
        let per_page_ratio = per_page.total_bytes() as f64 / final_bytes as f64;
        assert!(
            per_page_ratio >= pages as f64 / 4.0,
            "pages={pages}: per-page ratio {per_page_ratio}"
        );
        // Un-checkpointed input peaks near half the import under doubling.
        assert!(geometric.peak_pending_bytes > per_page.peak_pending_bytes);

        // Capping the gap trades back some write volume for bounded loss.
        let cap = geometric.peak_pending_bytes / 4;
        let capped = run_schedule(
            &format!("initial_import_1707_write_capped_{pages}"),
            pages,
            ROWS,
            |_, pending, checkpointed| geometric_checkpoint_due(pending, checkpointed, Some(cap)),
        );
        // The gap can overshoot the cap by at most the page that crossed it.
        assert!(capped.peak_pending_bytes < cap + per_page.peak_pending_bytes);
        assert!(capped.total_bytes() <= per_page.total_bytes());
        assert!(capped.checkpoints.len() >= geometric.checkpoints.len());
    }
    // The geometric amplification does not grow with the page count.
    assert!(geometric_ratios[1] <= 3.0 && geometric_ratios[0] <= 3.0);
}
