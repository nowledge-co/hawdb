// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Repeated explicit `EmbeddedStoreHandle::checkpoint()` calls must reclaim
//! data-bearing WAL generations, not only the empty ones (#1727).
//!
//! Each checkpoint of the admitted `main` branch seals its private WAL beside
//! `branch.head` and selects a successor. The superseded branch WAL must be
//! reclaimed by durable generation reclamation, independently of background
//! maintenance, and without leaving reclamation debt.

use super::*;

#[derive(Clone, Copy, Debug)]
enum Variant {
    GraphOnly,
    Resident,
    Incremental,
}

fn walk(dir: &std::path::Path, root: &std::path::Path, out: &mut Vec<(String, u64)>) {
    for entry in std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        if path.is_dir() {
            walk(&path, root, out);
        } else {
            let len = entry.metadata().map_or(0, |metadata| metadata.len());
            out.push((path.strip_prefix(root).unwrap().display().to_string(), len));
        }
    }
}

/// Every `wal.<generation>.hawdb` under the graph root, including the
/// branch-owned WAL directory, with its size.
fn wal_files(graph: &std::path::Path) -> Vec<String> {
    let mut all = Vec::new();
    walk(graph, graph, &mut all);
    let mut wal = all
        .into_iter()
        .filter(|(path, _)| {
            let name = path.rsplit('/').next().unwrap();
            name.starts_with("wal.") && name.ends_with(".hawdb")
        })
        .collect::<Vec<_>>();
    wal.sort();
    wal.into_iter()
        .map(|(path, len)| format!("{path}:{len}"))
        .collect()
}

struct VariantRun {
    history: Vec<Vec<String>>,
    retry_required: bool,
    pending_files: usize,
    pending_bytes: u64,
}

fn open_options(
    variant: Variant,
    graph: &std::path::Path,
    search: &std::path::Path,
    background_maintenance: bool,
) -> EmbeddedStoreOpenOptions {
    let mut config = DatabaseConfig::default();
    config.runtime_capabilities.background_maintenance = background_maintenance;
    match variant {
        Variant::GraphOnly => {
            EmbeddedStoreOpenOptions::graph_only(graph, GraphMode::WritableCutover)
        }
        Variant::Resident => EmbeddedStoreOpenOptions::with_search_projection(
            graph,
            search,
            GraphMode::WritableCutover,
        ),
        Variant::Incremental => EmbeddedStoreOpenOptions::with_search_projection(
            graph,
            search,
            GraphMode::WritableCutover,
        )
        .with_incremental_search_maintenance(Default::default()),
    }
    .with_database_config(config)
}

fn run_variant(variant: Variant, background_maintenance: bool) -> VariantRun {
    let name = format!("wal_reclaim_{variant:?}_{background_maintenance}");
    let root = unique_embedded_store_test_dir(&name);
    let graph = root.join("graph");
    let search = root.join("search");
    if matches!(variant, Variant::Incremental) {
        let mut index = SearchIndex::open(&search).unwrap();
        index
            .upsert_projection_row(SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "bootstrap".into(),
                title: "bootstrap".into(),
                body: "bootstrap".into(),
                embedding: None,
                source_id: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        index.checkpoint().unwrap();
        drop(index);
    }
    let options = open_options(variant, &graph, &search, background_maintenance);
    let (handle, _) = EmbeddedStoreHandle::open_with_options(options).unwrap();
    let mut history = Vec::new();
    for round in 0..8 {
        for item in 0..4 {
            handle
                .query_with_report(&format!(
                    "CREATE (:Memory {{id: 'm{round}_{item}', title: 'round {round} item {item}'}})"
                ))
                .unwrap();
        }
        if !matches!(variant, Variant::GraphOnly) {
            let _ = handle.catch_up_search_projection(64, 4);
        }
        handle.checkpoint().unwrap();
        let files = wal_files(&graph);
        eprintln!("{variant:?} bg={background_maintenance} round {round}: {files:?}");
        history.push(files);
    }
    let pressure = handle.storage_pressure_snapshot().unwrap();
    drop(handle);

    // Reclamation must not cost data: reopen and read back every write.
    let (reopened, _) = EmbeddedStoreHandle::open_with_options(open_options(
        variant,
        &graph,
        &search,
        background_maintenance,
    ))
    .unwrap();
    let ids = reopened
        .query_with_report("MATCH (m:Memory) RETURN m.id AS id ORDER BY id")
        .unwrap()
        .output
        .rows
        .into_iter()
        .map(|row| row["id"].clone())
        .collect::<Vec<_>>();
    // Single-digit rounds and items: lexical `ORDER BY id` is this order.
    let expected = (0..8)
        .flat_map(|round| (0..4).map(move |item| Value::String(format!("m{round}_{item}"))))
        .collect::<Vec<_>>();
    assert_eq!(ids, expected, "{variant:?} bg={background_maintenance}");
    drop(reopened);
    let _ = std::fs::remove_dir_all(root);
    VariantRun {
        history,
        retry_required: pressure.generation_reclamation_retry_required,
        pending_files: pressure.generation_reclamation_pending_files,
        pending_bytes: pressure.generation_reclamation_pending_bytes,
    }
}

fn assert_bounded(variant: Variant, background_maintenance: bool) {
    let run = run_variant(variant, background_maintenance);
    let history = &run.history;
    let last = history.last().unwrap();
    // Each WAL directory (store root, branch directory, branch data and
    // runtime roots) keeps at most the current and previous generation, and
    // repeated checkpoints must not grow the total.
    let mut per_directory = BTreeMap::<String, usize>::new();
    for file in last {
        let directory = file.rsplit_once('/').map_or("", |(directory, _)| directory);
        *per_directory.entry(directory.to_string()).or_default() += 1;
    }
    assert!(
        per_directory.values().all(|count| *count <= 2) && last.len() <= history[1].len(),
        "{variant:?} bg={background_maintenance} retained WAL generations: {history:#?}"
    );
    assert!(
        !run.retry_required && run.pending_files == 0 && run.pending_bytes == 0,
        "{variant:?} bg={background_maintenance} left reclamation debt: retry={} files={} bytes={}",
        run.retry_required,
        run.pending_files,
        run.pending_bytes
    );
}

#[test]
fn explicit_checkpoint_reclaims_wal_graph_only() {
    assert_bounded(Variant::GraphOnly, false);
}

#[test]
fn explicit_checkpoint_reclaims_wal_resident_projection() {
    assert_bounded(Variant::Resident, false);
}

#[test]
fn explicit_checkpoint_reclaims_wal_incremental_projection() {
    assert_bounded(Variant::Incremental, false);
}

#[test]
fn explicit_checkpoint_reclaims_wal_graph_only_background() {
    assert_bounded(Variant::GraphOnly, true);
}

#[test]
fn explicit_checkpoint_reclaims_wal_resident_projection_background() {
    assert_bounded(Variant::Resident, true);
}

#[test]
fn explicit_checkpoint_reclaims_wal_incremental_projection_background() {
    assert_bounded(Variant::Incremental, true);
}
