// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! #1711: resident (full-residency) search checkpoints through
//! `EmbeddedStore`. Every recorded IO cut must reopen the store to the
//! previous or the new checkpoint with identical search results.

use super::*;
use crate::{
    EmbeddedStore, EmbeddedStoreOpenOptions, GraphMode, SearchDocument, SearchIndex, SearchMode,
    SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
};

const SEARCH: &str = "search";
const SELECTOR: &str = "search/search_projection.out_of_core.manifest.hawdb";
const IDS: [&str; 6] = ["p0", "p1", "p2", "p3", "p4", "p5"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Path_ {
    /// `apply_search_projection_delta_and_checkpoint` with incremental search
    /// disabled: the runtime path for full-residency hosts.
    Runtime,
    /// `apply_initial_import_projection_delta_and_checkpoint`.
    InitialImport,
    /// `apply_initial_import_projection_delta` then the deferred
    /// `checkpoint_initial_import_projection` (#1707).
    DeferredInitialImport,
}

fn options(root: &Path) -> EmbeddedStoreOpenOptions {
    let mut options = EmbeddedStoreOpenOptions::with_search_projection(
        root,
        root.join(SEARCH),
        GraphMode::WritableCutover,
    );
    options.database_config = Some(DatabaseConfig {
        max_open_files: 32,
        strict_capacity_limit: true,
        ..Default::default()
    });
    options
}

fn open_store(root: &Path) -> EmbeddedStore {
    EmbeddedStore::open_with_options(options(root))
        .unwrap_or_else(|error| panic!("resident projection must reopen: {error}"))
        .0
}

fn row(page: usize, version: usize) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: IDS[page % IDS.len()].into(),
        title: format!("page {page}"),
        body: format!("resident atomic checkpoint page {page} version {version}"),
        embedding: Some(vec![1.0, (page + version) as f32 / 8.0]),
        source_id: None,
        metadata: BTreeMap::from([("space_id".into(), "default".into())]),
    }
}

/// One acknowledged page: a graph commit, then its search delta.
fn page(store: &mut EmbeddedStore, path: Path_, page: usize) {
    store
        .graph_mut()
        .query(&format!("CREATE (:Memory {{id: 'graph-{page}'}})"))
        .unwrap();
    let mut deletes = Vec::new();
    if page == 4 {
        deletes.push("memory:p1".into());
    }
    let delta = SearchProjectionDelta {
        // Pages after the first replace one earlier document and insert one.
        upserts: vec![row(page, 0), row(page.saturating_sub(2), page)],
        deletes,
        max_operations: None,
        source_graph_commit_epoch: Some(store.graph().database().commit_epoch().unwrap()),
    };
    match path {
        Path_::Runtime => {
            store
                .apply_search_projection_delta_and_checkpoint(delta)
                .unwrap();
        }
        Path_::InitialImport => {
            store
                .apply_initial_import_projection_delta_and_checkpoint(delta, 41)
                .unwrap();
        }
        Path_::DeferredInitialImport => {
            store
                .apply_initial_import_projection_delta(delta, 41)
                .unwrap();
            store.checkpoint_initial_import_projection().unwrap();
        }
    }
}

#[derive(Debug, PartialEq)]
struct State {
    source_epoch: Option<u64>,
    import_epoch: Option<u64>,
    documents: Vec<Option<SearchDocument>>,
    hits: Vec<Vec<(String, [u64; 6], String)>>,
}

fn state(index: &SearchIndex) -> State {
    let mut hits = Vec::new();
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        for query in ["resident atomic checkpoint", "version 3", "page 4"] {
            hits.push(
                index
                    .search(query, Some(&[1.0, 0.5]), mode, 10)
                    .unwrap()
                    .into_iter()
                    .map(|hit| {
                        (
                            hit.id,
                            [
                                hit.score.to_bits(),
                                hit.vector_score.to_bits(),
                                hit.text_score.to_bits(),
                                hit.rrf_score.to_bits(),
                                hit.vector_rrf_score.to_bits(),
                                hit.text_rrf_score.to_bits(),
                            ],
                            format!(
                                "{:?}",
                                (
                                    hit.vector_rank,
                                    hit.text_rank,
                                    hit.matched_terms,
                                    hit.fallback_reason_codes
                                )
                            ),
                        )
                    })
                    .collect(),
            );
        }
    }
    let freshness = index.projection_freshness();
    State {
        source_epoch: freshness.source_graph_commit_epoch,
        import_epoch: freshness.import_source_graph_commit_epoch,
        documents: IDS
            .iter()
            .map(|id| index.document(&format!("memory:{id}")).cloned())
            .collect(),
        hits,
    }
}

fn store_state(store: &EmbeddedStore) -> State {
    state(store.search_projection().unwrap().index())
}

fn generation(root: &Path) -> u64 {
    // The selector is JSON; its generation is the only top-level number used.
    let bytes = std::fs::read(root.join(SELECTOR)).unwrap();
    let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["body"]["generation"].as_u64().unwrap()
}

fn plans(snapshot: &PowerLossSnapshot, event: IoEvent) -> (Vec<CrashPlan>, usize) {
    let mut plans = publication_fault_plans(snapshot);
    let mut torn = 0;
    if event == IoEvent::Write
        && let Some(path) = snapshot.observed_path()
    {
        let complete = snapshot.persist_all_plan();
        for write in snapshot.uncovered_writes(path).unwrap() {
            if write.length < 2 {
                continue;
            }
            for bytes in [0..1, write.length / 2..write.length] {
                let mut plan = complete.clone();
                plan.persistence
                    .retain(|operation| *operation != PersistOperation::Whole(write.operation));
                plan.persistence.push(PersistOperation::TornWrite {
                    operation: write.operation,
                    bytes,
                });
                plans.push(plan);
                torn += 1;
            }
        }
    }
    (plans, torn)
}

/// A cut of the resident checkpoint and whether it precedes the selector.
struct Cut {
    event: IoEvent,
    boundary: ObservationBoundary,
    /// Project-relative path; `None` observes descendants of the search root.
    name: Option<String>,
    before_selector: bool,
}

fn cuts(next: u64) -> Vec<Cut> {
    let named = |event, boundary, name: String, before_selector| Cut {
        event,
        boundary,
        name: Some(format!("{SEARCH}/{name}")),
        before_selector,
    };
    vec![
        // First unsynchronized write of the checkpoint, with torn variants.
        Cut {
            event: IoEvent::Write,
            boundary: ObservationBoundary::After,
            name: None,
            before_selector: true,
        },
        // The old protocol replaced the snapshot here, before the selector.
        named(
            IoEvent::Rename,
            ObservationBoundary::After,
            format!("search_projection.{next}.hawdb"),
            true,
        ),
        named(
            IoEvent::Rename,
            ObservationBoundary::After,
            format!("search_projection_out_of_core_layout.{next}.hawdb"),
            true,
        ),
        named(
            IoEvent::Rename,
            ObservationBoundary::Before,
            "search_projection.out_of_core.manifest.hawdb".into(),
            true,
        ),
        named(
            IoEvent::Rename,
            ObservationBoundary::After,
            "search_projection.out_of_core.manifest.hawdb".into(),
            false,
        ),
        named(
            IoEvent::Rename,
            ObservationBoundary::After,
            "search_projection.hawdb".into(),
            false,
        ),
        named(
            IoEvent::Rename,
            ObservationBoundary::After,
            "search_lexical.manifest.hawdb".into(),
            false,
        ),
        // Old-generation cleanup after the new selector: the snapshot two
        // generations back is no longer current or previous.
        named(
            IoEvent::Remove,
            ObservationBoundary::After,
            format!("search_projection.{}.hawdb", next - 2),
            false,
        ),
    ]
}

fn qualify(path: Path_) {
    // Three acknowledged checkpoints, so the fourth has a previous generation
    // to retain and an older one to remove. The fourth is cut.
    const ACKNOWLEDGED: usize = 3;
    let probe = Fixture::new();
    let next = {
        let mut store = open_store(&probe.root);
        for number in 0..ACKNOWLEDGED {
            page(&mut store, path, number);
        }
        drop(store);
        generation(&probe.root) + 1
    };
    drop(probe);
    for cut in cuts(next) {
        let mut fixture = Fixture::new();
        let mut store = open_store(&fixture.root);
        for number in 0..ACKNOWLEDGED {
            page(&mut store, path, number);
        }
        drop(store);
        let previous_generation = generation(&fixture.root);
        assert_eq!(previous_generation + 1, next);
        let mut store = open_store(&fixture.root);
        let old = store_state(&store);
        let point = ObservationPoint {
            event: cut.event,
            relative_path: cut.name.clone().unwrap_or_else(|| SEARCH.into()).into(),
            boundary: cut.boundary,
            skip_matches: 0,
            include_descendants: cut.name.is_none(),
            keep_last: false,
        };
        fixture.model.observe(point).unwrap();
        page(&mut store, path, ACKNOWLEDGED);
        let new = store_state(&store);
        assert_ne!(old, new);
        drop(store);
        assert_eq!(generation(&fixture.root), next);
        assert_eq!(
            state(&SearchIndex::open(fixture.root.join(SEARCH)).unwrap()),
            new
        );
        let acknowledged = fixture.model.capture().unwrap();
        let snapshot = fixture
            .model
            .take_observation()
            .unwrap()
            .unwrap_or_else(|| {
                panic!(
                    "{path:?} cut {:?} {:?} was not reached",
                    cut.event, cut.name
                )
            });
        let (plans, torn) = plans(&snapshot, cut.event);
        eprintln!(
            "search-power-resident-v1 path={path:?} event={:?} boundary={:?} name={:?} plans={} torn={torn}",
            cut.event,
            cut.boundary,
            cut.name,
            plans.len()
        );
        if cut.event == IoEvent::Write {
            assert!(
                torn > 0,
                "the first checkpoint write must exercise torn images"
            );
        }
        let mut saw = [false; 2];
        for plan in plans {
            let image = fixture.image(&snapshot, &plan);
            let store = EmbeddedStore::open_with_options(options(&image))
                .unwrap_or_else(|error| {
                    panic!(
                        "{path:?} cut {:?} {:?} left an unopenable projection: {error}; plan={plan:?}",
                        cut.event, cut.name
                    )
                })
                .0;
            let recovered = store_state(&store);
            let is_new = recovered == new;
            assert!(
                is_new || recovered == old,
                "{path:?} cut {:?} {:?} reopened neither checkpoint; plan={plan:?}",
                cut.event,
                cut.name
            );
            if cut.before_selector {
                assert!(!is_new, "a pre-selector cut cannot publish the checkpoint");
            }
            saw[usize::from(is_new)] = true;
            drop(store);
        }
        if !cut.before_selector {
            // The persist-all plan always includes the selector rename.
            assert!(saw[1], "{path:?} cut {:?} {:?}", cut.event, cut.name);
        }
        // A lost acknowledgement must still retain the whole checkpoint.
        let image = fixture.image(&acknowledged, &CrashPlan::default());
        let store = open_store(&image);
        assert_eq!(store_state(&store), new);
        drop(store);
    }
}

#[test]
fn resident_runtime_checkpoint_survives_lost_torn_and_reordered_writes() {
    qualify(Path_::Runtime);
}

#[test]
fn resident_initial_import_checkpoint_survives_lost_torn_and_reordered_writes() {
    qualify(Path_::InitialImport);
}

#[test]
fn resident_deferred_initial_import_checkpoint_survives_lost_torn_and_reordered_writes() {
    qualify(Path_::DeferredInitialImport);
}
