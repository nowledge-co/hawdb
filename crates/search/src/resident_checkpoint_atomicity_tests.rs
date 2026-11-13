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

//! #1711: an unregistered resident checkpoint reopens to the previous or the
//! new checkpoint after an abort at any step, never to an unopenable mix.

use super::*;
use crate::out_of_core::{inject_resident_checkpoint_fault, ResidentCheckpointStep as Step};

const IDS: [&str; 5] = ["a", "b", "c", "d", "e"];

struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        Self(test_temp_dir().join(format!(
            "hawdb-resident-atomic-{name}-{}",
            hawdb_core::generate_uuidv7().unwrap()
        )))
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn row(id: &str, body: &str, embedding: [f32; 2]) -> SearchProjectionRow {
    SearchProjectionRow {
        kind: SearchProjectionKind::Memory,
        external_id: id.into(),
        title: format!("title {id}"),
        body: body.into(),
        embedding: Some(embedding.to_vec()),
        source_id: None,
        metadata: BTreeMap::from([("kind".into(), "memory".into())]),
    }
}

fn delta(epoch: u64, upserts: Vec<SearchProjectionRow>, deletes: &[&str]) -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts,
        deletes: deletes.iter().map(|id| format!("memory:{id}")).collect(),
        max_operations: None,
        source_graph_commit_epoch: Some(epoch),
    }
}

/// Two acknowledged checkpoints, so the previous generation is retained.
fn base(path: &Path) {
    let mut index = SearchIndex::open(path).unwrap();
    index
        .apply_projection_delta(delta(
            3,
            vec![
                row("a", "atomic checkpoint alpha", [1.0, 0.0]),
                row("b", "atomic checkpoint beta", [0.0, 1.0]),
            ],
            &[],
        ))
        .unwrap();
    index.checkpoint().unwrap();
    index
        .apply_projection_delta(delta(
            5,
            vec![row("c", "checkpoint gamma", [0.6, 0.8])],
            &[],
        ))
        .unwrap();
    index.checkpoint().unwrap();
}

/// Replacement, insertion and deletion with an advanced source epoch.
fn mutation() -> SearchProjectionDelta {
    delta(
        9,
        vec![
            row("b", "replaced atomic beta", [0.8, 0.6]),
            row("d", "inserted atomic delta checkpoint", [0.3, 0.9]),
        ],
        &["a"],
    )
}

#[derive(Debug, PartialEq, Eq)]
struct Hit {
    id: String,
    bits: [u64; 6],
    ranks: [Option<usize>; 2],
    /// Matched terms and any fallback reason: a reopen that lost its lexical
    /// or vector projection must not pass as the same state.
    evidence: String,
}

#[derive(Debug, PartialEq)]
struct State {
    source_epoch: Option<u64>,
    document_count: usize,
    documents: Vec<Option<SearchDocument>>,
    hits: Vec<Vec<Hit>>,
}

/// Search modes the crate was built with. Feature-reduced builds (the Bazel
/// `minimal`, `vector_only` and `*_background` profiles) still compare epochs
/// and documents, plus whichever retrievers they compile.
fn compiled_modes() -> Vec<SearchMode> {
    let mut modes = Vec::new();
    if cfg!(feature = "full-text-search") {
        modes.push(SearchMode::Text);
    }
    if cfg!(feature = "vector-search") {
        modes.push(SearchMode::Vector);
    }
    if cfg!(all(feature = "full-text-search", feature = "vector-search")) {
        modes.push(SearchMode::Hybrid);
    }
    modes
}

fn state(index: &SearchIndex) -> State {
    let mut hits = Vec::new();
    for mode in compiled_modes() {
        for query in ["atomic checkpoint", "beta", "delta"] {
            hits.push(
                index
                    .search(query, Some(&[0.7, 0.7]), mode, 10)
                    .unwrap()
                    .into_iter()
                    .map(|hit| Hit {
                        id: hit.id,
                        bits: [
                            hit.score.to_bits(),
                            hit.vector_score.to_bits(),
                            hit.text_score.to_bits(),
                            hit.rrf_score.to_bits(),
                            hit.vector_rrf_score.to_bits(),
                            hit.text_rrf_score.to_bits(),
                        ],
                        ranks: [hit.vector_rank, hit.text_rank],
                        evidence: format!(
                            "{:?}",
                            (
                                hit.kind,
                                hit.matched_terms,
                                hit.matched_spans,
                                hit.fallback_reason_codes
                            )
                        ),
                    })
                    .collect(),
            );
        }
    }
    State {
        source_epoch: index.projection_freshness().source_graph_commit_epoch,
        document_count: index.document_count(),
        documents: IDS
            .iter()
            .map(|id| index.document(&format!("memory:{id}")).cloned())
            .collect(),
        hits,
    }
}

fn reopened_state(path: &Path) -> State {
    state(&SearchIndex::open(path).unwrap())
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }
}

fn manifest_generation(path: &Path) -> u64 {
    out_of_core::published_generation_for_test(path)
}

/// Abort `checkpoint` at `step`, then reopen. Returns whether the reopened
/// projection is the new checkpoint (otherwise it equals the previous one).
fn abort_and_reopen(step: Step, old: &State, new: &State, root: &Path) -> bool {
    let path = root.join(format!("{step:?}").replace(['"', '(', ')', '.'], "_"));
    base(&path);
    let previous_generation = manifest_generation(&path);
    {
        let mut index = SearchIndex::open(&path).unwrap();
        index.apply_projection_delta(mutation()).unwrap();
        inject_resident_checkpoint_fault(Some(step));
        let error = index.checkpoint().unwrap_err();
        inject_resident_checkpoint_fault(None);
        assert!(error.to_string().contains("injected"), "{step:?}: {error}");
    }
    let reopened = SearchIndex::open(&path)
        .unwrap_or_else(|error| panic!("{step:?} left an unopenable projection: {error}"));
    let recovered = state(&reopened);
    let is_new = recovered == *new;
    assert!(
        is_new || recovered == *old,
        "{step:?} reopened neither checkpoint: {recovered:#?}"
    );
    // The previous generation's snapshot is retained until a newer one is
    // published, and the current generation's snapshot is always present.
    let current = manifest_generation(&path);
    assert_eq!(current, previous_generation + u64::from(is_new), "{step:?}");
    assert!(path.join(resident_snapshot_file(current)).is_file());
    assert!(path
        .join(resident_snapshot_file(previous_generation))
        .is_file());
    // A recovered projection checkpoints and reopens again.
    drop(reopened);
    let mut index = SearchIndex::open(&path).unwrap();
    if !is_new {
        index.apply_projection_delta(mutation()).unwrap();
    }
    index.checkpoint().unwrap();
    drop(index);
    assert_eq!(reopened_state(&path), *new, "{step:?} retry");
    is_new
}

#[test]
fn resident_checkpoint_abort_at_every_step_reopens_previous_or_new() {
    let root = Dir::new("steps");
    let oracle = root.0.join("oracle");
    base(&oracle);
    let old = reopened_state(&oracle);
    let mut index = SearchIndex::open(&oracle).unwrap();
    index.apply_projection_delta(mutation()).unwrap();
    index.checkpoint().unwrap();
    drop(index);
    let new = reopened_state(&oracle);
    assert_ne!(old, new);
    assert_eq!(new.source_epoch, Some(9));
    assert!(new.hits.iter().all(|hits| !hits.is_empty()));

    for (step, expect_new) in [
        (Step::RaBitQ, false),
        (Step::Snapshot, false),
        (Step::SegmentArtifacts, false),
        (Step::Lexical, false),
        (Step::BeforeSelector, false),
        (Step::AfterSelector, true),
        (Step::AfterAlias(SEARCH_SNAPSHOT_FILE), true),
        (Step::AfterAlias(SEARCH_SEGMENT_PAYLOAD_FILE), true),
        (Step::AfterAlias(SEARCH_SEGMENT_DESCRIPTOR_FILE), true),
        (Step::AfterAlias(lexical_projection::MANIFEST_FILE), true),
        (Step::Cleanup, true),
    ] {
        assert_eq!(
            abort_and_reopen(step, &old, &new, &root.0),
            expect_new,
            "{step:?}"
        );
    }
}

#[test]
fn resident_checkpoint_repeated_deltas_abort_on_the_nth_checkpoint() {
    // Runtime hosts with incremental search disabled checkpoint every small
    // delta. A crash on any one of them reopens to the last acknowledged one.
    let root = Dir::new("repeated");
    let path = root.0.join("search");
    let rows = |n: u64| {
        delta(
            10 + n,
            vec![row(
                IDS[(n as usize) % IDS.len()],
                &format!("runtime atomic checkpoint {n}"),
                [1.0, n as f32 / 8.0],
            )],
            &[],
        )
    };
    for crash_at in 1..=4u64 {
        let _ = std::fs::remove_dir_all(&path);
        let mut acknowledged = None;
        for n in 0..=crash_at {
            let mut index = SearchIndex::open(&path).unwrap();
            index.apply_projection_delta(rows(n)).unwrap();
            if n == crash_at {
                inject_resident_checkpoint_fault(Some(Step::BeforeSelector));
                index.checkpoint().unwrap_err();
                inject_resident_checkpoint_fault(None);
            } else {
                index.checkpoint().unwrap();
                drop(index);
                acknowledged = Some(reopened_state(&path));
            }
        }
        assert_eq!(reopened_state(&path), acknowledged.unwrap());
        // The next acknowledged delta also survives a post-selector abort.
        let mut index = SearchIndex::open(&path).unwrap();
        index.apply_projection_delta(rows(crash_at)).unwrap();
        inject_resident_checkpoint_fault(Some(Step::AfterSelector));
        index.checkpoint().unwrap_err();
        inject_resident_checkpoint_fault(None);
        let expected = state(&index);
        drop(index);
        assert_eq!(reopened_state(&path), expected);
    }
}

#[test]
fn old_layout_without_generation_snapshots_reopens_and_upgrades() {
    let root = Dir::new("old_layout");
    let path = root.0.join("search");
    base(&path);
    let expected = reopened_state(&path);
    // Main before #1711 kept only the unsuffixed snapshot.
    for entry in std::fs::read_dir(&path).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if name.starts_with("search_projection.") && name != SEARCH_SNAPSHOT_FILE {
            if name.starts_with("search_projection.out_of_core.") {
                continue;
            }
            std::fs::remove_file(path.join(&name)).unwrap();
        }
    }
    let snapshot = std::fs::read(path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
    std::fs::remove_file(path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
    std::fs::write(path.join(SEARCH_SNAPSHOT_FILE), &snapshot).unwrap();
    assert_eq!(reopened_state(&path), expected);

    // The first new-protocol checkpoint over the old layout is atomic too.
    let mut index = SearchIndex::open(&path).unwrap();
    index.apply_projection_delta(mutation()).unwrap();
    inject_resident_checkpoint_fault(Some(Step::BeforeSelector));
    index.checkpoint().unwrap_err();
    inject_resident_checkpoint_fault(None);
    drop(index);
    assert_eq!(reopened_state(&path), expected);
    let mut index = SearchIndex::open(&path).unwrap();
    index.apply_projection_delta(mutation()).unwrap();
    index.checkpoint().unwrap();
    let new = state(&index);
    drop(index);
    assert_eq!(reopened_state(&path), new);
    assert!(path
        .join(resident_snapshot_file(manifest_generation(&path)))
        .is_file());
}

/// The refusal is typed so a host can rebuild the projection (#1735), and
/// its text is unchanged.
fn assert_search_projection_mismatch(error: &HawDBError, projection: &Path) {
    assert!(
        matches!(
            error,
            HawDBError::SearchProjectionMismatch { projection: Some(path), .. }
                if path == projection
        ),
        "{error:?}"
    );
    assert_eq!(
        error.to_string(),
        "storage error: resident search snapshot does not match the published immutable projection; incremental catch-up cannot repair it: rebuild it from the graph into a new directory with EmbeddedStoreHandle::rebuild_out_of_core_search_projection, or rebuild a resident projection with Database::rebuild_search_projection"
    );
}

#[test]
fn old_layout_mixed_snapshot_still_fails_closed() {
    // Without a generation snapshot there is no verified source to re-derive
    // from, so the pre-#1711 crash state keeps its explicit refusal.
    let root = Dir::new("old_mixed");
    let path = root.0.join("search");
    base(&path);
    let stale = root.0.join("stale");
    copy_dir(&path, &stale);
    let mut index = SearchIndex::open(&path).unwrap();
    index.apply_projection_delta(mutation()).unwrap();
    index.checkpoint().unwrap();
    drop(index);
    for entry in std::fs::read_dir(&stale).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if name.starts_with("search_projection.")
            && name != SEARCH_SNAPSHOT_FILE
            && !name.starts_with("search_projection.out_of_core.")
        {
            std::fs::remove_file(stale.join(name)).unwrap();
        }
    }
    std::fs::copy(
        path.join(SEARCH_SNAPSHOT_FILE),
        stale.join(SEARCH_SNAPSHOT_FILE),
    )
    .unwrap();
    let error = SearchIndex::open(&stale).err().unwrap();
    assert_search_projection_mismatch(&error, &stale);
}

#[test]
fn damaged_generation_snapshot_is_never_linked() {
    let root = Dir::new("damaged");
    let path = root.0.join("search");
    base(&path);
    let mut index = SearchIndex::open(&path).unwrap();
    index.apply_projection_delta(mutation()).unwrap();
    inject_resident_checkpoint_fault(Some(Step::AfterSelector));
    index.checkpoint().unwrap_err();
    inject_resident_checkpoint_fault(None);
    drop(index);
    // The selected generation's snapshot no longer matches its receipt.
    let selected = path.join(resident_snapshot_file(manifest_generation(&path)));
    let mut bytes = std::fs::read(&selected).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 1;
    std::fs::remove_file(&selected).unwrap();
    std::fs::write(&selected, bytes).unwrap();
    let error = SearchIndex::open(&path).err().unwrap();
    assert_search_projection_mismatch(&error, &path);
}

#[test]
fn reopen_sweeps_only_resident_checkpoint_scratch() {
    let root = Dir::new("scratch");
    let path = root.0.join("search");
    base(&path);
    let expected = reopened_state(&path);
    let snapshot = std::fs::read(path.join(SEARCH_SNAPSHOT_FILE)).unwrap();
    // A crash while installing an alias or writing a generation file.
    let scratch = [
        "search_projection.tmp.4242.7",
        "search_projection_segment_payloads.tmp.1.0",
        "search_projection_segments.tmp.1.1",
        "search_lexical.manifest.tmp.1.2",
        "search_projection.77.hawdb.zstd.tmp",
        "search_projection.77.hawdb.tmp",
        "search_projection_segments.77.hawdb.tmp",
        "search_lexical.manifest.77.hawdb.tmp",
    ];
    // Names this sweep does not own, including current and selected files.
    let mut kept = vec![
        "search_projection.tmp.x.7".to_owned(),
        "search_projection.out_of_core.manifest.tmp.3".to_owned(),
        "search_lexical.77.hawdb.tmp".to_owned(),
        "search_projection_out_of_core_layout.77.tmp.1.2".to_owned(),
        "unrelated.tmp.1.2".to_owned(),
    ];
    for name in scratch
        .iter()
        .copied()
        .chain(kept.iter().map(String::as_str))
    {
        std::fs::write(path.join(name), &snapshot).unwrap();
    }
    for entry in std::fs::read_dir(&path).unwrap() {
        let name = entry.unwrap().file_name().into_string().unwrap();
        if !scratch.contains(&name.as_str()) && !kept.contains(&name) {
            kept.push(name);
        }
    }
    let current = |name: &str| std::fs::read(path.join(name)).unwrap();
    let before = kept
        .iter()
        .map(|name| (name.clone(), current(name)))
        .collect::<Vec<_>>();
    assert_eq!(reopened_state(&path), expected);
    for name in scratch {
        assert!(!path.join(name).exists(), "{name} survived reopen");
    }
    for (name, bytes) in before {
        assert_eq!(current(&name), bytes, "{name} changed");
    }
}
