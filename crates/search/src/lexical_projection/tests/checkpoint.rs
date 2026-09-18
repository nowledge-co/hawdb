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

use super::*;
use crate::lexical_snapshot_test_gate::query_gate;
use crate::{SearchIndex, SearchMode, SearchQueryOptions};

fn search_options() -> SearchQueryOptions {
    SearchQueryOptions {
        limit: 10,
        offset: 0,
        rank_window: Some(10),
        fusion_weights: crate::SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    }
}

fn checkpoint_query_snapshot(query: &str) {
    let root = super::projection_root(query);
    let mut index = SearchIndex::open(&root).unwrap();
    index.upsert(super::document("a", "orchard", "")).unwrap();
    index
        .upsert(super::document("b", "cobalt cobalt", ""))
        .unwrap();
    index.checkpoint().unwrap();
    index.upsert(super::document("a", "cobalt", "")).unwrap();
    let search = || index.try_search_with_options(query, None, SearchMode::Text, search_options());
    let expected = search().unwrap();
    assert!(expected.retrievers.iter().any(|retriever| {
        retriever.name == "text" && retriever.segmented_lexical_projection_used
    }));
    let expected_ids: &[&str] = if query == "cobalt" { &["a", "b"] } else { &[] };
    assert_eq!(
        expected
            .hits
            .iter()
            .map(|hit| hit.id.as_str())
            .collect::<Vec<_>>(),
        expected_ids,
    );
    let old_reader = index.lexical_projection.lock().unwrap().clone().unwrap();
    let (gate, controller) = query_gate();
    let actual = std::thread::scope(|scope| {
        // Own the controller inside the scope so unwind releases the query
        // before the scope joins its worker.
        let controller = controller;
        let query = scope.spawn(|| gate.run(search));
        controller.wait_until_captured();
        index.checkpoint().unwrap();
        {
            let reader = index.lexical_projection.lock().unwrap();
            let delta = index.lexical_delta.lock().unwrap();
            assert_ne!(
                reader.as_ref().unwrap().generation(),
                old_reader.generation()
            );
            assert!(delta.upserts.is_empty());
            assert!(delta.deletes.is_empty());
        }
        drop(controller);
        query.join().unwrap()
    });
    let actual = actual.unwrap();
    assert_eq!(actual.total_hits, expected.total_hits);
    assert_eq!(actual.hits, expected.hits);
    assert_eq!(search().unwrap().hits, expected.hits);
    drop(old_reader);
    drop(index);
    let reopened = SearchIndex::open(&root).unwrap();
    let reopened_result = reopened
        .try_search_with_options(query, None, SearchMode::Text, search_options())
        .unwrap();
    assert_eq!(reopened_result.hits, expected.hits);
    drop(reopened);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn checkpoint_query_snapshot_keeps_replacement_hits() {
    checkpoint_query_snapshot("cobalt");
}

#[test]
fn checkpoint_query_snapshot_excludes_replaced_terms() {
    checkpoint_query_snapshot("orchard");
}
