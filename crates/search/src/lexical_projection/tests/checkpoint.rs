use super::*;
use crate::{SearchIndex, SearchMode, SearchQueryOptions};
use std::time::{Duration, Instant};

fn wait_until(mut ready: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !ready() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    true
}

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
    let initial_readers = Arc::strong_count(&old_reader);
    let epoch_guard = index.durable_source_graph_commit_epoch.lock().unwrap();
    let (captured, published, actual, checkpoint) = std::thread::scope(|scope| {
        let query = scope.spawn(search);
        // The query retains its reader before waiting for projection freshness.
        let captured = wait_until(|| Arc::strong_count(&old_reader) > initial_readers);
        let checkpoint = scope.spawn(|| index.checkpoint());
        let published = wait_until(|| {
            let Ok(reader) = index.lexical_projection.try_lock() else {
                return false;
            };
            let Ok(delta) = index.lexical_delta.try_lock() else {
                return false;
            };
            reader.as_ref().unwrap().generation() != old_reader.generation()
                && delta.upserts.is_empty()
                && delta.deletes.is_empty()
        });
        // Release both workers even if a synchronization condition timed out.
        drop(epoch_guard);
        (
            captured,
            published,
            query.join().unwrap(),
            checkpoint.join().unwrap(),
        )
    });
    checkpoint.unwrap();
    assert!(captured, "query did not capture the original reader");
    assert!(published, "checkpoint did not publish and reset the delta");
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
