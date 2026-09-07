//! Real query/publication interleavings for generation-bound delta ownership.

use std::cell::RefCell;

type Hook = Option<Box<dyn FnOnce()>>;
thread_local! {
    static AFTER_PIN: RefCell<Hook> = const { RefCell::new(None) };
    static BEFORE_SWAP: RefCell<Hook> = const { RefCell::new(None) };
}

pub(super) fn after_pin() {
    let hook = AFTER_PIN.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

pub(super) fn before_swap() {
    let hook = BEFORE_SWAP.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

#[cfg(feature = "full-text-search")]
mod cases {
    use super::*;
    use crate::*;
    use std::sync::mpsc;
    use std::time::Duration;

    const DEADLINE: Duration = Duration::from_secs(30);

    fn document(id: &str, content: &str) -> SearchDocument {
        SearchDocument {
            id: id.to_string(),
            title: String::new(),
            content: content.to_string(),
            embedding: None,
            metadata: BTreeMap::new(),
        }
    }

    fn fixture(name: &str) -> (PathBuf, SearchIndex) {
        let root = std::env::temp_dir().join(format!(
            "skein-lexical-state-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        fs::create_dir(&root).unwrap();
        let mut index = SearchIndex::open(&root).unwrap();
        for document in [document("a", "graph graph"), document("b", "graph memory")] {
            index.upsert(document).unwrap();
        }
        index.checkpoint().unwrap();
        index.upsert(document("a", "vector")).unwrap();
        index.delete("b");
        index.upsert(document("c", "graph query")).unwrap();
        (root, index)
    }

    fn scores(index: &SearchIndex) -> Vec<(String, f64)> {
        let result = index
            .try_search_with_options("graph", None, SearchMode::Text, options())
            .unwrap();
        assert!(result
            .retrievers
            .iter()
            .any(|report| report.segmented_lexical_projection_used));
        result
            .hits
            .into_iter()
            .map(|hit| (hit.id, hit.text_score))
            .collect()
    }

    fn options() -> SearchQueryOptions {
        SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: Some(10),
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::new(),
            policy_epoch: None,
        }
    }

    #[test]
    fn pinned_delta_allows_parallel_queries_and_blocks_generation_swap() {
        let (root, index) = fixture("delta-query-generation-pin");
        let expected = scores(&index);
        assert_eq!(expected.len(), 1);
        assert_eq!(expected[0].0, "c");
        let generation = index
            .lexical_state
            .read()
            .unwrap()
            .projection
            .as_ref()
            .unwrap()
            .generation();
        let index = Arc::new(index);
        let (pinned_tx, pinned_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let query_index = Arc::clone(&index);
        let query = std::thread::spawn(move || {
            let hook_index = Arc::clone(&query_index);
            AFTER_PIN.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    assert!(matches!(
                        hook_index.lexical_state.try_write(),
                        Err(std::sync::TryLockError::WouldBlock)
                    ));
                    pinned_tx.send(()).unwrap();
                    release_rx.recv_timeout(DEADLINE).unwrap();
                }))
            });
            scores(&query_index)
        });
        pinned_rx.recv_timeout(DEADLINE).unwrap();

        // A mutex held across scoring would serialize this second real query.
        let second_index = Arc::clone(&index);
        let (second_tx, second_rx) = mpsc::channel();
        let second = std::thread::spawn(move || second_tx.send(scores(&second_index)).unwrap());
        assert_eq!(second_rx.recv_timeout(DEADLINE).unwrap(), expected);
        second.join().unwrap();

        let publisher_index = Arc::clone(&index);
        let publisher_root = root.clone();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (published_tx, published_rx) = mpsc::channel();
        let publisher = std::thread::spawn(move || {
            let hook_index = Arc::clone(&publisher_index);
            BEFORE_SWAP.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    assert!(matches!(
                        hook_index.lexical_state.try_write(),
                        Err(std::sync::TryLockError::WouldBlock)
                    ));
                    ready_tx.send(()).unwrap();
                }))
            });
            publisher_index
                .write_lexical_projection(&publisher_root)
                .unwrap();
            published_tx.send(()).unwrap();
        });
        ready_rx.recv_timeout(DEADLINE).unwrap();
        assert_eq!(
            crate::lexical_projection::manifest_generation(
                &root.join(crate::lexical_projection::MANIFEST_FILE)
            )
            .unwrap(),
            generation + 1
        );
        assert!(matches!(
            published_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));
        release_tx.send(()).unwrap();
        assert_eq!(query.join().unwrap(), expected);
        published_rx.recv_timeout(DEADLINE).unwrap();
        publisher.join().unwrap();
        assert_eq!(
            index
                .lexical_state
                .read()
                .unwrap()
                .projection
                .as_ref()
                .unwrap()
                .generation(),
            generation + 1
        );
        assert_eq!(scores(&index), expected);
        drop(index);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn read_panic_releases_pin_without_poisoning_state() {
        let (root, index) = fixture("delta-query-pin-unwind");
        let expected = scores(&index);
        AFTER_PIN
            .with(|slot| *slot.borrow_mut() = Some(Box::new(|| panic!("injected query stop"))));
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| scores(&index))).is_err());
        assert!(index.lexical_state.try_write().is_ok());
        assert_eq!(scores(&index), expected);
        drop(index);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn delta_admission_failure_invalidates_reader_and_delta_together() {
        for delete in [false, true] {
            let (root, mut index) = fixture(if delete {
                "delta-delete-fallback"
            } else {
                "delta-upsert-fallback"
            });
            index.lexical_config.mini_delta_bytes = std::num::NonZeroU64::MIN;
            if delete {
                index.delete("a");
            } else {
                index.upsert(document("c", "graph graph storage")).unwrap();
            }
            {
                let state = index.lexical_state.read().unwrap();
                assert!(state.projection.is_none());
                assert_eq!(
                    format!("{:?}", state.delta),
                    format!("{:?}", LexicalMiniDelta::default())
                );
            }
            let fallback = index
                .try_search_with_options("graph", None, SearchMode::Text, options())
                .unwrap();
            assert!(!fallback
                .retrievers
                .iter()
                .any(|report| report.segmented_lexical_projection_used));
            assert_eq!(fallback.hits.len(), 1);
            assert_eq!(fallback.hits[0].id, "c");
            index.checkpoint().unwrap();
            assert_eq!(
                scores(&index),
                fallback
                    .hits
                    .into_iter()
                    .map(|hit| (hit.id, hit.text_score))
                    .collect::<Vec<_>>()
            );
            drop(index);
            fs::remove_dir_all(root).unwrap();
        }
    }
}
