// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn canonical_decoding_refuses_instead_of_returning_only_the_live_overlay() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "hawdb-retained-source-{}-{nonce}",
        std::process::id()
    ));
    let config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        config,
    )
    .unwrap();
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("score".into(), Value::Int(1))]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("score".into(), Value::Int(2))]),
        )
        .unwrap();
    assert!(store.is_out_of_core());
    let mut unexpected = |_| -> Result<Box<dyn GraphReadAllocation>> {
        panic!("unsupported point storage must not invoke admission");
    };
    let admission = GraphReadAdmission::new(&mut unexpected);
    assert!(matches!(
        store
            .materialized_node_point_read(NodeId(u64::MAX), &admission)
            .unwrap(),
        MaterializedNodePointRead::Unsupported
    ));

    assert!(store.try_materialized_node_read_source().unwrap().is_none());
    assert!(store
        .try_scan_materialized_nodes_after(catalog.label_id("Item"), None)
        .unwrap()
        .is_none());
    let mut count = 0;
    store
        .visit_nodes_owned(catalog.label_id("Item"), |_| {
            count += 1;
            GraphScanControl::Continue
        })
        .unwrap();
    assert_eq!(count, 2);
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn canonical_projected_point_admission_preserves_refusal_and_allows_retry() {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "hawdb-projected-point-admission-{}-{nonce}",
        std::process::id()
    ));
    let config = WalReplayConfig {
        residency_mode: StorageResidencyMode::OutOfCore,
        ..WalReplayConfig::default()
    };
    let mut catalog = Catalog::default();
    let mut store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        config,
    )
    .unwrap();
    let id = store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([
                ("score".into(), Value::Int(9)),
                ("content".into(), Value::String("x".repeat(128 * 1024))),
            ]),
        )
        .unwrap();
    store.checkpoint(&catalog).unwrap();
    drop(store);
    let store = GraphStore::open_with_durability_and_replay_config(
        &path,
        &mut catalog,
        DurabilityPolicy::default(),
        config,
    )
    .unwrap();
    assert!(store.is_out_of_core());
    assert!(!store.nodes.contains_key(&id));
    assert_eq!(
        store.storage_residency_report().segment_cache_pinned_bytes,
        0
    );
    let properties = BTreeSet::from(["score".into()]);
    let mut refused_bytes = None;
    let error = store
        .projected_node_owned_admitted(id, &properties, &mut |bytes| {
            assert!(refused_bytes.replace(bytes).is_none());
            assert!(store.storage_residency_report().segment_cache_pinned_bytes > 0);
            Err(HawDBError::Execution("admission sentinel".into()))
        })
        .unwrap_err();
    assert!(matches!(error, HawDBError::Execution(reason) if reason == "admission sentinel"));
    assert!(refused_bytes.unwrap() < 4096);
    assert_eq!(
        store.storage_residency_report().segment_cache_pinned_bytes,
        0
    );
    let mut retry_calls = 0;
    let node = store
        .projected_node_owned_admitted(id, &properties, &mut |bytes| {
            retry_calls += 1;
            assert_eq!(Some(bytes), refused_bytes);
            Ok(())
        })
        .unwrap()
        .unwrap();
    assert_eq!(retry_calls, 1);
    assert_eq!(
        store.storage_residency_report().segment_cache_pinned_bytes,
        0
    );
    assert_eq!(node.id, id);
    assert_eq!(
        node.properties,
        BTreeMap::from([("score".into(), Value::Int(9))])
    );
    assert!(store
        .projected_node_owned_admitted(NodeId(u64::MAX), &properties, &mut |_| {
            panic!("missing nodes must not invoke admission")
        })
        .unwrap()
        .is_none());
    drop(store);
    std::fs::remove_dir_all(path).unwrap();
}

#[test]
fn demanded_scan_resumes_borrowed_label_order_across_publication() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    for id in 0..1025 {
        store
            .create_node(
                &mut catalog,
                if id % 3 == 0 { "Other" } else { "Item" },
                BTreeMap::from([("score".into(), Value::Int(id))]),
            )
            .unwrap();
    }
    let label = catalog.label_id("Item").unwrap();
    let snapshot = store.snapshot_for_read();
    let expected = snapshot
        .scan_nodes(Some(label))
        .map(|node| node.id)
        .collect::<Vec<_>>();
    let mut last = None;
    let mut actual = Vec::new();
    loop {
        let mut batch_len = 0;
        for node in snapshot
            .try_scan_materialized_nodes_after(Some(label), last)
            .unwrap()
            .unwrap()
            .take(17)
        {
            assert!(std::ptr::eq(node, snapshot.nodes.get(&node.id).unwrap()));
            last = Some(node.id);
            actual.push(node.id);
            batch_len += 1;
        }
        if batch_len == 0 {
            break;
        }
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([("score".into(), Value::Int(-1))]),
            )
            .unwrap();
    }
    assert_eq!(actual, expected);
    assert!(store.scan_nodes(Some(label)).count() > expected.len());
    assert!(snapshot
        .try_scan_materialized_nodes_after(None, Some(NodeId(u64::MAX)))
        .unwrap()
        .unwrap()
        .next()
        .is_none());
}

#[test]
fn minimal_source_shares_records_and_survives_store_mutation_and_destruction() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    for score in 0..4 {
        store
            .create_node(
                &mut catalog,
                "Item",
                BTreeMap::from([("score".into(), Value::Int(score))]),
            )
            .unwrap();
    }
    let source = store.try_materialized_node_read_source().unwrap().unwrap();
    assert_eq!(source.row_count(), 4);
    assert_eq!(source.page_count(), store.nodes.segment_count());
    assert_eq!(
        source.directory_capacity_bytes(),
        store.nodes.directory_capacity_bytes()
    );
    assert!(source.nodes.shares_storage_with(&store.nodes));
    let original = source.iter_after(None).unwrap().next().unwrap();
    assert!(std::ptr::eq(
        original,
        store.nodes.get(&original.id).unwrap()
    ));
    let first_id = original.id;
    store
        .nodes
        .get_mut(&first_id)
        .unwrap()
        .properties
        .insert("score".into(), Value::Int(99));
    assert_eq!(original.properties["score"], Value::Int(0));
    assert_eq!(
        store.nodes.get(&first_id).unwrap().properties["score"],
        Value::Int(99)
    );
    drop(store);
    let scores = source
        .iter_after(Some(first_id))
        .unwrap()
        .map(|node| node.properties["score"].clone())
        .collect::<Vec<_>>();
    assert_eq!(scores, vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
}

#[test]
fn minimal_source_observes_both_shared_poison_signals() {
    for integrity in [false, true] {
        let store = GraphStore::default();
        let source = store.try_materialized_node_read_source().unwrap().unwrap();
        assert!(source.iter_after(None).unwrap().next().is_none());
        if integrity {
            store
                .integrity_poisoned
                .store(true, AtomicOrdering::Release);
        } else {
            store
                .post_wal_apply_poisoned
                .store(true, AtomicOrdering::Release);
        }
        let Err(error) = source.iter_after(None) else {
            panic!("poisoned source must fail before yielding records");
        };
        assert_eq!(
            error.to_string(),
            store.ensure_usable().unwrap_err().to_string()
        );
    }
}

struct PointReadAllocation {
    bytes: usize,
    retained: std::rc::Rc<std::cell::Cell<bool>>,
    released: std::rc::Rc<std::cell::Cell<bool>>,
}

impl GraphReadAllocation for PointReadAllocation {
    fn bytes(&self) -> usize {
        self.bytes
    }

    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.bytes += bytes;
        Ok(())
    }

    fn retain_state(&mut self) -> Result<()> {
        self.retained.set(true);
        Ok(())
    }
}

impl Drop for PointReadAllocation {
    fn drop(&mut self) {
        self.released.set(true);
    }
}

#[test]
fn admitted_point_source_preserves_addresses_and_reservation_until_release() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let mut text = String::with_capacity(8192);
    text.push_str("original");
    let id = store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("text".into(), Value::String(text))]),
        )
        .unwrap();
    let node_address = std::ptr::from_ref(store.nodes.get(&id).unwrap());
    let retained = std::rc::Rc::new(std::cell::Cell::new(false));
    let released = std::rc::Rc::new(std::cell::Cell::new(false));
    let mut allocate = |bytes| -> Result<Box<dyn GraphReadAllocation>> {
        assert!(bytes >= 8192);
        Ok(Box::new(PointReadAllocation {
            bytes,
            retained: retained.clone(),
            released: released.clone(),
        }))
    };
    let admission = GraphReadAdmission::new(&mut allocate);
    let MaterializedNodePointRead::Node(source) =
        store.materialized_node_point_read(id, &admission).unwrap()
    else {
        panic!("resident node must have an admitted source");
    };
    assert!(retained.get());
    assert!(!released.get());
    assert_eq!(std::ptr::from_ref(source.node().unwrap()), node_address);
    let Value::String(original) = &source.node().unwrap().properties["text"] else {
        panic!("fixture must retain a string");
    };
    let text_address = original.as_ptr();
    store.nodes.get_mut(&id).unwrap().properties.clear();
    drop(store);
    let Value::String(text) = &source.node().unwrap().properties["text"] else {
        panic!("source must survive store mutation and destruction");
    };
    assert_eq!(text, "original");
    assert_eq!(text.as_ptr(), text_address);
    assert!(!released.get());
    drop(source);
    assert!(released.get());
}

#[test]
fn point_source_rejects_short_permits_before_pin_and_missing_skips_admission() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(&mut catalog, "Item", BTreeMap::new())
        .unwrap();
    let (page, _) = store.nodes.retained_point_row_preflight(id).unwrap();
    let owners = page.owner_count();
    let mut short =
        |_| -> Result<Box<dyn GraphReadAllocation>> { Ok(Box::new(UnaccountedReadAllocation(0))) };
    let admission = GraphReadAdmission::new(&mut short);
    assert!(matches!(
        store.materialized_node_point_read(id, &admission),
        Err(HawDBError::Execution(reason)) if reason.contains("insufficient allocation permit")
    ));
    assert_eq!(page.owner_count(), owners);
    let mut refuse = |_| -> Result<Box<dyn GraphReadAllocation>> {
        Err(HawDBError::Execution("point admission sentinel".into()))
    };
    let admission = GraphReadAdmission::new(&mut refuse);
    assert!(matches!(
        store.materialized_node_point_read(id, &admission),
        Err(HawDBError::Execution(reason)) if reason == "point admission sentinel"
    ));
    assert_eq!(page.owner_count(), owners);
    assert!(matches!(
        store
            .materialized_node_point_read(NodeId(u64::MAX), &admission)
            .unwrap(),
        MaterializedNodePointRead::Missing
    ));
    store.ensure_usable().unwrap();
}

#[test]
fn admitted_point_source_observes_later_poison() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(&mut catalog, "Item", BTreeMap::new())
        .unwrap();
    let mut allocate = |bytes| -> Result<Box<dyn GraphReadAllocation>> {
        Ok(Box::new(UnaccountedReadAllocation(bytes)))
    };
    let admission = GraphReadAdmission::new(&mut allocate);
    let MaterializedNodePointRead::Node(source) =
        store.materialized_node_point_read(id, &admission).unwrap()
    else {
        panic!("resident point must be supported");
    };
    store
        .post_wal_apply_poisoned
        .store(true, AtomicOrdering::Release);
    assert!(source.node().is_err());
}

struct ThreadPointAllocation {
    bytes: usize,
    released: Arc<AtomicBool>,
    retained: Arc<AtomicBool>,
    fail_retention: bool,
    retained_bytes: Option<usize>,
    row_on_release: Option<std::sync::Weak<crate::cow::CowData<NodeRecord>>>,
}

impl GraphReadAllocation for ThreadPointAllocation {
    fn bytes(&self) -> usize {
        self.bytes
    }

    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.bytes += bytes;
        Ok(())
    }

    fn retain_state(&mut self) -> Result<()> {
        self.retained.store(true, AtomicOrdering::Release);
        if self.fail_retention {
            return Err(HawDBError::Execution("typed retention sentinel".into()));
        }
        if let Some(bytes) = self.retained_bytes {
            self.bytes = bytes;
        }
        Ok(())
    }
}

impl Drop for ThreadPointAllocation {
    fn drop(&mut self) {
        if let Some(page) = &self.row_on_release {
            assert_eq!(page.strong_count(), 0, "row must drop before its permit");
        }
        self.released.store(true, AtomicOrdering::Release);
    }
}

#[test]
fn point_source_preserves_host_refusal_before_pin() {
    #[derive(Debug)]
    enum AdmissionError {
        Refused { retryable: bool, identity: Arc<()> },
        Storage(HawDBError),
    }
    impl From<HawDBError> for AdmissionError {
        fn from(error: HawDBError) -> Self {
            Self::Storage(error)
        }
    }
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(&mut catalog, "Item", BTreeMap::new())
        .unwrap();
    let (row, _) = store.nodes.retained_point_row_preflight(id).unwrap();
    let owners = row.owner_count();
    for retryable in [true, false] {
        let identity = Arc::new(());
        let result = store.materialized_node_point_read_with_admission(
            id,
            |_| -> std::result::Result<ThreadPointAllocation, AdmissionError> {
                assert_eq!(row.owner_count(), owners);
                Err(AdmissionError::Refused {
                    retryable,
                    identity: Arc::clone(&identity),
                })
            },
        );
        match result {
            Err(AdmissionError::Refused {
                retryable: observed,
                identity: observed_identity,
            }) => {
                assert_eq!(observed, retryable);
                assert!(Arc::ptr_eq(&observed_identity, &identity));
            }
            Err(AdmissionError::Storage(error)) => panic!("host error was converted: {error}"),
            Ok(_) => panic!("host refusal was ignored"),
        }
        assert_eq!(row.owner_count(), owners);
        store.ensure_usable().unwrap();
    }
    assert!(matches!(
        store
            .materialized_node_point_read_with_admission(
                NodeId(u64::MAX),
                |_| -> std::result::Result<ThreadPointAllocation, AdmissionError> {
                    panic!("missing point must not invoke host admission");
                },
            )
            .unwrap(),
        MaterializedNodePointRead::Missing
    ));
}

#[test]
fn typed_point_source_preserves_auto_traits_and_cross_thread_release() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<MaterializedNodePointSource<ThreadPointAllocation>>();
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(
            &mut catalog,
            "Item",
            BTreeMap::from([("text".into(), Value::String("original".into()))]),
        )
        .unwrap();
    let address = std::ptr::from_ref(store.nodes.get(&id).unwrap()) as usize;
    let page = store
        .nodes
        .retained_point_row_preflight(id)
        .unwrap()
        .0
        .downgrade();
    let released = Arc::new(AtomicBool::new(false));
    let retained = Arc::new(AtomicBool::new(false));
    let MaterializedNodePointRead::Node(source) = store
        .materialized_node_point_read_typed(id, |bytes| {
            Ok(ThreadPointAllocation {
                bytes,
                released: Arc::clone(&released),
                retained: Arc::clone(&retained),
                fail_retention: false,
                retained_bytes: None,
                row_on_release: Some(page.clone()),
            })
        })
        .unwrap()
    else {
        panic!("resident typed point must be supported");
    };
    assert!(retained.load(AtomicOrdering::Acquire));
    store.nodes.get_mut(&id).unwrap().properties.clear();
    drop(store);
    let observed_release = Arc::clone(&released);
    std::thread::spawn(move || {
        let node = source.node().unwrap();
        assert_eq!(std::ptr::from_ref(node) as usize, address);
        assert!(matches!(&node.properties["text"], Value::String(text) if text == "original"));
        assert!(!observed_release.load(AtomicOrdering::Acquire));
        drop(source);
        assert!(observed_release.load(AtomicOrdering::Acquire));
    })
    .join()
    .unwrap();
    assert!(released.load(AtomicOrdering::Acquire));
}

#[test]
fn typed_point_admission_validates_before_and_after_retained_transfer() {
    let mut store = GraphStore::default();
    let mut catalog = Catalog::default();
    let id = store
        .create_node(&mut catalog, "Item", BTreeMap::new())
        .unwrap();
    let (page, _) = store.nodes.retained_point_row_preflight(id).unwrap();
    let owners = page.owner_count();
    for (short, fail, shrink, expected) in [
        (true, false, false, "insufficient allocation permit"),
        (false, true, false, "typed retention sentinel"),
        (false, false, true, "lost its allocation permit"),
    ] {
        let released = Arc::new(AtomicBool::new(false));
        let retained = Arc::new(AtomicBool::new(false));
        let result = store.materialized_node_point_read_typed(id, |bytes| {
            assert_eq!(page.owner_count(), owners);
            Ok(ThreadPointAllocation {
                bytes: if short { 0 } else { bytes },
                released: Arc::clone(&released),
                retained: Arc::clone(&retained),
                fail_retention: fail,
                retained_bytes: shrink.then_some(0),
                row_on_release: None,
            })
        });
        assert!(matches!(result, Err(HawDBError::Execution(reason)) if reason.contains(expected)));
        assert!(released.load(AtomicOrdering::Acquire));
        assert_eq!(retained.load(AtomicOrdering::Acquire), !short);
        assert_eq!(page.owner_count(), owners);
        store.ensure_usable().unwrap();
    }
}

#[test]
fn completion_health_guard_preserves_both_poison_signals_after_store_drop() {
    for integrity in [false, true] {
        let store = GraphStore::in_memory();
        let health = store.materialized_read_health().unwrap();
        health.validate_usable().unwrap();
        if integrity {
            store
                .integrity_poisoned
                .store(true, AtomicOrdering::Release);
        } else {
            store
                .post_wal_apply_poisoned
                .store(true, AtomicOrdering::Release);
        }
        drop(store);
        assert!(health.validate_usable().is_err());
    }
}
