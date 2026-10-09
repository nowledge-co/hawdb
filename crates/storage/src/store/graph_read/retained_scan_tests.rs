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
