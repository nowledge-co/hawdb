// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn resumed_borrowed_pages_match_full_order_at_gaps_and_page_boundaries() {
    let values = (0..1536)
        .map(|id| (NodeId(id * 2), NodeId(id)))
        .collect::<BTreeMap<_, _>>();
    let map = CowSegmentedMap::from(values);
    assert!(map.segment_count() > 1);
    for after in [
        None,
        Some(0),
        Some(1),
        Some(1022),
        Some(1023),
        Some(1024),
        Some(2046),
        Some(3070),
        Some(u64::MAX),
    ] {
        let expected = map
            .iter()
            .filter(|(key, _)| after.is_none_or(|after| key.0 > after))
            .collect::<Vec<_>>();
        let actual = map.iter_after(after.map(NodeId)).collect::<Vec<_>>();
        assert_eq!(actual, expected);
        for (key, value) in actual {
            assert!(std::ptr::eq(value, map.get(key).unwrap()));
        }
    }
    assert!(CowSegmentedMap::<NodeId, NodeId>::default()
        .iter_after(None)
        .next()
        .is_none());
}

#[test]
fn resumed_scan_survives_new_publication_without_cloning_its_values() {
    let mut live = CowSegmentedMap::from(
        (0..1536)
            .map(|id| (NodeId(id), NodeId(id)))
            .collect::<BTreeMap<_, _>>(),
    );
    let snapshot = live.clone();
    let mut last = None;
    let mut observed = Vec::new();
    while let Some((key, value)) = snapshot.iter_after(last).next() {
        last = Some(*key);
        observed.push(*value);
        live.remove(key);
        live.insert(NodeId(key.0 + 1536), NodeId(u64::MAX));
        assert!(std::ptr::eq(value, snapshot.get(key).unwrap()));
    }
    assert_eq!(observed, (0..1536).map(NodeId).collect::<Vec<_>>());
    assert_eq!(live.iter_after(None).next().unwrap().0, &NodeId(1536));
}
