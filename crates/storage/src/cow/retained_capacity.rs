// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

/// Bound the pinned Rust 1.97.1 B-tree allocations, including sparse internal
/// nodes and an allocated empty root. Every non-root node has at least five
/// pairs; the root can have one. A tree with eleven pairs can therefore have
/// three nodes, rather than one full leaf.
pub(super) fn tree_capacity<K, V>(len: usize) -> Option<usize> {
    let nodes = 1usize.checked_add(len.saturating_sub(1) / 5)?;
    let node_bytes = std::mem::size_of::<(K, V)>()
        .checked_mul(11)?
        .checked_add(12usize.checked_mul(std::mem::size_of::<usize>())?)?
        .checked_add(32)?;
    nodes.checked_mul(node_bytes)
}

pub(super) fn properties_capacity(values: &BTreeMap<String, Value>, depth: usize) -> Option<usize> {
    values.iter().try_fold(
        tree_capacity::<String, Value>(values.len())?,
        |bytes, (name, value)| {
            bytes
                .checked_add(name.capacity())?
                .checked_add(value_capacity(value, depth)?)
        },
    )
}

fn value_capacity(value: &Value, depth: usize) -> Option<usize> {
    // Bound preflight stack space independently of untrusted recursive input.
    if depth >= 64 {
        return None;
    }
    match value {
        Value::String(value) => Some(value.capacity()),
        Value::Binary(value) => Some(value.capacity()),
        Value::List(values) => values.iter().try_fold(
            values
                .capacity()
                .checked_mul(std::mem::size_of::<Value>())?,
            |bytes, value| bytes.checked_add(value_capacity(value, depth + 1)?),
        ),
        Value::Map(values) => properties_capacity(values, depth + 1),
        Value::Null | Value::Bool(_) | Value::Int(_) | Value::Float(_) | Value::Uuid(_) => Some(0),
    }
}

fn node_page_capacity(
    page: &BTreeMap<NodeId, NodeRecord>,
    bytes: usize,
    inspected: &mut usize,
) -> Option<usize> {
    let bytes = bytes
        .checked_add(2usize.checked_mul(std::mem::size_of::<usize>())?)?
        .checked_add(std::mem::size_of::<CowData<BTreeMap<NodeId, NodeRecord>>>())?
        .checked_add(tree_capacity::<NodeId, NodeRecord>(page.len())?)?;
    page.values().try_fold(bytes, |bytes, node| {
        *inspected = inspected.saturating_add(1);
        bytes
            .checked_add(tree_capacity::<LabelId, ()>(node.labels.len())?)?
            .checked_add(properties_capacity(&node.properties, 0)?)
    })
}

impl CowSegmentedMap<NodeId, NodeRecord> {
    /// Borrowed preflight of all allocations kept alive by a node snapshot.
    /// Unlike page-splitting weights, this uses owned buffer capacities, not
    /// logical lengths. It allocates/clones no source values. None refuses
    /// overflow or excessive nesting rather than returning a partial bound.
    pub fn retained_capacity_bytes(&self) -> Option<usize> {
        self.retained_capacity_preflight().0
    }

    /// Return the capacity and this caller's actual cold-preflight row count.
    /// Concurrent readers share initialization; a cache hit inspects no records.
    pub fn retained_capacity_preflight(&self) -> (Option<usize>, usize) {
        let mut inspected = 0usize;
        let capacity = *self.segments.retained_capacity.get_or_init(|| {
            self.segments
                .iter()
                .try_fold(self.directory_capacity_bytes(), |bytes, page| {
                    node_page_capacity(page, bytes, &mut inspected)
                })
        });
        (capacity, inspected)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retained_capacity_includes_spare_and_nested_buffers_without_copying() {
        let mut text = String::with_capacity(16 * 1024);
        text.push('x');
        let mut binary = Vec::with_capacity(32 * 1024);
        binary.push(7);
        let mut values = Vec::with_capacity(128);
        values.push(Value::Map(BTreeMap::from([
            ("text".into(), Value::String(text)),
            ("binary".into(), Value::Binary(binary)),
        ])));
        let node = NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::from([LabelId(1)]),
            properties: BTreeMap::from([("unrequested".into(), Value::List(values))]),
        };
        let map = CowSegmentedMap::from(BTreeMap::from([(node.id, node)]));
        let original = map.get(&NodeId(1)).unwrap();
        let bound = map.retained_capacity_bytes().unwrap();
        assert!(bound >= 48 * 1024 + 128 * std::mem::size_of::<Value>());
        assert!(std::ptr::eq(original, map.get(&NodeId(1)).unwrap()));
        let snapshot = map.clone();
        assert_eq!(snapshot.retained_capacity_bytes(), Some(bound));
        assert!(std::ptr::eq(original, snapshot.get(&NodeId(1)).unwrap()));
    }

    #[test]
    fn source_bound_survives_mutation_and_keeps_each_generation_independent() {
        let node = NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([("pad".into(), Value::String("x".repeat(64 * 1024)))]),
        };
        let mut map = CowSegmentedMap::from(BTreeMap::from([(node.id, node)]));
        let snapshot = map.clone();
        let bound = snapshot.retained_capacity_bytes().unwrap();
        map.get_mut(&NodeId(1)).unwrap().properties.clear();
        assert!(map.retained_capacity_bytes().unwrap() < bound);
        assert_eq!(snapshot.retained_capacity_bytes(), Some(bound));
    }

    #[test]
    fn independent_point_page_preserves_values_without_retaining_other_pages() {
        let mut map = CowSegmentedMap::from(
            (0..1025)
                .map(|index| {
                    let id = NodeId(index * 3 + 1);
                    let mut text = String::with_capacity(1024);
                    text.push_str("original");
                    (
                        id,
                        NodeRecord {
                            id,
                            labels: BTreeSet::from([LabelId(1)]),
                            properties: BTreeMap::from([("text".into(), Value::String(text))]),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        );
        let id = NodeId(1);
        let index = map.segment_index(&id).unwrap();
        assert!(map.segment_count() > 1);
        let page_refs = map
            .segments
            .iter()
            .map(CowSegment::downgrade)
            .collect::<Vec<_>>();
        let directory_ref = map.segments.downgrade();
        let original = map.get(&id).unwrap();
        let original_address = std::ptr::from_ref(original);
        let Value::String(original_text) = &original.properties["text"] else {
            panic!("fixture text must be a string");
        };
        let text_address = original_text.as_ptr();
        let full_capacity = map.retained_capacity_bytes().unwrap();

        // A separate one-page owner exercises the existing COW implementation,
        // without exposing a production lease or changing admission semantics.
        let page = map.segments[index].clone();
        let page_rows = page.len();
        let source = CowSegmentedMap {
            delta_pressure_bytes: page
                .values()
                .map(|node| u128::from(node.delta_pressure_bytes()))
                .sum(),
            segments: CowSegment::from(CowPageDirectory::new(vec![page])),
            len: page_rows,
        };
        let page_capacity = source.retained_capacity_bytes().unwrap();
        assert!(page_capacity < full_capacity);
        assert!(page_capacity >= page_rows * 1024);
        assert_eq!(
            std::ptr::from_ref(source.get(&id).unwrap()),
            original_address
        );

        map.get_mut(&id)
            .unwrap()
            .properties
            .insert("text".into(), Value::String("updated".into()));
        assert_ne!(std::ptr::from_ref(map.get(&id).unwrap()), original_address);
        assert_eq!(source.retained_capacity_bytes(), Some(page_capacity));
        assert!(map.remove(&id).is_some());
        assert!(map.get(&id).is_none());
        drop(map);

        assert!(directory_ref.upgrade().is_none());
        for (page_index, reference) in page_refs.iter().enumerate() {
            assert_eq!(reference.upgrade().is_some(), page_index == index);
        }
        let Value::String(text) = &source.get(&id).unwrap().properties["text"] else {
            panic!("retained text must remain a string");
        };
        assert_eq!(text, "original");
        assert_eq!(text.as_ptr(), text_address);
        assert_eq!(source.retained_capacity_bytes(), Some(page_capacity));
        drop(source);
        assert!(page_refs[index].upgrade().is_none());
    }

    #[test]
    fn sparse_root_and_recursive_overflow_fail_closed() {
        assert!(
            tree_capacity::<String, Value>(11).unwrap()
                >= 3 * 11 * std::mem::size_of::<(String, Value)>()
        );
        assert!(tree_capacity::<String, Value>(0).unwrap() > 0);
        assert!(tree_capacity::<String, Value>(usize::MAX).is_none());
        let mut value = Value::Null;
        for _ in 0..64 {
            value = Value::List(vec![value]);
        }
        assert!(value_capacity(&value, 0).is_none());
    }

    #[test]
    fn snapshots_created_before_preflight_share_the_source_bound() {
        let node = NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([("pad".into(), Value::String("x".repeat(1024)))]),
        };
        let mut map = CowSegmentedMap::from(BTreeMap::from([(node.id, node)]));
        let first = map.clone();
        let second = map.clone();
        let (bound, inspected) = first.retained_capacity_preflight();
        assert_eq!(inspected, 1);
        assert_eq!(second.retained_capacity_preflight(), (bound, 0));
        assert_eq!(map.retained_capacity_preflight(), (bound, 0));
        map.get_mut(&NodeId(1))
            .unwrap()
            .properties
            .insert("pad".into(), Value::String("y".repeat(16 * 1024)));
        let (new_bound, inspected) = map.retained_capacity_preflight();
        assert_eq!(inspected, 1);
        assert!(new_bound > bound);
        assert_eq!(first.retained_capacity_preflight(), (bound, 0));
        assert_eq!(second.retained_capacity_preflight(), (bound, 0));
    }

    #[test]
    fn capacity_cache_is_generation_local_and_every_mutation_invalidates_it() {
        let node = NodeRecord {
            id: NodeId(1),
            labels: BTreeSet::new(),
            properties: BTreeMap::from([("pad".into(), Value::String("x".repeat(1024)))]),
        };
        let mut map = CowSegmentedMap::from(BTreeMap::from([(node.id, node.clone())]));
        let (bound, inspected) = map.retained_capacity_preflight();
        assert_eq!(inspected, 1);
        assert_eq!(map.retained_capacity_preflight(), (bound, 0));
        let snapshot = map.clone();
        assert_eq!(snapshot.retained_capacity_preflight(), (bound, 0));
        map.get_mut(&NodeId(1))
            .unwrap()
            .properties
            .insert("pad".into(), Value::String("y".repeat(16 * 1024)));
        let (new, inspected) = map.retained_capacity_preflight();
        assert_eq!(inspected, 1);
        assert!(new > bound);
        assert_eq!(snapshot.retained_capacity_preflight(), (bound, 0));
        map.rebalance_key(&NodeId(1));
        assert_eq!(map.retained_capacity_preflight().1, 1);
        map.insert(
            NodeId(2),
            NodeRecord {
                id: NodeId(2),
                ..node.clone()
            },
        );
        assert_eq!(map.retained_capacity_preflight().1, 2);
        map.retain(|_, record| {
            record.properties.clear();
            true
        });
        assert_eq!(map.retained_capacity_preflight().1, 2);
        map.remove(&NodeId(2));
        assert_eq!(map.retained_capacity_preflight().1, 1);
        assert_eq!(snapshot.retained_capacity_preflight(), (bound, 0));
        let mut defaults = CowSegmentedMap::<NodeId, Vec<Value>>::default();
        defaults.segments.retained_capacity.set(Some(1)).unwrap();
        defaults.entry_or_default(NodeId(3)).push(Value::Int(1));
        assert!(defaults.segments.retained_capacity.get().is_none());
    }
}
