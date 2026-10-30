// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Sorted graph-variable slots. Row clones share immutable records and names;
//! expanding a row copies only the small slot array. Mutating a record detaches
//! that record, preserving the owned Binding contract used by spill and writes.

use std::collections::BTreeMap;
use std::ops::Index;
use std::sync::Arc;

type Slot<T> = (Arc<String>, Arc<T>);

#[derive(Debug, Clone)]
pub struct GraphSlots<T> {
    slots: Option<Arc<Vec<Slot<T>>>>,
}

impl<T: PartialEq> PartialEq for GraphSlots<T> {
    fn eq(&self, other: &Self) -> bool {
        self.entries() == other.entries()
    }
}
impl<T: Eq> Eq for GraphSlots<T> {}

impl<T> Default for GraphSlots<T> {
    fn default() -> Self {
        Self { slots: None }
    }
}

impl<T> GraphSlots<T> {
    pub fn new() -> Self {
        Self::default()
    }

    fn entries(&self) -> &[Slot<T>] {
        self.slots.as_deref().map(Vec::as_slice).unwrap_or(&[])
    }

    fn position(&self, name: &str) -> Result<usize, usize> {
        self.entries()
            .binary_search_by(|(key, _)| key.as_str().cmp(name))
    }

    pub fn len(&self) -> usize {
        self.entries().len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries().is_empty()
    }
    pub fn contains_key(&self, name: &str) -> bool {
        self.position(name).is_ok()
    }
    pub fn get(&self, name: &str) -> Option<&T> {
        self.position(name)
            .ok()
            .map(|i| self.entries()[i].1.as_ref())
    }
    pub fn iter(&self) -> GraphSlotIter<'_, T> {
        GraphSlotIter(self.entries().iter())
    }
    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.iter().map(|(name, _)| name)
    }
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().map(|(_, value)| value)
    }
    pub fn clear(&mut self) {
        self.slots = None;
    }
}

impl<T: Clone> GraphSlots<T> {
    fn entries_mut(&mut self) -> &mut Vec<Slot<T>> {
        Arc::make_mut(self.slots.get_or_insert_with(|| Arc::new(Vec::new())))
    }
    pub fn insert(&mut self, name: String, value: T) -> Option<T> {
        let position = self.position(&name);
        let slots = self.entries_mut();
        match position {
            Ok(i) => Some(Arc::unwrap_or_clone(std::mem::replace(
                &mut slots[i].1,
                Arc::new(value),
            ))),
            Err(i) => {
                slots.insert(i, (Arc::new(name), Arc::new(value)));
                None
            }
        }
    }
    pub fn remove(&mut self, name: &str) -> Option<T> {
        let i = self.position(name).ok()?;
        let slots = self.entries_mut();
        let value = slots.remove(i).1;
        if slots.capacity() > slots.len().saturating_mul(2).max(4) {
            slots.shrink_to_fit();
        }
        if slots.is_empty() {
            self.clear();
        }
        Some(Arc::unwrap_or_clone(value))
    }
    pub fn get_mut(&mut self, name: &str) -> Option<&mut T> {
        let i = self.position(name).ok()?;
        Some(Arc::make_mut(&mut self.entries_mut()[i].1))
    }
    /// Join graph slots without converting shared records back to owned values.
    pub(crate) fn extend_shared(&mut self, other: &Self) {
        for (name, value) in other.entries() {
            let position = self.position(name.as_str());
            let slots = self.entries_mut();
            match position {
                Ok(i) => slots[i] = (Arc::clone(name), Arc::clone(value)),
                Err(i) => slots.insert(i, (Arc::clone(name), Arc::clone(value))),
            }
        }
    }
    /// Remove a replaced variable without copying its record payload.
    pub(crate) fn without(&self, name: Option<&str>) -> Self {
        let mut slots = self.clone();
        if let Some(i) = name.and_then(|name| slots.position(name).ok()) {
            let entries = slots.entries_mut();
            entries.remove(i);
            if entries.capacity() > entries.len().saturating_mul(2).max(4) {
                entries.shrink_to_fit();
            }
        }
        if slots.is_empty() {
            slots.clear();
        }
        slots
    }
}

pub struct GraphSlotIter<'a, T>(std::slice::Iter<'a, Slot<T>>);
impl<'a, T> Iterator for GraphSlotIter<'a, T> {
    type Item = (&'a String, &'a T);
    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(name, value)| (name.as_ref(), value.as_ref()))
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
impl<T> ExactSizeIterator for GraphSlotIter<'_, T> {}
impl<'a, T> IntoIterator for &'a GraphSlots<T> {
    type Item = (&'a String, &'a T);
    type IntoIter = GraphSlotIter<'a, T>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
impl<T: Clone> IntoIterator for GraphSlots<T> {
    type Item = (String, T);
    type IntoIter = std::vec::IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        self.slots
            .map(Arc::unwrap_or_clone)
            .unwrap_or_default()
            .into_iter()
            .map(|(name, value)| (Arc::unwrap_or_clone(name), Arc::unwrap_or_clone(value)))
            .collect::<Vec<_>>()
            .into_iter()
    }
}
impl<T: Clone> FromIterator<(String, T)> for GraphSlots<T> {
    fn from_iter<I: IntoIterator<Item = (String, T)>>(iter: I) -> Self {
        // BTreeMap keeps last-wins duplicate semantics at the conversion boundary.
        Self::from(iter.into_iter().collect::<BTreeMap<_, _>>())
    }
}
impl<T> From<BTreeMap<String, T>> for GraphSlots<T> {
    fn from(values: BTreeMap<String, T>) -> Self {
        if values.is_empty() {
            return Self::new();
        }
        Self {
            slots: Some(Arc::new(
                values
                    .into_iter()
                    .map(|(name, value)| (Arc::new(name), Arc::new(value)))
                    .collect(),
            )),
        }
    }
}
impl<T> Index<&str> for GraphSlots<T> {
    type Output = T;
    fn index(&self, name: &str) -> &Self::Output {
        self.get(name).expect("missing graph variable")
    }
}
impl<T> Index<&String> for GraphSlots<T> {
    type Output = T;
    fn index(&self, name: &String) -> &Self::Output {
        &self[name.as_str()]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug)]
    struct Tracked {
        payload: String,
        clones: Arc<AtomicUsize>,
    }
    impl Clone for Tracked {
        fn clone(&self) -> Self {
            self.clones.fetch_add(1, Ordering::Relaxed);
            Self {
                payload: self.payload.clone(),
                clones: self.clones.clone(),
            }
        }
    }

    #[test]
    fn fanout_shares_payload_and_mutation_detaches_only_modified_record() {
        let clones = Arc::new(AtomicUsize::new(0));
        let original = GraphSlots::from(BTreeMap::from([(
            "seed".into(),
            Tracked {
                payload: "x".repeat(65536),
                clones: clones.clone(),
            },
        )]));
        let mut expanded = original.clone();
        for index in 0..512 {
            let mut output = original.without(None);
            output.insert(
                "target".into(),
                Tracked {
                    payload: index.to_string(),
                    clones: clones.clone(),
                },
            );
            assert!(std::ptr::eq(
                original.get("seed").unwrap(),
                output.get("seed").unwrap()
            ));
            expanded = output;
        }
        assert_eq!(clones.load(Ordering::Relaxed), 0);
        expanded.get_mut("seed").unwrap().payload.push('!');
        assert_eq!(clones.load(Ordering::Relaxed), 1);
        assert_eq!(original.get("seed").unwrap().payload.len(), 65536);
        assert_eq!(expanded.get("seed").unwrap().payload.len(), 65537);
        let replaced = original.without(Some("seed"));
        assert!(replaced.is_empty());
        assert!(replaced.slots.is_none());
        assert_eq!(clones.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn order_duplicate_replacement_removal_and_shared_join_match_map_contract() {
        let mut values: GraphSlots<i32> = [("z".into(), 1), ("a".into(), 2), ("z".into(), 3)]
            .into_iter()
            .collect();
        assert_eq!(
            values
                .iter()
                .map(|(key, value)| (key.as_str(), *value))
                .collect::<Vec<_>>(),
            [("a", 2), ("z", 3)]
        );
        let snapshot = values.clone();
        assert_eq!(values.insert("z".into(), 4), Some(3));
        assert_eq!(values.remove("a"), Some(2));
        assert_eq!(snapshot["a"], 2);
        let mut emptied = GraphSlots::from(BTreeMap::from([("single".into(), 1)]));
        emptied.remove("single");
        assert_eq!(emptied, GraphSlots::new());
        assert!(emptied.slots.is_none());
        values.extend_shared(&snapshot);
        assert_eq!(values, snapshot);
        assert_eq!(
            values.into_iter().collect::<BTreeMap<_, _>>(),
            BTreeMap::from([("a".into(), 2), ("z".into(), 3)])
        );
    }
}
