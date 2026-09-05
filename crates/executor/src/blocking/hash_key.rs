use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hash, Hasher};

/// Hash the structural key once, including when a probe is followed by a merge
/// or insertion. Hash collisions never replace full-key equality.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct HashedKey<K> {
    hash: u64,
    pub(super) key: K,
}

impl<K: Hash> HashedKey<K> {
    pub(super) fn new(key: K, state: &RandomState) -> Self {
        Self {
            hash: state.hash_one(&key),
            key,
        }
    }
}

impl<K> Hash for HashedKey<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

/// Conservative per-entry headroom for buckets, growth, and the temporary
/// sorted output/run vector. Variable-sized key/state payloads are charged by
/// the operator separately. Keeping this charge per entry also retains it
/// while the table is consumed into a sorted vector.
pub(super) const fn hash_entry_overhead<K, V>() -> usize {
    std::mem::size_of::<(HashedKey<K>, V)>()
        .saturating_add(1)
        .saturating_mul(4)
        .saturating_add(32)
}

/// Charge capacity rather than a worst-case initial table for every distinct
/// value. Double-capacity headroom covers bucket/control storage and the old
/// table while an insertion grows the allocation.
pub(super) const fn hash_set_capacity_bytes<K>(capacity: usize) -> usize {
    if capacity == 0 {
        0
    } else {
        capacity
            .saturating_mul(2)
            .saturating_mul(std::mem::size_of::<K>().saturating_add(1))
            .saturating_add(32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use skein_core::Value;
    use std::collections::{BTreeMap, HashMap};

    #[test]
    fn colliding_hashes_compare_complete_structural_keys() {
        let keys = [
            vec![Value::Null],
            vec![Value::Int(1)],
            vec![Value::Float(1.0)],
            vec![Value::Float(-0.0)],
            vec![Value::Float(0.0)],
            vec![Value::Float(f64::from_bits(0x7ff8_0000_0000_0001))],
            vec![Value::Float(f64::from_bits(0x7ff8_0000_0000_0002))],
            vec![Value::List(vec![Value::String("nested".to_string())])],
            vec![Value::Map(BTreeMap::from([(
                "key".to_string(),
                Value::Null,
            )]))],
        ];
        let mut groups = HashMap::new();
        for (index, key) in keys.iter().enumerate() {
            groups.insert(
                HashedKey {
                    hash: 0,
                    key: key.clone(),
                },
                index,
            );
        }
        assert_eq!(groups.len(), keys.len());
        for (index, key) in keys.into_iter().enumerate() {
            assert_eq!(groups.get(&HashedKey { hash: 0, key }), Some(&index));
        }
    }

    #[test]
    fn equal_keys_share_a_precomputed_hash() {
        let state = RandomState::new();
        let key = vec![Value::List(vec![Value::Float(f64::NAN), Value::Null])];
        assert_eq!(
            HashedKey::new(key.clone(), &state),
            HashedKey::new(key, &state)
        );
    }
}
