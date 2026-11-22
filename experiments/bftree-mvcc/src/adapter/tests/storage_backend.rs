// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::storage::{ScanLimit, StorageCursor};
use std::ops::Bound::{Included, Unbounded};

#[derive(Default)]
struct MapStorage {
    records: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    refuse_delete: AtomicBool,
}

struct MapCursor(std::vec::IntoIter<(Vec<u8>, Vec<u8>)>);

impl StorageCursor for MapCursor {
    fn next(&mut self, buffer: &mut [u8]) -> Result<Option<(usize, usize)>> {
        Ok(self.0.next().map(|(key, value)| {
            let (key_buffer, value_buffer) = buffer.split_at_mut(key.len());
            key_buffer.copy_from_slice(&key);
            value_buffer[..value.len()].copy_from_slice(&value);
            (key.len(), value.len())
        }))
    }
}

impl Storage for MapStorage {
    type Cursor<'a> = MapCursor;

    fn new_empty() -> Result<Self> {
        Ok(Self::default())
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.records
            .write()
            .unwrap()
            .insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8], buffer: &mut [u8]) -> Result<Option<usize>> {
        Ok(self.records.read().unwrap().get(key).map(|value| {
            buffer[..value.len()].copy_from_slice(value);
            value.len()
        }))
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.refuse_delete.load(Ordering::Relaxed) {
            return Err(Error::Index("test backend refused deletion".into()));
        }
        self.records.write().unwrap().remove(key);
        Ok(())
    }

    fn scan(&self, start: &[u8], limit: ScanLimit<'_>) -> Result<Self::Cursor<'_>> {
        let (end, count) = match limit {
            ScanLimit::Count(count) => (Unbounded, count.get()),
            ScanLimit::EndInclusive(end) => (Included(end), usize::MAX),
        };
        let rows: Vec<_> = self
            .records
            .read()
            .unwrap()
            .range::<[u8], _>((Included(start), end))
            .take(count)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(MapCursor(rows.into_iter()))
    }
}

#[test]
fn storage_backend_can_replay_and_write_the_same_journal_across_generations() {
    for layout in [Layout::Versioned, Layout::Hybrid] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal");
        let key = b"\0\xff";
        let upper = b"\xff";
        let store = Store::open_layout(&path, layout).unwrap();
        let mut seed = store.begin_write().unwrap();
        seed.put(b"", b"seed").unwrap();
        seed.put(key, b"seed").unwrap();
        seed.put(upper, b"exclusive bound").unwrap();
        seed.commit(1).unwrap();
        store.checkpoint().unwrap();

        let mut update = store.begin_write().unwrap();
        update.put(b"", b"current").unwrap();
        update.delete(key).unwrap();
        update.commit(2).unwrap();
        drop(store);

        let store = Store::<MapStorage>::open_with_storage(&path, layout).unwrap();
        let stats = store.storage_stats().unwrap();
        assert_eq!(stats.recovery.checkpoint_epoch, 1);
        assert_eq!(stats.recovery.replayed_batches, 1);
        let old = store.begin_read().unwrap();
        assert_eq!(old.epoch(), 2);
        assert_eq!(
            old.scan(b"", upper).unwrap(),
            vec![(vec![], b"current".to_vec())]
        );

        let mut update = store.begin_write().unwrap();
        update.put(b"", b"updated").unwrap();
        update.put(key, b"").unwrap();
        update.commit(3).unwrap();
        store.checkpoint().unwrap();

        let mut update = store.begin_write().unwrap();
        update.delete(b"").unwrap();
        update.commit(4).unwrap();
        assert_eq!(old.get(b"").unwrap(), Some(b"current".to_vec()));
        assert_eq!(
            old.get_many(&[key, b"", b"missing", b""]).unwrap().values,
            vec![
                None,
                Some(b"current".to_vec()),
                None,
                Some(b"current".to_vec())
            ]
        );
        drop(old);
        let collection = store.collect_garbage(MAX_GC_WORK).unwrap();
        assert!(collection.complete_pass);
        assert_eq!(collection.removed, 5);
        assert_eq!(
            store.begin_read().unwrap().scan(b"", upper).unwrap(),
            vec![(key.to_vec(), vec![])]
        );
        drop(store);

        let reopened = Store::open_layout(&path, layout).unwrap();
        let stats = reopened.storage_stats().unwrap();
        assert_eq!(stats.recovery.checkpoint_epoch, 3);
        assert_eq!(stats.recovery.replayed_batches, 1);
        assert_eq!(stats.retained_versions, 3);
        assert_eq!(
            reopened.begin_read().unwrap().scan(b"", upper).unwrap(),
            vec![(key.to_vec(), vec![])]
        );
    }
}

#[test]
fn storage_backend_deletion_error_requires_recovery_after_durable_collection() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("journal");
    let store = Store::<MapStorage>::open_with_storage(&path, Layout::Hybrid).unwrap();
    for epoch in [1, 2] {
        let mut write = store.begin_write().unwrap();
        write.put(b"key", &[epoch]).unwrap();
        write.commit(u64::from(epoch)).unwrap();
    }
    {
        let guard = store.0.index.read().unwrap();
        let Index::Ordered { storage, .. } = &*guard else {
            panic!("expected the configured ordered backend");
        };
        storage.refuse_delete.store(true, Ordering::Relaxed);
    }
    assert!(matches!(
        store.collect_garbage(MAX_GC_WORK),
        Err(Error::CommitUncertain(reason)) if reason.contains("test backend refused deletion")
    ));
    assert!(matches!(store.begin_read(), Err(Error::NeedsRecovery)));
    drop(store);

    let reopened = Store::<MapStorage>::open_with_storage(&path, Layout::Hybrid).unwrap();
    assert_eq!(reopened.storage_stats().unwrap().retained_versions, 1);
    assert_eq!(
        reopened.begin_read().unwrap().get(b"key").unwrap(),
        Some(vec![2])
    );
}
