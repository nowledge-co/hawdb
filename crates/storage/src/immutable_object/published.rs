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

//! Process-local record of immutable objects that this process published.
//!
//! A streamed publication hashes the exact bytes it writes into a new,
//! exclusively created staging inode, syncs that inode, hard-links it into the
//! object namespace and syncs the directories. Only then is the object
//! recorded here, keyed by its complete reference and native file identity,
//! with the length and modification time observed on the written descriptor.
//!
//! Later whole-object identity re-hashes in the same process (branch mount,
//! cold immutable-handle validation and selector dependency checks) may skip
//! the hash when the opened descriptor still has exactly that identity, length
//! and modification time. Everything else still hashes: objects this process
//! did not publish, every object after a restart or crash (the record lives
//! only in memory), reuse of an object found on disk, reclamation and explicit
//! scrub. Page and block checks on demand reads are unchanged.
//!
//! The stamp detects replacement (a different inode) and ordinary later
//! writes, which change the modification time. It does not detect media
//! corruption, a write within the same coarse timestamp tick, or a writer that
//! restores the modification time; the database directory is exclusively
//! owned and immutable objects are never written in place. Those cases are
//! covered by demand-read page/block checksums, `scrub_storage` and full
//! verification on the next cold open.

use super::ObjectReference;
use crate::native_file_identity::NativeIdentity;
use std::collections::BTreeMap;
use std::io;
use std::path::Path;
use std::sync::Mutex;
use std::time::SystemTime;

/// Bounds memory (a few MiB). Evicting a record only restores a full re-hash.
const MAX_RECORDS: usize = 16 * 1024;

/// Native identity, length and modification time of a written descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WrittenStamp {
    identity: NativeIdentity,
    length: u64,
    modified: SystemTime,
}

impl WrittenStamp {
    #[cfg(any(unix, windows))]
    pub(crate) fn of_native(file: &std::fs::File) -> io::Result<Self> {
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(io::Error::other("published object is not a regular file"));
        }
        Ok(Self {
            identity: crate::native_file_identity::of_file(file)?,
            length: metadata.len(),
            modified: metadata.modified()?,
        })
    }

    /// Without a native file identity nothing is recorded or skipped.
    #[cfg(not(any(unix, windows)))]
    pub(crate) fn of_native(_file: &std::fs::File) -> io::Result<Self> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "published object stamps require a native file identity",
        ))
    }
}

type Key = (ObjectReference, NativeIdentity);

struct Record {
    length: u64,
    modified: SystemTime,
    skipped_rehashes: u64,
    last_used: u64,
}

/// Records plus their recency order; the least recently published or
/// skipped record is evicted first, independent of object kind or size.
struct Registry {
    records: BTreeMap<Key, Record>,
    recency: BTreeMap<u64, Key>,
    tick: u64,
}

impl Registry {
    const fn new() -> Self {
        Self {
            records: BTreeMap::new(),
            recency: BTreeMap::new(),
            tick: 0,
        }
    }

    fn touch(&mut self, key: Key) {
        let tick = self.tick;
        self.tick = self.tick.wrapping_add(1);
        if let Some(record) = self.records.get_mut(&key) {
            self.recency.remove(&record.last_used);
            record.last_used = tick;
            self.recency.insert(tick, key);
        }
    }

    /// Inserts or replaces `key`, evicting least recently used records to
    /// stay within `capacity`.
    fn insert(&mut self, key: Key, stamp: WrittenStamp, capacity: usize) {
        self.remove(&key);
        while self.records.len() >= capacity {
            let Some((_, oldest)) = self.recency.pop_first() else {
                break;
            };
            self.records.remove(&oldest);
        }
        self.records.insert(
            key,
            Record {
                length: stamp.length,
                modified: stamp.modified,
                skipped_rehashes: 0,
                last_used: 0,
            },
        );
        self.touch(key);
    }

    fn remove(&mut self, key: &Key) -> bool {
        match self.records.remove(key) {
            Some(record) => {
                self.recency.remove(&record.last_used);
                true
            }
            None => false,
        }
    }
}

static PUBLISHED: Mutex<Registry> = Mutex::new(Registry::new());

fn registry() -> std::sync::MutexGuard<'static, Registry> {
    PUBLISHED.lock().unwrap_or_else(|error| error.into_inner())
}

/// Records an object after its hashed bytes and namespace entry are durable.
pub(crate) fn record(reference: ObjectReference, stamp: WrittenStamp) {
    if stamp.length != reference.byte_length {
        return;
    }
    registry().insert((reference, stamp.identity), stamp, MAX_RECORDS);
}

/// Returns true, and counts the skip, only when `file` is the exact inode
/// this process published for `reference`, unmodified since publication.
/// Any inspection failure returns false so the caller verifies fully.
pub(crate) fn skip_rehash(reference: ObjectReference, file: &std::fs::File) -> bool {
    let Ok(stamp) = WrittenStamp::of_native(file) else {
        return false;
    };
    let key = (reference, stamp.identity);
    let mut registry = registry();
    match registry.records.get_mut(&key) {
        Some(record)
            if record.length == stamp.length
                && record.length == reference.byte_length
                && record.modified == stamp.modified =>
        {
            record.skipped_rehashes = record.skipped_rehashes.saturating_add(1);
            registry.touch(key);
            true
        }
        _ => false,
    }
}

fn stamp_of_path(path: &Path) -> io::Result<WrittenStamp> {
    crate::file_io::File::open(path)?.with_native(WrittenStamp::of_native)
}

/// Number of identity re-hashes skipped for the object file at `path`, or
/// `None` when this process has no publication record for that inode.
/// Diagnostic and test hook; it does not affect verification.
#[doc(hidden)]
pub fn published_object_skipped_rehashes(path: &Path) -> io::Result<Option<u64>> {
    let stamp = stamp_of_path(path)?;
    Ok(registry()
        .records
        .iter()
        .find(|((_, identity), record)| {
            // A freed inode number can be reused; match the complete stamp.
            *identity == stamp.identity
                && record.length == stamp.length
                && record.modified == stamp.modified
        })
        .map(|(_, record)| record.skipped_rehashes))
}

/// Forgets every publication record for the object file at `path`, so the
/// next verification in this process re-hashes it as a cold open would.
#[doc(hidden)]
pub fn forget_published_object(path: &Path) -> io::Result<bool> {
    let stamp = stamp_of_path(path)?;
    let mut registry = registry();
    let keys = registry
        .records
        .keys()
        .filter(|(_, identity)| *identity == stamp.identity)
        .copied()
        .collect::<Vec<_>>();
    for key in &keys {
        registry.remove(key);
    }
    Ok(!keys.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::immutable_object::ObjectKind;

    fn key(byte: u8) -> Key {
        (
            ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &[byte]),
            NativeIdentity::default(),
        )
    }

    fn stamp() -> WrittenStamp {
        WrittenStamp {
            identity: NativeIdentity::default(),
            length: 1,
            modified: SystemTime::UNIX_EPOCH,
        }
    }

    #[test]
    fn eviction_is_least_recently_used_not_key_order() {
        let mut registry = Registry::new();
        // Insert in descending key order so key order and age disagree.
        let mut keys = (0..4).map(key).collect::<Vec<_>>();
        keys.sort();
        keys.reverse();
        for key in &keys[..3] {
            registry.insert(*key, stamp(), 3);
        }
        // A skip refreshes the oldest record; the next oldest is evicted.
        registry.touch(keys[0]);
        registry.insert(keys[3], stamp(), 3);
        assert!(registry.records.contains_key(&keys[0]));
        assert!(!registry.records.contains_key(&keys[1]));
        assert!(registry.records.contains_key(&keys[2]));
        assert!(registry.records.contains_key(&keys[3]));
        assert_eq!(registry.records.len(), registry.recency.len());

        // Re-recording an existing key replaces it without evicting another.
        registry.insert(keys[2], stamp(), 3);
        assert_eq!(registry.records.len(), 3);
        assert_eq!(registry.recency.len(), 3);
        assert!(registry.remove(&keys[2]));
        assert!(!registry.remove(&keys[2]));
        assert_eq!(registry.records.len(), registry.recency.len());
    }
}
