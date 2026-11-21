// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::adapter::{Batch, Error, Result, MAX_KEY, MAX_SCAN_RECORDS, MAX_SCAN_ROWS, MAX_VALUE};
use crate::storage::{ScanLimit, Storage, StorageCursor};
use hawdb_storage::cow::{CowPageWeight, CowSegmentedMap};
use serde::Serialize;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub type Rows = Vec<(Vec<u8>, Vec<u8>)>;
const SCAN_WINDOW: usize = 32;
mod batch;
pub use batch::BatchRead;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Layout {
    Versioned,
    Hybrid,
    Cow,
}
impl Layout {
    pub fn id(self) -> u64 {
        match self {
            Self::Versioned => 1,
            Self::Hybrid => 2,
            Self::Cow => 3,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Self::Versioned => "versioned",
            Self::Hybrid => "hybrid",
            Self::Cow => "cow",
        }
    }
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "versioned" => Ok(Self::Versioned),
            "hybrid" => Ok(Self::Hybrid),
            "cow" => Ok(Self::Cow),
            _ => Err(Error::Invalid("unknown layout")),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key(Vec<u8>);
impl CowPageWeight for Key {
    fn cow_page_bytes(&self) -> usize {
        self.0.len()
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Value(Arc<Vec<u8>>);
impl CowPageWeight for Value {
    fn cow_page_bytes(&self) -> usize {
        self.0.len()
    }
}
type CowMap = CowSegmentedMap<Key, Value>;

pub enum Index<S: Storage> {
    Ordered { storage: Arc<S>, layout: Layout },
    Cow(Mutex<CowMap>),
}
pub enum View<S: Storage> {
    Ordered {
        storage: Arc<S>,
        layout: Layout,
        epoch: u64,
    },
    Cow(CowMap),
}
pub struct Previous<'a> {
    pub epoch: u64,
    pub value: &'a Option<Vec<u8>>,
}
pub enum Prepared {
    Ordered,
    Cow(CowMap),
}

struct EncodedKey {
    bytes: [u8; MAX_KEY * 2 + 10],
    len: usize,
}
impl EncodedKey {
    fn copy_from(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > self.bytes.len() {
            return Err(Error::Invalid("oversized encoded key"));
        }
        self.bytes[..bytes.len()].copy_from_slice(bytes);
        self.len = bytes.len();
        Ok(())
    }
}
impl std::ops::Deref for EncodedKey {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
fn prefix(key: &[u8], namespace: u8) -> EncodedKey {
    let mut encoded = EncodedKey {
        bytes: [0; MAX_KEY * 2 + 10],
        len: key.len() * 2 + 2,
    };
    encoded.bytes[0] = namespace;
    for (i, &b) in key.iter().enumerate() {
        encoded.bytes[1 + i * 2] = 1;
        encoded.bytes[2 + i * 2] = b;
    }
    encoded
}
fn version_key(key: &[u8], epoch: u64) -> EncodedKey {
    let mut encoded = prefix(key, 1);
    encoded.bytes[encoded.len..encoded.len + 8].copy_from_slice(&(!epoch).to_be_bytes());
    encoded.len += 8;
    encoded
}
fn version_key_parts(bytes: &[u8]) -> Result<(&[u8], u64)> {
    if bytes.first() != Some(&1) {
        return Err(Error::Invalid("invalid key namespace"));
    }
    let mut offset = 1;
    loop {
        match bytes.get(offset) {
            Some(0) => {
                offset += 1;
                break;
            }
            Some(1) => {
                bytes.get(offset + 1).ok_or(Error::Invalid("short key"))?;
                offset += 2;
                if offset > MAX_KEY * 2 + 1 {
                    return Err(Error::Invalid("oversized encoded key"));
                }
            }
            _ => return Err(Error::Invalid("invalid key encoding")),
        }
    }
    let epoch = !u64::from_be_bytes(
        bytes
            .get(offset..)
            .ok_or(Error::Invalid("short version"))?
            .try_into()
            .map_err(|_| Error::Invalid("invalid version"))?,
    );
    Ok((&bytes[..offset], epoch))
}
fn decode_key(prefix: &[u8]) -> Vec<u8> {
    prefix[1..prefix.len() - 1]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| pair[1])
        .collect()
}
fn encode_value<'a>(
    value: &Option<Vec<u8>>,
    epoch: Option<u64>,
    out: &'a mut [u8; MAX_VALUE + 9],
) -> &'a [u8] {
    let offset = if let Some(epoch) = epoch {
        out[..8].copy_from_slice(&epoch.to_be_bytes());
        8
    } else {
        0
    };
    match value {
        None => {
            out[offset] = 0;
            &out[..offset + 1]
        }
        Some(value) => {
            out[offset] = 1;
            out[offset + 1..offset + 1 + value.len()].copy_from_slice(value);
            &out[..offset + 1 + value.len()]
        }
    }
}
fn decode_value(bytes: &[u8]) -> Result<Option<Vec<u8>>> {
    match bytes.first() {
        Some(0) if bytes.len() == 1 => Ok(None),
        Some(1) => Ok(Some(bytes[1..].to_vec())),
        _ => Err(Error::Invalid("invalid value tag")),
    }
}
fn history_get(storage: &impl Storage, key: &[u8], epoch: u64) -> Result<Option<Vec<u8>>> {
    let seek = version_key(key, epoch);
    let mut buffer = [0; MAX_KEY * 2 + MAX_VALUE + 64];
    let mut scan = storage.scan(&seek, ScanLimit::Count(NonZeroUsize::MIN))?;
    let Some((k, v)) = scan.next(&mut buffer)? else {
        return Ok(None);
    };
    if buffer.first() != Some(&1) {
        return Ok(None);
    }
    // The exact encoded prefix identifies this logical key without allocating
    // and decoding another key on every point read.
    if k != seek.len() || buffer[..k - 8] != seek[..seek.len() - 8] {
        return Ok(None);
    }
    let version = !u64::from_be_bytes(buffer[k - 8..k].try_into().expect("epoch bytes"));
    if version > epoch {
        return Ok(None);
    }
    decode_value(&buffer[k..k + v])
}

fn push_scan_row(rows: &mut Rows, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
    if rows.last().is_some_and(|(previous, _)| *previous >= key) {
        return Err(Error::Invalid("non-monotonic scan"));
    }
    if rows.len() == MAX_SCAN_ROWS {
        return Err(Error::Budget("scan rows"));
    }
    rows.push((key, value));
    Ok(())
}

fn head_key_pairs(encoded: &[u8]) -> Result<&[[u8; 2]]> {
    let [2, encoded @ .., 0] = encoded else {
        return Err(Error::Invalid("invalid head key"));
    };
    let (pairs, remainder) = encoded.as_chunks::<2>();
    if !remainder.is_empty() || pairs.len() > MAX_KEY || pairs.iter().any(|pair| pair[0] != 1) {
        return Err(Error::Invalid("invalid head key"));
    }
    Ok(pairs)
}

fn scan_hybrid(storage: &impl Storage, epoch: u64, start: &[u8], end: &[u8]) -> Result<Rows> {
    let mut resume = prefix(start, 2);
    let upper = prefix(end, 2);
    let mut rows = Vec::new();
    let mut deferred = Vec::new();
    let mut work = 0;
    loop {
        let mut resume_needed = false;
        {
            let mut scan = storage.scan(&resume, ScanLimit::EndInclusive(&upper))?;
            let mut buffer = [0; MAX_KEY * 2 + MAX_VALUE + 64];
            for visited in 1..=SCAN_WINDOW {
                let Some((key_len, value_len)) = scan.next(&mut buffer)? else {
                    break;
                };
                work += 1;
                if work > MAX_SCAN_RECORDS {
                    return Err(Error::Budget("scan physical records"));
                }
                let key = &buffer[..key_len];
                let pairs = head_key_pairs(key)?;
                if key >= &*upper {
                    break;
                }
                if visited == SCAN_WINDOW {
                    resume.copy_from(key)?;
                    // The terminator sorts before every extension of this
                    // key, including an extension by a zero byte.
                    resume.bytes[key_len - 1] = 1;
                    resume_needed = true;
                }

                let (version, value) = buffer[key_len..key_len + value_len]
                    .split_at_checked(8)
                    .ok_or(Error::Invalid("short head"))?;
                let version = u64::from_be_bytes(version.try_into().expect("epoch bytes"));
                if version > epoch {
                    deferred.push((pairs.iter().map(|pair| pair[1]).collect(), None));
                } else if let Some(value) = decode_value(value)? {
                    let key = pairs.iter().map(|pair| pair[1]).collect();
                    if deferred.is_empty() {
                        push_scan_row(&mut rows, key, value)?;
                    } else {
                        // Preserve order after the first head that needs a
                        // historical seek in this window.
                        deferred.push((key, Some(value)));
                    }
                }
            }
        }

        // A reader may race a commit even when captured at the latest epoch.
        // Resolve those heads after releasing the native scan guard.
        for (key, value) in deferred.drain(..) {
            let visible = match value {
                Some(value) => Some(value),
                None => history_get(storage, &key, epoch)?,
            };
            if let Some(value) = visible {
                push_scan_row(&mut rows, key, value)?;
            }
        }
        if !resume_needed {
            return Ok(rows);
        }
    }
}

impl<S: Storage> Index<S> {
    pub fn retained_value(&self, key: &[u8], epoch: u64, latest: bool) -> Result<Option<Vec<u8>>> {
        if let Self::Cow(state) = self {
            if !latest {
                return Err(Error::Invalid("COW history belongs to retained snapshots"));
            }
            return Ok(state
                .lock()
                .map_err(|_| Error::NeedsRecovery)?
                .get(&Key(key.to_vec()))
                .map(|value| value.0.as_ref().clone()));
        }
        let Self::Ordered { storage, layout } = self else {
            unreachable!()
        };
        let head = *layout == Layout::Hybrid && latest;
        let encoded = if head {
            prefix(key, 2)
        } else {
            version_key(key, epoch)
        };
        let mut buffer = [0; MAX_VALUE + 9];
        match storage.get(&encoded, &mut buffer)? {
            Some(n) => {
                let value = &buffer[..n];
                if head {
                    let found_epoch = u64::from_be_bytes(
                        value
                            .get(..8)
                            .ok_or(Error::Invalid("short retained head"))?
                            .try_into()
                            .expect("epoch"),
                    );
                    if found_epoch != epoch {
                        return Err(Error::Invalid("retained head epoch mismatch"));
                    }
                    decode_value(&value[8..])
                } else {
                    decode_value(value)
                }
            }
            None => Err(Error::Invalid("retained index version is missing")),
        }
    }
    pub fn current_head(&self, key: &[u8]) -> Result<Option<(u64, Option<Vec<u8>>)>> {
        let Self::Ordered {
            storage,
            layout: Layout::Hybrid,
        } = self
        else {
            return Ok(None);
        };
        let mut buffer = [0; MAX_VALUE + 9];
        match storage.get(&prefix(key, 2), &mut buffer)? {
            Some(n) => {
                let bytes = &buffer[..n];
                let epoch = u64::from_be_bytes(
                    bytes
                        .get(..8)
                        .ok_or(Error::Invalid("short replay head"))?
                        .try_into()
                        .expect("epoch"),
                );
                Ok(Some((epoch, decode_value(&bytes[8..])?)))
            }
            None => Ok(None),
        }
    }
    pub fn restore_version(
        &self,
        key: &[u8],
        epoch: u64,
        value: &Option<Vec<u8>>,
        latest: bool,
    ) -> Result<()> {
        let mut buffer = [0; MAX_VALUE + 9];
        match self {
            Self::Ordered { storage, layout } => {
                let head = *layout == Layout::Hybrid && latest;
                let key = if head {
                    prefix(key, 2)
                } else {
                    version_key(key, epoch)
                };
                storage.put(
                    &key,
                    encode_value(value, head.then_some(epoch), &mut buffer),
                )
            }
            Self::Cow(state) => {
                let mut state = state.lock().map_err(|_| Error::NeedsRecovery)?;
                if let Some(value) = value {
                    state.insert(Key(key.to_vec()), Value(Arc::new(value.clone())));
                } else {
                    state.remove(&Key(key.to_vec()));
                }
                Ok(())
            }
        }
    }
    pub fn remove_version(&self, key: &[u8], epoch: u64) -> Result<()> {
        if let Self::Ordered { storage, .. } = self {
            storage.delete(&version_key(key, epoch))?;
        }
        Ok(())
    }
    pub fn new(layout: Layout) -> Result<Self> {
        if layout == Layout::Cow {
            return Ok(Self::Cow(Mutex::new(CowMap::default())));
        }
        Ok(Self::Ordered {
            storage: Arc::new(S::new_empty()?),
            layout,
        })
    }
    pub fn capture(&self, published: &AtomicU64) -> Result<(u64, View<S>)> {
        Ok(match self {
            Self::Ordered { storage, layout } => {
                let epoch = published.load(Ordering::Acquire);
                (
                    epoch,
                    View::Ordered {
                        storage: Arc::clone(storage),
                        layout: *layout,
                        epoch,
                    },
                )
            }
            Self::Cow(state) => {
                let state = state.lock().map_err(|_| Error::NeedsRecovery)?;
                (published.load(Ordering::Acquire), View::Cow(state.clone()))
            }
        })
    }
    pub fn prepare<'a>(
        &self,
        batch: &Batch,
        mut previous: impl FnMut(&[u8]) -> Result<Option<Previous<'a>>>,
        mut installed: impl FnMut(usize) -> Result<()>,
    ) -> Result<Prepared> {
        let mut cow = match self {
            Self::Cow(state) => Some(state.lock().map_err(|_| Error::NeedsRecovery)?.clone()),
            _ => None,
        };
        let mut encoded_value = [0; MAX_VALUE + 9];
        for (i, change) in batch.writes.iter().enumerate() {
            match self {
                Self::Ordered {
                    storage,
                    layout: Layout::Versioned,
                } => storage.put(
                    &version_key(&change.key, batch.epoch),
                    encode_value(&change.value, None, &mut encoded_value),
                )?,
                Self::Ordered { storage, .. } => {
                    let head = prefix(&change.key, 2);
                    // The coordinator already retains exact immutable mutation
                    // receipts for retries. Borrow the previous value rather than
                    // reading and cache-promoting the head again on every update.
                    if let Some(previous) = previous(&change.key)? {
                        storage.put(
                            &version_key(&change.key, previous.epoch),
                            encode_value(previous.value, None, &mut encoded_value),
                        )?;
                    }
                    storage.put(
                        &head,
                        encode_value(&change.value, Some(batch.epoch), &mut encoded_value),
                    )?;
                }
                Self::Cow(_) => {
                    let state = cow.as_mut().expect("COW candidate");
                    match &change.value {
                        Some(v) => {
                            state.insert(Key(change.key.clone()), Value(Arc::new(v.clone())));
                        }
                        None => {
                            state.remove(&Key(change.key.clone()));
                        }
                    }
                }
            }
            installed(i + 1)?;
        }
        Ok(cow.map_or(Prepared::Ordered, Prepared::Cow))
    }
    pub fn publish(&self, prepared: Prepared, epoch: u64, published: &AtomicU64) -> Result<()> {
        match (self, prepared) {
            (Self::Cow(state), Prepared::Cow(candidate)) => {
                let mut state = state.lock().map_err(|_| Error::NeedsRecovery)?;
                *state = candidate;
                published.store(epoch, Ordering::Release);
            }
            (Self::Ordered { .. }, Prepared::Ordered) => published.store(epoch, Ordering::Release),
            _ => return Err(Error::Invalid("layout mismatch")),
        }
        Ok(())
    }
}

impl<S: Storage> View<S> {
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Cow(state) => Ok(state.get(&Key(key.to_vec())).map(|v| v.0.as_ref().clone())),
            Self::Ordered {
                storage,
                layout,
                epoch,
            } => {
                if *layout == Layout::Hybrid {
                    let mut head = [0; MAX_VALUE + 32];
                    match storage.get(&prefix(key, 2), &mut head)? {
                        Some(n) => {
                            let head = &head[..n];
                            let version = u64::from_be_bytes(
                                head.get(..8)
                                    .ok_or(Error::Invalid("short head"))?
                                    .try_into()
                                    .map_err(|_| Error::Invalid("short head"))?,
                            );
                            if version <= *epoch {
                                return decode_value(&head[8..]);
                            }
                        }
                        None => return Ok(None),
                    }
                }
                history_get(storage.as_ref(), key, *epoch)
            }
        }
    }
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Rows> {
        if start >= end {
            return Err(Error::Invalid("scan requires start < end"));
        }
        if let Self::Cow(state) = self {
            let mut rows = Vec::new();
            let lower = Key(start.to_vec());
            if let Some(v) = state.get(&lower) {
                rows.push((lower.0.clone(), v.0.as_ref().clone()));
            }
            for (k, v) in state.iter_after(Some(lower)) {
                if k.0.as_slice() >= end {
                    break;
                }
                rows.push((k.0.clone(), v.0.as_ref().clone()));
                if rows.len() > MAX_SCAN_ROWS {
                    return Err(Error::Budget("scan rows"));
                }
            }
            return Ok(rows);
        }
        let Self::Ordered {
            storage,
            layout,
            epoch,
        } = self
        else {
            unreachable!()
        };
        if *layout == Layout::Hybrid {
            return scan_hybrid(storage.as_ref(), *epoch, start, end);
        }
        let namespace = 1;
        let mut resume = prefix(start, namespace);
        let upper = prefix(end, namespace);
        let mut result: Rows = Vec::new();
        let mut seen: Option<EncodedKey> = None;
        let mut work = 0;
        let mut skip_resume = false;
        let mut batch = Vec::with_capacity(SCAN_WINDOW);
        loop {
            // Visibility and duplicate-version checks borrow the scan buffer.
            // Only result rows or keys needing a history seek become owned.
            // Bound native latch lifetime by physical records, including those
            // discarded here; drop it before any separate history lookup.
            let mut next_resume = prefix(start, namespace);
            let mut visited = 0;
            let mut finished = false;
            {
                let mut scan = storage.scan(&resume, ScanLimit::EndInclusive(&upper))?;
                let mut buffer = [0; MAX_KEY * 2 + MAX_VALUE + 64];
                while let Some((k, v)) = scan.next(&mut buffer)? {
                    let physical = &buffer[..k];
                    if skip_resume && physical == &*resume {
                        continue;
                    }
                    visited += 1;
                    work += 1;
                    if work > MAX_SCAN_RECORDS {
                        return Err(Error::Budget("scan physical records"));
                    }
                    next_resume.copy_from(physical)?;
                    let (encoded, version) = version_key_parts(physical)?;
                    if encoded >= &*upper {
                        finished = true;
                        break;
                    }
                    if version <= *epoch && seen.as_deref() != Some(encoded) {
                        // Mark tombstones too: their older values must not
                        // reappear, even across native scan chunks.
                        let mut selected = prefix(start, namespace);
                        selected.copy_from(encoded)?;
                        seen = Some(selected);
                        if let Some(value) = decode_value(&buffer[k..k + v])? {
                            batch.push((decode_key(encoded), value));
                        }
                    }
                    if visited == SCAN_WINDOW {
                        break;
                    }
                }
            }
            // An all-filtered physical chunk still advances the resume key.
            // An empty output batch is not proof that the range is exhausted.
            if visited == 0 {
                break;
            }
            let mut skip_next_resume = true;
            if visited == SCAN_WINDOW && !finished {
                let (encoded, version) = version_key_parts(&next_resume)?;
                let prefix_len = encoded.len();
                if seen.as_deref() == Some(encoded) {
                    // The visible value or tombstone is already selected.
                    // A terminator of 1 seeks after this key's versions but
                    // before every extension, including a zero-byte suffix.
                    next_resume.len = prefix_len;
                    next_resume.bytes[prefix_len - 1] = 1;
                    skip_next_resume = false;
                } else if version > *epoch {
                    // Skip remaining versions newer than the fixed snapshot.
                    // The boundary is inclusive: an exact epoch match has not
                    // been visited and must not be skipped on the next scan.
                    next_resume.bytes[prefix_len..prefix_len + 8]
                        .copy_from_slice(&(!*epoch).to_be_bytes());
                    skip_next_resume = false;
                }
            }
            resume = next_resume;
            skip_resume = skip_next_resume;
            for (key, value) in batch.drain(..) {
                push_scan_row(&mut result, key, value)?;
            }
            if finished {
                break;
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn physical_scan_budget_counts_versions_discarded_before_copying() {
        let index = Index::<crate::storage::BfTreeStorage>::new(Layout::Versioned).unwrap();
        let (_, empty) = index.capture(&AtomicU64::new(0)).unwrap();
        let Index::Ordered { storage, .. } = &index else {
            unreachable!()
        };
        // Populate the actual index without journal synchronization: this
        // checks scan work admission, not transaction durability. Every
        // key is distinct, newer than the retained snapshot and produces no
        // row. Version-run seeks cannot skip these unvisited logical keys.
        for key in 0..MAX_SCAN_RECORDS as u64 {
            storage
                .put(&version_key(&key.to_be_bytes(), 1), &[0])
                .unwrap();
        }
        assert!(empty.scan(b"", b"z").unwrap().is_empty());
        storage
            .put(
                &version_key(&(MAX_SCAN_RECORDS as u64).to_be_bytes(), 1),
                &[0],
            )
            .unwrap();
        assert!(matches!(
            empty.scan(b"", b"z"),
            Err(Error::Budget("scan physical records"))
        ));
    }
}
