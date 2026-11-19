// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use crate::adapter::{Error, Result};
use bf_tree::{BfTree, Config, LeafInsertResult, LeafReadResult, ScanIter, ScanReturnField};
use std::num::NonZeroUsize;

/// An ordered byte store for one rebuildable MVCC index generation.
///
/// Reads must see completed mutations and cannot observe a partially written
/// record. Keys use unsigned bytewise lexicographic order. The MVCC layer
/// owns version encoding, snapshot visibility, and journal durability.
pub trait Storage: Send + Sync + Sized {
    type Cursor<'a>: StorageCursor
    where
        Self: 'a;

    /// Open a fresh empty generation, independent of any still in use by
    /// readers. Both recovery and checkpoint reconstruction use this entry.
    fn new_empty() -> Result<Self>;

    /// Insert or replace the exact key.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Copy the value into the caller's buffer, or return None for a missing
    /// key. The caller supplies space for the largest encoded value.
    fn get(&self, key: &[u8], buffer: &mut [u8]) -> Result<Option<usize>>;

    /// Remove the physical key. An already missing key is a successful no-op.
    fn delete(&self, key: &[u8]) -> Result<()>;

    /// Seek to the first key at or after start, then yield keys in ascending
    /// order up to the bound. A scan may see concurrent updates and must visit
    /// every matching key that remains present throughout the scan. The caller
    /// drops the cursor before further reads, so it may hold read guards.
    fn scan(&self, start: &[u8], limit: ScanLimit<'_>) -> Result<Self::Cursor<'_>>;
}

pub enum ScanLimit<'a> {
    Count(NonZeroUsize),
    EndInclusive(&'a [u8]),
}

pub trait StorageCursor {
    /// Copy the next key followed by its value into the caller's buffer and
    /// return their lengths. None marks exhaustion. The caller supplies space
    /// for the largest encoded record.
    fn next(&mut self, buffer: &mut [u8]) -> Result<Option<(usize, usize)>>;
}

pub struct BfTreeStorage(BfTree);

impl Storage for BfTreeStorage {
    type Cursor<'a> = ScanIter<'a, 'a>;

    fn new_empty() -> Result<Self> {
        let mut config = Config::default();
        config
            // bf-tree 0.5.6 can scan-promote a clean record-cache mini-page
            // without loading its base buffer. Use whole-page read caching
            // until that dependency path is fixed; keep both layouts identical.
            .read_record_cache(false)
            .cb_min_record_size(4)
            .cb_max_record_size(1400)
            .cb_max_key_len(256)
            .cb_size_byte(4 * 1024 * 1024);
        BfTree::with_config(config, None)
            .map(Self)
            .map_err(bftree_error)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        match self.0.insert(key, value) {
            LeafInsertResult::Success => Ok(()),
            LeafInsertResult::InvalidKV(reason) => Err(Error::Index(format!("Bf-Tree: {reason}"))),
        }
    }

    fn get(&self, key: &[u8], buffer: &mut [u8]) -> Result<Option<usize>> {
        match self.0.read(key, buffer) {
            LeafReadResult::Found(len) => Ok(Some(len as usize)),
            LeafReadResult::NotFound => Ok(None),
            other => Err(bftree_error(other)),
        }
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.0.delete(key);
        Ok(())
    }

    fn scan(&self, start: &[u8], limit: ScanLimit<'_>) -> Result<Self::Cursor<'_>> {
        match limit {
            ScanLimit::Count(count) => {
                self.0
                    .scan_with_count(start, count.get(), ScanReturnField::KeyAndValue)
            }
            ScanLimit::EndInclusive(end) => {
                self.0
                    .scan_with_end_key(start, end, ScanReturnField::KeyAndValue)
            }
        }
        .map_err(bftree_error)
    }
}

impl StorageCursor for ScanIter<'_, '_> {
    fn next(&mut self, buffer: &mut [u8]) -> Result<Option<(usize, usize)>> {
        Ok(ScanIter::next(self, buffer))
    }
}

fn bftree_error(error: impl std::fmt::Debug) -> Error {
    Error::Index(format!("Bf-Tree: {error:?}"))
}
