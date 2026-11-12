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

//! Resident relational row maps.
//!
//! [`RelationalRowPages`] keeps its logical row count and the exact sum of
//! [`relational_row_entry_bytes`] over its rows next to the pages. The fields
//! are private to this module so every row mutation goes through a method
//! that maintains both totals; per-commit checkpoint sizing then reads the
//! totals instead of walking every resident row (nowledge-co/hawdb#1519).

use super::{
    relational_row_entry_bytes, RelationalError, RelationalKey, RelationalPrimaryKeyRange,
    RelationalRow, RelationalRowDirectory, RelationalRowPage, RelationalValue,
    RELATIONAL_ROW_PAGE_MAX_ENTRIES, RELATIONAL_ROW_PAGE_TARGET_BYTES,
};
use std::collections::BTreeMap;

/// An ordered row map backed by immutable COW pages.
///
/// A relational snapshot shares every page. A mutation clones only the page
/// containing the affected primary key instead of cloning the entire table.
///
/// `len` and `entry_bytes` are maintained by every mutating method through
/// [`Self::account_entry`], so reading either is O(1). `entry_bytes` is the
/// exact (unsaturated) sum of [`relational_row_entry_bytes`] over the stored
/// key/row pairs; `u128` cannot overflow for any addressable row count.
#[derive(Debug, Clone, Default)]
pub(super) struct RelationalRowPages {
    pages: RelationalRowDirectory,
    len: usize,
    entry_bytes: u128,
}

impl RelationalRowPages {
    /// Records one row-entry change. `removed` is the entry the map held for
    /// the affected key before the change and `added` the entry it holds after
    /// it, each measured with the key actually stored in the map.
    fn account_entry(&mut self, removed: Option<usize>, added: Option<usize>) {
        match (removed, added) {
            (None, Some(_)) => self.len = self.len.saturating_add(1),
            (Some(_), None) => self.len = self.len.saturating_sub(1),
            _ => {}
        }
        if let Some(removed) = removed {
            self.entry_bytes = self
                .entry_bytes
                .checked_sub(removed as u128)
                .expect("resident row byte total covers every resident row");
        }
        if let Some(added) = added {
            self.entry_bytes += added as u128;
        }
    }

    /// Entry bytes removed and added by inserting `row` under `key` into
    /// `page`. `BTreeMap::insert` keeps the stored key when it replaces a
    /// value, so a replacement is measured with the stored key.
    fn insert_entry_bytes(
        page: &BTreeMap<RelationalKey, RelationalRow>,
        key: &RelationalKey,
        row: &RelationalRow,
    ) -> (Option<usize>, usize) {
        match page.get_key_value(key) {
            Some((stored, previous)) => (
                Some(relational_row_entry_bytes(stored, previous)),
                relational_row_entry_bytes(stored, row),
            ),
            None => (None, relational_row_entry_bytes(key, row)),
        }
    }

    /// Exact sum of [`relational_row_entry_bytes`] over all resident rows.
    pub(super) fn entry_bytes(&self) -> u128 {
        self.entry_bytes
    }

    /// Full O(rows) recomputation of [`Self::entry_bytes`], used only to
    /// verify the maintained total.
    #[cfg(any(test, debug_assertions))]
    pub(super) fn recomputed_entry_bytes(&self) -> u128 {
        super::without_row_entry_byte_probe(|| {
            self.iter()
                .map(|(key, row)| relational_row_entry_bytes(key, row) as u128)
                .sum()
        })
    }

    /// An empty map over a caller-allocated (checkpoint-owned) directory.
    pub(super) fn from_empty_directory(pages: RelationalRowDirectory) -> Self {
        assert!(
            pages.is_empty(),
            "an empty row map needs an empty directory"
        );
        Self {
            pages,
            len: 0,
            entry_bytes: 0,
        }
    }

    /// Adopts ordered, already paged rows whose totals the caller computed
    /// while building the pages (the metered checkpoint decoder). Debug builds
    /// verify both totals.
    pub(super) fn from_ordered_pages(
        pages: Vec<RelationalRowPage>,
        len: usize,
        entry_bytes: u128,
    ) -> Self {
        let output = Self {
            pages: pages.into(),
            len,
            entry_bytes,
        };
        #[cfg(debug_assertions)]
        {
            debug_assert_eq!(output.iter().count(), len);
            debug_assert_eq!(output.recomputed_entry_bytes(), entry_bytes);
        }
        output
    }

    /// Appends one authenticated hydrated row in sorted order, moving keys
    /// into bounded pages without split-key clones or directory growth.
    pub(super) fn push_sorted_with_allocations(
        &mut self,
        key: RelationalKey,
        row: RelationalRow,
        context: &crate::background::CheckpointDecodeContext,
        directory_capacity: usize,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        let entry_bytes = relational_row_entry_bytes(&key, &row);
        let new_page = self.pages.last().is_none_or(|page| {
            page.len() >= RELATIONAL_ROW_PAGE_MAX_ENTRIES
                || (!page.is_empty()
                    && page
                        .iter()
                        .map(|(key, row)| relational_row_entry_bytes(key, row))
                        .sum::<usize>()
                        .saturating_add(entry_bytes)
                        > RELATIONAL_ROW_PAGE_TARGET_BYTES)
        });
        context
            .reserve(crate::cow::checkpoint_tree_node_bytes::<
                RelationalKey,
                RelationalRow,
            >())
            .map_err(admission)?;
        if self.pages.is_empty() {
            let pages = context
                .vector_capacity(directory_capacity)
                .map_err(admission)?;
            context
                .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
                .map_err(admission)?;
            self.pages = pages.into();
        }
        if new_page {
            context
                .reserve(RelationalRowPage::checkpoint_ownership_bytes())
                .map_err(admission)?;
            let unit = context
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            self.pages
                .get_unique_mut()
                .expect("private sparse directory")
                .push(BTreeMap::new().into());
            unit.finish();
        }
        let unit = context
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let page = self
            .pages
            .get_unique_mut()
            .expect("private sparse directory")
            .last_mut()
            .expect("sparse row page")
            .get_unique_mut()
            .expect("private sparse row page");
        let (removed, added) = Self::insert_entry_bytes(page, &key, &row);
        let replaced = page.insert(key, row);
        self.account_entry(removed, Some(added));
        unit.finish();
        Ok(replaced)
    }

    #[cfg(test)]
    pub(super) fn pages(&self) -> &RelationalRowDirectory {
        &self.pages
    }

    pub(super) fn prepare_checkpoint_copy_for_key(
        &mut self,
        key: &RelationalKey,
        work: &crate::background::CheckpointWorkContext,
    ) -> Result<(), RelationalError> {
        let Some(index) = self.page_index(key) else {
            return Ok(());
        };
        let copy_directory = self.pages.is_shared();
        let copy_page = copy_directory || self.pages[index].is_shared();
        if !copy_page {
            return Ok(());
        }
        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        // Inventories precede temporary buffers, so denied copies destroy data first.
        let directory_allocations = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let page_allocations = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut directory = if copy_directory {
            let mut pages = directory_allocations
                .vector_capacity(self.pages.len())
                .map_err(admission)?;
            directory_allocations
                .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
                .map_err(admission)?;
            for page in self.pages.iter() {
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                pages.push(page.clone());
                unit.finish();
            }
            Some(pages)
        } else {
            None
        };
        let node_bytes = (self.pages[index].len() / 4 + 2)
            .checked_mul(crate::cow::checkpoint_tree_node_bytes::<
                RelationalKey,
                RelationalRow,
            >())
            .ok_or_else(|| RelationalError::Admission("row page copy capacity overflow".into()))?;
        page_allocations.reserve(node_bytes).map_err(admission)?;
        page_allocations
            .reserve(RelationalRowPage::checkpoint_ownership_bytes())
            .map_err(admission)?;
        let mut rows = BTreeMap::new();
        for (key, row) in self.pages[index].iter() {
            let key = key.clone_with_allocations(&page_allocations)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            rows.insert(key, row.clone());
            unit.finish();
        }
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let replacement = crate::cow::CowSegment::from_checkpoint_value(
            rows,
            page_allocations.memory.into_inner(),
        );
        if let Some(mut pages) = directory.take() {
            pages[index] = replacement;
            self.pages = crate::cow::CowSegment::from_checkpoint_value(
                pages,
                directory_allocations.memory.into_inner(),
            );
        } else {
            self.pages.get_unique_mut().expect("private row directory")[index] = replacement;
        }
        Ok(())
    }

    pub(super) fn insert_with_allocations(
        &mut self,
        key: RelationalKey,
        row: RelationalRow,
        allocations: Option<&crate::background::CheckpointDecodeContext>,
    ) -> Result<Option<RelationalRow>, RelationalError> {
        let Some(context) = allocations else {
            return Ok(self.insert(key, row));
        };
        let work = &context.work;
        let admission =
            |error: hawdb_core::HawDBError| RelationalError::Admission(error.to_string());
        let directory_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let page_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let split_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let index = self.page_index(&key).unwrap_or(0);
        let previous_len = self.pages.get(index).map_or(0, |page| page.len());
        let capacity =
            self.pages.len().checked_add(1).ok_or_else(|| {
                RelationalError::Admission("row directory growth overflow".into())
            })?;
        let mut directory = directory_context
            .vector_capacity(capacity)
            .map_err(admission)?;
        directory_context
            .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
            .map_err(admission)?;
        for page in self.pages.iter() {
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            directory.push(page.clone());
            unit.finish();
        }
        let nodes = previous_len
            .checked_add(3)
            .ok_or_else(|| RelationalError::Admission("row page growth overflow".into()))?;
        let bytes = nodes
            .checked_mul(crate::cow::checkpoint_tree_node_bytes::<
                RelationalKey,
                RelationalRow,
            >())
            .and_then(|bytes| {
                bytes.checked_add(2 * RelationalRowPage::checkpoint_ownership_bytes())
            })
            .ok_or_else(|| RelationalError::Admission("row page growth overflow".into()))?;
        page_context.reserve(bytes).map_err(admission)?;
        let mut rows = BTreeMap::new();
        if let Some(page) = self.pages.get(index) {
            for (key, row) in page.iter() {
                let key = key.clone_with_allocations(&page_context)?;
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                rows.insert(key, row.clone());
                unit.finish();
            }
        }
        let key = key.clone_with_allocations(&page_context)?;
        let (removed_bytes, added_bytes) = Self::insert_entry_bytes(&rows, &key, &row);
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let previous = rows.insert(key, row);
        unit.finish();
        let mut page_bytes = 0usize;
        for (key, row) in &rows {
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            page_bytes = page_bytes.saturating_add(relational_row_entry_bytes(key, row));
            unit.finish();
        }
        let right = if rows.len() > 1
            && (rows.len() > RELATIONAL_ROW_PAGE_MAX_ENTRIES
                || page_bytes > RELATIONAL_ROW_PAGE_TARGET_BYTES)
        {
            let split_key = rows
                .keys()
                .nth(rows.len() / 2)
                .expect("nonempty split page")
                .clone_with_allocations(&split_context)?;
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let right = rows.split_off(&split_key);
            unit.finish();
            Some(right)
        } else {
            None
        };
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let memory = page_context.memory.into_inner();
        let left = crate::cow::CowSegment::from_checkpoint_value(rows, memory.clone());
        if directory.is_empty() {
            directory.push(left);
        } else {
            directory[index] = left;
        }
        if let Some(right) = right {
            directory.insert(
                index + 1,
                crate::cow::CowSegment::from_checkpoint_value(right, memory),
            );
        }
        self.pages = crate::cow::CowSegment::from_checkpoint_value(
            directory,
            directory_context.memory.into_inner(),
        );
        self.account_entry(removed_bytes, Some(added_bytes));
        Ok(previous)
    }

    pub(super) fn from_map(rows: BTreeMap<RelationalKey, RelationalRow>) -> Self {
        let len = rows.len();
        let mut pages = Vec::new();
        let mut page = BTreeMap::new();
        let mut page_bytes = 0usize;
        let mut total_entry_bytes = 0u128;
        for (key, row) in rows {
            let entry_bytes = relational_row_entry_bytes(&key, &row);
            total_entry_bytes += entry_bytes as u128;
            if !page.is_empty()
                && (page.len() >= RELATIONAL_ROW_PAGE_MAX_ENTRIES
                    || page_bytes.saturating_add(entry_bytes) > RELATIONAL_ROW_PAGE_TARGET_BYTES)
            {
                pages.push(crate::cow::CowSegment::from(std::mem::take(&mut page)));
                page_bytes = 0;
            }
            page_bytes = page_bytes.saturating_add(entry_bytes);
            page.insert(key, row);
        }
        if !page.is_empty() {
            pages.push(crate::cow::CowSegment::from(page));
        }
        Self {
            pages: pages.into(),
            len,
            entry_bytes: total_entry_bytes,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn page_index(&self, key: &RelationalKey) -> Option<usize> {
        if self.pages.is_empty() {
            return None;
        }
        let index = self.pages.partition_point(|page| {
            page.last_key_value()
                .is_some_and(|(last_key, _)| last_key < key)
        });
        Some(index.min(self.pages.len() - 1))
    }

    pub(super) fn get(&self, key: &RelationalKey) -> Option<&RelationalRow> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get(key))
    }

    pub(super) fn get_key_value(
        &self,
        key: &RelationalKey,
    ) -> Option<(&RelationalKey, &RelationalRow)> {
        self.page_index(key)
            .and_then(|index| self.pages[index].get_key_value(key))
    }

    pub(super) fn contains_key(&self, key: &RelationalKey) -> bool {
        self.get(key).is_some()
    }

    pub(super) fn iter(&self) -> impl DoubleEndedIterator<Item = (&RelationalKey, &RelationalRow)> {
        self.pages.iter().flat_map(|page| page.iter())
    }

    pub(super) fn range(
        &self,
        range: Option<RelationalPrimaryKeyRange>,
    ) -> impl DoubleEndedIterator<Item = (&RelationalKey, &RelationalRow)> {
        use std::ops::Bound;
        let range = range.unwrap_or(RelationalPrimaryKeyRange {
            lower: Bound::Unbounded,
            upper: Bound::Unbounded,
        });
        let start = match &range.lower {
            Bound::Included(key) | Bound::Excluded(key) => self.page_index(key).unwrap_or(0),
            Bound::Unbounded => 0,
        };
        let end = if range.is_empty() {
            start
        } else {
            match &range.upper {
                Bound::Included(key) | Bound::Excluded(key) => {
                    self.page_index(key).map_or(0, |index| index + 1)
                }
                Bound::Unbounded => self.pages.len(),
            }
            .max(start)
        };
        self.pages[start..end]
            .iter()
            .flat_map(move |page| page.range((range.lower.as_ref(), range.upper.as_ref())))
    }

    pub(super) fn first_key_after(&self, after: Option<&RelationalKey>) -> Option<&RelationalKey> {
        let Some(after) = after else {
            return self.pages.first()?.first_key_value().map(|(key, _)| key);
        };
        let page = self
            .pages
            .partition_point(|page| page.last_key_value().is_none_or(|(last, _)| last <= after));
        self.pages
            .get(page)?
            .range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            .next()
            .map(|(key, _)| key)
    }

    pub(super) fn values(&self) -> impl DoubleEndedIterator<Item = &RelationalRow> {
        self.iter().map(|(_, row)| row)
    }

    pub(super) fn append_value_to_all_with_allocations(
        &mut self,
        value: &RelationalValue,
        allocations: &crate::background::CheckpointDecodeContext,
    ) -> Result<(), RelationalError> {
        let work = &allocations.work;
        let directory_context = crate::background::CheckpointDecodeContext {
            work: work.clone(),
            memory: Default::default(),
        };
        let mut pages = directory_context
            .vector_capacity(self.pages.len())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        directory_context
            .reserve(RelationalRowDirectory::checkpoint_ownership_bytes())
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        // Every row gains a value, so the total is rebuilt alongside the pages.
        let mut entry_bytes = 0u128;
        for source in self.pages.iter() {
            let page_context = crate::background::CheckpointDecodeContext {
                work: work.clone(),
                memory: Default::default(),
            };
            let bytes = (source.len() / 4 + 2)
                .checked_mul(crate::cow::checkpoint_tree_node_bytes::<
                    RelationalKey,
                    RelationalRow,
                >())
                .and_then(|bytes| {
                    bytes.checked_add(RelationalRowPage::checkpoint_ownership_bytes())
                })
                .ok_or_else(|| {
                    RelationalError::Admission("schema row page capacity overflow".into())
                })?;
            page_context
                .reserve(bytes)
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            let mut rows = BTreeMap::new();
            for (key, row) in source.iter() {
                let key = key.clone_with_allocations(&page_context)?;
                let count = row.values().len().checked_add(1).ok_or_else(|| {
                    RelationalError::Admission("schema row value count overflow".into())
                })?;
                let row = RelationalRow::reconstruct_values_with_allocations(
                    count,
                    Some(allocations),
                    |ordinal| {
                        Ok(if ordinal + 1 == count {
                            value.as_ref()
                        } else {
                            row.values()[ordinal].as_ref()
                        })
                    },
                )?;
                entry_bytes += relational_row_entry_bytes(&key, &row) as u128;
                let unit = work
                    .start_unit()
                    .map_err(|error| RelationalError::Admission(error.to_string()))?;
                rows.insert(key, row);
                unit.finish();
            }
            let unit = work
                .start_unit()
                .map_err(|error| RelationalError::Admission(error.to_string()))?;
            pages.push(RelationalRowPage::from_checkpoint_value(
                rows,
                page_context.memory.into_inner(),
            ));
            unit.finish();
        }
        let unit = work
            .start_unit()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        let replacement = RelationalRowDirectory::from_checkpoint_value(
            pages,
            directory_context.memory.into_inner(),
        );
        unit.finish();
        work.checkpoint()
            .map_err(|error| RelationalError::Admission(error.to_string()))?;
        self.pages = replacement;
        self.entry_bytes = entry_bytes;
        Ok(())
    }

    pub(super) fn append_value_to_all(&mut self, value: &RelationalValue) {
        // Every row gains a value, so the total is rebuilt alongside the rows.
        let mut entry_bytes = 0u128;
        let pages = &mut *self.pages;
        for page in pages {
            for (key, row) in page.iter_mut() {
                let mut values = row.values().to_vec();
                values.push(value.clone());
                *row = RelationalRow::new(values);
                entry_bytes += relational_row_entry_bytes(key, row) as u128;
            }
        }
        self.entry_bytes = entry_bytes;
    }

    pub(super) fn insert(
        &mut self,
        key: RelationalKey,
        row: RelationalRow,
    ) -> Option<RelationalRow> {
        if self.pages.is_empty() {
            let added = relational_row_entry_bytes(&key, &row);
            self.pages = vec![crate::cow::CowSegment::from(BTreeMap::from([(key, row)]))].into();
            self.len = 1;
            self.entry_bytes = added as u128;
            return None;
        }
        let index = self
            .page_index(&key)
            .expect("non-empty relational row pages have a target page");
        let (removed, added) = Self::insert_entry_bytes(&self.pages[index], &key, &row);
        let pages = &mut *self.pages;
        let previous = pages[index].insert(key, row);
        split_relational_row_page(pages, index);
        self.account_entry(removed, Some(added));
        previous
    }

    pub(super) fn remove(&mut self, key: &RelationalKey) -> Option<RelationalRow> {
        let index = self.page_index(key)?;
        if !self.pages[index].contains_key(key) {
            return None;
        }
        let pages = &mut *self.pages;
        let (stored, removed) = pages[index].remove_entry(key)?;
        if pages[index].is_empty() {
            pages.remove(index);
        }
        self.account_entry(Some(relational_row_entry_bytes(&stored, &removed)), None);
        Some(removed)
    }

    #[cfg(test)]
    pub(super) fn page_count(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    pub(super) fn shared_page_count(&self, other: &Self) -> usize {
        self.pages
            .iter()
            .filter(|left| {
                other
                    .pages
                    .iter()
                    .any(|right| left.shares_storage_with(right))
            })
            .count()
    }
}

fn split_relational_row_page(pages: &mut Vec<RelationalRowPage>, index: usize) {
    let page = &mut *pages[index];
    let page_bytes = page
        .iter()
        .map(|(key, row)| relational_row_entry_bytes(key, row))
        .sum::<usize>();
    if page.len() <= 1
        || (page.len() <= RELATIONAL_ROW_PAGE_MAX_ENTRIES
            && page_bytes <= RELATIONAL_ROW_PAGE_TARGET_BYTES)
    {
        return;
    }
    let split_key = page
        .keys()
        .nth(page.len() / 2)
        .cloned()
        .expect("oversized relational row page is non-empty");
    let right = page.split_off(&split_key);
    pages.insert(index + 1, right.into());
}
