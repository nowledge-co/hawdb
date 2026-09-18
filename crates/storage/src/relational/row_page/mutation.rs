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

//! Deterministic copy-on-write mutation planning for relational row pages.

use super::{
    ImmutableRelationalRowPage, RelationalRowPageEntry, RelationalRowPageError,
    RelationalRowPageId, RelationalRowPagePublicationConfig, RelationalRowPagePublicationError,
    RelationalRowPageRootDescriptor, RelationalRowPageRootReader, RelationalRowPageTableDelta,
};
use crate::relational::{
    ordered_key::encode_ordered_relational_key, RelationalKey, RelationalRow, RelationalRowChange,
    RelationalRowChangeCaptureLimits,
};
use hawdb_integrity::Sha256Digest;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::{NonZeroU32, NonZeroU64};

#[derive(Debug)]
pub enum RelationalRowPageMutationError {
    Admission(String),
    Corrupt(String),
    Publication(RelationalRowPagePublicationError),
    Page(RelationalRowPageError),
}

impl fmt::Display for RelationalRowPageMutationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Admission(message) => {
                write!(
                    formatter,
                    "relational row-page mutation admission failed: {message}"
                )
            }
            Self::Corrupt(message) => {
                write!(formatter, "corrupt relational row-page mutation: {message}")
            }
            Self::Publication(error) => write!(formatter, "{error}"),
            Self::Page(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for RelationalRowPageMutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Publication(error) => Some(error),
            Self::Page(error) => Some(error),
            Self::Admission(_) | Self::Corrupt(_) => None,
        }
    }
}

impl From<RelationalRowPagePublicationError> for RelationalRowPageMutationError {
    fn from(error: RelationalRowPagePublicationError) -> Self {
        Self::Publication(error)
    }
}

impl From<RelationalRowPageError> for RelationalRowPageMutationError {
    fn from(error: RelationalRowPageError) -> Self {
        Self::Page(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalRowPageIdAllocator {
    next_page_id: NonZeroU64,
}

impl RelationalRowPageIdAllocator {
    pub const fn new(next_page_id: NonZeroU64) -> Self {
        Self { next_page_id }
    }

    pub const fn next_page_id(self) -> NonZeroU64 {
        self.next_page_id
    }

    pub fn allocate(&mut self) -> Result<RelationalRowPageId, RelationalRowPageMutationError> {
        let allocated = self.next_page_id;
        let next_page_id = allocated
            .get()
            .checked_add(1)
            .and_then(NonZeroU64::new)
            .ok_or_else(|| {
                RelationalRowPageMutationError::Admission(
                    "relational row-page id allocator is exhausted".to_string(),
                )
            })?;
        self.next_page_id = next_page_id;
        Ok(RelationalRowPageId::new(allocated))
    }

    fn peek(&self) -> RelationalRowPageId {
        RelationalRowPageId::new(self.next_page_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageMutationPlan {
    pub delta: RelationalRowPageTableDelta,
    pub distinct_changes: usize,
    pub base_pages_read: usize,
    pub dirty_pages: usize,
    pub deleted_pages: usize,
    pub split_pages: usize,
}

pub struct RelationalRowPageMutationPlanner<'a> {
    base: Option<&'a RelationalRowPageRootReader>,
    generation: u64,
    source_commit_epoch: u64,
    config: RelationalRowPagePublicationConfig,
}

impl<'a> RelationalRowPageMutationPlanner<'a> {
    pub fn new(
        base: Option<&'a RelationalRowPageRootReader>,
        generation: u64,
        source_commit_epoch: u64,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPageMutationError> {
        if generation == 0 || source_commit_epoch == 0 {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page mutation generation and epoch must be non-zero, got {generation}/{source_commit_epoch}"
            )));
        }
        if let Some(base) = base {
            if generation <= base.manifest().generation {
                return Err(RelationalRowPageMutationError::Admission(format!(
                    "row-page mutation generation {generation} does not follow base generation {}",
                    base.manifest().generation
                )));
            }
            if source_commit_epoch < base.manifest().source_commit_epoch {
                return Err(RelationalRowPageMutationError::Admission(format!(
                    "row-page mutation epoch {source_commit_epoch} precedes base epoch {}",
                    base.manifest().source_commit_epoch
                )));
            }
        }
        Ok(Self {
            base,
            generation,
            source_commit_epoch,
            config,
        })
    }

    pub fn plan_table(
        &self,
        table: &str,
        schema_digest: Sha256Digest,
        column_count: usize,
        changes: impl IntoIterator<Item = RelationalRowChange>,
    ) -> Result<RelationalRowPageMutationPlan, RelationalRowPageMutationError> {
        if table.is_empty() || table.len() > self.config.max_table_name_bytes.get() {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page mutation table name contains {} bytes, outside admitted range",
                table.len()
            )));
        }
        if column_count == 0 || column_count > self.config.page_limits.max_columns.get() {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page mutation column count {column_count} is outside admitted range"
            )));
        }

        let base_table = self.base.and_then(|base| {
            base.manifest()
                .tables
                .binary_search_by(|candidate| candidate.table.as_str().cmp(table))
                .ok()
                .map(|index| &base.manifest().tables[index])
        });
        if let Some(base_table) = base_table
            && base_table.schema_digest != schema_digest
        {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "table {table} schema digest differs from its published row root"
            )));
        }
        let mut allocator = RelationalRowPageIdAllocator::new(base_table.map_or_else(
            || NonZeroU64::new(1).expect("initial row-page id is non-zero"),
            |root| root.next_page_id,
        ));
        let changes = collect_changes(table, changes, self.config)?;
        let distinct_changes = changes.len();
        let mut groups: BTreeMap<Vec<u8>, MutationGroup> = BTreeMap::new();
        let mut bootstrap_changes = BTreeMap::new();

        if base_table.is_some_and(|root| root.page_count > 0) {
            let base = self.base.expect("base table implies a base reader");
            let mut current_descriptor: Option<RelationalRowPageRootDescriptor> = None;
            for (encoded_key, change) in changes {
                let descriptor = match current_descriptor.as_ref().filter(|descriptor| {
                    descriptor.upper_bound.as_slice() >= encoded_key.as_slice()
                }) {
                    Some(descriptor) => descriptor.clone(),
                    None => base
                        .find_table_page_descriptor(table, &change.primary_key)?
                        .ok_or_else(|| {
                            RelationalRowPageMutationError::Corrupt(format!(
                                "table {table} has a non-empty root without a target leaf"
                            ))
                        })?,
                };
                current_descriptor = Some(descriptor.clone());
                groups
                    .entry(descriptor.lower_bound.clone())
                    .or_insert_with(|| MutationGroup {
                        descriptor,
                        changes: BTreeMap::new(),
                    })
                    .changes
                    .insert(encoded_key, change);
            }
        } else {
            bootstrap_changes = changes;
        }

        let mut dirty_pages = Vec::new();
        let mut deleted_page_ids = BTreeSet::new();
        let mut base_pages_read = 0usize;
        let mut split_pages = 0usize;
        for group in groups.into_values() {
            let base = self.base.expect("mutation group implies a base reader");
            let page = base.read_page(&group.descriptor)?;
            base_pages_read = base_pages_read.checked_add(1).ok_or_else(|| {
                RelationalRowPageMutationError::Admission(
                    "row-page mutation read count overflow".to_string(),
                )
            })?;
            validate_base_page(&page, &group.descriptor, schema_digest, column_count)?;
            let original_page_id = page.page_id;
            let (rows, changed) = apply_changes(page.rows, group.changes)?;
            if !changed {
                continue;
            }
            if rows.is_empty() {
                deleted_page_ids.insert(original_page_id);
                continue;
            }
            let first = dirty_pages.len();
            self.pack_rows(
                schema_digest,
                column_count,
                Some(original_page_id),
                &mut allocator,
                rows,
                |page| {
                    admit_dirty_pages(dirty_pages.len().saturating_add(1), self.config)?;
                    dirty_pages.push(page);
                    Ok(())
                },
            )?;
            let written = dirty_pages.len() - first;
            split_pages = split_pages
                .checked_add(written.saturating_sub(1))
                .ok_or_else(|| {
                    RelationalRowPageMutationError::Admission(
                        "row-page split count overflow".to_string(),
                    )
                })?;
        }

        if !bootstrap_changes.is_empty() {
            let (rows, _) = apply_changes(Vec::new(), bootstrap_changes)?;
            self.pack_rows(
                schema_digest,
                column_count,
                None,
                &mut allocator,
                rows,
                |page| {
                    admit_dirty_pages(dirty_pages.len().saturating_add(1), self.config)?;
                    dirty_pages.push(page);
                    Ok(())
                },
            )?;
            split_pages = dirty_pages.len().saturating_sub(1);
        }

        dirty_pages.sort_by(|left, right| {
            let left = encode_ordered_relational_key(&left.rows[0].primary_key)
                .expect("planned row-page key was validated");
            let right = encode_ordered_relational_key(&right.rows[0].primary_key)
                .expect("planned row-page key was validated");
            left.cmp(&right)
        });
        let deleted_pages = deleted_page_ids.len();
        let dirty_page_count = dirty_pages.len();
        Ok(RelationalRowPageMutationPlan {
            delta: RelationalRowPageTableDelta {
                table: table.to_string(),
                schema: None,
                schema_digest,
                column_count: NonZeroU32::new(u32::try_from(column_count).map_err(|_| {
                    RelationalRowPageMutationError::Admission(format!(
                        "table {table} column count does not fit u32"
                    ))
                })?)
                .ok_or_else(|| {
                    RelationalRowPageMutationError::Admission(format!(
                        "table {table} has no columns"
                    ))
                })?,
                next_page_id: allocator.next_page_id(),
                dirty_pages,
                deleted_page_ids: deleted_page_ids.into_iter().collect(),
            },
            distinct_changes,
            base_pages_read,
            dirty_pages: dirty_page_count,
            deleted_pages,
            split_pages,
        })
    }

    fn pack_rows(
        &self,
        schema_digest: Sha256Digest,
        column_count: usize,
        first_page_id: Option<RelationalRowPageId>,
        allocator: &mut RelationalRowPageIdAllocator,
        rows: Vec<RelationalRowPageEntry>,
        mut emit: impl FnMut(ImmutableRelationalRowPage) -> Result<(), RelationalRowPageMutationError>,
    ) -> Result<(), RelationalRowPageMutationError> {
        let mut state = PagePackState::new(
            self.generation,
            self.source_commit_epoch,
            schema_digest,
            column_count,
            first_page_id,
            self.config,
        );
        for row in rows {
            state.push(row, allocator, &mut emit)?;
        }
        state.finish(allocator, emit)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationalRowPageBootstrapReport {
    pub rows: usize,
    pub pages: usize,
    pub peak_buffered_rows: usize,
    pub next_page_id: NonZeroU64,
}

pub struct RelationalRowPageBootstrap {
    state: PagePackState,
    allocator: RelationalRowPageIdAllocator,
    failed: bool,
}

impl RelationalRowPageBootstrap {
    pub fn new(
        generation: u64,
        source_commit_epoch: u64,
        schema_digest: Sha256Digest,
        column_count: usize,
        next_page_id: NonZeroU64,
        config: RelationalRowPagePublicationConfig,
    ) -> Result<Self, RelationalRowPageMutationError> {
        if generation == 0 || source_commit_epoch == 0 {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page bootstrap generation and epoch must be non-zero, got {generation}/{source_commit_epoch}"
            )));
        }
        if column_count == 0 || column_count > config.page_limits.max_columns.get() {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page bootstrap column count {column_count} is outside admitted range"
            )));
        }
        Ok(Self {
            state: PagePackState::new(
                generation,
                source_commit_epoch,
                schema_digest,
                column_count,
                None,
                config,
            ),
            allocator: RelationalRowPageIdAllocator::new(next_page_id),
            failed: false,
        })
    }

    pub fn push(
        &mut self,
        entry: RelationalRowPageEntry,
        emit: impl FnMut(ImmutableRelationalRowPage) -> Result<(), RelationalRowPageMutationError>,
    ) -> Result<(), RelationalRowPageMutationError> {
        if self.failed {
            return Err(RelationalRowPageMutationError::Admission(
                "row-page bootstrap cannot continue after a previous failure".to_string(),
            ));
        }
        self.state
            .push(entry, &mut self.allocator, emit)
            .inspect_err(|_| self.failed = true)
    }

    pub fn finish(
        mut self,
        emit: impl FnMut(ImmutableRelationalRowPage) -> Result<(), RelationalRowPageMutationError>,
    ) -> Result<RelationalRowPageBootstrapReport, RelationalRowPageMutationError> {
        if self.failed {
            return Err(RelationalRowPageMutationError::Admission(
                "row-page bootstrap cannot finish after a previous failure".to_string(),
            ));
        }
        self.state.finish(&mut self.allocator, emit)?;
        Ok(RelationalRowPageBootstrapReport {
            rows: self.state.emitted_rows,
            pages: self.state.emitted_pages,
            peak_buffered_rows: self.state.peak_buffered_rows,
            next_page_id: self.allocator.next_page_id(),
        })
    }
}

#[derive(Debug)]
struct PlannedChange {
    primary_key: RelationalKey,
    row: Option<RelationalRow>,
    charged_bytes: usize,
}

struct MutationGroup {
    descriptor: RelationalRowPageRootDescriptor,
    changes: BTreeMap<Vec<u8>, PlannedChange>,
}

fn collect_changes(
    table: &str,
    changes: impl IntoIterator<Item = RelationalRowChange>,
    config: RelationalRowPagePublicationConfig,
) -> Result<BTreeMap<Vec<u8>, PlannedChange>, RelationalRowPageMutationError> {
    collect_changes_with_limits(
        table,
        changes,
        config,
        RelationalRowChangeCaptureLimits::default(),
    )
}

fn collect_changes_with_limits(
    table: &str,
    changes: impl IntoIterator<Item = RelationalRowChange>,
    config: RelationalRowPagePublicationConfig,
    limits: RelationalRowChangeCaptureLimits,
) -> Result<BTreeMap<Vec<u8>, PlannedChange>, RelationalRowPageMutationError> {
    let mut collected: BTreeMap<Vec<u8>, PlannedChange> = BTreeMap::new();
    let mut charged_bytes = 0usize;
    for change in changes {
        if change.table != table {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page mutation for table {table} contains change for {}",
                change.table
            )));
        }
        let encoded_key = encode_ordered_relational_key(&change.primary_key).map_err(|error| {
            RelationalRowPageMutationError::Admission(format!(
                "row-page mutation primary key cannot be encoded: {error}"
            ))
        })?;
        if encoded_key.len() > config.page_limits.max_key_bytes.get() {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page mutation primary key contains {} bytes, exceeding limit {}",
                encoded_key.len(),
                config.page_limits.max_key_bytes
            )));
        }
        if !collected.contains_key(&encoded_key) && collected.len() >= limits.max_entries.get() {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page mutation exceeds change limit {}",
                limits.max_entries
            )));
        }
        let change_bytes = crate::relational::estimated_row_change_encoding_bytes(&change)
            .ok_or_else(|| {
                RelationalRowPageMutationError::Admission(
                    "row-page mutation change-byte accounting overflow".to_string(),
                )
            })?;
        let replaced_bytes = collected
            .get(&encoded_key)
            .map_or(0, |existing| existing.charged_bytes);
        let next_bytes = charged_bytes
            .checked_sub(replaced_bytes)
            .and_then(|bytes| bytes.checked_add(change_bytes))
            .ok_or_else(|| {
                RelationalRowPageMutationError::Admission(
                    "row-page mutation change-byte accounting overflow".to_string(),
                )
            })?;
        if next_bytes > limits.max_bytes.get() {
            return Err(RelationalRowPageMutationError::Admission(format!(
                "row-page mutation changes use {next_bytes} bytes, exceeding limit {}",
                limits.max_bytes
            )));
        }
        collected.insert(
            encoded_key,
            PlannedChange {
                primary_key: change.primary_key,
                row: change.row,
                charged_bytes: change_bytes,
            },
        );
        charged_bytes = next_bytes;
    }
    Ok(collected)
}

fn apply_changes(
    rows: Vec<RelationalRowPageEntry>,
    changes: BTreeMap<Vec<u8>, PlannedChange>,
) -> Result<(Vec<RelationalRowPageEntry>, bool), RelationalRowPageMutationError> {
    let mut merged = BTreeMap::new();
    for entry in rows {
        let encoded_key = encode_ordered_relational_key(&entry.primary_key).map_err(|error| {
            RelationalRowPageMutationError::Corrupt(format!(
                "published row-page primary key cannot be encoded: {error}"
            ))
        })?;
        if merged.insert(encoded_key, entry).is_some() {
            return Err(RelationalRowPageMutationError::Corrupt(
                "published row page contains duplicate primary keys".to_string(),
            ));
        }
    }
    let mut changed = false;
    for (encoded_key, change) in changes {
        match change.row {
            Some(row) => {
                let entry = RelationalRowPageEntry {
                    primary_key: change.primary_key,
                    row,
                };
                changed |= merged.get(&encoded_key) != Some(&entry);
                merged.insert(encoded_key, entry);
            }
            None => {
                changed |= merged.remove(&encoded_key).is_some();
            }
        }
    }
    Ok((merged.into_values().collect(), changed))
}

fn validate_base_page(
    page: &ImmutableRelationalRowPage,
    descriptor: &RelationalRowPageRootDescriptor,
    schema_digest: Sha256Digest,
    column_count: usize,
) -> Result<(), RelationalRowPageMutationError> {
    if page.page_id != descriptor.logical_page_id
        || page.schema_digest != schema_digest
        || page.column_count != column_count
    {
        return Err(RelationalRowPageMutationError::Corrupt(format!(
            "row-page {} does not match its table mutation contract",
            descriptor.logical_page_id.get()
        )));
    }
    Ok(())
}

fn admit_dirty_pages(
    pages: usize,
    config: RelationalRowPagePublicationConfig,
) -> Result<(), RelationalRowPageMutationError> {
    if pages > config.max_dirty_pages.get() {
        return Err(RelationalRowPageMutationError::Admission(format!(
            "row-page mutation emits {pages} dirty pages, exceeding limit {}",
            config.max_dirty_pages
        )));
    }
    let bytes = (pages as u64)
        .checked_mul(config.page_limits.max_page_bytes.get() as u64)
        .ok_or_else(|| {
            RelationalRowPageMutationError::Admission(
                "row-page mutation dirty-byte reservation overflow".to_string(),
            )
        })?;
    if bytes > config.max_dirty_bytes.get() {
        return Err(RelationalRowPageMutationError::Admission(format!(
            "row-page mutation reserves {bytes} dirty bytes, exceeding limit {}",
            config.max_dirty_bytes
        )));
    }
    Ok(())
}

struct PagePackState {
    generation: u64,
    source_commit_epoch: u64,
    schema_digest: Sha256Digest,
    column_count: usize,
    first_page_id: Option<RelationalRowPageId>,
    config: RelationalRowPagePublicationConfig,
    pending: Option<ImmutableRelationalRowPage>,
    previous_key: Option<Vec<u8>>,
    emitted_rows: usize,
    emitted_pages: usize,
    peak_buffered_rows: usize,
}

impl PagePackState {
    fn new(
        generation: u64,
        source_commit_epoch: u64,
        schema_digest: Sha256Digest,
        column_count: usize,
        first_page_id: Option<RelationalRowPageId>,
        config: RelationalRowPagePublicationConfig,
    ) -> Self {
        Self {
            generation,
            source_commit_epoch,
            schema_digest,
            column_count,
            first_page_id,
            config,
            pending: None,
            previous_key: None,
            emitted_rows: 0,
            emitted_pages: 0,
            peak_buffered_rows: 0,
        }
    }

    fn push(
        &mut self,
        entry: RelationalRowPageEntry,
        allocator: &mut RelationalRowPageIdAllocator,
        mut emit: impl FnMut(ImmutableRelationalRowPage) -> Result<(), RelationalRowPageMutationError>,
    ) -> Result<(), RelationalRowPageMutationError> {
        let encoded_key = encode_ordered_relational_key(&entry.primary_key).map_err(|error| {
            RelationalRowPageMutationError::Admission(format!(
                "row-page pack key cannot be encoded: {error}"
            ))
        })?;
        if self
            .previous_key
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= encoded_key.as_slice())
        {
            return Err(RelationalRowPageMutationError::Admission(
                "row-page pack input is not strictly ordered".to_string(),
            ));
        }
        if self.pending.is_none() {
            self.pending = Some(self.empty_page(self.peek_page_id(allocator)));
        }
        let pending = self.pending.as_mut().expect("pending page was initialized");
        pending.rows.push(entry);
        self.peak_buffered_rows = self.peak_buffered_rows.max(pending.rows.len());
        match pending.encode(self.config.page_limits) {
            Ok(_) => {
                self.previous_key = Some(encoded_key);
                Ok(())
            }
            Err(error) => {
                let entry = pending
                    .rows
                    .pop()
                    .expect("candidate page contains the pushed row");
                if pending.rows.is_empty() {
                    return Err(error.into());
                }
                pending.encode(self.config.page_limits)?;
                self.emit_pending(allocator, &mut emit)?;
                let mut next = self.empty_page(self.peek_page_id(allocator));
                next.rows.push(entry);
                next.encode(self.config.page_limits)?;
                self.pending = Some(next);
                self.previous_key = Some(encoded_key);
                Ok(())
            }
        }
    }

    fn finish(
        &mut self,
        allocator: &mut RelationalRowPageIdAllocator,
        emit: impl FnMut(ImmutableRelationalRowPage) -> Result<(), RelationalRowPageMutationError>,
    ) -> Result<(), RelationalRowPageMutationError> {
        if self.pending.is_some() {
            self.emit_pending(allocator, emit)?;
        }
        Ok(())
    }

    fn emit_pending(
        &mut self,
        allocator: &mut RelationalRowPageIdAllocator,
        mut emit: impl FnMut(ImmutableRelationalRowPage) -> Result<(), RelationalRowPageMutationError>,
    ) -> Result<(), RelationalRowPageMutationError> {
        let page = self.pending.take().expect("pending page exists");
        let mut next_allocator = *allocator;
        let mut next_first_page_id = self.first_page_id;
        let expected_page_id = next_first_page_id
            .take()
            .map_or_else(|| next_allocator.allocate(), Ok)?;
        if page.page_id != expected_page_id {
            return Err(RelationalRowPageMutationError::Corrupt(
                "row-page allocator changed while packing a page".to_string(),
            ));
        }
        let row_count = page.rows.len();
        emit(page)?;
        *allocator = next_allocator;
        self.first_page_id = next_first_page_id;
        self.emitted_pages = self.emitted_pages.checked_add(1).ok_or_else(|| {
            RelationalRowPageMutationError::Admission(
                "row-page bootstrap page count overflow".to_string(),
            )
        })?;
        self.emitted_rows = self.emitted_rows.checked_add(row_count).ok_or_else(|| {
            RelationalRowPageMutationError::Admission(
                "row-page bootstrap row count overflow".to_string(),
            )
        })?;
        admit_dirty_pages(self.emitted_pages, self.config)
    }

    fn peek_page_id(&self, allocator: &RelationalRowPageIdAllocator) -> RelationalRowPageId {
        self.first_page_id.unwrap_or_else(|| allocator.peek())
    }

    fn empty_page(&self, page_id: RelationalRowPageId) -> ImmutableRelationalRowPage {
        ImmutableRelationalRowPage {
            generation: self.generation,
            source_commit_epoch: self.source_commit_epoch,
            page_id,
            schema_digest: self.schema_digest,
            column_count: self.column_count,
            rows: Vec::new(),
        }
    }
}

#[cfg(test)]
#[path = "mutation/tests.rs"]
mod tests;
