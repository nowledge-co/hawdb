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

use super::super::{RelationalIndexRangeScan, RelationalIndexScanDirection};
use super::{
    encode_relational_key, index_leaf_posting_rows, IndexLeafPosting, IndexPageId,
    RelationalIndexRootDescriptor, RelationalIndexShadowError, RelationalIndexShadowReader,
};
use crate::index_page::{ImmutableIndexPage, ImmutableIndexPageBody, IndexPostingPage};
use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroUsize};

#[cfg(test)]
mod posting_count_tests;

pub const DEFAULT_RELATIONAL_INDEX_READ_PAGES: usize = 256;
pub const DEFAULT_RELATIONAL_INDEX_READ_ROWS: usize = 4096;
pub const DEFAULT_RELATIONAL_INDEX_READ_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexReadLimits {
    /// Maximum logical index pages traversed, including cache hits.
    pub max_pages: NonZeroUsize,
    /// Maximum index rows visited.
    pub max_rows: NonZeroUsize,
    /// Maximum logical index bytes traversed, including cache hits.
    pub max_bytes: NonZeroUsize,
    /// Maximum bytes fetched from index files. A zero budget permits cache hits only.
    pub max_file_bytes: usize,
    pub max_tree_height: NonZeroU32,
}

impl Default for RelationalIndexReadLimits {
    fn default() -> Self {
        Self {
            max_pages: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_READ_PAGES)
                .expect("default relational index page limit is non-zero"),
            max_rows: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_READ_ROWS)
                .expect("default relational index row limit is non-zero"),
            max_bytes: NonZeroUsize::new(DEFAULT_RELATIONAL_INDEX_READ_BYTES)
                .expect("default relational index byte limit is non-zero"),
            max_file_bytes: DEFAULT_RELATIONAL_INDEX_READ_BYTES,
            max_tree_height: NonZeroU32::new(DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT)
                .expect("default relational index tree-height limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalIndexReadReport {
    pub pages_read: usize,
    pub bytes_read: usize,
    /// Selected page slots fetched, excluding whole-object handle validation.
    pub file_pages_read: usize,
    /// All payload bytes fetched, including cold immutable handle validation.
    pub file_bytes_read: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub leaf_entries_visited: usize,
    pub matched_index_keys: usize,
    pub rows_visited: usize,
    pub stopped_early: bool,
}

/// Per-operation admission shared by nested reads; reports remain per-read.
pub(crate) trait IndexReadObserver {
    fn check_charge(&self, charge: IndexReadCharge) -> Result<(), IndexReadPreflightError>;
    fn set_budget_refusal(&self, refused: bool);
    fn charge(&self, charge: IndexReadCharge) -> Result<(), RelationalIndexShadowError>;
    fn file_budget(&self, requested: usize) -> Result<usize, RelationalIndexShadowError>;
}

/// Only owner-budget preflight can certify refusal before an operation begins.
/// Closed owners, cancellation and accounting failures retain their own cause.
pub(crate) enum IndexReadPreflightError {
    Budget,
    Failed(RelationalIndexShadowError),
}

impl IndexReadPreflightError {
    pub(crate) fn into_error(self) -> RelationalIndexShadowError {
        match self {
            Self::Budget => RelationalIndexShadowError::Admission(
                "authoritative relational index reader exceeded its transaction budget".to_string(),
            ),
            Self::Failed(error) => error,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum IndexReadCharge {
    Page(usize),
    FileBytes(usize),
    LiveBytes(usize),
    Row,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct IndexReadAdmission<'a> {
    observer: Option<&'a dyn IndexReadObserver>,
    charge_rows: bool,
}

impl<'a> IndexReadAdmission<'a> {
    pub(crate) fn new(observer: &'a dyn IndexReadObserver) -> Self {
        Self {
            observer: Some(observer),
            charge_rows: true,
        }
    }

    pub(crate) fn without_rows(self) -> Self {
        Self {
            charge_rows: false,
            ..self
        }
    }

    pub(crate) fn charge(self, charge: IndexReadCharge) -> Result<(), RelationalIndexShadowError> {
        if matches!(charge, IndexReadCharge::Row) && !self.charge_rows {
            return Ok(());
        }
        self.set_budget_refusal(false);
        self.observer
            .map_or(Ok(()), |observer| observer.charge(charge))
    }

    pub(crate) fn check_charge(
        self,
        charge: IndexReadCharge,
    ) -> Result<(), IndexReadPreflightError> {
        if matches!(charge, IndexReadCharge::Row) && !self.charge_rows {
            return Ok(());
        }
        self.observer
            .map_or(Ok(()), |observer| observer.check_charge(charge))
    }

    pub(crate) fn set_budget_refusal(self, refused: bool) {
        if let Some(observer) = self.observer {
            observer.set_budget_refusal(refused);
        }
    }

    /// Check true owners before native positive envelopes can mask their cause.
    pub(crate) fn preflight_charge(
        self,
        charge: IndexReadCharge,
    ) -> Result<(), RelationalIndexShadowError> {
        self.set_budget_refusal(false);
        self.check_charge(charge).map_err(|error| {
            self.set_budget_refusal(matches!(error, IndexReadPreflightError::Budget));
            error.into_error()
        })
    }

    pub(crate) fn file_budget(self, requested: usize) -> Result<usize, RelationalIndexShadowError> {
        self.observer
            .map_or(Ok(requested), |observer| observer.file_budget(requested))
    }
}

impl RelationalIndexShadowReader {
    /// Returns the declared posting count for one complete index key and the
    /// actual metadata read report. Logical traversal and decoding visit only
    /// the checked root-to-leaf path, without decoding posting-chain pages or
    /// visiting canonical rows. Inline locator bytes are decoded as part of the
    /// leaf page, without interpreting them as relational keys.
    /// Cold or evicted mounted handles additionally validate the whole immutable
    /// object; these physical bytes are admitted and included in the report.
    ///
    /// Page, logical-byte, file-byte and tree-height limits still apply, even
    /// for cache hits. The count may exceed the row-visit limit because no rows
    /// are visited. Partial keys are rejected. A count does not verify undecoded
    /// posting-chain structure; known reader poison is rejected.
    /// Callers must share these reads with their statement accounting and
    /// cancellation rather than treating each estimate as a new read budget.
    pub fn count_exact_postings(
        &self,
        table: &str,
        index: &str,
        key: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
    ) -> Result<(u64, RelationalIndexReadReport), RelationalIndexShadowError> {
        self.count_exact_postings_admitted(table, index, key, limits, IndexReadAdmission::default())
    }

    pub(crate) fn count_exact_postings_admitted(
        &self,
        table: &str,
        index: &str,
        key: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        read_admission: IndexReadAdmission<'_>,
    ) -> Result<(u64, RelationalIndexReadReport), RelationalIndexShadowError> {
        self.check_not_poisoned()?;
        let descriptor = self.root_descriptor(table, index)?;
        let width = descriptor.statistics.leading_prefixes.len();
        if width == 0 || key.0.len() != width {
            return Err(RelationalIndexShadowError::Admission(format!(
                "index posting count requires {width} complete key components, received {}",
                key.0.len()
            )));
        }
        let encoded = self.encode_lookup_key(key)?;
        let mut context = ReadContext::new_admitted(self, limits, read_admission);
        let rows = context
            .find_exact_posting(descriptor, &encoded)?
            .as_ref()
            .map(index_leaf_posting_rows)
            .transpose()?
            .unwrap_or(0);
        Ok((rows, context.report))
    }

    /// Visits the postings for one complete encoded index key.
    ///
    /// Values observed by `visit` are provisional until this method returns
    /// `Ok`; callers must discard them when traversal fails. Returning
    /// `false` stops further logical posting-chain traversal. Cold mounted handle
    /// validation may already have fetched those bytes under the file budget.
    pub fn visit_exact_postings(
        &self,
        table: &str,
        index: &str,
        key: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        self.visit_exact_postings_admitted(
            table,
            index,
            key,
            limits,
            visit,
            IndexReadAdmission::default(),
        )
    }

    pub(crate) fn visit_exact_postings_admitted(
        &self,
        table: &str,
        index: &str,
        key: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&super::RelationalKey) -> bool,
        read_admission: IndexReadAdmission<'_>,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        let descriptor = self.root_descriptor(table, index)?.clone();
        let encoded = self.encode_lookup_key(key)?;
        let mut context = ReadContext::new_admitted(self, limits, read_admission);
        if let Some(posting) = context.find_exact_posting(&descriptor, &encoded)? {
            let outcome = context.visit_posting(&posting, &mut visit)?;
            context.report.stopped_early = outcome == VisitOutcome::Stopped;
        }
        Ok(context.report)
    }

    /// Visits postings whose complete index key has the supplied leading
    /// relational-key prefix. Traversal decodes root-to-leaf paths and selected
    /// posting pages, leaving unrelated subtrees undecoded. Cold mounted handles
    /// additionally validate the whole immutable object under the file budget;
    /// the report includes those physical bytes.
    ///
    /// Values observed by `visit` are provisional until this method returns
    /// `Ok`; callers must discard them on error.
    pub fn visit_prefix_postings(
        &self,
        table: &str,
        index: &str,
        prefix: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        self.visit_prefix_postings_admitted(
            table,
            index,
            prefix,
            limits,
            visit,
            IndexReadAdmission::default(),
        )
    }

    pub(crate) fn visit_prefix_postings_admitted(
        &self,
        table: &str,
        index: &str,
        prefix: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&super::RelationalKey) -> bool,
        read_admission: IndexReadAdmission<'_>,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        self.visit_prefix_entries_admitted(
            table,
            index,
            prefix,
            limits,
            |_, primary_key| visit(primary_key),
            read_admission,
        )
    }

    /// Visits ordered `(index_key, primary_key)` entries whose complete index
    /// key has the supplied leading relational-key prefix.
    ///
    /// Values observed by `visit` are provisional until this method returns
    /// `Ok`; callers must discard them on error.
    pub fn visit_prefix_entries(
        &self,
        table: &str,
        index: &str,
        prefix: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        self.visit_prefix_entries_admitted(
            table,
            index,
            prefix,
            limits,
            visit,
            IndexReadAdmission::default(),
        )
    }

    pub(crate) fn visit_prefix_entries_admitted(
        &self,
        table: &str,
        index: &str,
        prefix: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
        read_admission: IndexReadAdmission<'_>,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        let descriptor = self.root_descriptor(table, index)?.clone();
        let encoded = self.encode_lookup_key(prefix)?;
        let mut context = ReadContext::new_admitted(self, limits, read_admission);
        let root = context.read_root(&descriptor)?;
        let outcome =
            context.visit_prefix_subtree(root.child, root.height, None, &encoded, &mut visit)?;
        context.report.stopped_early = outcome == VisitOutcome::Stopped;
        Ok(context.report)
    }

    /// Visits entries for a deduplicated set of equally wide index-key
    /// prefixes under one immutable root read, cache scope, and read budget.
    ///
    /// Values observed by `visit` are provisional until this method returns
    /// `Ok`; callers must discard them on error. The first callback argument
    /// identifies the requested prefix that selected the entry.
    pub fn visit_prefix_entries_many(
        &self,
        table: &str,
        index: &str,
        prefixes: &[super::RelationalKey],
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&super::RelationalKey, &super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        self.visit_prefix_entries_many_admitted(
            table,
            index,
            prefixes,
            limits,
            visit,
            IndexReadAdmission::default(),
        )
    }

    pub(crate) fn visit_prefix_entries_many_admitted(
        &self,
        table: &str,
        index: &str,
        prefixes: &[super::RelationalKey],
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(
            &super::RelationalKey,
            &super::RelationalKey,
            &super::RelationalKey,
        ) -> bool,
        read_admission: IndexReadAdmission<'_>,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        let mut encoded_prefixes = BTreeMap::new();
        let mut prefix_width = None;
        for prefix in prefixes {
            if let Some(width) = prefix_width {
                if width != prefix.0.len() {
                    return Err(RelationalIndexShadowError::Admission(
                        "batch index prefixes must have one common key width".to_string(),
                    ));
                }
            } else {
                prefix_width = Some(prefix.0.len());
            }
            encoded_prefixes
                .entry(self.encode_lookup_key(prefix)?)
                .or_insert_with(|| prefix.clone());
        }
        if encoded_prefixes.is_empty() {
            return Ok(RelationalIndexReadReport::default());
        }

        let descriptor = self.root_descriptor(table, index)?.clone();
        let mut context = ReadContext::new_admitted(self, limits, read_admission);
        let root = context.read_root(&descriptor)?;
        for (encoded_prefix, prefix) in encoded_prefixes {
            let outcome = context.visit_prefix_subtree(
                root.child,
                root.height,
                None,
                &encoded_prefix,
                &mut |index_key, primary_key| visit(&prefix, index_key, primary_key),
            )?;
            if outcome == VisitOutcome::Stopped {
                context.report.stopped_early = true;
                break;
            }
        }
        Ok(context.report)
    }

    /// Visits an ordered exclusive range within one leading index-key prefix.
    pub fn visit_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        visit: impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        self.visit_range_entries_admitted(
            table,
            index,
            scan,
            limits,
            visit,
            IndexReadAdmission::default(),
        )
    }

    pub(crate) fn visit_range_entries_admitted(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
        read_admission: IndexReadAdmission<'_>,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        let descriptor = self.root_descriptor(table, index)?.clone();
        let encoded_prefix = self.encode_lookup_key(&scan.prefix)?;
        let encoded_bound = scan
            .exclusive_bound
            .as_ref()
            .map(|bound| self.encode_lookup_key(bound))
            .transpose()?;
        let mut context = ReadContext::new_admitted(self, limits, read_admission);
        let root = context.read_root(&descriptor)?;
        let outcome = match scan.direction {
            RelationalIndexScanDirection::Forward => context.visit_forward_range_subtree(
                root.child,
                root.height,
                None,
                &encoded_prefix,
                encoded_bound.as_deref(),
                &mut visit,
            )?,
            RelationalIndexScanDirection::Backward => {
                let prefix_successor;
                let upper = match encoded_bound.as_deref() {
                    Some(bound) => bound,
                    None => {
                        prefix_successor =
                            encoded_prefix_successor(&encoded_prefix).ok_or_else(|| {
                                RelationalIndexShadowError::Admission(
                                    "relational index prefix has no finite exclusive upper bound"
                                        .to_string(),
                                )
                            })?;
                        &prefix_successor
                    }
                };
                context.visit_backward_range_subtree(
                    root.child,
                    root.height,
                    None,
                    &encoded_prefix,
                    upper,
                    &mut visit,
                )?
            }
        };
        context.report.stopped_early = outcome == VisitOutcome::Stopped;
        Ok(context.report)
    }

    fn root_descriptor(
        &self,
        table: &str,
        index: &str,
    ) -> Result<&RelationalIndexRootDescriptor, RelationalIndexShadowError> {
        self.manifest()
            .root(table, index)
            .ok_or_else(|| RelationalIndexShadowError::MissingIndex {
                table: table.to_string(),
                index: index.to_string(),
            })
    }

    fn encode_lookup_key(
        &self,
        key: &super::RelationalKey,
    ) -> Result<Vec<u8>, RelationalIndexShadowError> {
        let encoded = encode_relational_key(key).map_err(|error| match error {
            RelationalIndexShadowError::Corrupt(message) => RelationalIndexShadowError::Admission(
                format!("relational index lookup key is not encodable: {message}"),
            ),
            error => error,
        })?;
        if encoded.len() > self.config.page_limits.max_key_bytes.get() {
            return Err(RelationalIndexShadowError::Admission(format!(
                "relational index lookup key contains {} encoded bytes, exceeding limit {}",
                encoded.len(),
                self.config.page_limits.max_key_bytes
            )));
        }
        Ok(encoded)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VisitOutcome {
    Continue,
    PastPrefix,
    Stopped,
}

struct ReadContext<'a> {
    reader: &'a RelationalIndexShadowReader,
    read_admission: IndexReadAdmission<'a>,
    limits: RelationalIndexReadLimits,
    report: RelationalIndexReadReport,
}

impl<'a> ReadContext<'a> {
    #[cfg(test)]
    const fn new(
        reader: &'a RelationalIndexShadowReader,
        limits: RelationalIndexReadLimits,
    ) -> Self {
        Self::new_admitted(
            reader,
            limits,
            IndexReadAdmission {
                observer: None,
                charge_rows: false,
            },
        )
    }

    const fn new_admitted(
        reader: &'a RelationalIndexShadowReader,
        limits: RelationalIndexReadLimits,
        read_admission: IndexReadAdmission<'a>,
    ) -> Self {
        Self {
            reader,
            read_admission,
            limits,
            report: RelationalIndexReadReport {
                pages_read: 0,
                bytes_read: 0,
                file_pages_read: 0,
                file_bytes_read: 0,
                cache_hits: 0,
                cache_misses: 0,
                cache_admission_rejections: 0,
                leaf_entries_visited: 0,
                matched_index_keys: 0,
                rows_visited: 0,
                stopped_early: false,
            },
        }
    }

    fn find_exact_posting(
        &mut self,
        descriptor: &RelationalIndexRootDescriptor,
        encoded: &[u8],
    ) -> Result<Option<IndexLeafPosting>, RelationalIndexShadowError> {
        let root = self.read_root(descriptor)?;
        let Some(leaf) = self.find_leaf(root.child, root.height, encoded)? else {
            return Ok(None);
        };
        let ImmutableIndexPageBody::Leaf(mut leaf) = leaf.body else {
            return Err(self.corrupt("index traversal ended on a non-leaf page"));
        };
        let mut comparisons = 0usize;
        let position = leaf.entries.binary_search_by(|entry| {
            comparisons = comparisons.saturating_add(1);
            entry.key.as_slice().cmp(encoded)
        });
        self.report.leaf_entries_visited = self
            .report
            .leaf_entries_visited
            .checked_add(comparisons)
            .ok_or_else(|| self.admission("leaf-entry counter overflow"))?;
        Ok(position.ok().map(|position| {
            self.report.matched_index_keys = 1;
            leaf.entries.swap_remove(position).posting
        }))
    }

    fn read_root(
        &mut self,
        descriptor: &RelationalIndexRootDescriptor,
    ) -> Result<crate::index_page::IndexRootPage, RelationalIndexShadowError> {
        if descriptor.height > self.limits.max_tree_height.get() {
            return Err(self.admission(format!(
                "index tree height {} exceeds read limit {}",
                descriptor.height, self.limits.max_tree_height
            )));
        }
        let page = self.read_page(descriptor.root_page_id)?;
        let ImmutableIndexPageBody::Root(root) = page.body else {
            return Err(self.corrupt("manifest root descriptor references a non-root page"));
        };
        if root.identity != descriptor.identity
            || root.schema_digest != descriptor.schema_digest
            || root.height != descriptor.height
        {
            return Err(self.corrupt("root page disagrees with its manifest descriptor"));
        }
        Ok(root)
    }

    fn find_leaf(
        &mut self,
        mut page_id: IndexPageId,
        mut height: u32,
        key: &[u8],
    ) -> Result<Option<ImmutableIndexPage>, RelationalIndexShadowError> {
        let mut expected_upper_bound: Option<Vec<u8>> = None;
        while height > 1 {
            let page = self.read_page(page_id)?;
            if let Some(expected) = &expected_upper_bound {
                self.validate_page_upper_bound(&page, expected)?;
            }
            let ImmutableIndexPageBody::Interior(interior) = page.body else {
                return Err(self.corrupt("index traversal expected an interior page"));
            };
            let position = interior
                .entries
                .partition_point(|entry| entry.upper_bound.as_slice() < key);
            let Some(entry) = interior.entries.get(position) else {
                return Ok(None);
            };
            page_id = entry.child;
            expected_upper_bound = Some(entry.upper_bound.clone());
            height -= 1;
        }
        let page = self.read_page(page_id)?;
        if let Some(expected) = &expected_upper_bound {
            self.validate_page_upper_bound(&page, expected)?;
        }
        Ok(Some(page))
    }

    fn visit_prefix_subtree(
        &mut self,
        page_id: IndexPageId,
        height: u32,
        expected_upper_bound: Option<&[u8]>,
        prefix: &[u8],
        visit: &mut impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        let page = self.read_page(page_id)?;
        if let Some(expected) = expected_upper_bound {
            self.validate_page_upper_bound(&page, expected)?;
        }
        if height == 1 {
            let ImmutableIndexPageBody::Leaf(leaf) = page.body else {
                return Err(self.corrupt("index traversal expected a leaf page"));
            };
            return self.visit_leaf_prefix(leaf.entries, prefix, visit);
        }
        let ImmutableIndexPageBody::Interior(interior) = page.body else {
            return Err(self.corrupt("index traversal expected an interior page"));
        };
        let start = interior
            .entries
            .partition_point(|entry| entry.upper_bound.as_slice() < prefix);
        for entry in interior.entries.into_iter().skip(start) {
            match self.visit_prefix_subtree(
                entry.child,
                height - 1,
                Some(&entry.upper_bound),
                prefix,
                visit,
            )? {
                VisitOutcome::Continue => {}
                outcome => return Ok(outcome),
            }
        }
        Ok(VisitOutcome::Continue)
    }

    fn visit_forward_range_subtree(
        &mut self,
        page_id: IndexPageId,
        height: u32,
        expected_upper_bound: Option<&[u8]>,
        prefix: &[u8],
        exclusive_bound: Option<&[u8]>,
        visit: &mut impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        let page = self.read_page(page_id)?;
        if let Some(expected) = expected_upper_bound {
            self.validate_page_upper_bound(&page, expected)?;
        }
        if height == 1 {
            let ImmutableIndexPageBody::Leaf(leaf) = page.body else {
                return Err(self.corrupt("index traversal expected a leaf page"));
            };
            let start = match exclusive_bound {
                Some(bound) => leaf
                    .entries
                    .partition_point(|entry| entry.key.as_slice() <= bound),
                None => leaf
                    .entries
                    .partition_point(|entry| entry.key.as_slice() < prefix),
            };
            return self.visit_forward_range_leaf(leaf.entries, start, prefix, visit);
        }
        let ImmutableIndexPageBody::Interior(interior) = page.body else {
            return Err(self.corrupt("index traversal expected an interior page"));
        };
        let start = match exclusive_bound {
            Some(bound) => interior
                .entries
                .partition_point(|entry| entry.upper_bound.as_slice() <= bound),
            None => interior
                .entries
                .partition_point(|entry| entry.upper_bound.as_slice() < prefix),
        };
        for entry in interior.entries.into_iter().skip(start) {
            match self.visit_forward_range_subtree(
                entry.child,
                height - 1,
                Some(&entry.upper_bound),
                prefix,
                exclusive_bound,
                visit,
            )? {
                VisitOutcome::Continue => {}
                outcome => return Ok(outcome),
            }
        }
        Ok(VisitOutcome::Continue)
    }

    fn visit_forward_range_leaf(
        &mut self,
        entries: Vec<crate::index_page::IndexLeafEntry>,
        start: usize,
        prefix: &[u8],
        visit: &mut impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        for entry in entries.into_iter().skip(start) {
            self.report.leaf_entries_visited = self
                .report
                .leaf_entries_visited
                .checked_add(1)
                .ok_or_else(|| self.admission("leaf-entry counter overflow"))?;
            if !entry.key.starts_with(prefix) {
                return Ok(VisitOutcome::PastPrefix);
            }
            self.report.matched_index_keys = self
                .report
                .matched_index_keys
                .checked_add(1)
                .ok_or_else(|| self.admission("matched-key counter overflow"))?;
            let index_key = decode_relational_key(&entry.key).inspect_err(|_| {
                self.reader.poison();
            })?;
            if self.visit_ordered_posting(&index_key, &entry.posting, visit)?
                == VisitOutcome::Stopped
            {
                return Ok(VisitOutcome::Stopped);
            }
        }
        Ok(VisitOutcome::Continue)
    }

    fn visit_backward_range_subtree(
        &mut self,
        page_id: IndexPageId,
        height: u32,
        expected_upper_bound: Option<&[u8]>,
        prefix: &[u8],
        exclusive_upper: &[u8],
        visit: &mut impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        let page = self.read_page(page_id)?;
        if let Some(expected) = expected_upper_bound {
            self.validate_page_upper_bound(&page, expected)?;
        }
        if height == 1 {
            let ImmutableIndexPageBody::Leaf(leaf) = page.body else {
                return Err(self.corrupt("index traversal expected a leaf page"));
            };
            let end = leaf
                .entries
                .partition_point(|entry| entry.key.as_slice() < exclusive_upper);
            for entry in leaf.entries.into_iter().take(end).rev() {
                self.report.leaf_entries_visited = self
                    .report
                    .leaf_entries_visited
                    .checked_add(1)
                    .ok_or_else(|| self.admission("leaf-entry counter overflow"))?;
                if !entry.key.starts_with(prefix) {
                    if entry.key.as_slice() < prefix {
                        return Ok(VisitOutcome::PastPrefix);
                    }
                    continue;
                }
                self.report.matched_index_keys = self
                    .report
                    .matched_index_keys
                    .checked_add(1)
                    .ok_or_else(|| self.admission("matched-key counter overflow"))?;
                let index_key = decode_relational_key(&entry.key).inspect_err(|_| {
                    self.reader.poison();
                })?;
                if self.visit_ordered_posting(&index_key, &entry.posting, visit)?
                    == VisitOutcome::Stopped
                {
                    return Ok(VisitOutcome::Stopped);
                }
            }
            return Ok(VisitOutcome::Continue);
        }
        let ImmutableIndexPageBody::Interior(interior) = page.body else {
            return Err(self.corrupt("index traversal expected an interior page"));
        };
        let end = interior
            .entries
            .partition_point(|entry| entry.upper_bound.as_slice() < exclusive_upper);
        let inclusive_end = end.saturating_add(1).min(interior.entries.len());
        for entry in interior.entries.into_iter().take(inclusive_end).rev() {
            match self.visit_backward_range_subtree(
                entry.child,
                height - 1,
                Some(&entry.upper_bound),
                prefix,
                exclusive_upper,
                visit,
            )? {
                VisitOutcome::Continue => {}
                outcome => return Ok(outcome),
            }
        }
        Ok(VisitOutcome::Continue)
    }

    fn validate_page_upper_bound(
        &self,
        page: &ImmutableIndexPage,
        expected: &[u8],
    ) -> Result<(), RelationalIndexShadowError> {
        let actual = match &page.body {
            ImmutableIndexPageBody::Interior(interior) => interior
                .entries
                .last()
                .map(|entry| entry.upper_bound.as_slice()),
            ImmutableIndexPageBody::Leaf(leaf) => {
                leaf.entries.last().map(|entry| entry.key.as_slice())
            }
            _ => None,
        };
        if actual != Some(expected) {
            return Err(self.corrupt("parent separator disagrees with child page upper bound"));
        }
        Ok(())
    }

    fn visit_leaf_prefix(
        &mut self,
        entries: Vec<crate::index_page::IndexLeafEntry>,
        prefix: &[u8],
        visit: &mut impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        let start = entries.partition_point(|entry| entry.key.as_slice() < prefix);
        for entry in entries.into_iter().skip(start) {
            self.report.leaf_entries_visited = self
                .report
                .leaf_entries_visited
                .checked_add(1)
                .ok_or_else(|| self.admission("leaf-entry counter overflow"))?;
            if !entry.key.starts_with(prefix) {
                return Ok(VisitOutcome::PastPrefix);
            }
            self.report.matched_index_keys = self
                .report
                .matched_index_keys
                .checked_add(1)
                .ok_or_else(|| self.admission("matched-key counter overflow"))?;
            let index_key = decode_relational_key(&entry.key).inspect_err(|_| {
                self.reader.poison();
            })?;
            if self.visit_ordered_posting(&index_key, &entry.posting, visit)?
                == VisitOutcome::Stopped
            {
                return Ok(VisitOutcome::Stopped);
            }
        }
        Ok(VisitOutcome::Continue)
    }

    fn visit_ordered_posting(
        &mut self,
        index_key: &super::RelationalKey,
        posting: &IndexLeafPosting,
        visit: &mut impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        self.visit_posting(posting, &mut |primary_key| visit(index_key, primary_key))
    }

    fn visit_posting(
        &mut self,
        posting: &IndexLeafPosting,
        visit: &mut impl FnMut(&super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        match posting {
            IndexLeafPosting::Inline(row_ids) => {
                for row_id in row_ids {
                    if !self.visit_row(row_id.as_bytes(), visit)? {
                        return Ok(VisitOutcome::Stopped);
                    }
                }
            }
            IndexLeafPosting::Page { first, total_rows } => {
                return self.visit_posting_chain(*first, *total_rows, visit);
            }
        }
        Ok(VisitOutcome::Continue)
    }

    fn visit_posting_chain(
        &mut self,
        first: IndexPageId,
        total_rows: u64,
        visit: &mut impl FnMut(&super::RelationalKey) -> bool,
    ) -> Result<VisitOutcome, RelationalIndexShadowError> {
        let mut next = Some(first);
        let mut remaining = total_rows;
        let mut previous_row_id: Option<Vec<u8>> = None;
        while let Some(page_id) = next {
            let page = self.read_page(page_id)?;
            let ImmutableIndexPageBody::Posting(IndexPostingPage {
                next: following,
                row_ids,
            }) = page.body
            else {
                return Err(self.corrupt("posting reference points to a non-posting page"));
            };
            let row_count = u64::try_from(row_ids.len())
                .map_err(|_| self.corrupt("posting row count does not fit u64"))?;
            if row_count > remaining {
                return Err(self.corrupt("posting chain exceeds its declared cardinality"));
            }
            for row_id in row_ids {
                if previous_row_id
                    .as_ref()
                    .is_some_and(|previous| previous.as_slice() >= row_id.as_bytes())
                {
                    return Err(self.corrupt("posting chain row locators are not strictly ordered"));
                }
                previous_row_id = Some(row_id.as_bytes().to_vec());
                if !self.visit_row(row_id.as_bytes(), visit)? {
                    return Ok(VisitOutcome::Stopped);
                }
            }
            remaining -= row_count;
            if remaining == 0 && following.is_some() {
                return Err(self.corrupt("posting chain continues past its declared cardinality"));
            }
            if remaining > 0 && following.is_none() {
                return Err(self.corrupt("posting chain ends before its declared cardinality"));
            }
            next = following;
        }
        if remaining != 0 {
            return Err(self.corrupt("posting chain has an incomplete declared cardinality"));
        }
        Ok(VisitOutcome::Continue)
    }

    fn visit_row(
        &mut self,
        encoded: &[u8],
        visit: &mut impl FnMut(&super::RelationalKey) -> bool,
    ) -> Result<bool, RelationalIndexShadowError> {
        if self.report.rows_visited >= self.limits.max_rows.get() {
            return Err(self.admission(format!(
                "index lookup exceeds row limit {}",
                self.limits.max_rows
            )));
        }
        let key = decode_relational_key(encoded).inspect_err(|_| {
            self.reader.poison();
        })?;
        self.report.rows_visited += 1;
        Ok(visit(&key))
    }

    fn read_page(
        &mut self,
        page_id: IndexPageId,
    ) -> Result<ImmutableIndexPage, RelationalIndexShadowError> {
        let page_bytes = usize::try_from(self.reader.manifest().page_bytes)
            .map_err(|_| self.corrupt("manifest page size overflows usize"))?;
        self.read_admission
            .preflight_charge(IndexReadCharge::Page(page_bytes))?;
        if self.report.pages_read >= self.limits.max_pages.get() {
            return Err(self.admission(format!(
                "index lookup exceeds page limit {}",
                self.limits.max_pages
            )));
        }
        let next_bytes = self
            .report
            .bytes_read
            .checked_add(page_bytes)
            .ok_or_else(|| self.admission("index lookup byte counter overflow"))?;
        if next_bytes > self.limits.max_bytes.get() {
            return Err(self.admission(format!(
                "index lookup needs {next_bytes} bytes, exceeding byte limit {}",
                self.limits.max_bytes
            )));
        }
        let remaining_file_bytes = self
            .limits
            .max_file_bytes
            .checked_sub(self.report.file_bytes_read)
            .ok_or_else(|| self.admission("index file byte counter exceeds its limit"))?;
        self.read_admission
            .charge(IndexReadCharge::Page(page_bytes))?;
        let remaining_file_bytes = self.read_admission.file_budget(remaining_file_bytes)?;
        let read =
            self.reader
                .read_page_accounted(page_id, remaining_file_bytes, self.read_admission)?;
        self.report.pages_read += 1;
        self.report.bytes_read = next_bytes;
        self.report.cache_hits += usize::from(read.cache_hit);
        self.report.cache_misses += usize::from(read.cache_miss);
        self.report.cache_admission_rejections += usize::from(read.cache_admission_rejected);
        if !read.cache_hit {
            self.report.file_pages_read += 1;
            self.report.file_bytes_read = self
                .report
                .file_bytes_read
                .checked_add(read.file_bytes_read)
                .ok_or_else(|| self.admission("index file byte counter overflow"))?;
        }
        Ok(read.page)
    }

    fn corrupt(&self, message: impl Into<String>) -> RelationalIndexShadowError {
        self.reader.poison();
        RelationalIndexShadowError::Corrupt(message.into())
    }

    fn admission(&self, message: impl Into<String>) -> RelationalIndexShadowError {
        RelationalIndexShadowError::Admission(message.into())
    }
}

fn encoded_prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut successor = prefix.to_vec();
    for index in (0..successor.len()).rev() {
        if successor[index] != u8::MAX {
            successor[index] += 1;
            successor.truncate(index + 1);
            return Some(successor);
        }
    }
    None
}

pub(super) fn decode_relational_key(
    encoded: &[u8],
) -> Result<super::RelationalKey, RelationalIndexShadowError> {
    let mut values = Vec::new();
    let mut offset = 0usize;
    while offset < encoded.len() {
        let tag = encoded[offset];
        offset += 1;
        let value = match tag {
            0 => super::RelationalValue::Null,
            1 => {
                let value = *encoded.get(offset).ok_or_else(|| {
                    RelationalIndexShadowError::Corrupt("truncated boolean row locator".to_string())
                })?;
                offset += 1;
                match value {
                    0 => super::RelationalValue::Boolean(false),
                    1 => super::RelationalValue::Boolean(true),
                    _ => {
                        return Err(RelationalIndexShadowError::Corrupt(
                            "invalid boolean row locator".to_string(),
                        ));
                    }
                }
            }
            2 => {
                let ordered = read_ordered_u64(encoded, &mut offset, "bigint row locator")?;
                super::RelationalValue::BigInt((ordered ^ (1_u64 << 63)) as i64)
            }
            3 => {
                let ordered = read_ordered_u64(encoded, &mut offset, "double row locator")?;
                let bits = if ordered >> 63 == 0 {
                    !ordered
                } else {
                    ordered ^ (1_u64 << 63)
                };
                super::RelationalValue::DoublePrecision(f64::from_bits(bits))
            }
            4 => {
                let bytes = decode_escaped_bytes(encoded, &mut offset, "text row locator")?;
                super::RelationalValue::Text(String::from_utf8(bytes).map_err(|_| {
                    RelationalIndexShadowError::Corrupt(
                        "text row locator is not valid UTF-8".to_string(),
                    )
                })?)
            }
            5 => super::RelationalValue::Bytea(decode_escaped_bytes(
                encoded,
                &mut offset,
                "bytea row locator",
            )?),
            _ => {
                return Err(RelationalIndexShadowError::Corrupt(format!(
                    "unknown relational row-locator tag {tag}"
                )));
            }
        };
        values.push(value);
    }
    if values.is_empty() {
        return Err(RelationalIndexShadowError::Corrupt(
            "relational row locator must contain at least one value".to_string(),
        ));
    }
    Ok(super::RelationalKey(values))
}

fn read_ordered_u64(
    encoded: &[u8],
    offset: &mut usize,
    context: &str,
) -> Result<u64, RelationalIndexShadowError> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("{context} length overflow")))?;
    let bytes = encoded
        .get(*offset..end)
        .ok_or_else(|| RelationalIndexShadowError::Corrupt(format!("truncated {context}")))?;
    *offset = end;
    Ok(u64::from_be_bytes(
        bytes.try_into().expect("ordered u64 length was checked"),
    ))
}

fn decode_escaped_bytes(
    encoded: &[u8],
    offset: &mut usize,
    context: &str,
) -> Result<Vec<u8>, RelationalIndexShadowError> {
    let mut decoded = Vec::new();
    loop {
        let byte = *encoded.get(*offset).ok_or_else(|| {
            RelationalIndexShadowError::Corrupt(format!("unterminated {context}"))
        })?;
        *offset += 1;
        if byte != 0 {
            decoded.push(byte);
            continue;
        }
        let escape = *encoded.get(*offset).ok_or_else(|| {
            RelationalIndexShadowError::Corrupt(format!("truncated {context} escape"))
        })?;
        *offset += 1;
        match escape {
            0 => return Ok(decoded),
            255 => decoded.push(0),
            _ => {
                return Err(RelationalIndexShadowError::Corrupt(format!(
                    "invalid {context} escape"
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_row_locator_round_trips_all_inline_value_kinds() {
        let key = super::super::RelationalKey(vec![
            super::super::RelationalValue::Null,
            super::super::RelationalValue::Boolean(true),
            super::super::RelationalValue::BigInt(i64::MIN),
            super::super::RelationalValue::DoublePrecision(-0.0),
            super::super::RelationalValue::DoublePrecision(f64::NAN),
            super::super::RelationalValue::Text("a\0b".to_string()),
            super::super::RelationalValue::Bytea(vec![0, 1, 255]),
        ]);
        let encoded = super::super::encode_relational_key(&key).unwrap();
        assert_eq!(decode_relational_key(&encoded).unwrap(), key);
    }

    #[test]
    fn malformed_row_locator_fails_closed() {
        assert!(matches!(
            decode_relational_key(&[4, b'a', 0, 1]),
            Err(RelationalIndexShadowError::Corrupt(_))
        ));
        assert!(matches!(
            decode_relational_key(&[99]),
            Err(RelationalIndexShadowError::Corrupt(_))
        ));
    }
}
