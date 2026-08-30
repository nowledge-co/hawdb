use super::super::{RelationalIndexRangeScan, RelationalIndexScanDirection};
use super::{
    encode_relational_key, IndexLeafPosting, IndexPageId, RelationalIndexRootDescriptor,
    RelationalIndexShadowError, RelationalIndexShadowReader,
};
use crate::{ImmutableIndexPage, ImmutableIndexPageBody, IndexPostingPage};
use std::num::{NonZeroU32, NonZeroUsize};

pub const DEFAULT_RELATIONAL_INDEX_READ_PAGES: usize = 256;
pub const DEFAULT_RELATIONAL_INDEX_READ_ROWS: usize = 4096;
pub const DEFAULT_RELATIONAL_INDEX_READ_BYTES: usize = 16 * 1024 * 1024;
pub const DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT: u32 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationalIndexReadLimits {
    pub max_pages: NonZeroUsize,
    pub max_rows: NonZeroUsize,
    pub max_bytes: NonZeroUsize,
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
            max_tree_height: NonZeroU32::new(DEFAULT_RELATIONAL_INDEX_READ_TREE_HEIGHT)
                .expect("default relational index tree-height limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RelationalIndexReadReport {
    pub pages_read: usize,
    pub bytes_read: usize,
    pub file_pages_read: usize,
    pub file_bytes_read: usize,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_admission_rejections: usize,
    pub leaf_entries_visited: usize,
    pub matched_index_keys: usize,
    pub rows_visited: usize,
    pub stopped_early: bool,
}

impl RelationalIndexShadowReader {
    /// Visits the postings for one complete encoded index key.
    ///
    /// Values observed by `visit` are provisional until this method returns
    /// `Ok`; callers must discard them when traversal fails. Returning
    /// `false` stops before reading the remainder of a posting chain.
    pub fn visit_exact_postings(
        &self,
        table: &str,
        index: &str,
        key: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        let descriptor = self.root_descriptor(table, index)?.clone();
        let encoded = self.encode_lookup_key(key)?;
        let mut context = ReadContext::new(self, limits);
        let root = context.read_root(&descriptor)?;
        let Some(leaf) = context.find_leaf(root.child, root.height, &encoded)? else {
            return Ok(context.report);
        };
        let ImmutableIndexPageBody::Leaf(leaf) = leaf.body else {
            return Err(context.corrupt("index traversal ended on a non-leaf page"));
        };
        let mut comparisons = 0usize;
        let position = leaf.entries.binary_search_by(|entry| {
            comparisons = comparisons.saturating_add(1);
            entry.key.as_slice().cmp(&encoded)
        });
        context.report.leaf_entries_visited = context
            .report
            .leaf_entries_visited
            .checked_add(comparisons)
            .ok_or_else(|| context.admission("leaf-entry counter overflow"))?;
        if let Ok(position) = position {
            context.report.matched_index_keys = 1;
            let outcome = context.visit_posting(&leaf.entries[position].posting, &mut visit)?;
            context.report.stopped_early = outcome == VisitOutcome::Stopped;
        }
        Ok(context.report)
    }

    /// Visits postings whose complete index key has the supplied leading
    /// relational-key prefix. Only root-to-leaf paths and selected posting
    /// pages are read; opening the reader and unrelated subtrees remain cold.
    ///
    /// Values observed by `visit` are provisional until this method returns
    /// `Ok`; callers must discard them on error.
    pub fn visit_prefix_postings(
        &self,
        table: &str,
        index: &str,
        prefix: &super::RelationalKey,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        self.visit_prefix_entries(table, index, prefix, limits, |_, primary_key| {
            visit(primary_key)
        })
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
        mut visit: impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        let descriptor = self.root_descriptor(table, index)?.clone();
        let encoded = self.encode_lookup_key(prefix)?;
        let mut context = ReadContext::new(self, limits);
        let root = context.read_root(&descriptor)?;
        let outcome =
            context.visit_prefix_subtree(root.child, root.height, None, &encoded, &mut visit)?;
        context.report.stopped_early = outcome == VisitOutcome::Stopped;
        Ok(context.report)
    }

    /// Visits an ordered exclusive range within one leading index-key prefix.
    pub fn visit_range_entries(
        &self,
        table: &str,
        index: &str,
        scan: &RelationalIndexRangeScan,
        limits: RelationalIndexReadLimits,
        mut visit: impl FnMut(&super::RelationalKey, &super::RelationalKey) -> bool,
    ) -> Result<RelationalIndexReadReport, RelationalIndexShadowError> {
        let descriptor = self.root_descriptor(table, index)?.clone();
        let encoded_prefix = self.encode_lookup_key(&scan.prefix)?;
        let encoded_bound = scan
            .exclusive_bound
            .as_ref()
            .map(|bound| self.encode_lookup_key(bound))
            .transpose()?;
        let mut context = ReadContext::new(self, limits);
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
    limits: RelationalIndexReadLimits,
    report: RelationalIndexReadReport,
}

impl<'a> ReadContext<'a> {
    const fn new(
        reader: &'a RelationalIndexShadowReader,
        limits: RelationalIndexReadLimits,
    ) -> Self {
        Self {
            reader,
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

    fn read_root(
        &mut self,
        descriptor: &RelationalIndexRootDescriptor,
    ) -> Result<crate::IndexRootPage, RelationalIndexShadowError> {
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
        entries: Vec<crate::IndexLeafEntry>,
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
        entries: Vec<crate::IndexLeafEntry>,
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
        if self.report.pages_read >= self.limits.max_pages.get() {
            return Err(self.admission(format!(
                "index lookup exceeds page limit {}",
                self.limits.max_pages
            )));
        }
        let page_bytes = usize::try_from(self.reader.manifest().page_bytes)
            .map_err(|_| self.corrupt("manifest page size overflows usize"))?;
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
        let read = self.reader.read_page_accounted(page_id)?;
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
                .checked_add(page_bytes)
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
