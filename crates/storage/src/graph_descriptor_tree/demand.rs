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

//! Demand traversal and exhaustive scrub for one selected descriptor tree.

use super::{
    admission, corrupt, GraphDescriptorTreeBuildConfig, GraphDescriptorTreeError,
    GraphDescriptorTreeRoot, GraphDescriptorTreeRootReader,
};
use crate::cache::SegmentCacheIdentity;
use crate::graph_descriptor_page::{
    GraphDescriptorKind, GraphDescriptorPageError, GraphDescriptorPageRef,
    ImmutableGraphDescriptorPage, ImmutableGraphDescriptorPageBody,
};
use crate::{
    content_digest, ManifestGeneration, RepresentationKind, SegmentCache, SegmentCacheError,
    SegmentCacheKey, StoreId,
};
use hawdb_integrity::IntegrityHasher;
use std::collections::BTreeSet;
use std::fmt::{self, Debug, Formatter};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::num::{NonZeroU32, NonZeroU64};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const DEFAULT_MAX_READ_PAGES: u64 = 4_096;
const DEFAULT_MAX_READ_BYTES: u64 = 64 * 1024 * 1024;
const DEFAULT_MAX_READ_DESCRIPTORS: u64 = 1_048_576;
const DEFAULT_MAX_TREE_HEIGHT: u32 = 32;
const SCRUB_BUFFER_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GraphDescriptorTreeReadLimits {
    pub(crate) max_pages: NonZeroU64,
    pub(crate) max_page_bytes: NonZeroU64,
    pub(crate) max_descriptors: NonZeroU64,
    pub(crate) max_tree_height: NonZeroU32,
}

impl Default for GraphDescriptorTreeReadLimits {
    fn default() -> Self {
        Self {
            max_pages: NonZeroU64::new(DEFAULT_MAX_READ_PAGES)
                .expect("default graph descriptor read page limit is non-zero"),
            max_page_bytes: NonZeroU64::new(DEFAULT_MAX_READ_BYTES)
                .expect("default graph descriptor read byte limit is non-zero"),
            max_descriptors: NonZeroU64::new(DEFAULT_MAX_READ_DESCRIPTORS)
                .expect("default graph descriptor read descriptor limit is non-zero"),
            max_tree_height: NonZeroU32::new(DEFAULT_MAX_TREE_HEIGHT)
                .expect("default graph descriptor tree height limit is non-zero"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct GraphDescriptorTreeReadReport {
    pub(crate) pages_visited: u64,
    pub(crate) page_bytes_decoded: u64,
    pub(crate) storage_bytes_read: u64,
    pub(crate) leaf_entries_examined: u64,
    pub(crate) descriptors_emitted: u64,
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) cache_admission_rejections: u64,
    pub(crate) maximum_depth: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct GraphDescriptorTreeScrubReport {
    pub(crate) checked_pages: u64,
    pub(crate) checked_leaf_pages: u64,
    pub(crate) checked_descriptors: u64,
    pub(crate) page_bytes_decoded: u64,
    pub(crate) artifact_bytes_hashed: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum GraphDescriptorTreeScanControl {
    Continue,
    Stop,
}

#[derive(Clone)]
pub(crate) struct GraphDescriptorTreeDemandReader {
    root: Arc<GraphDescriptorTreeRoot>,
    page_artifact: PathBuf,
    config: GraphDescriptorTreeBuildConfig,
    cache: Arc<SegmentCache>,
    store_id: StoreId,
    representation: RepresentationKind,
    poisoned: Arc<AtomicBool>,
}

impl Debug for GraphDescriptorTreeDemandReader {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GraphDescriptorTreeDemandReader")
            .field("root", &self.root)
            .field("page_artifact", &self.page_artifact)
            .field("poisoned", &self.is_poisoned())
            .finish_non_exhaustive()
    }
}

impl GraphDescriptorTreeDemandReader {
    pub(crate) fn open(
        root_reader: GraphDescriptorTreeRootReader,
        config: GraphDescriptorTreeBuildConfig,
        cache: Arc<SegmentCache>,
        store_id: StoreId,
    ) -> Result<Self, GraphDescriptorTreeError> {
        let root = Arc::clone(&root_reader.root);
        let representation = match root.kind {
            GraphDescriptorKind::CanonicalSegment => {
                RepresentationKind::CanonicalSegmentDescriptorPage
            }
            GraphDescriptorKind::CanonicalAdjacency => {
                RepresentationKind::CanonicalAdjacencyDescriptorPage
            }
            GraphDescriptorKind::PropertyProjection => {
                RepresentationKind::PropertyProjectionDescriptorPage
            }
            GraphDescriptorKind::PropertySpill => RepresentationKind::PropertySpillDescriptorPage,
        };
        Ok(Self {
            root,
            page_artifact: root_reader.paths.page_artifact.clone(),
            config,
            cache,
            store_id,
            representation,
            poisoned: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Acquire)
    }

    pub(crate) fn scan_prefix(
        &self,
        prefix: &[u8],
        limits: GraphDescriptorTreeReadLimits,
        mut consumer: impl FnMut(
            &[u8],
            &[u8],
        )
            -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError>,
    ) -> Result<
        (
            GraphDescriptorTreeReadReport,
            GraphDescriptorTreeScanControl,
        ),
        GraphDescriptorTreeError,
    > {
        self.ensure_healthy()?;
        if prefix.is_empty() || prefix.len() > self.config.page_limits.max_key_bytes.get() {
            return Err(admission(format!(
                "graph descriptor prefix contains {} bytes, outside admitted range 1..={}",
                prefix.len(),
                self.config.page_limits.max_key_bytes
            )));
        }
        if self.root.height > limits.max_tree_height.get() {
            return Err(admission(format!(
                "graph descriptor tree height {} exceeds read limit {}",
                self.root.height, limits.max_tree_height
            )));
        }
        let mut state = TraversalState::new(limits, self.root.page_count, false)?;
        let start = GraphDescriptorTreeScanStart::Prefix(prefix);
        let result = match &self.root.root {
            Some(root) if start.range_may_match(root) => self.scan_page(
                root,
                self.root.height,
                start,
                true,
                &mut state,
                &mut consumer,
            ),
            Some(_) | None => Ok(GraphDescriptorTreeScanControl::Continue),
        };
        self.poison_on_physical_failure(&result);
        result.map(|control| (state.report, control))
    }

    pub(crate) fn scan_from(
        &self,
        lower_bound: &[u8],
        limits: GraphDescriptorTreeReadLimits,
        mut consumer: impl FnMut(
            &[u8],
            &[u8],
        )
            -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError>,
    ) -> Result<
        (
            GraphDescriptorTreeReadReport,
            GraphDescriptorTreeScanControl,
        ),
        GraphDescriptorTreeError,
    > {
        self.ensure_healthy()?;
        if lower_bound.is_empty() || lower_bound.len() > self.config.page_limits.max_key_bytes.get()
        {
            return Err(admission(format!(
                "graph descriptor lower bound contains {} bytes, outside admitted range 1..={}",
                lower_bound.len(),
                self.config.page_limits.max_key_bytes
            )));
        }
        if self.root.height > limits.max_tree_height.get() {
            return Err(admission(format!(
                "graph descriptor tree height {} exceeds read limit {}",
                self.root.height, limits.max_tree_height
            )));
        }
        let mut state = TraversalState::new(limits, self.root.page_count, false)?;
        let start = GraphDescriptorTreeScanStart::LowerBound(lower_bound);
        let result = match &self.root.root {
            Some(root) if start.range_may_match(root) => self.scan_page(
                root,
                self.root.height,
                start,
                true,
                &mut state,
                &mut consumer,
            ),
            Some(_) | None => Ok(GraphDescriptorTreeScanControl::Continue),
        };
        self.poison_on_physical_failure(&result);
        result.map(|control| (state.report, control))
    }

    pub(crate) fn deep_visit(
        &self,
        mut consumer: impl FnMut(
            &[u8],
            &[u8],
        )
            -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError>,
    ) -> Result<GraphDescriptorTreeScrubReport, GraphDescriptorTreeError> {
        self.ensure_healthy()?;
        let result = self.deep_visit_inner(&mut consumer);
        self.poison_on_physical_failure(&result);
        result
    }

    fn deep_visit_inner(
        &self,
        consumer: &mut impl FnMut(
            &[u8],
            &[u8],
        )
            -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError>,
    ) -> Result<GraphDescriptorTreeScrubReport, GraphDescriptorTreeError> {
        let artifact_bytes_hashed = self.verify_whole_artifact()?;
        let limits = GraphDescriptorTreeReadLimits {
            max_pages: self.config.max_page_count,
            max_page_bytes: self.config.max_page_artifact_bytes,
            max_descriptors: NonZeroU64::new(u64::MAX)
                .expect("maximum scrub descriptor limit is non-zero"),
            max_tree_height: NonZeroU32::new(self.root.height.max(1))
                .expect("scrub height limit is non-zero"),
        };
        let mut state = TraversalState::new(limits, self.root.page_count, true)?;
        state.verify_global_key_order = true;
        let control = match &self.root.root {
            Some(root) => self.scan_page(
                root,
                self.root.height,
                GraphDescriptorTreeScanStart::All,
                false,
                &mut state,
                consumer,
            ),
            None => Ok(GraphDescriptorTreeScanControl::Continue),
        }?;
        if control != GraphDescriptorTreeScanControl::Continue
            || state.report.pages_visited != self.root.page_count
            || state.leaf_pages != self.root.leaf_page_count
            || state.report.leaf_entries_examined != self.root.descriptor_count
            || state.report.storage_bytes_read != self.root.page_artifact_len
        {
            return Err(corrupt(format!(
                "graph descriptor scrub counts pages/leaf/descriptors/bytes {}/{}/{}/{} do not match root {}/{}/{}/{}",
                state.report.pages_visited,
                state.leaf_pages,
                state.report.leaf_entries_examined,
                state.report.storage_bytes_read,
                self.root.page_count,
                self.root.leaf_page_count,
                self.root.descriptor_count,
                self.root.page_artifact_len
            )));
        }
        Ok(GraphDescriptorTreeScrubReport {
            checked_pages: state.report.pages_visited,
            checked_leaf_pages: state.leaf_pages,
            checked_descriptors: state.report.leaf_entries_examined,
            page_bytes_decoded: state.report.page_bytes_decoded,
            artifact_bytes_hashed,
        })
    }

    fn verify_whole_artifact(&self) -> Result<u64, GraphDescriptorTreeError> {
        let mut file = File::open(&self.page_artifact)?;
        let actual_len = file.metadata()?.len();
        if actual_len != self.root.page_artifact_len {
            return Err(corrupt(format!(
                "graph descriptor artifact length mismatch during scrub: expected {}, got {actual_len}",
                self.root.page_artifact_len
            )));
        }
        let mut hasher = IntegrityHasher::new();
        let mut buffer = vec![0u8; SCRUB_BUFFER_BYTES];
        let mut total = 0u64;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
            total = total
                .checked_add(read as u64)
                .ok_or_else(|| corrupt("graph descriptor scrub byte count overflow"))?;
        }
        let digest = hasher.finish();
        if digest.crc32c != self.root.page_artifact_crc32c
            || digest.sha256 != self.root.page_artifact_sha256
        {
            return Err(corrupt(
                "graph descriptor page artifact checksum mismatch during scrub",
            ));
        }
        Ok(total)
    }

    fn scan_page(
        &self,
        reference: &GraphDescriptorPageRef,
        remaining_height: u32,
        start: GraphDescriptorTreeScanStart<'_>,
        use_cache: bool,
        state: &mut TraversalState,
        consumer: &mut impl FnMut(
            &[u8],
            &[u8],
        )
            -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError>,
    ) -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError> {
        state.admit_page(reference)?;
        let page = self.read_page(reference, use_cache, &mut state.report)?;
        state.report.maximum_depth = state
            .report
            .maximum_depth
            .max(self.root.height.saturating_sub(remaining_height));
        match (remaining_height, page.body) {
            (0, ImmutableGraphDescriptorPageBody::Leaf(entries)) => {
                state.leaf_pages = state
                    .leaf_pages
                    .checked_add(1)
                    .ok_or_else(|| corrupt("graph descriptor leaf page count overflow"))?;
                for entry in entries {
                    state.report.leaf_entries_examined = state
                        .report
                        .leaf_entries_examined
                        .checked_add(1)
                        .ok_or_else(|| corrupt("graph descriptor entry count overflow"))?;
                    if state.verify_global_key_order {
                        if state
                            .previous_key
                            .as_ref()
                            .is_some_and(|previous| previous.as_slice() >= entry.key.as_slice())
                        {
                            return Err(corrupt(
                                "graph descriptor leaf keys are not globally ordered",
                            ));
                        }
                        state.previous_key = Some(entry.key.clone());
                    }
                    match start {
                        GraphDescriptorTreeScanStart::All => {}
                        GraphDescriptorTreeScanStart::Prefix(prefix) => {
                            if !entry.key.starts_with(prefix) {
                                if entry.key.as_slice() > prefix {
                                    break;
                                }
                                continue;
                            }
                        }
                        GraphDescriptorTreeScanStart::LowerBound(lower_bound) => {
                            if entry.key.as_slice() < lower_bound {
                                continue;
                            }
                        }
                    }
                    state.report.descriptors_emitted = state
                        .report
                        .descriptors_emitted
                        .checked_add(1)
                        .ok_or_else(|| admission("graph descriptor result count overflow"))?;
                    if state.report.descriptors_emitted > state.limits.max_descriptors.get() {
                        return Err(admission(format!(
                            "graph descriptor scan emits {} descriptors, exceeding limit {}",
                            state.report.descriptors_emitted, state.limits.max_descriptors
                        )));
                    }
                    if consumer(&entry.key, &entry.value)? == GraphDescriptorTreeScanControl::Stop {
                        return Ok(GraphDescriptorTreeScanControl::Stop);
                    }
                }
                Ok(GraphDescriptorTreeScanControl::Continue)
            }
            (height, ImmutableGraphDescriptorPageBody::Interior(entries)) => {
                if height == 0 {
                    return Err(corrupt(
                        "graph descriptor interior page appears below the declared tree height",
                    ));
                }
                for entry in entries {
                    if !start.range_may_match(&entry.child) {
                        continue;
                    }
                    let control = self.scan_page(
                        &entry.child,
                        remaining_height - 1,
                        start,
                        use_cache,
                        state,
                        consumer,
                    )?;
                    if control == GraphDescriptorTreeScanControl::Stop {
                        return Ok(control);
                    }
                }
                Ok(GraphDescriptorTreeScanControl::Continue)
            }
            (_, ImmutableGraphDescriptorPageBody::Leaf(_)) => Err(corrupt(
                "graph descriptor leaf page appears above the declared tree height",
            )),
        }
    }

    fn read_page(
        &self,
        reference: &GraphDescriptorPageRef,
        use_cache: bool,
        report: &mut GraphDescriptorTreeReadReport,
    ) -> Result<ImmutableGraphDescriptorPage, GraphDescriptorTreeError> {
        self.validate_selected_reference(reference)?;
        let identity = SegmentCacheIdentity {
            store_id: self.store_id,
            manifest_generation: ManifestGeneration(reference.physical_generation),
            segment_id: reference.page_id.get(),
            representation: self.representation,
        };
        if use_cache && let Some(encoded) = self.cache.get_by_identity(&identity) {
            report.cache_hits = report
                .cache_hits
                .checked_add(1)
                .ok_or_else(|| admission("graph descriptor cache hit accounting overflow"))?;
            return ImmutableGraphDescriptorPage::decode_bound(
                reference,
                self.root.kind,
                self.root.source_commit_epoch,
                &encoded,
                self.config.page_limits,
            )
            .map_err(GraphDescriptorTreeError::Page);
        }
        if use_cache {
            report.cache_misses = report
                .cache_misses
                .checked_add(1)
                .ok_or_else(|| admission("graph descriptor cache miss accounting overflow"))?;
        }
        let next_storage_bytes = report
            .storage_bytes_read
            .checked_add(reference.length.get())
            .ok_or_else(|| admission("graph descriptor read byte accounting overflow"))?;
        let mut file = File::open(&self.page_artifact)?;
        let actual_len = file.metadata()?.len();
        let end = reference
            .offset
            .checked_add(reference.length.get())
            .ok_or_else(|| corrupt("graph descriptor page range overflow"))?;
        if end > actual_len {
            return Err(corrupt(format!(
                "graph descriptor page range {}/{} exceeds artifact length {actual_len}",
                reference.offset, reference.length
            )));
        }
        file.seek(SeekFrom::Start(reference.offset))?;
        let page_bytes = usize::try_from(reference.length.get())
            .map_err(|_| admission("graph descriptor page length does not fit this platform"))?;
        let mut encoded = vec![0u8; page_bytes];
        file.read_exact(&mut encoded)?;
        report.storage_bytes_read = next_storage_bytes;
        let page = ImmutableGraphDescriptorPage::decode_bound(
            reference,
            self.root.kind,
            self.root.source_commit_epoch,
            &encoded,
            self.config.page_limits,
        )
        .map_err(GraphDescriptorTreeError::Page)?;
        if use_cache {
            let key = SegmentCacheKey {
                store_id: identity.store_id,
                manifest_generation: identity.manifest_generation,
                segment_id: identity.segment_id,
                content_digest: content_digest(&encoded),
                representation: identity.representation,
            };
            match self.cache.insert(key, encoded) {
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.error(),
                        SegmentCacheError::EntryTooLarge { .. }
                            | SegmentCacheError::PinnedCapacity { .. }
                    ) =>
                {
                    report.cache_admission_rejections = report
                        .cache_admission_rejections
                        .checked_add(1)
                        .ok_or_else(|| {
                            admission("graph descriptor cache rejection accounting overflow")
                        })?;
                }
                Err(error) => {
                    return Err(corrupt(format!(
                        "graph descriptor cache rejected immutable page identity: {error}"
                    )))
                }
            }
        }
        Ok(page)
    }

    fn validate_selected_reference(
        &self,
        reference: &GraphDescriptorPageRef,
    ) -> Result<(), GraphDescriptorTreeError> {
        reference
            .validate(self.config.page_limits)
            .map_err(GraphDescriptorTreeError::Page)?;
        if reference.artifact_id != self.root.page_artifact_id
            || reference.physical_generation != self.root.generation
        {
            return Err(corrupt(format!(
                "graph descriptor page identity {}/{} is outside selected artifact {}/{}",
                reference.artifact_id,
                reference.physical_generation,
                self.root.page_artifact_id,
                self.root.generation
            )));
        }
        let end = reference
            .offset
            .checked_add(reference.length.get())
            .ok_or_else(|| corrupt("graph descriptor page range overflow"))?;
        if end > self.root.page_artifact_len {
            return Err(corrupt(format!(
                "graph descriptor page range ends at {end}, beyond selected artifact length {}",
                self.root.page_artifact_len
            )));
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), GraphDescriptorTreeError> {
        if self.is_poisoned() {
            return Err(corrupt(
                "graph descriptor demand reader is poisoned by an earlier physical failure",
            ));
        }
        Ok(())
    }

    fn poison_on_physical_failure<T>(&self, result: &Result<T, GraphDescriptorTreeError>) {
        if matches!(
            result,
            Err(GraphDescriptorTreeError::Io(_))
                | Err(GraphDescriptorTreeError::Page(
                    GraphDescriptorPageError::Corrupt(_)
                ))
                | Err(GraphDescriptorTreeError::Corrupt(_))
        ) {
            self.poisoned.store(true, Ordering::Release);
        }
    }
}

#[derive(Clone, Copy)]
enum GraphDescriptorTreeScanStart<'a> {
    All,
    Prefix(&'a [u8]),
    LowerBound(&'a [u8]),
}

impl GraphDescriptorTreeScanStart<'_> {
    fn range_may_match(self, reference: &GraphDescriptorPageRef) -> bool {
        match self {
            Self::All => true,
            Self::Prefix(prefix) => range_can_contain_prefix(reference, prefix),
            Self::LowerBound(lower_bound) => reference.upper_bound.as_slice() >= lower_bound,
        }
    }
}

struct TraversalState {
    limits: GraphDescriptorTreeReadLimits,
    report: GraphDescriptorTreeReadReport,
    visited: VisitedPages,
    leaf_pages: u64,
    previous_key: Option<Vec<u8>>,
    verify_global_key_order: bool,
}

impl TraversalState {
    fn new(
        limits: GraphDescriptorTreeReadLimits,
        root_page_count: u64,
        dense_tracking: bool,
    ) -> Result<Self, GraphDescriptorTreeError> {
        Ok(Self {
            limits,
            report: GraphDescriptorTreeReadReport::default(),
            visited: if dense_tracking {
                VisitedPages::dense(root_page_count)?
            } else {
                VisitedPages::Sparse(BTreeSet::new())
            },
            leaf_pages: 0,
            previous_key: None,
            verify_global_key_order: false,
        })
    }

    fn admit_page(
        &mut self,
        reference: &GraphDescriptorPageRef,
    ) -> Result<(), GraphDescriptorTreeError> {
        if !self.visited.insert(reference)? {
            return Err(corrupt(
                "graph descriptor tree references the same physical page more than once",
            ));
        }
        let pages = self
            .report
            .pages_visited
            .checked_add(1)
            .ok_or_else(|| admission("graph descriptor page accounting overflow"))?;
        if pages > self.limits.max_pages.get() {
            return Err(admission(format!(
                "graph descriptor scan visits {pages} pages, exceeding limit {}",
                self.limits.max_pages
            )));
        }
        let page_bytes = self
            .report
            .page_bytes_decoded
            .checked_add(reference.length.get())
            .ok_or_else(|| admission("graph descriptor page byte accounting overflow"))?;
        if page_bytes > self.limits.max_page_bytes.get() {
            return Err(admission(format!(
                "graph descriptor scan decodes {page_bytes} page bytes, exceeding limit {}",
                self.limits.max_page_bytes
            )));
        }
        self.report.pages_visited = pages;
        self.report.page_bytes_decoded = page_bytes;
        Ok(())
    }
}

enum VisitedPages {
    Sparse(BTreeSet<(u64, u64, u64)>),
    Dense { words: Vec<u64>, page_count: u64 },
}

impl VisitedPages {
    fn dense(page_count: u64) -> Result<Self, GraphDescriptorTreeError> {
        let word_count = page_count
            .checked_add(u64::BITS as u64 - 1)
            .map(|bits| bits / u64::BITS as u64)
            .ok_or_else(|| admission("graph descriptor scrub tracker length overflow"))?;
        let word_count = usize::try_from(word_count)
            .map_err(|_| admission("graph descriptor scrub tracker does not fit this platform"))?;
        Ok(Self::Dense {
            words: vec![0; word_count],
            page_count,
        })
    }

    fn insert(
        &mut self,
        reference: &GraphDescriptorPageRef,
    ) -> Result<bool, GraphDescriptorTreeError> {
        match self {
            Self::Sparse(visited) => Ok(visited.insert((
                reference.artifact_id,
                reference.physical_generation,
                reference.page_id.get(),
            ))),
            Self::Dense { words, page_count } => {
                let page_id = reference.page_id.get();
                if page_id > *page_count {
                    return Err(corrupt(format!(
                        "graph descriptor page id {page_id} exceeds root page count {page_count}"
                    )));
                }
                let ordinal = page_id - 1;
                let word = usize::try_from(ordinal / u64::BITS as u64)
                    .map_err(|_| corrupt("graph descriptor page id does not fit scrub tracker"))?;
                let mask = 1u64 << (ordinal % u64::BITS as u64);
                let was_absent = words[word] & mask == 0;
                words[word] |= mask;
                Ok(was_absent)
            }
        }
    }
}

fn range_can_contain_prefix(reference: &GraphDescriptorPageRef, prefix: &[u8]) -> bool {
    reference.upper_bound.as_slice() >= prefix
        && (reference.lower_bound.as_slice() <= prefix || reference.lower_bound.starts_with(prefix))
}

#[cfg(test)]
mod tests;
