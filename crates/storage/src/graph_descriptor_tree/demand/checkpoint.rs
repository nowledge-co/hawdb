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

//! Private checkpoint traversal uses captured reads and allocation ownership,
//! independent of the serving descriptor cache and its verification leases.

use super::*;
use crate::background::{CheckpointValues, CheckpointWorkContext};
use crate::graph_descriptor_page::CheckpointDescriptorPage;
use crate::scan::{CheckpointRangeReadError, SegmentReadError, SegmentReadRange};

impl GraphDescriptorTreeDemandReader {
    pub(crate) fn checkpoint_scan_from(
        &self,
        lower_bound: &[u8],
        limits: GraphDescriptorTreeReadLimits,
        work: &CheckpointWorkContext,
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
        let unit = work.start_unit()?;
        if lower_bound.is_empty() || lower_bound.len() > self.config.page_limits.max_key_bytes.get()
        {
            return Err(admission(
                "checkpoint descriptor lower bound exceeds admitted key limits",
            ));
        }
        if self.root.height > limits.max_tree_height.get() {
            return Err(admission(
                "checkpoint descriptor tree exceeds admitted height",
            ));
        }
        unit.finish();
        let capacity =
            usize::try_from(self.root.page_count.min(limits.max_pages.get())).map_err(|_| {
                admission("checkpoint descriptor tracker capacity exceeds address space")
            })?;
        let mut traversal = CheckpointTraversal {
            reader: self,
            work,
            lower_bound,
            limits,
            report: GraphDescriptorTreeReadReport::default(),
            visited: CheckpointValues::new(capacity, work)?,
        };
        let result = match self.root.root.as_ref() {
            Some(reference) if reference.upper_bound.as_slice() >= lower_bound => {
                traversal.scan_page(reference, self.root.height, &mut consumer)
            }
            Some(_) | None => Ok(GraphDescriptorTreeScanControl::Continue),
        }
        .and_then(|control| {
            work.checkpoint()?;
            Ok(control)
        });
        self.poison_on_physical_failure(&result);
        result.map(|control| (traversal.report, control))
    }
}

struct CheckpointTraversal<'a> {
    reader: &'a GraphDescriptorTreeDemandReader,
    work: &'a CheckpointWorkContext,
    lower_bound: &'a [u8],
    limits: GraphDescriptorTreeReadLimits,
    report: GraphDescriptorTreeReadReport,
    visited: CheckpointValues<(u64, u64, u64)>,
}

impl CheckpointTraversal<'_> {
    fn scan_page(
        &mut self,
        reference: &GraphDescriptorPageRef,
        height: u32,
        consumer: &mut impl FnMut(
            &[u8],
            &[u8],
        )
            -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError>,
    ) -> Result<GraphDescriptorTreeScanControl, GraphDescriptorTreeError> {
        let page = self.read_page(reference)?;
        self.report.maximum_depth = self
            .report
            .maximum_depth
            .max(self.reader.root.height.saturating_sub(height));
        let (first, comparisons) = page.lower_bound(self.lower_bound, self.work)?;
        if page.is_leaf() {
            if height != 0 {
                return Err(corrupt(
                    "checkpoint descriptor leaf appears above declared height",
                ));
            }
            self.report.leaf_entries_examined = self
                .report
                .leaf_entries_examined
                .checked_add(comparisons)
                .ok_or_else(|| corrupt("checkpoint descriptor comparison accounting overflow"))?;
            for index in first..page.len() {
                let unit = self.work.start_unit()?;
                let (key, value) = page.leaf_entry(index)?;
                self.report.leaf_entries_examined = self
                    .report
                    .leaf_entries_examined
                    .checked_add(1)
                    .ok_or_else(|| corrupt("checkpoint descriptor leaf accounting overflow"))?;
                self.report.descriptors_emitted = self
                    .report
                    .descriptors_emitted
                    .checked_add(1)
                    .ok_or_else(|| admission("checkpoint descriptor result accounting overflow"))?;
                if self.report.descriptors_emitted > self.limits.max_descriptors.get() {
                    return Err(admission(
                        "checkpoint descriptor results exceed admitted limit",
                    ));
                }
                let control = consumer(key, value)?;
                unit.finish();
                self.work.checkpoint()?;
                if control == GraphDescriptorTreeScanControl::Stop {
                    return Ok(control);
                }
            }
        } else {
            if height == 0 {
                return Err(corrupt(
                    "checkpoint descriptor interior appears below declared height",
                ));
            }
            for index in first..page.len() {
                let child = page.child(index, self.reader.config.page_limits, self.work)?;
                if self.scan_page(&child.reference, height - 1, consumer)?
                    == GraphDescriptorTreeScanControl::Stop
                {
                    return Ok(GraphDescriptorTreeScanControl::Stop);
                }
            }
        }
        self.work.checkpoint()?;
        Ok(GraphDescriptorTreeScanControl::Continue)
    }

    fn read_page(
        &mut self,
        reference: &GraphDescriptorPageRef,
    ) -> Result<CheckpointDescriptorPage, GraphDescriptorTreeError> {
        let unit = self.work.start_unit()?;
        self.reader.validate_selected_reference(reference)?;
        let pages = self
            .report
            .pages_visited
            .checked_add(1)
            .ok_or_else(|| admission("checkpoint descriptor page accounting overflow"))?;
        let bytes = self
            .report
            .page_bytes_decoded
            .checked_add(reference.length.get())
            .ok_or_else(|| admission("checkpoint descriptor byte accounting overflow"))?;
        if pages > self.limits.max_pages.get()
            || bytes > self.limits.max_page_bytes.get()
            || pages > self.reader.root.page_count
        {
            return Err(admission(
                "checkpoint descriptor traversal exceeds admitted page or byte limit",
            ));
        }
        unit.finish();
        let identity = (
            reference.artifact_id,
            reference.physical_generation,
            reference.page_id.get(),
        );
        for previous in self.visited.as_slice() {
            let unit = self.work.start_unit()?;
            if *previous == identity {
                return Err(corrupt(
                    "checkpoint descriptor tree repeats a physical page",
                ));
            }
            unit.finish();
        }
        self.visited.push(identity, self.work)?;
        self.report.pages_visited = pages;
        self.report.page_bytes_decoded = bytes;
        let range = SegmentReadRange::new(
            reference.artifact_id,
            reference.page_id.get(),
            reference.offset,
            reference.length,
        );
        // Page checksums exclude the stored checksum fields, so the admitted
        // page codec verifies both CRC32C and SHA256 against the selecting ref.
        let bytes = self
            .reader
            .checkpoint_reader
            .checkpoint_range_with_work_context(&range, self.work)
            .map_err(|error| match error {
                CheckpointRangeReadError::Work(error) => GraphDescriptorTreeError::Work(error),
                CheckpointRangeReadError::Read(SegmentReadError::Io { source, .. }) => {
                    GraphDescriptorTreeError::Io(source)
                }
                CheckpointRangeReadError::Read(error) => {
                    corrupt(format!("checkpoint descriptor range read failed: {error}"))
                }
            })?;
        self.report.storage_bytes_read = self
            .report
            .storage_bytes_read
            .checked_add(reference.length.get())
            .ok_or_else(|| admission("checkpoint descriptor storage accounting overflow"))?;
        CheckpointDescriptorPage::decode_bound(
            reference,
            self.reader.root.kind,
            self.reader.root.source_commit_epoch,
            bytes,
            self.reader.config.page_limits,
            self.work,
        )
    }
}
