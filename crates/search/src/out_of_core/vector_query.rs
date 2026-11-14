// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Selected vector rows are read and decoded under the original query account and I/O.

use super::*;
use crate::lexical_projection::QueryContext;
use hawdb_executor::QueryMemoryLease;

#[cfg(test)]
mod layout_probe;
#[cfg(test)]
mod tests;

/// Query-admitted source: every transient buffer is charged to the query
/// account, and every physical read borrows the query's I/O admission.
pub(super) struct AdmittedSource<'a, R> {
    read: R,
    context: QueryContext<'a>,
    lease: QueryMemoryLease,
}

impl<'a, R: FnMut(u64, &mut [u8]) -> Result<()>> AdmittedSource<'a, R> {
    pub(super) fn new(read: R, context: QueryContext<'a>) -> Result<Self> {
        Ok(Self {
            read,
            lease: context.memory.reserve(0)?,
            context,
        })
    }
}

impl<R: FnMut(u64, &mut [u8]) -> Result<()>> vector_segment::Source for AdmittedSource<'_, R> {
    fn read_exact_at(&mut self, offset: u64, output: &mut [u8]) -> Result<()> {
        self.context.checkpoint()?;
        (self.read)(offset, output)
    }

    fn reserve(&mut self, bytes: usize) -> Result<()> {
        self.lease.grow(bytes)?;
        Ok(())
    }

    fn checkpoint(&mut self) -> Result<()> {
        self.context.checkpoint()
    }
}

impl SearchOutOfCoreReader {
    /// Visits the selected vector rows of one segment. Unselected rows are not
    /// read; the segment head and each visited row are checksum-verified.
    pub(super) fn visit_query_vector_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        context: QueryContext<'_>,
        selected: impl FnMut(u64) -> bool,
        visit: impl FnMut(&SearchVectorDocument) -> Result<()>,
    ) -> Result<()> {
        self.visit_vector_segment(artifact, segment, metrics, Some(context), selected, visit)
    }

    /// Admitted when `context` is present; otherwise an unadmitted serving read.
    pub(super) fn visit_vector_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        context: Option<QueryContext<'_>>,
        selected: impl FnMut(u64) -> bool,
        visit: impl FnMut(&SearchVectorDocument) -> Result<()>,
    ) -> Result<()> {
        let (range, expected) = self.vector_expectation(artifact, segment)?;
        let stats = match context {
            Some(context) => vector_segment::visit_selected(
                &mut AdmittedSource::new(admitted_read(artifact, range, context), context)?,
                expected,
                selected,
                visit,
            )?,
            None => vector_segment::visit_selected(
                &mut PlainVectorSource {
                    file: &artifact.vector_payload,
                    offset: range.offset,
                },
                expected,
                selected,
                visit,
            )?,
        };
        record_vector_reads(metrics, stats);
        Ok(())
    }

    fn vector_expectation<'a>(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &'a SearchSegmentDescriptorEntry,
    ) -> Result<(SearchOutOfCoreRange, vector_segment::Expected<'a>)> {
        let layout = Self::artifact_layout_range(artifact, segment.segment_id)?;
        let range = layout.vectors;
        Ok((
            range,
            vector_segment::Expected {
                segment,
                payload_len: range.length,
                count: range.entry_count,
                vector_ordinal_base: layout.vector_ordinal_base,
                dimension: self.manifest.embedding_dimension,
            },
        ))
    }
}

fn admitted_read<'a>(
    artifact: &'a SearchOutOfCoreSegmentReader,
    range: SearchOutOfCoreRange,
    context: QueryContext<'a>,
) -> impl FnMut(u64, &mut [u8]) -> Result<()> + 'a {
    move |offset, output| {
        let mut input = crate::build_memory::read::QueryFileRange {
            file: &artifact.vector_payload,
            expected_length: Some(artifact.vector_payload_length),
            offset: range
                .offset
                .checked_add(offset)
                .ok_or_else(|| HawDBError::Storage("vector range offset overflow".into()))?,
            remaining: output.len() as u64,
            context,
            require_execution: true,
        };
        input
            .read_exact(output)
            .map_err(crate::build_memory::read::io_error)
    }
}

#[cfg(feature = "vector-search")]
impl SearchOutOfCoreReader {
    /// Maps vector ordinals to document IDs from the verified segment head
    /// without reading any vector row.
    pub(super) fn visit_vector_segment_ids(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
        metrics: &mut SearchOutOfCoreMetrics,
        context: Option<QueryContext<'_>>,
        visit: impl FnMut(u64, &str) -> Result<()>,
    ) -> Result<()> {
        let (range, expected) = self.vector_expectation(artifact, segment)?;
        let stats = match context {
            Some(context) => vector_segment::visit_ids(
                &mut AdmittedSource::new(admitted_read(artifact, range, context), context)?,
                expected,
                visit,
            )?,
            None => vector_segment::visit_ids(
                &mut PlainVectorSource {
                    file: &artifact.vector_payload,
                    offset: range.offset,
                },
                expected,
                visit,
            )?,
        };
        record_vector_reads(metrics, stats);
        Ok(())
    }
}

pub(super) fn record_vector_reads(
    metrics: &mut SearchOutOfCoreMetrics,
    stats: vector_segment::ReadStats,
) {
    metrics.segment_range_reads = metrics.segment_range_reads.saturating_add(stats.reads);
    metrics.segment_bytes_read = metrics.segment_bytes_read.saturating_add(stats.bytes);
    metrics.vector_segment_bytes_read = metrics
        .vector_segment_bytes_read
        .saturating_add(stats.bytes);
    metrics.peak_vector_segment_bytes = metrics.peak_vector_segment_bytes.max(stats.peak_bytes);
}
