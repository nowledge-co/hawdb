use super::{
    ProjectionHit, ProjectionSearchOptions, RankedHit, SegmentReader,
    MIN_ALLOWLIST_DOCUMENTS_PER_WORKER, SEARCH_FIXED_BYTES, WORKER_FIXED_BYTES, WORKER_STACK_BYTES,
};
use crate::error::{ProjectionError, Result};
use std::cmp::Reverse;
use std::mem::size_of;

/// A checked envelope for search buffers and bounded worker allowances. This
/// is standalone admission, not a lease against the caller's shared query root.
pub(super) struct SearchMemoryPlan {
    pub top_k: usize,
    pub worker_count: usize,
    pub working_bytes: usize,
}

impl SearchMemoryPlan {
    pub fn admit<R: SegmentReader>(
        reader: &R,
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> Result<Self> {
        let manifest = reader.manifest();
        let query_bytes = allocation_bytes(manifest.dimension, size_of::<f32>())?;
        let allowed = options.candidates.map(|candidates| candidates.ids);
        // No heap can return more distinct hits than the corpus or allowlist.
        // A huge requested limit must not cause a huge, otherwise unused heap.
        let top_k = top_k
            .min(manifest.document_count)
            .min(allowed.map_or(usize::MAX, <[u64]>::len));
        if top_k == 0 || manifest.segments.is_empty() {
            require_budget(query_bytes, options.max_working_bytes)?;
            return Ok(Self {
                top_k,
                worker_count: 0,
                working_bytes: query_bytes,
            });
        }

        let heap_bytes = allocation_bytes(top_k, size_of::<Reverse<RankedHit>>())?;
        let output_bytes = allocation_bytes(top_k, size_of::<ProjectionHit>())?;
        let mask_bytes = if allowed.is_some() {
            allocation_bytes(
                reader.max_segment_rows().div_ceil(u64::BITS as usize),
                size_of::<u64>(),
            )?
        } else {
            0
        };
        let worker_bytes = checked_sum(&[
            reader.max_segment_payload_bytes(),
            mask_bytes,
            heap_bytes,
            WORKER_STACK_BYTES,
            WORKER_FIXED_BYTES,
        ])?;
        // During parallel merge, the extra heap overlaps every worker heap.
        // During final conversion, workers have been joined/dropped, so one
        // worker heap allowance plus this output allowance covers both buffers.
        // The single-worker path never creates an extra merge heap.
        let global_bytes = checked_sum(&[
            query_bytes,
            heap_bytes.max(output_bytes),
            SEARCH_FIXED_BYTES,
        ])?;
        let minimum_bytes = checked_sum(&[global_bytes, worker_bytes])?;
        require_budget(minimum_bytes, options.max_working_bytes)?;
        let admitted_by_memory = (options.max_working_bytes - global_bytes) / worker_bytes;
        let admitted_by_allowlist = allowed.map_or(usize::MAX, |ids| {
            ids.len().div_ceil(MIN_ALLOWLIST_DOCUMENTS_PER_WORKER)
        });
        let worker_count = options
            .max_parallelism
            .get()
            .min(
                options
                    .task_context
                    .map_or(usize::MAX, |context| context.admitted_parallelism().get()),
            )
            .min(manifest.segments.len())
            .min(admitted_by_memory)
            .min(admitted_by_allowlist);
        let working_bytes = checked_sum(&[
            global_bytes,
            worker_bytes
                .checked_mul(worker_count)
                .ok_or_else(size_overflow)?,
        ])?;
        Ok(Self {
            top_k,
            worker_count,
            working_bytes,
        })
    }
}

fn allocation_bytes(count: usize, element_bytes: usize) -> Result<usize> {
    count
        .checked_mul(element_bytes)
        .filter(|bytes| *bytes <= isize::MAX as usize)
        .ok_or_else(size_overflow)
}

fn checked_sum(parts: &[usize]) -> Result<usize> {
    parts.iter().try_fold(0usize, |sum, part| {
        sum.checked_add(*part).ok_or_else(size_overflow)
    })
}

fn size_overflow() -> ProjectionError {
    ProjectionError::InvalidConfiguration("projection search memory size overflow".to_string())
}

fn require_budget(required: usize, available: usize) -> Result<()> {
    if required > available {
        return Err(ProjectionError::ResourceBudgetExceeded {
            required,
            available,
        });
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests;
