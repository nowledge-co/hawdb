use super::*;
use crate::model::{encoded_vector_bytes, InMemoryProjection, ProjectionManifest, RaBitQBitWidth};
use crate::scan::{
    search_projection, KernelPreference, ProjectionSearchOutput, ScanKernel, SegmentSearchResult,
    TopK,
};
use crate::transform::normalize_and_transform;
use crate::{ProjectionBuildConfig, ProjectionBuilder, ProjectionIdentity, ProjectionWriter};
use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};
use std::cell::Cell;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};

mod fuzz;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Allocations {
    queries: usize,
    heaps: usize,
    heap_slots: usize,
}

thread_local! {
    static ALLOCATIONS: Cell<Allocations> = Cell::default();
}

pub(in crate::scan) fn record_query_allocation() {
    let mut counts = ALLOCATIONS.get();
    counts.queries += 1;
    ALLOCATIONS.set(counts);
}

pub(in crate::scan) fn record_heap_allocation(slots: usize) {
    let mut counts = ALLOCATIONS.get();
    counts.heaps += 1;
    counts.heap_slots += slots;
    ALLOCATIONS.set(counts);
}

fn reset_allocations() {
    ALLOCATIONS.set(Allocations::default());
}

fn fixture(rows: usize, dimension: usize, segment_rows: usize) -> InMemoryProjection {
    let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1))
        .with_segment_rows(segment_rows);
    let mut builder = ProjectionBuilder::new(config).unwrap();
    for id in 0..rows {
        let vector = (0..dimension)
            .map(|column| ((id * 17 + column * 13) as f32).sin())
            .collect::<Vec<_>>();
        builder.push(id as u64, &vector).unwrap();
    }
    builder.finish().unwrap()
}

struct ReadProbe {
    projection: InMemoryProjection,
    calls: AtomicUsize,
    payload_bytes: usize,
    max_rows: usize,
}

impl ReadProbe {
    fn new(projection: InMemoryProjection) -> Self {
        let max_rows = projection.max_segment_rows();
        Self {
            projection,
            calls: AtomicUsize::new(0),
            payload_bytes: 0,
            max_rows,
        }
    }
}

impl SegmentReader for ReadProbe {
    fn manifest(&self) -> &ProjectionManifest {
        &self.projection.manifest
    }
    fn max_segment_rows(&self) -> usize {
        self.max_rows
    }
    fn max_segment_payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    fn scan_segment(
        &self,
        index: usize,
        query: &[f32],
        top: &mut TopK,
        kernel: ScanKernel,
        allowed: Option<&[u64]>,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<SegmentSearchResult> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.projection
            .scan_segment(index, query, top, kernel, allowed, context)
    }
}

// Independent phase envelope: one heap per worker, a merge heap while workers
// remain live, then a separate output vector after they have all been joined.
// Do not obtain the expected boundary from the production planner or report.
fn envelope<R: SegmentReader>(reader: &R, limit: usize, allowed: Option<&[u64]>) -> (usize, usize) {
    let manifest = reader.manifest();
    let slots = limit
        .min(manifest.document_count)
        .min(allowed.map_or(usize::MAX, <[u64]>::len));
    let query = manifest.dimension * size_of::<f32>();
    if slots == 0 || manifest.segments.is_empty() {
        return (query, 0);
    }
    let heap = slots * size_of::<Reverse<RankedHit>>();
    let result = slots * size_of::<ProjectionHit>();
    let mask = allowed.map_or(0, |_| reader.max_segment_rows().div_ceil(64) * 8);
    (
        query + heap.max(result) + 1024,
        reader.max_segment_payload_bytes() + mask + heap + 512 * 1024 + 1024,
    )
}

#[test]
fn budget_denial_precedes_query_allocation_and_segment_entry() {
    let reader = ReadProbe::new(fixture(65, 17, 8));
    let query = [1.0; 17];
    let (global, worker) = envelope(&reader, 7, None);
    let required = global + worker;
    reset_allocations();
    let result = search_projection(
        &reader,
        &query,
        7,
        ProjectionSearchOptions::new().with_max_working_bytes(required - 1),
    );
    assert!(
        matches!(result, Err(ProjectionError::ResourceBudgetExceeded {
        required: actual, available
    }) if actual == required && available == required - 1)
    );
    assert_eq!(ALLOCATIONS.get(), Allocations::default());
    assert_eq!(reader.calls.load(Ordering::Relaxed), 0);
    let output = search_projection(
        &reader,
        &query,
        7,
        ProjectionSearchOptions::new().with_max_working_bytes(required),
    )
    .unwrap();
    assert_eq!(output.report.admitted_working_bytes, required);
    assert_eq!(
        reader.calls.load(Ordering::Relaxed),
        reader.manifest().segments.len()
    );
    assert_eq!(
        ALLOCATIONS.get(),
        Allocations {
            queries: 1,
            heaps: 1,
            heap_slots: 7
        }
    );
    assert_eq!(output.hits, oracle(&reader.projection, &query, 7, None));
}

#[test]
fn empty_search_paths_enforce_query_budget_and_preserve_validation() {
    let populated = fixture(3, 8, 1);
    let empty = fixture(0, 8, 1);
    for (reader, limit, options) in [
        (&populated, 0, ProjectionSearchOptions::new()),
        (
            &populated,
            3,
            ProjectionSearchOptions::new().with_allowed_ids(&[]),
        ),
        (&empty, usize::MAX, ProjectionSearchOptions::new()),
    ] {
        reset_allocations();
        let denied = reader.search(&[1.0; 8], limit, options.with_max_working_bytes(31));
        assert!(matches!(
            denied,
            Err(ProjectionError::ResourceBudgetExceeded {
                required: 32,
                available: 31
            })
        ));
        assert_eq!(ALLOCATIONS.get(), Allocations::default());
        let empty = reader
            .search(&[1.0; 8], limit, options.with_max_working_bytes(32))
            .unwrap();
        assert!(empty.hits.is_empty());
        assert_eq!(empty.report.worker_count, 0);
        assert_eq!(empty.report.admitted_working_bytes, 32);
        assert_eq!(
            ALLOCATIONS.get(),
            Allocations {
                queries: 1,
                ..Allocations::default()
            }
        );
        assert!(matches!(
            reader.search(&[f32::NAN; 8], limit, options),
            Err(ProjectionError::InvalidVector(_))
        ));
    }
}

#[test]
fn one_worker_reuses_one_heap_across_segments_and_caps_huge_limits() {
    let projection = fixture(129, 9, 1);
    let query = [0.0; 9];
    for allowed in [None, Some([1, 7, 21].as_slice())] {
        let mut options = ProjectionSearchOptions::new();
        if let Some(ids) = allowed {
            options = options.with_allowed_ids(ids);
        }
        let slots = allowed.map_or(129, <[u64]>::len);
        let (global, worker) = envelope(&projection, usize::MAX, allowed);
        reset_allocations();
        let output = projection
            .search(
                &query,
                usize::MAX,
                options.with_max_working_bytes(global + worker),
            )
            .unwrap();
        assert_eq!(
            ALLOCATIONS.get(),
            Allocations {
                queries: 1,
                heaps: 1,
                heap_slots: slots
            }
        );
        assert_eq!(
            output.hits,
            oracle(&projection, &query, usize::MAX, allowed)
        );
        assert_eq!(output.hits.len(), slots);
    }
}

#[test]
fn worker_admission_obeys_exact_memory_allowlist_and_task_boundaries() {
    let projection = fixture(2050, 3, 257);
    let allowed = (0..2050).collect::<Vec<u64>>();
    let query = [1.0, -1.0, 0.0];
    for (ids, allowlist_ceiling) in [
        (None, 4),
        (Some(allowed.as_slice()), 3),
        (Some(&allowed[..1]), 1),
    ] {
        for task_limit in [1, 2, 4] {
            let context = RuntimeTaskContext::default()
                .with_admitted_parallelism(NonZeroUsize::new(task_limit).unwrap());
            let mut options = ProjectionSearchOptions::new()
                .with_max_parallelism(NonZeroUsize::new(4).unwrap())
                .with_task_context(&context);
            if let Some(ids) = ids {
                options = options.with_allowed_ids(ids);
            }
            let expected = oracle(&projection, &query, 7, ids);
            let (global, worker) = envelope(&projection, 7, ids);
            for slots in 1..=4 {
                for budget in [global + slots * worker, global + (slots + 1) * worker - 1] {
                    let output = projection
                        .search(&query, 7, options.with_max_working_bytes(budget))
                        .unwrap();
                    let workers = slots.min(task_limit).min(allowlist_ceiling);
                    assert_eq!(output.report.worker_count, workers);
                    assert_eq!(
                        output.report.admitted_working_bytes,
                        global + workers * worker
                    );
                    assert!(output.report.admitted_working_bytes <= budget);
                    assert_eq!(output.hits, expected);
                }
            }
        }
    }
}

#[test]
fn virtual_oversized_inputs_reject_checked_arithmetic_without_allocating() {
    let options = ProjectionSearchOptions::new().with_max_working_bytes(usize::MAX);
    let mut reader = ReadProbe::new(fixture(3, 8, 1));
    reset_allocations();
    for dimension in [usize::MAX, isize::MAX as usize / 4 + 1] {
        reader.projection.manifest.dimension = dimension;
        assert!(matches!(
            SearchMemoryPlan::admit(&reader, 1, options),
            Err(ProjectionError::InvalidConfiguration(_))
        ));
    }
    reader.projection.manifest.dimension = 8;
    reader.projection.manifest.document_count = usize::MAX;
    assert!(matches!(
        SearchMemoryPlan::admit(&reader, usize::MAX, options),
        Err(ProjectionError::InvalidConfiguration(_))
    ));
    reader.projection.manifest.document_count = 3;
    reader.payload_bytes = usize::MAX;
    assert!(matches!(
        SearchMemoryPlan::admit(&reader, 1, options),
        Err(ProjectionError::InvalidConfiguration(_))
    ));
    assert_eq!(ALLOCATIONS.get(), Allocations::default());
    assert_eq!(reader.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn cancellation_precedes_memory_and_query_allocation() {
    let reader = ReadProbe::new(fixture(3, 8, 1));
    let token = RuntimeCancellationToken::new();
    let context = RuntimeTaskContext::without_deadline(token.clone());
    token.cancel();
    reset_allocations();
    let output = search_projection(
        &reader,
        &[1.0; 8],
        usize::MAX,
        ProjectionSearchOptions::new()
            .with_task_context(&context)
            .with_max_working_bytes(0),
    );
    assert!(matches!(output, Err(ProjectionError::Cancelled(_))));
    assert_eq!(ALLOCATIONS.get(), Allocations::default());
    assert_eq!(reader.calls.load(Ordering::Relaxed), 0);
}

#[test]
fn failed_segment_does_not_publish_a_partially_accumulated_heap() {
    let mut projection = fixture(129, 9, 8);
    let query = [1.0; 9];
    let expected = oracle(&projection, &query, 7, None);
    let scale = projection.segments[1].reconstruction_scales[0];
    projection.segments[1].reconstruction_scales[0] = f32::NAN;
    for workers in [1, 4] {
        let output = projection.search(
            &query,
            7,
            ProjectionSearchOptions::new()
                .with_max_parallelism(NonZeroUsize::new(workers).unwrap()),
        );
        assert!(matches!(output, Err(ProjectionError::CorruptArtifact(_))));
    }
    projection.segments[1].reconstruction_scales[0] = scale;
    assert_eq!(
        projection
            .search(&query, 7, ProjectionSearchOptions::new())
            .unwrap()
            .hits,
        expected,
    );
}

// Decode sign/refinement bits directly and full-sort all scores. This does not
// call the production scorer, heap comparator, candidate mask, or budget plan.
// The unchanged query transform is shared; this is quantized ranking parity,
// not a new qualification of transform accuracy or approximate recall.
fn oracle(
    projection: &InMemoryProjection,
    query: &[f32],
    limit: usize,
    allowed: Option<&[u64]>,
) -> Vec<ProjectionHit> {
    let dimension = query.len();
    let mut transformed = vec![0.0; dimension];
    normalize_and_transform(query, projection.manifest.transform_seed, &mut transformed).unwrap();
    let query_sum = transformed.iter().sum::<f32>();
    let refinement = projection.manifest.bit_width as usize - 1;
    let bit_width = RaBitQBitWidth::from_bits(projection.manifest.bit_width).unwrap();
    let stride = encoded_vector_bytes(dimension, bit_width);
    let mut hits = Vec::new();
    for segment in &projection.segments {
        for (row, &id) in segment.ids.iter().enumerate() {
            if allowed.is_some_and(|ids| !ids.contains(&id)) {
                continue;
            }
            let bytes = &segment.codes[row * stride..(row + 1) * stride];
            let bit = |offset: usize| (bytes[offset / 8] >> (offset % 8)) & 1;
            let dot = transformed
                .iter()
                .enumerate()
                .map(|(column, value)| {
                    let mut code = bit(column) << refinement;
                    for lane in 0..refinement {
                        code |= bit(dimension.div_ceil(8) * 8 + column * refinement + lane) << lane;
                    }
                    value * f32::from(code)
                })
                .sum::<f32>();
            hits.push(ProjectionHit {
                id,
                score: dot * segment.reconstruction_scales[row]
                    + query_sum * segment.reconstruction_offsets[row],
            });
        }
    }
    hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.id.cmp(&b.id)));
    hits.truncate(limit);
    hits
}
