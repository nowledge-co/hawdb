use crate::artifact::{FileProjection, SegmentParts};
use crate::error::{ProjectionError, Result};
use crate::kernel::{score_function, select_kernel, KernelPreference, ScanKernel};
use crate::model::{encoded_vector_bytes, InMemoryProjection, ProjectionManifest, RaBitQBitWidth};
use crate::transform::normalize_and_transform;
use skein_core::RuntimeTaskContext;
use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::Mutex;

const SCAN_BLOCK_ROWS: usize = 32;
const DEFAULT_SEARCH_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const WORKER_FIXED_BYTES: usize = 1_024;
const WORKER_STACK_BYTES: usize = 512 * 1024;
const SEARCH_FIXED_BYTES: usize = 1_024;
const MIN_ALLOWLIST_DOCUMENTS_PER_WORKER: usize = 1_024;

/// A pre-computed set of document IDs that candidate generation has already
/// narrowed the search to, e.g. an ACL/tenant/time-range filter compiled
/// upstream of the scan.
///
/// Today this always wraps a sorted, deduplicated ID slice (`exact: true`):
/// membership in `ids` is the final answer, not a hint. The `exact` flag
/// exists so a future approximate pre-filter -- an IVF/HNSW candidate list,
/// for instance -- can flow through the same `ProjectionSearchOptions` slot
/// and be distinguished in `ProjectionSearchReport` from today's exact
/// allowlists, without another options field or call-site change.
#[derive(Debug, Clone, Copy)]
pub struct CandidateSet<'a> {
    pub ids: &'a [u64],
    pub exact: bool,
}

impl<'a> CandidateSet<'a> {
    /// Wrap a sorted, deduplicated, exact ID allowlist.
    pub fn exact(ids: &'a [u64]) -> Self {
        Self { ids, exact: true }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProjectionSearchOptions<'a> {
    pub max_parallelism: NonZeroUsize,
    pub max_working_bytes: usize,
    pub kernel: KernelPreference,
    pub candidates: Option<CandidateSet<'a>>,
    pub task_context: Option<&'a RuntimeTaskContext>,
}

impl<'a> ProjectionSearchOptions<'a> {
    pub fn new() -> Self {
        Self {
            max_parallelism: NonZeroUsize::MIN,
            max_working_bytes: DEFAULT_SEARCH_MEMORY_BYTES,
            kernel: KernelPreference::Auto,
            candidates: None,
            task_context: None,
        }
    }

    pub fn with_max_parallelism(mut self, max_parallelism: NonZeroUsize) -> Self {
        self.max_parallelism = max_parallelism;
        self
    }

    pub fn with_max_working_bytes(mut self, max_working_bytes: usize) -> Self {
        self.max_working_bytes = max_working_bytes;
        self
    }

    pub fn with_kernel(mut self, kernel: KernelPreference) -> Self {
        self.kernel = kernel;
        self
    }

    /// Restrict the search to an exact, sorted, deduplicated ID allowlist.
    pub fn with_allowed_ids(mut self, allowed_ids: &'a [u64]) -> Self {
        self.candidates = Some(CandidateSet::exact(allowed_ids));
        self
    }

    pub fn with_candidate_set(mut self, candidates: CandidateSet<'a>) -> Self {
        self.candidates = Some(candidates);
        self
    }

    pub fn with_task_context(mut self, task_context: &'a RuntimeTaskContext) -> Self {
        self.task_context = Some(task_context);
        self
    }
}

impl Default for ProjectionSearchOptions<'_> {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionHit {
    pub id: u64,
    pub score: f32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSearchReport {
    pub kernel: ScanKernel,
    pub worker_count: usize,
    pub segment_count: usize,
    pub scanned_segment_count: usize,
    pub document_count: usize,
    pub scored_document_count: usize,
    pub filtered_document_count: usize,
    pub scanned_block_count: usize,
    pub skipped_block_count: usize,
    pub payload_bytes_read: u64,
    pub admitted_working_bytes: usize,
    pub candidate_count: usize,
    /// `None` when no candidate set was supplied; otherwise mirrors
    /// `CandidateSet::exact` for the set that was actually applied.
    pub candidate_set_exact: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionSearchOutput {
    pub hits: Vec<ProjectionHit>,
    pub report: ProjectionSearchReport,
}

/// The physical data plane a query scans: given a segment index, produce
/// that segment's scored candidates. `InMemoryProjection` and
/// `FileProjection` are today's only two backends; a future mmap-range or
/// remote-object backend would implement this same boundary rather than
/// growing another bespoke `search` method.
trait SegmentReader: Sync {
    fn manifest(&self) -> &ProjectionManifest;

    /// Upper bound on any single segment's row count, used to size the
    /// per-worker allowlist mask budget before scanning starts.
    fn max_segment_rows(&self) -> usize;

    /// Upper bound on any single segment's on-disk payload size, used to
    /// size the per-worker read-buffer budget. Zero for backends (e.g.
    /// in-memory) that do not materialize a transient per-read buffer.
    fn max_segment_payload_bytes(&self) -> usize;

    #[allow(clippy::too_many_arguments)]
    fn scan_segment(
        &self,
        segment_index: usize,
        query: &[f32],
        top_k: usize,
        kernel: ScanKernel,
        allowed_ids: Option<&[u64]>,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<SegmentSearchResult>;
}

impl SegmentReader for InMemoryProjection {
    fn manifest(&self) -> &ProjectionManifest {
        &self.manifest
    }

    fn max_segment_rows(&self) -> usize {
        self.segments
            .iter()
            .map(|segment| segment.row_count())
            .max()
            .unwrap_or(0)
    }

    fn max_segment_payload_bytes(&self) -> usize {
        0
    }

    fn scan_segment(
        &self,
        segment_index: usize,
        query: &[f32],
        top_k: usize,
        kernel: ScanKernel,
        allowed_ids: Option<&[u64]>,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<SegmentSearchResult> {
        let segment = &self.segments[segment_index];
        scan_segment(
            segment.row_count(),
            self.manifest.dimension,
            RaBitQBitWidth::from_bits(self.manifest.bit_width)?,
            |row| segment.ids[row],
            |row| segment.reconstruction_scales[row],
            |row| segment.reconstruction_offsets[row],
            &segment.codes,
            query,
            top_k,
            kernel,
            allowed_ids,
            context,
            0,
        )
    }
}

impl SegmentReader for FileProjection {
    fn manifest(&self) -> &ProjectionManifest {
        FileProjection::manifest(self)
    }

    fn max_segment_rows(&self) -> usize {
        self.manifest()
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .max()
            .unwrap_or(0)
    }

    fn max_segment_payload_bytes(&self) -> usize {
        self.manifest()
            .segments
            .iter()
            .map(|segment| usize::try_from(segment.payload_bytes).unwrap_or(usize::MAX))
            .max()
            .unwrap_or(0)
    }

    fn scan_segment(
        &self,
        segment_index: usize,
        query: &[f32],
        top_k: usize,
        kernel: ScanKernel,
        allowed_ids: Option<&[u64]>,
        context: Option<&RuntimeTaskContext>,
    ) -> Result<SegmentSearchResult> {
        let descriptor = &self.manifest().segments[segment_index];
        let buffer = self.read_segment(segment_index)?;
        let bit_width = RaBitQBitWidth::from_bits(self.manifest().bit_width)?;
        let parts = buffer.parts(self.manifest().dimension, bit_width, descriptor.row_count)?;
        scan_file_segment(
            parts,
            descriptor.row_count,
            self.manifest().dimension,
            bit_width,
            query,
            top_k,
            kernel,
            allowed_ids,
            context,
            descriptor.payload_bytes,
        )
    }
}

impl InMemoryProjection {
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> Result<ProjectionSearchOutput> {
        search_projection(self, query, top_k, options)
    }
}

impl FileProjection {
    pub fn search(
        &self,
        query: &[f32],
        top_k: usize,
        options: ProjectionSearchOptions<'_>,
    ) -> Result<ProjectionSearchOutput> {
        search_projection(self, query, top_k, options)
    }
}

fn search_projection<R: SegmentReader>(
    reader: &R,
    query: &[f32],
    top_k: usize,
    options: ProjectionSearchOptions<'_>,
) -> Result<ProjectionSearchOutput> {
    let manifest = reader.manifest();
    let max_segment_rows = reader.max_segment_rows();
    let max_segment_payload_bytes = reader.max_segment_payload_bytes();
    if query.len() != manifest.dimension {
        return Err(ProjectionError::InvalidVector(format!(
            "expected query dimension {}, got {}",
            manifest.dimension,
            query.len()
        )));
    }
    let allowed_ids: Option<&[u64]> = options.candidates.map(|candidates| candidates.ids);
    let candidate_set_exact = options.candidates.map(|candidates| candidates.exact);
    if allowed_ids.is_some_and(|ids| ids.windows(2).any(|pair| pair[0] >= pair[1])) {
        return Err(ProjectionError::InvalidConfiguration(
            "allowed ids must be sorted and unique".to_string(),
        ));
    }
    if let Some(context) = options.task_context {
        context.checkpoint()?;
    }
    let kernel = select_kernel(options.kernel)?;
    let mut transformed_query = vec![0.0; manifest.dimension];
    normalize_and_transform(query, manifest.transform_seed, &mut transformed_query)?;
    let segment_count = manifest.segments.len();
    if top_k == 0 || segment_count == 0 || allowed_ids.is_some_and(<[u64]>::is_empty) {
        return Ok(ProjectionSearchOutput {
            hits: Vec::new(),
            report: ProjectionSearchReport {
                kernel,
                worker_count: 0,
                segment_count,
                scanned_segment_count: 0,
                document_count: manifest.document_count,
                scored_document_count: 0,
                filtered_document_count: manifest.document_count,
                scanned_block_count: 0,
                skipped_block_count: manifest.document_count.div_ceil(SCAN_BLOCK_ROWS),
                payload_bytes_read: 0,
                admitted_working_bytes: transformed_query
                    .len()
                    .saturating_mul(std::mem::size_of::<f32>()),
                candidate_count: 0,
                candidate_set_exact,
            },
        });
    }

    let query_bytes = transformed_query
        .len()
        .saturating_mul(std::mem::size_of::<f32>());
    let mask_bytes = allowed_ids.map_or(0, |_| {
        max_segment_rows.div_ceil(u64::BITS as usize) * std::mem::size_of::<u64>()
    });
    let top_k_bytes = top_k.saturating_mul(std::mem::size_of::<ProjectionHit>());
    let per_worker_bytes = max_segment_payload_bytes
        .saturating_add(mask_bytes)
        .saturating_add(top_k_bytes)
        .saturating_add(WORKER_STACK_BYTES)
        .saturating_add(WORKER_FIXED_BYTES);
    let global_top_k_bytes = top_k.saturating_mul(std::mem::size_of::<ProjectionHit>());
    let global_bytes = query_bytes
        .saturating_add(global_top_k_bytes)
        .saturating_add(SEARCH_FIXED_BYTES);
    let available_for_workers = options.max_working_bytes.saturating_sub(global_bytes);
    let admitted_by_memory = available_for_workers / per_worker_bytes.max(1);
    if admitted_by_memory == 0 {
        return Err(ProjectionError::ResourceBudgetExceeded {
            required: global_bytes.saturating_add(per_worker_bytes),
            available: options.max_working_bytes,
        });
    }
    let admitted_by_allowlist = allowed_ids.map_or(usize::MAX, |allowed| {
        allowed
            .len()
            .div_ceil(MIN_ALLOWLIST_DOCUMENTS_PER_WORKER)
            .max(1)
    });
    let worker_count = options
        .max_parallelism
        .get()
        .min(
            options
                .task_context
                .map_or(usize::MAX, |context| context.admitted_parallelism().get()),
        )
        .min(segment_count)
        .min(admitted_by_memory)
        .min(admitted_by_allowlist)
        .max(1);
    let admitted_working_bytes = global_bytes.saturating_add(worker_count * per_worker_bytes);

    let report = Mutex::new(ReportAccumulator::default());
    let first_error = Mutex::new(None);
    let stopped = AtomicBool::new(false);
    let next_segment = AtomicUsize::new(0);

    // Each worker accumulates its own bounded top-k with no cross-thread
    // locking on the scan hot path; the per-worker heaps are merged into one
    // final top-k only once, after every worker has finished.
    let run_worker = || -> TopK {
        let mut local_top_k = TopK::new(top_k);
        loop {
            if stopped.load(AtomicOrdering::Acquire) {
                break;
            }
            if let Some(context) = options.task_context
                && let Err(reason) = context.checkpoint()
            {
                store_error(&first_error, &stopped, ProjectionError::Cancelled(reason));
                break;
            }
            let segment_index = next_segment.fetch_add(1, AtomicOrdering::Relaxed);
            if segment_index >= segment_count {
                break;
            }
            match reader.scan_segment(
                segment_index,
                &transformed_query,
                top_k,
                kernel,
                allowed_ids,
                options.task_context,
            ) {
                Ok(segment_result) => {
                    report
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .add(&segment_result);
                    local_top_k.extend(segment_result.hits);
                }
                Err(error) => {
                    store_error(&first_error, &stopped, error);
                    break;
                }
            }
        }
        local_top_k
    };

    let mut merged_top_k = TopK::new(top_k);
    if worker_count == 1 {
        merged_top_k = run_worker();
    } else {
        let panic_payload = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(worker_count);
            for worker in 0..worker_count {
                match std::thread::Builder::new()
                    .name(format!("skein-rabitq-scan-{worker}"))
                    .stack_size(WORKER_STACK_BYTES)
                    .spawn_scoped(scope, run_worker)
                {
                    Ok(handle) => handles.push(handle),
                    Err(error) => {
                        store_error(&first_error, &stopped, ProjectionError::Io(error));
                        break;
                    }
                }
            }
            let mut panic_payload = None;
            for handle in handles {
                match handle.join() {
                    Ok(local_top_k) => merged_top_k.extend(local_top_k.into_hits()),
                    Err(payload) => {
                        panic_payload.get_or_insert(payload);
                    }
                }
            }
            panic_payload
        });
        if let Some(payload) = panic_payload {
            std::panic::resume_unwind(payload);
        }
    }

    if let Some(error) = first_error
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
    {
        return Err(error);
    }
    if let Some(context) = options.task_context {
        context.checkpoint()?;
    }
    let hits = merged_top_k.finish();
    let accumulated = report
        .into_inner()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    Ok(ProjectionSearchOutput {
        report: ProjectionSearchReport {
            kernel,
            worker_count,
            segment_count,
            scanned_segment_count: accumulated.scanned_segment_count,
            document_count: manifest.document_count,
            scored_document_count: accumulated.scored_document_count,
            filtered_document_count: accumulated.filtered_document_count,
            scanned_block_count: accumulated.scanned_block_count,
            skipped_block_count: accumulated.skipped_block_count,
            payload_bytes_read: accumulated.payload_bytes_read,
            admitted_working_bytes,
            candidate_count: hits.len(),
            candidate_set_exact,
        },
        hits,
    })
}

#[allow(clippy::too_many_arguments)]
fn scan_file_segment(
    parts: SegmentParts<'_>,
    rows: usize,
    dimension: usize,
    bit_width: RaBitQBitWidth,
    query: &[f32],
    top_k: usize,
    kernel: ScanKernel,
    allowed_ids: Option<&[u64]>,
    context: Option<&RuntimeTaskContext>,
    payload_bytes_read: u64,
) -> Result<SegmentSearchResult> {
    scan_segment(
        rows,
        dimension,
        bit_width,
        |row| parts.id(row),
        |row| parts.reconstruction_scale(row),
        |row| parts.reconstruction_offset(row),
        parts.codes,
        query,
        top_k,
        kernel,
        allowed_ids,
        context,
        payload_bytes_read,
    )
}

#[allow(clippy::too_many_arguments)]
fn scan_segment<Id, Scale, Offset>(
    rows: usize,
    dimension: usize,
    bit_width: RaBitQBitWidth,
    id_at: Id,
    scale_at: Scale,
    offset_at: Offset,
    codes: &[u8],
    query: &[f32],
    top_k: usize,
    kernel: ScanKernel,
    allowed_ids: Option<&[u64]>,
    context: Option<&RuntimeTaskContext>,
    payload_bytes_read: u64,
) -> Result<SegmentSearchResult>
where
    Id: Fn(usize) -> u64,
    Scale: Fn(usize) -> f32,
    Offset: Fn(usize) -> f32,
{
    let bytes_per_vector = encoded_vector_bytes(dimension, bit_width);
    if codes.len() != rows.saturating_mul(bytes_per_vector) {
        return Err(ProjectionError::CorruptArtifact(
            "segment code length does not match rows and dimension".to_string(),
        ));
    }
    let mask = allowed_ids.map(|allowed| build_allowed_mask(rows, &id_at, allowed));
    let score = score_function(kernel);
    let query_sum = query.iter().sum::<f32>();
    let mut top = TopK::new(top_k);
    let mut scored_document_count = 0usize;
    let mut scanned_block_count = 0usize;
    let mut skipped_block_count = 0usize;
    for block_start in (0..rows).step_by(SCAN_BLOCK_ROWS) {
        if let Some(context) = context {
            context.checkpoint()?;
        }
        let block_end = (block_start + SCAN_BLOCK_ROWS).min(rows);
        if mask
            .as_deref()
            .is_some_and(|mask| !mask_has_any(mask, block_start, block_end))
        {
            skipped_block_count = skipped_block_count.saturating_add(1);
            continue;
        }
        scanned_block_count = scanned_block_count.saturating_add(1);
        let mut score_row = |row: usize| -> Result<()> {
            let scale = scale_at(row);
            if !scale.is_finite() || scale < 0.0 {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "row {row} has an invalid RaBitQ reconstruction scale"
                )));
            }
            let offset = offset_at(row);
            if !offset.is_finite() {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "row {row} has an invalid RaBitQ reconstruction offset"
                )));
            }
            let start = row * bytes_per_vector;
            let score = score(&codes[start..start + bytes_per_vector], query, bit_width) * scale
                + (offset * query_sum);
            if !score.is_finite() {
                return Err(ProjectionError::CorruptArtifact(format!(
                    "row {row} produced a non-finite score"
                )));
            }
            top.push(ProjectionHit {
                id: id_at(row),
                score,
            });
            scored_document_count = scored_document_count.saturating_add(1);
            Ok(())
        };
        match mask.as_deref() {
            Some(mask) => {
                for_each_selected_row(mask, block_start, block_end, &mut score_row)?;
            }
            None => {
                for row in block_start..block_end {
                    score_row(row)?;
                }
            }
        }
    }
    Ok(SegmentSearchResult {
        hits: top.finish(),
        scanned_segment_count: 1,
        scored_document_count,
        filtered_document_count: rows.saturating_sub(scored_document_count),
        scanned_block_count,
        skipped_block_count,
        payload_bytes_read,
    })
}

fn mask_has_any(mask: &[u64], start: usize, end: usize) -> bool {
    mask_words(mask, start, end).any(|(_, selected, _)| selected != 0)
}

fn build_allowed_mask<Id>(rows: usize, id_at: &Id, allowed: &[u64]) -> Vec<u64>
where
    Id: Fn(usize) -> u64,
{
    let mut words = vec![0u64; rows.div_ceil(u64::BITS as usize)];
    if rows == 0 || allowed.is_empty() {
        return words;
    }
    let first_id = id_at(0);
    let mut allowed_index = allowed.partition_point(|id| *id < first_id);
    for row in 0..rows {
        if allowed_index == allowed.len() {
            break;
        }
        let id = id_at(row);
        while allowed_index < allowed.len() && allowed[allowed_index] < id {
            allowed_index += 1;
        }
        if allowed_index < allowed.len() && allowed[allowed_index] == id {
            words[row / u64::BITS as usize] |= 1u64 << (row % u64::BITS as usize);
            allowed_index += 1;
        }
    }
    words
}

fn for_each_selected_row(
    mask: &[u64],
    start: usize,
    end: usize,
    visit: &mut impl FnMut(usize) -> Result<()>,
) -> Result<()> {
    for (word_start, mut selected, valid) in mask_words(mask, start, end) {
        if selected == 0 {
            continue;
        }
        if selected == valid {
            let first = valid.trailing_zeros() as usize;
            let last = (u64::BITS - valid.leading_zeros()) as usize;
            for bit in first..last {
                visit(word_start + bit)?;
            }
            continue;
        }
        while selected != 0 {
            let bit = selected.trailing_zeros() as usize;
            visit(word_start + bit)?;
            selected &= selected - 1;
        }
    }
    Ok(())
}

fn mask_words(
    mask: &[u64],
    start: usize,
    end: usize,
) -> impl Iterator<Item = (usize, u64, u64)> + '_ {
    let first_word = start / u64::BITS as usize;
    let end_word = end.div_ceil(u64::BITS as usize);
    (first_word..end_word).map(move |word_index| {
        let word_start = word_index * u64::BITS as usize;
        let first_bit = start.saturating_sub(word_start).min(u64::BITS as usize);
        let end_bit = end.saturating_sub(word_start).min(u64::BITS as usize);
        let below_end = if end_bit == u64::BITS as usize {
            u64::MAX
        } else {
            (1u64 << end_bit) - 1
        };
        let below_start = if first_bit == u64::BITS as usize {
            u64::MAX
        } else {
            (1u64 << first_bit) - 1
        };
        let valid = below_end & !below_start;
        let selected = mask.get(word_index).copied().unwrap_or(0) & valid;
        (word_start, selected, valid)
    })
}

fn store_error(
    first_error: &Mutex<Option<ProjectionError>>,
    stopped: &AtomicBool,
    error: ProjectionError,
) {
    let mut slot = first_error
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if slot.is_none() {
        *slot = Some(error);
    }
    stopped.store(true, AtomicOrdering::Release);
}

#[derive(Debug)]
struct SegmentSearchResult {
    hits: Vec<ProjectionHit>,
    scanned_segment_count: usize,
    scored_document_count: usize,
    filtered_document_count: usize,
    scanned_block_count: usize,
    skipped_block_count: usize,
    payload_bytes_read: u64,
}

#[derive(Debug, Default)]
struct ReportAccumulator {
    scanned_segment_count: usize,
    scored_document_count: usize,
    filtered_document_count: usize,
    scanned_block_count: usize,
    skipped_block_count: usize,
    payload_bytes_read: u64,
}

impl ReportAccumulator {
    fn add(&mut self, result: &SegmentSearchResult) {
        self.scanned_segment_count = self
            .scanned_segment_count
            .saturating_add(result.scanned_segment_count);
        self.scored_document_count = self
            .scored_document_count
            .saturating_add(result.scored_document_count);
        self.filtered_document_count = self
            .filtered_document_count
            .saturating_add(result.filtered_document_count);
        self.scanned_block_count = self
            .scanned_block_count
            .saturating_add(result.scanned_block_count);
        self.skipped_block_count = self
            .skipped_block_count
            .saturating_add(result.skipped_block_count);
        self.payload_bytes_read = self
            .payload_bytes_read
            .saturating_add(result.payload_bytes_read);
    }
}

#[derive(Debug)]
struct TopK {
    limit: usize,
    heap: BinaryHeap<Reverse<RankedHit>>,
}

#[derive(Debug)]
struct RankedHit(ProjectionHit);

impl PartialEq for RankedHit {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for RankedHit {}

impl PartialOrd for RankedHit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RankedHit {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_best(&self.0, &other.0)
    }
}

impl TopK {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            heap: BinaryHeap::with_capacity(limit),
        }
    }

    fn push(&mut self, hit: ProjectionHit) {
        if self.limit == 0 {
            return;
        }
        let candidate = RankedHit(hit);
        if self.heap.len() < self.limit {
            self.heap.push(Reverse(candidate));
            return;
        }

        // Reverse keeps the worst retained hit at the root, so each candidate
        // takes O(log k) instead of scanning all k retained hits.
        if self
            .heap
            .peek()
            .is_some_and(|Reverse(worst)| candidate.cmp(worst).is_gt())
        {
            self.heap.pop();
            self.heap.push(Reverse(candidate));
        }
    }

    fn extend(&mut self, hits: Vec<ProjectionHit>) {
        for hit in hits {
            self.push(hit);
        }
    }

    fn into_hits(self) -> Vec<ProjectionHit> {
        self.heap
            .into_iter()
            .map(|Reverse(RankedHit(hit))| hit)
            .collect()
    }

    fn finish(self) -> Vec<ProjectionHit> {
        let mut hits = self.into_hits();
        hits.sort_by(|left, right| compare_best(right, left));
        hits
    }
}

pub(crate) fn compare_best(left: &ProjectionHit, right: &ProjectionHit) -> Ordering {
    left.score
        .total_cmp(&right.score)
        .then_with(|| right.id.cmp(&left.id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        FileProjection, ProjectionBuildConfig, ProjectionBuilder, ProjectionIdentity,
        ProjectionWriter, RaBitQBitWidth,
    };
    use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn bounded_top_k_matches_full_sort_for_every_limit() {
        let scores = [
            f32::NEG_INFINITY,
            -3.0,
            -0.0,
            0.0,
            1.0,
            1.0,
            7.5,
            f32::INFINITY,
            f32::NAN,
        ];
        let hits = (0..257u64)
            .map(|id| ProjectionHit {
                id,
                score: scores[(id as usize * 17 + 3) % scores.len()],
            })
            .collect::<Vec<_>>();

        for limit in 0..=hits.len() + 1 {
            let mut expected = hits.clone();
            expected.sort_by(|left, right| compare_best(right, left));
            expected.truncate(limit);

            let mut top_k = TopK::new(limit);
            top_k.extend(hits.clone());

            let actual = top_k.finish();
            assert_eq!(actual.len(), expected.len(), "limit={limit}");
            for (actual, expected) in actual.iter().zip(&expected) {
                assert_eq!(actual.id, expected.id, "limit={limit}");
                assert_eq!(
                    actual.score.to_bits(),
                    expected.score.to_bits(),
                    "limit={limit}, id={}",
                    actual.id
                );
            }
        }
    }

    #[test]
    fn scalar_projection_finds_nearest_vector_and_honors_filter() {
        let projection = sample_in_memory_projection(8, 2);
        let query = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let output = projection
            .search(
                &query,
                2,
                ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar),
            )
            .unwrap();
        assert_eq!(output.hits[0].id, 10);

        let allowed = [20];
        let filtered = projection
            .search(
                &query,
                2,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_allowed_ids(&allowed),
            )
            .unwrap();
        assert_eq!(filtered.hits.len(), 1);
        assert_eq!(filtered.hits[0].id, 20);
        assert_eq!(filtered.report.scored_document_count, 1);
    }

    #[test]
    fn allowlist_must_be_sorted_and_unique() {
        let projection = sample_in_memory_projection(8, 2);
        let allowed = [20, 10];
        let result = projection.search(
            &[1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            2,
            ProjectionSearchOptions::new().with_allowed_ids(&allowed),
        );

        assert!(matches!(
            result,
            Err(ProjectionError::InvalidConfiguration(message))
                if message == "allowed ids must be sorted and unique"
        ));
    }

    #[test]
    fn linear_allowlist_mask_and_word_iteration_cover_sparse_and_dense_ranges() {
        let ids = (100..230u64).collect::<Vec<_>>();
        let allowed = [99, 100, 102, 163, 164, 165, 228, 229, 300];
        let mask = build_allowed_mask(ids.len(), &|row| ids[row], &allowed);
        let mut selected = Vec::new();
        for_each_selected_row(&mask, 1, 129, &mut |row| {
            selected.push(row);
            Ok(())
        })
        .unwrap();
        assert_eq!(selected, vec![2, 63, 64, 65, 128]);

        let dense_allowed = ids.clone();
        let dense_mask = build_allowed_mask(ids.len(), &|row| ids[row], &dense_allowed);
        let mut dense = Vec::new();
        for_each_selected_row(&dense_mask, 5, 70, &mut |row| {
            dense.push(row);
            Ok(())
        })
        .unwrap();
        assert_eq!(dense, (5..70).collect::<Vec<_>>());
    }

    #[test]
    fn search_budget_accounts_for_global_top_k_and_worker_stack() {
        let projection = sample_in_memory_projection(8, 2);
        let query = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0];
        let query_bytes = query.len() * std::mem::size_of::<f32>();
        let top_k_bytes = 2 * std::mem::size_of::<ProjectionHit>();
        let required = query_bytes
            + top_k_bytes
            + SEARCH_FIXED_BYTES
            + top_k_bytes
            + WORKER_STACK_BYTES
            + WORKER_FIXED_BYTES;
        let result = projection.search(
            &query,
            2,
            ProjectionSearchOptions::new().with_max_working_bytes(required - 1),
        );

        assert!(matches!(
            result,
            Err(ProjectionError::ResourceBudgetExceeded {
                required: actual,
                available
            }) if actual == required && available == required - 1
        ));
    }

    #[test]
    fn parallel_file_scan_matches_sequential_scan() {
        let root = unique_test_dir("parallel");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.skein");
        let config =
            ProjectionBuildConfig::new(64, ProjectionIdentity::new(1)).with_segment_rows(3);
        let mut writer = ProjectionWriter::create(&artifact, config).unwrap();
        for id in 0..30u64 {
            let vector = (0..64)
                .map(|dimension| ((id * 17 + dimension as u64 * 13) as f32).sin())
                .collect::<Vec<_>>();
            writer.push(id, &vector).unwrap();
        }
        let projection = writer.finish().unwrap();
        let query = (0..64)
            .map(|dimension| (dimension as f32 * 0.31).cos())
            .collect::<Vec<_>>();
        let sequential = projection
            .search(
                &query,
                7,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_max_parallelism(NonZeroUsize::MIN),
            )
            .unwrap();
        let parallel = projection
            .search(
                &query,
                7,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_max_parallelism(NonZeroUsize::new(4).unwrap()),
            )
            .unwrap();
        assert_eq!(parallel.hits, sequential.hits);
        assert_eq!(parallel.report.worker_count, 4);

        let context =
            RuntimeTaskContext::default().with_admitted_parallelism(NonZeroUsize::new(2).unwrap());
        let governed = projection
            .search(
                &query,
                7,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_max_parallelism(NonZeroUsize::new(4).unwrap())
                    .with_task_context(&context),
            )
            .unwrap();
        assert_eq!(governed.hits, sequential.hits);
        assert_eq!(governed.report.worker_count, 2);

        let sparse_allowed = [0];
        let sparse = projection
            .search(
                &query,
                7,
                ProjectionSearchOptions::new()
                    .with_kernel(KernelPreference::Scalar)
                    .with_max_parallelism(NonZeroUsize::new(4).unwrap())
                    .with_allowed_ids(&sparse_allowed),
            )
            .unwrap();
        assert_eq!(sparse.report.worker_count, 1);
        assert_eq!(sparse.report.scored_document_count, 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cancellation_stops_between_scan_blocks() {
        let projection = sample_in_memory_projection(64, 1);
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        token.cancel();
        let result = projection.search(
            &[1.0; 64],
            2,
            ProjectionSearchOptions::new().with_task_context(&context),
        );
        assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
    }

    #[test]
    fn auto_kernel_matches_scalar_top_k() {
        let dimension = 96;
        let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1));
        let mut builder = ProjectionBuilder::new(config).unwrap();
        for id in 0..128u64 {
            let vector = (0..dimension)
                .map(|coordinate| ((id * 19 + coordinate as u64 * 7) as f32).cos())
                .collect::<Vec<_>>();
            builder.push(id, &vector).unwrap();
        }
        let projection = builder.finish().unwrap();
        let query = (0..dimension)
            .map(|coordinate| (coordinate as f32 * 0.23).sin())
            .collect::<Vec<_>>();
        let scalar = projection
            .search(
                &query,
                10,
                ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar),
            )
            .unwrap();
        let automatic = projection
            .search(&query, 10, ProjectionSearchOptions::new())
            .unwrap();
        assert_eq!(
            automatic.hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            scalar.hits.iter().map(|hit| hit.id).collect::<Vec<_>>()
        );
        for (automatic, scalar) in automatic.hits.iter().zip(&scalar.hits) {
            assert!((automatic.score - scalar.score).abs() < 1e-4);
        }
    }

    #[test]
    fn one_bit_file_and_in_memory_scans_have_identical_candidates() {
        let dimension = 9;
        let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1))
            .with_bit_width(RaBitQBitWidth::One)
            .with_segment_rows(2);
        let mut builder = ProjectionBuilder::new(config.clone()).unwrap();
        let root = unique_test_dir("one-bit");
        fs::create_dir_all(&root).unwrap();
        let artifact = root.join("search_rabitq.1.skein");
        let mut writer = ProjectionWriter::create(&artifact, config).unwrap();
        for id in 0..5u64 {
            let vector = (0..dimension)
                .map(|coordinate| ((id * 11 + coordinate as u64 * 5) as f32).sin())
                .collect::<Vec<_>>();
            builder.push(id, &vector).unwrap();
            writer.push(id, &vector).unwrap();
        }
        let in_memory = builder.finish().unwrap();
        let file = writer.finish().unwrap();
        let query = (0..dimension)
            .map(|coordinate| (coordinate as f32 * 0.37).cos())
            .collect::<Vec<_>>();
        let options = ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar);
        let in_memory = in_memory.search(&query, 3, options).unwrap();
        let file = file
            .search(
                &query,
                3,
                ProjectionSearchOptions::new().with_kernel(KernelPreference::Scalar),
            )
            .unwrap();

        assert_eq!(file.hits, in_memory.hits);
        fs::remove_dir_all(root).unwrap();
    }

    fn sample_in_memory_projection(dimension: usize, segment_rows: usize) -> InMemoryProjection {
        let config = ProjectionBuildConfig::new(dimension, ProjectionIdentity::new(1))
            .with_segment_rows(segment_rows);
        let mut builder = ProjectionBuilder::new(config).unwrap();
        let mut first = vec![0.0; dimension];
        first[0] = 1.0;
        let mut second = vec![0.0; dimension];
        second[1] = 1.0;
        let mut third = vec![0.0; dimension];
        third[2] = 1.0;
        builder.push(10, &first).unwrap();
        builder.push(20, &second).unwrap();
        builder.push(30, &third).unwrap();
        builder.finish().unwrap()
    }

    fn unique_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "skein_vector_scan_{name}_{}_{nanos}",
            std::process::id()
        ))
    }

    #[allow(dead_code)]
    fn assert_file_projection_is_send_sync(_: &FileProjection) {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<FileProjection>();
    }
}
