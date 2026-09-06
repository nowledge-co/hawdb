//! Un-indexed writes held ahead of an immutable base projection.
//!
//! `InMemoryProjection`/`FileProjection` are built once (via `ProjectionBuilder`
//! / `ProjectionWriter`) and have no update path: every row is an immutable
//! RaBitQ-quantized artifact. `DeltaBuffer` holds recent upserts that
//! have not yet been folded into that base, scored by exact cosine
//! similarity at query time rather than quantized -- the same
//! immutable-base-plus-un-indexed-delta shape used by comparable systems
//! (an append-only base segment with a flat-scanned growing buffer on top).
//!
//! `DeltaBuffer` does not attempt to fold itself into the base: RaBitQ
//! encoding is lossy, so a base row's original vector cannot be recovered
//! from its quantized codes. Folding means the host re-running the existing
//! full-rebuild path (`ProjectionBuilder`/`ProjectionWriter`) over canonical
//! storage for base plus delta, then calling `clear()`. `should_optimize`
//! is the signal for when that is due.
//!
//! `benches/vector_projection_delta_fraction.rs` measured that signal's
//! cost: the delta's exact per-row scan (unquantized, no SIMD, no block
//! skipping) is markedly more expensive per row than the base's quantized
//! scan, so query latency grows fast well before the delta is large --
//! roughly double at just 1% of the base's document count, on an 8,192-row
//! base at dimension 384. `DEFAULT_OPTIMIZE_THRESHOLD` is set low (2%)
//! accordingly; callers whose delta scan is cheaper relative to their base
//! (smaller dimension, larger base) or who can tolerate more latency
//! between rebuilds can raise it via `with_optimize_threshold`.

use crate::error::{ProjectionError, Result};
use crate::scan::{compare_best, ProjectionHit, ProjectionSearchOptions, ProjectionSearchOutput};
use skein_core::RuntimeTaskContext;
use std::collections::HashSet;

const DEFAULT_OPTIMIZE_THRESHOLD: f64 = 0.02;
/// Matches `DEFAULT_BUILD_MEMORY_BYTES`/`DEFAULT_SEARCH_MEMORY_BYTES`
/// elsewhere in this crate.
const DEFAULT_DELTA_MEMORY_BYTES: usize = 64 * 1024 * 1024;
/// Fixed per-entry overhead (id, `Vec` header) beyond the vector payload,
/// folded into the admission estimate.
const DELTA_ENTRY_FIXED_BYTES: usize = 32;

#[derive(Debug, Clone)]
struct DeltaEntry {
    id: u64,
    vector: Vec<f32>,
}

/// A small, unindexed set of upserted vectors awaiting a full rebuild.
#[derive(Debug, Clone)]
pub struct DeltaBuffer {
    dimension: usize,
    optimize_threshold: f64,
    max_working_bytes: usize,
    bytes_used: usize,
    entries: Vec<DeltaEntry>,
}

impl DeltaBuffer {
    pub fn new(dimension: usize) -> Self {
        Self {
            dimension,
            optimize_threshold: DEFAULT_OPTIMIZE_THRESHOLD,
            max_working_bytes: DEFAULT_DELTA_MEMORY_BYTES,
            bytes_used: 0,
            entries: Vec::new(),
        }
    }

    /// Fraction of combined base+delta document count at which
    /// `should_optimize` starts returning true. Default 0.02 (2%); see the
    /// module docs for the benchmark this default was calibrated from.
    pub fn with_optimize_threshold(mut self, optimize_threshold: f64) -> Self {
        self.optimize_threshold = optimize_threshold;
        self
    }

    /// Fail-closed ceiling on `working_bytes()`. `should_optimize` is
    /// meant to trigger a fold well before this is reached in normal
    /// operation; this is the hard backstop for a host that ignores that
    /// signal, not the primary control.
    pub fn with_max_working_bytes(mut self, max_working_bytes: usize) -> Self {
        self.max_working_bytes = max_working_bytes;
        self
    }

    /// Approximate heap bytes retained by this buffer: vector payloads
    /// plus fixed per-entry overhead. Does not include allocator overhead
    /// or `Vec` spare capacity.
    pub fn working_bytes(&self) -> usize {
        self.bytes_used
    }

    fn entry_bytes(&self) -> usize {
        self.dimension
            .saturating_mul(std::mem::size_of::<f32>())
            .saturating_add(DELTA_ENTRY_FIXED_BYTES)
    }

    /// Insert `id`, or replace its vector if already present. Re-upserting
    /// an id already in the buffer keeps the buffer's size (and
    /// `working_bytes()`) unchanged. Fails closed with
    /// `ResourceBudgetExceeded` rather than growing past
    /// `max_working_bytes` when inserting a genuinely new id.
    pub fn upsert(&mut self, id: u64, vector: &[f32]) -> Result<()> {
        if vector.len() != self.dimension {
            return Err(ProjectionError::InvalidVector(format!(
                "expected dimension {}, got {}",
                self.dimension,
                vector.len()
            )));
        }
        if !vector.iter().all(|value| value.is_finite()) {
            return Err(ProjectionError::InvalidVector(
                "coordinates must be finite".to_string(),
            ));
        }
        match self.entries.binary_search_by_key(&id, |entry| entry.id) {
            Ok(index) => vector.clone_into(&mut self.entries[index].vector),
            Err(index) => {
                let entry_bytes = self.entry_bytes();
                let required = self.bytes_used.saturating_add(entry_bytes);
                if required > self.max_working_bytes {
                    return Err(ProjectionError::ResourceBudgetExceeded {
                        required,
                        available: self.max_working_bytes,
                    });
                }
                self.entries.insert(
                    index,
                    DeltaEntry {
                        id,
                        vector: vector.to_vec(),
                    },
                );
                self.bytes_used = required;
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.bytes_used = 0;
    }

    /// Fraction of `base_document_count + len()` this buffer represents.
    pub fn fraction_of(&self, base_document_count: usize) -> f64 {
        let total = base_document_count.saturating_add(self.entries.len());
        if total == 0 {
            0.0
        } else {
            self.entries.len() as f64 / total as f64
        }
    }

    /// Whether the buffer has grown past its configured optimize threshold
    /// relative to `base_document_count` and the host should fold it into
    /// a fresh base rebuild.
    pub fn should_optimize(&self, base_document_count: usize) -> bool {
        self.fraction_of(base_document_count) >= self.optimize_threshold
    }

    /// Exact cosine-similarity scan, comparable to the base projection's
    /// approximate RaBitQ score (see module docs): both approximate
    /// the same cosine-similarity metric, since indexed and query vectors
    /// are unit-normalized before RaBitQ's orthogonal transform.
    ///
    /// Checkpoints once per call, before scanning, rather than once per
    /// entry: the buffer is already bounded by `max_working_bytes`, so one
    /// call's worst case is bounded, and this scan is not a hot inner
    /// loop the way `scan_segment`'s per-block checks are.
    fn scan(
        &self,
        query: &[f32],
        top_k: usize,
        allowed_ids: Option<&[u64]>,
        task_context: Option<&RuntimeTaskContext>,
    ) -> Result<Vec<ProjectionHit>> {
        if let Some(context) = task_context {
            context.checkpoint()?;
        }
        if query.len() != self.dimension {
            return Err(ProjectionError::InvalidVector(format!(
                "expected query dimension {}, got {}",
                self.dimension,
                query.len()
            )));
        }
        let query_norm = l2_norm(query);
        let mut hits: Vec<ProjectionHit> = self
            .entries
            .iter()
            .filter(|entry| {
                allowed_ids.is_none_or(|allowed| allowed.binary_search(&entry.id).is_ok())
            })
            .filter_map(|entry| {
                cosine_similarity(query, query_norm, &entry.vector).map(|score| ProjectionHit {
                    id: entry.id,
                    score,
                })
            })
            .collect();
        hits.sort_by(|left, right| compare_best(right, left));
        hits.truncate(top_k);
        Ok(hits)
    }
}

fn l2_norm(vector: &[f32]) -> f64 {
    vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt()
}

fn cosine_similarity(query: &[f32], query_norm: f64, vector: &[f32]) -> Option<f32> {
    let vector_norm = l2_norm(vector);
    if query_norm <= f64::EPSILON || vector_norm <= f64::EPSILON {
        return None;
    }
    let dot = query
        .iter()
        .zip(vector.iter())
        .map(|(query_value, vector_value)| f64::from(*query_value) * f64::from(*vector_value))
        .sum::<f64>();
    Some((dot / (query_norm * vector_norm)) as f32)
}

/// Result of merging a base projection search with a `DeltaBuffer` scan.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaMergedSearchOutput {
    pub hits: Vec<ProjectionHit>,
    pub base: ProjectionSearchOutput,
    pub delta_document_count: usize,
    /// `delta_document_count / (base.report.document_count + delta_document_count)`.
    pub delta_fraction: f64,
}

/// Search a base projection and a `DeltaBuffer` together, letting delta
/// entries shadow any base row sharing the same id (the delta value is
/// always the more recent write). `base_search` is expected to be
/// `|q, k, o| base.search(q, k, o)` for whichever `InMemoryProjection` or
/// `FileProjection` is being queried.
///
/// Full shadowing headroom is intentional: more than `top_k` high-ranked base
/// rows can be replaced by poor delta vectors. Capping the base request at
/// `2 * top_k` can then discard the best non-shadowed hits. The base search
/// must enforce `options.max_working_bytes`, returning a budget error rather
/// than silently shortening a correct result; callers should rebuild when
/// `DeltaBuffer::should_optimize` signals sustained amplification.
pub fn search_with_delta<F>(
    query: &[f32],
    top_k: usize,
    options: ProjectionSearchOptions<'_>,
    delta: &DeltaBuffer,
    base_search: F,
) -> Result<DeltaMergedSearchOutput>
where
    F: FnOnce(&[f32], usize, ProjectionSearchOptions<'_>) -> Result<ProjectionSearchOutput>,
{
    // Ask the base for extra headroom equal to the delta size, so that
    // filtering out ids the delta shadows still leaves up to top_k results
    // whenever the base has that many non-shadowed candidates.
    let base_top_k = top_k.saturating_add(delta.len());
    let allowed_ids = options.candidates.map(|candidates| candidates.ids);
    let base_output = base_search(query, base_top_k, options)?;
    let delta_hits = delta.scan(query, top_k, allowed_ids, options.task_context)?;

    let delta_ids: HashSet<u64> = delta.entries.iter().map(|entry| entry.id).collect();
    let mut hits: Vec<ProjectionHit> = base_output
        .hits
        .iter()
        .filter(|hit| !delta_ids.contains(&hit.id))
        .cloned()
        .collect();
    hits.extend(delta_hits);
    hits.sort_by(|left, right| compare_best(right, left));
    hits.truncate(top_k);

    let delta_document_count = delta.len();
    let total_document_count = base_output
        .report
        .document_count
        .saturating_add(delta_document_count);
    let delta_fraction = if total_document_count == 0 {
        0.0
    } else {
        delta_document_count as f64 / total_document_count as f64
    };
    Ok(DeltaMergedSearchOutput {
        hits,
        base: base_output,
        delta_document_count,
        delta_fraction,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ProjectionBuildConfig, ProjectionBuilder, ProjectionIdentity};

    #[test]
    fn full_shadowing_headroom_matches_an_independent_merge_oracle() {
        let report = sample_base()
            .search(&[1.0, 0.0, 0.0, 0.0], 0, ProjectionSearchOptions::new())
            .unwrap()
            .report;
        let mut unsafe_cap_cases = 0;
        for seed in 0..32 {
            for top_k in 1..=4 {
                let shadowed = 6 + seed % 9;
                let count = shadowed + top_k + 3;
                let base = (0..count)
                    .map(|id| ProjectionHit {
                        id: id as u64,
                        score: 1.0 - id as f32 / 100.0,
                    })
                    .collect::<Vec<_>>();
                let mut delta = DeltaBuffer::new(2);
                let mut replacements = Vec::new();
                for id in 0..shadowed {
                    let score = if seed % 3 == 0 && id % 2 == 0 {
                        0.0
                    } else {
                        -1.0
                    };
                    let vector = if score == 0.0 {
                        [0.0, 1.0]
                    } else {
                        [-1.0, 0.0]
                    };
                    delta.upsert(id as u64, &vector).unwrap();
                    replacements.push(ProjectionHit {
                        id: id as u64,
                        score,
                    });
                }
                for filtered in [false, true] {
                    let allowed = (0..count as u64)
                        .filter(|id| id % 3 != 0)
                        .collect::<Vec<_>>();
                    let options = if filtered {
                        ProjectionSearchOptions::new().with_allowed_ids(&allowed)
                    } else {
                        ProjectionSearchOptions::new()
                    };
                    let eligible = |id: u64| !filtered || allowed.contains(&id);
                    let base_search =
                        |_: &[f32], requested: usize, _: ProjectionSearchOptions<'_>| {
                            Ok(ProjectionSearchOutput {
                                hits: base
                                    .iter()
                                    .filter(|hit| eligible(hit.id))
                                    .take(requested)
                                    .cloned()
                                    .collect(),
                                report: report.clone(),
                            })
                        };
                    let output =
                        search_with_delta(&[1.0, 0.0], top_k, options, &delta, |q, k, o| {
                            assert_eq!(k, top_k + shadowed);
                            base_search(q, k, o)
                        })
                        .unwrap();
                    // Build the complete latest-value map first, independently
                    // of the merge implementation's post-top-k filtering.
                    let mut latest = base
                        .iter()
                        .map(|hit| (hit.id, hit.score))
                        .collect::<std::collections::BTreeMap<_, _>>();
                    for hit in &replacements {
                        latest.insert(hit.id, hit.score);
                    }
                    let mut expected = latest
                        .into_iter()
                        .filter(|(id, _)| eligible(*id))
                        .map(|(id, score)| ProjectionHit { id, score })
                        .collect::<Vec<_>>();
                    expected.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.id.cmp(&b.id)));
                    expected.truncate(top_k);
                    assert_eq!(
                        output.hits, expected,
                        "seed {seed}, k {top_k}, filtered {filtered}"
                    );
                    let capped =
                        search_with_delta(&[1.0, 0.0], top_k, options, &delta, |q, _, o| {
                            base_search(q, 2 * top_k, o)
                        })
                        .unwrap();
                    unsafe_cap_cases += usize::from(capped.hits != expected);
                }
            }
        }
        assert!(
            unsafe_cap_cases >= 128,
            "only {unsafe_cap_cases} counterexamples"
        );
    }

    #[test]
    fn shadowing_headroom_obeys_base_search_memory_admission() {
        let base = sample_base();
        let mut delta = DeltaBuffer::new(4);
        for id in 0..256 {
            delta.upsert(id, &axis_vector(4, 0)).unwrap();
        }
        let error = search_with_delta(
            &axis_vector(4, 0),
            2,
            ProjectionSearchOptions::new().with_max_working_bytes(1024),
            &delta,
            |q, k, o| base.search(q, k, o),
        )
        .unwrap_err();
        assert!(
            matches!(error, ProjectionError::ResourceBudgetExceeded { .. }),
            "{error}"
        );
        assert_eq!(delta.len(), 256);
    }

    fn axis_vector(dimension: usize, axis: usize) -> Vec<f32> {
        let mut vector = vec![0.0; dimension];
        vector[axis] = 1.0;
        vector
    }

    fn sample_base() -> crate::InMemoryProjection {
        let config = ProjectionBuildConfig::new(4, ProjectionIdentity::new(1));
        let mut builder = ProjectionBuilder::new(config).unwrap();
        builder.push(10, &axis_vector(4, 0)).unwrap();
        builder.push(20, &axis_vector(4, 1)).unwrap();
        builder.finish().unwrap()
    }

    #[test]
    fn delta_hit_shadows_stale_base_row_with_the_same_id() {
        let base = sample_base();
        let mut delta = DeltaBuffer::new(4);
        // id 10 is re-upserted in the delta pointing at a different axis;
        // the merged result must reflect the delta's value, not the base's.
        delta.upsert(10, &axis_vector(4, 3)).unwrap();

        let query = axis_vector(4, 3);
        let output = search_with_delta(
            &query,
            2,
            ProjectionSearchOptions::new().with_kernel(crate::KernelPreference::Scalar),
            &delta,
            |q, k, o| base.search(q, k, o),
        )
        .unwrap();

        assert_eq!(output.hits[0].id, 10);
        assert!(output.hits[0].score > 0.99);
        assert_eq!(output.delta_document_count, 1);
    }

    #[test]
    fn empty_delta_matches_plain_base_search() {
        let base = sample_base();
        let delta = DeltaBuffer::new(4);
        let query = axis_vector(4, 1);

        let output = search_with_delta(
            &query,
            2,
            ProjectionSearchOptions::new().with_kernel(crate::KernelPreference::Scalar),
            &delta,
            |q, k, o| base.search(q, k, o),
        )
        .unwrap();

        assert_eq!(output.hits[0].id, 20);
        assert_eq!(output.delta_document_count, 0);
        assert_eq!(output.delta_fraction, 0.0);
    }

    #[test]
    fn should_optimize_trips_at_the_configured_threshold() {
        let mut delta = DeltaBuffer::new(4).with_optimize_threshold(0.25);
        for id in 0..3 {
            delta.upsert(id, &axis_vector(4, 0)).unwrap();
        }
        // 3 / (9 + 3) = 0.25 -- exactly at threshold.
        assert!(delta.should_optimize(9));
        assert!(!delta.should_optimize(20));
    }

    #[test]
    fn upsert_rejects_dimension_mismatch() {
        let mut delta = DeltaBuffer::new(4);
        let result = delta.upsert(1, &[0.0, 1.0]);
        assert!(matches!(result, Err(ProjectionError::InvalidVector(_))));
    }

    #[test]
    fn upsert_fails_closed_once_the_memory_budget_is_exhausted() {
        // Each entry costs dimension * 4 + 32 fixed bytes = 48 bytes; a
        // 100-byte budget admits exactly two entries.
        let mut delta = DeltaBuffer::new(4).with_max_working_bytes(100);
        delta.upsert(1, &axis_vector(4, 0)).unwrap();
        delta.upsert(2, &axis_vector(4, 1)).unwrap();
        let result = delta.upsert(3, &axis_vector(4, 2));
        assert!(matches!(
            result,
            Err(ProjectionError::ResourceBudgetExceeded { .. })
        ));
        assert_eq!(delta.len(), 2);
    }

    #[test]
    fn re_upserting_an_existing_id_does_not_grow_working_bytes() {
        let mut delta = DeltaBuffer::new(4).with_max_working_bytes(100);
        delta.upsert(1, &axis_vector(4, 0)).unwrap();
        delta.upsert(2, &axis_vector(4, 1)).unwrap();
        let bytes_before = delta.working_bytes();
        // Replacing an existing id must not need new budget headroom, and
        // must not be rejected by the exhausted budget from the test above.
        delta.upsert(1, &axis_vector(4, 3)).unwrap();
        assert_eq!(delta.working_bytes(), bytes_before);
        assert_eq!(delta.len(), 2);
    }

    #[test]
    fn clear_resets_working_bytes() {
        let mut delta = DeltaBuffer::new(4);
        delta.upsert(1, &axis_vector(4, 0)).unwrap();
        assert!(delta.working_bytes() > 0);
        delta.clear();
        assert_eq!(delta.working_bytes(), 0);
    }

    #[test]
    fn search_with_delta_honors_a_cancelled_task_context() {
        use skein_core::{RuntimeCancellationToken, RuntimeTaskContext};

        let base = sample_base();
        let mut delta = DeltaBuffer::new(4);
        delta.upsert(10, &axis_vector(4, 3)).unwrap();
        let token = RuntimeCancellationToken::new();
        let context = RuntimeTaskContext::without_deadline(token.clone());
        token.cancel();

        let result = search_with_delta(
            &axis_vector(4, 3),
            2,
            ProjectionSearchOptions::new()
                .with_kernel(crate::KernelPreference::Scalar)
                .with_task_context(&context),
            &delta,
            |q, k, o| base.search(q, k, o),
        );
        assert!(matches!(result, Err(ProjectionError::Cancelled(_))));
    }
}
