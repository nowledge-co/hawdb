use super::*;
use crate::{RuntimeCancellationToken, SearchQueryOptions};
use std::cell::{Cell, RefCell};
use std::num::NonZeroU64;

mod fuzz;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::out_of_core) struct Evidence {
    pub ranks: usize,
    pub heaps: usize,
    pub pages: usize,
}
thread_local! {
    static EVIDENCE: Cell<Evidence> = Cell::default();
    static HEAP_CAPACITY: Cell<usize> = const { Cell::new(0) };
    static CANCEL_PAGE: RefCell<Option<RuntimeCancellationToken>> = const { RefCell::new(None) };
}
pub(super) fn record_ranks() {
    let mut value = EVIDENCE.get();
    value.ranks += 1;
    EVIDENCE.set(value);
}
pub(super) fn record_heap() {
    let mut value = EVIDENCE.get();
    value.heaps += 1;
    EVIDENCE.set(value);
}
pub(super) fn record_page() {
    let mut value = EVIDENCE.get();
    value.pages += 1;
    EVIDENCE.set(value);
    CANCEL_PAGE.with_borrow_mut(|token| {
        if let Some(token) = token.take() {
            token.cancel();
        }
    });
}
pub(super) fn record_heap_capacity(capacity: usize) {
    HEAP_CAPACITY.set(capacity);
}
pub(in crate::out_of_core) fn take() -> Evidence {
    EVIDENCE.replace(Evidence::default())
}

fn memory(bytes: usize) -> QueryMemory {
    QueryMemory::new(NonZeroU64::new(bytes as u64).unwrap(), None).unwrap()
}
fn scores(values: &[(&str, f64)]) -> BTreeMap<String, f64> {
    values
        .iter()
        .map(|(id, score)| ((*id).to_owned(), *score))
        .collect()
}
fn options(offset: usize, limit: usize, window: Option<usize>) -> SearchQueryOptions {
    SearchQueryOptions {
        offset,
        limit,
        rank_window: window,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    }
}

// Quadratic pairwise ranking is independent of the production sorted-index
// representation. Full union/sort is intentionally retained only in this oracle.
fn oracle(
    vector: &BTreeMap<String, f64>,
    text: &BTreeMap<String, f64>,
    mode: SearchMode,
    options: &SearchQueryOptions,
) -> (usize, Vec<SearchScoredCandidate>) {
    let rank = |scores: &BTreeMap<String, f64>, id: &str| {
        scores.get(id).and_then(|score| {
            let rank = 1 + scores
                .iter()
                .filter(|(other_id, other_score)| {
                    **other_score > *score || (**other_score == *score && other_id.as_str() < id)
                })
                .count();
            (mode != SearchMode::Hybrid || options.rank_window.is_none_or(|end| rank <= end))
                .then_some(rank)
        })
    };
    let ids = vector
        .keys()
        .chain(text.keys())
        .collect::<std::collections::BTreeSet<_>>();
    let mut all = Vec::new();
    for id in ids {
        let vector_rank = rank(vector, id);
        let text_rank = rank(text, id);
        let child = |rank: Option<usize>| rank.map_or(0.0, |value| 1.0 / (60.0 + value as f64));
        let vector_rrf_score = child(vector_rank);
        let text_rrf_score = child(text_rank);
        let rrf_score = vector_rrf_score * options.fusion_weights.vector_weight
            + text_rrf_score * options.fusion_weights.text_weight;
        let vector_score = vector.get(id).copied().unwrap_or(0.0);
        let text_score = text.get(id).copied().unwrap_or(0.0);
        let score = match mode {
            SearchMode::Hybrid => rrf_score,
            SearchMode::Vector => vector_score,
            SearchMode::Text => text_score,
        };
        if score > 0.0 {
            all.push(SearchScoredCandidate {
                id: id.clone(),
                score,
                vector_score,
                text_score,
                rrf_score,
                vector_rrf_score,
                text_rrf_score,
                vector_rank,
                text_rank,
            });
        }
    }
    all.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap()
            .then_with(|| left.id.cmp(&right.id))
    });
    let count = all.len();
    (
        count,
        all.into_iter()
            .skip(options.offset)
            .take(options.limit)
            .collect(),
    )
}

fn check(
    vector: &BTreeMap<String, f64>,
    text: &BTreeMap<String, f64>,
    mode: SearchMode,
    options: &SearchQueryOptions,
    memory: &QueryMemory,
) -> Result<RankedPage> {
    let task = RuntimeTaskContext::default();
    let vr = RankedScores::new(vector, &memory.working, &task)?;
    let tr = RankedScores::new(text, &memory.working, &task)?;
    for (source, ranked) in [(vector, &vr), (text, &tr)] {
        let previous = crate::window_ranks(&crate::ranked_scores(source), options.rank_window);
        assert_eq!(ranked.window_len(options.rank_window), previous.len());
        assert_eq!(
            ranked.top_ids(options.rank_window, options.limit),
            crate::top_ranked_ids(&previous, options.limit)
        );
        assert_eq!(
            ranked.top_candidates(options.rank_window, options.limit),
            crate::top_ranked_candidates(&previous, source, options.limit)
        );
    }
    let page = RankedPage::build(&vr, &tr, mode, options, memory, &task)?;
    let expected = oracle(vector, text, mode, options);
    assert_eq!(page.matching_count, expected.0);
    assert_eq!(*page.candidates, expected.1);
    Ok(page)
}

#[test]
fn rank_arrays_borrow_ids_and_admit_exact_capacity_before_allocation() {
    let scores = scores(&[("z", 1.0), ("a", 2.0), ("b", 2.0)]);
    let bytes = 3 * (size_of::<RankedScore<'_>>() + size_of::<usize>());
    for bound in [bytes - 1, bytes] {
        let memory = memory(bound + 137);
        let other = memory.scores.reserve(137).unwrap();
        take();
        let rank = RankedScores::new(&scores, &memory.working, &RuntimeTaskContext::default());
        assert_eq!(rank.is_ok(), bound == bytes);
        assert_eq!(take().ranks, usize::from(bound == bytes));
        if let Ok(rank) = rank {
            assert_eq!(rank.top_ids(None, 3), ["a", "b", "z"]);
            for (entry, (id, _)) in rank.0.by_id.iter().zip(&scores) {
                assert!(std::ptr::eq(entry.id.as_ptr(), id.as_ptr()));
            }
            assert_eq!(rank.0.by_id.capacity(), scores.len());
            assert_eq!(rank.0.order.capacity(), scores.len());
            assert_eq!(memory.ledger.snapshot().used_bytes, bytes + 137);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(memory.ledger.snapshot().account_count, 2);
    }
    assert!(buffer_bytes::<usize>(usize::MAX).is_err());
    assert!(buffer_bytes::<u8>(isize::MAX as usize + 1).is_err());
}

#[test]
fn heap_and_owned_page_overlap_share_exact_and_one_short_root_capacity() {
    let vector = scores(&[("a", 4.0), ("c", 2.0)]);
    let text = scores(&[("a", 1.0), ("b", 3.0)]);
    let rank_bytes = 4 * (size_of::<RankedScore<'_>>() + size_of::<usize>());
    let heap_bytes = 2 * size_of::<Reverse<Candidate<'_>>>();
    let page_bytes = 2 * (size_of::<SearchScoredCandidate>() + 1);
    let peak = 137 + rank_bytes + heap_bytes + page_bytes;
    for bound in [peak - 1, peak] {
        let memory = memory(bound);
        let other = memory.scores.reserve(137).unwrap();
        take();
        let page = check(
            &vector,
            &text,
            SearchMode::Hybrid,
            &options(0, 2, None),
            &memory,
        );
        assert_eq!(page.is_ok(), bound == peak);
        assert_eq!(
            take(),
            Evidence {
                ranks: 2,
                heaps: 1,
                pages: usize::from(bound == peak)
            }
        );
        if let Ok(page) = page {
            assert_eq!(memory.ledger.snapshot().peak_bytes, peak);
            assert_eq!(memory.ledger.snapshot().used_bytes, 137 + page_bytes);
            drop(page);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 137);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn page_retains_result_ownership_after_rank_and_query_drop() {
    let vector = scores(&[("a", 3.0), ("long-id", 2.0)]);
    let text = BTreeMap::new();
    let memory = memory(1024 * 1024);
    let ledger = memory.ledger.clone();
    let page = check(
        &vector,
        &text,
        SearchMode::Vector,
        &options(1, 1, None),
        &memory,
    )
    .unwrap();
    drop(memory);
    drop(vector);
    drop(text);
    assert_eq!(
        ledger.snapshot().used_bytes,
        size_of::<SearchScoredCandidate>() + "long-id".len()
    );
    assert_eq!(page.candidates[0].id, "long-id");
    drop(page);
    assert_eq!(ledger.snapshot().used_bytes, 0);
}

#[test]
fn windows_weights_modes_and_pages_preserve_independent_fusion_semantics() {
    let vector = scores(&[("a", 0.7), ("b", 0.7), ("c", 0.4)]);
    let text = scores(&[("b", 8.0), ("c", 2.0), ("d", 2.0)]);
    let memory = memory(1024 * 1024);
    for mode in [SearchMode::Text, SearchMode::Vector, SearchMode::Hybrid] {
        for window in [None, Some(0), Some(1), Some(2), Some(99)] {
            for (offset, limit) in [(0, 0), (0, 1), (1, 2), (99, 3), (0, usize::MAX)] {
                for (vector_weight, text_weight) in [(1.0, 1.0), (0.0, 1.0), (2.0, 0.5), (0.0, 0.0)]
                {
                    let mut options = options(offset, limit, window);
                    options.fusion_weights = SearchFusionWeights {
                        vector_weight,
                        text_weight,
                    };
                    drop(check(&vector, &text, mode, &options, &memory).unwrap());
                    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
                }
            }
        }
    }
}

#[test]
fn empty_zero_limit_and_overflow_paths_do_not_allocate_a_union() {
    let source = scores(&[("a", 1.0), ("b", 1.0)]);
    let memory = memory(1024 * 1024);
    let page = check(
        &source,
        &source,
        SearchMode::Hybrid,
        &options(1, 0, None),
        &memory,
    )
    .unwrap();
    assert_eq!(page.matching_count, 2);
    assert!(page.candidates.is_empty());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(
        memory.ledger.snapshot().peak_bytes,
        4 * (size_of::<RankedScore<'_>>() + size_of::<usize>())
    );
    let task = RuntimeTaskContext::default();
    let empty = BTreeMap::new();
    let rank = RankedScores::new(&empty, &memory.working, &task).unwrap();
    take();
    assert!(RankedPage::build(
        &rank,
        &rank,
        SearchMode::Hybrid,
        &options(usize::MAX, 1, None),
        &memory,
        &task
    )
    .is_err());
    assert_eq!(take(), Evidence::default());
    assert!(check(
        &empty,
        &empty,
        SearchMode::Hybrid,
        &options(0, 2, None),
        &memory
    )
    .unwrap()
    .candidates
    .is_empty());
}

#[test]
fn cancellation_before_entry_and_during_page_copy_releases_all_charges() {
    let source = scores(&[("a", 2.0), ("b", 1.0)]);
    let memory = memory(1024 * 1024);
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let ranks = RankedScores::new(&source, &memory.working, &task).unwrap();
    let retained = memory.ledger.snapshot().used_bytes;
    CANCEL_PAGE.with_borrow_mut(|slot| *slot = Some(token.clone()));
    take();
    assert!(RankedPage::build(
        &ranks,
        &ranks,
        SearchMode::Hybrid,
        &options(0, 2, None),
        &memory,
        &task
    )
    .is_err());
    assert_eq!(
        take(),
        Evidence {
            ranks: 0,
            heaps: 1,
            pages: 1
        }
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    assert!(RankedScores::new(&source, &memory.working, &task).is_err());
    assert!(RankedPage::build(
        &ranks,
        &ranks,
        SearchMode::Hybrid,
        &options(0, 2, None),
        &memory,
        &task
    )
    .is_err());
    assert_eq!(take(), Evidence::default());
    drop(ranks);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn small_pages_bound_heap_capacity_independently_of_union_size() {
    let source = (0..4096)
        .map(|id| (format!("{id:05}"), (id % 16 + 1) as f64))
        .collect();
    let empty = BTreeMap::new();
    let rank_bytes = 4096 * (size_of::<RankedScore<'_>>() + size_of::<usize>());
    let heap_bytes = 3 * size_of::<Reverse<Candidate<'_>>>();
    let page_bytes = 2 * (size_of::<SearchScoredCandidate>() + 5);
    let memory = memory(rank_bytes + heap_bytes + page_bytes);
    let task = RuntimeTaskContext::default();
    let ranks = RankedScores::new(&source, &memory.working, &task).unwrap();
    let zero = RankedScores::new(&empty, &memory.working, &task).unwrap();
    let page = RankedPage::build(
        &ranks,
        &zero,
        SearchMode::Vector,
        &options(1, 2, None),
        &memory,
        &task,
    )
    .unwrap();
    assert_eq!(page.matching_count, 4096);
    assert_eq!(HEAP_CAPACITY.get(), 3);
    assert_eq!(page.candidates.capacity(), 2);
    assert_eq!(
        page.candidates
            .iter()
            .map(|value| value.id.as_str())
            .collect::<Vec<_>>(),
        ["00031", "00047"]
    );
    assert_eq!(
        memory.ledger.snapshot().peak_bytes,
        rank_bytes + heap_bytes + page_bytes
    );
    drop(page);
    drop(ranks);
    drop(zero);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn result_component_limit_rejects_page_even_with_spare_working_capacity() {
    let source = scores(&[("a", 1.0)]);
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(skein_core::RuntimeMemoryReservation::new(1024 * 1024, 1));
    let memory = QueryMemory::new(NonZeroU64::new(1024 * 1024).unwrap(), Some(&task)).unwrap();
    take();
    assert!(check(
        &source,
        &source,
        SearchMode::Hybrid,
        &options(0, 1, None),
        &memory
    )
    .is_err());
    assert_eq!(
        take(),
        Evidence {
            ranks: 2,
            heaps: 1,
            pages: 0
        }
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
