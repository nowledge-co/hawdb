use super::*;
use crate::query_memory::QueryMemory;
use crate::{RuntimeCancellationToken, SearchDocument};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU64;

mod fuzz;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::out_of_core) struct Evidence {
    pub orders: usize,
    pub comparisons: usize,
    pub summaries: usize,
    pub fields: usize,
    pub values: usize,
}
thread_local! { static EVIDENCE: Cell<Evidence> = Cell::default(); }
pub(super) fn record_order() {
    let mut v = EVIDENCE.get();
    v.orders += 1;
    EVIDENCE.set(v);
}
pub(super) fn record_comparison() {
    let mut v = EVIDENCE.get();
    v.comparisons += 1;
    EVIDENCE.set(v);
}
pub(super) fn record_summary() {
    let mut v = EVIDENCE.get();
    v.summaries += 1;
    EVIDENCE.set(v);
}
pub(super) fn record_fields(summary: &SegmentSummary) {
    let mut v = EVIDENCE.get();
    v.fields += summary.fields.len();
    v.values += summary
        .fields
        .values()
        .filter_map(|field| field.enum_dictionary.as_ref())
        .map(|dictionary| dictionary.values.len())
        .sum::<usize>();
    EVIDENCE.set(v);
}
pub(in crate::out_of_core) fn take() -> Evidence {
    EVIDENCE.replace(Evidence::default())
}

fn memory(bytes: usize) -> QueryMemory {
    QueryMemory::new(NonZeroU64::new(bytes as u64).unwrap(), None).unwrap()
}

fn segment(documents: &[SearchDocument]) -> SearchSegmentDescriptorEntry {
    let fields = documents
        .iter()
        .flat_map(|document| document.metadata.keys().cloned())
        .collect::<BTreeSet<_>>();
    SearchSegmentDescriptorEntry::from_documents(0, &documents.iter().collect::<Vec<_>>(), &fields)
}

fn document(id: usize, space: &str) -> SearchDocument {
    SearchDocument {
        id: format!("memory:{id:03}"),
        title: String::new(),
        content: String::new(),
        embedding: None,
        metadata: BTreeMap::from([
            ("space_id".to_owned(), space.to_owned()),
            ("score".to_owned(), (id + 1).to_string()),
            (
                "kind".to_owned(),
                if id.is_multiple_of(2) {
                    "memory"
                } else {
                    "source-chunk"
                }
                .to_owned(),
            ),
            (
                "created_at".to_owned(),
                format!("2026-09-{:02}", id % 28 + 1),
            ),
        ]),
    }
}

fn compare(
    segment: &SearchSegmentDescriptorEntry,
    predicates: &SearchPredicateSet,
    memory: &QueryMemory,
) -> Result<bool> {
    let task = RuntimeTaskContext::default();
    let mut previous = SearchFieldPruningAccumulator::new(predicates);
    previous.observe_persisted_segment(segment, predicates);
    let expected = segment.may_match_predicates(predicates);
    let pruner = SegmentPruning::new(predicates, &memory.working, &task)?;
    let mut current = SearchFieldPruningAccumulator::new(predicates);
    let actual = pruner.evaluate(segment, &mut current, &memory.working, &task)?;
    assert_eq!(actual, expected);
    let current = current.into_reports();
    for field in &current {
        assert_eq!(field.segment_count, 1);
        assert_eq!(field.pruned_segment_count + field.scanned_segment_count, 1);
    }
    assert_eq!(current, previous.into_reports());
    Ok(actual)
}

#[test]
fn order_is_pre_admitted_and_retains_its_charge_after_query_drop() {
    let predicates = SearchPredicateSet::new(vec![
        SearchPredicate::eq("z", "1"),
        SearchPredicate::eq("a", "2"),
    ]);
    let task = RuntimeTaskContext::default();
    let bytes = 2 * size_of::<&SearchPredicate>();
    take();
    assert!(SegmentPruning::new(&predicates, &memory(bytes - 1).working, &task).is_err());
    assert_eq!(take(), Evidence::default());
    let memory = memory(bytes);
    let ledger = memory.ledger.clone();
    let prepared = SegmentPruning::new(&predicates, &memory.working, &task).unwrap();
    drop(memory);
    assert_eq!(ledger.snapshot().used_bytes, bytes);
    assert_eq!(prepared.ordered[0].field().name(), "a");
    drop(prepared);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    assert_eq!(ledger.snapshot().account_count, 2);
}

#[test]
fn exact_and_one_short_workspace_share_competing_query_capacity() {
    let segment = segment(&[document(0, "team"), document(1, "team")]);
    let predicates = SearchPredicateSet::new(vec![SearchPredicate::eq("space_id", "team")]);
    let baseline = memory(1024 * 1024);
    let other = baseline.scores.reserve(137).unwrap();
    assert!(compare(&segment, &predicates, &baseline).unwrap());
    let peak = baseline.ledger.snapshot().peak_bytes;
    // Independently spell out the one-field/one-value overlap contract. Reading
    // the expected peak from the ledger alone would miss an omitted charge.
    let summary = 16 * size_of::<(String, FieldSummary)>()
        + "space_id".len()
        + 512
        + 1024
        + 12 * "team".len();
    let predicate = "space_id".len()
        + 12 * "team".len()
        + 2 * (size_of::<Value>() + size_of::<PruningDecision>())
        + size_of::<ScanPredicate>()
        + 1024;
    assert_eq!(
        peak,
        137 + size_of::<&SearchPredicate>() + summary + predicate
    );
    assert_eq!(baseline.ledger.snapshot().used_bytes, 137);
    drop(other);
    for bound in [peak, peak - 1] {
        let memory = memory(bound);
        let other = memory.scores.reserve(137).unwrap();
        let task = RuntimeTaskContext::default();
        let prepared = SegmentPruning::new(&predicates, &memory.working, &task).unwrap();
        let mut reports = SearchFieldPruningAccumulator::new(&predicates);
        take();
        let result = prepared.evaluate(&segment, &mut reports, &memory.working, &task);
        assert_eq!(result.is_ok(), bound == peak);
        assert_eq!(take().summaries, usize::from(bound == peak));
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            137 + size_of::<&SearchPredicate>()
        );
        drop(prepared);
        drop(other);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn summaries_exclude_unreferenced_fields_and_unused_dictionaries() {
    let mut doc = document(0, "team");
    doc.metadata
        .insert("unrelated".to_owned(), "x".repeat(128 * 1024));
    let segment = segment(&[doc]);
    let predicates = SearchPredicateSet::new(vec![SearchPredicate::eq("space_id", "team")]);
    let memory = memory(32 * 1024);
    take();
    assert!(compare(&segment, &predicates, &memory).unwrap());
    assert_eq!(
        take(),
        Evidence {
            orders: 1,
            comparisons: 1,
            summaries: 1,
            fields: 1,
            values: 1
        }
    );
    assert!(memory.ledger.snapshot().peak_bytes < 32 * 1024);
    let predicates = SearchPredicateSet::new(vec![SearchPredicate::exists("unrelated")]);
    take();
    assert!(compare(&segment, &predicates, &memory).unwrap());
    assert_eq!(take().values, 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn normalization_is_admitted_before_matching_and_released_before_return() {
    let mut doc = document(0, "team");
    doc.metadata
        .insert("unicode".to_owned(), "\u{130}".repeat(128));
    let segment = segment(&[doc]);
    let predicates = SearchPredicateSet::new(vec![SearchPredicate::not_in_list(
        "unicode",
        ["excluded".to_owned()],
    )]);
    let actual_bytes = segment.metadata["unicode"].values.first().unwrap().len();
    let required = size_of::<&SearchPredicate>() + 1024 + 12 * (actual_bytes + "excluded".len());
    for budget in [required - 1, required] {
        let memory = memory(budget);
        let task = RuntimeTaskContext::default();
        let prepared = SegmentPruning::new(&predicates, &memory.working, &task).unwrap();
        take();
        let result = prepared.evaluate(
            &segment,
            &mut SearchFieldPruningAccumulator::new(&predicates),
            &memory.working,
            &task,
        );
        assert_eq!(result.is_ok(), budget == required);
        let evidence = take();
        assert_eq!(evidence.comparisons, usize::from(budget == required));
        assert_eq!(evidence.summaries, 0);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            size_of::<&SearchPredicate>()
        );
        drop(prepared);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn rejection_still_observes_later_fields_and_duplicate_field_operations() {
    let segment = segment(&[document(0, "team"), document(1, "private")]);
    let predicates = SearchPredicateSet::new(vec![
        SearchPredicate::gte("score", "1"),
        SearchPredicate::eq("absent", "x"),
        SearchPredicate::lte("score", "9"),
        SearchPredicate::exists("space_id"),
    ]);
    take();
    let memory = memory(1024 * 1024);
    assert!(!compare(&segment, &predicates, &memory).unwrap());
    let evidence = take();
    assert_eq!(evidence.comparisons, 3);
    assert_eq!(evidence.summaries, 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn empty_unsatisfiable_cancelled_and_not_in_paths_release_workspace() {
    let segment = segment(&[document(0, "team")]);
    for (predicates, expected) in [
        (SearchPredicateSet::empty(), true),
        (SearchPredicateSet::unsatisfiable(), false),
        (
            SearchPredicateSet::new(vec![SearchPredicate::not_in_list(
                "space_id",
                ["private".to_owned()],
            )]),
            true,
        ),
    ] {
        let memory = memory(16 * 1024);
        take();
        assert_eq!(compare(&segment, &predicates, &memory).unwrap(), expected);
        assert_eq!(take().summaries, 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
    let predicates = SearchPredicateSet::new(vec![SearchPredicate::eq("space_id", "team")]);
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let memory = memory(16 * 1024);
    let prepared = SegmentPruning::new(&predicates, &memory.working, &task).unwrap();
    token.cancel();
    take();
    assert!(prepared
        .evaluate(
            &segment,
            &mut SearchFieldPruningAccumulator::new(&predicates),
            &memory.working,
            &task
        )
        .is_err());
    assert!(SegmentPruning::new(&predicates, &memory.working, &task).is_err());
    assert_eq!(take(), Evidence::default());
    drop(prepared);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn reused_plan_bounds_peak_across_segments_and_releases_on_cancellation() {
    let segment = segment(&[document(0, "team")]);
    let predicates = SearchPredicateSet::new(vec![
        SearchPredicate::eq("space_id", "team"),
        SearchPredicate::gte("score", "1"),
    ]);
    let token = RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    let memory = memory(1024 * 1024);
    let prepared = SegmentPruning::new(&predicates, &memory.working, &task).unwrap();
    let mut reports = SearchFieldPruningAccumulator::new(&predicates);
    let retained = 2 * size_of::<&SearchPredicate>();
    let mut peak = None;
    for _ in 0..128 {
        assert!(prepared
            .evaluate(&segment, &mut reports, &memory.working, &task)
            .unwrap());
        let snapshot = memory.ledger.snapshot();
        assert_eq!(snapshot.used_bytes, retained);
        assert_eq!(
            snapshot.peak_bytes,
            *peak.get_or_insert(snapshot.peak_bytes)
        );
        assert_eq!(snapshot.account_count, 2);
    }
    token.cancel();
    take();
    assert!(prepared
        .evaluate(&segment, &mut reports, &memory.working, &task)
        .is_err());
    assert_eq!(take(), Evidence::default());
    for field in reports.into_reports() {
        assert_eq!(field.segment_count, 128);
        assert_eq!(field.scanned_segment_count, 128);
    }
    drop(prepared);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
