use super::query_io::{add, checkpoint, mul};
use crate::query_memory::Admitted;
use crate::{
    search_storage_scan_predicate, FieldSummary, Result, RuntimeTaskContext, ScanPredicate,
    SearchFieldPruningAccumulator, SearchPredicate, SearchPredicateOp, SearchPredicateSet,
    SearchScalarValue, SearchSegmentDescriptorEntry, SearchSegmentFieldSummary, SegmentPruner,
    SegmentSummary, Value,
};
use skein_executor::QueryMemoryAccount;
use skein_storage::PruningDecision;
use std::mem::size_of;

/// Borrow predicates, rather than cloning fields and grouping them per segment.
/// The reference array owns its charge; the caller still owns the parsed input.
pub(super) struct SegmentPruning<'a> {
    ordered: Admitted<Vec<&'a SearchPredicate>>,
    unsatisfiable: bool,
}

impl<'a> SegmentPruning<'a> {
    pub fn new(
        predicates: &'a SearchPredicateSet,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<Self> {
        checkpoint(task)?;
        let lease = memory.reserve(mul(
            predicates.predicates().len(),
            size_of::<&SearchPredicate>(),
        )?)?;
        #[cfg(test)]
        tests::record_order();
        let mut ordered = Vec::with_capacity(predicates.predicates().len());
        ordered.extend(predicates.predicates());
        ordered.sort_unstable_by(|a, b| a.field().name().cmp(b.field().name()));
        checkpoint(task)?;
        Ok(Self {
            ordered: Admitted::new(ordered, lease),
            unsatisfiable: predicates.is_unsatisfiable(),
        })
    }

    pub fn evaluate(
        &self,
        segment: &SearchSegmentDescriptorEntry,
        reports: &mut SearchFieldPruningAccumulator,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<bool> {
        checkpoint(task)?;
        if self.unsatisfiable {
            return Ok(false);
        }
        let mut may_match = true;
        for group in self
            .ordered
            .chunk_by(|a, b| a.field().name() == b.field().name())
        {
            checkpoint(task)?;
            let field = group[0].field().name();
            let persisted = segment.metadata.get(field);
            let comparison = memory.reserve(comparison_bytes(persisted, group, task)?)?;
            #[cfg(test)]
            tests::record_comparison();
            let mut raw_match = true;
            for predicate in group {
                checkpoint(task)?;
                if !segment.may_match_predicate(predicate) {
                    raw_match = false;
                    break;
                }
            }
            reports.observe_field(field, raw_match);
            drop(comparison);
            // Reports observe every field even after another field rejects the
            // segment. Storage refinement cannot change their legacy counters.
            may_match &= raw_match;
            if !may_match
                || group
                    .iter()
                    .all(|predicate| matches!(predicate.op(), SearchPredicateOp::NotIn(_)))
            {
                continue;
            }
            let with_values = group.iter().any(|predicate| {
                matches!(
                    predicate.op(),
                    SearchPredicateOp::Eq(_) | SearchPredicateOp::In(_)
                )
            });
            let _workspace =
                memory.reserve(storage_bytes(field, persisted, group, with_values, task)?)?;
            #[cfg(test)]
            tests::record_summary();
            let mut summary =
                SegmentSummary::new(segment.segment_id, segment.document_count as u64);
            if let Some(persisted) = persisted {
                summary.insert_field(
                    field.to_owned(),
                    persisted.storage_summary(field, segment.document_count, with_values),
                );
            }
            #[cfg(test)]
            tests::record_fields(&summary);
            for predicate in group {
                checkpoint(task)?;
                if let Some(predicate) = search_storage_scan_predicate(predicate)
                    && !SegmentPruner::new(&summary)
                        .evaluate(&predicate)
                        .should_open_payload()
                {
                    may_match = false;
                    break;
                }
            }
        }
        checkpoint(task)?;
        Ok(may_match)
    }
}

fn scalar_bytes(
    op: &SearchPredicateOp,
    mut measure: impl FnMut(&SearchScalarValue) -> Result<usize>,
) -> Result<usize> {
    match op {
        SearchPredicateOp::Eq(value)
        | SearchPredicateOp::Gt(value)
        | SearchPredicateOp::Gte(value)
        | SearchPredicateOp::Lt(value)
        | SearchPredicateOp::Lte(value) => measure(value),
        SearchPredicateOp::In(values) | SearchPredicateOp::NotIn(values) => values
            .iter()
            .try_fold(0, |sum, value| add(sum, measure(value)?)),
        SearchPredicateOp::Exists | SearchPredicateOp::IsMissing => Ok(0),
    }
}

fn comparison_bytes(
    persisted: Option<&SearchSegmentFieldSummary>,
    group: &[&SearchPredicate],
    task: &RuntimeTaskContext,
) -> Result<usize> {
    let mut expected = 0;
    let mut compares_values = false;
    for predicate in group {
        checkpoint(task)?;
        if !matches!(
            predicate.op(),
            SearchPredicateOp::Eq(_) | SearchPredicateOp::In(_) | SearchPredicateOp::NotIn(_)
        ) {
            continue;
        }
        compares_values = true;
        scalar_bytes(predicate.op(), |value| {
            checkpoint(task)?;
            expected = expected.max(value.as_str().len());
            Ok(0)
        })?;
    }
    let mut actual = 0;
    if compares_values && let Some(persisted) = persisted {
        for value in &persisted.values {
            checkpoint(task)?;
            actual = actual.max(value.len());
        }
    }
    // Simultaneous Unicode normalization and kind aliases. The same 12x byte
    // envelope is used by candidate filtering; fixed slack covers short aliases.
    add(mul(add(actual, expected)?, 12)?, 1024)
}

fn storage_bytes(
    field: &str,
    persisted: Option<&SearchSegmentFieldSummary>,
    group: &[&SearchPredicate],
    with_values: bool,
    task: &RuntimeTaskContext,
) -> Result<usize> {
    // A one-field B-tree can allocate a full leaf node. Cover 16 entry slots,
    // links and header slack, rather than charging only its one live value.
    let mut summary = add(
        mul(size_of::<(String, FieldSummary)>(), 16)?,
        add(field.len(), 512)?,
    )?;
    if with_values && let Some(persisted) = persisted {
        for value in &persisted.values {
            checkpoint(task)?;
            // Normalized Value -> ScanScalar clones and set construction/split
            // slack; there are no exact-row bitmaps or membership filters here.
            summary = add(
                summary,
                add(crate::build_memory::SET_ENTRY_BYTES, mul(value.len(), 12)?)?,
            )?;
        }
    }
    let mut predicate_peak = 0;
    for predicate in group {
        checkpoint(task)?;
        let scalars = scalar_bytes(predicate.op(), |value| {
            checkpoint(task)?;
            add(
                mul(value.as_str().len(), 12)?,
                2 * (size_of::<Value>() + size_of::<PruningDecision>()),
            )
        })?;
        // IN evaluation materializes a decision vector alongside its values.
        predicate_peak = predicate_peak.max(add(predicate.field().name().len(), scalars)?);
    }
    add(
        summary,
        add(predicate_peak, size_of::<ScanPredicate>() + 1024)?,
    )
}

#[cfg(test)]
pub(super) mod tests;
