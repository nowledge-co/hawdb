// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::build_memory::reserve_capacity;
use crate::{
    SearchEmptyReasonCode, SearchPredicateFieldPruningReport, SearchRetrieverCandidate,
    SearchRetrieverCandidateSetReport,
};
use hawdb_optimizer_predicate::{SearchPredicateOp, SearchPredicateSet};

impl Memory<'_> {
    pub(in crate::out_of_core) fn freshness(
        self,
        value: &SearchProjectionFreshness,
    ) -> Result<SearchProjectionFreshness> {
        if self.0.is_none() {
            return Ok(value.clone());
        }
        Ok(SearchProjectionFreshness {
            full_reindex_reasons: self.strings(&value.full_reindex_reasons)?,
            metadata_repair_reasons: self.strings(&value.metadata_repair_reasons)?,
            embedding_model: self.optional(value.embedding_model.as_deref())?,
            embedding_version: self.optional(value.embedding_version.as_deref())?,
            document_count: value.document_count,
            import_source_graph_commit_epoch: value.import_source_graph_commit_epoch,
            source_graph_commit_epoch: value.source_graph_commit_epoch,
            durable_source_graph_commit_epoch: value.durable_source_graph_commit_epoch,
            has_uncheckpointed_changes: value.has_uncheckpointed_changes,
            full_reindex_needed: value.full_reindex_needed,
            metadata_repair_needed: value.metadata_repair_needed,
            embedding_dimension: value.embedding_dimension,
        })
    }
    pub(in crate::out_of_core) fn immutable_freshness(
        self,
        reader: &SearchOutOfCoreReader,
    ) -> Result<SearchProjectionFreshness> {
        Ok(SearchProjectionFreshness {
            document_count: reader.manifest.document_count,
            import_source_graph_commit_epoch: reader.manifest.import_source_graph_commit_epoch,
            source_graph_commit_epoch: reader.manifest.source_graph_commit_epoch,
            durable_source_graph_commit_epoch: reader.manifest.source_graph_commit_epoch,
            has_uncheckpointed_changes: false,
            full_reindex_needed: false,
            full_reindex_reasons: Vec::new(),
            metadata_repair_needed: false,
            metadata_repair_reasons: Vec::new(),
            embedding_model: self.optional(reader.manifest.embedding_model.as_deref())?,
            embedding_version: self.optional(reader.manifest.embedding_version.as_deref())?,
            embedding_dimension: reader.manifest.embedding_dimension,
        })
    }
    pub(in crate::out_of_core) fn candidate(
        self,
        value: &SearchCandidateSetReport,
    ) -> Result<SearchCandidateSetReport> {
        if self.0.is_none() {
            return Ok(value.clone());
        }
        Ok(SearchCandidateSetReport {
            id_space: self.text(&value.id_space)?,
            representation: self.text(&value.representation)?,
            cardinality: value.cardinality,
            exact: value.exact,
            snapshot_source_graph_commit_epoch: value.snapshot_source_graph_commit_epoch,
            policy_epoch: value.policy_epoch,
            filtered_out_count: value.filtered_out_count,
            metadata_filters: self.map(&value.metadata_filters)?,
            metadata_predicate_pushdown: self.predicate(&value.metadata_predicate_pushdown)?,
        })
    }
    fn predicate(
        self,
        value: &SearchPredicatePushdownReport,
    ) -> Result<SearchPredicatePushdownReport> {
        let mut fields = self.vec(value.field_summaries.len())?;
        for field in &value.field_summaries {
            fields.push(SearchPredicateFieldPruningReport {
                field: self.text(&field.field)?,
                value_kind: self.text(&field.value_kind)?,
                operation_kinds: self.strings(&field.operation_kinds)?,
                segment_count: field.segment_count,
                pruned_segment_count: field.pruned_segment_count,
                scanned_segment_count: field.scanned_segment_count,
                numeric_range_summary_used: field.numeric_range_summary_used,
                timestamp_range_summary_used: field.timestamp_range_summary_used,
                value_summary_used: field.value_summary_used,
            });
        }
        Ok(SearchPredicatePushdownReport {
            parse_error: self.optional(value.parse_error.as_deref())?,
            field_summaries: fields,
            input_predicate_count: value.input_predicate_count,
            pushed_predicate_count: value.pushed_predicate_count,
            residual_predicate_count: value.residual_predicate_count,
            unsatisfiable: value.unsatisfiable,
            segment_count: value.segment_count,
            pruned_segment_count: value.pruned_segment_count,
            scanned_segment_count: value.scanned_segment_count,
            segment_pruning_candidate_document_count: value
                .segment_pruning_candidate_document_count,
            segment_pruned_document_count: value.segment_pruned_document_count,
            segment_scanned_document_count: value.segment_scanned_document_count,
            persisted_segment_descriptor_used: value.persisted_segment_descriptor_used,
            physical_range_read_count: value.physical_range_read_count,
            physical_bytes_read: value.physical_bytes_read,
        })
    }
    pub(in crate::out_of_core) fn retriever_set(
        self,
        cardinality: usize,
        epoch: Option<u64>,
        policy: Option<u64>,
    ) -> Result<SearchRetrieverCandidateSetReport> {
        Ok(SearchRetrieverCandidateSetReport {
            id_space: self.text("search_projection_document_id")?,
            representation: self.text("ranked_document_ids")?,
            cardinality,
            exact: true,
            snapshot_source_graph_commit_epoch: epoch,
            policy_epoch: policy,
        })
    }
    pub(in crate::out_of_core) fn top_ids(
        self,
        ranks: &BTreeMap<String, usize>,
        limit: usize,
    ) -> Result<Vec<String>> {
        let Some(frame) = self.0 else {
            return Ok(top_ranked_ids(ranks, limit));
        };
        let ranked = Ranked::new(ranks, frame.query)?;
        let mut output = self.vec(limit.min(ranks.len()))?;
        for (id, _) in ranked.values.iter().take(limit) {
            output.push(self.text(id)?);
        }
        Ok(output)
    }
    pub(in crate::out_of_core) fn top_candidates(
        self,
        ranks: &BTreeMap<String, usize>,
        scores: &BTreeMap<String, f64>,
        limit: usize,
    ) -> Result<Vec<SearchRetrieverCandidate>> {
        let Some(frame) = self.0 else {
            return Ok(top_ranked_candidates(ranks, scores, limit));
        };
        let ranked = Ranked::new(ranks, frame.query)?;
        let mut output = self.vec(limit.min(ranks.len()))?;
        for (id, rank, score) in ranked
            .values
            .iter()
            .filter_map(|(id, rank)| scores.get(*id).map(|score| (*id, *rank, *score)))
            .take(limit)
        {
            output.push(SearchRetrieverCandidate {
                id: self.text(id)?,
                rank,
                score,
            });
        }
        Ok(output)
    }
    pub(in crate::out_of_core) fn empty_codes(
        self,
        state: Empty,
    ) -> Result<Vec<SearchEmptyReasonCode>> {
        let code = if !state.empty {
            None
        } else if state.documents == 0 {
            Some(SearchEmptyReasonCode::ProjectionEmpty)
        } else if state.filtered == 0 {
            Some(SearchEmptyReasonCode::MetadataFilterEmpty)
        } else if state.total == 0 {
            Some(SearchEmptyReasonCode::RetrieverNoHits)
        } else {
            Some(SearchEmptyReasonCode::LimitExcludedAllHits)
        };
        self.codes(code.as_slice())
    }
    pub(in crate::out_of_core) fn empty_reasons(
        self,
        state: Empty,
        truncation: &[String],
        fallback: &[String],
    ) -> Result<Vec<String>> {
        if self.0.is_none() {
            return Ok(search_empty_reasons(
                state.empty,
                state.documents,
                state.filtered,
                state.total,
                state.page,
                truncation,
                fallback,
            ));
        }
        if !state.empty {
            return Ok(Vec::new());
        }
        let mut output = self.vec(if state.total == 0 {
            add(fallback.len(), 1)?
        } else {
            1
        })?;
        if state.documents == 0 {
            output.push(self.text("search projection has no documents")?);
        } else if state.filtered == 0 {
            output.push(self.text("metadata filters matched no search documents")?);
        } else if state.total == 0 {
            output.push(self.text("search retrievers returned no hits inside filtered scope")?);
            for reason in fallback {
                output.push(self.text(reason)?);
            }
        } else if state.page.offset > 0 {
            output.push(self.format(format_args!(
                "offset {} limit {} returned no hits from {} matching hits",
                state.page.offset, state.page.limit, state.total
            ))?);
        } else {
            drop(output);
            return self.strings(truncation);
        }
        Ok(output)
    }
}

#[derive(Clone, Copy)]
pub(in crate::out_of_core) struct Empty {
    pub(in crate::out_of_core) empty: bool,
    pub(in crate::out_of_core) documents: usize,
    pub(in crate::out_of_core) filtered: usize,
    pub(in crate::out_of_core) total: usize,
    pub(in crate::out_of_core) page: SearchPageWindow,
}
struct Ranked<'a> {
    values: Vec<(&'a str, usize)>,
    _memory: QueryMemoryLease,
}
impl<'a> Ranked<'a> {
    fn new(ranks: &'a BTreeMap<String, usize>, query: QueryContext<'_>) -> Result<Self> {
        let mut output = Self {
            values: Vec::new(),
            _memory: query.memory.reserve(0)?,
        };
        reserve_capacity(&mut output.values, ranks.len(), &mut output._memory)?;
        for (id, rank) in ranks {
            query.checkpoint()?;
            output.values.push((id.as_str(), *rank));
        }
        output
            .values
            .sort_unstable_by(|(a, ar), (b, br)| ar.cmp(br).then_with(|| a.cmp(b)));
        query.checkpoint()?;
        Ok(output)
    }
}

pub(in crate::out_of_core) enum Fields<'a> {
    Legacy(SearchFieldPruningAccumulator),
    Query(NativeFields<'a>),
}
pub(in crate::out_of_core) struct NativeFields<'a> {
    fields: BTreeMap<&'a str, Stats>,
    _memory: QueryMemoryLease,
}
struct Stats {
    value_kind: &'static str,
    operations: Vec<&'static str>,
    segments: usize,
    pruned: usize,
    scanned: usize,
    numeric: bool,
    timestamp: bool,
    value: bool,
}
impl<'a> Fields<'a> {
    pub(in crate::out_of_core) fn new(
        predicates: &'a SearchPredicateSet,
        query: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        let Some(query) = query else {
            return Ok(Self::Legacy(SearchFieldPruningAccumulator::new(predicates)));
        };
        let mut output = NativeFields {
            fields: BTreeMap::new(),
            _memory: query.memory.reserve(0)?,
        };
        for predicate in predicates.predicates() {
            query.checkpoint()?;
            let name = predicate.field().name();
            let kind = crate::search_predicate_value_kind(predicate);
            let op = crate::search_predicate_op_kind(predicate.op());
            if !output.fields.contains_key(name) {
                output._memory.grow(MAP_ENTRY_BYTES)?;
                output.fields.insert(
                    name,
                    Stats {
                        value_kind: kind,
                        operations: Vec::new(),
                        segments: 0,
                        pruned: 0,
                        scanned: 0,
                        numeric: false,
                        timestamp: false,
                        value: false,
                    },
                );
            }
            let stats = output.fields.get_mut(name).expect("field was inserted");
            if kind != stats.value_kind {
                stats.value_kind = "mixed";
            }
            if !stats.operations.contains(&op) {
                let capacity = add(stats.operations.len(), 1)?;
                reserve_capacity(&mut stats.operations, capacity, &mut output._memory)?;
                stats.operations.push(op);
            }
            match predicate.op() {
                SearchPredicateOp::Gt(_)
                | SearchPredicateOp::Gte(_)
                | SearchPredicateOp::Lt(_)
                | SearchPredicateOp::Lte(_) => {
                    stats.numeric = true;
                    stats.timestamp |= crate::range_predicate_expects_timestamp(predicate);
                }
                _ => stats.value = true,
            }
        }
        for stats in output.fields.values_mut() {
            stats.operations.sort_unstable();
        }
        Ok(Self::Query(output))
    }
    pub(in crate::out_of_core) fn observe(
        &mut self,
        segment: &SearchSegmentDescriptorEntry,
        predicates: &SearchPredicateSet,
    ) {
        match self {
            Self::Legacy(fields) => fields.observe_persisted_segment(segment, predicates),
            Self::Query(fields) => {
                for (field, stats) in &mut fields.fields {
                    stats.segments += 1;
                    if predicates
                        .predicates()
                        .iter()
                        .filter(|predicate| predicate.field().name() == *field)
                        .all(|predicate| segment.may_match_predicate(predicate))
                    {
                        stats.scanned += 1;
                    } else {
                        stats.pruned += 1;
                    }
                }
            }
        }
    }
    pub(in crate::out_of_core) fn into_reports(
        self,
        memory: Memory<'_>,
    ) -> Result<Vec<SearchPredicateFieldPruningReport>> {
        match self {
            Self::Legacy(fields) => Ok(fields.into_reports()),
            Self::Query(fields) => {
                let mut output = memory.vec(fields.fields.len())?;
                for (field, stats) in fields.fields {
                    let mut operations = memory.vec(stats.operations.len())?;
                    for op in stats.operations {
                        operations.push(memory.text(op)?);
                    }
                    output.push(SearchPredicateFieldPruningReport {
                        field: memory.text(field)?,
                        value_kind: memory.text(stats.value_kind)?,
                        operation_kinds: operations,
                        segment_count: stats.segments,
                        pruned_segment_count: stats.pruned,
                        scanned_segment_count: stats.scanned,
                        numeric_range_summary_used: stats.numeric,
                        timestamp_range_summary_used: stats.timestamp,
                        value_summary_used: stats.value,
                    });
                }
                Ok(output)
            }
        }
    }
}
