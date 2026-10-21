// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

//! Query stages share one working account. Native result ownership is coordinated
//! separately; ordinary APIs and complete registered serving remain distinct.

use super::*;
use crate::build_memory::{checked_add, MAP_ENTRY_BYTES, SET_ENTRY_BYTES};
use crate::lexical_projection::{AccountedCorpusStatistics, QueryContext, ScoringInputs};
use hawdb_executor::QueryMemoryLease;
use hawdb_optimizer_predicate::{SearchPredicateConstructionError, SearchPredicateSet};

#[cfg(test)]
mod tests;

#[derive(Clone, Copy)]
pub(super) struct Execution<'a> {
    pub(super) base: SearchOutOfCoreExecutionContext<'a>,
    pub(super) query: Option<QueryContext<'a>>,
    pub(super) immutable_freshness: bool,
    pub(super) scratch: Option<&'a ScratchDirectory>,
    pub(super) output: Option<&'a super::query_output::Frame<'a>>,
}

impl<'a> From<SearchOutOfCoreExecutionContext<'a>> for Execution<'a> {
    fn from(base: SearchOutOfCoreExecutionContext<'a>) -> Self {
        Self {
            base,
            query: None,
            immutable_freshness: false,
            scratch: None,
            output: None,
        }
    }
}

pub(super) struct Predicates {
    pub(super) predicates: SearchPredicateSet,
    pub(super) report: SearchPredicatePushdownReport,
    // Both AST and diagnostic payload close before their construction envelope.
    _memory: Option<QueryMemoryLease>,
}

impl Predicates {
    pub(super) fn new(
        filters: &BTreeMap<String, String>,
        query: Option<QueryContext<'_>>,
    ) -> Result<Self> {
        let mut memory = query.map(|query| query.memory.reserve(0)).transpose()?;
        let parsed = SearchPredicateSet::from_metadata_filters_with_admission(filters, |bytes| {
            if let Some(query) = query {
                query.checkpoint()?;
            }
            memory.as_mut().map_or(Ok(()), |memory| memory.grow(bytes))
        });
        let (predicates, parse_error) = match parsed {
            Ok(predicates) => (predicates, None),
            Err(SearchPredicateConstructionError::Parse(error)) => {
                (SearchPredicateSet::unsatisfiable(), Some(error.to_string()))
            }
            Err(SearchPredicateConstructionError::Admission(error)) => return Err(error),
            Err(SearchPredicateConstructionError::SizeOverflow) => {
                return Err(HawDBError::Execution(
                    "metadata predicate construction size overflow".into(),
                ));
            }
        };
        let pushdown = crate::metadata_predicate_pushdown(filters.len(), predicates, parse_error);
        let output = Self {
            predicates: pushdown.predicates,
            report: pushdown.report,
            _memory: memory,
        };
        if let Some(query) = query {
            query.checkpoint()?;
        }
        Ok(output)
    }
}

pub(super) struct Terms {
    pub(super) values: BTreeSet<String>,
    _memory: Option<QueryMemoryLease>,
}

pub(super) struct TextScores {
    pub(super) values: BTreeMap<String, f64>,
    pub(super) matching_count: usize,
    pub(super) postings_visited: u64,
    pub(super) bytes_read: u64,
    pub(super) blocks_skipped: u64,
    _memory: Option<QueryMemoryLease>,
}

impl SearchOutOfCoreReader {
    pub(super) fn query_terms(
        &self,
        text: &str,
        context: Option<QueryContext<'_>>,
    ) -> Result<Terms> {
        let projection = &self.primary_segment().lexical_projection;
        let Some(context) = context else {
            return Ok(Terms {
                values: projection.tokenize_query(
                    text,
                    &self.analyzer_lexicon,
                    self.lexical_term_policy.max_term_bytes(),
                )?,
                _memory: None,
            });
        };
        let normalized = projection.tokenize_accounted(
            text,
            &self.analyzer_lexicon,
            self.lexical_term_policy.max_term_bytes(),
            context,
        )?;
        // Hydration currently consumes String terms. Admit its separate set
        // before copying; both normalization and the destination remain funded.
        let bytes = normalized.terms.iter().try_fold(0, |bytes, term| {
            checked_add(bytes, checked_add(SET_ENTRY_BYTES, term.len())?)
        })?;
        let memory = context.memory.reserve(bytes)?;
        let mut terms = Terms {
            values: BTreeSet::new(),
            _memory: Some(memory),
        };
        for term in &normalized.terms {
            context.checkpoint()?;
            let value = term.as_str().to_owned();
            if value.capacity() > term.len() {
                return Err(HawDBError::Execution(
                    "query term copy exceeded admission".into(),
                ));
            }
            terms.values.insert(value);
        }
        context.checkpoint()?;
        Ok(terms)
    }

    pub(super) fn query_text_scores(
        &self,
        terms: &BTreeSet<String>,
        retained_limit: Option<usize>,
        prune: bool,
        candidates: &CandidateSet<'_>,
        metrics: &mut SearchOutOfCoreMetrics,
        context: Option<QueryContext<'_>>,
    ) -> Result<TextScores> {
        let mut accounted = context
            .map(|context| {
                AccountedCorpusStatistics::aggregate(
                    self.segments
                        .iter()
                        .map(|segment| &segment.lexical_projection),
                    terms,
                    self.lexical_term_policy.max_term_bytes(),
                    context,
                )
            })
            .transpose()?;
        let mut ordinary = if context.is_none() {
            Some(LexicalCorpusStatistics::aggregate(
                self.segments
                    .iter()
                    .map(|segment| &segment.lexical_projection),
                terms,
                self.lexical_term_policy.max_term_bytes(),
            )?)
        } else {
            None
        };
        let statistics = match &mut accounted {
            Some(accounted) => &mut accounted.statistics,
            None => ordinary.as_mut().expect("ordinary query statistics"),
        };
        if !self.visibility.is_empty() {
            let retractions = self.visibility.retractions().map(|entry| {
                (
                    entry.retraction.lexical_document_len,
                    &entry.retraction.unique_terms,
                )
            });
            if context.is_some() {
                statistics.retract_streamed_with_context(retractions, context)?;
            } else {
                statistics.retract_streamed(retractions)?;
            }
        }
        let mut output = TextScores {
            values: BTreeMap::new(),
            matching_count: 0,
            postings_visited: 0,
            bytes_read: statistics.bytes_read(),
            blocks_skipped: 0,
            _memory: context
                .map(|context| context.memory.reserve(0))
                .transpose()?,
        };
        for segment in &self.segments {
            if let Some(context) = context {
                context.checkpoint()?;
            }
            let allowed = |id: &str| {
                Ok(self.visibility.is_visible(segment.content_segment_id, id)
                    && candidates.contains(id, metrics)?)
            };
            let report = if context.is_some() {
                segment.lexical_projection.score_terms_accounted(
                    terms,
                    ScoringInputs {
                        delta: &crate::lexical_projection::LexicalMiniDelta::default(),
                        max_term_bytes: self.lexical_term_policy.max_term_bytes(),
                        retained_score_limit: retained_limit,
                        global_statistics: Some(statistics),
                        prune_blocks: prune,
                        context,
                    },
                    allowed,
                )?
            } else {
                crate::lexical_projection::AccountedQueryReport {
                    report: segment
                        .lexical_projection
                        .score_with_global_statistics_and_pruning(
                            terms,
                            self.lexical_term_policy.max_term_bytes(),
                            retained_limit,
                            statistics,
                            prune,
                            allowed,
                        )?,
                    _memory: None,
                }
            };
            output.matching_count = output
                .matching_count
                .checked_add(report.report.matching_document_count)
                .ok_or_else(|| HawDBError::Storage("search text match count overflow".into()))?;
            output.postings_visited = output
                .postings_visited
                .saturating_add(report.report.postings_visited);
            output.bytes_read = output.bytes_read.saturating_add(report.report.bytes_read);
            output.blocks_skipped = output
                .blocks_skipped
                .saturating_add(report.report.blocks_skipped);
            for (id, score) in report.report.scores {
                if let Some(context) = context {
                    context.checkpoint()?;
                }
                if output.values.contains_key(&id) {
                    return Err(HawDBError::Storage(format!(
                        "search out-of-core segment set has duplicate document {id}"
                    )));
                }
                if let Some(memory) = &mut output._memory {
                    memory.grow(checked_add(MAP_ENTRY_BYTES, id.capacity())?)?;
                }
                output.values.insert(id, score);
            }
        }
        if let Some(context) = context {
            context.checkpoint()?;
        }
        Ok(output)
    }
}
