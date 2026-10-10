// Copyright 2026 Nowledge
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;
use crate::lexical_projection::{
    AccountedCorpusStatistics, QueryContext, ScoreCollector, ScoringInputs,
};
use hawdb_executor::{QueryMemoryAccount, QueryMemoryLease};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchTextSeedReport {
    pub generation: u64,
    pub source_graph_commit_epoch: Option<u64>,
    pub corpus_document_count: usize,
    pub normalized_term_count: usize,
    pub matching_document_count: usize,
    pub returned_count: usize,
    pub postings_visited: u64,
    pub bytes_read: u64,
    pub document_bytes_read: u64,
}

/// Raw BM25 seeds bound to one immutable artifact closure and caller account.
///
/// Scores remain charged while this owner is alive. Consumers borrow entries
/// and admit their own downstream rows before making copies.
#[derive(Debug)]
pub struct SearchTextSeedOutput {
    scores: BTreeMap<String, f64>,
    pub report: SearchTextSeedReport,
    _memory: Option<QueryMemoryLease>,
}

impl SearchTextSeedOutput {
    pub fn scores(&self) -> &BTreeMap<String, f64> {
        &self.scores
    }
}

impl SearchOutOfCoreReader {
    /// Produces bounded raw BM25 seeds without loading document bodies.
    ///
    /// `max_candidates` is an explicit producer window, independent of a
    /// downstream scoring K. The predicate supplies canonical scope selection;
    /// current content visibility is always enforced by this reader.
    pub fn text_seed_scores_with_context(
        &self,
        query_text: &str,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &crate::RuntimeTaskContext,
        mut allowed: impl FnMut(&str) -> Result<bool>,
    ) -> Result<SearchTextSeedOutput> {
        let context = QueryContext { memory, task };
        context.checkpoint()?;
        self.require_search_capabilities(SearchMode::Text)?;
        if max_candidates > self.config.max_score_entries.get() {
            return Err(HawDBError::Storage(format!(
                "text seed window exceeds the admitted {} score entries",
                self.config.max_score_entries
            )));
        }
        let mut report = SearchTextSeedReport {
            generation: self.manifest.generation,
            source_graph_commit_epoch: self.manifest.source_graph_commit_epoch,
            corpus_document_count: self.manifest.document_count,
            normalized_term_count: 0,
            matching_document_count: 0,
            returned_count: 0,
            postings_visited: 0,
            bytes_read: 0,
            document_bytes_read: 0,
        };
        if max_candidates == 0 {
            return Ok(SearchTextSeedOutput {
                scores: BTreeMap::new(),
                report,
                _memory: None,
            });
        }
        let max_term_bytes = self.lexical_term_policy.max_term_bytes();
        let terms = self
            .primary_segment()
            .lexical_projection
            .tokenize_accounted(query_text, &self.analyzer_lexicon, max_term_bytes, context)?;
        report.normalized_term_count = terms.terms.len();
        let mut statistics = AccountedCorpusStatistics::aggregate(
            self.segments
                .iter()
                .map(|segment| segment.lexical_projection.as_ref()),
            &terms.terms,
            max_term_bytes,
            context,
        )?;
        statistics.statistics.retract_streamed_with_context(
            self.visibility.retractions().map(|entry| {
                (
                    entry.retraction.lexical_document_len,
                    &entry.retraction.unique_terms,
                )
            }),
            Some(context),
        )?;
        if statistics.statistics.document_count() != self.manifest.document_count {
            return Err(HawDBError::Storage(
                "text seed corpus statistics do not match the selected manifest".into(),
            ));
        }
        report.bytes_read = statistics.statistics.bytes_read();
        let delta = crate::LexicalMiniDelta::default();
        let mut collector = ScoreCollector::new(
            Some(max_candidates),
            self.config.max_score_entries.get(),
            Some(context),
        )?;
        for segment in &self.segments {
            context.checkpoint()?;
            let scored = segment.lexical_projection.score_terms_accounted(
                &terms.terms,
                ScoringInputs {
                    delta: &delta,
                    max_term_bytes,
                    retained_score_limit: Some(max_candidates),
                    global_statistics: Some(&statistics.statistics),
                    prune_blocks: false,
                    context: Some(context),
                },
                |id| {
                    if self.visibility.is_visible(segment.content_segment_id, id) {
                        allowed(id)
                    } else {
                        Ok(false)
                    }
                },
            )?;
            report.matching_document_count = report
                .matching_document_count
                .checked_add(scored.report.matching_document_count)
                .ok_or_else(|| HawDBError::Storage("text seed match count overflow".into()))?;
            report.postings_visited = report
                .postings_visited
                .saturating_add(scored.report.postings_visited);
            report.bytes_read = report.bytes_read.saturating_add(scored.report.bytes_read);
            report.document_bytes_read = report
                .document_bytes_read
                .saturating_add(scored.report.document_bytes_read);
            for (id, score) in &scored.report.scores {
                // The complete artifact result remains charged while the global
                // collector admits its copy, including replacement overlap.
                collector.push(id, *score)?;
            }
        }
        let (scores, memory) = collector.finish()?;
        report.returned_count = scores.len();
        let output = SearchTextSeedOutput {
            scores,
            report,
            _memory: memory,
        };
        drop(statistics);
        drop(terms);
        context.checkpoint()?;
        Ok(output)
    }
}
