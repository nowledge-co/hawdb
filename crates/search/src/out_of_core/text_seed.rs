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
    external_ids: BTreeMap<String, Option<String>>,
    pub report: SearchTextSeedReport,
    _memory: Option<QueryMemoryLease>,
    _external_ids_memory: Option<QueryMemoryLease>,
}

impl SearchTextSeedOutput {
    pub fn scores(&self) -> &BTreeMap<String, f64> {
        &self.scores
    }

    pub fn external_id(&self, id: &str) -> Option<&str> {
        self.external_ids.get(id).and_then(Option::as_deref)
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
        let context = QueryContext {
            execution: None,
            memory,
            task,
        };
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
                external_ids: BTreeMap::new(),
                report,
                _memory: None,
                _external_ids_memory: None,
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
                .map(|segment| &segment.lexical_projection),
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
            external_ids: BTreeMap::new(),
            report,
            _memory: memory,
            _external_ids_memory: None,
        };
        drop(statistics);
        drop(terms);
        context.checkpoint()?;
        Ok(output)
    }

    /// Applies generic metadata predicates before ranking and retains the
    /// external identities needed by a bounded query result.
    pub fn text_seed_scores_with_metadata_filters(
        &self,
        query_text: &str,
        metadata_filters: &BTreeMap<String, String>,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &crate::RuntimeTaskContext,
    ) -> Result<SearchTextSeedOutput> {
        self.text_seed_scores_with_metadata_filters_and_observer(
            query_text,
            metadata_filters,
            max_candidates,
            memory,
            task,
            |_| Ok(()),
        )
    }

    pub(super) fn text_seed_scores_with_metadata_filters_and_observer(
        &self,
        query_text: &str,
        metadata_filters: &BTreeMap<String, String>,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &crate::RuntimeTaskContext,
        mut observe_candidate: impl FnMut(&str) -> Result<()>,
    ) -> Result<SearchTextSeedOutput> {
        let filter_bytes = metadata_filters
            .iter()
            .try_fold(0usize, |total, (key, value)| {
                total
                    .checked_add(key.len())
                    .and_then(|total| total.checked_add(value.len()))
                    .ok_or_else(|| HawDBError::Execution("text filter size overflow".into()))
            })?;
        let filter_reservation = filter_bytes
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(metadata_filters.len().saturating_mul(4096)))
            .ok_or_else(|| HawDBError::Execution("text filter admission overflow".into()))?;
        let _filter_memory = memory.reserve(filter_reservation)?;
        let predicates = search_metadata_predicate_pushdown(metadata_filters).predicates;
        let mut metadata = AdmittedMetadataLookup::new(self, memory, task);
        let mut output =
            self.text_seed_scores_with_context(query_text, max_candidates, memory, task, |id| {
                observe_candidate(id)?;
                metadata.matches(id, &predicates)
            })?;
        let mut external_ids_memory = memory.reserve(0)?;
        for id in output.scores.keys() {
            let external_id = metadata
                .get(id)?
                .metadata
                .get("external_id")
                .map(String::as_str);
            let bytes = std::mem::size_of::<(String, Option<String>)>()
                .checked_add(128)
                .and_then(|bytes| bytes.checked_add(id.len()))
                .and_then(|bytes| bytes.checked_add(external_id.map_or(0, str::len)))
                .ok_or_else(|| HawDBError::Execution("text external ID size overflow".into()))?;
            external_ids_memory.grow(bytes)?;
            output
                .external_ids
                .insert(id.clone(), external_id.map(str::to_owned));
        }
        output._external_ids_memory = Some(external_ids_memory);
        crate::query_control::checkpoint(task)?;
        Ok(output)
    }
}

struct AdmittedMetadataSegment {
    layer: usize,
    segment_id: u64,
    documents: Vec<SearchMetadataDocument>,
    _memory: QueryMemoryLease,
}

struct AdmittedMetadataLookup<'a> {
    reader: &'a SearchOutOfCoreReader,
    memory: &'a QueryMemoryAccount,
    task: &'a crate::RuntimeTaskContext,
    cached: Option<AdmittedMetadataSegment>,
}

impl<'a> AdmittedMetadataLookup<'a> {
    fn new(
        reader: &'a SearchOutOfCoreReader,
        memory: &'a QueryMemoryAccount,
        task: &'a crate::RuntimeTaskContext,
    ) -> Self {
        Self {
            reader,
            memory,
            task,
            cached: None,
        }
    }

    fn matches(&mut self, id: &str, predicates: &crate::SearchPredicateSet) -> Result<bool> {
        if predicates.is_empty() {
            return Ok(true);
        }
        if predicates.is_unsatisfiable() {
            return Ok(false);
        }
        let memory = self.memory;
        let document = self.get(id)?;
        let value_bytes = document
            .metadata
            .iter()
            .try_fold(id.len(), |total, (key, value)| {
                total
                    .checked_add(key.len())
                    .and_then(|total| total.checked_add(value.len()))
                    .ok_or_else(|| HawDBError::Execution("text filter value size overflow".into()))
            })?;
        let scratch_bytes = value_bytes
            .checked_mul(8)
            .and_then(|bytes| bytes.checked_add(predicates.predicates().len().saturating_mul(1024)))
            .ok_or_else(|| HawDBError::Execution("text filter scratch overflow".into()))?;
        let _scratch = memory.reserve(scratch_bytes)?;
        Ok(crate::search_metadata_matches_predicates(
            &document.id,
            &document.metadata,
            predicates,
        ))
    }

    fn get(&mut self, id: &str) -> Result<&SearchMetadataDocument> {
        crate::query_control::checkpoint(self.task)?;
        let (layer, segment_id) = self
            .reader
            .segments
            .iter()
            .enumerate()
            .rev()
            .find_map(|(layer, artifact)| {
                self.reader
                    .visibility
                    .is_visible(artifact.content_segment_id, id)
                    .then(|| {
                        artifact
                            .descriptor
                            .segments
                            .iter()
                            .find(|segment| {
                                segment.first_document_id.as_str() <= id
                                    && id <= segment.last_document_id.as_str()
                            })
                            .map(|segment| (layer, segment.segment_id))
                    })
                    .flatten()
            })
            .ok_or_else(|| HawDBError::Storage(format!("text seed document {id} is missing")))?;
        if self
            .cached
            .as_ref()
            .is_none_or(|cached| cached.layer != layer || cached.segment_id != segment_id)
        {
            self.cached = None;
            let artifact = &self.reader.segments[layer];
            let segment = artifact
                .descriptor
                .segments
                .iter()
                .find(|segment| segment.segment_id == segment_id)
                .expect("selected metadata segment exists");
            let memory = self.reserve_segment(artifact, segment)?;
            let documents = self.reader.read_metadata_segment_admitted(
                artifact,
                segment,
                &mut SearchOutOfCoreMetrics::default(),
                self.memory,
                self.task,
            )?;
            self.cached = Some(AdmittedMetadataSegment {
                layer,
                segment_id,
                documents,
                _memory: memory,
            });
        }
        let cached = self.cached.as_ref().expect("metadata segment was loaded");
        let offset = cached
            .documents
            .binary_search_by(|document| document.id.as_str().cmp(id))
            .map_err(|_| HawDBError::Storage(format!("text seed document {id} is missing")))?;
        crate::query_control::checkpoint(self.task)?;
        Ok(&cached.documents[offset])
    }

    fn reserve_segment(
        &self,
        artifact: &SearchOutOfCoreSegmentReader,
        segment: &SearchSegmentDescriptorEntry,
    ) -> Result<QueryMemoryLease> {
        let range =
            SearchOutOfCoreReader::artifact_layout_range(artifact, segment.segment_id)?.metadata;
        let prefix_len = usize::try_from(range.length.min(512)).map_err(|_| {
            HawDBError::Execution("text metadata header exceeds address space".into())
        })?;
        let mut prefix = [0u8; 512];
        read_search_range(
            &artifact.metadata_payload,
            range.offset,
            &mut prefix[..prefix_len],
        )?;
        let header_end = prefix[..prefix_len]
            .windows(2)
            .position(|window| window == b"\n\n")
            .ok_or_else(|| HawDBError::Storage("text metadata header is missing".into()))?;
        let header = std::str::from_utf8(&prefix[..header_end]).map_err(|error| {
            HawDBError::Storage(format!("text metadata header is invalid: {error}"))
        })?;
        let uncompressed_len = crate::parse_snapshot_header(header)?
            .uncompressed_len
            .ok_or_else(|| HawDBError::Storage("text metadata length is missing".into()))?;
        if uncompressed_len as u64 > self.reader.config.max_uncompressed_segment_bytes.get() {
            return Err(HawDBError::Storage(
                "text metadata length exceeds the segment limit".into(),
            ));
        }
        let compressed_bytes = usize::try_from(range.length).map_err(|_| {
            HawDBError::Execution("text metadata range exceeds address space".into())
        })?;
        let bytes = compressed_bytes
            .checked_mul(2)
            .and_then(|bytes| uncompressed_len.checked_mul(16)?.checked_add(bytes))
            .and_then(|bytes| {
                segment
                    .document_count
                    .checked_mul(std::mem::size_of::<SearchMetadataDocument>() + 256)?
                    .checked_add(bytes)
            })
            .ok_or_else(|| HawDBError::Execution("text metadata admission overflow".into()))?;
        self.memory.reserve(bytes)
    }
}
