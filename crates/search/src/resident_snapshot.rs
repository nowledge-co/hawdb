// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Immutable resident generations and their separately admitted query leases.

use super::*;
use hawdb_executor::QueryMemoryAccount;
use hawdb_qos::{RuntimePermit, RuntimeRetainedMemory};
use std::sync::{OnceLock, Weak};

mod capacity;

#[derive(Debug, Clone)]
pub struct SearchIndexReadSnapshot {
    generation: Arc<ReadGeneration>,
}

#[derive(Debug)]
pub(super) struct ReadGeneration {
    index: SearchIndex,
    freshness: Arc<SearchProjectionFreshness>,
    capacity: OnceLock<usize>,
    funding: Mutex<Weak<RuntimeRetainedMemory>>,
}

/// A captured source plus retained memory, without CPU, task or I/O admission.
#[derive(Debug)]
pub struct SearchIndexReadLease {
    // Release this reader's source ownership before refunding capacity.
    snapshot: SearchIndexReadSnapshot,
    _funding: Arc<RuntimeRetainedMemory>,
}

impl SearchIndexReadLease {
    pub fn snapshot(&self) -> &SearchIndexReadSnapshot {
        &self.snapshot
    }
}

fn same_owner<T: ?Sized>(left: Option<&Arc<T>>, right: Option<&Arc<T>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

impl SearchIndex {
    /// Capture the currently observed local generation without file I/O or
    /// document-body copies. Marker changes made through this owner are included;
    /// out-of-process edits become visible when the index is reopened.
    /// The host must pair this capture with its graph publication.
    pub fn read_snapshot(&self) -> SearchIndexReadSnapshot {
        let lexical = self.lexical_snapshot();
        let markers = self
            .marker_lines
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut cached = self
            .read_generation
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(generation) = cached.upgrade()
            && generation.matches(self, &lexical, &markers)
        {
            return SearchIndexReadSnapshot { generation };
        }
        let freshness = Arc::new(
            self.freshness_with_reasons(
                markers
                    .get(FULL_REINDEX_MARKER)
                    .cloned()
                    .unwrap_or_default(),
                markers
                    .get(METADATA_REPAIR_MARKER)
                    .cloned()
                    .unwrap_or_default(),
            ),
        );
        let (lexical_projection, lexical_delta) = match lexical {
            Some((reader, delta)) => (Some(reader), delta),
            None => (None, Arc::default()),
        };
        let index = SearchIndex {
            documents: self.documents.clone(),
            vector_index_descriptor: self.vector_index_descriptor.clone(),
            vector_scoring: self.vector_scoring,
            vector_document_summary: self.vector_document_summary,
            canonical_vector_source_epoch: self.canonical_vector_source_epoch,
            path: self.path.clone(),
            _project_files: self._project_files.clone(),
            embedding_dimension: self.embedding_dimension,
            rabitq_checkpoint: self.rabitq_checkpoint,
            embedding_manifest: self.embedding_manifest.clone(),
            import_source_graph_commit_epoch: self.import_source_graph_commit_epoch,
            source_graph_commit_epoch: self.source_graph_commit_epoch,
            durable_source_graph_commit_epoch: Mutex::new(
                freshness.durable_source_graph_commit_epoch,
            ),
            marker_lines: Mutex::new(markers.clone()),
            read_freshness: Some(Arc::clone(&freshness)),
            analyzer_lexicon: Arc::clone(&self.analyzer_lexicon),
            lexical_projection: Mutex::new(lexical_projection),
            lexical_delta: Mutex::new(lexical_delta),
            lexical_config: self.lexical_config,
            #[cfg(feature = "vector-search")]
            rabitq_projection: Mutex::new(self.rabitq_projection()),
            #[cfg(feature = "vector-search")]
            rabitq_build_options: self.rabitq_build_options,
            #[cfg(feature = "vector-search")]
            resident_diskann: self.resident_diskann.capture(),
            segment_descriptor: self.segment_descriptor.clone(),
            segment_payload_reader: self.segment_payload_reader.clone(),
            range_read_config: self.range_read_config,
            runtime_capabilities: self.runtime_capabilities,
            telemetry: self.telemetry.clone(),
            ..Self::default()
        };
        let generation = Arc::new(ReadGeneration {
            index,
            freshness,
            capacity: OnceLock::new(),
            funding: Mutex::new(Weak::new()),
        });
        *cached = Arc::downgrade(&generation);
        SearchIndexReadSnapshot { generation }
    }
}

impl ReadGeneration {
    fn matches(
        &self,
        live: &SearchIndex,
        lexical: &Option<(Arc<LexicalProjectionReader>, Arc<LexicalMiniDelta>)>,
        markers: &BTreeMap<String, Vec<String>>,
    ) -> bool {
        let captured = &self.index;
        if !captured.documents.shares_storage_with(&live.documents)
            || captured.vector_index_descriptor != live.vector_index_descriptor
            || captured.vector_scoring != live.vector_scoring
            || captured.vector_document_summary != live.vector_document_summary
            || captured.canonical_vector_source_epoch != live.canonical_vector_source_epoch
            || captured.rabitq_checkpoint != live.rabitq_checkpoint
            || captured.path != live.path
            || captured.embedding_dimension != live.embedding_dimension
            || captured.embedding_manifest != live.embedding_manifest
            || captured.import_source_graph_commit_epoch != live.import_source_graph_commit_epoch
            || captured.source_graph_commit_epoch != live.source_graph_commit_epoch
            || self.freshness.durable_source_graph_commit_epoch
                != *live
                    .durable_source_graph_commit_epoch
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
            || *captured
                .marker_lines
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                != *markers
            || !Arc::ptr_eq(&captured.analyzer_lexicon, &live.analyzer_lexicon)
            || captured.lexical_config != live.lexical_config
            || !same_owner(
                captured.segment_descriptor.as_ref(),
                live.segment_descriptor.as_ref(),
            )
            || !same_owner(
                captured.segment_payload_reader.as_ref(),
                live.segment_payload_reader.as_ref(),
            )
            || captured.range_read_config != live.range_read_config
            || captured.runtime_capabilities != live.runtime_capabilities
            || !same_owner(captured.telemetry.as_ref(), live.telemetry.as_ref())
        {
            return false;
        }
        #[cfg(feature = "vector-search")]
        if captured.rabitq_build_options != live.rabitq_build_options
            || (live.path.is_some()
                && !same_owner(
                    captured.rabitq_projection().as_ref(),
                    live.rabitq_projection().as_ref(),
                ))
        {
            return false;
        }
        match (captured.lexical_snapshot(), lexical) {
            (None, None) => true,
            (Some((reader, delta)), Some((other_reader, other_delta))) => {
                Arc::ptr_eq(&reader, other_reader) && Arc::ptr_eq(&delta, other_delta)
            }
            _ => false,
        }
    }
}

impl SearchIndexReadSnapshot {
    /// Run after execution admission and after releasing the publication lock.
    /// This is a nonqueueing capacity attempt; callers must drop a refused
    /// source before waiting or retrying admission. Concurrent readers of this
    /// generation and current host governor share one charge. Switching hosts
    /// replaces the weak cache; existing leases keep their original funding.
    pub fn retain_with_permit(&self, permit: &RuntimePermit) -> Result<SearchIndexReadLease> {
        let existing = || {
            self.generation
                .funding
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .upgrade()
                .filter(|owner| owner.shares_governor_with(permit))
        };
        if let Some(funding) = existing() {
            return Ok(SearchIndexReadLease {
                snapshot: self.clone(),
                _funding: funding,
            });
        }
        let bytes = u64::try_from(self.retained_capacity_bytes()).unwrap_or(u64::MAX);
        // Admission may sample process memory; invoke it without cache locks.
        let reservation = permit.reserve_retained_memory(bytes);
        let mut entries = self
            .generation
            .funding
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(funding) = entries
            .upgrade()
            .filter(|owner| owner.shares_governor_with(permit))
        {
            drop(entries);
            drop(reservation);
            return Ok(SearchIndexReadLease {
                snapshot: self.clone(),
                _funding: funding,
            });
        }
        let funding =
            Arc::new(reservation.map_err(|error| HawDBError::Execution(error.to_string()))?);
        *entries = Arc::downgrade(&funding);
        Ok(SearchIndexReadLease {
            snapshot: self.clone(),
            _funding: funding,
        })
    }

    /// Conservative generation capacity, computed once outside publication.
    /// Includes native mapped payloads; query scratch and host control objects
    /// (telemetry/governor/file-cache owners) retain their independent funding.
    pub fn retained_capacity_bytes(&self) -> usize {
        *self
            .generation
            .capacity
            .get_or_init(|| capacity::generation(&self.generation))
    }

    pub fn visit_projection_documents(
        &self,
        budget: SearchProjectionScanBudget,
        task: &RuntimeTaskContext,
        visitor: &mut dyn FnMut(&SearchDocument) -> Result<()>,
    ) -> Result<SearchProjectionScanReport> {
        self.generation
            .index
            .visit_projection_documents(budget, task, visitor)
    }
    pub fn document(&self, id: &str) -> Option<&SearchDocument> {
        self.generation.index.document(id)
    }

    pub fn document_count(&self) -> usize {
        self.generation.index.document_count()
    }

    pub fn projection_freshness(&self) -> &SearchProjectionFreshness {
        &self.generation.freshness
    }

    /// Conservative capacity bound for document pages and document buffers.
    /// Projection indexes and query scratch are separate owners, not included.
    pub fn document_retained_capacity_bytes(&self) -> usize {
        self.generation.index.documents.retained_capacity_bytes()
    }

    pub fn text_seed_scores_with_context(
        &self,
        query_text: &str,
        metadata_filters: &BTreeMap<String, String>,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<SearchResidentTextSeedOutput> {
        self.generation.index.text_seed_scores_with_context(
            query_text,
            metadata_filters,
            max_candidates,
            memory,
            task,
        )
    }

    pub fn complete_text_seed_scores_with_context(
        &self,
        query_text: &str,
        metadata_filters: &BTreeMap<String, String>,
        max_candidates: usize,
        memory: &QueryMemoryAccount,
        task: &RuntimeTaskContext,
    ) -> Result<SearchResidentTextSeedOutput> {
        self.generation
            .index
            .complete_text_seed_scores_with_context(
                query_text,
                metadata_filters,
                max_candidates,
                memory,
                task,
            )
    }

    pub fn try_search_with_options_adaptive_vector_projection_context(
        &self,
        query_text: &str,
        query_embedding: Option<&[f32]>,
        mode: SearchMode,
        options: SearchQueryOptions,
        adaptive_options: AdaptiveVectorSearchOptions,
        vector_execution_options: VectorSearchExecutionOptions<'_>,
    ) -> Result<SearchResultSet> {
        self.generation
            .index
            .try_search_with_options_adaptive_vector_projection_context(
                query_text,
                query_embedding,
                mode,
                options,
                adaptive_options,
                vector_execution_options,
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(id: &str, content: &str, space: &str) -> SearchDocument {
        SearchDocument {
            id: id.into(),
            title: id.into(),
            content: content.into(),
            embedding: None,
            metadata: BTreeMap::from([("space_id".into(), space.into())]),
        }
    }

    #[test]
    fn captured_documents_and_configuration_survive_writer_progress() {
        let mut index = SearchIndex::in_memory();
        index.upsert(document("a", "original", "old")).unwrap();
        index.source_graph_commit_epoch = Some(1);
        let snapshot = index.read_snapshot();
        assert!(std::ptr::eq(
            index.document("a").unwrap(),
            snapshot.document("a").unwrap()
        ));
        let index = std::thread::spawn(move || {
            for epoch in 2..=32 {
                index
                    .upsert(document("a", &format!("revision {epoch}"), "new"))
                    .unwrap();
                index.source_graph_commit_epoch = Some(epoch);
            }
            index.delete("a");
            index.set_analyzer_lexicon(SearchAnalyzerLexicon::empty());
            index
        })
        .join()
        .unwrap();
        assert_eq!(snapshot.document("a").unwrap().content, "original");
        assert_eq!(
            snapshot.projection_freshness().source_graph_commit_epoch,
            Some(1)
        );
        assert_eq!(
            index.projection_freshness().source_graph_commit_epoch,
            Some(32)
        );
        assert_eq!(index.document_count(), 0);
        assert_eq!(snapshot.document_count(), 1);
        assert!(
            snapshot.document_retained_capacity_bytes()
                >= snapshot.document("a").unwrap().content.capacity()
        );
        assert!(!Arc::ptr_eq(
            &snapshot.generation.index.analyzer_lexicon,
            &index.analyzer_lexicon
        ));
    }

    #[test]
    fn retained_generation_shares_admission_and_refunds_after_the_last_lease() {
        use hawdb_qos::{
            IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
            RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkKind, RuntimeWorkPriority,
            RuntimeWorkRequest,
        };
        let mut index = SearchIndex::in_memory();
        index
            .upsert(document("a", &"payload".repeat(4096), "space"))
            .unwrap();
        let snapshot = index.read_snapshot();
        assert!(Arc::ptr_eq(
            &snapshot.generation,
            &index.read_snapshot().generation
        ));
        let capacity = snapshot.retained_capacity_bytes() as u64;
        let governor = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(capacity),
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
                RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
            ),
            IoConcurrencyBudget::new(2, 1),
        );
        let permit = governor
            .try_admit(RuntimeWorkRequest::new(
                RuntimeWorkPriority::Foreground,
                RuntimeWorkKind::Control,
            ))
            .unwrap();
        let first = snapshot.retain_with_permit(&permit).unwrap();
        let second = index.read_snapshot().retain_with_permit(&permit).unwrap();
        assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
        let retired = Arc::downgrade(&snapshot.generation);
        drop(snapshot);
        index.upsert(document("a", "replacement", "space")).unwrap();
        let current = index.read_snapshot();
        assert!(current.retain_with_permit(&permit).is_err());
        assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
        assert_eq!(
            first.snapshot().document("a").unwrap().content.len(),
            7 * 4096
        );
        drop(first);
        assert!(retired.upgrade().is_some());
        assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
        drop(second);
        assert!(retired.upgrade().is_none());
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        let next = current.retain_with_permit(&permit).unwrap();
        assert_eq!(
            governor.snapshot().admitted_memory_bytes,
            current.retained_capacity_bytes() as u64
        );
        drop(permit);
        assert_eq!(governor.snapshot().active_cpu_slots, 0);
        assert_eq!(governor.snapshot().active_foreground_tasks, 0);
        assert_eq!(
            next.snapshot().document("a").unwrap().content,
            "replacement"
        );
        drop(next);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }

    #[cfg(feature = "vector-search")]
    #[test]
    fn default_graph_read_generations_preserve_results_across_crud() {
        for prewarm in [false, true] {
            let mut index = SearchIndex::in_memory();
            for id in 0..32 {
                let mut value = document(&format!("vector-{id:02}"), "body", "space");
                value.embedding = Some(vec![1.0, id as f32 / 32.0]);
                index.upsert(value).unwrap();
            }
            let options = SearchQueryOptions {
                limit: 3,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            };
            let search = |index: &SearchIndex, exact: bool| {
                let result = if exact {
                    index.search_with_options_exact(
                        "",
                        Some(&[1.0, 0.0]),
                        SearchMode::Vector,
                        options.clone(),
                    )
                } else {
                    index.search_with_options(
                        "",
                        Some(&[1.0, 0.0]),
                        SearchMode::Vector,
                        options.clone(),
                    )
                }
                .unwrap();
                assert_eq!(
                    result.retrievers[0].backend,
                    if exact {
                        "scalar_vector_scan"
                    } else {
                        "hawdb_diskann_candidate_projection"
                    }
                );
                result
                    .hits
                    .into_iter()
                    .map(|hit| (hit.id, hit.vector_score))
                    .collect::<Vec<_>>()
            };
            if prewarm {
                search(&index, false);
            }
            let old = index.read_snapshot();
            let expected = search(&old.generation.index, true);
            assert_eq!(search(&old.generation.index, false), expected);
            index.delete("vector-00");
            let mut replacement = document("vector-01", "updated", "space");
            replacement.embedding = Some(vec![0.0, 1.0]);
            index.upsert(replacement).unwrap();
            index
                .upsert(document("vector-02", "null", "space"))
                .unwrap();
            let mut inserted = document("new", "inserted", "space");
            inserted.embedding = Some(vec![1.0, 0.0]);
            index.upsert(inserted).unwrap();
            let current = index.read_snapshot();
            assert!(!Arc::ptr_eq(&old.generation, &current.generation));
            assert_eq!(search(&old.generation.index, false), expected);
            assert_eq!(
                search(&current.generation.index, false),
                search(&index, true)
            );
            assert_eq!(search(&index, false), search(&index, true));
            assert_eq!(search(&old.generation.index, false), expected);
        }
    }

    #[cfg(feature = "vector-search")]
    #[test]
    fn resident_vector_generation_funds_and_reuses_one_lazy_projection() {
        use hawdb_qos::{
            IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
            RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkKind, RuntimeWorkPriority,
            RuntimeWorkRequest,
        };
        let mut index = SearchIndex::in_memory();
        for id in 0..17 {
            let mut value = document(&format!("vector-{id}"), "body", "space");
            value.embedding = Some(vec![1.0, 0.5, 0.25, 0.0]);
            index.upsert(value).unwrap();
        }
        let snapshot = index.read_snapshot();
        let capacity = snapshot.retained_capacity_bytes() as u64;
        let resources = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        );
        for fits in [false, true] {
            let governor = RuntimeGovernor::new(
                RuntimeGovernorConfig {
                    memory_budget_bytes: Some(capacity - u64::from(!fits)),
                    ..RuntimeGovernorConfig::shared_host()
                },
                resources,
                IoConcurrencyBudget::new(2, 1),
            );
            let permit = governor
                .try_admit(RuntimeWorkRequest::new(
                    RuntimeWorkPriority::Foreground,
                    RuntimeWorkKind::Control,
                ))
                .unwrap();
            let lease = snapshot.retain_with_permit(&permit);
            if !fits {
                assert!(lease.is_err());
                assert!(snapshot.generation.index.rabitq_projection().is_none());
                continue;
            }
            let lease = lease.unwrap();
            let search = |source: &SearchIndexReadSnapshot| {
                let output = source
                    .try_search_with_options_adaptive_vector_projection_context(
                        "",
                        Some(&[1.0, 0.5, 0.25, 0.0]),
                        SearchMode::Vector,
                        SearchQueryOptions {
                            limit: 3,
                            offset: 0,
                            rank_window: None,
                            fusion_weights: Default::default(),
                            metadata_filters: BTreeMap::new(),
                            policy_epoch: None,
                        },
                        AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Required),
                        VectorSearchExecutionOptions::default(),
                    )
                    .unwrap();
                assert_eq!(output.hits.len(), 3);
                source
                    .generation
                    .index
                    .rabitq_projection()
                    .expect("query must build the projection")
            };
            let first = search(lease.snapshot());
            let second = search(&snapshot);
            assert!(Arc::ptr_eq(&first, &second));
            assert!(Arc::ptr_eq(
                &snapshot.generation,
                &index.read_snapshot().generation
            ));
            assert!(
                first.retained_capacity_bytes()
                    <= capacity as usize - snapshot.document_retained_capacity_bytes()
            );
            assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
            let mut replacement = document("vector-0", "updated", "space");
            replacement.embedding = Some(vec![0.0, 0.0, 0.5, 1.0]);
            index.upsert(replacement).unwrap();
            let current = index.read_snapshot();
            assert!(!Arc::ptr_eq(&snapshot.generation, &current.generation));
            assert!(current.generation.index.rabitq_projection().is_none());
            assert!(Arc::ptr_eq(
                &first,
                &snapshot.generation.index.rabitq_projection().unwrap()
            ));
            drop(lease);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        }
    }

    #[cfg(feature = "full-text-search")]
    #[test]
    fn captured_payload_reads_old_generation_after_checkpoint_replacement() {
        let path = std::env::temp_dir().join(format!(
            "hawdb-resident-snapshot-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let project =
            hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire_with_capacity_limit(
                &path, 64, true,
            )
            .unwrap();
        let mut index = SearchIndex::open(&path).unwrap();
        for (id, space) in [("a", "other"), ("b", "other"), ("c", "selected")] {
            index
                .upsert(document(id, "original payload", space))
                .unwrap();
        }
        index.source_graph_commit_epoch = Some(1);
        index.checkpoint().unwrap();
        drop(index);
        let mut index = SearchIndex::open(&path).unwrap();
        let old = index.read_snapshot();
        let options = SearchQueryOptions {
            limit: 10,
            offset: 0,
            rank_window: None,
            fusion_weights: SearchFusionWeights::default(),
            metadata_filters: BTreeMap::from([("space_id".into(), "selected".into())]),
            policy_epoch: None,
        };
        let search = |snapshot: &SearchIndexReadSnapshot, text: &str| {
            snapshot
                .try_search_with_options_adaptive_vector_projection_context(
                    text,
                    None,
                    SearchMode::Text,
                    options.clone(),
                    AdaptiveVectorSearchOptions::new(CompressedVectorSearchMode::Disabled),
                    VectorSearchExecutionOptions::default(),
                )
                .unwrap()
        };
        index
            .upsert(document("c", "replacement payload", "selected"))
            .unwrap();
        index.source_graph_commit_epoch = Some(2);
        index.checkpoint().unwrap();
        drop(index);
        let current = SearchIndex::open(&path).unwrap();
        let next = current.read_snapshot();
        current
            .mark_full_reindex_needed("new generation only")
            .unwrap();
        let old_result = search(&old, "original");
        assert_eq!(&old_result.projection_freshness, old.projection_freshness());
        assert!(!old_result.projection_freshness.full_reindex_needed);
        assert_eq!(
            old_result
                .projection_freshness
                .durable_source_graph_commit_epoch,
            Some(1)
        );
        assert_eq!(
            old_result
                .hits
                .iter()
                .map(|hit| hit.id.as_str())
                .collect::<Vec<_>>(),
            ["c"]
        );
        assert_eq!(
            old_result
                .candidate_set
                .metadata_predicate_pushdown
                .physical_range_read_count,
            1
        );
        assert!(
            old_result
                .candidate_set
                .metadata_predicate_pushdown
                .physical_bytes_read
                > 0
        );
        assert!(search(&next, "original").hits.is_empty());
        assert_eq!(search(&next, "replacement").hits.len(), 1);
        assert_eq!(old.document("c").unwrap().content, "original payload");
        assert_eq!(next.document("c").unwrap().content, "replacement payload");
        assert_eq!(
            old.projection_freshness().durable_source_graph_commit_epoch,
            Some(1)
        );
        assert_eq!(
            next.projection_freshness()
                .durable_source_graph_commit_epoch,
            Some(2)
        );
        drop(current);
        drop(next);
        assert!(project.metrics().open > 0);
        drop(old);
        assert_eq!(project.metrics().open, 0);
        assert_eq!(project.metrics().reserved, 0);
        drop(project);
        std::fs::remove_dir_all(path).unwrap();
    }
}
