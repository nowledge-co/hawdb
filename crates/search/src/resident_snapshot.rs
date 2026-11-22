// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Immutable resident generations and their separately admitted query leases.

use super::*;
use hawdb_executor::QueryMemoryAccount;
use hawdb_qos::{RuntimePermit, RuntimeRetainedMemory};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{OnceLock, Weak};

mod capacity;

static READ_GENERATION_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone)]
pub struct SearchIndexReadSnapshot {
    generation: Arc<ReadGeneration>,
}

#[derive(Debug)]
pub(super) struct ReadGeneration {
    id: u64,
    index: SearchIndex,
    freshness: Arc<SearchProjectionFreshness>,
    capacity: OnceLock<usize>,
    leases: Mutex<GenerationLeases>,
}

/// Who pays for a generation's resident memory.
///
/// A published generation is already resident: its publication holds it
/// whether or not anyone reads it, so the host's memory sample already counts
/// it. Readers of a published generation therefore share an unfunded owner.
/// Once the last publication drops, every owner still held by a reader is
/// charged the generation's capacity, because those readers alone keep the
/// superseded generation alive. A reader that pins an already superseded
/// generation must reserve that capacity before it is admitted.
#[derive(Debug, Default)]
struct GenerationLeases {
    publications: usize,
    // One owner per governor; existing leases keep the owner they started with.
    funding: Vec<Weak<LeaseFunding>>,
}

impl GenerationLeases {
    fn shared_with(&mut self, permit: &RuntimePermit) -> Option<Arc<LeaseFunding>> {
        self.funding.retain(|owner| owner.strong_count() > 0);
        self.funding
            .iter()
            .filter_map(Weak::upgrade)
            .find(|owner| owner.shares_governor_with(permit))
    }

    fn insert(&mut self, memory: RuntimeRetainedMemory) -> Arc<LeaseFunding> {
        let funding = Arc::new(LeaseFunding {
            memory: Mutex::new(memory),
        });
        self.funding.push(Arc::downgrade(&funding));
        funding
    }
}

#[derive(Debug)]
struct LeaseFunding {
    memory: Mutex<RuntimeRetainedMemory>,
}

impl LeaseFunding {
    fn shares_governor_with(&self, permit: &RuntimePermit) -> bool {
        self.memory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .shares_governor_with(permit)
    }

    /// Charge the rest of the generation's capacity; idempotent.
    fn fund_superseded(&self, capacity: u64) {
        let mut memory = self
            .memory
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let missing = capacity.saturating_sub(memory.bytes());
        memory.charge_resident(missing);
    }
}

/// A captured source plus retained memory, without CPU, task or I/O admission.
#[derive(Debug)]
pub struct SearchIndexReadLease {
    // Release this reader's source ownership before refunding capacity.
    snapshot: SearchIndexReadSnapshot,
    funding: Arc<LeaseFunding>,
}

impl SearchIndexReadLease {
    pub fn snapshot(&self) -> &SearchIndexReadSnapshot {
        &self.snapshot
    }

    /// Bytes currently charged to the readers sharing this lease's owner:
    /// zero while the generation is published, its retained capacity once it
    /// is superseded.
    pub fn funded_bytes(&self) -> u64 {
        self.funding
            .memory
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .bytes()
    }
}

/// The resident generation a host publication holds.
///
/// While at least one publication of a generation is alive, the generation is
/// resident regardless of readers, so [`SearchIndexReadSnapshot::retain_with_permit`]
/// admits its readers without a new reservation. Dropping the last
/// publication charges the generation's capacity to the readers that still
/// hold it, and that charge is refunded when the last of them drops.
#[derive(Debug)]
pub struct SearchIndexPublishedSnapshot {
    snapshot: SearchIndexReadSnapshot,
}

impl SearchIndexPublishedSnapshot {
    pub fn snapshot(&self) -> &SearchIndexReadSnapshot {
        &self.snapshot
    }
}

impl Drop for SearchIndexPublishedSnapshot {
    fn drop(&mut self) {
        let readers = {
            let mut leases = self
                .snapshot
                .generation
                .leases
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            leases.publications -= 1;
            if leases.publications > 0 {
                return;
            }
            leases
                .funding
                .iter()
                .filter_map(Weak::upgrade)
                .collect::<Vec<_>>()
        };
        if readers.is_empty() {
            return;
        }
        // Capacity may be computed here for the first time; keep it outside
        // the generation lock. A reader that joins meanwhile shares an owner
        // collected above, so it is charged too.
        let capacity = u64::try_from(self.snapshot.retained_capacity_bytes()).unwrap_or(u64::MAX);
        for funding in readers {
            funding.fund_superseded(capacity);
        }
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
            artifact_source_graph_commit_epoch: Mutex::new(
                *self
                    .artifact_source_graph_commit_epoch
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
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
            id: READ_GENERATION_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            index,
            freshness,
            capacity: OnceLock::new(),
            leases: Mutex::default(),
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
            || captured.artifact_source_graph_commit_epoch()
                != live.artifact_source_graph_commit_epoch()
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
    /// Process-local identity for detecting a projection change between
    /// independently acquired pages. This does not retain the generation.
    #[doc(hidden)]
    pub fn page_generation(&self) -> SearchProjectionPageGeneration {
        SearchProjectionPageGeneration::Resident(self.generation.id)
    }

    /// Copies at most `limit + 1` matching IDs from this immutable resident
    /// generation. The embedded facade holds the publication lock for this
    /// short operation instead of acquiring a generation lease.
    #[doc(hidden)]
    pub fn list_projection_ids(
        &self,
        kinds: &BTreeSet<SearchProjectionKind>,
        after: Option<&str>,
        limit: usize,
        task: &RuntimeTaskContext,
        permit: &RuntimePermit,
    ) -> Result<SearchProjectionIdBatch> {
        task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)?;
        let wanted = limit.saturating_add(1);
        let mut ids = Vec::with_capacity(wanted.min(self.generation.index.documents.len()));
        for id in self.generation.index.documents.keys() {
            task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)?;
            if after.is_some_and(|after| id.as_str() <= after)
                || !projection_id_page::requested_kind(id, kinds)
            {
                continue;
            }
            let memory = permit
                .reserve_retained_memory(u64::try_from(id.len()).unwrap_or(u64::MAX))
                .map_err(HawDBError::from)?;
            ids.push((id.clone(), memory));
            if ids.len() == wanted {
                break;
            }
        }
        task.checkpoint().map_err(HawDBError::RuntimeTaskStopped)?;
        Ok(projection_id_page::finish_ids(ids, limit))
    }

    /// Mark this generation as held by a host publication. See
    /// [`SearchIndexPublishedSnapshot`] for how its memory is accounted.
    pub fn publish(self) -> SearchIndexPublishedSnapshot {
        self.generation
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .publications += 1;
        SearchIndexPublishedSnapshot { snapshot: self }
    }

    /// Run after execution admission and after releasing the publication lock.
    /// This is a nonqueueing capacity attempt; callers must drop a refused
    /// source before waiting or retrying admission.
    ///
    /// A generation that a [`SearchIndexPublishedSnapshot`] still holds is
    /// already resident, so pinning it reserves nothing beyond the caller's
    /// own admission. Pinning a superseded generation reserves its retained
    /// capacity, because the lease alone keeps it alive. Concurrent readers of
    /// a generation and one host governor share one owner; if the generation
    /// is superseded while they hold it, that owner is charged its capacity
    /// until the last of them drops. Switching hosts adds a new owner; existing
    /// leases keep their original one.
    ///
    /// A refusal is the typed `HawDBError::RuntimeAdmissionRefused` (usually
    /// `memory_saturated`, retryable while other work holds the memory). It
    /// leaves the permit, the generation and the caller's handle usable.
    ///
    /// The embedded handle calls this for every read that pins the resident
    /// search projection: typed search (`search_candidates*`,
    /// `retrieve_knowledge*`, `search_projection_documents*`, shadow evidence),
    /// `with_bounded_read_snapshot*`, and Cypher reads seeded by
    /// `text_search`/`vector_search`. Other graph and relational reads, and
    /// out-of-core projections, do not retain a resident generation.
    pub fn retain_with_permit(&self, permit: &RuntimePermit) -> Result<SearchIndexReadLease> {
        let lease = |funding| SearchIndexReadLease {
            snapshot: self.clone(),
            funding,
        };
        {
            let mut leases = self
                .generation
                .leases
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(funding) = leases.shared_with(permit) {
                return Ok(lease(funding));
            }
            if leases.publications > 0 {
                let funding = leases.insert(permit.resident_memory_owner());
                return Ok(lease(funding));
            }
        }
        let bytes = u64::try_from(self.retained_capacity_bytes()).unwrap_or(u64::MAX);
        // Admission may sample process memory; invoke it without cache locks.
        let reservation = permit.reserve_retained_memory(bytes);
        let mut leases = self
            .generation
            .leases
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(funding) = leases.shared_with(permit) {
            drop(leases);
            drop(reservation);
            return Ok(lease(funding));
        }
        // A refusal stays the typed, non-poisoning admission error. If the
        // generation was republished meanwhile, the reservation is kept until
        // these readers drop, which only overstates the charge.
        let funding = leases.insert(reservation.map_err(HawDBError::from)?);
        Ok(lease(funding))
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

    /// See [`crate::SearchIndex::exact_vector_search_capacity`].
    pub fn exact_vector_search_capacity(
        &self,
        working_bytes: usize,
        max_document_id_bytes: usize,
    ) -> usize {
        self.generation
            .index
            .exact_vector_search_capacity(working_bytes, max_document_id_bytes)
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

    /// Retrieve candidate evidence from this retained generation under admission.
    pub fn try_search_candidates_with_report_context(
        &self,
        request: &crate::candidate_evidence::SearchCandidateRequest,
        execution: VectorSearchExecutionOptions<'_>,
    ) -> Result<crate::candidate_evidence::SearchCandidateOutput> {
        self.generation
            .index
            .try_search_candidates_with_report_context(request, execution)
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
        // A refusal is the typed, retryable admission error rather than an
        // opaque execution string, so callers can back off and retry.
        let Err(HawDBError::RuntimeAdmissionRefused(refusal)) = current.retain_with_permit(&permit)
        else {
            panic!("retaining over budget must be a typed admission refusal");
        };
        assert_eq!(refusal.code, "memory_saturated");
        assert!(refusal.retryable);
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

    fn small_governor(memory_budget_bytes: u64) -> hawdb_qos::RuntimeGovernor {
        use hawdb_qos::{
            IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
            RuntimeResourceBudget, RuntimeResourceSnapshot,
        };
        RuntimeGovernor::new(
            RuntimeGovernorConfig {
                memory_budget_bytes: Some(memory_budget_bytes),
                ..RuntimeGovernorConfig::shared_host()
            },
            RuntimeResourceSnapshot::from_parts(
                RuntimeResourceBudget::from_limits(std::num::NonZeroUsize::MIN, None, None),
                RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
            ),
            IoConcurrencyBudget::new(2, 1),
        )
    }

    fn control_permit(governor: &hawdb_qos::RuntimeGovernor) -> RuntimePermit {
        governor
            .try_admit(hawdb_qos::RuntimeWorkRequest::new(
                hawdb_qos::RuntimeWorkPriority::Foreground,
                hawdb_qos::RuntimeWorkKind::Control,
            ))
            .unwrap()
    }

    #[test]
    fn published_generation_is_pinned_without_a_new_reservation() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(document("a", &"payload".repeat(4096), "space"))
            .unwrap();
        let published = index.read_snapshot().publish();
        let capacity = published.snapshot().retained_capacity_bytes() as u64;
        // Far less than the generation: reserving it again would be refused.
        let governor = small_governor(capacity / 4);
        let permit = control_permit(&governor);
        for _ in 0..3 {
            // Every idle-to-read transition is admitted without a charge.
            let lease = published.snapshot().retain_with_permit(&permit).unwrap();
            let shared = index.read_snapshot().retain_with_permit(&permit).unwrap();
            assert_eq!(lease.funded_bytes(), 0);
            assert_eq!(shared.funded_bytes(), 0);
            assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
            assert_eq!(
                shared.snapshot().document("a").unwrap().content.len(),
                7 * 4096
            );
        }
        // Publishing the same generation again keeps it publication-funded.
        let republished = index.read_snapshot().publish();
        drop(published);
        let lease = republished.snapshot().retain_with_permit(&permit).unwrap();
        assert_eq!(lease.funded_bytes(), 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }

    #[test]
    fn lease_held_across_publication_charges_the_superseded_generation() {
        let mut index = SearchIndex::in_memory();
        index
            .upsert(document("a", &"payload".repeat(4096), "space"))
            .unwrap();
        let published = index.read_snapshot().publish();
        let old = published.snapshot().clone();
        let capacity = old.retained_capacity_bytes() as u64;
        let governor = small_governor(capacity * 4);
        let permit = control_permit(&governor);
        let first = old.retain_with_permit(&permit).unwrap();
        let second = old.retain_with_permit(&permit).unwrap();
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);

        // A writer publishes a new generation and retires the old one.
        index.upsert(document("a", "replacement", "space")).unwrap();
        let next = index.read_snapshot().publish();
        assert!(!Arc::ptr_eq(&old.generation, &next.snapshot().generation));
        drop(published);
        // Only the readers keep the old generation alive now: charged once.
        assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
        assert_eq!(first.funded_bytes(), capacity);
        assert_eq!(second.funded_bytes(), capacity);
        // A later reader of the superseded generation shares that charge.
        let third = old.retain_with_permit(&permit).unwrap();
        assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
        // Pinning the current generation still reserves nothing.
        let current = next.snapshot().retain_with_permit(&permit).unwrap();
        assert_eq!(current.funded_bytes(), 0);
        assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
        assert_eq!(
            first.snapshot().document("a").unwrap().content.len(),
            7 * 4096
        );
        drop((first, second));
        assert_eq!(governor.snapshot().admitted_memory_bytes, capacity);
        drop(third);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
        assert_eq!(
            current.snapshot().document("a").unwrap().content,
            "replacement"
        );
        drop(current);

        // With no reader left, pinning the superseded generation must reserve
        // it again, and a small budget refuses that with the typed error.
        let small = small_governor(capacity / 4);
        let small_permit = control_permit(&small);
        let Err(HawDBError::RuntimeAdmissionRefused(refusal)) =
            old.retain_with_permit(&small_permit)
        else {
            panic!("pinning a superseded generation over budget must be refused");
        };
        assert_eq!(refusal.code, "memory_saturated");
        assert_eq!(refusal.requested, capacity);
        assert_eq!(small.snapshot().admitted_memory_bytes, 0);
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
                assert!(matches!(lease, Err(HawDBError::RuntimeAdmissionRefused(_))));
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
