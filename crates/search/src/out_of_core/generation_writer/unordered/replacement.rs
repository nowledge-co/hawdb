// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

/// Sorts a complete uniquely owned document set into a private full replacement.
/// Absence from the input removes old documents. There are no delete/delta APIs
/// and no old-target reads. The held reader supplies policies and expected-base
/// identity, not documents. Those policies are copied during construction, so
/// the writer borrows neither the reader nor its host owner after setup.
/// Source completeness remains the facade's contract.
/// Artifact construction and final installation are still blocking operations.
pub struct SearchOutOfCoreUnorderedReplacementWriter {
    inner: SearchOutOfCoreUnorderedMutationWriter<'static>,
    bound: bool,
    _memory: QueryMemoryLease,
}

pub(in crate::out_of_core::generation_writer) fn from_inner(
    inner: SearchOutOfCoreMutationWriter<'_>,
    limits: SearchUnorderedMutationLimits,
    epoch: Option<u64>,
    max_id_bytes: usize,
) -> Result<SearchOutOfCoreUnorderedReplacementWriter> {
    let inner = inner.into_owned_replacement()?;
    let mut inner = super::from_inner(inner, limits, max_id_bytes)?;
    inner.require_unique_upserts()?;
    let memory = inner.reserve_session_metadata(std::mem::size_of::<
        SearchOutOfCoreUnorderedReplacementWriter,
    >())?;
    let mut result = SearchOutOfCoreUnorderedReplacementWriter {
        inner,
        bound: false,
        _memory: memory,
    };
    if let Some(epoch) = epoch {
        result.bind_source_epoch(epoch)?;
    }
    Ok(result)
}
impl SearchOutOfCoreUnorderedReplacementWriter {
    /// Bind the native captured epoch exactly once, before offering any input.
    #[doc(hidden)]
    pub fn bind_source_epoch(&mut self, epoch: u64) -> Result<()> {
        if self.bound || self.inner.failed || self.inner.control.report.input_operations != 0 {
            self.inner.failed = true;
            return Err(unusable());
        }
        self.inner.failed = true;
        checkpoint(self.task_context())?;
        self.inner
            .inner
            .writer
            .options
            .bind_replacement_epoch(epoch)?;
        self.bound = true;
        self.inner.failed = false;
        Ok(())
    }
    #[doc(hidden)]
    pub fn task_context(&self) -> &RuntimeTaskContext {
        self.inner.task_context()
    }
    #[doc(hidden)]
    pub fn reserve_session_metadata(&self, bytes: usize) -> Result<QueryMemoryLease> {
        self.inner.reserve_session_metadata(bytes)
    }
    pub fn report(&self) -> SearchUnorderedMutationReport {
        self.inner.report()
    }
    pub fn ready_for_input(&self) -> bool {
        self.bound && self.inner.ready_for_input()
    }
    pub fn ordering_complete(&self) -> bool {
        self.bound && self.inner.ordering_complete()
    }
    fn require_bound(&mut self) -> Result<()> {
        if !self.bound {
            self.inner.failed = true;
            return Err(HawDBError::Execution(
                "replacement source epoch is not bound".into(),
            ));
        }
        Ok(())
    }
    pub fn queue_upsert(&mut self, document: SearchDocument) -> Result<()> {
        self.require_bound()?;
        self.inner.queue_upsert(document)
    }
    pub fn finish_input(&mut self) -> Result<()> {
        self.require_bound()?;
        self.inner.finish_input()
    }
    pub fn advance_ordering(
        &mut self,
        limits: SearchMutationSortStepLimits,
    ) -> Result<SearchMutationSortStep> {
        self.require_bound()?;
        self.inner.advance_ordering(limits)
    }
    /// Full replacement only: no base union or mutation targets enter this path.
    pub fn prepare_publication(
        mut self,
    ) -> Result<(
        SearchUnorderedMutationReport,
        SearchOutOfCorePreparedGeneration,
    )> {
        self.finish_input()?;
        while !self.ordering_complete() {
            self.advance_ordering(unlimited_step())?;
        }
        self.inner.failed = true;
        if let Some(run) = self.inner.ordering.take_reduced() {
            let mut source = Reader::open(run, &mut self.inner.control, &self.inner.inner.writer)?;
            while let Some(entry) = source.next(&mut self.inner.control)? {
                let run::Entry::Upsert(record, needs_chinese) = entry else {
                    return Err(HawDBError::Storage(
                        "full replacement contains a delete".into(),
                    ));
                };
                self.inner.control.read(record.encoded_bytes)?;
                self.inner
                    .inner
                    .writer
                    .push_spool_record(*record, needs_chinese)?;
            }
            source.remove(&self.inner.control)?;
        }
        let report = self.inner.report();
        Ok((report, self.inner.inner.writer.prepare_publication()?))
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{document, fixture, limits};
    use super::*;
    fn stage(reader: &SearchOutOfCoreReader) -> SearchOutOfCoreUnorderedReplacementWriter {
        let inner = SearchOutOfCoreMutationWriter::create_replacement(
            reader,
            Default::default(),
            Default::default(),
            BuildMemory::new(&Default::default()).unwrap(),
        )
        .unwrap();
        from_inner(
            inner,
            limits(1),
            Some(100),
            reader.config().max_document_header_bytes.get(),
        )
        .unwrap()
    }
    #[test]
    fn replacement_has_no_old_targets_and_drops_the_complete_old_closure() {
        for empty in [false, true] {
            let (root, reader) = fixture("full-replacement");
            let mut w = stage(&reader);
            assert!(w.inner.inner.writer.mutations.is_none());
            assert!(w.inner.inner.writer.active_manifest_update.is_none());
            let mut expected = document("z", "replacement");
            expected.embedding = Some(vec![0.25, 0.75]);
            expected.metadata.insert("owner".into(), "full".into());
            if !empty {
                w.queue_upsert(expected.clone()).unwrap();
            }
            let (_, candidate) = w.prepare_publication().unwrap();
            assert_eq!(
                candidate.expected_active_generation(),
                Some(reader.generation())
            );
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().document_count(),
                3
            );
            candidate.publish().unwrap();
            let rebuilt = SearchOutOfCoreReader::open(&root).unwrap();
            assert_eq!(rebuilt.document_count(), usize::from(!empty));
            assert_eq!(rebuilt.source_graph_commit_epoch(), Some(100));
            for id in ["a", "b", "k"] {
                assert!(rebuilt.segment_for_document(id).unwrap().is_none());
            }
            assert_eq!(rebuilt.segment_for_document("z").unwrap().is_some(), !empty);
            assert!(rebuilt.manifest.mutation_runs.is_empty());
            let mut check = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                &rebuilt,
                Default::default(),
                limits(1),
                Default::default(),
            )
            .unwrap();
            check.require_unique_upserts().unwrap();
            if !empty {
                check.upsert(expected).unwrap();
            }
            check.finish_input().unwrap();
            while !check.ordering_complete() {
                check.advance_ordering(unlimited_step()).unwrap();
            }
            let mut check = check
                .compare_complete_baseline(SearchBaselineComparisonLimits {
                    lookups: NonZeroUsize::new(2).unwrap(),
                    compressed_segment_bytes: NonZeroU64::new(65536).unwrap(),
                    expanded_segment_bytes: NonZeroU64::new(65536).unwrap(),
                    total_compressed_bytes: NonZeroU64::new(131072).unwrap(),
                    total_expanded_bytes: NonZeroU64::new(131072).unwrap(),
                })
                .unwrap();
            while !check.advance(unlimited_step()).unwrap().complete {}
            assert_eq!(
                check.finish().unwrap().report().compared_documents,
                usize::from(!empty)
            );
        }
    }
    #[test]
    fn replacement_clears_a_mutated_base_closure() {
        let (root, reader) = fixture("full-replacement-mutated-base");
        let mut delta = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(1),
            Default::default(),
        )
        .unwrap();
        delta.delete("b").unwrap();
        delta.upsert(document("a", "updated")).unwrap();
        delta.prepare_publication().unwrap().1.publish().unwrap();
        let base = SearchOutOfCoreReader::open(&root).unwrap();
        assert!(!base.manifest.mutation_runs.is_empty());
        let mut full = stage(&base);
        full.queue_upsert(document("z", "only")).unwrap();
        full.prepare_publication().unwrap().1.publish().unwrap();
        let rebuilt = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(rebuilt.document_count(), 1);
        assert!(rebuilt.manifest.mutation_runs.is_empty());
        assert_eq!(rebuilt.source_graph_commit_epoch(), Some(100));
        for id in ["a", "b", "k"] {
            assert!(rebuilt.segment_for_document(id).unwrap().is_none());
        }
        assert!(rebuilt.segment_for_document("z").unwrap().is_some());
    }
    #[test]
    fn replacement_rejects_duplicate_owners_rebinding_and_unbound_intake() {
        let (_root, reader) = fixture("full-replacement-invalid");
        let incremental = SearchOutOfCoreMutationWriter::create(
            &reader,
            Default::default(),
            Default::default(),
            BuildMemory::new(&Default::default()).unwrap(),
        )
        .unwrap();
        assert!(from_inner(
            incremental,
            limits(1),
            Some(100),
            reader.config().max_document_header_bytes.get()
        )
        .is_err());
        let mut w = stage(&reader);
        for _ in 0..2 {
            w.queue_upsert(document("x", "same")).unwrap();
            while !w.ready_for_input() {
                w.advance_ordering(unlimited_step()).unwrap();
            }
        }
        assert!(w.prepare_publication().is_err());
        let mut w = stage(&reader);
        assert!(w.bind_source_epoch(101).is_err());
        assert!(w.prepare_publication().is_err());
        let inner = SearchOutOfCoreMutationWriter::create_replacement(
            &reader,
            Default::default(),
            Default::default(),
            BuildMemory::new(&Default::default()).unwrap(),
        )
        .unwrap();
        let mut w = from_inner(
            inner,
            limits(1),
            None,
            reader.config().max_document_header_bytes.get(),
        )
        .unwrap();
        assert!(w.queue_upsert(document("x", "body")).is_err());
        assert!(w.bind_source_epoch(100).is_err());
    }
}
