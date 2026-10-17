// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::out_of_core::hydration::source::Source;
use std::path::Path;

/// Conservative per-lookup source reservations, separate from ordering quanta.
/// Routing CPU/index I/O and reader residency are not covered by these counters.
#[derive(Debug, Clone, Copy)]
pub struct SearchBaselineComparisonLimits {
    pub lookups: NonZeroUsize,
    pub compressed_segment_bytes: NonZeroU64,
    pub expanded_segment_bytes: NonZeroU64,
    pub total_compressed_bytes: NonZeroU64,
    pub total_expanded_bytes: NonZeroU64,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SearchBaselineComparisonReport {
    pub compared_documents: usize,
    pub reserved_lookups: usize,
    pub reserved_compressed_bytes: u64,
    pub reserved_expanded_bytes: u64,
}

/// Exact set/content comparison with a held immutable reader, not publication,
/// source-contract, binding/incarnation or independent-vector authority.
pub struct SearchMatchedBaseline<'reader> {
    reader: &'reader SearchOutOfCoreReader,
    report: SearchBaselineComparisonReport,
}
impl SearchMatchedBaseline<'_> {
    pub fn generation(&self) -> u64 {
        self.reader.generation()
    }
    pub fn source_epoch(&self) -> Option<u64> {
        self.reader.source_graph_commit_epoch()
    }
    pub fn report(&self) -> SearchBaselineComparisonReport {
        self.report
    }
}

/// Owns a complete unique expected-document stream independently of the base.
/// No delta artifact is built or published. Successful End also checks the full
/// base document count, detecting retained extra documents.
pub struct SearchOutOfCoreBaselineComparison<'reader> {
    writer: SearchOutOfCoreUnorderedMutationWriter<'reader>,
    limits: SearchBaselineComparisonLimits,
    report: SearchBaselineComparisonReport,
    complete: bool,
    failed: bool,
    _memory: QueryMemoryLease,
}
impl<'reader> SearchOutOfCoreUnorderedMutationWriter<'reader> {
    pub fn compare_complete_baseline(
        self,
        limits: SearchBaselineComparisonLimits,
    ) -> Result<SearchOutOfCoreBaselineComparison<'reader>> {
        SearchOutOfCoreBaselineComparison::take_from(&mut Some(self), limits)
    }
}
impl<'reader> SearchOutOfCoreBaselineComparison<'reader> {
    /// Fallible admission precedes ownership transfer. On refusal the poisoned
    /// writer remains with its source-owning caller, retaining its live permit.
    #[doc(hidden)]
    pub fn take_from(
        stage: &mut Option<SearchOutOfCoreUnorderedMutationWriter<'reader>>,
        limits: SearchBaselineComparisonLimits,
    ) -> Result<Self> {
        let writer = stage.as_mut().ok_or_else(unusable)?;
        if writer.failed
            || !writer.ordering_complete()
            || !writer.control.unique_upserts
            || writer.inspection.started
        {
            writer.failed = true;
            return Err(HawDBError::Execution(
                "baseline comparison requires unique, closed, uninspected full-source output"
                    .into(),
            ));
        }
        writer.failed = true;
        let memory = writer.reserve_session_metadata(std::mem::size_of::<Self>())?;
        writer
            .inner
            .writer
            .stage
            .reserve_additional_disk(limits.expanded_segment_bytes.get())?;
        writer.failed = false;
        Ok(Self {
            writer: stage.take().unwrap(),
            limits,
            report: Default::default(),
            complete: false,
            failed: false,
            _memory: memory,
        })
    }
}
impl SearchOutOfCoreBaselineComparison<'_> {
    pub fn report(&self) -> SearchBaselineComparisonReport {
        self.report
    }
    pub fn advance(
        &mut self,
        limits: SearchMutationSortStepLimits,
    ) -> Result<SearchMutationInspectionStep> {
        if self.failed {
            return Err(unusable());
        }
        self.failed = true;
        let base = self.writer.inner.base_reader();
        let memory = self.writer.inner.writer.memory.clone();
        let task = self.writer.task_context().clone();
        let path = io::GenerationIo::new(&memory, &task).path(
            &self.writer.inner.writer.stage.path,
            Path::new("baseline-source.body"),
        )?;
        let report = &mut self.report;
        let caps = self.limits;
        let result = self.writer.inspect_reduced_next(limits, |input| {
            let Some(entry) = input.entry else {
                if report.compared_documents != base.document_count() {
                    return Err(HawDBError::Execution(
                        "complete baseline document count differs".into(),
                    ));
                }
                return Ok(());
            };
            let run::Entry::Upsert(expected, _) = entry else {
                return Err(HawDBError::Execution(
                    "full baseline cannot contain delete intents".into(),
                ));
            };
            report.reserved_lookups = report
                .reserved_lookups
                .checked_add(1)
                .filter(|n| *n <= caps.lookups.get())
                .ok_or_else(|| {
                    HawDBError::Execution("baseline lookup admission exhausted".into())
                })?;
            report.reserved_compressed_bytes = reserve(
                report.reserved_compressed_bytes,
                caps.compressed_segment_bytes.get(),
                caps.total_compressed_bytes.get(),
            )?;
            report.reserved_expanded_bytes = reserve(
                report.reserved_expanded_bytes,
                caps.expanded_segment_bytes.get(),
                caps.total_expanded_bytes.get(),
            )?;
            let route = base.segment_for_document(entry.id())?.ok_or_else(|| {
                HawDBError::Execution("expected baseline document is absent".into())
            })?;
            let range = route
                .segment
                .payload_range
                .ok_or_else(|| HawDBError::Storage("baseline segment has no payload".into()))?;
            if range.length > caps.compressed_segment_bytes.get() {
                return Err(HawDBError::Execution(
                    "baseline segment exceeds comparison admission".into(),
                ));
            }
            let file = io::GenerationIo::new(&memory, &task).native(&[&path], || {
                OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .read(true)
                    .write(true)
                    .open(&*path)
            })??;
            let actual = Source::capture(
                &base.segments[route.artifact_index],
                route.segment,
                entry.id(),
                file,
                (
                    caps.expanded_segment_bytes
                        .get()
                        .min(base.config.max_uncompressed_segment_bytes.get()),
                    base.config.max_document_header_bytes.get(),
                ),
                &memory,
                &task,
            )?;
            input.control.read(expected.encoded_bytes)?;
            expected.require_source(&actual, &memory, &task)?;
            drop(actual);
            io::GenerationIo::new(&memory, &task)
                .native(&[&path], || fs::remove_file(&*path))??;
            report.compared_documents = report
                .compared_documents
                .checked_add(1)
                .ok_or_else(|| HawDBError::Execution("baseline document count overflow".into()))?;
            Ok(())
        })?;
        self.complete = result.complete;
        self.failed = false;
        Ok(result)
    }
}
impl<'reader> SearchOutOfCoreBaselineComparison<'reader> {
    pub fn finish(self) -> Result<SearchMatchedBaseline<'reader>> {
        if self.failed || !self.complete {
            return Err(HawDBError::Execution(
                "baseline comparison is incomplete or failed".into(),
            ));
        }
        checkpoint(self.writer.task_context())?;
        Ok(SearchMatchedBaseline {
            reader: self.writer.inner.base_reader(),
            report: self.report,
        })
    }
}
fn reserve(current: u64, amount: u64, cap: u64) -> Result<u64> {
    current
        .checked_add(amount)
        .filter(|n| *n <= cap)
        .ok_or_else(|| HawDBError::Execution("baseline source reservation exhausted".into()))
}

#[cfg(test)]
mod tests {
    use super::super::tests::{document, fixture, limits};
    use super::*;
    fn caps() -> SearchBaselineComparisonLimits {
        SearchBaselineComparisonLimits {
            lookups: NonZeroUsize::new(16).unwrap(),
            compressed_segment_bytes: NonZeroU64::new(65536).unwrap(),
            expanded_segment_bytes: NonZeroU64::new(65536).unwrap(),
            total_compressed_bytes: NonZeroU64::new(1048576).unwrap(),
            total_expanded_bytes: NonZeroU64::new(1048576).unwrap(),
        }
    }
    fn stage<'a>(
        reader: &'a SearchOutOfCoreReader,
        docs: Vec<SearchDocument>,
    ) -> SearchOutOfCoreUnorderedMutationWriter<'a> {
        let mut w = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            reader,
            Default::default(),
            limits(1),
            Default::default(),
        )
        .unwrap();
        w.require_unique_upserts().unwrap();
        for doc in docs {
            w.upsert(doc).unwrap();
        }
        w.finish_input().unwrap();
        while !w.ordering_complete() {
            w.advance_ordering(unlimited_step()).unwrap();
        }
        w
    }
    fn docs() -> Vec<SearchDocument> {
        vec![
            document("k", "keep"),
            document("b", "removed"),
            document("a", "old"),
        ]
    }
    #[test]
    fn baseline_compares_complete_set_and_preserves_held_head() {
        let (root, reader) = fixture("baseline-match");
        let mut c = stage(&reader, docs())
            .compare_complete_baseline(caps())
            .unwrap();
        let mut steps = 0;
        while !c.advance(unlimited_step()).unwrap().complete {
            steps += 1;
            assert!(steps < 100);
        }
        assert_eq!(c.report().compared_documents, 3);
        assert_eq!(c.report().reserved_lookups, 3);
        let matched = c.finish().unwrap();
        assert_eq!(matched.generation(), reader.generation());
        assert_eq!(matched.report().compared_documents, 3);
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            reader.generation()
        );
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn baseline_comparison_uses_complete_visible_mutation_closure() {
        let (root, old) = fixture("baseline-visible-closure");
        let mut delta = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &old,
            Default::default(),
            limits(1),
            Default::default(),
        )
        .unwrap();
        delta.delete("a").unwrap();
        delta.upsert(document("b", "new")).unwrap();
        delta.prepare_publication().unwrap().1.publish().unwrap();
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.document_count(), 2);
        let mut c = stage(&reader, vec![document("k", "keep"), document("b", "new")])
            .compare_complete_baseline(caps())
            .unwrap();
        for _ in 0..100 {
            if c.advance(unlimited_step()).unwrap().complete {
                break;
            }
        }
        let matched = c.finish().unwrap();
        assert_eq!(matched.report().compared_documents, 2);
        assert_eq!(matched.generation(), reader.generation());
        drop(reader);
        drop(old);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn baseline_refuses_extra_missing_body_metadata_and_vector_mismatches() {
        for kind in ["extra", "missing", "body", "metadata", "embedding", "empty"] {
            let (root, reader) = fixture(kind);
            let mut expected = docs();
            match kind {
                "extra" => {
                    expected.remove(0);
                }
                "missing" => expected[0].id = "z".into(),
                "body" => expected[0].content = "weep".into(),
                "metadata" => {
                    expected[0].metadata.insert("x".into(), "changed".into());
                }
                "embedding" => expected[0].embedding = Some(vec![1.0, 2.0]),
                _ => expected.clear(),
            }
            let mut c = stage(&reader, expected)
                .compare_complete_baseline(caps())
                .unwrap();
            let mut failed = false;
            for _ in 0..100 {
                match c.advance(unlimited_step()) {
                    Err(_) => {
                        failed = true;
                        break;
                    }
                    Ok(s) => assert!(!s.complete),
                }
            }
            assert!(failed);
            assert!(c.advance(unlimited_step()).is_err());
            assert!(c.finish().is_err());
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                reader.generation()
            );
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn full_source_duplicate_ids_refuse_even_identical_payload_across_runs() {
        for buffer in [1, 8] {
            let (root, reader) = fixture("baseline-duplicate");
            let mut w = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                &reader,
                Default::default(),
                limits(buffer),
                Default::default(),
            )
            .unwrap();
            w.require_unique_upserts().unwrap();
            w.upsert(document("a", "old")).unwrap();
            w.upsert(document("a", "old")).unwrap();
            w.finish_input().unwrap();
            let mut failed = false;
            for _ in 0..100 {
                match w.advance_ordering(unlimited_step()) {
                    Err(e) => {
                        assert!(e.to_string().contains("duplicate full-source"));
                        failed = true;
                        break;
                    }
                    Ok(s) => assert!(!s.ordering_complete),
                }
            }
            assert!(failed);
            assert!(w.prepare_publication().is_err());
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                reader.generation()
            );
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn baseline_limits_and_partial_finish_refuse_without_reset() {
        for kind in [
            "lookup",
            "compressed",
            "expanded",
            "total",
            "partial",
            "delete",
            "mode",
        ] {
            let (root, reader) = fixture(kind);
            let mut w = stage(&reader, docs());
            let mut cap = caps();
            match kind {
                "lookup" => cap.lookups = NonZeroUsize::new(1).unwrap(),
                "compressed" => cap.compressed_segment_bytes = NonZeroU64::new(1).unwrap(),
                "expanded" => cap.expanded_segment_bytes = NonZeroU64::new(1).unwrap(),
                "total" => cap.total_expanded_bytes = NonZeroU64::new(1).unwrap(),
                "mode" => {
                    w.control.unique_upserts = false;
                }
                "delete" => {
                    drop(w);
                    w = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                        &reader,
                        Default::default(),
                        limits(1),
                        Default::default(),
                    )
                    .unwrap();
                    w.require_unique_upserts().unwrap();
                    w.delete("a").unwrap();
                    w.finish_input().unwrap();
                    while !w.ordering_complete() {
                        w.advance_ordering(unlimited_step()).unwrap();
                    }
                }
                _ => {}
            }
            if kind == "mode" {
                assert!(w.compare_complete_baseline(cap).is_err());
            } else {
                let mut c = w.compare_complete_baseline(cap).unwrap();
                if kind != "partial" {
                    let mut refused = false;
                    for _ in 0..100 {
                        if c.advance(unlimited_step()).is_err() {
                            refused = true;
                            break;
                        }
                    }
                    assert!(refused);
                }
                assert!(c.finish().is_err());
            }
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                reader.generation()
            );
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}

#[cfg(test)]
mod handoff_tests {
    use super::super::tests::{document, fixture, limits};
    use super::*;
    use hawdb_qos::{
        IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
        RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
    };
    #[test]
    fn baseline_handoff_admission_failure_retains_stage_and_governor_permit() {
        let (root, reader) = fixture("baseline-handoff");
        let resources = RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        );
        let g = RuntimeGovernor::new(
            RuntimeGovernorConfig {
                background_task_limit: Some(NonZeroUsize::new(1).unwrap()),
                ..RuntimeGovernorConfig::shared_host()
            },
            resources,
            IoConcurrencyBudget::new(2, 1),
        );
        let admission = SearchGenerationAdmission::acquire(
            &g,
            RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1),
        )
        .unwrap();
        let mut writer = admission
            .prepare_unordered_update(&reader, Default::default(), limits(1), Default::default())
            .unwrap();
        writer.require_unique_upserts().unwrap();
        writer.upsert(document("a", "old")).unwrap();
        writer.finish_input().unwrap();
        while !writer.ordering_complete() {
            writer.advance_ordering(unlimited_step()).unwrap();
        }
        let memory = writer.inner.writer.memory.clone();
        let before = memory.ledger.snapshot();
        let needed = std::mem::size_of::<SearchOutOfCoreBaselineComparison<'_>>();
        let pressure = writer
            .reserve_session_metadata(before.budget_bytes - before.used_bytes - (needed - 1))
            .unwrap();
        let mut owned = Some(writer);
        let cap = SearchBaselineComparisonLimits {
            lookups: NonZeroUsize::new(1).unwrap(),
            compressed_segment_bytes: NonZeroU64::new(65536).unwrap(),
            expanded_segment_bytes: NonZeroU64::new(65536).unwrap(),
            total_compressed_bytes: NonZeroU64::new(65536).unwrap(),
            total_expanded_bytes: NonZeroU64::new(65536).unwrap(),
        };
        assert!(SearchOutOfCoreBaselineComparison::take_from(&mut owned, cap).is_err());
        assert!(owned.as_ref().unwrap().failed);
        assert_eq!(g.snapshot().active_background_tasks, 1);
        drop(pressure);
        drop(owned);
        drop(memory);
        assert_eq!(g.snapshot().active_background_tasks, 0);
        assert_eq!(g.snapshot().admitted_memory_bytes, 0);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}
