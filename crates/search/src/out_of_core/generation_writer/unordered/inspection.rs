// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;

/// One borrowed final mutation, or explicit End (`id() == None`). No stage,
/// reader, cursor or document body can escape through this view.
pub struct SearchMutationInspectionInput<'a> {
    pub(super) entry: Option<&'a run::Entry>,
    pub(super) control: &'a mut Control,
    checked: bool,
    failed: bool,
}
impl SearchMutationInspectionInput<'_> {
    pub fn id(&self) -> Option<&str> {
        self.entry.map(run::Entry::id)
    }
    pub fn is_delete(&self) -> bool {
        matches!(self.entry, Some(run::Entry::Delete(_)))
    }
    /// Compare the exact canonical encoding, including body, metadata and
    /// embedding. At most one comparison per callback; errors remain sticky.
    /// Expected-document construction is host work, outside stage accounting.
    pub fn require_document(&mut self, expected: &SearchDocument) -> Result<()> {
        if self.failed || self.checked {
            self.failed = true;
            return Err(HawDBError::Execution(
                "inspection comparison already attempted".into(),
            ));
        }
        self.failed = true;
        self.checked = true;
        let entry = self
            .entry
            .ok_or_else(|| HawDBError::Execution("cannot compare End to a document".into()))?;
        self.control.require_document(entry, expected)?;
        self.failed = false;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SearchMutationInspectionStep {
    pub usage: SearchMutationSortStepUsage,
    /// True for a successful mutation or explicit End callback.
    pub visited: bool,
    pub complete: bool,
}

pub(super) struct Inspection {
    pub(super) started: bool,
    pub(super) complete: bool,
    source: Option<Reader>,
}
impl Inspection {
    pub(super) fn new() -> Self {
        Self {
            started: false,
            complete: false,
            source: None,
        }
    }
}
impl SearchOutOfCoreUnorderedMutationWriter<'_> {
    /// Walk the immutable final reduced run once, in bounded logical I/O/work
    /// quanta. One callback per call, including a checked End. Callback failure,
    /// panic or ignored comparison error poisons the writer. Once begun,
    /// artifact construction refuses until this walk is complete. This alone
    /// does not verify business ownership, a baseline or source coverage.
    pub fn inspect_reduced_next(
        &mut self,
        limits: SearchMutationSortStepLimits,
        visitor: impl FnOnce(&mut SearchMutationInspectionInput<'_>) -> Result<()>,
    ) -> Result<SearchMutationInspectionStep> {
        if self.failed {
            return Err(unusable());
        }
        self.failed = true;
        self.control.begin_step(limits);
        let result = self.inspect_reduced_inner(visitor);
        let usage = self.control.end_step();
        let visited = result?;
        self.failed = false;
        Ok(SearchMutationInspectionStep {
            usage,
            visited,
            complete: self.inspection.complete,
        })
    }
    fn inspect_reduced_inner(
        &mut self,
        visitor: impl FnOnce(&mut SearchMutationInspectionInput<'_>) -> Result<()>,
    ) -> Result<bool> {
        checkpoint(&self.inner.writer.task_context)?;
        if !self.ordering.complete() {
            return Err(HawDBError::Execution(
                "inspection requires closed, completed ordering".into(),
            ));
        }
        if self.inspection.complete {
            return Ok(false);
        }
        if !self.inspection.started {
            if !self.control.allow(1, spool::SPOOL_HEADER.len() as u64, 0)? {
                return Ok(false);
            }
            self.inspection.started = true;
            if let Some(run) = self.ordering.take_reduced() {
                self.inspection.source =
                    Some(Reader::open(run, &mut self.control, &self.inner.writer)?);
            }
        }
        let entry = if let Some(source) = &mut self.inspection.source {
            let Some(entry) = source.next_for_inspection(&mut self.control)? else {
                return Ok(false);
            };
            entry
        } else {
            if !self.control.allow(1, 0, 0)? {
                return Ok(false);
            }
            None
        };
        let ended = entry.is_none();
        let mut input = SearchMutationInspectionInput {
            entry: entry.as_ref(),
            control: &mut self.control,
            checked: false,
            failed: false,
        };
        visitor(&mut input)?;
        if input.failed {
            return Err(HawDBError::Execution(
                "inspection ignored a comparison failure".into(),
            ));
        }
        checkpoint(&self.inner.writer.task_context)?;
        // The scoped entry is dropped before returning its run for finalization.
        drop(entry);
        if ended {
            self.ordering
                .restore_reduced(self.inspection.source.take().map(Reader::into_run));
            self.inspection.complete = true;
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{document, fixture, limits};
    use super::*;

    fn prepared<'a>(
        reader: &'a SearchOutOfCoreReader,
    ) -> SearchOutOfCoreUnorderedMutationWriter<'a> {
        let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            reader,
            Default::default(),
            limits(1),
            Default::default(),
        )
        .unwrap();
        writer.upsert(document("z", "final")).unwrap();
        writer.delete("b").unwrap();
        writer.finish_input().unwrap();
        while !writer.ordering_complete() {
            writer.advance_ordering(unlimited_step()).unwrap();
        }
        writer
    }
    #[test]
    fn reduced_inspection_reads_exact_output_once_and_preserves_artifact() {
        let (root, reader) = fixture("reduced-inspection");
        let mut writer = prepared(&reader);
        let mut ids = Vec::new();
        let quantum = SearchMutationSortStepLimits {
            work: NonZeroUsize::new(1).unwrap(),
            read_bytes: NonZeroU64::new(4096).unwrap(),
            spill_bytes: NonZeroU64::new(1).unwrap(),
        };
        let mut yields = 0;
        for _ in 0..100 {
            let before = writer.report();
            let step = writer
                .inspect_reduced_next(quantum, |input| {
                    ids.push(input.id().map(str::to_owned));
                    if input.id() == Some("z") {
                        input.require_document(&document("z", "final"))?;
                    } else if input.id().is_some() {
                        assert!(input.is_delete());
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(
                writer.report().reserved_read_bytes - before.reserved_read_bytes,
                step.usage.read_bytes
            );
            assert!(
                step.usage.work <= 1
                    && step.usage.read_bytes <= 4096
                    && step.usage.spill_bytes == 0
            );
            if !step.visited {
                yields += 1;
            }
            if step.complete {
                break;
            }
        }
        assert!(yields > 0);
        assert_eq!(ids, [Some("b".into()), Some("z".into()), None]);
        let step = writer
            .inspect_reduced_next(quantum, |_| panic!("End replay"))
            .unwrap();
        assert!(step.complete && !step.visited);
        assert_eq!(
            SearchOutOfCoreReader::open(&root).unwrap().generation(),
            reader.generation()
        );
        let (_, candidate) = writer.prepare_publication().unwrap();
        candidate.publish().unwrap();
        let current = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(current.document_count(), 3);
        assert_eq!(
            current.hydrate_documents(&["z".into()]).unwrap().documents[0].content,
            "final"
        );
        drop(current);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn incomplete_inspection_and_ignored_callback_failures_cannot_finalize() {
        for kind in ["partial", "mismatch", "second", "panic", "callback", "end"] {
            let (root, reader) = fixture(kind);
            let mut writer = prepared(&reader);
            if kind == "second" || kind == "mismatch" {
                writer
                    .inspect_reduced_next(unlimited_step(), |_| Ok(()))
                    .unwrap();
            }
            if kind == "end" {
                for _ in 0..2 {
                    writer
                        .inspect_reduced_next(unlimited_step(), |_| Ok(()))
                        .unwrap();
                }
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                writer.inspect_reduced_next(unlimited_step(), |input| {
                    match kind {
                        "panic" => panic!("verifier panic"),
                        "callback" => return Err(HawDBError::Execution("owner mismatch".into())),
                        "partial" => {}
                        "second" => {
                            input.require_document(&document("z", "final"))?;
                            let _ = input.require_document(&document("z", "final"));
                        }
                        "end" => {
                            let _ = input.require_document(&document("z", "final"));
                        }
                        _ => {
                            let _ = input.require_document(&document("wrong", "wrong"));
                        }
                    }
                    Ok(())
                })
            }));
            if kind == "partial" {
                assert!(result.unwrap().is_ok());
            } else if kind == "panic" {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().is_err());
            }
            assert!(writer.prepare_publication().is_err());
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                reader.generation()
            );
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn inspection_requires_ordering_and_preserves_cumulative_io_admission() {
        for early in [false, true] {
            let (root, reader) = fixture("inspection-admission");
            let mut writer = if early {
                SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                    &reader,
                    Default::default(),
                    limits(1),
                    Default::default(),
                )
                .unwrap()
            } else {
                prepared(&reader)
            };
            if !early {
                writer.control.limits.max_read_bytes =
                    NonZeroU64::new(writer.report().reserved_read_bytes + 8).unwrap();
            }
            assert!(writer
                .inspect_reduced_next(unlimited_step(), |_| Ok(()))
                .is_err());
            assert!(writer.prepare_publication().is_err());
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                reader.generation()
            );
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
    #[test]
    fn inspection_compares_body_metadata_embedding_and_equal_crc_bytes() {
        for kind in ["collision", "metadata", "embedding", "large"] {
            let (root, reader) = fixture(kind);
            let mut actual = document("a", "487a6ddf463ff161");
            if kind == "metadata" {
                actual.metadata.insert("x".into(), "left".into());
            }
            if kind == "embedding" {
                actual.embedding = Some(vec![1.0, 2.0]);
            }
            if kind == "large" {
                actual.content = "x".repeat(65536);
            }
            let mut expected = actual.clone();
            match kind {
                "collision" => expected.content = "301516450b94bbce".into(),
                "metadata" => {
                    expected.metadata.insert("x".into(), "rght".into());
                }
                "embedding" => expected.embedding = Some(vec![1.0, 3.0]),
                _ => {}
            }
            let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
                &reader,
                Default::default(),
                limits(1),
                Default::default(),
            )
            .unwrap();
            writer.upsert(actual).unwrap();
            writer.finish_input().unwrap();
            while !writer.ordering_complete() {
                writer.advance_ordering(unlimited_step()).unwrap();
            }
            let result = writer
                .inspect_reduced_next(unlimited_step(), |input| input.require_document(&expected));
            if kind == "large" {
                assert!(result.is_ok());
            } else {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("differs from expected"));
            }
            drop(writer);
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().generation(),
                reader.generation()
            );
            drop(reader);
            std::fs::remove_dir_all(root).unwrap();
        }
    }

    #[test]
    fn empty_inspection_requires_explicit_end() {
        let (root, reader) = fixture("inspection-empty");
        let mut writer = SearchOutOfCoreGenerationWriter::prepare_unordered_delta(
            &reader,
            Default::default(),
            limits(1),
            Default::default(),
        )
        .unwrap();
        writer.finish_input().unwrap();
        while !writer.ordering_complete() {
            writer.advance_ordering(unlimited_step()).unwrap();
        }
        let step = writer
            .inspect_reduced_next(unlimited_step(), |input| {
                assert!(input.id().is_none() && !input.is_delete());
                Ok(())
            })
            .unwrap();
        assert!(step.complete && step.visited);
        let (_, candidate) = writer.prepare_publication().unwrap();
        drop(candidate);
        drop(reader);
        std::fs::remove_dir_all(root).unwrap();
    }
}
