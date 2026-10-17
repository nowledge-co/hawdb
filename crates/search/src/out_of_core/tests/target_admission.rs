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
use crate::build_memory::{
    read::{tests::governor, ReadAdmission},
    BuildMemory,
};
use hawdb_qos::RuntimeWorkRequest;

struct Fixture {
    root: PathBuf,
    reader: SearchOutOfCoreReader,
    run: SearchOutOfCoreMutationRunManifest,
    _index: SearchIndex,
}
impl Fixture {
    fn new() -> Self {
        Self::with_edit(|_| {})
    }
    fn with_edit(edit: impl FnOnce(&mut mutation_run::SearchMutationRunEntry)) -> Self {
        Self::with_document(document(0, "team"), edit)
    }
    fn with_document(
        old: SearchDocument,
        edit: impl FnOnce(&mut mutation_run::SearchMutationRunEntry),
    ) -> Self {
        let root = test_dir("admitted-target-validation");
        let mut index = SearchIndex::open(&root).unwrap();
        index.upsert(old.clone()).unwrap();
        index.checkpoint().unwrap();
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        install_edited_mutation_run(&root, &old, 0, 2, edit);
        let body = SearchOutOfCoreManifestBody::decode(
            &std::fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        )
        .unwrap();
        Self {
            root,
            reader,
            run: body.mutation_runs[0].clone(),
            _index: index,
        }
    }
    fn open_run(&self, admission: &ReadAdmission<'_>) -> mutation_run::SearchMutationRun {
        let mut budget = mutation_run::MutationRunBudget::new(32 << 20, 1, 1).unwrap();
        mutation_run::SearchMutationRun::open_with_read_admission(
            &self.root,
            &self.run,
            32 << 20,
            lexical_analyzer_digest(&self.reader.analyzer_lexicon),
            &mut budget,
            Some(admission),
            32 << 20,
        )
        .unwrap()
    }
    fn validate(
        &self,
        run: &mutation_run::SearchMutationRun,
        admission: &ReadAdmission<'_>,
    ) -> Result<()> {
        mutation_run::validate_targets_with_admission(
            &self.root,
            &self.reader.segments,
            std::iter::once(run),
            &self.reader.config,
            &self.reader.analyzer_lexicon,
            self.reader.lexical_source_policy,
            Some((admission, 32 << 20)),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn target_validation_uses_one_admission_for_probe_hydration_and_stored_terms() {
    let fixture = Fixture::new();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: &permit,
    };
    let run = fixture.open_run(&admission);
    let baseline = memory.ledger.snapshot().used_bytes;
    fixture.validate(&run, &admission).unwrap();
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    // Governed terms cannot silently fall back to the legacy default task.
    let error = mutation_run::validate_targets(
        &fixture.root,
        &fixture.reader.segments,
        std::iter::once(&run),
        &fixture.reader.config,
        &fixture.reader.analyzer_lexicon,
        fixture.reader.lexical_source_policy,
    )
    .unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
    task.cancellation().cancel();
    let error = fixture.validate(&run, &admission).unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    drop(run);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}

#[test]
fn target_validation_refuses_current_task_budget_despite_large_legacy_config() {
    let fixture = Fixture::new();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let run = fixture.open_run(&ReadAdmission {
        memory: &memory,
        task: &task,
        permit: &permit,
    });
    drop(memory);
    drop(task);
    drop(permit);
    let retained = governor.snapshot().admitted_memory_bytes;
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(1).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let error = fixture
        .validate(
            &run,
            &ReadAdmission {
                memory: &memory,
                task: &task,
                permit: &permit,
            },
        )
        .unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, retained);
    drop(run);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn target_hydration_cancellation_during_selected_physical_read_refunds_io_and_memory() {
    let fixture = Fixture::new();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: &permit,
    };
    let artifact = &fixture.reader.segments[0];
    let segment = &artifact.descriptor.segments[0];
    let output = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(fixture.root.join("cancelled-body.tmp"))
        .unwrap();
    let baseline = memory.ledger.snapshot().used_bytes;
    use crate::build_control::read_observation as observation;
    observation::take();
    observation::take_max_request();
    let _cancel = observation::cancel_after_bytes(1, task.cancellation().clone());
    let result = hydration::source::Source::capture_with_read_admission(
        artifact,
        segment,
        segment.first_document_id.as_str(),
        output,
        (
            fixture.reader.config.max_uncompressed_segment_bytes.get(),
            fixture.reader.config.max_document_header_bytes.get(),
        ),
        &admission,
        32 << 20,
    );
    assert!(matches!(result, Err(HawDBError::Execution(_))));
    let (_, read) = observation::take();
    assert!(read > 0);
    assert!(observation::take_max_request() <= 8192);
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn admitted_target_validation_rejects_checksum_valid_wrong_retractions_without_publication() {
    for corruption in ["length", "digest", "term"] {
        let fixture = Fixture::with_edit(|entry| match corruption {
            "length" => entry.retraction.lexical_document_len += 1,
            "digest" => entry.retraction.documents_digest ^= 1,
            "term" => {
                entry.retraction.unique_terms.owned_mut().pop().unwrap();
            }
            _ => unreachable!(),
        });
        let original = std::fs::read(fixture.root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let governor = governor();
        let permit = governor
            .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
            .unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: &permit,
        };
        let run = fixture.open_run(&admission);
        let baseline = memory.ledger.snapshot().used_bytes;
        let error = fixture.validate(&run, &admission).unwrap_err();
        assert!(
            matches!(&error, HawDBError::Storage(message) if message.contains("retraction does not match its target document")),
            "{corruption}: {error:?}"
        );
        assert_eq!(
            std::fs::read(fixture.root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            original
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        drop(run);
        drop(memory);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    }
}

#[test]
fn admitted_cjk_target_reanalysis_borrows_execution_until_worker_join_and_refunds() {
    let mut old = document(0, "team");
    old.title = "中国 Graph".into();
    let fixture = Fixture::with_document(old, |_| {});
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: &permit,
    };
    let run = fixture.open_run(&admission);
    let artifact = &fixture.reader.segments[0];
    let segment = &artifact.descriptor.segments[0];
    let output = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(fixture.root.join("cjk-body.tmp"))
        .unwrap();
    let source = hydration::source::Source::capture_with_read_admission(
        artifact,
        segment,
        segment.first_document_id.as_str(),
        output,
        (
            fixture.reader.config.max_uncompressed_segment_bytes.get(),
            fixture.reader.config.max_document_header_bytes.get(),
        ),
        &admission,
        32 << 20,
    )
    .unwrap();
    assert!(source.needs_chinese);
    let baseline = memory.ledger.snapshot().used_bytes;
    let observe = crate::analyzer_workspace::observation::Observe::new();
    // Leave exactly the worker's stack allowance in the shared root. Its
    // frame and bookkeeping must also fit; capture has already completed.
    let competing = memory
        .spool
        .reserve((8 << 20) - baseline - (2 << 20))
        .unwrap();
    let context = || crate::lexical_projection::retraction::RetractionContext {
        root: &fixture.root,
        memory: &memory,
        task: &task,
        needs_chinese: source.needs_chinese,
        source_policy: fixture.reader.lexical_source_policy,
    };
    let error = artifact
        .lexical_projection
        .source_retraction(&source, &fixture.reader.analyzer_lexicon, context(), |_| {
            Ok(())
        })
        .unwrap_err();
    assert!(
        matches!(&error, HawDBError::Execution(message) if message.contains("query memory")),
        "{error:?}"
    );
    observe.assert_joined(0);
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        baseline + competing.bytes()
    );
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(competing);
    let mut expected = run.entries()[0]
        .retraction
        .unique_terms
        .cursor_with_query_context(Some(
            crate::lexical_projection::QueryContext::from_read_admission(&admission),
        ))
        .unwrap();
    let length = artifact
        .lexical_projection
        .source_retraction(
            &source,
            &fixture.reader.analyzer_lexicon,
            context(),
            |term| {
                assert_eq!(expected.next()?.as_deref(), Some(&*term));
                Ok(())
            },
        )
        .unwrap();
    assert!(expected.next().unwrap().is_none());
    assert_eq!(length, run.entries()[0].retraction.lexical_document_len);
    observe.assert_joined(1);
    drop(expected);
    assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(source);
    drop(run);
    drop(memory);
    drop(task);
    drop(permit);
    observe.assert_released();
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.admitted_memory_bytes, 0);
}
