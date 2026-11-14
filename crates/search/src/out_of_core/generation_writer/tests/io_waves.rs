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
use hawdb_core::{RuntimeIoWaveController, RuntimeIoWaveError, RuntimeIoWavePermit};
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkPriority, RuntimeWorkRequest,
};
use std::sync::{mpsc, Arc};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Acquire,
    Finished,
}
#[derive(Debug)]
struct ObservedIo {
    task: RuntimeTaskContext,
    events: mpsc::Sender<Event>,
}
impl RuntimeIoWaveController for ObservedIo {
    fn try_acquire(
        &self,
        slots: NonZeroUsize,
        _: &RuntimeTaskContext,
    ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
        match self.task.try_acquire_io_wave(slots)? {
            hawdb_core::RuntimeIoWaveTryAcquire::Acquired(permit) => Ok(permit),
            hawdb_core::RuntimeIoWaveTryAcquire::Pending => Ok(None),
        }
    }
    fn acquire(
        &self,
        slots: NonZeroUsize,
        _: &RuntimeTaskContext,
    ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
        self.events.send(Event::Acquire).unwrap();
        Ok(self.task.acquire_io_wave(slots)?.unwrap())
    }
}
fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(256 << 20),
            background_task_limit: Some(NonZeroUsize::new(4).unwrap()),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(4).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

#[test]
fn standalone_creation_waits_for_original_wave_before_native_effects() {
    let root = test_dir("standalone-io-wait");
    let governor = governor();
    let blocker = governor
        .try_admit(
            RuntimeWorkRequest::io(RuntimeWorkPriority::Background, 0, 0).with_io_wave_slots(1),
        )
        .unwrap();
    let held = blocker
        .bind_task_context(Default::default())
        .acquire_io_wave(NonZeroUsize::MIN)
        .unwrap();
    let permit = Arc::new(
        governor
            .try_admit(RuntimeWorkRequest::background_maintenance(64 << 20).with_io_wave_slots(1))
            .unwrap(),
    );
    let task = permit.bind_task_context(Default::default());
    let cancellation = task.cancellation().clone();
    let (events, receiver) = mpsc::channel();
    let task = task.clone().with_io_wave_controller(Arc::new(ObservedIo {
        task,
        events: events.clone(),
    }));
    let worker_root = root.clone();
    let worker = std::thread::spawn(move || {
        let mut memory = BuildMemory::new(&task).unwrap();
        memory.host_admission = Some(permit);
        let options = context_memory::Options::new(Default::default(), &memory, &task).unwrap();
        let result = SearchOutOfCoreGenerationWriter::create_with_memory(
            &worker_root,
            options,
            task,
            memory,
        );
        events.send(Event::Finished).unwrap();
        result
    });
    let first = receiver.recv_timeout(Duration::from_secs(20)).unwrap();
    let absent = !root.exists();
    cancellation.cancel();
    drop(held);
    let result = worker.join().unwrap();
    let failed = result.is_err();
    drop(result);
    let _ = fs::remove_dir_all(&root);
    assert_eq!(first, Event::Acquire);
    assert!(absent, "native creation preceded wave admission");
    assert!(failed, "cancelled admission unexpectedly created a writer");
}

#[test]
fn idle_writer_and_prepared_token_release_wave_and_single_slot_partitions_complete() {
    for task_scope in [false, true] {
        let root = test_dir("standalone-io-lifecycle");
        let governor = governor();
        let request = RuntimeWorkRequest::background_maintenance(64 << 20);
        let request = if task_scope {
            request.with_io_slots(1)
        } else {
            request.with_io_wave_slots(1)
        };
        let admission = SearchGenerationAdmission::acquire(&governor, request).unwrap();
        let mut writer = admission
            .create_writer_with_context(
                &root,
                SearchOutOfCoreGenerationBuildOptions {
                    max_content_documents: NonZeroUsize::MIN,
                    ..Default::default()
                },
                RuntimeTaskContext::new(
                    hawdb_core::RuntimeCancellationToken::new(),
                    Some(std::time::Instant::now() + Duration::from_secs(20)),
                ),
            )
            .unwrap();
        // Reacquiring the original task's single slot proves no active phase
        // is retained by the wrapper. Task-scope host reservation stays owned.
        drop(
            writer
                .writer()
                .task_context
                .acquire_io_wave(NonZeroUsize::MIN)
                .unwrap(),
        );
        writer.writer_mut().push(document(0)).unwrap();
        writer.writer_mut().push(document(1)).unwrap();
        drop(
            writer
                .writer()
                .task_context
                .acquire_io_wave(NonZeroUsize::MIN)
                .unwrap(),
        );
        let task = writer.writer().task_context.clone();
        let prepared = writer.prepare_publication().unwrap();
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        let report = prepared.publish().unwrap();
        assert_eq!(report.document_count, 2);
        assert!(!report.cleanup_retry_required, "{report:?}");
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn zero_io_declaration_refuses_before_root_creation() {
    let root = test_dir("standalone-zero-io");
    let governor = governor();
    let result = SearchGenerationAdmission::acquire(
        &governor,
        RuntimeWorkRequest::background_maintenance(64 << 20),
    )
    .unwrap()
    .create_writer(&root, Default::default());
    assert!(result.err().unwrap().to_string().contains("reservation"));
    assert!(!root.exists());
}

#[test]
fn unfinished_spool_drop_does_not_flush_without_admission_and_cleanup_retries() {
    let root = test_dir("standalone-drop-io");
    let governor = governor();
    let mut writer = SearchGenerationAdmission::acquire(
        &governor,
        RuntimeWorkRequest::background_maintenance(64 << 20).with_io_wave_slots(1),
    )
    .unwrap()
    .create_writer(&root, Default::default())
    .unwrap();
    writer.writer_mut().push(document(0)).unwrap();
    let spool = writer.writer().spool_path.to_path_buf();
    let len = fs::metadata(&spool).unwrap().len();
    let blocker = governor
        .try_admit(
            RuntimeWorkRequest::io(RuntimeWorkPriority::Background, 0, 0).with_io_wave_slots(1),
        )
        .unwrap();
    let held = blocker
        .bind_task_context(Default::default())
        .acquire_io_wave(NonZeroUsize::MIN)
        .unwrap();
    drop(writer);
    assert_eq!(fs::metadata(&spool).unwrap().len(), len);
    let pending = SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 0).unwrap();
    assert_eq!(pending.pending_stages, 1);
    assert_eq!(
        SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 1)
            .unwrap()
            .pending_stages,
        1
    );
    drop(held);
    let retry = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(64 << 20).with_io_wave_slots(1))
        .unwrap();
    let report = SearchOutOfCoreGenerationWriter::retry_staging_cleanup_with_context(
        &root,
        1,
        &retry.bind_task_context(Default::default()),
    )
    .unwrap();
    assert_eq!(report.pending_stages, 0);
    assert!(!spool.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn input_build_and_publish_wait_before_native_changes_and_cancel_cleanly() {
    enum Work {
        Input(Box<SearchOutOfCoreGenerationWriter>),
        Build(Box<SearchOutOfCoreGenerationWriter>),
        Publish(Box<SearchOutOfCorePreparedGeneration>),
    }
    fn files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut entries = Vec::new();
        for entry in std::fs::read_dir(root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                entries.extend(files(&path));
            } else {
                entries.push((path.clone(), std::fs::read(&path).unwrap()));
            }
        }
        entries.sort();
        entries
    }
    for phase in 0..3 {
        let root = test_dir("standalone-phase-cancel");
        let mut previous =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        previous.push(document(0)).unwrap();
        previous.finish().unwrap();
        let selected = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
        let governor = governor();
        let permit = Arc::new(
            governor
                .try_admit(
                    RuntimeWorkRequest::background_maintenance(64 << 20).with_io_wave_slots(1),
                )
                .unwrap(),
        );
        let task = permit.bind_task_context(RuntimeTaskContext::new(
            hawdb_core::RuntimeCancellationToken::new(),
            Some(std::time::Instant::now() + Duration::from_secs(20)),
        ));
        let mut memory = BuildMemory::new(&task).unwrap();
        memory.host_admission = Some(permit.clone());
        let options = context_memory::Options::new(Default::default(), &memory, &task).unwrap();
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
            &root,
            options,
            task.clone(),
            memory.clone(),
        )
        .unwrap();
        writer.push(document(0)).unwrap();
        let (events, receiver) = mpsc::channel();
        let observed = task.clone().with_io_wave_controller(Arc::new(ObservedIo {
            task: task.clone(),
            events: events.clone(),
        }));
        let work = if phase == 2 {
            let mut prepared = writer.prepare_publication().unwrap();
            prepared.restore_io_task(observed);
            Work::Publish(Box::new(prepared))
        } else {
            writer.set_io_task(observed);
            if phase == 0 {
                Work::Input(Box::new(writer))
            } else {
                Work::Build(Box::new(writer))
            }
        };
        let before = files(&root);
        let blocker = governor
            .try_admit(
                RuntimeWorkRequest::io(RuntimeWorkPriority::Background, 0, 0).with_io_wave_slots(1),
            )
            .unwrap();
        let held = blocker
            .bind_task_context(Default::default())
            .acquire_io_wave(NonZeroUsize::MIN)
            .unwrap();
        let worker = std::thread::spawn(move || {
            let result = match work {
                Work::Input(mut writer) => {
                    let mut document = document(1);
                    document.content = "x".repeat(32 << 10);
                    let result = writer.push(document);
                    assert!(
                        !writer.poisoned,
                        "pre-effect denial poisoned untouched input"
                    );
                    result
                }
                Work::Build(writer) => (*writer).prepare_publication().map(drop),
                Work::Publish(prepared) => (*prepared).publish().map(drop),
            };
            events.send(Event::Finished).unwrap();
            result
        });
        let first = receiver.recv_timeout(Duration::from_secs(20)).unwrap();
        let unchanged = files(&root) == before;
        task.cancellation().cancel();
        drop(held);
        let result = worker.join().unwrap();
        let retry_task = permit.bind_task_context(Default::default());
        let cleanup = SearchOutOfCoreGenerationWriter::retry_staging_cleanup_with_context(
            &root,
            8,
            &retry_task,
        )
        .unwrap();
        assert_eq!(cleanup.pending_stages, 0, "{cleanup:?}");
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(
            fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            selected
        );
        fs::remove_dir_all(root).unwrap();
        assert_eq!(first, Event::Acquire, "phase={phase}");
        assert!(unchanged, "native work preceded phase admission: {phase}");
        assert!(result.is_err(), "cancelled phase completed: {phase}");
    }
}

use std::io::{self, Cursor, Read};
struct GovernedBody {
    task: RuntimeTaskContext,
    input: Cursor<&'static [u8]>,
}
impl Read for GovernedBody {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let _wave = self
            .task
            .acquire_io_wave(NonZeroUsize::MIN)
            .map_err(io::Error::other)?;
        self.input.read(bytes)
    }
}

#[test]
fn streamed_input_yields_output_wave_to_a_governed_reader_at_depth_one() {
    for task_scope in [false, true] {
        let root = test_dir("streamed-source-wave");
        let governor = governor();
        let request = RuntimeWorkRequest::background_maintenance(64 << 20);
        let request = if task_scope {
            request.with_io_slots(1)
        } else {
            request.with_io_wave_slots(1)
        };
        let permit = Arc::new(governor.try_admit(request).unwrap());
        let task = permit.bind_task_context(RuntimeTaskContext::new(
            hawdb_core::RuntimeCancellationToken::new(),
            Some(std::time::Instant::now() + Duration::from_secs(20)),
        ));
        let mut memory = BuildMemory::new(&task).unwrap();
        memory.host_admission = Some(permit.clone());
        let options = context_memory::Options::new(Default::default(), &memory, &task).unwrap();
        let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
            &root,
            options,
            task.clone(),
            memory.clone(),
        )
        .unwrap();
        // Wave requests can use an independently admitted source. Task requests
        // already own the sole lifetime reservation, so their source shares it.
        let source = (!task_scope).then(|| {
            governor
                .try_admit(
                    RuntimeWorkRequest::io(RuntimeWorkPriority::Background, 0, 0)
                        .with_io_wave_slots(1),
                )
                .unwrap()
        });
        let source_task = source.as_ref().map_or_else(
            || task.clone(),
            |permit| permit.bind_task_context(task.clone()),
        );
        let reader = GovernedBody {
            task: source_task,
            input: Cursor::new(b"graph"),
        };
        writer
            .push_reader(
                crate::SearchDocumentHeader {
                    id: "a".into(),
                    title: "a".into(),
                    embedding: None,
                    metadata: Default::default(),
                },
                reader,
                crate::SearchDocumentBody {
                    bytes: 5,
                    expected_checksum: None,
                },
            )
            .unwrap();
        assert_eq!(writer.finish().unwrap().document_count, 1);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        let reader = super::super::super::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(
            reader.hydrate_documents(&["a".into()]).unwrap().documents[0].content,
            "graph"
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
fn post_create_refusal_cleans_the_stage_inside_the_original_wave() {
    for task_scope in [false, true] {
        let root = test_dir("standalone-post-create-cleanup");
        let governor = governor();
        let request = RuntimeWorkRequest::background_maintenance(64 << 20);
        let request = if task_scope {
            request.with_io_slots(1)
        } else {
            request.with_io_wave_slots(1)
        };
        let permit = Arc::new(governor.try_admit(request).unwrap());
        let task = permit.bind_task_context(Default::default());
        let mut memory = BuildMemory::new(&task).unwrap();
        memory.host_admission = Some(permit);
        let options = context_memory::Options::new(
            SearchOutOfCoreGenerationBuildOptions {
                max_metadata_fields: NonZeroUsize::MIN,
                ..Default::default()
            },
            &memory,
            &task,
        )
        .unwrap();
        let error = SearchOutOfCoreGenerationWriter::create_with_memory(
            &root,
            options,
            task.clone(),
            memory.clone(),
        )
        .unwrap_err();
        assert!(
            error.to_string().contains("descriptor field admission"),
            "{error}"
        );
        assert!(root.is_dir(), "refusal must happen after native creation");
        let cleanup = SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 0).unwrap();
        assert_eq!(cleanup.pending_stages, 0, "{cleanup:?}");
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        drop(memory);
        drop(task);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        fs::remove_dir_all(root).unwrap();
    }
}

#[test]
#[ignore = "manual input admission measurement; no throughput gate"]
fn manual_bulk_input_admission_profile() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Debug)]
    struct CountedIo {
        task: RuntimeTaskContext,
        acquisitions: Arc<AtomicUsize>,
    }
    impl RuntimeIoWaveController for CountedIo {
        fn try_acquire(
            &self,
            slots: NonZeroUsize,
            _: &RuntimeTaskContext,
        ) -> std::result::Result<Option<Box<dyn RuntimeIoWavePermit>>, RuntimeIoWaveError> {
            match self.task.try_acquire_io_wave(slots)? {
                hawdb_core::RuntimeIoWaveTryAcquire::Acquired(permit) => {
                    self.acquisitions
                        .fetch_add(usize::from(permit.is_some()), Ordering::Relaxed);
                    Ok(permit)
                }
                hawdb_core::RuntimeIoWaveTryAcquire::Pending => Ok(None),
            }
        }
        fn acquire(
            &self,
            slots: NonZeroUsize,
            _: &RuntimeTaskContext,
        ) -> std::result::Result<Box<dyn RuntimeIoWavePermit>, RuntimeIoWaveError> {
            let permit = self.task.acquire_io_wave(slots)?.unwrap();
            self.acquisitions.fetch_add(1, Ordering::Relaxed);
            Ok(permit)
        }
    }
    const DOCUMENTS: usize = 256;
    let body = "word ".repeat(6554);
    for governed in [false, true] {
        for streamed in [false, true] {
            let root = test_dir("manual-input-admission");
            let governor = governor();
            let acquisitions = Arc::new(AtomicUsize::new(0));
            let (task, permit) = if governed {
                let permit = Arc::new(
                    governor
                        .try_admit(
                            RuntimeWorkRequest::background_maintenance(64 << 20)
                                .with_io_wave_slots(1),
                        )
                        .unwrap(),
                );
                let task = permit.bind_task_context(Default::default());
                (
                    task.clone().with_io_wave_controller(Arc::new(CountedIo {
                        task,
                        acquisitions: acquisitions.clone(),
                    })),
                    Some(permit),
                )
            } else {
                (RuntimeTaskContext::default(), None)
            };
            let mut memory = BuildMemory::new(&task).unwrap();
            memory.host_admission = permit;
            let options = context_memory::Options::new(Default::default(), &memory, &task).unwrap();
            let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
                &root,
                options,
                task,
                memory.clone(),
            )
            .unwrap();
            acquisitions.store(0, Ordering::Relaxed);
            let started = std::time::Instant::now();
            for number in 0..DOCUMENTS {
                let mut source = document(number);
                if streamed {
                    let header = crate::SearchDocumentHeader {
                        id: source.id,
                        title: source.title,
                        embedding: source.embedding,
                        metadata: source.metadata,
                    };
                    writer
                        .push_reader(
                            header,
                            std::io::Cursor::new(body.as_bytes()),
                            crate::SearchDocumentBody {
                                bytes: body.len() as u64,
                                expected_checksum: None,
                            },
                        )
                        .unwrap();
                } else {
                    source.content = body.clone();
                    writer.push(source).unwrap();
                }
            }
            writer.io_phase().unwrap().seal_input_spool().unwrap();
            eprintln!("input_profile governed={governed} streamed={streamed} documents={DOCUMENTS} body_bytes={} wave_acquisitions={} elapsed_us={}",
                DOCUMENTS * body.len(), acquisitions.load(Ordering::Relaxed), started.elapsed().as_micros());
            assert_eq!(writer.document_count, DOCUMENTS);
            drop(writer);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
            fs::remove_dir_all(root).unwrap();
        }
    }
}

mod mutation_phases;
