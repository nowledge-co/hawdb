// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::SearchOutOfCoreReader;
use std::num::NonZeroU64;

fn fixture(root: &Path) -> SearchOutOfCoreReader {
    let mut writer = SearchOutOfCoreGenerationWriter::create(root, Default::default()).unwrap();
    writer.push(document(0)).unwrap();
    writer.finish().unwrap();
    SearchOutOfCoreReader::open(root).unwrap()
}

fn files(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(root).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            result.extend(files(&path));
        } else {
            result.push((path.clone(), std::fs::read(path).unwrap()));
        }
    }
    result.sort();
    result
}

#[test]
fn mutation_target_and_unordered_output_wait_and_cancel_before_native_effects() {
    enum Work<'a> {
        Prepare(Box<SearchOutOfCoreGenerationWriter>),
        Target(Box<SearchOutOfCoreMutationWriter<'a>>),
        Ordering(Box<SearchOutOfCoreUnorderedMutationWriter<'a>>),
    }
    for phase in 0..3 {
        let root = test_dir("mutation-wave-cancel");
        let reader = fixture(&root);
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
        let (events, receiver) = mpsc::channel();
        let observed = task.clone().with_io_wave_controller(Arc::new(ObservedIo {
            task: task.clone(),
            events: events.clone(),
        }));
        let work = if phase == 0 {
            let options = context_memory::Options::new(Default::default(), &memory, &task).unwrap();
            let mut writer = SearchOutOfCoreGenerationWriter::create_with_memory(
                &root,
                options,
                task.clone(),
                memory.clone(),
            )
            .unwrap();
            writer.set_io_task(observed);
            Work::Prepare(Box::new(writer))
        } else {
            let mut inner = SearchOutOfCoreMutationWriter::create(
                &reader,
                Default::default(),
                task.clone(),
                memory.clone(),
            )
            .unwrap();
            inner.writer.set_io_task(observed);
            if phase == 2 {
                let mut writer = unordered::from_inner(
                    inner,
                    Default::default(),
                    reader.config().max_document_header_bytes.get(),
                )
                .unwrap();
                writer.queue_upsert(document(1)).unwrap();
                writer.finish_input().unwrap();
                Work::Ordering(Box::new(writer))
            } else {
                Work::Target(Box::new(inner))
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
        let (first, unchanged, result) = std::thread::scope(|scope| {
            let base = &reader;
            let worker = scope.spawn(move || {
                let result = match work {
                    Work::Prepare(writer) => delta::mutation::Prepared::new(
                        base,
                        &writer.stage.path,
                        &writer.memory,
                        &writer.task_context,
                        true,
                    )
                    .map(drop),
                    Work::Target(mut writer) => writer.delete(&document(0).id),
                    Work::Ordering(mut writer) => {
                        writer.advance_ordering(Default::default()).map(drop)
                    }
                };
                events.send(Event::Finished).unwrap();
                result
            });
            let first = receiver.recv_timeout(Duration::from_secs(20)).unwrap();
            let unchanged = files(&root) == before;
            task.cancellation().cancel();
            drop(held);
            (first, unchanged, worker.join().unwrap())
        });
        let retry = permit.bind_task_context(Default::default());
        let cleanup =
            SearchOutOfCoreGenerationWriter::retry_staging_cleanup_with_context(&root, 8, &retry)
                .unwrap();
        assert_eq!(cleanup.pending_stages, 0, "{cleanup:?}");
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(
            fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
            selected
        );
        drop(retry.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        drop(reader);
        fs::remove_dir_all(root).unwrap();
        assert_eq!(first, Event::Acquire, "phase={phase}");
        assert!(unchanged, "native work preceded admission: phase={phase}");
        assert!(result.is_err(), "cancelled mutation completed");
    }
}

#[test]
fn unordered_drop_discards_pending_flush_and_retains_cleanup_debt() {
    let root = test_dir("unordered-drop-wave");
    let reader = fixture(&root);
    let selected = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let governor = governor();
    let mut writer = SearchGenerationAdmission::acquire(
        &governor,
        RuntimeWorkRequest::background_maintenance(64 << 20).with_io_wave_slots(1),
    )
    .unwrap()
    .prepare_unordered_update(
        &reader,
        Default::default(),
        Default::default(),
        RuntimeTaskContext::new(
            hawdb_core::RuntimeCancellationToken::new(),
            Some(std::time::Instant::now() + Duration::from_secs(20)),
        ),
    )
    .unwrap();
    writer.queue_upsert(document(1)).unwrap();
    writer.finish_input().unwrap();
    let quantum = SearchMutationSortStepLimits {
        work: NonZeroUsize::MIN,
        read_bytes: NonZeroU64::new(1 << 20).unwrap(),
        spill_bytes: NonZeroU64::new(1 << 20).unwrap(),
    };
    let spool = (0..16)
        .find_map(|_| {
            writer.advance_ordering(quantum).unwrap();
            files(&root).into_iter().find_map(|(path, bytes)| {
                (path
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("unordered-")
                    && bytes.is_empty())
                .then_some(path)
            })
        })
        .expect("ordering yielded with an unflushed run");
    let task = writer.task_context().clone();
    drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
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
    assert_eq!(fs::metadata(&spool).unwrap().len(), 0);
    let pending = SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&root, 0).unwrap();
    assert_eq!(pending.pending_stages, 1);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        selected
    );
    drop(held);
    let report =
        SearchOutOfCoreGenerationWriter::retry_staging_cleanup_with_context(&root, 8, &task)
            .unwrap();
    assert_eq!(report.pending_stages, 0, "{report:?}");
    assert!(!spool.exists());
    drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
    drop(reader);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn owned_streamed_and_unordered_updates_complete_with_one_slot_and_release_idle_waves() {
    for task_scope in [false, true] {
        let root = test_dir("mutation-wave-lifecycle");
        let reader = fixture(&root);
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
            Some(std::time::Instant::now() + Duration::from_secs(30)),
        ));
        let mut memory = BuildMemory::new(&task).unwrap();
        memory.host_admission = Some(permit.clone());
        let update = delta::SearchOutOfCoreGenerationUpdate::prepare_with_memory(
            &reader,
            SearchProjectionDelta {
                upserts: vec![],
                deletes: vec![document(0).id],
                max_operations: Some(1),
                source_graph_commit_epoch: None,
            },
            Default::default(),
            task.clone(),
            memory.clone(),
        )
        .unwrap();
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        let prepared = update.prepare_publication().unwrap();
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        assert_eq!(prepared.publish().unwrap().0.after_document_count, 0);
        drop(reader);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        let inner = mutations::SearchOutOfCoreMutationWriter::create(
            &reader,
            Default::default(),
            task.clone(),
            memory.clone(),
        )
        .unwrap();
        let mut writer = unordered::from_inner(
            inner,
            SearchUnorderedMutationLimits {
                max_buffer_records: NonZeroUsize::MIN,
                ..Default::default()
            },
            reader.config().max_document_header_bytes.get(),
        )
        .unwrap();
        for number in [2, 0, 1] {
            writer.upsert(document(number)).unwrap();
            drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        }
        let (_, prepared) = writer.prepare_publication().unwrap();
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        assert_eq!(prepared.publish().unwrap().0.after_document_count, 3);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        drop(reader);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.document_count(), 3);
        for number in 0..3 {
            assert_eq!(
                reader
                    .hydrate_documents(&[document(number).id])
                    .unwrap()
                    .documents[0]
                    .content,
                document(number).content
            );
        }
        let mut streamed = SearchOutOfCoreMutationWriter::create(
            &reader,
            Default::default(),
            task.clone(),
            memory.clone(),
        )
        .unwrap();
        let original = document(0);
        streamed
            .upsert_reader(
                crate::SearchDocumentHeader {
                    id: original.id,
                    title: original.title,
                    embedding: original.embedding,
                    metadata: original.metadata,
                },
                GovernedBody {
                    task: task.clone(),
                    input: Cursor::new(b"graph"),
                },
                crate::SearchDocumentBody {
                    bytes: 5,
                    expected_checksum: None,
                },
            )
            .unwrap();
        drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
        assert_eq!(streamed.finish().unwrap().0.after_document_count, 3);
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        drop(reader);
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id])
                .unwrap()
                .documents[0]
                .content,
            "graph"
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}
