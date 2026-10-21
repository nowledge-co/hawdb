// Copyright 2026 Nowledge
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::build_memory::{
    read::{tests::governor, QueryIoScope, ReadAdmission, ReadPermit},
    BuildMemory,
};
use hawdb_qos::RuntimeWorkRequest;
use hawdb_storage::{file_descriptors::ProjectFileDescriptors, file_io::SourceDirectory};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn with_query(
    work: impl FnOnce(&Path, &ProjectFileDescriptors, &BuildMemory, QueryContext<'_>, &ScratchDirectory),
) {
    let directory = Directory(crate::test_temp_dir().join(format!(
        "candidate-spill-{}",
        hawdb_core::generate_uuidv7().unwrap()
    )));
    std::fs::create_dir(&directory.0).unwrap();
    let project = ProjectFileDescriptors::acquire(&directory.0, 64).unwrap();
    let parent = SourceDirectory::capture(&directory.0, &project.io_context()).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(hawdb_core::RuntimeTaskContext::with_timeout(
        std::time::Duration::from_secs(10),
    ));
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: ReadPermit::Governed(&permit),
    };
    let mut stage = generation_writer::StageDirectory::create_captured(
        &directory.0,
        &parent,
        &memory,
        &task,
        Some(&admission),
    )
    .unwrap();
    stage.reserve_disk(1 << 20);
    let scratch = stage.scratch_directory().unwrap();
    let scope = QueryIoScope::acquire(&task).unwrap();
    let context = QueryContext {
        memory: &memory.spool,
        task: &task,
        execution: Some(&permit),
        io_scope: Some(&scope),
    };
    assert_eq!(governor.snapshot().active_background_io_slots, 1);
    work(&directory.0, &project, &memory, context, &scratch);
    drop(scope);
    drop(scratch);
    drop(stage);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn captured_candidate_spill_uses_original_fd_domain_and_one_query_io_slot() {
    with_query(|root, project, _, context, scratch| {
        let diagnostic = root.join("not-a-directory");
        std::fs::write(&diagnostic, b"preserve").unwrap();
        let config = SearchOutOfCoreConfig {
            spill_directory: diagnostic.clone(),
            ..Default::default()
        };
        let before = project.metrics();
        let ready = std::sync::Barrier::new(2);
        let release = std::sync::Barrier::new(2);
        std::thread::scope(|threads| {
            threads.spawn(|| {
                let grant = project
                    .reserve(before.limit - before.open - before.reserved)
                    .unwrap();
                ready.wait();
                release.wait();
                drop(grant);
            });
            ready.wait();
            let result = SpillFile::create(&config, Some(context), Some(scratch));
            release.wait();
            let error = result.err().expect("original project is full");
            assert!(matches!(error, HawDBError::FileDescriptors(_)), "{error:?}");
        });
        let foreign_task = context.task.clone();
        let error = SpillFile::create(
            &config,
            Some(QueryContext {
                task: &foreign_task,
                ..context
            }),
            Some(scratch),
        )
        .err()
        .expect("scope belongs to exact original task");
        assert!(matches!(error, HawDBError::Execution(_)), "{error:?}");
        let mut file = SpillFile::create(&config, Some(context), Some(scratch)).unwrap();
        assert_eq!(project.metrics().open, before.open + 1);
        file.write_all(b"private candidates").unwrap();
        file.sync_all().unwrap();
        let mut bytes = [0; 18];
        file.read_exact_at(0, &mut bytes).unwrap();
        assert_eq!(&bytes, b"private candidates");
        assert_eq!(std::fs::read(diagnostic).unwrap(), b"preserve");
        drop(file);
        assert_eq!(project.metrics().open, before.open);
    });
}

fn set<'a>(context: QueryContext<'a>, scratch: &'a ScratchDirectory) -> SpilledCandidateSet<'a> {
    let mut file = SpillFile::create(&Default::default(), Some(context), Some(scratch)).unwrap();
    let mut blocks = Vec::new();
    let mut offset = 0;
    for (segment_id, id) in ["a", "b"].into_iter().enumerate() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(id.len() as u32).to_le_bytes());
        bytes.extend_from_slice(id.as_bytes());
        bytes.extend_from_slice(&(segment_id as u64).to_le_bytes());
        file.write_all(&bytes).unwrap();
        blocks.push(CandidateBlock {
            layer: 0,
            segment_id: segment_id as u64,
            first_document_id: id.into(),
            last_document_id: id.into(),
            offset,
            length: bytes.len() as u64,
            cardinality: 1,
        });
        offset += bytes.len() as u64;
    }
    file.sync_all().unwrap();
    SpilledCandidateSet {
        file,
        blocks,
        cardinality: 2,
        max_block_bytes: 1024,
        cache: Mutex::new(None),
        _memory: None,
    }
}

#[test]
fn candidate_cache_refusal_keeps_previous_cache_and_refunds_new_block() {
    with_query(|_, _, memory, context, scratch| {
        let baseline = memory.ledger.snapshot().used_bytes;
        let set = set(context, scratch);
        let mut metrics = SearchOutOfCoreMetrics::default();
        assert!(set.contains("a", &mut metrics).unwrap());
        let cached = memory.ledger.snapshot().used_bytes;
        let blocked = context
            .memory
            .reserve(context.memory.available_bytes())
            .unwrap();
        assert!(matches!(
            set.contains("b", &mut metrics),
            Err(HawDBError::ResourceBudgetExceeded(_))
        ));
        assert!(set.contains("a", &mut metrics).unwrap());
        assert_eq!(metrics.candidate_block_reads, 1);
        drop(blocked);
        assert_eq!(memory.ledger.snapshot().used_bytes, cached);
        assert!(set.contains("b", &mut metrics).unwrap());
        assert_eq!(metrics.candidate_block_reads, 2);
        drop(set);
        assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    });
}

#[test]
#[cfg(feature = "vector-search")]
fn candidate_ordinals_keep_funding_after_spill_closes() {
    with_query(|_, _, memory, context, scratch| {
        let baseline = memory.ledger.snapshot().used_bytes;
        let set = set(context, scratch);
        let mut metrics = SearchOutOfCoreMetrics::default();
        let ordinals = set
            .vector_ordinals_for_layer(0, 1024, Some(context.task), &mut metrics)
            .unwrap();
        assert_eq!(ordinals.values, [0, 1]);
        drop(set);
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            baseline + ordinals.capacity() * size_of::<u64>()
        );
        drop(ordinals);
        assert_eq!(memory.ledger.snapshot().used_bytes, baseline);
    });
}

#[test]
fn cancelled_candidate_spill_retains_funded_debt_until_fresh_original_cleanup() {
    let directory = Directory(crate::test_temp_dir().join(format!(
        "candidate-spill-cancel-{}",
        hawdb_core::generate_uuidv7().unwrap()
    )));
    #[cfg(unix)]
    let moved = Directory(directory.0.with_extension("moved"));
    std::fs::create_dir(&directory.0).unwrap();
    let project = ProjectFileDescriptors::acquire(&directory.0, 64).unwrap();
    let parent = SourceDirectory::capture(&directory.0, &project.io_context()).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: ReadPermit::Governed(&permit),
    };
    let mut stage = generation_writer::StageDirectory::create_captured(
        &directory.0,
        &parent,
        &memory,
        &task,
        Some(&admission),
    )
    .unwrap();
    stage.reserve_disk(1024);
    let scratch = stage.scratch_directory().unwrap();
    let scope = QueryIoScope::acquire(&task).unwrap();
    let context = QueryContext {
        memory: &memory.spool,
        task: &task,
        execution: Some(&permit),
        io_scope: Some(&scope),
    };
    let mut file = SpillFile::create(&Default::default(), Some(context), Some(&scratch)).unwrap();
    file.write_all(b"retained private bytes").unwrap();
    task.cancellation().cancel();
    assert!(matches!(
        file.write_all(b"refused"),
        Err(HawDBError::Execution(_))
    ));
    drop(file);
    drop(scope);
    drop(scratch);
    drop(stage);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert!(governor.snapshot().admitted_memory_bytes > 0);
    assert!(memory.ledger.snapshot().used_bytes > 0);
    let report = SearchOutOfCoreGenerationWriter::retry_staging_cleanup(&directory.0, 1).unwrap();
    assert_eq!(report.pending_stages, 1);
    assert_eq!(report.blocked_stages, 1);
    assert_eq!(report.reserved_disk_bytes, 1024);
    #[cfg(unix)]
    {
        std::fs::rename(&directory.0, &moved.0).unwrap();
        std::fs::create_dir(&directory.0).unwrap();
        std::fs::write(directory.0.join("foreign"), b"preserve").unwrap();
    }
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(4 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let report =
        SearchOutOfCoreGenerationWriter::retry_staging_cleanup_with_context(&directory.0, 1, &task)
            .unwrap();
    assert_eq!(report.pending_stages, 0);
    assert_eq!(report.removed_stages, 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    #[cfg(unix)]
    {
        assert_eq!(
            std::fs::read(directory.0.join("foreign")).unwrap(),
            b"preserve"
        );
        assert_eq!(std::fs::read_dir(&moved.0).unwrap().count(), 0);
    }
    drop(parent);
    assert_eq!(project.metrics().open, 0);
}
