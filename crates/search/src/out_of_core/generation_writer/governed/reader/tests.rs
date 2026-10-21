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

use super::super::super::SearchGenerationAdmission;
use super::*;
use crate::out_of_core::generation_writer::tests::test_dir;
use crate::test_allocation as allocation;
use hawdb_qos::{
    IoConcurrencyBudget, RuntimeGovernor, RuntimeGovernorConfig, RuntimeMemorySnapshot,
    RuntimeResourceBudget, RuntimeResourceSnapshot, RuntimeWorkRequest,
};
use hawdb_storage::file_io::{self as fs, File};
use hawdb_vector_projection::{
    FileProjection, ProjectionBuildConfig, ProjectionError, ProjectionIdentity, ProjectionWriter,
};
use std::num::NonZeroUsize;
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn governor() -> RuntimeGovernor {
    RuntimeGovernor::new(
        RuntimeGovernorConfig {
            memory_budget_bytes: Some(64 << 20),
            background_task_limit: Some(NonZeroUsize::MIN),
            ..RuntimeGovernorConfig::shared_host()
        },
        RuntimeResourceSnapshot::from_parts(
            RuntimeResourceBudget::from_limits(NonZeroUsize::new(2).unwrap(), None, None),
            RuntimeMemorySnapshot::from_limits(Some(1 << 30), Some(1 << 30), None, None, None),
        ),
        IoConcurrencyBudget::new(2, 1),
    )
}

fn artifact(root: &std::path::Path, model_bytes: usize) -> PathBuf {
    fs::create_dir_all(root).unwrap();
    let path = root.join("projection.hawdb");
    let mut identity = ProjectionIdentity::new(1);
    identity.embedding_model = Some("m".repeat(model_bytes));
    let mut writer =
        ProjectionWriter::create(&path, ProjectionBuildConfig::new(8, identity)).unwrap();
    writer.push(10, &[1.0; 8]).unwrap();
    drop(writer.finish().unwrap());
    path
}

fn warm(writer: &GovernedSearchGenerationWriter) {
    for account in [
        &writer.writer.memory.input,
        &writer.writer.memory.spool,
        &writer.writer.memory.retained,
    ] {
        drop(account.reserve(1).unwrap());
    }
    drop(
        writer
            .writer
            .task_context
            .acquire_io_wave(NonZeroUsize::MIN)
            .unwrap(),
    );
}

#[test]
fn admitted_vector_metadata_outlives_execution_and_last_clone_retains_same_governor() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture(test_dir("admitted-vector-metadata"));
    let path = artifact(&fixture.0.join("source"), 256 << 10);
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1);
    let writer = SearchGenerationAdmission::acquire(&governor, request)
        .unwrap()
        .create_writer(fixture.0.join("writer"), Default::default())
        .unwrap();
    warm(&writer);
    let input = File::open(&path).unwrap();
    let (projection, peak) =
        allocation::measure(|| FileProjection::open_admitted_file(input, &path, &writer).unwrap());
    assert!(allocation::live() > 256 << 10);
    assert!(peak <= writer.writer.memory.ledger.snapshot().peak_bytes);
    let live = allocation::live();
    let (cloned, clone_peak) = allocation::measure(|| projection.clone());
    assert_eq!(
        clone_peak, live,
        "cloning admitted metadata must allocate nothing"
    );
    assert!(std::ptr::eq(projection.manifest(), cloned.manifest()));
    drop(writer);
    let retained = governor.snapshot();
    assert_eq!(retained.active_cpu_slots, 0);
    assert_eq!(retained.active_background_tasks, 0);
    assert_eq!(retained.active_background_io_slots, 0);
    assert!(retained.admitted_memory_bytes >= allocation::live() as u64);
    let next = governor.try_admit(request).unwrap();
    let task = next.bind_task_context(Default::default());
    let result = cloned
        .search(
            &[1.0; 8],
            1,
            hawdb_vector_projection::ProjectionSearchOptions::new()
                .with_max_parallelism(NonZeroUsize::MIN)
                .with_task_context(&task),
        )
        .unwrap();
    assert_eq!(result.hits.len(), 1);
    assert_eq!(result.hits[0].id, 10);
    drop(result);
    drop(task);
    drop(next);
    drop(projection);
    assert_eq!(
        governor.snapshot().admitted_memory_bytes,
        retained.admitted_memory_bytes
    );
    let other = cloned.clone();
    let start = std::sync::Arc::new(std::sync::Barrier::new(3));
    let first_start = start.clone();
    let first = std::thread::spawn(move || {
        first_start.wait();
        drop(cloned);
    });
    let second_start = start.clone();
    let second = std::thread::spawn(move || {
        second_start.wait();
        drop(other);
    });
    start.wait();
    first.join().unwrap();
    second.join().unwrap();
    assert_eq!(allocation::live(), 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn admitted_vector_encoded_budget_refuses_before_large_allocation_and_refunds() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture(test_dir("admitted-vector-encoded-denial"));
    let path = artifact(&fixture.0.join("source"), 3 << 20);
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1);
    let writer = SearchGenerationAdmission::acquire(&governor, request)
        .unwrap()
        .create_writer(fixture.0.join("writer"), Default::default())
        .unwrap();
    warm(&writer);
    let baseline = writer.writer.memory.ledger.snapshot().used_bytes;
    let host = governor.snapshot().admitted_memory_bytes;
    let input = File::open(&path).unwrap();
    let (_, peak) = allocation::measure(|| {
        assert!(matches!(
            FileProjection::open_admitted_file(input, &path, &writer),
            Err(ProjectionError::Admission(_))
        ));
    });
    assert!(
        peak < 3 << 20,
        "the encoded manifest must not be allocated before admission"
    );
    assert_eq!(allocation::live(), 0);
    assert_eq!(writer.writer.memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().admitted_memory_bytes, host);
    drop(writer);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn admitted_vector_compact_record_budget_refuses_before_deserialization() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture(test_dir("admitted-vector-compact-denial"));
    let path = artifact(&fixture.0.join("source"), 0);
    let original = fs::read(&path).unwrap();
    let footer = original.len() - 20;
    let manifest_len =
        u64::from_le_bytes(original[footer..footer + 8].try_into().unwrap()) as usize;
    let payload = footer - manifest_len;
    let mut value: serde_json::Value = serde_json::from_slice(&original[payload..footer]).unwrap();
    let template = &value["segments"][0];
    let fields = [
        "index",
        "base_ordinal",
        "row_count",
        "payload_offset",
        "payload_bytes",
        "payload_checksum",
    ];
    let record =
        serde_json::Value::Array(fields.iter().map(|name| template[*name].clone()).collect());
    value["segments"] = serde_json::Value::Array(vec![record; 16385]);
    let encoded = serde_json::to_vec(&value).unwrap();
    let parsed: hawdb_vector_projection::ProjectionManifest =
        serde_json::from_slice(&encoded).unwrap();
    assert_eq!(parsed.segments.len(), 16385);
    drop(parsed);
    assert!(
        encoded.len() < 2 << 20,
        "encoded admission must succeed before record admission refuses"
    );
    // Independent IEEE CRC32 reference for a checksum-valid malformed fixture.
    let checksum = !encoded.iter().fold(!0_u32, |mut state, byte| {
        state ^= u32::from(*byte);
        for _ in 0..8 {
            state = (state >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(state & 1));
        }
        state
    });
    let mut altered = original[..payload].to_vec();
    altered.extend_from_slice(&encoded);
    altered.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
    altered.extend_from_slice(&checksum.to_le_bytes());
    altered.extend_from_slice(&original[footer + 12..]);
    fs::write(&path, altered).unwrap();
    assert!(
        matches!(FileProjection::open(&path), Err(ProjectionError::CorruptArtifact(message)) if message.contains("invalid index"))
    );
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1);
    let writer = SearchGenerationAdmission::acquire(&governor, request)
        .unwrap()
        .create_writer(fixture.0.join("writer"), Default::default())
        .unwrap();
    warm(&writer);
    let baseline = writer.writer.memory.ledger.snapshot().used_bytes;
    let host = governor.snapshot().admitted_memory_bytes;
    let input = File::open(&path).unwrap();
    let (_, peak) = allocation::measure(|| {
        assert!(matches!(
            FileProjection::open_admitted_file(input, &path, &writer),
            Err(ProjectionError::Admission(_))
        ));
    });
    assert!(
        peak <= encoded.len() + 64 * 1024,
        "decoded records must not be allocated before admission: {peak}"
    );
    assert_eq!(allocation::live(), 0);
    assert_eq!(writer.writer.memory.ledger.snapshot().used_bytes, baseline);
    assert_eq!(governor.snapshot().admitted_memory_bytes, host);
    drop(writer);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}
