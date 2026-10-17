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
use crate::build_memory::read::{tests::governor, ReadAdmission};
use crate::build_memory::BuildMemory;
use crate::test_allocation as allocation;
use hawdb_core::RuntimeTaskContext;
use hawdb_qos::RuntimeWorkRequest;

#[test]
fn segment_constructor_admits_descriptor_before_decoding_rejected_field_maps() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let root = test_dir("descriptor-admission");
    let mut index = SearchIndex::open(&root).unwrap();
    index.upsert(document(0, "team")).unwrap();
    index.checkpoint().unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let descriptor = SearchSegmentDescriptor {
        target_documents: 256,
        document_count: 1,
        segments: vec![SearchSegmentDescriptorEntry {
            segment_id: 0,
            first_document_id: "a".into(),
            last_document_id: "z".into(),
            document_count: 1,
            payload_range: None,
            metadata: (0..8192)
                .map(|field| {
                    (
                        format!("field-{field}"),
                        crate::SearchSegmentFieldSummary {
                            present_count: 1,
                            values: BTreeSet::from(["value".into()]),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
        }],
    };
    let mut bytes = Vec::new();
    crate::document_encoding::DescriptorEncoding::new(&descriptor, u64::MAX)
        .unwrap()
        .write_to(&mut bytes)
        .unwrap();
    let mut selected = reader.manifest.segments[0].clone();
    selected.descriptor_len = bytes.len() as u64;
    selected.descriptor_checksum = checksum_bytes(&bytes);
    fs::write(root.join(&selected.descriptor_file), &bytes).unwrap();
    // A schema-decodable, checksum-valid candidate rejected by the later
    // physical-range validator still needs admission before its maps exist.
    let (legacy, legacy_peak) = allocation::measure(|| {
        SearchOutOfCoreSegmentReader::open(
            &root,
            &reader.config,
            &reader.analyzer_lexicon,
            reader.lexical_source_policy,
            reader.lexical_term_policy,
            &reader.manifest,
            &selected,
        )
        .unwrap_err()
    });
    assert!(
        matches!(&legacy, HawDBError::Storage(message) if message.contains("no physical payload range"))
    );
    assert!(legacy_peak > 2 << 20);
    drop(legacy);
    assert_eq!(allocation::live(), 0);
    assert_segment_refused_before_expansion(&root, &reader, &selected, bytes.len());
    drop(reader);
    drop(index);
    fs::remove_dir_all(root).unwrap();
}

fn assert_segment_refused_before_expansion(
    root: &Path,
    reader: &SearchOutOfCoreReader,
    selected: &SearchOutOfCoreSegmentManifest,
    encoded_len: usize,
) {
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let memory = BuildMemory::new(&task).unwrap();
    for account in [&memory.input, &memory.spool, &memory.retained] {
        drop(account.reserve(1).unwrap());
    }
    drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
    let before = memory.ledger.snapshot().used_bytes;
    let host = governor.snapshot().admitted_memory_bytes;
    let (_, peak) = allocation::measure(|| {
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: &permit,
        };
        let result = SearchOutOfCoreSegmentReader::open_with_read_admission(
            SegmentOpenContext {
                root,
                config: &reader.config,
                analyzer_lexicon: &reader.analyzer_lexicon,
                lexical_source_policy: reader.lexical_source_policy,
                lexical_term_policy: reader.lexical_term_policy,
                active_manifest: &reader.manifest,
                admission: Some(&admission),
                max_artifact_read_bytes: 32 << 20,
            },
            selected,
        );
        assert!(matches!(result, Err(HawDBError::Execution(_))));
    });
    assert!(
        peak <= encoded_len + 65536,
        "owned metadata allocated before admission: {peak}"
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, before);
    assert_eq!(governor.snapshot().admitted_memory_bytes, host);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    assert_eq!(allocation::live(), 0);
}

#[test]
fn segment_constructor_admits_compact_layout_before_decoding_rejected_records() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let root = test_dir("layout-admission");
    let mut index = SearchIndex::open(&root).unwrap();
    index.upsert(document(0, "team")).unwrap();
    index.checkpoint().unwrap();
    let reader = SearchOutOfCoreReader::open(&root).unwrap();
    let mut selected = reader.manifest.segments[0].clone();
    // Serde accepts compact sequence records, so the encoded-byte cap alone
    // does not bound the retained struct vector or its growth overlap.
    let records = std::iter::repeat_n("[0,0,[0,0,0,0],[0,0,0,0]]", 16385)
        .collect::<Vec<_>>()
        .join(",");
    let body = format!(
        "{{\"format\":\"{}\",\"generation\":{},\"document_count\":1,\"segments\":[{}]}}",
        OUT_OF_CORE_LAYOUT_FORMAT, selected.generation, records
    );
    let decoded: SearchOutOfCoreLayoutBody = serde_json::from_str(&body).unwrap();
    // The envelope checksum covers canonical serialization of the decoded
    // schema, whereas the file checksum covers the exact compact input.
    let checksum = checksum_bytes(&serde_json::to_vec(&decoded).unwrap());
    let bytes = format!("{{\"body\":{body},\"checksum\":{checksum}}}").into_bytes();
    assert!(bytes.len() < 2 << 20);
    assert_eq!(
        SearchOutOfCoreLayoutBody::decode(&bytes)
            .unwrap()
            .segments
            .len(),
        16385
    );
    selected.layout_len = bytes.len() as u64;
    selected.layout_checksum = checksum_bytes(&bytes);
    fs::write(root.join(&selected.layout_file), &bytes).unwrap();
    let (legacy, peak) = allocation::measure(|| {
        SearchOutOfCoreSegmentReader::open(
            &root,
            &reader.config,
            &reader.analyzer_lexicon,
            reader.lexical_source_policy,
            reader.lexical_term_policy,
            &reader.manifest,
            &selected,
        )
        .unwrap_err()
    });
    assert!(matches!(&legacy, HawDBError::Storage(message) if message.contains("layout header")));
    assert!(
        peak > 2 << 20,
        "fixture must exceed native working budget: {peak}"
    );
    drop(legacy);
    assert_eq!(allocation::live(), 0);
    assert_segment_refused_before_expansion(&root, &reader, &selected, bytes.len());
    drop(reader);
    drop(index);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancelled_layout_decode_preserves_execution_error_before_invalid_storage() {
    let task = RuntimeTaskContext::default();
    task.cancellation().cancel();
    let error =
        SearchOutOfCoreLayoutBody::decode_with_context(b"invalid", Some(&task)).unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)));
    assert!(matches!(
        SearchOutOfCoreLayoutBody::decode(b"invalid"),
        Err(HawDBError::Storage(_))
    ));
}

#[test]
fn admitted_segment_keeps_lexical_metadata_after_producer_close_without_execution() {
    assert_admitted_segment_lifetime(false);
}

#[cfg(feature = "vector-search")]
#[test]
fn admitted_vector_segment_keeps_all_metadata_without_holding_execution() {
    assert_admitted_segment_lifetime(true);
}

fn assert_admitted_segment_lifetime(with_vectors: bool) {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let root = test_dir("admitted-segment-lifetime");
    let mut source = document(0, "team");
    source.embedding = with_vectors.then(|| vec![1.0; 8]);
    let index = if with_vectors {
        // The out-of-core generation writer publishes the vector artifact as
        // part of the selected closure. A resident SearchIndex checkpoint does
        // not select that artifact in this manifest.
        let mut writer = generation_writer::SearchOutOfCoreGenerationWriter::create(
            &root,
            generation_writer::SearchOutOfCoreGenerationBuildOptions {
                embedding_manifest: Some(SearchEmbeddingManifest {
                    model: "admitted-vector-fixture".into(),
                    version: Some("v1".into()),
                    dimension: 8,
                }),
                ..Default::default()
            },
        )
        .unwrap();
        writer.push(source).unwrap();
        writer.finish().unwrap();
        None
    } else {
        let mut index = SearchIndex::open(&root).unwrap();
        index.upsert(source).unwrap();
        index.checkpoint().unwrap();
        Some(index)
    };
    let ordinary = SearchOutOfCoreReader::open(&root).unwrap();
    let governor = governor();
    let request = RuntimeWorkRequest::background_maintenance(16 << 20).with_io_wave_slots(1);
    let permit = governor.try_admit(request).unwrap();
    let task = permit.bind_task_context(RuntimeTaskContext::default());
    let memory = BuildMemory::new(&task).unwrap();
    assert!(memory.host_admission.is_none());
    for account in [&memory.input, &memory.spool, &memory.retained] {
        drop(account.reserve(1).unwrap());
    }
    drop(task.acquire_io_wave(NonZeroUsize::MIN).unwrap());
    let (reader, peak) = allocation::measure(|| {
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: &permit,
        };
        SearchOutOfCoreSegmentReader::open_with_read_admission(
            SegmentOpenContext {
                root: &root,
                config: &ordinary.config,
                analyzer_lexicon: &ordinary.analyzer_lexicon,
                lexical_source_policy: ordinary.lexical_source_policy,
                lexical_term_policy: ordinary.lexical_term_policy,
                active_manifest: &ordinary.manifest,
                admission: Some(&admission),
                max_artifact_read_bytes: 32 << 20,
            },
            &ordinary.manifest.segments[0],
        )
        .unwrap()
    });
    assert!(
        peak <= memory.ledger.snapshot().peak_bytes,
        "peak {peak} exceeds query admission"
    );
    assert_eq!(
        reader.lexical_projection.generation(),
        ordinary.segments[0].lexical_projection.generation()
    );
    #[cfg(feature = "vector-search")]
    assert_eq!(reader.rabitq_projection.is_some(), with_vectors);
    drop(memory);
    drop(task);
    drop(permit);
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
    assert!(snapshot.admitted_memory_bytes >= allocation::live() as u64);
    // measure reports absolute peak live bytes, including the retained view.
    let live = allocation::live();
    let (clones, clone_peak) = allocation::measure(|| [reader.clone(), reader.clone()]);
    assert_eq!(clone_peak, live, "cloning must add zero live allocation");
    assert_eq!(allocation::live(), live);
    assert!(std::ptr::eq(&*reader, &*clones[0]));
    let next = governor.try_admit(request).unwrap();
    #[cfg(feature = "vector-search")]
    if let Some(projection) = reader.rabitq_projection.as_ref() {
        let task = next.bind_task_context(Default::default());
        let result = projection
            .search(
                &[1.0; 8],
                1,
                hawdb_vector_projection::ProjectionSearchOptions::new().with_task_context(&task),
            )
            .unwrap();
        assert_eq!(result.hits.len(), 1);
    }
    drop(next);
    drop(reader);
    assert!(allocation::live() > 0);
    let [first, second] = clones;
    let start = std::sync::Barrier::new(3);
    std::thread::scope(|scope| {
        let start = &start;
        scope.spawn(move || {
            start.wait();
            drop(first);
        });
        scope.spawn(move || {
            start.wait();
            drop(second);
        });
        start.wait();
    });
    assert_eq!(allocation::live(), 0);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(ordinary);
    drop(index);
    fs::remove_dir_all(root).unwrap();
}
