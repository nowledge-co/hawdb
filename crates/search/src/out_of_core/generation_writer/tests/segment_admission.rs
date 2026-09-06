use super::*;
use crate::build_io;
use crate::build_memory::AdmittedDocument;
use crate::out_of_core::generation_writer::{segment_io, segment_memory};
use crate::{
    checksum_bytes, encode_embedding, encode_metadata, encode_search_segment_descriptor_body,
    encode_search_snapshot_text, encode_string, SearchSegmentDescriptor,
    SearchSegmentDescriptorEntry,
};
use skein_core::{RuntimeCancellationToken, RuntimeMemoryReservation, RuntimeTaskContext};
use std::fmt::Write as _;

mod fuzz;

fn memory(limit: usize) -> BuildMemory {
    BuildMemory::new(
        &RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(limit as u64, 0)),
    )
    .unwrap()
}

fn fixture() -> Vec<SearchDocument> {
    let mut documents = (0..4).map(document).collect::<Vec<_>>();
    documents[0].id = "id\0\t\n\u{0130}\u{03a3}".to_owned();
    documents[0].content = "text\0\t\n\u{1f980}".to_owned();
    documents[0].embedding = Some(vec![0.0, -0.0, f32::from_bits(1), f32::NAN, f32::INFINITY]);
    documents[1].embedding = None;
    documents[2].embedding = Some(Vec::new());
    for (index, document) in documents.iter_mut().enumerate() {
        document.metadata.extend([
            ("kind".to_owned(), " Chunk ".to_owned()),
            ("score".to_owned(), format!("{}", index as f64 - 0.5)),
            ("created_at".to_owned(), "2026-09-07T01:02:03Z".to_owned()),
            (
                "metadata.tags".to_owned(),
                " A ,a,,\u{0130},\u{039f}\u{03a3} ".to_owned(),
            ),
            (
                "labels".to_owned(),
                [
                    r#"[" A ", "a", "\u0130", "", "x,y"]"#,
                    "A,a,,B",
                    r#"["broken",42]"#,
                    "[]",
                ][index]
                    .to_owned(),
            ),
        ]);
    }
    documents
}

fn fields(documents: &[SearchDocument]) -> BTreeSet<String> {
    documents
        .iter()
        .flat_map(|document| document.metadata.keys().cloned())
        .collect()
}

fn admit(documents: Vec<SearchDocument>, memory: &BuildMemory) -> Vec<AdmittedDocument> {
    documents
        .into_iter()
        .map(|document| memory.admit_document(document).unwrap())
        .collect()
}

// Keep the original allocation-heavy encodings independent of the new writer.
fn reference_body(kind: segment_io::Kind, documents: &[SearchDocument], base: u64) -> String {
    let mut output = String::from(match kind {
        segment_io::Kind::Document => "SKEIN_SEARCH_SEGMENT_V1\n",
        segment_io::Kind::Metadata => "SKEIN_SEARCH_METADATA_SEGMENT_V1\n",
        segment_io::Kind::Vector => "SKEIN_SEARCH_VECTOR_SEGMENT_V1\n",
    });
    let mut ordinal = base;
    for document in documents {
        match kind {
            segment_io::Kind::Document => {
                writeln!(
                    output,
                    "doc\t{}\t{}\t{}\t{}\t{}",
                    encode_string(&document.id),
                    encode_string(&document.title),
                    encode_string(&document.content),
                    encode_embedding(document.embedding.as_deref()),
                    encode_metadata(&document.metadata)
                )
                .unwrap();
            }
            segment_io::Kind::Metadata => {
                let vector = if document.embedding.is_some() {
                    let value = ordinal.to_string();
                    ordinal += 1;
                    value
                } else {
                    "-".to_owned()
                };
                writeln!(
                    output,
                    "meta\t{}\t{vector}\t{}",
                    encode_string(&document.id),
                    encode_metadata(&document.metadata)
                )
                .unwrap();
            }
            segment_io::Kind::Vector => {
                if document.embedding.is_some() {
                    writeln!(
                        output,
                        "vector\t{ordinal}\t{}\t{}",
                        encode_string(&document.id),
                        encode_embedding(document.embedding.as_deref())
                    )
                    .unwrap();
                    ordinal += 1;
                }
            }
        }
    }
    output
}

#[test]
fn sizing_rejects_before_allocation_and_fixed_buffer_keeps_its_lease() {
    let task = RuntimeTaskContext::default();
    let memory = memory(8);
    build_io::evidence::take();
    assert!(build_io::formatted(7, &memory, &task, |out| out.write_str("12345678")).is_err());
    assert_eq!(build_io::evidence::take(), 0);
    assert!(build_io::formatted(9, &memory, &task, |out| out.write_str("123456789")).is_err());
    assert_eq!(build_io::evidence::take(), 0);
    let mut output =
        build_io::formatted(8, &memory, &task, |out| out.write_str("12345678")).unwrap();
    assert_eq!(output.capacity(), 8);
    assert_eq!(memory.ledger.snapshot().used_bytes, 8);
    assert!(output.write_all(b"x").is_err());
    assert_eq!(output.as_ref(), b"12345678");
    drop(output);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(memory.ledger.snapshot().account_count, 3);
}

#[test]
fn cancellation_precedes_output_allocation() {
    let token = RuntimeCancellationToken::new();
    token.cancel();
    let task = RuntimeTaskContext::without_deadline(token);
    let memory = memory(1024);
    build_io::evidence::take();
    assert!(build_io::formatted(1024, &memory, &task, |out| out.write_str("data")).is_err());
    assert_eq!(build_io::evidence::take(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn all_segment_bodies_preserve_wire_bytes_and_enforce_exact_capacity() {
    let task = RuntimeTaskContext::default();
    let input = fixture();
    let memory = memory(16 * 1024 * 1024);
    let documents = admit(input.clone(), &memory);
    let retained = memory.ledger.snapshot().used_bytes;
    for kind in [
        segment_io::Kind::Document,
        segment_io::Kind::Metadata,
        segment_io::Kind::Vector,
    ] {
        let expected = reference_body(kind, &input, 1_u64 << 40);
        let output = segment_io::body(
            kind,
            &documents,
            1_u64 << 40,
            expected.len() as u64,
            &memory,
            &task,
        )
        .unwrap();
        assert_eq!(output.as_ref(), expected.as_bytes());
        assert_eq!(
            memory.ledger.snapshot().used_bytes,
            retained + output.capacity()
        );
        drop(output);
        build_io::evidence::take();
        assert!(segment_io::body(
            kind,
            &documents,
            1_u64 << 40,
            expected.len() as u64 - 1,
            &memory,
            &task
        )
        .is_err());
        assert_eq!(build_io::evidence::take(), 0);
        assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    }
    assert!(segment_io::body(
        segment_io::Kind::Vector,
        &documents,
        u64::MAX,
        u64::MAX,
        &memory,
        &task
    )
    .is_err());
    drop(documents);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn zstd_workspace_is_admitted_before_codec_entry_and_output_survives_it() {
    let task = RuntimeTaskContext::default();
    let text = "incompressible-ish payload with a small frame";
    let workspace = segment_io::COMPRESSION_WORKSPACE_BYTES;
    let bound = zstd::zstd_safe::compress_bound(text.len());
    for limit in [workspace - 1, workspace + bound - 1, workspace + bound] {
        let memory = memory(limit);
        segment_io::evidence::take();
        let output = segment_io::compress(text.as_bytes(), u64::MAX, &memory, &task);
        if limit < workspace + bound {
            assert!(output.is_err());
            assert_eq!(segment_io::evidence::take(), 0);
        } else {
            let output = output.unwrap();
            assert_eq!(segment_io::evidence::take(), 1);
            assert_eq!(output.as_ref(), encode_search_snapshot_text(text).unwrap());
            assert_eq!(memory.ledger.snapshot().used_bytes, output.capacity());
            assert_eq!(memory.ledger.snapshot().peak_bytes, limit);
            drop(output);
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn streamed_compression_preserves_bytes_across_block_and_window_boundaries() {
    let task = RuntimeTaskContext::default();
    let memory = memory(32 * 1024 * 1024);
    let mut seed = 0x206_c0de_u64;
    for length in [
        0,
        1,
        32768,
        131071,
        131072,
        131073,
        2 * 1024 * 1024 + 131073,
    ] {
        let text = (0..length)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                (32 + seed % 95) as u8 as char
            })
            .collect::<String>();
        let expected = encode_search_snapshot_text(&text).unwrap();
        let output = segment_io::compress(text.as_bytes(), u64::MAX, &memory, &task).unwrap();
        assert_eq!(output.as_ref(), expected, "length {length}");
        drop(output);
        let error =
            segment_io::compress(text.as_bytes(), expected.len() as u64 - 1, &memory, &task);
        assert!(error.is_err());
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

fn descriptor_trial(input: &[SearchDocument], limit: usize, succeeds: bool) -> usize {
    let task = RuntimeTaskContext::default();
    let fields = fields(input);
    let references = input.iter().collect::<Vec<_>>();
    let expected = SearchSegmentDescriptor {
        target_documents: 128,
        document_count: input.len(),
        segments: vec![SearchSegmentDescriptorEntry::from_documents(
            7,
            &references,
            &fields,
        )],
    };
    let expected_body = encode_search_segment_descriptor_body(&expected);
    let expected = format!(
        "{expected_body}checksum\t{}\n",
        checksum_bytes(expected_body.as_bytes())
    );
    let memory = memory(limit);
    let documents = admit(input.to_vec(), &memory);
    let mut lease = memory.retained.reserve(0).unwrap();
    let result = (|| -> Result<()> {
        let entry = segment_memory::descriptor(7, &documents, &fields, &memory, &mut lease, &task)?;
        let mut segments = Vec::new();
        segment_memory::grow_slots(&mut segments, &mut lease)?;
        segments.push(entry);
        let descriptor = SearchSegmentDescriptor {
            target_documents: 128,
            document_count: documents.len(),
            segments,
        };
        let encoded =
            segment_memory::encode_descriptor(&descriptor, expected.len() as u64, &memory, &task)?;
        assert_eq!(encoded.as_ref(), expected.as_bytes());
        assert_eq!(encoded.len(), encoded.capacity());
        Ok(())
    })();
    assert_eq!(result.is_ok(), succeeds, "limit={limit}, result={result:?}");
    drop(documents);
    drop(lease);
    let snapshot = memory.ledger.snapshot();
    assert_eq!(snapshot.used_bytes, 0);
    assert_eq!(snapshot.account_count, 3);
    snapshot.peak_bytes
}

#[test]
fn descriptor_summary_and_bytes_match_reference_at_exact_and_short_root_budgets() {
    let input = fixture();
    let peak = descriptor_trial(&input, 1024 * 1024, true);
    assert_eq!(descriptor_trial(&input, peak, true), peak);
    descriptor_trial(&input, peak - 1, false);
}

#[test]
fn descriptor_list_scratch_is_admitted_before_parsing() {
    let mut input = document(0);
    input.metadata = BTreeMap::from([("labels".to_owned(), r#"["x"]"#.to_owned())]);
    let fields = BTreeSet::from(["labels".to_owned()]);
    let document_size = crate::build_memory::document_bytes(&input).unwrap();
    let retained = 2 * input.id.len() + 2048 + "labels".len();
    let memory = memory(document_size + retained);
    let documents = admit(vec![input], &memory);
    let mut lease = memory.retained.reserve(0).unwrap();
    segment_memory::evidence::take();
    assert!(segment_memory::descriptor(
        0,
        &documents,
        &fields,
        &memory,
        &mut lease,
        &RuntimeTaskContext::default()
    )
    .is_err());
    assert_eq!(segment_memory::evidence::take(), 0);
    drop(documents);
    drop(lease);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn directory_growth_admits_old_and_new_slots_together() {
    let memory = memory(95);
    let mut lease = memory.retained.reserve(0).unwrap();
    let mut entries = Vec::<u64>::new();
    segment_memory::grow_slots(&mut entries, &mut lease).unwrap();
    entries.extend([1, 2, 3, 4]);
    assert_eq!(lease.bytes(), 32);
    assert!(segment_memory::grow_slots(&mut entries, &mut lease).is_err());
    assert_eq!(entries, [1, 2, 3, 4]);
    assert_eq!(lease.bytes(), 32);
    drop(entries);
    drop(lease);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn returned_layout_keeps_its_lease_and_json_matches_legacy_envelope() {
    let root = test_dir("segment_layout_admission");
    fs::create_dir(&root).unwrap();
    let task = RuntimeTaskContext::default();
    let memory = memory(16 * 1024 * 1024);
    let fields = BTreeSet::new();
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let mut builder =
        SegmentArtifactBuilder::new_with_memory(&root, 9, &fields, &options, memory.clone())
            .unwrap();
    builder.push(0, document(0)).unwrap();
    let output = builder.finish(1).unwrap();
    let retained = memory.ledger.snapshot().used_bytes;
    assert_eq!(
        retained,
        output.layout.format.capacity()
            + output.layout.segments.capacity()
                * std::mem::size_of::<crate::out_of_core::SearchOutOfCoreSegmentLayout>()
    );
    assert!(retained > 0);
    let expected = output.layout.encode().unwrap();
    let encoded =
        build_io::json_envelope(&output.layout, expected.len() as u64, &memory, &task).unwrap();
    assert_eq!(encoded.as_ref(), expected);
    assert_eq!(
        memory.ledger.snapshot().used_bytes,
        retained + encoded.capacity()
    );
    drop(encoded);
    build_io::evidence::take();
    assert!(
        build_io::json_envelope(&output.layout, expected.len() as u64 - 1, &memory, &task).is_err()
    );
    assert_eq!(build_io::evidence::take(), 0);
    assert_eq!(memory.ledger.snapshot().used_bytes, retained);
    drop(output);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn failed_segment_flush_cannot_be_retried() {
    let root = test_dir("segment_failed_flush");
    fs::create_dir(&root).unwrap();
    let memory = memory(1024 * 1024);
    let fields = BTreeSet::new();
    let options = SearchOutOfCoreGenerationBuildOptions::default();
    let mut builder =
        SegmentArtifactBuilder::new_with_memory(&root, 1, &fields, &options, memory.clone())
            .unwrap();
    for index in 0..SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS {
        builder.push(index as u64, document(index)).unwrap();
    }
    // A full segment flushes before accepting the next document.
    assert!(builder
        .push(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS as u64, document(999))
        .unwrap_err()
        .to_string()
        .contains("memory"));
    let error = builder
        .push(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS as u64, document(999))
        .unwrap_err();
    assert!(error.to_string().contains("failed"), "{error}");
    assert!(builder
        .finish(SEARCH_FILTER_SEGMENT_TARGET_DOCUMENTS)
        .is_err());
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn publication_root_denial_preserves_every_old_artifact_and_releases_layout() {
    for allow_layout in [false, true] {
        let root = test_dir("publication_root_denial");
        let mut initial =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        initial.push(document(0)).unwrap();
        let first = initial.finish().unwrap();
        let before = published_files(&root);
        let manifest = before.get(OUT_OF_CORE_MANIFEST_FILE).unwrap();
        let decoded = crate::out_of_core::SearchOutOfCoreManifestBody::decode(manifest).unwrap();
        assert_eq!(decoded.encode().unwrap(), *manifest);

        let limit = 16 * 1024 * 1024;
        let task = RuntimeTaskContext::default()
            .with_memory_reservation(RuntimeMemoryReservation::new(limit as u64, 0));
        let mut writer =
            SearchOutOfCoreGenerationWriter::create_with_context(&root, Default::default(), task)
                .unwrap();
        writer.push(document(1)).unwrap();
        let ledger = writer.memory.ledger.clone();
        let sibling = std::rc::Rc::new(std::cell::RefCell::new(None));
        let held = sibling.clone();
        let error = writer
            .finish_with_artifacts(|input, source, generation| {
                let artifacts = input.build_artifacts(source, generation)?;
                let allowed = if allow_layout {
                    artifacts.segment.layout.encode()?.len()
                } else {
                    0
                };
                // Inject at the encoding boundary so path preparation cannot
                // mask either the layout or manifest admission failure.
                publication::before_encoding_for_test(move |memory| {
                    *held.borrow_mut() = Some(
                        memory
                            .retained
                            .reserve(limit - memory.ledger.snapshot().used_bytes - allowed)
                            .unwrap(),
                    );
                    build_io::evidence::take();
                });
                Ok(artifacts)
            })
            .unwrap_err();
        assert!(error.to_string().contains("query memory"), "{error}");
        assert_eq!(build_io::evidence::take(), usize::from(allow_layout));
        assert_eq!(
            ledger.snapshot().used_bytes,
            sibling.borrow().as_ref().unwrap().bytes()
        );
        drop(sibling);
        assert_eq!(ledger.snapshot().used_bytes, 0);
        assert_eq!(stage_directories(&root), 0);
        assert_eq!(published_files(&root), before);
        let reader = crate::out_of_core::SearchOutOfCoreReader::open(&root).unwrap();
        assert_eq!(reader.generation(), first.generation);
        assert_eq!(
            reader
                .hydrate_documents(&[document(0).id])
                .unwrap()
                .documents,
            vec![document(0)]
        );
        drop(reader);
        fs::remove_dir_all(root).unwrap();
    }
}
