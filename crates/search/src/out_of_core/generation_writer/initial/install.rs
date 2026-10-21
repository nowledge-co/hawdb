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

//! Preflight the complete closure, install new dependencies, then select it.

use super::*;
use crate::out_of_core::{SearchOutOfCoreManifestBody, SearchOutOfCoreSegmentManifest};

pub(super) struct InstalledInitial {
    head: json::EncodedManifest,
    published_bytes: u64,
    document_count: usize,
    documents_digest: u64,
}

impl InstalledInitial {
    fn publish(
        self,
        writer: &SearchOutOfCoreGenerationWriter,
        active: Option<u64>,
        report: &mut SearchOutOfCoreGenerationBuildReport,
    ) -> Result<u64> {
        check_active(writer, active)?;
        let io = io::GenerationIo::new(&writer.memory, &writer.task_context);
        io.write(
            &io.path(&writer.root, Path::new(OUT_OF_CORE_MANIFEST_FILE))?,
            &self.head.bytes,
        )?;
        report.generation_bytes = self.published_bytes;
        report.document_count = self.document_count;
        report.documents_digest = self.documents_digest;
        Ok(self.head.bytes.len() as u64)
    }
}

pub(super) fn publish(
    writer: &SearchOutOfCoreGenerationWriter,
    private: &Path,
    active: Option<u64>,
    report: &mut SearchOutOfCoreGenerationBuildReport,
) -> Result<u64> {
    install(writer, private, active, report)?.publish(writer, active, report)
}

pub(super) fn install(
    writer: &SearchOutOfCoreGenerationWriter,
    private: &Path,
    active: Option<u64>,
    report: &SearchOutOfCoreGenerationBuildReport,
) -> Result<InstalledInitial> {
    let memory = &writer.memory;
    let task = &writer.task_context;
    let io = io::GenerationIo::new(memory, task);
    let head = io.read(
        &io.path(private, Path::new(OUT_OF_CORE_MANIFEST_FILE))?,
        MAX_OUT_OF_CORE_MANIFEST_BYTES,
    )?;
    let decode = memory
        .spool
        .reserve(SearchOutOfCoreManifestBody::decode_capacity(
            &head.bytes,
            task,
        )?)?;
    let envelope: SearchOutOfCoreManifestEnvelope =
        serde_json::from_slice(&head.bytes).map_err(|error| {
            HawDBError::Storage(format!("invalid staged initial manifest: {error}"))
        })?;
    if json::checksum_with_context(&envelope.body, Some(task))? != envelope.checksum {
        return Err(HawDBError::Storage(
            "staged initial manifest checksum mismatch".into(),
        ));
    }
    let manifest = &envelope.body;
    manifest.validate_names()?;
    if manifest.generation != report.generation
        || manifest.document_count != writer.document_count
        || manifest.documents_digest != writer.documents_digest.finish()
        || !manifest.mutation_runs.is_empty()
        || manifest.segments.len() != report.published_content_segments
    {
        return Err(HawDBError::Storage(
            "staged initial manifest identity mismatch".into(),
        ));
    }
    let update = update::prepare(writer, envelope.body, decode)?;
    let manifest = update.manifest();
    manifest.validate_names()?;
    let final_head = json::encode_with_context(
        manifest,
        MAX_OUT_OF_CORE_MANIFEST_BYTES,
        memory,
        task,
        "partitioned search manifest",
    )?;
    let mutation_bytes = update
        .mutation
        .as_ref()
        .map_or(0, |(_, encoded)| encoded.bytes.len() as u64);
    let published_bytes = report
        .generation_bytes
        .checked_add(mutation_bytes)
        .ok_or_else(|| HawDBError::Storage("partitioned published size overflows".into()))?;
    let total = published_bytes
        .checked_add(final_head.bytes.len() as u64)
        .ok_or_else(|| HawDBError::Storage("initial published size overflows".into()))?;
    if total > writer.options.max_generation_bytes.get() {
        return Err(HawDBError::Storage(
            "complete initial generation exceeds publication admission".into(),
        ));
    }
    let mut complete_content_bytes = 0_u64;
    for (offset, segment) in manifest.segments.iter().enumerate() {
        checkpoint(task)?;
        if offset >= update.old_segments {
            let bytes = dependencies(writer, private, segment, |source, _, object, checksum| {
                io.verify_artifact(
                    source,
                    object.byte_length,
                    object,
                    checksum,
                    "staged initial dependency",
                )
            })?;
            if segment.document_count > writer.options.max_content_documents.get()
                || (segment.document_count > 1
                    && bytes > writer.options.max_content_artifact_bytes.get())
            {
                return Err(HawDBError::Storage(
                    "initial content owner exceeds admission".into(),
                ));
            }
            complete_content_bytes = complete_content_bytes
                .checked_add(bytes)
                .ok_or_else(|| HawDBError::Storage("initial content size overflows".into()))?;
        }
    }
    if complete_content_bytes != report.generation_bytes {
        return Err(HawDBError::Storage(
            "initial dependency lengths differ from the complete publication report".into(),
        ));
    }
    check_active(writer, active)?;
    // All ownership, aggregate and manifest admission checks precede real-root
    // dependency links. Only the new owners/run are installed; old bodies are
    // neither copied nor scanned during incremental publication.
    for segment in &manifest.segments[update.old_segments..] {
        checkpoint(task)?;
        dependencies(
            writer,
            private,
            segment,
            |source, target, object, checksum| {
                io.link(source, target)?;
                io.verify_artifact(
                    target,
                    object.byte_length,
                    object,
                    checksum,
                    "initial dependency",
                )
            },
        )?;
    }
    if let Some((name, encoded)) = &update.mutation {
        io.write(&io.path(&writer.root, name.as_ref())?, &encoded.bytes)?;
        io.verify_object(
            &io.path(&writer.root, name.as_ref())?,
            crate::out_of_core::ArtifactObject::for_bytes(&encoded.bytes),
        )?;
    }
    Ok(InstalledInitial {
        head: final_head,
        published_bytes,
        document_count: manifest.document_count,
        documents_digest: manifest.documents_digest,
    })
}

fn check_active(writer: &SearchOutOfCoreGenerationWriter, expected: Option<u64>) -> Result<()> {
    let actual = discovery::active(&writer.root, &writer.memory, &writer.task_context)?;
    if actual != expected {
        return Err(HawDBError::TransactionConflict {
            read_epoch: expected.unwrap_or_default(),
            committed_epoch: actual.unwrap_or_default(),
            key: "initial search publication changed".into(),
        });
    }
    Ok(())
}

fn dependencies(
    writer: &SearchOutOfCoreGenerationWriter,
    private: &Path,
    segment: &SearchOutOfCoreSegmentManifest,
    mut visit: impl FnMut(&Path, &Path, crate::out_of_core::ArtifactObject, Option<u64>) -> Result<()>,
) -> Result<u64> {
    let io = io::GenerationIo::new(&writer.memory, &writer.task_context);
    let lexical = artifact_name::Name::generated(
        "search_lexical.",
        segment.objects.lexical_generation,
        &writer.memory,
        &writer.task_context,
    )?;
    let lexical_len = segment.objects.lexical_payload.byte_length;
    let mut content_bytes = 0_u64;
    for (file, length, checksum) in [
        (
            segment.descriptor_file.as_str(),
            segment.descriptor_len,
            Some(segment.descriptor_checksum),
        ),
        (segment.payload_file.as_str(), segment.payload_len, None),
        (
            segment.metadata_payload_file.as_str(),
            segment.metadata_payload_len,
            None,
        ),
        (
            segment.vector_payload_file.as_str(),
            segment.vector_payload_len,
            None,
        ),
        (
            segment.layout_file.as_str(),
            segment.layout_len,
            Some(segment.layout_checksum),
        ),
        (
            segment.lexical_manifest_file.as_str(),
            segment.lexical_manifest_len,
            Some(segment.lexical_manifest_checksum),
        ),
        (lexical.as_str(), lexical_len, None),
    ]
    .into_iter()
    .chain(
        segment
            .rabitq_artifact_file
            .as_deref()
            .zip(segment.rabitq_artifact_len)
            .map(|(file, length)| (file, length, segment.rabitq_artifact_checksum)),
    ) {
        content_bytes = content_bytes
            .checked_add(length)
            .ok_or_else(|| HawDBError::Storage("initial content size overflows".into()))?;
        let object = segment.object_for(file).ok_or_else(|| {
            HawDBError::StorageIntegrity("missing initial dependency reference".into())
        })?;
        if object.byte_length != length {
            return Err(HawDBError::StorageIntegrity(
                "initial dependency reference length mismatch".into(),
            ));
        }
        visit(
            &io.path(private, Path::new(file))?,
            &io.path(&writer.root, Path::new(file))?,
            object,
            checksum,
        )?;
    }
    Ok(content_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::out_of_core::generation_writer::tests::{document, test_dir};
    use crate::SearchOutOfCoreReader;
    use hawdb_core::RuntimeCancellationToken;

    #[test]
    fn installed_partitions_preserve_the_old_view_and_refuse_cancelled_or_stale_selection() {
        for outcome in ["select", "cancel", "stale"] {
            let root = test_dir("installed_partition_selector");
            let mut baseline =
                SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
            baseline.push(document(99)).unwrap();
            let baseline = baseline.finish().unwrap();
            let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
            let old_reader = SearchOutOfCoreReader::open(&root).unwrap();
            let cancellation = RuntimeCancellationToken::new();
            let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
                &root,
                SearchOutOfCoreGenerationBuildOptions {
                    max_content_documents: NonZeroUsize::new(1).unwrap(),
                    source_graph_commit_epoch: Some(7),
                    ..Default::default()
                },
                RuntimeTaskContext::without_deadline(cancellation.clone()),
            )
            .unwrap();
            writer.push(document(0)).unwrap();
            writer.push(document(1)).unwrap();
            // Exercise the internal install boundary after closing the captured
            // spool, without adding a public bypass for registered consumers.
            writer.seal_input_spool().unwrap();
            let mut report = super::super::prepare(&writer, baseline.generation + 1).unwrap();
            let mut lease =
                Some(crate::out_of_core::SearchProjectionPublishLease::acquire(&root).unwrap());
            let installed = install(
                &writer,
                &writer.stage.path,
                Some(baseline.generation),
                &report,
            )
            .unwrap();
            assert_eq!(
                fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
                before
            );
            assert_eq!(
                SearchOutOfCoreReader::open(&root).unwrap().document_count(),
                1
            );
            assert_eq!(
                old_reader
                    .hydrate_documents(&[document(99).id])
                    .unwrap()
                    .documents,
                vec![document(99)]
            );
            if outcome == "stale" {
                drop(lease.take());
                let mut winner =
                    SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
                winner.push(document(98)).unwrap();
                winner.finish().unwrap();
                let winner_head = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
                lease =
                    Some(crate::out_of_core::SearchProjectionPublishLease::acquire(&root).unwrap());
                assert!(matches!(
                    installed.publish(&writer, Some(baseline.generation), &mut report),
                    Err(HawDBError::TransactionConflict { .. })
                ));
                assert_eq!(
                    fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
                    winner_head
                );
                assert_eq!(
                    SearchOutOfCoreReader::open(&root)
                        .unwrap()
                        .hydrate_documents(&[document(98).id])
                        .unwrap()
                        .documents,
                    vec![document(98)]
                );
            } else if outcome == "cancel" {
                cancellation.cancel();
                assert!(installed
                    .publish(&writer, Some(baseline.generation), &mut report)
                    .is_err());
                assert_eq!(
                    fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
                    before
                );
                assert_eq!(
                    SearchOutOfCoreReader::open(&root).unwrap().document_count(),
                    1
                );
            } else {
                let _bytes = installed
                    .publish(&writer, Some(baseline.generation), &mut report)
                    .unwrap();
                let reader = SearchOutOfCoreReader::open(&root).unwrap();
                assert_eq!(reader.source_graph_commit_epoch(), Some(7));
                assert_eq!(
                    reader
                        .hydrate_documents(&[document(0).id, document(1).id])
                        .unwrap()
                        .documents,
                    vec![document(0), document(1)]
                );
            }
            drop(old_reader);
            drop(lease);
            drop(writer);
            fs::remove_dir_all(root).unwrap();
        }
    }
}
