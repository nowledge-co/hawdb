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

//! Private complete artifact construction; source/GC authority remains separate.
use super::*;
use crate::build_memory::{
    checked_add as add, checked_mul as mul,
    path::OwnedPath,
    read::{ReadAdmission, ReadBytes, ReaderMemory},
    SET_ENTRY_BYTES,
};
use hawdb_storage::file_descriptors::ProjectFileDescriptors;
use hawdb_storage::immutable_object::{ObjectKind, ObjectReference};
use std::mem::size_of;

pub(super) struct OpenContext<'a, 'execution> {
    pub(super) root: &'a Path,
    // Incoming host-owned buffers are moved, not cloned. Their actual capacity
    // is admitted before they can become retained reader ownership.
    pub(super) config: SearchOutOfCoreConfig,
    pub(super) analyzer_lexicon: SearchAnalyzerLexicon,
    pub(super) lexical_term_policy: SearchLexicalTermPolicy,
    pub(super) lexical_source_policy: SearchLexicalSourcePolicy,
    pub(super) admission: Option<&'a ReadAdmission<'execution>>,
    // Admission must borrow an existing project, never create an unaccounted
    // process-global descriptor/cache registry inside this constructor.
    pub(super) project: Option<&'a ProjectFileDescriptors>,
    pub(super) max_artifact_read_bytes: u64,
    // Exclusive construction-time borrow; the returned view never retains the
    // publication lease. Exact artifact/GC authority remains required separately.
    pub(super) publication: Option<&'a mut SearchProjectionPublishLease>,
    /// Fresh birth or completed native reopen establishes a held stationary
    /// target independently of its diagnostic pathname.
    pub(super) stationary: Option<StationaryRoot<'a>>,
    pub(super) expected_stationary_manifest: Option<ObjectReference>,
}

// Deliberately no Deref or public serving/refresh conversion for admitted roots.
// Until GC/cache ownership and fresh serving contexts are complete, internal
// construction evidence must not become an ungoverned public reader capability.
pub(super) struct OpenedRoot {
    reader: SearchOutOfCoreReader,
    stationary_immutable: bool,
    _native_custody: Option<super::root_registry::RootRef>,
    _root_memory: Option<ReaderMemory>,
    _manifest_memory: Option<ReaderMemory>,
    _collections_memory: Option<ReaderMemory>,
}
impl OpenedRoot {
    pub(super) fn shares_governor_with(&self, permit: &hawdb_qos::RuntimePermit) -> bool {
        self._root_memory
            .as_ref()
            .is_some_and(|memory| memory.shares_governor_with(permit))
            && self
                ._manifest_memory
                .as_ref()
                .is_some_and(|memory| memory.shares_governor_with(permit))
            && self
                ._collections_memory
                .as_ref()
                .is_some_and(|memory| memory.shares_governor_with(permit))
            && self.reader.segments.iter().all(|segment| {
                segment
                    ._layout_memory
                    .as_ref()
                    .is_some_and(|memory| memory.shares_governor_with(permit))
                    && segment
                        ._reader_memory
                        .as_ref()
                        .is_some_and(|memory| memory.shares_governor_with(permit))
            })
    }

    pub(super) fn probe_query(
        &self,
        query: super::CandidateQuery<'_>,
    ) -> super::SearchQueryResult<super::SearchRetainedOutput> {
        let budget = query.permit.request().memory_bytes;
        let metadata_bytes = crate::build_memory::ACCOUNT_METADATA_BYTES as u64;
        if budget < metadata_bytes {
            return Err(hawdb_core::ResourceBudgetError {
                resource: hawdb_core::ResourceBudgetKind::MemoryBytes,
                scope: hawdb_core::ResourceBudgetScope::Query,
                owner: "search query metadata".into(),
                memory_class: None,
                requested: metadata_bytes,
                used: 0,
                budget,
                budget_option: "RuntimePermit.request.memory_bytes",
                message: "search query metadata exceeds working admission".into(),
            }
            .into());
        }
        let memory = crate::build_memory::BuildMemory::new(query.task)?;
        let _metadata = memory
            .spool
            .reserve(crate::build_memory::ACCOUNT_METADATA_BYTES)?;
        let preflight = crate::lexical_projection::QueryContext {
            memory: &memory.spool,
            task: query.task,
            execution: Some(query.permit),
            io_scope: None,
        };
        let input = super::query_output::Input::new(query.options, preflight)?;
        let output = super::query_output::Frame::new(preflight, query.permit)?;
        // Private native scratch is created before borrowing the query's one
        // I/O wave. Declaration order releases that wave before cleanup can
        // request fresh I/O, including error and unwind paths.
        let stage = if !input.options.metadata_filters.is_empty()
            || !self.reader.visibility.is_empty()
        {
            let disk_bytes = self
                .reader
                .config
                .max_candidate_spill_bytes
                .get()
                .checked_add(super::CANDIDATE_FILE_HEADER.len() as u64)
                .ok_or_else(|| {
                    HawDBError::Execution("candidate spill reservation overflow".into())
                })?;
            let admission = ReadAdmission {
                memory: &memory,
                task: query.task,
                permit: crate::build_memory::read::ReadPermit::Governed(query.permit),
            };
            let mut stage = self
                ._native_custody
                .as_ref()
                .ok_or_else(|| {
                    HawDBError::Execution("candidate query lacks original native custody".into())
                })?
                .validation_stage(&self.reader.root, &admission)?;
            stage.reserve_disk(disk_bytes);
            Some(stage)
        } else {
            None
        };
        let scratch = stage.as_ref().and_then(|stage| stage.scratch_directory());
        let io = crate::build_memory::read::QueryIoScope::acquire(query.task)?;
        let context = crate::lexical_projection::QueryContext {
            memory: &memory.spool,
            task: query.task,
            execution: Some(query.permit),
            io_scope: Some(&io),
        };
        let result = self.reader.search_with_options_internal(
            query.text,
            query.embedding,
            query.mode,
            input.options,
            super::query_memory::Execution {
                base: super::SearchOutOfCoreExecutionContext {
                    access_control: None,
                    compressed_vector_search_mode: super::CompressedVectorSearchMode::Disabled,
                    vector_execution_options: super::VectorSearchExecutionOptions::bounded(
                        std::num::NonZeroUsize::new(
                            self.reader
                                .config
                                .max_vector_search_parallelism
                                .get()
                                .min(query.task.admitted_parallelism().get()),
                        )
                        .expect("nonzero vector parallelism"),
                        self.reader.config.max_vector_search_working_bytes.get(),
                        Some(query.task),
                    ),
                },
                query: Some(context),
                immutable_freshness: self.stationary_immutable,
                scratch: scratch.as_deref(),
                output: Some(&output),
            },
        )?;
        output.finish(result).map_err(Into::into)
    }

    pub(super) fn document_count(&self) -> usize {
        self.reader.document_count()
    }

    pub(super) fn source_epoch(&self) -> Option<u64> {
        self.reader.source_graph_commit_epoch()
    }

    pub(super) fn generation(&self) -> u64 {
        self.reader.generation()
    }

    pub(super) fn into_ordinary(self) -> Result<SearchOutOfCoreReader> {
        if self._root_memory.is_some() {
            return Err(HawDBError::Execution(
                "admitted root has no public serving/GC capability".into(),
            ));
        }
        Ok(self.reader)
    }
}

pub(super) fn open(context: OpenContext<'_, '_>) -> Result<OpenedRoot> {
    let OpenContext {
        root,
        config,
        analyzer_lexicon,
        lexical_term_policy,
        lexical_source_policy,
        admission,
        project,
        max_artifact_read_bytes,
        publication,
        stationary,
        expected_stationary_manifest,
    } = context;
    if stationary.is_some() != expected_stationary_manifest.is_some()
        || stationary.is_some() && (admission.is_none() || publication.is_none())
    {
        return Err(HawDBError::Execution(
            "born target requires captured publication and reader admission".into(),
        ));
    }
    if publication.is_some() && admission.is_none() {
        return Err(HawDBError::Execution(
            "publication custody transfer requires reader admission".into(),
        ));
    }
    let root_memory = admission
        .map(|admission| {
            admission.retained(root_bytes(
                root,
                &config,
                &analyzer_lexicon,
                admission.task,
            )?)
        })
        .transpose()?;
    let root = root.to_path_buf();
    let project_files = match admission {
        Some(admission) => {
            let project = project.ok_or_else(|| {
                HawDBError::Execution("admitted root requires an existing project authority".into())
            })?;
            if let Some(stationary) = stationary {
                let original = stationary.directory(admission.task)?;
                let publication = publication
                    .as_deref()
                    .expect("born target retains publication");
                if !publication.with_captured_directory(|directory| {
                    directory.same_directory(original)
                        && directory.shares_project_context(&project.io_context())
                }) {
                    return Err(HawDBError::StorageIntegrity(
                        "born reader does not match its captured target and descriptor domain"
                            .into(),
                    ));
                }
            } else {
                let _io = admission
                    .io()
                    .map_err(|error| HawDBError::Execution(error.to_string()))?;
                let canonical = OwnedPath::canonicalize(&root, admission.memory, admission.task)?;
                if !canonical.starts_with(project.root()) {
                    return Err(HawDBError::StorageIntegrity(
                        "reader root is outside its supplied project".into(),
                    ));
                }
            }
            // Containment is not a complete incarnation/binding proof.
            project.clone()
        }
        None => ProjectFileDescriptors::acquire_component(&root, false)?,
    };
    let mut initial_custody = match (admission, publication.is_none()) {
        (Some(admission), true) => Some(super::root_registry::RootRef::acquire(
            &root,
            &project_files.io_context(),
            admission,
        )?),
        _ => None,
    };
    let mut owned_publication = match (admission, publication.is_none()) {
        (Some(admission), true) => {
            let _io = admission
                .io()
                .map_err(|e| HawDBError::Execution(e.to_string()))?;
            Some(SearchProjectionPublishLease::acquire_for_capture(
                &root,
                admission.memory,
                admission.task,
            )?)
        }
        _ => None,
    };
    let publication = publication.or(owned_publication.as_mut());
    let capture = match (admission, publication) {
        (Some(admission), Some(publication)) => {
            {
                let _io = admission
                    .io()
                    .map_err(|e| HawDBError::Execution(e.to_string()))?;
                if stationary.is_none() {
                    publication.require_source_root(&root, &project_files.io_context())?;
                }
            }
            Some(match initial_custody.take() {
                Some(root) => root.begin_capture(publication, admission)?,
                None => publication.capture_source_root(&project_files.io_context(), admission)?,
            })
        }
        (None, None) => None,
        _ => unreachable!("admitted construction owns publication capture"),
    };
    let native_custody = capture.as_ref().map(|capture| capture.root().clone());
    if stationary.is_some() {
        let admission = admission.expect("born reader admission");
        let native = native_custody.as_ref().expect("born native custody");
        for marker in [FULL_REINDEX_MARKER, METADATA_REPAIR_MARKER] {
            native.require_absent_control_child(&project_files.io_context(), marker, admission)?;
        }
    }
    let head = match (admission, native_custody.as_ref()) {
        (Some(admission), Some(root)) => Some(root.open_head(
            &project_files.io_context(),
            OUT_OF_CORE_MANIFEST_FILE,
            admission,
        )?),
        _ => None,
    };
    // Pending capture is installed and the exact head inode is open. Decode,
    // validation and artifact hydration need not retain our publication lease.
    drop(owned_publication);
    let bytes = match admission {
        Some(admission) => admission.read_unbound_file(
            head.as_ref().expect("admitted capture opened its head"),
            MAX_OUT_OF_CORE_MANIFEST_BYTES.min(max_artifact_read_bytes),
        )?,
        None => ReadBytes::unaccounted(read_bounded_file(
            &root.join(OUT_OF_CORE_MANIFEST_FILE),
            MAX_OUT_OF_CORE_MANIFEST_BYTES,
        )?),
    };
    if let Some(expected) = expected_stationary_manifest
        && ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes) != expected
    {
        return Err(HawDBError::StorageIntegrity(
            "born reader head differs from sealed manifest reference".into(),
        ));
    }
    drop(head);
    let manifest_memory = admission
        .map(|admission| {
            admission.retained(SearchOutOfCoreManifestBody::decode_capacity(
                &bytes,
                admission.task,
            )?)
        })
        .transpose()?;
    let manifest = SearchOutOfCoreManifestBody::decode_with_context(
        &bytes,
        admission.map(|admission| admission.task),
    )?;
    drop(bytes);
    let closure = load_artifact_closure_with_context(
        ClosureLoadContext {
            cleanup_source: None,
            validation_source: None,
            capture: capture.as_ref(),
            root: &root,
            config: &config,
            analyzer_lexicon: &analyzer_lexicon,
            lexical_term_policy,
            lexical_source_policy,
            previous: None,
            admission,
            max_artifact_read_bytes,
        },
        manifest,
    )?;
    drop(capture);
    Ok(OpenedRoot {
        stationary_immutable: stationary.is_some(),
        _native_custody: native_custody,
        reader: SearchOutOfCoreReader {
            root,
            _project_files: project_files,
            config,
            analyzer_lexicon,
            manifest: closure.manifest,
            segments: closure.segments,
            visibility: closure.visibility,
            lexical_source_policy,
            lexical_term_policy,
            runtime_capabilities: crate::compiled_runtime_capabilities(),
        },
        _root_memory: root_memory,
        _manifest_memory: manifest_memory,
        _collections_memory: closure.collections_memory,
    })
}

fn root_bytes(
    root: &Path,
    config: &SearchOutOfCoreConfig,
    analyzer: &SearchAnalyzerLexicon,
    task: &hawdb_core::RuntimeTaskContext,
) -> Result<usize> {
    crate::build_control::checkpoint(task)?;
    let mut bytes = add(
        size_of::<OpenedRoot>(),
        add(
            root.as_os_str().as_encoded_bytes().len(),
            mul(config.spill_directory.capacity(), 3)?,
        )?,
    )?;
    bytes = add(
        bytes,
        mul(
            analyzer.alias_rules.capacity(),
            size_of::<crate::SearchAnalyzerAliasRule>(),
        )?,
    )?;
    for rule in &analyzer.alias_rules {
        crate::build_control::checkpoint(task)?;
        for words in [&rule.inputs, &rule.aliases] {
            bytes = add(bytes, mul(words.capacity(), size_of::<String>())?)?;
            for word in words {
                crate::build_control::checkpoint(task)?;
                bytes = add(bytes, word.capacity())?;
            }
        }
    }
    bytes = add(bytes, mul(analyzer.stopwords.len(), SET_ENTRY_BYTES)?)?;
    for word in &analyzer.stopwords {
        crate::build_control::checkpoint(task)?;
        bytes = add(bytes, word.capacity())?;
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
