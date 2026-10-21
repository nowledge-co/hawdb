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
use crate::build_memory::{read::tests::governor, BuildMemory};
use crate::test_allocation as allocation;
use hawdb_qos::RuntimeWorkRequest;

struct Fixture {
    root: PathBuf,
    reader: SearchOutOfCoreReader,
    // Last owner: the shared native cache and its platform lock backing must
    // close with this fixture, rather than a thread-local test-dir registry.
    _project: ProjectFileDescriptors,
}
impl Fixture {
    fn new() -> Self {
        Self::new_with_chinese(false)
    }
    fn new_with_chinese(chinese: bool) -> Self {
        Self::new_with_content(chinese, None)
    }
    fn new_with_content(chinese: bool, content: Option<&str>) -> Self {
        let root = crate::test_temp_dir().join(format!(
            "hawdb-private-admitted-root-{}",
            hawdb_core::generate_uuidv7().unwrap()
        ));
        let project = ProjectFileDescriptors::acquire(
            &root,
            hawdb_storage::file_descriptors::DEFAULT_MAX_OPEN_FILES,
        )
        .unwrap();
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
        for id in ["a", "b"] {
            writer
                .push(SearchDocument {
                    id: format!("memory:{id}"),
                    title: if chinese && id == "a" {
                        "中国 Graph".into()
                    } else {
                        id.into()
                    },
                    content: content.unwrap_or("graph source text").into(),
                    embedding: None,
                    metadata: BTreeMap::new(),
                })
                .unwrap();
        }
        writer.finish().unwrap();
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        let update = SearchOutOfCoreGenerationWriter::prepare_delta(
            &reader,
            crate::SearchProjectionDelta {
                deletes: vec!["memory:a".into()],
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
        update.finish().unwrap();
        let reader = SearchOutOfCoreReader::open(&root).unwrap();
        assert!(
            !reader.manifest.mutation_runs.is_empty(),
            "exercise complete retraction validation"
        );
        Self {
            root,
            reader,
            _project: project,
        }
    }
    fn context<'a, 'execution>(
        &'a self,
        admission: &'a ReadAdmission<'execution>,
    ) -> OpenContext<'a, 'execution> {
        OpenContext {
            root: &self.root,
            config: self.reader.config.clone(),
            analyzer_lexicon: self.reader.analyzer_lexicon.clone(),
            lexical_term_policy: self.reader.lexical_term_policy,
            lexical_source_policy: self.reader.lexical_source_policy,
            admission: Some(admission),
            project: Some(&self.reader._project_files),
            max_artifact_read_bytes: 32 << 20,
            publication: None,
            stationary: None,
            expected_stationary_manifest: None,
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[test]
fn private_root_constructs_full_mutation_closure_and_releases_producer_execution() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
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
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let context = fixture.context(&admission);
    let input_bytes = root_bytes(
        context.root,
        &context.config,
        &context.analyzer_lexicon,
        &task,
    )
    .unwrap();
    let (opened, peak) = allocation::measure(|| open(context).unwrap());
    assert!(
        peak <= memory.ledger.snapshot().peak_bytes,
        "actual {peak} exceeds admitted peak"
    );
    assert!(memory.ledger.snapshot().used_bytes >= input_bytes);
    assert_eq!(opened.reader.manifest, fixture.reader.manifest);
    assert!(!opened.reader.visibility.is_visible(0, "memory:a"));
    assert!(opened.reader.visibility.is_visible(0, "memory:b"));
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    let baseline = allocation::live();
    let (child, clone_peak) = allocation::measure(|| opened.reader.segments[0].clone());
    assert_eq!(clone_peak, baseline);
    drop(memory);
    drop(task);
    drop(permit);
    let snapshot = governor.snapshot();
    assert_eq!(snapshot.active_cpu_slots, 0);
    assert_eq!(snapshot.active_background_tasks, 0);
    assert_eq!(snapshot.active_background_io_slots, 0);
    assert!(snapshot.admitted_memory_bytes >= allocation::live() as u64);
    drop(opened);
    assert!(governor.snapshot().admitted_memory_bytes >= allocation::live() as u64);
    assert_eq!(
        child.lexical_projection.generation(),
        fixture.reader.segments[0].lexical_projection.generation()
    );
    drop(child);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(fixture);
    // Native condvar backing is owned by the governor on macOS, so include
    // that owner when auditing complete measured-heap reclamation.
    drop(governor);
    assert_eq!(allocation::live(), 0);
}

#[test]
fn admitted_root_cannot_convert_into_an_unguarded_public_reader() {
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
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let error = open(fixture.context(&admission))
        .unwrap()
        .into_ordinary()
        .unwrap_err();
    assert!(matches!(error, HawDBError::Execution(_)));
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn root_admission_counts_incoming_analyzer_capacity_before_any_path_lookup() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture::new();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let mut input = String::with_capacity(4 << 20);
    input.push('a');
    let analyzer = SearchAnalyzerLexicon::empty().with_alias_rule([input], ["alias"]);
    let missing = fixture.root.join("must-not-look-up");
    let mut context = fixture.context(&admission);
    context.root = &missing;
    context.analyzer_lexicon = analyzer;
    let (error, peak) = allocation::measure(|| {
        open(context)
            .err()
            .expect("oversized incoming ownership must refuse")
    });
    assert!(
        matches!(&error, HawDBError::ResourceBudgetExceeded(cause)
            if cause.resource == hawdb_core::ResourceBudgetKind::MemoryBytes
                && cause.scope == hawdb_core::ResourceBudgetScope::Account
                && cause.owner == "search build state"
                && cause.budget == (2 << 20)
                && cause.requested > cause.budget.saturating_sub(cause.used)),
        "{error:?}"
    );
    assert!(
        peak < 16 << 10,
        "expanded/opened before metadata admission: {peak}"
    );
    drop(error);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    drop(fixture);
    assert_eq!(allocation::live(), 0);
}

#[test]
fn root_preflight_refuses_checksum_valid_compact_runs_before_owned_expansion() {
    let _serial = allocation::serial();
    assert_eq!(allocation::live(), 0);
    let fixture = Fixture::new();
    let mut body = fixture.reader.manifest.clone();
    // V4's mandatory fixed digest record makes each compact row wider. Keep
    // the malicious frame below the unchanged encoded ceiling while still
    // exceeding the unchanged 2MiB construction grant on owned expansion.
    body.generation = 5_001;
    body.mutation_runs = (1..=5_000)
        .map(|generation| SearchOutOfCoreMutationRunManifest {
            object: crate::out_of_core::ArtifactObject::for_bytes(&[0]),
            generation,
            file: mutation_run::artifact_file(generation),
            len: 1,
            checksum: 0,
            entry_count: 1,
            analyzer_digest: lexical_analyzer_digest(&fixture.reader.analyzer_lexicon),
        })
        .collect();
    let checksum = checksum_bytes(&serde_json::to_vec(&body).unwrap());
    let mut raw = serde_json::to_value(&body).unwrap();
    raw["mutation_runs"] = serde_json::Value::Array(
        body.mutation_runs
            .iter()
            .map(|run| {
                serde_json::json!([
                    run.generation,
                    run.object,
                    run.file,
                    run.len,
                    run.checksum,
                    run.entry_count,
                    run.analyzer_digest
                ])
            })
            .collect(),
    );
    let bytes =
        serde_json::to_vec(&serde_json::json!({"body": raw, "checksum": checksum})).unwrap();
    assert!(
        bytes.len() as u64 <= MAX_OUT_OF_CORE_MANIFEST_BYTES,
        "fixture must reach decode: {}",
        bytes.len()
    );
    let (decoded, peak) = allocation::measure(|| {
        let copied = bytes.clone();
        let decoded = SearchOutOfCoreManifestBody::decode(&copied).unwrap();
        (copied, decoded)
    });
    assert!(
        peak > 2 << 20,
        "fixture must exceed query budget in legacy decode: {peak}"
    );
    drop(decoded);
    assert_eq!(allocation::live(), 0);
    std::fs::write(fixture.root.join(OUT_OF_CORE_MANIFEST_FILE), &bytes).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(2 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let context = fixture.context(&admission);
    let (error, peak) = allocation::measure(|| {
        open(context)
            .err()
            .expect("must refuse before expanding declared runs")
    });
    assert!(
        matches!(&error, HawDBError::ResourceBudgetExceeded(cause)
            if cause.resource == hawdb_core::ResourceBudgetKind::MemoryBytes
                && cause.scope == hawdb_core::ResourceBudgetScope::Account
                && cause.owner == "search build state"
                && cause.budget == (2 << 20)
                && cause.requested > cause.budget.saturating_sub(cause.used)),
        "{error:?}"
    );
    assert!(
        peak < bytes.len() + (64 << 10),
        "owned run expansion before admission: {peak}"
    );
    drop(error);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    drop(governor);
    drop(fixture);
    assert_eq!(allocation::live(), 0);
}

#[test]
fn private_root_requires_existing_matching_project_and_refunds_on_mismatch() {
    let fixture = Fixture::new();
    let other_root = super::super::tests::test_dir("other-root-authority");
    let other = ProjectFileDescriptors::acquire_component(&other_root, false).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    for missing in [true, false] {
        let mut context = fixture.context(&admission);
        context.project = if missing { None } else { Some(&other) };
        let error = open(context)
            .err()
            .expect("must not invent or borrow unrelated root authority");
        if missing {
            assert!(
                matches!(&error, HawDBError::Execution(message) if message.contains("existing project authority")),
                "{error:?}"
            );
        } else {
            assert!(
                matches!(&error, HawDBError::StorageIntegrity(message) if message.contains("supplied project")),
                "{error:?}"
            );
        }
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
    }
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
    std::fs::remove_dir_all(other_root).unwrap();
}

#[test]
fn chunked_analyzer_identity_matches_legacy_bytes_and_checks_long_inputs() {
    let analyzer = SearchAnalyzerLexicon::empty()
        .with_alias_rule(["中国Graph".repeat(8192)], ["alias".repeat(2048)])
        .with_stopwords(["停止".repeat(2048)]);
    let task = hawdb_core::RuntimeTaskContext::default();
    let (digest, checks) = crate::build_control::observation::measure(|| {
        crate::lexical_projection::analyzer_digest_with_context(&analyzer, &task).unwrap()
    });
    assert_eq!(digest, lexical_analyzer_digest(&analyzer));
    assert!(checks >= (analyzer.alias_rules[0].inputs[0].len() / 8192));
    task.cancellation().cancel();
    assert!(matches!(
        crate::lexical_projection::analyzer_digest_with_context(&analyzer, &task),
        Err(HawDBError::Execution(_))
    ));
}

#[test]
fn private_root_joins_cjk_target_validation_before_releasing_borrowed_execution() {
    let fixture = Fixture::new_with_chinese(true);
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let observe = crate::analyzer_workspace::observation::Observe::new();
    let opened = open(fixture.context(&admission)).unwrap();
    observe.assert_joined(1);
    assert_eq!(opened.reader.manifest, fixture.reader.manifest);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_cpu_slots, 0);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    drop(opened);
    observe.assert_released();
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn complete_private_root_can_take_publisher_custody_without_retaining_the_lease() {
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
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let mut publication =
        SearchProjectionPublishLease::acquire_for_consumer(&fixture.root).unwrap();
    let mut context = fixture.context(&admission);
    context.publication = Some(&mut publication);
    let opened = open(context).unwrap();
    assert_eq!(opened.reader.manifest, fixture.reader.manifest);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    drop(publication);
    // Publication exclusion is released while the immutable root remains live.
    let successor = SearchProjectionPublishLease::acquire_for_consumer(&fixture.root).unwrap();
    drop(successor);
    drop(memory);
    drop(task);
    drop(permit);
    assert_eq!(governor.snapshot().active_background_tasks, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert!(governor.snapshot().admitted_memory_bytes > 0);
    drop(opened);
    assert_eq!(governor.snapshot().admitted_memory_bytes, 0);
}

#[test]
fn publisher_from_another_directory_in_the_same_project_cannot_supply_reader_custody() {
    let fixture = Fixture::new();
    let foreign = fixture.root.join("foreign-root");
    std::fs::create_dir(&foreign).unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let mut publication =
        SearchProjectionPublishLease::acquire_for_consumer(&fixture.root).unwrap();
    let descriptors = fixture.reader._project_files.metrics().open;
    let mut context = fixture.context(&admission);
    context.root = &foreign;
    context.publication = Some(&mut publication);
    let error = open(context)
        .err()
        .expect("another directory must refuse before head lookup");
    assert!(
        matches!(error, HawDBError::StorageIntegrity(message) if message == "publication root does not match reader root")
    );
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
    assert_eq!(fixture.reader._project_files.metrics().open, descriptors);
    // The failed construction leaves original publisher custody available.
    let mut retry = fixture.context(&admission);
    retry.publication = Some(&mut publication);
    drop(open(retry).unwrap());
    drop(publication);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

fn replace_twice(fixture: &Fixture) {
    for id in ["replacement-c", "replacement-d"] {
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&fixture.root, Default::default()).unwrap();
        writer
            .push(SearchDocument {
                id: id.into(),
                title: id.into(),
                content: "unrelated replacement".into(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        writer.finish().unwrap();
    }
}

// Keep this probe in the unpublished source-GC worktree until production
// discovery, candidate enumeration and unlink all use the captured authority.
fn cleanup_uses_one_captured_namespace(retry: bool) {
    cleanup_uses_one_captured_namespace_with_scratch_probe(retry, false);
}

fn cleanup_uses_one_captured_namespace_with_scratch_probe(retry: bool, check_scratch: bool) {
    use crate::generation_cleanup::{once::PreparedCleanup, SearchProjectionGenerations};
    struct Replacement {
        named: PathBuf,
        captured: PathBuf,
    }
    impl Drop for Replacement {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.named).unwrap();
            std::fs::rename(&self.captured, &self.named).unwrap();
        }
    }

    let fixture = Fixture::new();
    let task = hawdb_core::RuntimeTaskContext::default();
    let memory = BuildMemory::new(&task).unwrap();
    let active_name = fixture.reader.manifest.segments[0].payload_file.clone();
    let obsolete_name = "search_projection_segment_payloads.0.hawdb";
    std::fs::write(
        fixture.root.join(obsolete_name),
        b"captured obsolete source",
    )
    .unwrap();
    let publication = SearchProjectionPublishLease::acquire_for_cleanup(&fixture.root).unwrap();
    let run_cleanup = |root: &Path| {
        if retry {
            crate::generation_cleanup::SearchProjectionCleanupState::default()
                .run_under_publication(
                    root,
                    Default::default(),
                    &publication,
                    &fixture.reader.analyzer_lexicon,
                    None,
                );
        } else {
            PreparedCleanup::prepare(root, &memory, &task)
                .unwrap()
                .run_under_publication(
                    root,
                    Default::default(),
                    &task,
                    &publication,
                    &fixture.reader.analyzer_lexicon,
                    None,
                );
        }
    };
    let captured = fixture.root.with_extension("captured-gc-source");
    let rename = std::fs::rename(&fixture.root, &captured);
    if cfg!(windows)
        && rename
            .as_ref()
            .is_err_and(|error| error.kind() == std::io::ErrorKind::PermissionDenied)
    {
        // An open in-directory publication lock can prevent native rename on
        // Windows. The original namespace must remain owned and safe to clean.
        assert!(fixture.root.is_dir());
        assert!(!captured.exists());
        std::fs::create_dir(&captured).unwrap();
        let foreign_bytes = b"unrelated namespace must remain untouched";
        let foreign_file = captured.join(obsolete_name);
        std::fs::write(&foreign_file, foreign_bytes).unwrap();
        let foreign_directory_before = std::fs::metadata(&captured).unwrap().modified().unwrap();
        run_cleanup(&fixture.root);
        assert!(fixture.root.join(&active_name).is_file());
        assert!(!fixture.root.join(obsolete_name).exists());
        assert_eq!(std::fs::read(&foreign_file).unwrap(), foreign_bytes);
        if check_scratch {
            assert_eq!(
                std::fs::metadata(&captured).unwrap().modified().unwrap(),
                foreign_directory_before,
                "validation scratch must not touch an unrelated namespace"
            );
        }
        drop(publication);
        std::fs::remove_dir_all(captured).unwrap();
        return;
    }
    rename.unwrap();
    let replacement = Replacement {
        named: fixture.root.clone(),
        captured,
    };
    for number in 1..=6 {
        let mut writer =
            SearchOutOfCoreGenerationWriter::create(&fixture.root, Default::default()).unwrap();
        writer
            .push(SearchDocument {
                id: format!("foreign:{number}"),
                title: "foreign namespace".into(),
                content: "independent published closure".into(),
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        writer.finish().unwrap();
    }
    let foreign_bytes = b"foreign obsolete evidence must remain untouched";
    std::fs::write(fixture.root.join(obsolete_name), foreign_bytes).unwrap();
    let overlapping_foreign_bytes = b"foreign artifact sharing the captured active name";
    std::fs::write(fixture.root.join(&active_name), overlapping_foreign_bytes).unwrap();
    // This is deliberately the current production discovery call. Merely
    // changing the final unlink to be relative will still use this foreign
    // head's retention set and delete the captured active generation.
    let current = published_artifact_generations(&fixture.root, &fixture.reader.analyzer_lexicon)
        .unwrap()
        .unwrap();
    assert_eq!(current.active_generation, 6);
    let _foreign_generations = SearchProjectionGenerations {
        lexical: current.lexical_generations.last().copied(),
        out_of_core: Some(current.active_generation),
        rabitq: current.rabitq_generations.last().copied(),
        rabitq_remove_all: current.rabitq_generations.is_empty(),
        retained_lexical: current.lexical_generations,
        retained_out_of_core: current.out_of_core_generations,
        retained_rabitq: current.rabitq_generations,
        out_of_core_discovery_failed: false,
        rabitq_discovery_failed: false,
    };
    let foreign_directory_before = std::fs::metadata(&fixture.root)
        .unwrap()
        .modified()
        .unwrap();
    run_cleanup(&fixture.root);
    let foreign_directory_after = std::fs::metadata(&fixture.root)
        .unwrap()
        .modified()
        .unwrap();
    let active_survives = replacement.captured.join(&active_name).is_file();
    let captured_obsolete_removed = !replacement.captured.join(obsolete_name).exists();
    let foreign_untouched = std::fs::read(fixture.root.join(obsolete_name))
        .is_ok_and(|bytes| bytes == foreign_bytes)
        && std::fs::read(fixture.root.join(&active_name))
            .is_ok_and(|bytes| bytes == overlapping_foreign_bytes);
    drop(publication);
    drop(replacement);
    assert!(
        active_survives,
        "foreign discovery must not retire a captured active source"
    );
    assert!(
        foreign_untouched,
        "cleanup must not unlink the replacement directory"
    );
    assert!(
        captured_obsolete_removed,
        "cleanup must reclaim the captured obsolete source"
    );
    if check_scratch {
        assert_eq!(
            foreign_directory_before, foreign_directory_after,
            "validation scratch must not create/remove entries in the replacement namespace"
        );
    }
}

#[test]
fn immediate_cleanup_keeps_discovery_and_unlink_on_the_captured_namespace() {
    cleanup_uses_one_captured_namespace(false);
}

#[test]
fn queued_cleanup_keeps_discovery_and_unlink_on_the_captured_namespace() {
    cleanup_uses_one_captured_namespace(true);
}

#[test]
fn validation_scratch_keeps_all_operations_off_the_replacement_namespace() {
    cleanup_uses_one_captured_namespace_with_scratch_probe(false, true);
}

fn sweep_current(
    fixture: &Fixture,
    retry: bool,
    state: &mut crate::generation_cleanup::SearchProjectionCleanupState,
    memory: &BuildMemory,
    task: &hawdb_core::RuntimeTaskContext,
    admission: &ReadAdmission<'_>,
) {
    use crate::generation_cleanup::once::PreparedCleanup;
    let mut publication = SearchProjectionPublishLease::acquire_for_cleanup(&fixture.root).unwrap();
    publication.admit_cleanup_resources(admission).unwrap();
    if retry {
        state.run_under_publication(
            &fixture.root,
            Default::default(),
            &publication,
            &fixture.reader.analyzer_lexicon,
            Some(admission),
        );
    } else {
        PreparedCleanup::prepare(&fixture.root, memory, task)
            .unwrap()
            .run_under_publication(
                &fixture.root,
                Default::default(),
                task,
                &publication,
                &fixture.reader.analyzer_lexicon,
                Some(admission),
            );
    }
}

#[test]
fn admitted_segment_pin_survives_parent_and_real_generation_cleanup() {
    for retry in [false, true] {
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
            permit: crate::build_memory::read::ReadPermit::Governed(&permit),
        };
        let opened = open(fixture.context(&admission)).unwrap();
        let child = opened.reader.segments[0].clone();
        let old = &opened.reader.manifest.segments[0];
        let payload = fixture.root.join(&old.payload_file);
        let lexical = fixture.root.join(&old.lexical_manifest_file);
        let mutation = fixture
            .root
            .join(&opened.reader.manifest.mutation_runs[0].file);
        let expected = std::fs::read(&payload).unwrap();
        drop(opened);
        replace_twice(&fixture);
        let mut state = crate::generation_cleanup::SearchProjectionCleanupState::default();
        sweep_current(&fixture, retry, &mut state, &memory, &task, &admission);
        assert!(payload.exists() && lexical.exists());
        assert!(
            !mutation.exists(),
            "independent segment must not pin parent mutation runs"
        );
        let bytes = admission
            .read_unbound_file(&child.payload, child.payload_length)
            .unwrap();
        assert_eq!(&*bytes, expected.as_slice());
        drop(bytes);
        drop(child);
        sweep_current(&fixture, retry, &mut state, &memory, &task, &admission);
        assert!(
            !payload.exists() && !lexical.exists(),
            "final segment owner permits reclamation"
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn admitted_stored_terms_pin_survives_parent_and_real_generation_cleanup() {
    for retry in [false, true] {
        let fixture = Fixture::new();
        let governor = governor();
        let request = RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1);
        let permit = governor.try_admit(request).unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: crate::build_memory::read::ReadPermit::Governed(&permit),
        };
        let opened = open(fixture.context(&admission)).unwrap();
        let terms = opened
            .reader
            .visibility
            .retractions()
            .next()
            .unwrap()
            .retraction
            .unique_terms
            .clone();
        let mutation = fixture
            .root
            .join(&opened.reader.manifest.mutation_runs[0].file);
        let unrelated = fixture
            .root
            .join(&opened.reader.manifest.segments[0].payload_file);
        let mut expected = Vec::new();
        terms
            .visit_with_query_context(
                Some(crate::lexical_projection::QueryContext::from_read_admission(&admission)),
                |term| {
                    expected.push(term.to_owned());
                    Ok(())
                },
            )
            .unwrap();
        assert!(!expected.is_empty());
        drop(opened);
        drop(memory);
        drop(task);
        drop(permit);
        assert_eq!(governor.snapshot().active_background_tasks, 0);
        assert_eq!(governor.snapshot().active_background_io_slots, 0);
        replace_twice(&fixture);
        let permit = governor.try_admit(request).unwrap();
        let task = permit.bind_task_context(Default::default());
        let memory = BuildMemory::new(&task).unwrap();
        let admission = ReadAdmission {
            memory: &memory,
            task: &task,
            permit: crate::build_memory::read::ReadPermit::Governed(&permit),
        };
        let mut state = crate::generation_cleanup::SearchProjectionCleanupState::default();
        sweep_current(&fixture, retry, &mut state, &memory, &task, &admission);
        assert!(mutation.exists());
        assert!(
            !unrelated.exists(),
            "stored terms must not pin unrelated content"
        );
        let mut actual = Vec::new();
        terms
            .visit_with_query_context(
                Some(crate::lexical_projection::QueryContext::from_read_admission(&admission)),
                |term| {
                    actual.push(term.to_owned());
                    Ok(())
                },
            )
            .unwrap();
        assert_eq!(actual, expected);
        drop(terms);
        sweep_current(&fixture, retry, &mut state, &memory, &task, &admission);
        assert!(
            !mutation.exists(),
            "final stored-term owner permits reclamation"
        );
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[derive(Clone, Copy)]
enum CleanupRefusal {
    ByteLimit,
    FileLimit,
    MissingDependency,
    ReplacedWarmDependency,
}

fn cleanup_refuses_before_unlink(refusal: CleanupRefusal) {
    use crate::generation_cleanup::{once::PreparedCleanup, SearchProjectionCleanupState};
    for governed in [false, true] {
        for retry in [false, true] {
            let fixture = Fixture::new();
            let governor = governor();
            let permit = governor
                .try_admit(
                    RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1),
                )
                .unwrap();
            let task = if governed {
                permit.bind_task_context(Default::default())
            } else {
                Default::default()
            };
            let memory = BuildMemory::new(&task).unwrap();
            let admission = ReadAdmission {
                memory: &memory,
                task: &task,
                permit: crate::build_memory::read::ReadPermit::Governed(&permit),
            };
            // Warm the actual captured cache before replacing the name. The
            // cached inode remains readable, but cannot authenticate the new
            // dependency required by the persisted head for recovery.
            let opened = governed.then(|| open(fixture.context(&admission)).unwrap());
            let segment = &fixture.reader.manifest.segments[0];
            let path = fixture.root.join(&segment.payload_file);
            let original = std::fs::read(&path).unwrap();
            if let Some(opened) = &opened {
                let bytes = admission
                    .read_unbound_file(&opened.reader.segments[0].payload, segment.payload_len)
                    .unwrap();
                assert_eq!(&*bytes, original.as_slice());
            }
            let obsolete = fixture
                .root
                .join("search_projection_segment_payloads.0.hawdb");
            std::fs::write(&obsolete, b"obsolete evidence").unwrap();
            let mut options = crate::SearchProjectionCleanupOptions::default();
            match refusal {
                CleanupRefusal::ByteLimit => {
                    options.max_verification_bytes = std::num::NonZeroU64::MIN
                }
                CleanupRefusal::FileLimit => {
                    options.max_verification_files = std::num::NonZeroUsize::MIN
                }
                CleanupRefusal::MissingDependency => std::fs::remove_file(&path).unwrap(),
                CleanupRefusal::ReplacedWarmDependency => {
                    let mut changed = original.clone();
                    changed[0] ^= 1;
                    std::fs::remove_file(&path).unwrap();
                    std::fs::write(&path, changed).unwrap();
                }
            }
            let mut publication =
                SearchProjectionPublishLease::acquire_for_cleanup(&fixture.root).unwrap();
            if governed {
                publication.admit_cleanup_resources(&admission).unwrap();
            }
            let read = governed.then_some(&admission);
            let result = crate::generation_cleanup::source::VerifiedRetention::verify(
                &fixture.root,
                &publication,
                &fixture.reader.analyzer_lexicon,
                options,
                &task,
                read,
            );
            assert!(
                result.is_err(),
                "incomplete or over-budget closure must not authorize cleanup"
            );
            if retry {
                let report = SearchProjectionCleanupState::default().run_under_publication(
                    &fixture.root,
                    options,
                    &publication,
                    &fixture.reader.analyzer_lexicon,
                    read,
                );
                assert!(report.retry_required);
                assert_eq!(report.scanned_entries, 0);
                assert_eq!(report.delete_attempts, 0);
            } else {
                let report = PreparedCleanup::prepare(&fixture.root, &memory, &task)
                    .unwrap()
                    .run_under_publication(
                        &fixture.root,
                        options,
                        &task,
                        &publication,
                        &fixture.reader.analyzer_lexicon,
                        read,
                    );
                assert!(report.retry_required);
                assert_eq!(report.deleted_files, 0);
            }
            assert_eq!(std::fs::read(&obsolete).unwrap(), b"obsolete evidence");
            if matches!(refusal, CleanupRefusal::ReplacedWarmDependency)
                && let Some(opened) = &opened
            {
                let bytes = admission
                    .read_unbound_file(&opened.reader.segments[0].payload, segment.payload_len)
                    .unwrap();
                assert_eq!(
                    &*bytes,
                    original.as_slice(),
                    "held old inode is not current-name evidence"
                );
            }
            drop(publication);
            drop(opened);
            assert_eq!(memory.ledger.snapshot().used_bytes, 0);
        }
    }
}

#[test]
fn captured_cleanup_byte_ceiling_refuses_before_any_unlink() {
    cleanup_refuses_before_unlink(CleanupRefusal::ByteLimit);
}

#[test]
fn captured_cleanup_file_ceiling_refuses_before_any_unlink() {
    cleanup_refuses_before_unlink(CleanupRefusal::FileLimit);
}

#[test]
fn captured_cleanup_missing_dependency_refuses_before_any_unlink() {
    cleanup_refuses_before_unlink(CleanupRefusal::MissingDependency);
}

#[test]
fn captured_cleanup_checks_fresh_names_even_with_a_warm_managed_cache() {
    cleanup_refuses_before_unlink(CleanupRefusal::ReplacedWarmDependency);
}

#[test]
fn governed_cleanup_spills_reopens_and_removes_only_in_its_captured_scratch_namespace() {
    let content = (0..10_000)
        .map(|i| format!("uniquelexicalterm{i:05} "))
        .collect::<String>();
    let fixture = Fixture::new_with_content(false, Some(&content));
    let foreign = fixture.root.join("foreign-label");
    std::fs::create_dir(&foreign).unwrap();
    std::fs::write(foreign.join("evidence"), b"unchanged foreign root").unwrap();
    let before = std::fs::metadata(&foreign).unwrap().modified().unwrap();
    let governor = governor();
    let permit = governor
        .try_admit(RuntimeWorkRequest::background_maintenance(8 << 20).with_io_wave_slots(1))
        .unwrap();
    let task = permit.bind_task_context(Default::default());
    let memory = BuildMemory::new(&task).unwrap();
    let admission = ReadAdmission {
        memory: &memory,
        task: &task,
        permit: crate::build_memory::read::ReadPermit::Governed(&permit),
    };
    let mut publication = SearchProjectionPublishLease::acquire_for_cleanup(&fixture.root).unwrap();
    publication.admit_cleanup_resources(&admission).unwrap();
    crate::lexical_projection::take_captured_io();
    let verified = captured_artifact_generations(
        &foreign,
        &publication,
        &fixture.reader.analyzer_lexicon,
        &task,
        Default::default(),
        Some(&admission),
    )
    .unwrap();
    assert!(verified.generations.is_some());
    let [created, reopened, removed] = crate::lexical_projection::take_captured_io();
    assert!(created > 0 && reopened > 0 && removed > 0,
        "fixture must execute captured spill writes/cold opens/removal: {created}/{reopened}/{removed}");
    assert_eq!(
        std::fs::metadata(&foreign).unwrap().modified().unwrap(),
        before
    );
    assert_eq!(
        std::fs::read(foreign.join("evidence")).unwrap(),
        b"unchanged foreign root"
    );
    assert!(!std::fs::read_dir(&fixture.root).unwrap().any(|entry| entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .ends_with(".stage")));
    drop(verified);
    drop(publication);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    assert_eq!(governor.snapshot().active_background_io_slots, 0);
}
