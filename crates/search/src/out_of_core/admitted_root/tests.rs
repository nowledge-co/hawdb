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
}
impl Fixture {
    fn new() -> Self {
        Self::new_with_chinese(false)
    }
    fn new_with_chinese(chinese: bool) -> Self {
        let root = super::super::tests::test_dir("private-admitted-root");
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
                    content: "graph source text".into(),
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
        Self { root, reader }
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
        permit: &permit,
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
        permit: &permit,
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
        permit: &permit,
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
        matches!(&error, HawDBError::Execution(message) if message.contains("memory")),
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
    body.generation = 11_001;
    body.mutation_runs = (1..=11_000)
        .map(|generation| SearchOutOfCoreMutationRunManifest {
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
        permit: &permit,
    };
    let context = fixture.context(&admission);
    let (error, peak) = allocation::measure(|| {
        open(context)
            .err()
            .expect("must refuse before expanding declared runs")
    });
    assert!(
        matches!(&error, HawDBError::Execution(message) if message.contains("memory")),
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
        permit: &permit,
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
        permit: &permit,
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
