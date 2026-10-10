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
use crate::file_descriptors::{context_for_path, ProjectFileDescriptors};
use crate::immutable_files::ImmutableFileBinding;
use crate::immutable_object::{ImmutableObjectStore, ObjectKind, ObjectReference};
use crate::relational::relational_index_shadow_artifact_file;
use std::io::{Read, Seek, SeekFrom, Write};

struct MountedFixture {
    project: ProjectFileDescriptors,
    view: Arc<RelationalIndexReadView>,
    oracle: Oracle,
    reference: ObjectReference,
    object_path: std::path::PathBuf,
    selected_page_bytes: usize,
    unselected_page_offset: u64,
    fixture: Fixture,
}

impl MountedFixture {
    fn new() -> Self {
        let (fixture, view, oracle, _) = Fixture::open();
        let project = ProjectFileDescriptors::acquire_existing(&fixture.0, 8).unwrap();
        let path = fixture.0.join(relational_index_shadow_artifact_file(1));
        let bytes = std::fs::read(&path).unwrap();
        let RelationalIndexReadBackend::Base(reader) = &view.backend else {
            unreachable!()
        };
        let manifest = reader.manifest();
        let selected_page_bytes = usize::try_from(manifest.page_bytes).unwrap();
        assert!(bytes.len() > selected_page_bytes);
        let root = manifest.root(TABLE, INDEX).unwrap();
        assert_eq!(
            root.height, 1,
            "the owned fixture has one leaf below the root descriptor"
        );
        let selected = root.root_page_id.get();
        let unselected = if selected == 1 { 2 } else { 1 };
        let unselected_page_offset = (unselected - 1) * manifest.page_bytes;
        let mut objects = ImmutableObjectStore::open(fixture.0.join("immutable")).unwrap();
        let reference = ObjectReference::for_bytes(ObjectKind::CheckpointArtifact, 1, &bytes);
        objects.publish(reference, &bytes).unwrap();
        let object_path = objects.object_path(reference);
        project
            .immutable_handles
            .mount(
                &path,
                ImmutableFileBinding {
                    reference,
                    object_path: object_path.clone(),
                },
                &context_for_path(&path).unwrap(),
            )
            .unwrap();
        assert_eq!(
            project.metrics().cached_handles,
            0,
            "mount must not warm the handle"
        );
        Self {
            fixture,
            project,
            view,
            oracle,
            reference,
            object_path,
            selected_page_bytes,
            unselected_page_offset,
        }
    }

    fn corrupt_unselected_page(&self) {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.object_path)
            .unwrap();
        file.seek(SeekFrom::Start(self.unselected_page_offset))
            .unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(self.unselected_page_offset))
            .unwrap();
        file.write_all(&byte).unwrap();
    }

    fn evict_handle(&self) {
        let path = self.fixture.0.join("held-descriptor");
        std::fs::write(&path, []).unwrap();
        let mut held = Vec::new();
        while self.project.metrics().cached_handles > 0 {
            assert!(held.len() < 8);
            held.push(crate::file_io::File::open(&path).unwrap());
        }
        assert!(self.project.metrics().cache_evictions > 0);
        drop(held);
    }

    fn probe(
        &self,
        context: &RelationalIndexReadContext,
        kind: usize,
        limits: RelationalIndexReadLimits,
        rows: &mut Vec<(RelationalKey, RelationalKey)>,
    ) -> Result<RelationalIndexReadViewReport, RelationalIndexShadowError> {
        let scan = RelationalIndexRangeScan {
            prefix: key(&[1]),
            exclusive_bound: None,
            direction: RelationalIndexScanDirection::Forward,
        };
        let target = RelationalIndexReadTarget::View(&self.view);
        let mut visit = |index: &RelationalKey, primary: &RelationalKey| {
            rows.push((index.clone(), primary.clone()));
            true
        };
        match kind {
            0 => {
                context.visit_prefix_entries(target, TABLE, INDEX, &scan.prefix, limits, &mut visit)
            }
            1 => context.visit_prefix_entries_many(
                target,
                TABLE,
                INDEX,
                std::slice::from_ref(&scan.prefix),
                limits,
                &mut visit,
            ),
            _ => context.visit_range_entries(target, TABLE, INDEX, &scan, limits, &mut visit),
        }
    }
}

fn reject_cold_mounted_probe(evicted: bool) {
    for kind in 0..3 {
        let fixture = MountedFixture::new();
        if evicted {
            let context = RelationalIndexReadContext::new(Default::default());
            let mut rows = Vec::new();
            fixture
                .probe(&context, kind, Default::default(), &mut rows)
                .unwrap();
            fixture.evict_handle();
        }
        fixture.corrupt_unselected_page();
        let limits = RelationalIndexReadLimits {
            max_file_bytes: fixture.selected_page_bytes,
            ..Default::default()
        };
        let context = RelationalIndexReadContext::new(limits);
        let mut rows = Vec::new();
        let output = fixture.probe(&context, kind, limits, &mut rows);
        assert!(matches!(output, Err(RelationalIndexShadowError::Admission(_))), "must refuse whole-object validation before unselected payload: kind={kind}, evicted={evicted}, output={output:?}");
        assert!(rows.is_empty());
        assert!(!fixture.view.is_poisoned());
        assert_eq!(fixture.project.metrics().cached_handles, 0);
    }
}

#[test]
fn cold_mounted_base_validation_admits_before_unselected_payload() {
    reject_cold_mounted_probe(false);
}

#[test]
fn evicted_mounted_base_validation_admits_before_unselected_payload() {
    reject_cold_mounted_probe(true);
}

#[test]
fn mounted_base_validation_accounts_once_and_warm_reads_keep_complete_results() {
    for kind in 0..3 {
        let fixture = MountedFixture::new();
        let limits = RelationalIndexReadLimits {
            max_file_bytes: usize::try_from(fixture.reference.byte_length).unwrap()
                + 2 * fixture.selected_page_bytes,
            ..Default::default()
        };
        let context = RelationalIndexReadContext::new(limits);
        let mut rows = Vec::new();
        let report = fixture.probe(&context, kind, limits, &mut rows).unwrap();
        let scan = RelationalIndexRangeScan {
            prefix: key(&[1]),
            exclusive_bound: None,
            direction: RelationalIndexScanDirection::Forward,
        };
        assert_eq!(rows, expected(&fixture.oracle, &scan));
        let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
            unreachable!()
        };
        assert_eq!(
            report.file_bytes_read, limits.max_file_bytes,
            "physical report includes whole-object validation, root and leaf exactly once"
        );
        assert_eq!(context.remaining_limits().unwrap().max_file_bytes, 0);
        assert_eq!(fixture.project.metrics().cached_handles, 1);
        let warm_limits = RelationalIndexReadLimits {
            max_file_bytes: 2 * fixture.selected_page_bytes,
            ..Default::default()
        };
        let warm_context = RelationalIndexReadContext::new(warm_limits);
        let mut warm_rows = Vec::new();
        let warm_report = fixture
            .probe(&warm_context, kind, warm_limits, &mut warm_rows)
            .unwrap();
        assert_eq!(warm_rows, rows);
        let RelationalIndexReadViewBackendReport::Base(warm_report) = warm_report.backend else {
            unreachable!()
        };
        assert_eq!(warm_report.file_bytes_read, 2 * fixture.selected_page_bytes);
        assert_eq!(warm_report.file_pages_read, 2);
        assert_eq!(warm_context.remaining_limits().unwrap().max_file_bytes, 0);
        assert!(!fixture.view.is_poisoned());
    }
}

#[test]
fn admitted_mounted_validation_retains_whole_object_integrity() {
    let fixture = MountedFixture::new();
    fixture.corrupt_unselected_page();
    let limits = RelationalIndexReadLimits {
        max_file_bytes: usize::try_from(fixture.reference.byte_length).unwrap()
            + 2 * fixture.selected_page_bytes,
        ..Default::default()
    };
    let context = RelationalIndexReadContext::new(limits);
    let mut rows = Vec::new();
    let output = fixture.probe(&context, 0, limits, &mut rows);
    assert!(matches!(
        output,
        Err(RelationalIndexShadowError::Durability(_))
    ));
    assert!(rows.is_empty());
    assert!(fixture.view.is_poisoned());
    assert!(context.remaining_limits().is_err());
    assert_eq!(fixture.project.metrics().cached_handles, 0);
}

#[test]
fn mounted_descriptor_rejection_retains_payload_allowance_for_statement_retry() {
    let fixture = MountedFixture::new();
    let path = fixture.fixture.0.join("held-descriptor");
    std::fs::write(&path, []).unwrap();
    let held = (0..8)
        .map(|_| crate::file_io::File::open(&path).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(fixture.project.metrics().open, 8);
    let limits = RelationalIndexReadLimits {
        max_file_bytes: usize::try_from(fixture.reference.byte_length).unwrap()
            + 2 * fixture.selected_page_bytes,
        ..Default::default()
    };
    let context = RelationalIndexReadContext::new(limits);
    let mut rows = Vec::new();
    let output = fixture.probe(&context, 0, limits, &mut rows);
    assert!(matches!(
        output,
        Err(RelationalIndexShadowError::FileDescriptors(_))
    ));
    assert!(rows.is_empty());
    assert!(!fixture.view.is_poisoned());
    assert_eq!(
        context.remaining_limits().unwrap().max_file_bytes,
        limits.max_file_bytes
    );
    assert_eq!(fixture.project.metrics().cached_handles, 0);
    drop(held);
    let report = fixture.probe(&context, 0, limits, &mut rows).unwrap();
    let scan = RelationalIndexRangeScan {
        prefix: key(&[1]),
        exclusive_bound: None,
        direction: RelationalIndexScanDirection::Forward,
    };
    assert_eq!(rows, expected(&fixture.oracle, &scan));
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        unreachable!()
    };
    assert_eq!(report.file_bytes_read, limits.max_file_bytes);
    assert_eq!(context.remaining_limits().unwrap().max_file_bytes, 0);
    assert!(!fixture.view.is_poisoned());
}

fn retry_transaction_probe_after_descriptor_rejection(kind: usize) {
    let fixture = MountedFixture::new();
    let path = fixture.fixture.0.join("held-descriptor");
    std::fs::write(&path, []).unwrap();
    let held = (0..8)
        .map(|_| crate::file_io::File::open(&path).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(fixture.project.metrics().open, 8);
    let limits = RelationalIndexReadLimits {
        max_file_bytes: usize::try_from(fixture.reference.byte_length).unwrap()
            + 2 * fixture.selected_page_bytes,
        ..Default::default()
    };
    let transaction =
        RelationalTransactionIndexView::new(fixture.view.clone(), Default::default(), limits);
    let context = RelationalIndexReadContext::new(limits);
    let scan = RelationalIndexRangeScan {
        prefix: key(&[1]),
        exclusive_bound: None,
        direction: RelationalIndexScanDirection::Forward,
    };
    let target = RelationalIndexReadTarget::Transaction(&transaction);
    let mut rows = Vec::new();
    let probe = |rows: &mut Vec<(RelationalKey, RelationalKey)>| {
        let mut visit = |index: &RelationalKey, primary: &RelationalKey| {
            rows.push((index.clone(), primary.clone()));
            true
        };
        match kind {
            0 => {
                context.visit_prefix_entries(target, TABLE, INDEX, &scan.prefix, limits, &mut visit)
            }
            1 => context.visit_prefix_entries_many(
                target,
                TABLE,
                INDEX,
                std::slice::from_ref(&scan.prefix),
                limits,
                &mut visit,
            ),
            _ => context.visit_range_entries(target, TABLE, INDEX, &scan, limits, &mut visit),
        }
    };
    assert!(matches!(
        probe(&mut rows),
        Err(RelationalIndexShadowError::FileDescriptors(_))
    ));
    assert!(rows.is_empty());
    assert!(!fixture.view.is_poisoned());
    let remaining = context.remaining_limits().unwrap();
    assert_eq!(remaining.max_file_bytes, limits.max_file_bytes);
    assert!(
        remaining.max_pages.get() < limits.max_pages.get(),
        "logical operation charges are retained"
    );
    drop(held);
    let report = probe(&mut rows)
        .expect("the same transaction remains retryable after descriptor capacity returns");
    assert_eq!(rows, expected(&fixture.oracle, &scan));
    let RelationalIndexReadViewBackendReport::Base(report) = report.backend else {
        panic!("base")
    };
    assert_eq!(report.file_bytes_read, limits.max_file_bytes);
    assert_eq!(context.remaining_limits().unwrap().max_file_bytes, 0);
    assert_eq!(
        context.remaining_limits().unwrap().max_pages.get(),
        remaining.max_pages.get() - report.pages_read
    );
    assert!(matches!(
        transaction.count_exact_postings(TABLE, INDEX, &key(&[0, 0]), limits),
        Some(Err(RelationalIndexShadowError::Admission(_)))
    ));
    assert!(!fixture.view.is_poisoned());
}

#[test]
fn descriptor_rejection_retries_transaction_prefix() {
    retry_transaction_probe_after_descriptor_rejection(0);
}

#[test]
fn descriptor_rejection_retries_transaction_batch() {
    retry_transaction_probe_after_descriptor_rejection(1);
}

#[test]
fn descriptor_rejection_retries_transaction_range() {
    retry_transaction_probe_after_descriptor_rejection(2);
}

fn retry_constraint_after_descriptor_rejection(private: bool) {
    let fixture = MountedFixture::new();
    let limits = RelationalIndexReadLimits {
        max_file_bytes: usize::try_from(fixture.reference.byte_length).unwrap()
            + 2 * fixture.selected_page_bytes,
        ..Default::default()
    };
    let reader: Box<dyn RelationalConstraintIndex> = if private {
        Box::new(RelationalTransactionIndexView::new(
            fixture.view.clone(),
            Default::default(),
            limits,
        ))
    } else {
        Box::new(AuthoritativeRelationalConstraintIndex::new(
            fixture.view.clone(),
            limits,
        ))
    };
    let path = fixture.fixture.0.join("held-descriptor");
    std::fs::write(&path, []).unwrap();
    let held = (0..8)
        .map(|_| crate::file_io::File::open(&path).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(fixture.project.metrics().open, 8);
    let mut rows = Vec::new();
    let probe = |rows: &mut Vec<RelationalKey>| {
        reader.visit_exact_primary_keys(TABLE, INDEX, &key(&[0, 0]), &mut |row| {
            rows.push(row.clone());
            true
        })
    };
    assert!(matches!(
        probe(&mut rows),
        Err(RelationalError::FileDescriptors(_))
    ));
    assert!(rows.is_empty());
    assert!(!fixture.view.is_poisoned());
    drop(held);
    probe(&mut rows)
        .expect("the same constraint reader remains retryable after descriptor capacity returns");
    assert_eq!(rows, vec![key(&[0])]);
    rows.clear();
    assert!(matches!(
        probe(&mut rows),
        Err(RelationalError::Admission(_))
    ));
    assert!(rows.is_empty());
    assert!(!fixture.view.is_poisoned());
}

#[test]
fn descriptor_rejection_retries_transaction_private_constraint() {
    retry_constraint_after_descriptor_rejection(true);
}

#[test]
fn descriptor_rejection_retries_transaction_committed_constraint() {
    retry_constraint_after_descriptor_rejection(false);
}
