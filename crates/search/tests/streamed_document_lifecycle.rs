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

//! The complete large-body lifecycle has its own compilation and deadline.
#![cfg(feature = "full-text-search")]

#[path = "../src/out_of_core/verified_body/fixtures.rs"]
mod fixtures;

use fixtures::{header, options, task, Generated};
use hawdb_search::{
    SearchBodyReadOptions, SearchDocumentBody, SearchDocumentHeader, SearchFusionWeights,
    SearchLexicalSourcePolicy, SearchMode, SearchOutOfCoreConfig,
    SearchOutOfCoreGenerationBuildOptions, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
    SearchOutOfCoreSegmentCompactionPolicy, SearchQueryOptions,
};
use hawdb_storage::file_io as fs;
use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    // Keep the fixture's FD domain owned until its test thread exits.
    static TEST_PROJECTS: std::cell::RefCell<Vec<hawdb_storage::file_descriptors::ProjectFileDescriptors>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn test_dir(name: &str) -> PathBuf {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let temporary = std::env::var_os("TEST_TMPDIR").map_or_else(std::env::temp_dir, PathBuf::from);
    let path = temporary.join(format!(
        "hawdb-search-out-of-core-{name}-{}-{sequence}",
        std::process::id()
    ));
    let files = hawdb_storage::file_descriptors::ProjectFileDescriptors::acquire(
        &path,
        hawdb_storage::file_descriptors::DEFAULT_MAX_OPEN_FILES,
    )
    .unwrap();
    TEST_PROJECTS.with_borrow_mut(|owners| owners.push(files));
    path
}

fn search_options(limit: usize, rank_window: Option<usize>) -> SearchQueryOptions {
    SearchQueryOptions {
        limit,
        offset: 0,
        rank_window,
        fusion_weights: SearchFusionWeights::default(),
        metadata_filters: BTreeMap::new(),
        policy_epoch: None,
    }
}

// Typecheck the complete public fixture on every FTS host; anonymous-body
// transfer is executable only on Linux.
#[cfg_attr(target_os = "linux", test)]
pub fn streamed_mutable_lifecycle_keeps_large_bodies_out_of_operation_memory() {
    let bytes = 32 * 1024 * 1024;
    let root = test_dir("streamed_mutable_lifecycle");
    let config = SearchOutOfCoreConfig {
        max_uncompressed_segment_bytes: NonZeroU64::new(bytes * 2 + 4096).unwrap(),
        max_hydrated_bytes: NonZeroU64::MIN,
        max_reanalysis_working_bytes: NonZeroU64::new(8 * 1024 * 1024).unwrap(),
        ..Default::default()
    };
    let source_policy = SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes + 1024).unwrap())
        .unwrap()
        .with_max_document_tokens(NonZeroUsize::new(8_000_000).unwrap());
    let open = || {
        SearchOutOfCoreReader::open_with_source_policy(
            &root,
            config.clone(),
            Default::default(),
            source_policy,
        )
        .unwrap()
    };
    let source = || Generated {
        remaining: bytes,
        position: 0,
    };
    let declared = SearchDocumentBody {
        bytes,
        expected_checksum: None,
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_context(
        &root,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    writer.push_reader(header(), source(), declared).unwrap();
    writer.finish().unwrap();
    let original = open();
    let query = search_options(8, None);
    let original_candidate = original
        .search_candidates_with_options("graph", None, SearchMode::Text, query.clone())
        .unwrap()
        .result
        .hits
        .remove(0);
    let mut appended = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &original,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    let mut second = header();
    second.id = "b".into();
    appended.upsert_reader(second, source(), declared).unwrap();
    let (_, build, metrics) = appended.finish().unwrap();
    assert_eq!(build.document_count, 2);
    assert_eq!(metrics.streamed_documents, 0);
    let before_compaction = open();
    let before = before_compaction
        .search_candidates_with_options("graph", None, SearchMode::Text, query.clone())
        .unwrap();
    let compacted = SearchOutOfCoreGenerationWriter::prepare_segment_compaction_with_context(
        &before_compaction,
        SearchOutOfCoreSegmentCompactionPolicy::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(1024 * 1024 * 1024).unwrap(),
        )
        .unwrap(),
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap()
    .unwrap()
    .finish()
    .unwrap();
    assert_eq!(compacted.source_read_metrics().streamed_documents, 2);
    assert_eq!(compacted.source_read_metrics().hydrated_documents, 0);
    let base = open();
    let after = base
        .search_candidates_with_options("graph", None, SearchMode::Text, query.clone())
        .unwrap();
    assert_eq!(
        before
            .result
            .hits
            .iter()
            .map(|hit| &hit.scores)
            .collect::<Vec<_>>(),
        after
            .result
            .hits
            .iter()
            .map(|hit| &hit.scores)
            .collect::<Vec<_>>()
    );
    let mut replacement = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &base,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    let mut changed = header();
    changed.title = "replacement".into();
    replacement
        .upsert_reader(changed, source(), declared)
        .unwrap();
    let (_, build, metrics) = replacement.finish().unwrap();
    assert_eq!(build.document_count, 2);
    assert_eq!(metrics.streamed_body_bytes, bytes);
    assert_eq!(metrics.hydrated_documents, 0);
    let replaced = open();
    let visible = replaced
        .search_candidates_with_options("replacement", None, SearchMode::Text, query.clone())
        .unwrap();
    assert_eq!(visible.result.hits.len(), 1);
    assert_eq!(visible.result.hits[0].scores.id, "a");
    let mut repeated = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &replaced,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    let mut changed_again = header();
    changed_again.title = "secondreplacement".into();
    repeated
        .upsert_reader(changed_again, source(), declared)
        .unwrap();
    repeated.finish().unwrap();
    let repeated = open();
    assert!(repeated
        .search_candidates_with_options("replacement", None, SearchMode::Text, query.clone())
        .unwrap()
        .result
        .hits
        .is_empty());
    assert_eq!(
        repeated
            .search_candidates_with_options(
                "secondreplacement",
                None,
                SearchMode::Text,
                query.clone()
            )
            .unwrap()
            .result
            .hits
            .len(),
        1
    );
    let mut deleted = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &repeated,
        options(bytes),
        task(8 * 1024 * 1024),
    )
    .unwrap();
    deleted.delete("a").unwrap();
    let (_, build, metrics) = deleted.finish().unwrap();
    assert_eq!(build.document_count, 1);
    assert_eq!(metrics.streamed_body_bytes, bytes);
    let current = open();
    let visible = current
        .search_candidates_with_options("graph", None, SearchMode::Text, query)
        .unwrap();
    assert_eq!(visible.result.hits.len(), 1);
    assert_eq!(visible.result.hits[0].scores.id, "b");
    assert!(current
        .open_verified_body(
            &original_candidate,
            SearchBodyReadOptions::default(),
            task(4 * 1024 * 1024)
        )
        .is_err());
    let mut old_body = original
        .open_verified_body(
            &original_candidate,
            SearchBodyReadOptions {
                max_body_bytes: NonZeroU64::new(bytes).unwrap(),
                ..Default::default()
            },
            task(4 * 1024 * 1024),
        )
        .unwrap();
    assert_eq!(
        std::io::copy(&mut old_body, &mut std::io::sink()).unwrap(),
        bytes
    );
    assert!(old_body.is_complete());
    drop(old_body);
    let mut restored = SearchOutOfCoreGenerationWriter::prepare_streamed_delta_with_context(
        &current,
        options(bytes),
        task(16 * 1024 * 1024),
    )
    .unwrap();
    restored
        .upsert_reader(header(), source(), declared)
        .unwrap();
    restored.finish().unwrap();
    let restored = open();
    let visible = restored
        .search_candidates_with_options("graph", None, SearchMode::Text, search_options(8, None))
        .unwrap();
    assert_eq!(visible.result.hits.len(), 2);
    assert_eq!(visible.result.hits[0].scores.id, "a");
    assert_eq!(visible.result.hits[1].scores.id, "b");
    drop(restored);
    drop(current);
    drop(repeated);
    drop(replaced);
    drop(base);
    drop(before_compaction);
    drop(original);
    fs::remove_dir_all(root).unwrap();
}
