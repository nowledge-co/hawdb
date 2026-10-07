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
use crate::{
    SearchLexicalSourcePolicy, SearchLexicalTermPolicy, SearchOutOfCoreReader,
    SearchProjectionDelta, SearchProjectionKind, SearchProjectionRow,
};

fn policy(bytes: u64) -> SearchLexicalSourcePolicy {
    SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes).unwrap()).unwrap()
}

fn whitespace_document(id: &str) -> SearchDocument {
    SearchDocument {
        id: id.into(),
        title: String::new(),
        content: " ".repeat(4 * 1024 * 1024 + 1),
        embedding: None,
        metadata: BTreeMap::new(),
    }
}

fn whitespace_delta(id: &str) -> SearchProjectionDelta {
    SearchProjectionDelta {
        upserts: vec![SearchProjectionRow {
            kind: SearchProjectionKind::Memory,
            external_id: id.into(),
            title: String::new(),
            body: " ".repeat(4 * 1024 * 1024 + 1),
            embedding: None,
            source_id: None,
            metadata: BTreeMap::new(),
        }],
        deletes: Vec::new(),
        max_operations: None,
        source_graph_commit_epoch: None,
    }
}

#[test]
fn source_policy_has_checked_bounds_and_unchanged_default() {
    assert_eq!(
        SearchLexicalSourcePolicy::default(),
        policy(4 * 1024 * 1024)
    );
    assert_eq!(policy(1).max_document_source_bytes().get(), 1);
    assert_eq!(policy(1).max_document_tokens().get(), 1_000_000);
    for bytes in [isize::MAX as u64 + 1, u64::MAX] {
        assert!(SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes).unwrap()).is_err());
    }
}

#[cfg(feature = "full-text-search")]
#[test]
fn review_token_policy_survives_default_option_compaction_replace_delete_and_reopen() {
    use crate::{SearchDocumentBody, SearchDocumentHeader, SearchMode};

    let root = test_dir("inherited_token_policy");
    let content = "zz ".repeat(1_000_010);
    let policy = SearchLexicalSourcePolicy::default()
        .with_max_document_tokens(NonZeroUsize::new(1_000_100).unwrap());
    let header = |id: &str| SearchDocumentHeader {
        id: id.into(),
        title: String::new(),
        embedding: None,
        metadata: Default::default(),
    };
    let source = SearchDocumentBody {
        bytes: content.len() as u64,
        expected_checksum: None,
    };
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_source_policy(
        &root,
        Default::default(),
        policy,
    )
    .unwrap();
    assert_eq!(writer.lexical_source_policy(), policy);
    writer
        .push_reader(header("a"), content.as_bytes(), source)
        .unwrap();
    writer.finish().unwrap();
    let open = || {
        SearchOutOfCoreReader::open_with_source_policy(
            &root,
            Default::default(),
            Default::default(),
            policy,
        )
        .unwrap()
    };
    let original = open();
    let mut append =
        SearchOutOfCoreGenerationWriter::prepare_streamed_delta(&original, Default::default())
            .unwrap();
    append
        .upsert_reader(
            header("b"),
            &b"small"[..],
            SearchDocumentBody {
                bytes: 5,
                expected_checksum: None,
            },
        )
        .unwrap();
    append.finish().unwrap();
    let appended = open();
    SearchOutOfCoreGenerationWriter::prepare_segment_compaction_with_context(
        &appended,
        SearchOutOfCoreSegmentCompactionPolicy::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(512 * 1024 * 1024).unwrap(),
        )
        .unwrap(),
        Default::default(),
        RuntimeTaskContext::default(),
    )
    .unwrap()
    .unwrap()
    .finish()
    .unwrap();
    let mut compacted = open();
    assert_eq!(compacted.document_count(), 2);
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    compacted.set_lexical_source_policy(SearchLexicalSourcePolicy::default());
    let mut rejected =
        SearchOutOfCoreGenerationWriter::prepare_streamed_delta(&compacted, Default::default())
            .unwrap();
    assert!(rejected
        .delete("a")
        .unwrap_err()
        .to_string()
        .contains("tokens"));
    drop(rejected);
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );
    compacted.set_lexical_source_policy(policy);
    let mut replacement =
        SearchOutOfCoreGenerationWriter::prepare_streamed_delta(&compacted, Default::default())
            .unwrap();
    replacement
        .upsert_reader(header("a"), content.as_bytes(), source)
        .unwrap();
    replacement.finish().unwrap();
    let replaced = open();
    let query = crate::out_of_core::tests::options(8, None);
    assert_eq!(
        replaced
            .search_candidates_with_options("zz", None, SearchMode::Text, query.clone())
            .unwrap()
            .result
            .hits
            .len(),
        1
    );
    let mut delete =
        SearchOutOfCoreGenerationWriter::prepare_streamed_delta(&replaced, Default::default())
            .unwrap();
    delete.delete("a").unwrap();
    delete.finish().unwrap();
    let deleted = open();
    assert_eq!(deleted.document_count(), 1);
    assert!(deleted
        .search_candidates_with_options("zz", None, SearchMode::Text, query.clone())
        .unwrap()
        .result
        .hits
        .is_empty());
    assert_eq!(
        original
            .search_candidates_with_options("zz", None, SearchMode::Text, query)
            .unwrap()
            .result
            .hits
            .len(),
        1
    );
    drop((original, appended, compacted, replaced, deleted));
    fs::remove_dir_all(&root).unwrap();
}

#[test]
fn source_policy_covers_build_reopen_and_prepared_update_lifecycle() {
    let root = test_dir("source_policy_lifecycle");
    // Delta rows also retain fixed graph metadata, so leave room above the
    // body boundary while still crossing the unchanged default source limit.
    let expanded = policy(5 * 1024 * 1024);
    let first = whitespace_document("memory:000000");

    let mut rejected = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    rejected.push(first.clone()).unwrap();
    assert!(rejected
        .finish()
        .unwrap_err()
        .to_string()
        .contains("source bytes"));
    assert!(!root.join(OUT_OF_CORE_MANIFEST_FILE).exists());
    assert_eq!(stage_directories(&root), 0);

    let mut writer = SearchOutOfCoreGenerationWriter::create_with_lexical_policies(
        &root,
        Default::default(),
        SearchLexicalTermPolicy::default(),
        expanded,
    )
    .unwrap();
    assert_eq!(writer.lexical_source_policy(), expanded);
    writer.push(first).unwrap();
    writer.finish().unwrap();

    let mut reader = SearchOutOfCoreReader::open_with_lexical_policies(
        &root,
        Default::default(),
        Default::default(),
        SearchLexicalTermPolicy::default(),
        expanded,
    )
    .unwrap();
    let update = SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        whitespace_delta("000001"),
        Default::default(),
    )
    .unwrap();
    reader.set_lexical_source_policy(SearchLexicalSourcePolicy::default());
    let (_, report, _) = update.finish().unwrap();
    assert_eq!(report.document_count, 2);

    let reader = SearchOutOfCoreReader::open_with_source_policy(
        &root,
        Default::default(),
        Default::default(),
        expanded,
    )
    .unwrap();
    assert_eq!(reader.document_count(), 2);

    let active = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let failed = SearchOutOfCoreGenerationWriter::prepare_delta(
        &SearchOutOfCoreReader::open(&root).unwrap(),
        whitespace_delta("000002"),
        Default::default(),
    )
    .unwrap();
    assert!(failed
        .finish()
        .unwrap_err()
        .to_string()
        .contains("source bytes"));
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        active
    );
    assert_eq!(stage_directories(&root), 0);
    fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "full-text-search")]
#[test]
fn reader_token_policy_covers_default_updates_compaction_and_retraction() {
    let root = test_dir("reader_token_policy_lifecycle");
    let token_limit = NonZeroUsize::new(2_000_000).unwrap();
    let expanded = policy(8 * 1024 * 1024).with_max_document_tokens(token_limit);
    let body = "omega ".repeat(1_000_001);
    let options = SearchOutOfCoreGenerationBuildOptions {
        lexical_max_document_tokens: token_limit,
        ..Default::default()
    };
    let mut writer =
        SearchOutOfCoreGenerationWriter::create_with_source_policy(&root, options, expanded)
            .unwrap();
    writer
        .push(SearchDocument {
            id: "memory:000000".into(),
            title: String::new(),
            content: body.clone(),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    writer.finish().unwrap();
    let open = || {
        SearchOutOfCoreReader::open_with_source_policy(
            &root,
            Default::default(),
            Default::default(),
            expanded,
        )
        .unwrap()
    };

    let reader = open();
    SearchOutOfCoreGenerationWriter::prepare_delta(
        &reader,
        SearchProjectionDelta {
            upserts: vec![SearchProjectionRow {
                kind: SearchProjectionKind::Memory,
                external_id: "000001".into(),
                title: String::new(),
                body: body.clone(),
                embedding: None,
                source_id: None,
                metadata: Default::default(),
            }],
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap()
    .finish()
    .unwrap();
    let reader = open();
    let compaction = SearchOutOfCoreGenerationWriter::compact_segments(
        &reader,
        SearchOutOfCoreSegmentCompactionPolicy::new(
            NonZeroUsize::new(2).unwrap(),
            NonZeroU64::new(64 * 1024 * 1024).unwrap(),
        )
        .unwrap(),
        Default::default(),
    )
    .unwrap()
    .unwrap();
    assert_eq!(compaction.build().document_count, 2);

    // Artifacts cannot silently increase a default reader's token admission.
    let restricted = SearchOutOfCoreReader::open_with_source_policy(
        &root,
        Default::default(),
        Default::default(),
        policy(8 * 1024 * 1024),
    )
    .unwrap();
    let before = fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap();
    let mut rejected =
        SearchOutOfCoreGenerationWriter::prepare_streamed_delta(&restricted, Default::default())
            .unwrap();
    assert!(rejected
        .delete("memory:000000")
        .unwrap_err()
        .to_string()
        .contains("tokens"));
    assert!(rejected.finish().is_err());
    assert_eq!(
        fs::read(root.join(OUT_OF_CORE_MANIFEST_FILE)).unwrap(),
        before
    );

    let reader = open();
    let mut replaced =
        SearchOutOfCoreGenerationWriter::prepare_streamed_delta(&reader, Default::default())
            .unwrap();
    replaced
        .upsert_reader(
            crate::SearchDocumentHeader {
                id: "memory:000000".into(),
                title: String::new(),
                embedding: None,
                metadata: Default::default(),
            },
            body.as_bytes(),
            crate::SearchDocumentBody {
                bytes: body.len() as u64,
                expected_checksum: None,
            },
        )
        .unwrap();
    replaced.finish().unwrap();
    let reader = open();
    let mut deleted =
        SearchOutOfCoreGenerationWriter::prepare_streamed_delta(&reader, Default::default())
            .unwrap();
    deleted.delete("memory:000000").unwrap();
    deleted.finish().unwrap();
    assert_eq!(open().document_count(), 1);
    drop(reader);
    drop(restricted);
    fs::remove_dir_all(root).unwrap();
}
