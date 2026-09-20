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
    for bytes in [isize::MAX as u64 + 1, u64::MAX] {
        assert!(SearchLexicalSourcePolicy::new(NonZeroU64::new(bytes).unwrap()).is_err());
    }
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
