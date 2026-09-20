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

use crate::{
    SearchDocument, SearchLexicalSourcePolicy, SearchOutOfCoreGenerationWriter,
    SearchOutOfCoreReader,
};
use std::num::NonZeroU64;

#[test]
fn embedded_facade_supports_host_selected_lexical_source_policy() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-source-policy-facade-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let policy = SearchLexicalSourcePolicy::new(NonZeroU64::new(5 * 1024 * 1024).unwrap()).unwrap();
    let mut writer = SearchOutOfCoreGenerationWriter::create_with_source_policy(
        &root,
        Default::default(),
        policy,
    )
    .unwrap();
    writer
        .push(SearchDocument {
            id: "record".into(),
            title: String::new(),
            content: " ".repeat(4 * 1024 * 1024 + 1),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    writer.finish().unwrap();
    let reader = SearchOutOfCoreReader::open_with_source_policy(
        &root,
        Default::default(),
        Default::default(),
        policy,
    )
    .unwrap();
    assert_eq!(reader.lexical_source_policy(), policy);
    assert_eq!(reader.document_count(), 1);
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}
