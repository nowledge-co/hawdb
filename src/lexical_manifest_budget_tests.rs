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
    SearchDocument, SearchOutOfCoreConfig, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
use std::num::NonZeroU64;

#[test]
fn embedded_facade_supports_explicit_lexical_manifest_budget() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-manifest-budget-facade-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    assert_eq!(writer.max_lexical_manifest_bytes().get(), 256 * 1024 * 1024);
    let selected = NonZeroU64::new(512 * 1024 * 1024).unwrap();
    writer.set_max_lexical_manifest_bytes(selected).unwrap();
    writer
        .push(SearchDocument {
            id: "record".into(),
            title: String::new(),
            content: "graph storage".into(),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    assert!(writer
        .set_max_lexical_manifest_bytes(NonZeroU64::new(u64::MAX).unwrap())
        .is_err());
    assert_eq!(writer.max_lexical_manifest_bytes(), selected);
    let report = writer.finish().unwrap();
    let config = SearchOutOfCoreConfig {
        max_lexical_manifest_bytes: NonZeroU64::new(report.lexical_manifest_bytes).unwrap(),
        ..Default::default()
    };
    let reader = SearchOutOfCoreReader::open_with_config(&root, config.clone()).unwrap();
    assert_eq!(reader.document_count(), 1);
    drop(reader);
    assert!(SearchOutOfCoreReader::open_with_config(
        &root,
        SearchOutOfCoreConfig {
            max_lexical_manifest_bytes: NonZeroU64::new(report.lexical_manifest_bytes - 1).unwrap(),
            ..config
        }
    )
    .is_err());
    std::fs::remove_dir_all(root).unwrap();
}
