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
    SearchDocument, SearchLexicalTermPolicy, SearchOutOfCoreGenerationWriter, SearchOutOfCoreReader,
};
use std::num::NonZeroU64;

#[test]
fn embedded_facade_supports_dynamic_lexical_term_policy() {
    let root = std::env::temp_dir().join(format!(
        "hawdb-term-policy-facade-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let policy = SearchLexicalTermPolicy::new(NonZeroU64::new(5202).unwrap()).unwrap();
    let mut writer = SearchOutOfCoreGenerationWriter::create(&root, Default::default()).unwrap();
    writer
        .push(SearchDocument {
            id: "record".into(),
            title: String::new(),
            content: "x".repeat(5202),
            embedding: None,
            metadata: Default::default(),
        })
        .unwrap();
    writer.set_lexical_term_policy(policy);
    writer.finish().unwrap();
    assert!(SearchOutOfCoreReader::open(&root).is_err());
    let mut reader = SearchOutOfCoreReader::open_with_term_policy(
        &root,
        Default::default(),
        Default::default(),
        policy,
    )
    .unwrap();
    assert!(reader.set_lexical_term_policy(Default::default()).is_err());
    assert_eq!(reader.lexical_term_policy(), policy);
    #[cfg(feature = "full-text-search")]
    {
        let result = reader
            .search_with_options(
                &"x".repeat(5202),
                None,
                crate::SearchMode::Text,
                crate::SearchQueryOptions {
                    limit: 10,
                    offset: 0,
                    rank_window: None,
                    fusion_weights: Default::default(),
                    metadata_filters: Default::default(),
                    policy_epoch: None,
                },
            )
            .unwrap();
        assert_eq!(result.result.hits.len(), 1);
    }
    drop(reader);
    std::fs::remove_dir_all(root).unwrap();
}
