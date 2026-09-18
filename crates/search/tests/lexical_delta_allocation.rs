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

//! Measure the public read/update/delete path without counting fixture setup.
#![cfg(feature = "full-text-search")]

#[path = "support/allocation.rs"]
mod allocation;

use allocation::measure;
use hawdb_search::{SearchDocument, SearchIndex, SearchMode, SearchQueryOptions};
use std::collections::BTreeMap;
use std::fmt::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

struct Fixture(PathBuf);

impl Fixture {
    fn new(terms: usize) -> Self {
        Self(std::env::temp_dir().join(format!(
            "hawdb-delta-allocation-{}-{}-{terms}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        )))
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn document(content: String) -> SearchDocument {
    SearchDocument {
        id: "document".into(),
        title: String::new(),
        content,
        embedding: None,
        metadata: BTreeMap::new(),
    }
}

fn scores(index: &SearchIndex, expected_hits: usize) -> Vec<(String, f64)> {
    let report = index
        .try_search_with_options(
            "zzreplacement",
            None,
            SearchMode::Text,
            SearchQueryOptions {
                limit: 10,
                offset: 0,
                rank_window: None,
                fusion_weights: Default::default(),
                metadata_filters: BTreeMap::new(),
                policy_epoch: None,
            },
        )
        .unwrap();
    assert!(
        report
            .retrievers
            .iter()
            .any(|retriever| retriever.segmented_lexical_projection_used),
        "allocation evidence must execute the persisted lexical path"
    );
    assert_eq!(report.total_hits, expected_hits);
    report
        .hits
        .into_iter()
        .map(|hit| (hit.id, hit.score))
        .collect()
}

#[test]
fn retained_base_terms_do_not_multiply_read_update_delete_allocations() {
    let mut measurements = Vec::new();
    for terms in [64, 4096] {
        let fixture = Fixture::new(terms);
        let mut index = SearchIndex::open(&fixture.0).unwrap();
        let mut content = String::new();
        for ordinal in 0..terms {
            write!(&mut content, "token{ordinal:05} ").unwrap();
        }
        // The query term sorts after the base terms, so unrelated posting
        // block decoding does not grow with the historical term dictionary.
        index.upsert(document(content)).unwrap();
        index.checkpoint().unwrap();
        index.upsert(document("zzreplacement".into())).unwrap();
        let expected = scores(&index, 1);

        let (actual, read_bytes) = measure(|| scores(&index, 1));
        assert_eq!(actual, expected);
        let replacement = document("zzreplacement".into());
        let (result, update_bytes) = measure(|| index.upsert(replacement));
        result.unwrap();
        assert_eq!(scores(&index, 1), expected);
        let (_, delete_bytes) = measure(|| index.delete("document"));
        assert!(scores(&index, 0).is_empty());
        let replacement = document("zzreplacement".into());
        let (result, restore_bytes) = measure(|| index.upsert(replacement));
        result.unwrap();
        assert_eq!(scores(&index, 1), expected);

        index.checkpoint().unwrap();
        drop(index);
        let reopened = SearchIndex::open(&fixture.0).unwrap();
        assert_eq!(scores(&reopened, 1), expected);

        let allocated = [read_bytes, update_bytes, delete_bytes, restore_bytes];
        println!("base_terms={terms} requested_bytes={allocated:?}");
        measurements.push(allocated);
    }
    // The live documents and results are identical. Growing only the retained
    // historical term set must not add a document-sized temporary copy.
    for (operation, small, large) in ["read", "update", "delete", "restore"]
        .into_iter()
        .zip(measurements[0])
        .zip(measurements[1])
        .map(|((operation, small), large)| (operation, small, large))
    {
        assert!(
            large <= small + 8192,
            "{operation}: small={small}, large={large}"
        );
    }
}
