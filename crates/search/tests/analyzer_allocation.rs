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

//! Measure the real generation writer's rejected identifier path.

#[path = "support/allocation.rs"]
mod allocation;

use hawdb_search::{
    SearchAnalyzerLexicon, SearchDocument, SearchOutOfCoreGenerationBuildOptions,
    SearchOutOfCoreGenerationWriter,
};
use std::collections::BTreeMap;

#[test]
fn rejected_identifier_stops_before_collecting_all_parts_and_tokens() {
    let mut measurements = Vec::new();
    for repeats in [1024, 16384] {
        let source = "alphaBeta".repeat(repeats);
        let source_bytes = source.len();
        let root = std::env::temp_dir().join(format!(
            "hawdb-identifier-allocation-{}-{repeats}",
            std::process::id(),
        ));
        let mut writer = SearchOutOfCoreGenerationWriter::create(
            &root,
            SearchOutOfCoreGenerationBuildOptions {
                analyzer_lexicon: SearchAnalyzerLexicon::empty(),
                ..Default::default()
            },
        )
        .unwrap();
        writer
            .push(SearchDocument {
                id: "oversized-identifier".to_string(),
                title: String::new(),
                content: source,
                embedding: None,
                metadata: BTreeMap::new(),
            })
            .unwrap();
        let (result, requested_bytes) = allocation::measure(|| writer.finish());
        let error = result.unwrap_err().to_string();
        assert!(error.contains("lexical term uses"), "{error}");
        assert!(error.contains("exceeding 4096"), "{error}");
        assert!(!root
            .join("search_projection.out_of_core.manifest.hawdb")
            .exists());
        println!("source_bytes={source_bytes} requested_bytes={requested_bytes}");
        measurements.push((source_bytes, requested_bytes));
        std::fs::remove_dir_all(root).unwrap();
    }
    let source_growth = measurements[1].0 - measurements[0].0;
    let allocation_growth = measurements[1].1 - measurements[0].1;
    assert!(
        allocation_growth <= 8 * source_growth + 64 * 1024,
        "rejected identifier allocated its whole token expansion: {measurements:?}"
    );
}
