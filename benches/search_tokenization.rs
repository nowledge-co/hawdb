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

use hawdb::{SearchAnalyzerLexicon, SearchIndex, SearchMode};
use serde_json::json;
use std::time::Instant;

const DEFAULT_CJK_CHARS: usize = if cfg!(debug_assertions) {
    10_000
} else {
    400_000
};
/// Default `max_lexical_query_analyzed_terms`; longer queries are refused.
const ANALYZED_QUERY_TERM_CAP: usize = 1024;

fn main() -> hawdb::Result<()> {
    let character_count = std::env::var("HAWDB_SEARCH_TOKENIZATION_BENCH_CJK_CHARS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_CJK_CHARS);
    let query = cjk_query(character_count);

    // Throughput guard: analyze the complete query, independent of the
    // query-admission cap, so a superlinear segmentation regression shows.
    let analyzer = SearchAnalyzerLexicon::default();
    let started = Instant::now();
    let analyzed_terms = analyzer.analyzed_query_term_count(&query);
    let elapsed = started.elapsed();

    // Search admission stops analyzing once the analyzed-term cap is passed.
    let index = SearchIndex::in_memory();
    let started = Instant::now();
    let search = index.search(&query, None, SearchMode::Text, 10);
    let search_elapsed = started.elapsed();
    let refused = analyzed_terms > ANALYZED_QUERY_TERM_CAP;
    match search {
        Ok(hits) => assert!(!refused && hits.is_empty()),
        Err(error) => assert!(
            refused && error.to_string().contains("max_query_analyzed_terms"),
            "unexpected search error: {error}"
        ),
    }
    println!(
        "search_tokenization {}",
        json!({
            "cjk_character_count": character_count,
            "query_bytes": query.len(),
            "analyzed_term_count": analyzed_terms,
            "elapsed_millis": elapsed.as_millis(),
            "characters_per_second": character_count as f64 / elapsed.as_secs_f64(),
            "search_refused": refused,
            "search_elapsed_millis": search_elapsed.as_millis(),
        })
    );
    Ok(())
}

fn cjk_query(character_count: usize) -> String {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    (0..character_count)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            char::from_u32(0x4e00 + (state % (0x9fff - 0x4e00 + 1)) as u32)
                .expect("generated code point must be a CJK character")
        })
        .collect()
}
