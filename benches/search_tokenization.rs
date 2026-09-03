use serde_json::json;
use skein::{SearchIndex, SearchMode};
use std::time::Instant;

const DEFAULT_CJK_CHARS: usize = if cfg!(debug_assertions) {
    10_000
} else {
    400_000
};

fn main() {
    let character_count = std::env::var("SKEIN_SEARCH_TOKENIZATION_BENCH_CJK_CHARS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_CJK_CHARS);
    let query = cjk_query(character_count);
    let index = SearchIndex::in_memory();

    let started = Instant::now();
    let hits = index.search(&query, None, SearchMode::Text, 10);
    let elapsed = started.elapsed();

    assert!(hits.is_empty());
    println!(
        "search_tokenization {}",
        json!({
            "cjk_character_count": character_count,
            "query_bytes": query.len(),
            "elapsed_millis": elapsed.as_millis(),
            "characters_per_second": character_count as f64 / elapsed.as_secs_f64(),
        })
    );
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
