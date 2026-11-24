// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use hawdb_executor::{QueryMemoryClass, QueryMemoryLedger};

fn terms() -> BTreeSet<String> {
    ["rare", "storage", "unique0007", "unique1000", "absent"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

struct Observation {
    scores: Vec<(String, u64)>,
    matches: usize,
    postings: u64,
    posting_bytes: u64,
    peak: usize,
}

fn observe(reader: &LexicalProjectionReader, enabled: bool, budget: usize) -> Observation {
    let terms = terms();
    let task = RuntimeTaskContext::default();
    let grant = NonZeroUsize::new(budget).unwrap();
    let ledger = QueryMemoryLedger::new(grant);
    let account = ledger.account(QueryMemoryClass::ExternalRead, "posting cache test", grant);
    let context = QueryContext {
        memory: &account,
        task: &task,
        execution: None,
        io_scope: None,
    };
    let cache = PostingBlockCache::new(if enabled { budget } else { 0 });
    let statistics = AccountedCorpusStatistics::aggregate_cached(
        [reader],
        &terms,
        reader.config.max_term_bytes,
        context,
        Some(&cache),
    )
    .unwrap();
    let delta = LexicalMiniDelta::default();
    let report = reader
        .score_terms_with_stable_filter(
            &terms,
            ScoringInputs {
                delta: &delta,
                max_term_bytes: reader.config.max_term_bytes,
                retained_score_limit: Some(10),
                complete_candidate_limit: None,
                global_statistics: Some(&statistics.statistics),
                prune_blocks: false,
                context: Some(context),
                posting_cache: Some(&cache),
            },
            |id| Ok(!id.ends_with("0007")),
        )
        .unwrap();
    let observation = Observation {
        scores: report
            .report
            .scores
            .iter()
            .map(|(id, score)| (id.clone(), score.to_bits()))
            .collect(),
        matches: report.report.matching_document_count,
        postings: report.report.postings_visited,
        posting_bytes: report.report.bytes_read - report.report.document_bytes_read,
        peak: ledger.snapshot().peak_bytes,
    };
    drop(report);
    drop(statistics);
    drop(cache);
    assert_eq!(ledger.snapshot().used_bytes, 0);
    observation
}

#[test]
fn statistics_and_scoring_share_verified_blocks_without_changing_hits() {
    let root = projection_root("query-posting-cache");
    fs::create_dir_all(&root).unwrap();
    let reader = super::block_ranges::many_block_reader(&root);
    let baseline = observe(&reader, false, 16 * 1024 * 1024);
    let cached = observe(&reader, true, 16 * 1024 * 1024);
    assert_eq!(cached.scores, baseline.scores);
    assert_eq!(cached.matches, baseline.matches);
    assert_eq!(cached.postings, baseline.postings);
    assert!(baseline.posting_bytes > 0);
    assert_eq!(cached.posting_bytes, 0);
    // Optional cache ownership must yield to the former exact working budget.
    let tight = observe(&reader, true, baseline.peak);
    assert_eq!(tight.scores, baseline.scores);
    assert_eq!(tight.matches, baseline.matches);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cached_block_identity_binds_reader_and_integrity_and_still_checks_cancellation() {
    let root = projection_root("query-posting-cache-identity");
    fs::create_dir_all(&root).unwrap();
    let reader = super::block_ranges::many_block_reader(&root);
    let cache = PostingBlockCache::new(16 * 1024 * 1024);
    let block = &reader.manifest.blocks[reader.posting_block_range("storage").start];
    let mut first = PostingReadBuffer::default();
    assert_eq!(
        first
            .read_cached(&reader, block, None, Some(&cache), None)
            .unwrap()
            .1,
        block.length
    );
    let mut second = PostingReadBuffer::default();
    assert_eq!(
        second
            .read_cached(&reader, block, None, Some(&cache), None)
            .unwrap()
            .1,
        0
    );
    let mut corrupt = block.clone();
    corrupt.checksum ^= 1;
    assert!(second
        .read_cached(&reader, &corrupt, None, Some(&cache), None)
        .is_err());
    let token = hawdb_core::RuntimeCancellationToken::new();
    let task = RuntimeTaskContext::without_deadline(token.clone());
    token.cancel();
    assert!(second
        .read_cached(&reader, block, Some(&task), Some(&cache), None)
        .is_err());
    let other_root = projection_root("query-posting-cache-other");
    fs::create_dir_all(&other_root).unwrap();
    let other = super::block_ranges::many_block_reader(&other_root);
    assert!(cache.get(&other, block).is_none());
    cache.clear();
    assert!(cache.get(&reader, block).is_none());
    fs::remove_dir_all(root).unwrap();
    fs::remove_dir_all(other_root).unwrap();
}
