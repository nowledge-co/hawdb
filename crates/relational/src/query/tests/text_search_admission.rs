// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `text_search` admits skip metadata, ranked prefixes and bounded block and
//! lengths windows before allocating their retained state.

use super::projection_admission::PayloadProbe;
use super::*;

const DOCUMENTS: i64 = 4096;

fn search_state() -> RelationalState {
    search_state_of(DOCUMENTS)
}

fn search_state_of(documents: i64) -> RelationalState {
    state_of_bodies((0..documents).map(|id| {
        // Every document matches. The first twelve and every 331st repeat
        // the term, so after the first block the top-10 floor lies above
        // every block without a repeat.
        let repeats = if id < 12 || id % 331 == 0 {
            " common common"
        } else {
            ""
        };
        (id, format!("common d{id}{repeats}"))
    }))
}

/// Ten short documents repeat `common` 32 times and carry their own term;
/// every later document mentions `common` once among long filler.
fn wand_state() -> RelationalState {
    state_of_bodies((0..DOCUMENTS).map(|id| {
        let body = if id < 10 {
            format!("{} top{id}", "common ".repeat(32))
        } else {
            format!("common {} d{id}", "filler ".repeat(32))
        };
        (id, body)
    }))
}

fn state_of_bodies(bodies: impl Iterator<Item = (i64, String)>) -> RelationalState {
    let mut state = RelationalState::default();
    let values = bodies
        .map(|(id, body)| format!("({id}, '{body}')"))
        .collect::<Vec<_>>()
        .join(", ");
    for sql in [
        "CREATE TABLE docs (id BIGINT PRIMARY KEY, body TEXT)".to_owned(),
        "CREATE SEARCH INDEX docs_body ON docs (body)".to_owned(),
        format!("INSERT INTO docs (id, body) VALUES {values}"),
    ] {
        let transaction = compile_relational_statement_sql(&sql, &[], &state).unwrap();
        state = state
            .stage_transaction(transaction, Default::default(), Default::default())
            .unwrap();
    }
    state
}

fn search(
    state: &RelationalState,
    blocking_operator_bytes: usize,
    context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<RelationalQueryOutput> {
    search_sql(
        state,
        blocking_operator_bytes,
        context,
        "SELECT id, score FROM text_search('docs_body', 'common', top_k => 10)",
    )
}

fn search_sql(
    state: &RelationalState,
    blocking_operator_bytes: usize,
    context: Option<&hawdb_core::RuntimeTaskContext>,
    sql: &str,
) -> Result<RelationalQueryOutput> {
    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(blocking_operator_bytes).unwrap(),
        ..Default::default()
    };
    execute_relational_query_sql_with_runtime(
        sql,
        &[],
        state,
        RelationalQueryReadModes::new(
            RelationalIndexReadMode::<crate::RelationalMaterializedReader>::Materialized,
            RelationalRowReadMode::<crate::RelationalMaterializedReader>::CanonicalMemory,
        ),
        RelationalQueryLimits {
            max_output_rows: 1024,
            max_output_payload_bytes: 64 * 1024 * 1024,
            max_intermediate_rows: 1 << 20,
            max_candidate_work: 1 << 20,
            ..batched_index_join_limits()
        },
        &memory,
        context,
    )
}

#[test]
fn text_search_scan_admits_metadata_and_bounded_ranking() {
    let state = search_state();
    let control = search(&state, 64 * 1024 * 1024, None).unwrap();
    assert_eq!(control.rows.len(), 10);
    let evidence = control.search_index_scan.expect("scan evidence");
    assert_eq!(evidence.postings_total, DOCUMENTS as u64);
    assert!(
        evidence.postings_visited * 2 < evidence.postings_total
            && evidence.blocks_skipped * 2 > evidence.blocks_total,
        "{evidence:?}"
    );
    // Block-Max WAND retains a 10-entry heap, one decoded block per term,
    // a bounded read-ahead and lengths window: one fixed budget serves four
    // times as many matches. (The full-ranking scan needed more than 56
    // bytes per match: about 229 KB here, 917 KB for the larger corpus.)
    // Decoded length entries, including deleted slots, and the window's
    // fixed directory are charged independently of their compact encoding.
    let budget = 512 * 1024;
    let bounded = search(&state, budget, None).unwrap();
    assert_eq!(bounded.rows, control.rows);
    let larger = search_state_of(4 * DOCUMENTS);
    let evidence = search(&larger, budget, None)
        .unwrap()
        .search_index_scan
        .unwrap();
    assert_eq!(evidence.postings_total, 4 * DOCUMENTS as u64);
    // Below the scan's fixed state it is refused by its own account, before
    // that state is allocated.
    let error = search(&state, 512, None).unwrap_err().to_string();
    assert!(error.contains("Bm25IndexScan"), "{error}");
}

#[test]
fn text_search_admits_sparse_lengths_before_decoding_the_window() {
    use hawdb_storage::relational::{
        RelationalKey, RelationalTransaction, RelationalWrite, SearchDocumentEntry,
        SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
    };

    let state = state_of_bodies((0..2048).map(|id| (id, "common".to_owned())));
    // Keep only the last slot of each 64-document lengths row. Its compact
    // payload is small, but decoding still allocates all 64 entry slots.
    let state = state
        .stage_transaction(
            RelationalTransaction {
                writes: vec![RelationalWrite::DeleteByPrimaryKey {
                    table: "docs".into(),
                    keys: (0..2048)
                        .filter(|id| id % 64 != 63)
                        .map(|id| RelationalKey(vec![RelationalValue::BigInt(id)]))
                        .collect(),
                }],
            },
            Default::default(),
            Default::default(),
        )
        .unwrap();
    let sql = "SELECT id FROM text_search('docs_body', 'common', top_k => 1)";
    let entries_bytes = SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS as usize
        * std::mem::size_of::<Option<SearchDocumentEntry>>();
    let probe = PayloadProbe::start(entries_bytes);
    let control = search_sql(&state, 128 * 1024, None, sql).unwrap();
    let control_allocations = probe.finish();
    assert_eq!(control.rows.len(), 1);
    assert_eq!(control.search_index_scan.unwrap().lengths_rows_read, 32);
    let probe = PayloadProbe::start(entries_bytes);
    let refused = search_sql(&state, 32 * 1024, None, sql);
    let refused_allocations = probe.finish();
    let error = refused.unwrap_err().to_string();
    assert!(error.contains("Bm25IndexScan"), "{error}");
    assert!(
        refused_allocations < control_allocations / 2,
        "refused={refused_allocations}, control={control_allocations}"
    );
}

#[test]
fn text_search_observes_cancellation() {
    let state = search_state();
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    let error = search(&state, 64 * 1024 * 1024, Some(&context)).unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
}

#[test]
fn text_search_wand_matches_exhaustive_multi_term_scores_at_block_boundaries() {
    let state = wand_state();
    let sql = "SELECT id, score FROM text_search('docs_body', 'common top0', top_k => 10)";
    let wand = search_sql(&state, 64 * 1024 * 1024, None, sql).unwrap();
    // A residual predicate that keeps every row still ranks through the
    // index. Ranking the whole match set (top_k at least the postings, with
    // an ORDER BY that is not the bare rank order, so LIMIT is not pushed
    // into the scan) is the score-all oracle.
    let kept = search_sql(
        &state,
        64 * 1024 * 1024,
        None,
        &format!("{sql} WHERE id >= 0"),
    )
    .unwrap();
    let exhaustive = search_sql(
        &state,
        64 * 1024 * 1024,
        None,
        &format!(
            "SELECT id, score FROM text_search('docs_body', 'common top0', top_k => {}) \
             ORDER BY score DESC, id LIMIT 10",
            2 * DOCUMENTS
        ),
    )
    .unwrap();
    assert_eq!(wand.rows, exhaustive.rows);
    assert_eq!(kept.rows, exhaustive.rows);
    assert!(
        wand.row_execution_evidence.postings_visited
            < exhaustive.row_execution_evidence.postings_visited,
        "wand={:#?} exhaustive={:#?}",
        wand.row_execution_evidence,
        exhaustive.row_execution_evidence
    );
    assert!(wand.row_execution_evidence.posting_blocks_skipped > 0);
}

#[test]
fn text_search_bounds_broad_term_score_state_and_skips_losing_blocks() {
    let state = wand_state();
    let control = search(&state, 64 * 1024 * 1024, None).unwrap();
    assert_eq!(control.rows.len(), 10);
    assert!(
        control.row_execution_evidence.posting_blocks_skipped > 0,
        "{:#?}",
        control.row_execution_evidence
    );
    assert!(
        control.row_execution_evidence.postings_visited < DOCUMENTS as usize,
        "{:#?}",
        control.row_execution_evidence
    );
    // A score-all ranking needed at least one 56-byte entry per matching
    // document; the scan keeps only its stream workspace and the top 10.
    let budget = DOCUMENTS as usize * 56;
    let bounded = search(&state, budget, None).unwrap();
    assert_eq!(bounded.rows, control.rows);
}
