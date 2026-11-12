// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `text_search` retains only its admitted top-k score state.

use super::projection_admission::SizeProbe;
use super::*;

const DOCUMENTS: i64 = 4096;

fn search_state() -> RelationalState {
    let mut state = RelationalState::default();
    let values = (0..DOCUMENTS)
        .map(|id| {
            let body = if id < 10 {
                format!("{} top{id}", "common ".repeat(32))
            } else {
                format!("common {} d{id}", "filler ".repeat(32))
            };
            format!("({id}, '{body}')")
        })
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
) -> (Result<RelationalQueryOutput>, usize) {
    search_sql(
        state,
        blocking_operator_bytes,
        "SELECT id, score FROM text_search('docs_body', 'common', top_k => 10)",
    )
}

fn search_sql(
    state: &RelationalState,
    blocking_operator_bytes: usize,
    sql: &str,
) -> (Result<RelationalQueryOutput>, usize) {
    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(blocking_operator_bytes).unwrap(),
        ..Default::default()
    };
    // Count the allocations shaped like one leaf node of the score map.
    let probe = SizeProbe::start(SizeProbe::score_leaf_bytes());
    let result = execute_relational_query_sql_with_runtime(
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
        None,
    );
    (result, probe.finish())
}

#[test]
fn text_search_wand_matches_exhaustive_multi_term_scores_at_block_boundaries() {
    let state = search_state();
    let sql = "SELECT id, score FROM text_search('docs_body', 'common top0', top_k => 10)";
    let (wand, _) = search_sql(&state, 64 * 1024 * 1024, sql);
    // A residual predicate selects the exhaustive correctness path while
    // preserving every row, making it an independent score-all oracle.
    let (exhaustive, _) = search_sql(&state, 64 * 1024 * 1024, &format!("{sql} WHERE id >= 0"));
    let wand = wand.unwrap();
    let exhaustive = exhaustive.unwrap();
    assert_eq!(wand.rows, exhaustive.rows);
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
    let state = search_state();
    let (control, control_nodes) = search(&state, 64 * 1024 * 1024);
    let control = control.unwrap();
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
    // The old score-all path allocated one B-tree entry per matching document
    // and refused this budget. WAND keeps only the stream workspace and top 10.
    let budget = DOCUMENTS as usize * 56;
    let (bounded, bounded_nodes) = search(&state, budget);
    assert_eq!(bounded.unwrap().rows.len(), 10);
    assert!(
        bounded_nodes <= control_nodes.saturating_add(1),
        "bounded query allocated {bounded_nodes} score-shaped nodes versus {control_nodes}"
    );
}
