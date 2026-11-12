// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `text_search` admits its scan state before allocating it, and that state
//! is bounded by `k` and the blocks it reads, not by the number of matches.

use super::*;

const DOCUMENTS: i64 = 4096;

fn search_state() -> RelationalState {
    search_state_of(DOCUMENTS)
}

fn search_state_of(documents: i64) -> RelationalState {
    let mut state = RelationalState::default();
    let values = (0..documents)
        .map(|id| {
            // Every document matches. The first twelve and every 331st
            // repeat the term, so after the first block the top-10 floor
            // lies above every block without a repeat.
            let repeats = if id < 12 || id % 331 == 0 {
                " common common"
            } else {
                ""
            };
            format!("({id}, 'common d{id}{repeats}')")
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
    context: Option<&hawdb_core::RuntimeTaskContext>,
) -> Result<RelationalQueryOutput> {
    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(blocking_operator_bytes).unwrap(),
        ..Default::default()
    };
    execute_relational_query_sql_with_runtime(
        "SELECT id, score FROM text_search('docs_body', 'common', top_k => 10)",
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
fn text_search_scan_state_is_bounded_by_k_not_by_matches() {
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
    let budget = 320 * 1024;
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
fn text_search_observes_cancellation() {
    let state = search_state();
    let cancellation = hawdb_core::RuntimeCancellationToken::new();
    let context = hawdb_core::RuntimeTaskContext::without_deadline(cancellation.clone());
    cancellation.cancel();
    let error = search(&state, 64 * 1024 * 1024, Some(&context)).unwrap_err();
    assert!(error.to_string().contains("cancelled"), "{error}");
}
