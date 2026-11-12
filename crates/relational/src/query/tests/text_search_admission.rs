// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `text_search` admits its score state before allocating it.

use super::projection_admission::SizeProbe;
use super::*;

const DOCUMENTS: i64 = 4096;

fn search_state() -> RelationalState {
    let mut state = RelationalState::default();
    let values = (0..DOCUMENTS)
        .map(|id| format!("({id}, 'common d{id}')"))
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
    let memory = hawdb_executor::ExecutionMemoryConfig {
        blocking_operator_bytes: NonZeroUsize::new(blocking_operator_bytes).unwrap(),
        ..Default::default()
    };
    // Count the allocations shaped like one leaf node of the score map.
    let probe = SizeProbe::start(SizeProbe::score_leaf_bytes());
    let result = execute_relational_query_sql_with_runtime(
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
        None,
    );
    (result, probe.finish())
}

#[test]
fn text_search_admits_broad_term_score_state_before_allocating_it() {
    let state = search_state();
    let (control, control_nodes) = search(&state, 64 * 1024 * 1024);
    assert_eq!(control.unwrap().rows.len(), 10);
    assert!(control_nodes > 0);
    // Room for the postings, the needed lengths blocks and their rows, but
    // for only about a third of the per-document score entries. Charging
    // the score map only after building it would let it reach its full
    // size here before the identities phase refused.
    let budget = DOCUMENTS as usize * 56;
    let (refused, refused_nodes) = search(&state, budget);
    let error = refused.unwrap_err().to_string();
    assert!(error.contains("TextSearch scores"), "{error}");
    assert!(
        refused_nodes * 2 < control_nodes,
        "score map grew to {refused_nodes} of {control_nodes} leaf nodes before refusal"
    );
}
