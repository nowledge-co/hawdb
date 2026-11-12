// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `FROM text_search(index, query, top_k => k)`: BM25 over an engine-owned
//! search index. Each query term's dictionary row and posting blocks are read
//! from the index relation in the same snapshot as the source rows, scored
//! exactly with per-document lengths, and hydrated in descending
//! score order until `k` rows satisfy WHERE. Projection, ORDER BY, LIMIT and
//! OFFSET then apply to those rows.

use super::vector_index::{
    execute_candidate_select, validate_candidate_select, CandidateScore, VectorIndexCandidates,
    VectorIndexInput, VectorIndexSqlExecution,
};
use super::*;
use hawdb_sql::SqlTextSearch;
use hawdb_storage::relational::{
    decode_search_sealed_block, search_index_block_fence, search_index_dictionary_key,
    search_index_length_block_range, search_index_length_block_start,
    search_index_sealed_block_range, search_index_statistics_key, SearchIndexAnalyzer,
    SearchIndexStatistics, SearchLengthBlock, SearchPosting, SearchTermDictionary,
    SEARCH_INDEX_COLUMN_COUNT, SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
};
use std::collections::btree_map::Entry;

/// Okapi BM25 with the conventional Lucene defaults.
const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
pub const TEXT_SEARCH_SCORE_COLUMN: &str = "score";
/// Upper bound on `top_k`; larger result sets should page with OFFSET.
pub const TEXT_SEARCH_MAX_TOP_K: usize = 10_000;

/// Estimated resident bytes of one entry of a B-tree map or set: twice the
/// entry (nodes other than the root are at least half full) plus headers.
const fn btree_entry_bytes<K, V>() -> usize {
    2 * (std::mem::size_of::<K>() + std::mem::size_of::<V>()) + 16
}

fn semantic(message: impl Into<String>) -> HawDBError {
    HawDBError::Semantic(message.into())
}

fn bind<'a>(value: &'a SqlValue, parameters: &'a [Value], name: &str) -> Result<&'a Value> {
    match value {
        SqlValue::Literal(value) => Ok(value),
        SqlValue::Parameter(position) => position
            .checked_sub(1)
            .and_then(|position| parameters.get(position))
            .ok_or_else(|| semantic(format!("missing text_search {name} parameter"))),
        SqlValue::Expression(_) => Err(semantic(format!(
            "text_search {name} must be a literal or parameter"
        ))),
    }
}

struct BoundTextSearch<'a> {
    index: &'a str,
    query: &'a str,
    top_k: usize,
}

fn bind_call<'a>(call: &'a SqlTextSearch, parameters: &'a [Value]) -> Result<BoundTextSearch<'a>> {
    let Value::String(index) = bind(&call.index, parameters, "index")? else {
        return Err(semantic("text_search index must be a string"));
    };
    let Value::String(query) = bind(&call.query, parameters, "query")? else {
        return Err(semantic("text_search query must be a string"));
    };
    let top_k = match bind(&call.top_k, parameters, "top_k")? {
        Value::Int(value) => usize::try_from(*value)
            .ok()
            .filter(|value| (1..=TEXT_SEARCH_MAX_TOP_K).contains(value)),
        _ => None,
    }
    .ok_or_else(|| {
        semantic(format!(
            "text_search top_k must be an integer between 1 and {TEXT_SEARCH_MAX_TOP_K}"
        ))
    })?;
    Ok(BoundTextSearch {
        index,
        query,
        top_k,
    })
}

struct RankedCandidates(Vec<(String, f64)>);

impl VectorIndexCandidates for RankedCandidates {
    fn row_count(&self) -> usize {
        self.0.len()
    }

    fn row(&self, ordinal: usize) -> Result<(&str, f64)> {
        let (identity, score) = self
            .0
            .get(ordinal)
            .ok_or_else(|| HawDBError::Execution("text_search ordinal is out of range".into()))?;
        Ok((identity, *score))
    }
}

fn identity(document: &[u8]) -> String {
    use std::fmt::Write;
    let mut identity = String::with_capacity(document.len().saturating_mul(2).saturating_add(2));
    identity.push_str("r:");
    for byte in document {
        write!(&mut identity, "{byte:02x}").expect("String write");
    }
    identity
}

fn integrity(error: hawdb_storage::relational::RelationalError) -> HawDBError {
    HawDBError::StorageIntegrity(error.to_string())
}

fn data(row: &RelationalReadRow) -> Result<&[u8]> {
    match row.value(3)? {
        RelationalValue::Bytea(data) => Ok(data),
        _ => Err(HawDBError::StorageIntegrity(
            "search index row has an invalid payload".into(),
        )),
    }
}

/// Exact BM25 scores of every document containing at least one query term,
/// in descending score order with ties broken by encoded primary key.
fn rank<R: RelationalRowStoreReader>(
    state: &RelationalState,
    index: &str,
    analyzer: SearchIndexAnalyzer,
    query: &str,
    row_mode: RelationalRowReadMode<'_, R>,
    resources: &RelationalQueryResourceContext<'_>,
    ledger: &QueryMemoryLedger,
) -> Result<(Vec<(String, f64)>, RelationalRowExecutionEvidence)> {
    let default_task = hawdb_core::RuntimeTaskContext::default();
    let task = resources.task_context.unwrap_or(&default_task);
    let runtime = row_mode.open_runtime(
        state,
        crate::field_plan::RelationalFieldPlan::all_columns(index, SEARCH_INDEX_COLUMN_COUNT),
        resources.limits.row_read,
        resources.limits.hydration,
        task,
    )?;
    let Some(statistics) = runtime.read_point(index, &search_index_statistics_key())? else {
        return Ok((Vec::new(), runtime.evidence()));
    };
    let statistics = SearchIndexStatistics::decode(data(&statistics)?).map_err(integrity)?;
    if statistics.documents == 0 || statistics.tokens == 0 {
        return Ok((Vec::new(), runtime.evidence()));
    }
    let documents = statistics.documents as f64;
    let average_length = statistics.tokens as f64 / documents;
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "TextSearch scores",
        resources.execution_memory.blocking_operator_bytes,
    );
    let mut memory = account.reserve(0)?;
    // Query terms, the needed-lengths set and the score map are admitted
    // before each allocation, like the postings and identities below.
    let terms = analyzer.admitted_query_terms(query, |bytes| memory.grow(bytes))?;
    memory.grow(
        terms
            .len()
            .saturating_mul(std::mem::size_of::<(f64, Vec<SearchPosting>)>()),
    )?;
    let mut work = 0usize;
    let max_work = resources.limits.max_candidate_work;
    let mut charge_work = |amount: usize| {
        work = work.saturating_add(amount);
        if work > max_work {
            return Err(HawDBError::read_budget_exceeded(
                hawdb_core::ReadBudgetResource::Rows,
                max_work,
                "text_search postings exceed SQL candidate work admission",
            ));
        }
        Ok(())
    };
    // Phase 1: every query term's postings and IDF, in query-term order.
    let mut gathered = Vec::<(f64, Vec<SearchPosting>)>::with_capacity(terms.len());
    let mut needed = BTreeSet::<u64>::new();
    for term in &terms {
        hawdb_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        let Some(row) = runtime.read_point(index, &search_index_dictionary_key(term))? else {
            continue;
        };
        let dictionary = SearchTermDictionary::decode(data(&row)?).map_err(integrity)?;
        // The dictionary carries the document frequency, so an over-budget
        // term is refused before any of its blocks is read.
        let frequency = usize::try_from(dictionary.document_frequency).unwrap_or(usize::MAX);
        charge_work(frequency)?;
        memory.grow(frequency.saturating_mul(std::mem::size_of::<SearchPosting>()))?;
        let mut postings = Vec::with_capacity(frequency);
        if dictionary.sealed_blocks > 0 {
            let mut blocks = 0u64;
            runtime.visit_primary_key_range(
                index,
                &search_index_sealed_block_range(term).map_err(integrity)?,
                |row| {
                    blocks += 1;
                    let fence = search_index_block_fence(row.value(2)?).map_err(integrity)?;
                    decode_search_sealed_block(fence, data(&row)?, &mut postings)
                        .map_err(integrity)?;
                    Ok(true)
                },
            )?;
            if blocks != dictionary.sealed_blocks {
                return Err(HawDBError::StorageIntegrity(
                    "search index sealed block count disagrees with its dictionary".into(),
                ));
            }
        }
        postings.extend_from_slice(&dictionary.tail);
        if postings.len() as u64 != dictionary.document_frequency
            || postings
                .windows(2)
                .any(|pair| pair[0].ordinal >= pair[1].ordinal)
        {
            return Err(HawDBError::StorageIntegrity(
                "search index postings disagree with their document frequency".into(),
            ));
        }
        if postings.is_empty() {
            continue;
        }
        // Ordinals ascend within a term, so each block is checked once.
        let mut previous = None;
        for posting in &postings {
            let block = posting.ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS;
            if previous != Some(block) {
                previous = Some(block);
                if !needed.contains(&block) {
                    memory.grow(btree_entry_bytes::<u64, ()>())?;
                    needed.insert(block);
                }
            }
        }
        let frequency = postings.len() as f64;
        let idf = (1.0 + (documents - frequency + 0.5) / (frequency + 0.5)).ln();
        gathered.push((idf, postings));
    }
    let (Some(&first), Some(&last)) = (needed.first(), needed.last()) else {
        return Ok((Vec::new(), runtime.evidence()));
    };
    // Phase 2: the lengths rows of every scored document, in one ordered
    // range scan rather than one point read per row.
    let mut lengths = BTreeMap::<u64, SearchLengthBlock>::new();
    runtime.visit_primary_key_range(
        index,
        &search_index_length_block_range(
            first * SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
            last * SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS,
        )
        .map_err(integrity)?,
        |row| {
            charge_work(1)?;
            let block = search_index_length_block_start(row.value(2)?).map_err(integrity)?
                / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS;
            if needed.contains(&block) {
                let data = data(&row)?;
                memory.grow(data.len().saturating_mul(2).saturating_add(96))?;
                lengths.insert(block, SearchLengthBlock::decode(data).map_err(integrity)?);
            }
            Ok(true)
        },
    )?;
    let entry = |ordinal: u64| {
        lengths
            .get(&(ordinal / SEARCH_INDEX_LENGTH_BLOCK_DOCUMENTS))
            .and_then(|block| block.get(ordinal))
            .ok_or_else(|| {
                HawDBError::StorageIntegrity("search index posting names an absent document".into())
            })
    };
    // Phase 3: scores, accumulated per document in query-term order with the
    // same floating-point operations as a brute-force evaluation.
    let mut scores = BTreeMap::<u64, f64>::new();
    for (idf, postings) in &gathered {
        hawdb_executor::pipeline::runtime_checkpoint(resources.task_context)?;
        for posting in postings {
            let length = entry(posting.ordinal)?.length;
            let term_frequency = posting.frequency as f64;
            let normalization = BM25_K1 * (1.0 - BM25_B + BM25_B * length as f64 / average_length);
            let contribution =
                idf * term_frequency * (BM25_K1 + 1.0) / (term_frequency + normalization);
            let score = match scores.entry(posting.ordinal) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    // A document's first matching term admits its map entry.
                    memory.grow(btree_entry_bytes::<u64, f64>())?;
                    entry.insert(0.0)
                }
            };
            *score += contribution;
        }
    }
    drop(gathered);
    memory.grow(
        scores
            .len()
            .saturating_mul(std::mem::size_of::<(String, f64)>()),
    )?;
    let mut ranked = Vec::with_capacity(scores.len());
    for (ordinal, score) in scores {
        let document = &entry(ordinal)?.key;
        memory.grow(document.len().saturating_mul(2).saturating_add(2))?;
        ranked.push((identity(document), score));
    }
    // Hex identities preserve the encoded-key byte order used for ties.
    ranked.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    Ok((ranked, runtime.evidence()))
}

fn output_schema(select: &SelectStatement, source: &RelationalTableSchema) -> Result<QuerySchema> {
    let mut columns = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectProjection::Wildcard => {
                columns.extend(source.columns.iter().map(|column| column.name.clone()));
                columns.push(TEXT_SEARCH_SCORE_COLUMN.to_owned());
            }
            SelectProjection::Expression { expression, alias } => {
                columns.push(
                    alias
                        .clone()
                        .unwrap_or_else(|| crate::query_value::expression_name(expression)),
                );
            }
        }
    }
    QuerySchema::try_new(columns)
}

pub(super) fn execute_text_search_select<R: RelationalQueryStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<RelationalQueryOutput> {
    let call = select.text_search().expect("text_search source");
    if select.vector_index().is_some() {
        return Err(semantic(
            "text_search cannot be combined with vector_index_search",
        ));
    }
    validate_candidate_select(select, "text_search")?;
    let bound = bind_call(call, parameters)?;
    let index_schema = state
        .table_schema(bound.index)
        .ok_or_else(|| semantic(format!("unknown search index {}", bound.index)))?;
    let (Some(spec), Some(definition)) = (
        index_schema.search_index(),
        index_schema.materialized_view.as_deref(),
    ) else {
        return Err(semantic(format!("{} is not a search index", bound.index)));
    };
    let source = state.table_schema(&definition.source).ok_or_else(|| {
        HawDBError::StorageIntegrity("search index source table is absent".into())
    })?;
    let schema = output_schema(select, source)?;
    let ledger = QueryMemoryLedger::new(hawdb_executor::memory::enforced_query_memory_budget(
        resources.execution_memory,
        resources.task_context,
    )?);
    let (ranked, index_evidence) = rank(
        state,
        bound.index,
        spec.analyzer,
        bound.query,
        read_modes.row,
        &resources,
        &ledger,
    )?;
    // Keep the rank scan's evidence on the output even when nothing matched.
    let candidate_count = ranked.len();
    let candidates = RankedCandidates(ranked);
    let rows = execute_candidate_select(
        select,
        parameters,
        VectorIndexInput {
            table: &definition.source,
            candidates: &candidates,
        },
        VectorIndexSqlExecution {
            state,
            row_mode: read_modes.row,
            resources,
            memory_ledger: &ledger,
        },
        CandidateScore {
            function: "text_search",
            context: "text_search",
            column: TEXT_SEARCH_SCORE_COLUMN,
            valid: |score| score.is_finite() && score >= 0.0,
            invalid: "invalid text_search BM25 score",
            absent: "text_search candidate is absent from the bound SQL snapshot",
            max_accepted: Some(bound.top_k),
        },
    )?;
    let (rows, mut row_execution) = rows.into_parts();
    row_execution.rows_visited = row_execution
        .rows_visited
        .saturating_add(index_evidence.rows_visited);
    row_execution.logical_pages = row_execution
        .logical_pages
        .saturating_add(index_evidence.logical_pages);
    row_execution.logical_bytes = row_execution
        .logical_bytes
        .saturating_add(index_evidence.logical_bytes);
    Ok(RelationalQueryOutput {
        rows: QueryRows::try_from_named_rows(schema, rows)?,
        stage_timings,
        join_planning: RelationalJoinPlanningOutcome::not_eligible(
            RelationalJoinPlanningReason::NoJoin,
            Vec::new(),
            RelationalJoinEnumerationConfig::default(),
        ),
        operator_cardinality_profiles: Vec::new(),
        intermediate_rows: candidate_count,
        hydration: resources.limits.hydration,
        access_path: RelationalAccessPathDescriptor {
            kind: RelationalAccessPathKind::PrimaryKeyRange,
            name: format!("text_search({})", bound.index),
            index_columns: Vec::new(),
            access_columns: BTreeSet::new(),
            equality_prefix_len: 1,
            order_prefix_len: 0,
            exclusive_range: false,
            range: None,
            reverse_order: false,
            unique_point: false,
            covering: false,
            requires_row_fetch: true,
            estimated_rows: candidate_count,
        },
        join_access_paths: Vec::new(),
        index_execution_evidence: Vec::new(),
        row_execution_evidence: row_execution,
        blocking_operator_memory_reports: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identities_round_trip_through_primary_key_encoding() {
        let key = hawdb_storage::relational::RelationalKey(vec![RelationalValue::BigInt(7)]);
        let encoded = hawdb_storage::relational::encode_relational_primary_key(&key).unwrap();
        let identity = identity(&encoded);
        assert!(identity.starts_with("r:"));
        assert_eq!(identity.len(), 2 + encoded.len() * 2);
    }
}
