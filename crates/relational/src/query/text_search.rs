// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! `FROM text_search(index, query, top_k => k)`: the `Bm25IndexScan` access
//! path over an engine-owned search index.
//!
//! Planning binds the call to its index and decides how many accepted rows
//! the scan must produce: `top_k`, or `OFFSET + LIMIT` when the output is in
//! rank order (no `ORDER BY`, or `ORDER BY score DESC`). The scan ranks that
//! many documents exactly with the shared Block-Max WAND kernel
//! (`hawdb_executor::bm25`) over the index's block-max skip entries, so it
//! decodes only the blocks that can still reach the top-k. Candidates are
//! hydrated from the source table in rank order and checked against WHERE;
//! when some fail, the scan ranks a prefix four times as long (bounded iterative
//! over-fetch) and continues with the next candidates, so a filter never
//! requires ranking the whole match set first. Projection, ORDER BY, LIMIT
//! and OFFSET then apply to the accepted rows.

mod scan;

use super::vector_index::{
    execute_ranked_select, validate_candidate_select, CandidateIdentity, CandidateScore,
    CandidateSource, VectorIndexSqlExecution,
};
use super::*;
use hawdb_executor::bm25::Bm25Hit;
use hawdb_sql::{ExprKind, SqlOrderDirection, SqlTextSearch};
use hawdb_storage::relational::{SearchIndexAnalyzer, SEARCH_INDEX_COLUMN_COUNT};
pub use scan::SearchIndexScanEvidence;
use scan::{rank_top_k, ScanContext, TermPostings};

pub const TEXT_SEARCH_SCORE_COLUMN: &str = "score";
/// Upper bound on `top_k`; larger result sets should page with OFFSET.
pub const TEXT_SEARCH_MAX_TOP_K: usize = 10_000;

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

/// The physical `Bm25IndexScan` of one `text_search` call.
struct Bm25IndexScanPlan<'a> {
    index: &'a str,
    table: &'a str,
    column: &'a str,
    analyzer: SearchIndexAnalyzer,
    query: &'a str,
    top_k: usize,
    /// Accepted rows the scan must produce.
    needed: usize,
    /// `needed` came from LIMIT/OFFSET rather than `top_k`.
    limit_pushdown: bool,
    source: &'a RelationalTableSchema,
}

/// Whether the statement's output order is the scan's rank order (score
/// descending, ties by encoded primary key), so LIMIT can bound the scan.
fn output_is_rank_order(select: &SelectStatement) -> bool {
    let aliases_score = select.projection.iter().any(|item| {
        matches!(item, SelectProjection::Expression { alias: Some(alias), expression }
            if alias == TEXT_SEARCH_SCORE_COLUMN
                && !matches!(&expression.kind, ExprKind::Column(column)
                    if column.name == TEXT_SEARCH_SCORE_COLUMN))
    });
    match select.order_by.as_slice() {
        [] => true,
        [item] => {
            !aliases_score
                && item.direction == SqlOrderDirection::Desc
                && matches!(&item.expression.kind, ExprKind::Column(column)
                if column.name == TEXT_SEARCH_SCORE_COLUMN
                    && column.qualifier.as_deref().is_none_or(|qualifier| {
                        qualifier == select.from_alias.as_deref().unwrap_or("text_search")
                    }))
        }
        _ => false,
    }
}

fn plan<'a>(
    select: &'a SelectStatement,
    parameters: &'a [Value],
    state: &'a RelationalState,
) -> Result<Bm25IndexScanPlan<'a>> {
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
    let mut needed = bound.top_k;
    if output_is_rank_order(select)
        && let Some(limit) = bind_bound(select.limit, parameters, "LIMIT")?
    {
        let offset = bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0);
        let window = usize::try_from(limit.saturating_add(offset)).unwrap_or(usize::MAX);
        needed = needed.min(window);
    }
    Ok(Bm25IndexScanPlan {
        index: bound.index,
        table: &definition.source,
        column: &spec.column,
        analyzer: spec.analyzer,
        query: bound.query,
        top_k: bound.top_k,
        limit_pushdown: needed < bound.top_k,
        needed,
        source,
    })
}

/// Ranked candidates produced in rounds: each round is an exact top-k with a
/// k four times the previous one, issued only when hydrated candidates failed
/// WHERE. Rank order is total, so each round's prefix equals the previous
/// round; it is checked anyway.
/// Growth of the ranked prefix between over-fetch rounds.
const GROWTH: usize = 4;

struct RankedRounds<'a> {
    context: &'a ScanContext<'a>,
    /// The queried terms' summed document frequencies.
    matches_bound: usize,
    /// The statement has a residual WHERE.
    filtered: bool,
    terms: &'a [TermPostings],
    average_length: f64,
    needed: usize,
    k: usize,
    hits: Vec<Bm25Hit>,
    hits_bytes: usize,
    exhausted: bool,
}

impl RankedRounds<'_> {
    fn next_round(&mut self) -> Result<()> {
        // Each round re-reads what it ranks, so the prefix grows fourfold;
        // once it would cover every posting of the queried terms (an upper
        // bound on the matches), the round ranks the whole match set.
        // A residual WHERE is expected to reject candidates: its first
        // round already over-fetches by the growth factor.
        let mut k = if self.k == 0 {
            if self.filtered {
                self.needed.saturating_mul(GROWTH)
            } else {
                self.needed
            }
        } else {
            self.k.saturating_mul(GROWTH)
        };
        if k >= self.matches_bound {
            k = self.matches_bound.max(self.k.saturating_add(1));
        }
        let k = k.min(self.context.max_work.max(1));
        if k <= self.k {
            return Err(HawDBError::read_budget_exceeded(
                hawdb_core::ReadBudgetResource::Rows,
                self.context.max_work,
                "text_search candidates exceed SQL candidate work admission",
            ));
        }
        let before = self.context.memory.borrow().bytes();
        let hits = rank_top_k(self.context, self.terms, self.average_length, k)?;
        let retained = self.context.memory.borrow().bytes().saturating_sub(before);
        let consistent = hits.len() >= self.hits.len()
            && self.hits.iter().zip(&hits).all(|(old, new)| {
                old.ordinal == new.ordinal
                    && old.key == new.key
                    && old.score.to_bits() == new.score.to_bits()
            });
        if !consistent {
            return Err(HawDBError::StorageIntegrity(
                "text_search ranking rounds disagree on their common prefix".into(),
            ));
        }
        self.context
            .memory
            .borrow_mut()
            .shrink(std::mem::replace(&mut self.hits_bytes, retained));
        self.exhausted = hits.len() < k;
        self.hits = hits;
        self.k = k;
        Ok(())
    }
}

impl CandidateSource for RankedRounds<'_> {
    fn candidate(&mut self, position: usize) -> Result<Option<(CandidateIdentity<'_>, f64)>> {
        while position >= self.hits.len() {
            if self.exhausted || self.terms.is_empty() {
                return Ok(None);
            }
            self.next_round()?;
        }
        self.context.charge(1)?;
        self.context.evidence.borrow_mut().candidates_hydrated += 1;
        let hit = &self.hits[position];
        Ok(Some((CandidateIdentity::Encoded(&hit.key), hit.score)))
    }
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

fn access_path(plan: &Bm25IndexScanPlan<'_>) -> RelationalAccessPathDescriptor {
    RelationalAccessPathDescriptor {
        kind: RelationalAccessPathKind::SearchIndex,
        name: plan.index.to_owned(),
        index_columns: vec![plan.column.to_owned()],
        access_columns: BTreeSet::new(),
        equality_prefix_len: 0,
        order_prefix_len: 0,
        exclusive_range: false,
        range: None,
        reverse_order: false,
        unique_point: false,
        covering: false,
        requires_row_fetch: true,
        estimated_rows: plan.needed,
    }
}

pub(super) fn execute_text_search_select<R: RelationalQueryStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
) -> Result<RelationalQueryOutput> {
    let plan = plan(select, parameters, state)?;
    let schema = output_schema(select, plan.source)?;
    let ledger = QueryMemoryLedger::new(hawdb_executor::memory::enforced_query_memory_budget(
        resources.execution_memory,
        resources.task_context,
    )?);
    let default_task = hawdb_core::RuntimeTaskContext::default();
    let task = resources.task_context.unwrap_or(&default_task);
    let index_runtime = read_modes.row.open_runtime(
        state,
        crate::field_plan::RelationalFieldPlan::all_columns(plan.index, SEARCH_INDEX_COLUMN_COUNT),
        resources.limits.row_read,
        resources.limits.hydration,
        task,
    )?;
    let account = ledger.account(
        QueryMemoryClass::BlockingState,
        "Bm25IndexScan",
        resources.execution_memory.blocking_operator_bytes,
    );
    let context = ScanContext {
        runtime: &index_runtime,
        index: plan.index,
        task: resources.task_context,
        max_work: resources.limits.max_candidate_work,
        work: Default::default(),
        memory: std::cell::RefCell::new(account.reserve(0)?),
        evidence: std::cell::RefCell::new(SearchIndexScanEvidence {
            index: plan.index.to_owned(),
            table: plan.table.to_owned(),
            needed: plan.needed,
            limit_pushdown: plan.limit_pushdown,
            ..SearchIndexScanEvidence::default()
        }),
    };
    // Query terms are admitted before each allocation, like every other
    // piece of scan state below.
    let query_terms = plan
        .analyzer
        .admitted_query_terms(plan.query, |bytes| context.admit(bytes))?;
    let mut terms = Vec::new();
    let mut average_length = 1.0;
    if let Some(statistics) = context.statistics()?
        && statistics.documents > 0
        && statistics.tokens > 0
    {
        average_length = statistics.tokens as f64 / statistics.documents as f64;
        context.admit(
            query_terms
                .len()
                .saturating_mul(std::mem::size_of::<TermPostings>()),
        )?;
        for term in &query_terms {
            if let Some(postings) = TermPostings::load(&context, term, &statistics, average_length)?
            {
                terms.push(postings);
            }
        }
    }
    {
        let mut evidence = context.evidence.borrow_mut();
        evidence.query_terms = query_terms.len();
        evidence.matched_terms = terms.len();
        evidence.postings_total = terms.iter().map(TermPostings::document_frequency).sum();
    }
    let matches_bound = usize::try_from(context.evidence.borrow().postings_total)
        .unwrap_or(usize::MAX)
        .max(1);
    let mut rounds = RankedRounds {
        context: &context,
        matches_bound,
        filtered: select.selection.is_some(),
        terms: &terms,
        average_length,
        needed: plan.needed,
        k: 0,
        hits: Vec::new(),
        hits_bytes: 0,
        exhausted: false,
    };
    let rows = execute_ranked_select(
        select,
        parameters,
        plan.table,
        &mut rounds,
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
            valid: |score| score.is_finite() && score > 0.0,
            invalid: "invalid text_search BM25 score",
            absent: "text_search candidate is absent from the bound SQL snapshot",
            max_accepted: Some(plan.needed),
        },
    )?;
    drop(rounds);
    let (rows, mut row_execution) = rows.into_parts();
    let index_evidence = index_runtime.evidence();
    row_execution.rows_visited = row_execution
        .rows_visited
        .saturating_add(index_evidence.rows_visited);
    row_execution.logical_pages = row_execution
        .logical_pages
        .saturating_add(index_evidence.logical_pages);
    row_execution.logical_bytes = row_execution
        .logical_bytes
        .saturating_add(index_evidence.logical_bytes);
    let evidence = context.evidence.into_inner();
    Ok(RelationalQueryOutput {
        rows: QueryRows::try_from_named_rows(schema, rows)?,
        stage_timings,
        join_planning: RelationalJoinPlanningOutcome::not_eligible(
            RelationalJoinPlanningReason::NoJoin,
            Vec::new(),
            RelationalJoinEnumerationConfig::default(),
        ),
        operator_cardinality_profiles: Vec::new(),
        intermediate_rows: evidence.candidates_hydrated,
        hydration: resources.limits.hydration,
        access_path: access_path(&plan),
        join_access_paths: Vec::new(),
        index_execution_evidence: Vec::new(),
        row_execution_evidence: row_execution,
        blocking_operator_memory_reports: Vec::new(),
        search_index_scan: Some(evidence),
    })
}

/// `EXPLAIN [ANALYZE]` of a `text_search` query: the plan as operator rows,
/// with the scan's visit/skip evidence under ANALYZE.
pub(super) fn explain_text_search_select<R: RelationalQueryStoreReader>(
    select: &SelectStatement,
    parameters: &[Value],
    state: &RelationalState,
    read_modes: RelationalQueryReadModes<'_, R>,
    resources: RelationalQueryResourceContext<'_>,
    stage_timings: RelationalSqlStageTimings,
    analyze: bool,
) -> Result<RelationalQueryOutput> {
    let plan = plan(select, parameters, state)?;
    let terms = plan.analyzer.query_terms(plan.query);
    let mut output = if analyze {
        execute_text_search_select(
            select,
            parameters,
            state,
            read_modes,
            resources,
            stage_timings,
        )?
    } else {
        RelationalQueryOutput {
            rows: QueryRows::try_from_named_rows(output_schema(select, plan.source)?, Vec::new())?,
            stage_timings,
            join_planning: RelationalJoinPlanningOutcome::not_eligible(
                RelationalJoinPlanningReason::NoJoin,
                Vec::new(),
                RelationalJoinEnumerationConfig::default(),
            ),
            operator_cardinality_profiles: Vec::new(),
            intermediate_rows: 0,
            hydration: resources.limits.hydration,
            access_path: access_path(&plan),
            join_access_paths: Vec::new(),
            index_execution_evidence: Vec::new(),
            row_execution_evidence: RelationalRowExecutionEvidence::default(),
            blocking_operator_memory_reports: Vec::new(),
            search_index_scan: None,
        }
    };
    let statement_rows = output.rows.len();
    let evidence = output.search_index_scan.clone();
    let limit = bind_bound(select.limit, parameters, "LIMIT")?;
    let offset = bind_bound(select.offset, parameters, "OFFSET")?.unwrap_or(0);
    let mut nodes: Vec<crate::explain::ExplainRow> = Vec::new();
    if select.limit.is_some() || select.offset.is_some() {
        nodes.push(crate::explain::ExplainRow {
            operator: "LimitExec",
            id: "logical_limit".into(),
            estimated_rows: limit.map(|limit| usize::try_from(limit).unwrap_or(usize::MAX)),
            actual_rows: Some(statement_rows),
            access_object: String::new(),
            operator_info: format!(
                "implementation=fused, offset={offset}, count={}",
                limit.map_or_else(|| "unbounded".to_owned(), |limit| limit.to_string())
            ),
            execution_info: None,
        });
    }
    if !select.order_by.is_empty() {
        nodes.push(crate::explain::ExplainRow {
            operator: "TopNExec",
            id: "blocking_top_n".into(),
            estimated_rows: Some(plan.needed),
            actual_rows: None,
            access_object: String::new(),
            operator_info: format!(
                "order_by={}, offset={offset}, input=rank_order",
                crate::explain::explain_order_by(&select.order_by)
            ),
            execution_info: None,
        });
    }
    nodes.push(crate::explain::ExplainRow {
        operator: "ProjectionExec",
        id: "logical_projection".into(),
        estimated_rows: Some(plan.needed),
        actual_rows: None,
        access_object: String::new(),
        operator_info: format!(
            "implementation=fused, columns={}, virtual_columns=[{TEXT_SEARCH_SCORE_COLUMN}]",
            select.projection.len()
        ),
        execution_info: None,
    });
    if let Some(selection) = &select.selection {
        nodes.push(crate::explain::ExplainRow {
            operator: "SelectionExec",
            id: "logical_selection".into(),
            estimated_rows: Some(plan.needed),
            actual_rows: None,
            access_object: String::new(),
            operator_info: format!(
                "implementation=post_filter, residual_predicate={}, strategy=bounded_over_fetch, growth=4x",
                crate::explain::explain_predicate(selection)
            ),
            execution_info: None,
        });
    }
    nodes.push(crate::explain::ExplainRow {
        operator: "SearchHydrationExec",
        id: "search_hydration".into(),
        estimated_rows: Some(plan.needed),
        actual_rows: evidence
            .as_ref()
            .map(|evidence| evidence.candidates_hydrated),
        access_object: format!("table:{}", plan.table),
        operator_info: format!(
            "fetch=primary_key, order=rank, stop_after_accepted={}",
            plan.needed
        ),
        execution_info: evidence.as_ref().map(|evidence| {
            format!(
                "candidates_hydrated={}, rows_visited={}",
                evidence.candidates_hydrated, output.row_execution_evidence.rows_visited
            )
        }),
    });
    let scan_info = format!(
        "algorithm=block_max_wand, analyzer={}, terms=[{}], top_k={}, k={}, limit_pushdown={}, ties=primary_key",
        plan.analyzer.name(),
        terms.join(","),
        plan.top_k,
        plan.needed,
        plan.limit_pushdown
    );
    nodes.push(crate::explain::ExplainRow {
        operator: "Bm25IndexScan",
        id: "search_index_scan".into(),
        estimated_rows: Some(plan.needed),
        actual_rows: evidence.as_ref().map(|evidence| evidence.ranked_rows),
        access_object: format!(
            "table:{}, search_index:{}({})",
            plan.table, plan.index, plan.column
        ),
        operator_info: scan_info,
        execution_info: evidence.as_ref().map(|evidence| {
            format!(
                "rounds={}, final_k={}, ranked_rows={}, matched_terms={}/{}, postings_visited={}/{}, \
                 blocks_decoded={}, blocks_read={}, blocks_skipped={}, blocks_total={}, \
                 block_skips={}, documents_scored={}, lengths_rows_read={}, terminated_early={}",
                evidence.rounds,
                evidence.final_k,
                evidence.ranked_rows,
                evidence.matched_terms,
                evidence.query_terms,
                evidence.postings_visited,
                evidence.postings_total,
                evidence.blocks_decoded,
                evidence.blocks_read,
                evidence.blocks_skipped,
                evidence.blocks_total,
                evidence.block_skips,
                evidence.documents_scored,
                evidence.lengths_rows_read,
                evidence.terminated_early
            )
        }),
    });
    output.rows =
        crate::explain::render_explain_rows(&nodes, analyze, statement_rows, resources.limits)?;
    Ok(output)
}
