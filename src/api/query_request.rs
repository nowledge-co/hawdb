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

use super::{QueryAccessControlContext, QueryStreamOptions};
use crate::{HawDBError, Result, Value};
use hawdb_core::graph_rag::{ScoringProgram, ScoringProgramShape};
use hawdb_plan_cypher::{visit_plan, PhysicalPlan, SCORING_RERANK_SCORE_COLUMN};
use std::collections::BTreeMap;

/// Optional engine-owned ranking of the complete returned candidate stream.
/// Property names in the program refer to declared returned value aliases.
#[derive(Debug, Clone, PartialEq)]
pub struct ScoringRequest {
    program: ScoringProgram,
    score_column: String,
    limit: usize,
    candidate_window: bool,
    reference_time_millis: Option<u64>,
}

impl ScoringRequest {
    pub fn new(
        program: ScoringProgram,
        score_column: impl Into<String>,
        limit: usize,
    ) -> Result<Self> {
        let score_column = score_column.into();
        if score_column.is_empty()
            || score_column.contains('\0')
            || score_column == SCORING_RERANK_SCORE_COLUMN
        {
            return Err(HawDBError::Semantic(
                "scoring requires a nonempty input score column distinct from its result column"
                    .into(),
            ));
        }
        Ok(Self {
            program,
            score_column,
            limit,
            candidate_window: false,
            reference_time_millis: None,
        })
    }

    /// Explicitly rank within the query's existing LIMIT/OFFSET window.
    /// The window is retained; the final scoring K never limits its input.
    pub fn with_candidate_window(mut self) -> Self {
        self.candidate_window = true;
        self
    }

    /// Fix timestamp decay to an epoch-millisecond input supplied by the host.
    /// Otherwise the ordinary query entrypoint captures time once before planning.
    pub fn with_reference_time_millis(mut self, time: u64) -> Self {
        self.reference_time_millis = Some(time);
        self
    }

    pub fn program(&self) -> &ScoringProgram {
        &self.program
    }

    pub(super) fn bind(&self) -> BoundScoringRequest<'_> {
        BoundScoringRequest {
            request: self,
            reference_time_millis: self
                .reference_time_millis
                .unwrap_or_else(hawdb_executor::scoring::reference_time_millis),
        }
    }
}

/// A borrowed ordinary Cypher request. Existing string overloads remain valid.
#[derive(Clone, Copy)]
pub struct QueryRequest<'a> {
    pub(super) cypher: &'a str,
    pub(super) parameters: Option<&'a BTreeMap<String, Value>>,
    pub(super) scoring: Option<&'a ScoringRequest>,
    pub(super) access_control: Option<&'a QueryAccessControlContext>,
    pub(super) task_context: Option<&'a hawdb_core::RuntimeTaskContext>,
    pub(super) output_limits: QueryStreamOptions,
}

impl<'a> QueryRequest<'a> {
    pub fn new(cypher: &'a str) -> Self {
        Self {
            cypher,
            parameters: None,
            scoring: None,
            access_control: None,
            task_context: None,
            output_limits: QueryStreamOptions::default(),
        }
    }

    pub fn with_params(mut self, parameters: &'a BTreeMap<String, Value>) -> Self {
        self.parameters = Some(parameters);
        self
    }

    pub fn with_scoring(mut self, scoring: &'a ScoringRequest) -> Self {
        self.scoring = Some(scoring);
        self
    }

    pub fn with_access_control(mut self, access: &'a QueryAccessControlContext) -> Self {
        self.access_control = Some(access);
        self
    }

    pub fn with_task_context(mut self, context: &'a hawdb_core::RuntimeTaskContext) -> Self {
        self.task_context = Some(context);
        self
    }

    /// Restrict read output without relaxing database-level caps.
    /// Mutations retain the database's separate mutation limits.
    pub fn with_output_limits(mut self, limits: QueryStreamOptions) -> Self {
        self.output_limits = limits;
        self
    }
}

#[derive(Clone, Copy)]
pub(super) struct BoundScoringRequest<'a> {
    request: &'a ScoringRequest,
    reference_time_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct ScoringPlanCacheKey {
    program: ScoringProgramShape,
    score_column: String,
    limit: usize,
    candidate_window: bool,
}

impl BoundScoringRequest<'_> {
    pub(super) fn cache_key(self) -> ScoringPlanCacheKey {
        ScoringPlanCacheKey {
            program: self.request.program.shape(),
            score_column: self.request.score_column.clone(),
            limit: self.request.limit,
            candidate_window: self.request.candidate_window,
        }
    }

    pub(super) fn attach_template(self, input: PhysicalPlan) -> Result<PhysicalPlan> {
        if hawdb_executor::mutation::is_mutation_plan(&input)? {
            return Err(HawDBError::Semantic("scoring requires a read query".into()));
        }
        if !self.request.candidate_window {
            let mut windowed = false;
            visit_plan(&input, &mut |node| {
                windowed |= matches!(
                    node,
                    PhysicalPlan::LimitExec { offset, limit, .. }
                        if *offset != 0 || limit.is_some()
                ) || matches!(node, PhysicalPlan::TopNExec { .. });
            });
            if windowed {
                return Err(HawDBError::Semantic(
                    "scoring a query with LIMIT/OFFSET requires an explicit candidate window"
                        .into(),
                ));
            }
        }
        Ok(PhysicalPlan::ScoringProgramExec {
            score_column: self.request.score_column.clone(),
            program: self.request.program.neutral_template(),
            reference_time_millis: 0,
            limit: self.request.limit,
            input: Box::new(input),
        })
    }

    pub(super) fn rebind(self, plan: &mut PhysicalPlan) -> Result<()> {
        let PhysicalPlan::ScoringProgramExec {
            score_column,
            program,
            reference_time_millis,
            limit,
            ..
        } = plan
        else {
            return Err(HawDBError::Execution(
                "cached scoring plan is missing its scoring operator".into(),
            ));
        };
        if *score_column != self.request.score_column
            || *limit != self.request.limit
            || program.shape() != self.request.program.shape()
        {
            return Err(HawDBError::Execution(
                "cached scoring plan has a different scoring structure".into(),
            ));
        }
        *program = self.request.program.clone();
        *reference_time_millis = self.reference_time_millis;
        Ok(())
    }
}

impl<S: crate::executor::ExecutionStore> super::DatabaseReadTransaction<S> {
    pub fn query_request(&mut self, request: QueryRequest<'_>) -> Result<super::QueryOutput> {
        let task_context = self.task_context.clone();
        let options = super::query_runtime::QueryExecutionOptions::for_request(
            request,
            task_context.as_ref(),
        );
        let prepared = super::query_runtime::parse_runtime_execution(request.cypher)?;
        if let crate::cypher::Statement::Explain(explain) = &prepared.statement {
            self.store.ensure_usable()?;
            return self
                .execute_explain_request(
                    request.cypher,
                    explain,
                    request.parameters.unwrap_or(&BTreeMap::new()),
                    options,
                )
                .map(|profiled| profiled.output);
        }
        let mut rows = Vec::new();
        self.query_request_streaming_prepared_external(
            request,
            prepared,
            options,
            &mut crate::executor::NoExternalReadOperator,
            &mut |row| {
                rows.push(row);
                Ok(())
            },
        )?;
        Ok(super::QueryOutput { rows: rows.into() })
    }

    /// Uses the existing validated row-consumer boundary and snapshot budgets.
    pub fn query_request_streaming(
        &mut self,
        request: QueryRequest<'_>,
        consumer: impl FnMut(crate::executor::Row) -> Result<()>,
    ) -> Result<super::QueryStreamReport> {
        self.query_request_streaming_with_external(
            request,
            &mut crate::executor::NoExternalReadOperator,
            consumer,
        )
    }

    pub fn query_request_streaming_with_external(
        &mut self,
        request: QueryRequest<'_>,
        external: &mut dyn crate::executor::ExternalReadOperator,
        mut consumer: impl FnMut(crate::executor::Row) -> Result<()>,
    ) -> Result<super::QueryStreamReport> {
        let task_context = self.task_context.clone();
        let options = super::query_runtime::QueryExecutionOptions::for_request(
            request,
            task_context.as_ref(),
        );
        self.query_request_streaming_prepared_external(
            request,
            super::query_runtime::parse_runtime_execution(request.cypher)?,
            options,
            external,
            &mut consumer,
        )
    }

    fn query_request_streaming_prepared_external(
        &mut self,
        request: QueryRequest<'_>,
        prepared: super::query_runtime::PreparedRuntimeExecution,
        options: super::query_runtime::QueryExecutionOptions<'_>,
        external: &mut dyn crate::executor::ExternalReadOperator,
        consumer: &mut impl FnMut(crate::executor::Row) -> Result<()>,
    ) -> Result<super::QueryStreamReport> {
        let empty = BTreeMap::new();
        self.query_with_params_streaming_prepared_external_internal(
            request.cypher,
            prepared,
            request.parameters.unwrap_or(&empty),
            options.output_limits,
            super::ReadStreamingExecutionContext {
                task_context: options.task_context,
                external: Some(external),
                delivery: crate::executor::StreamDelivery::Validated,
                scoring: options.scoring,
                access_control: options.access_control,
            },
            consumer,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Database, DecayTerm, MissingScoringFeature, ScoreFeature, ScoringCombination, ScoringSpec,
        ScoringTerm,
    };

    const QUERY: &str = "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.id AS id, m.seed AS score, m.importance AS importance, m.created AS created ORDER BY id";

    fn fixture() -> Database {
        let mut database = Database::new_with_config(crate::DatabaseConfig {
            runtime_capabilities: hawdb_core::RuntimeCapabilities::default().with(
                hawdb_core::RuntimeCapability::AccessControl,
                cfg!(feature = "acl"),
            ),
            ..crate::DatabaseConfig::default()
        });
        for statement in [
            "CREATE (:Memory {id: 'early', kind: 'x', seed: 1.0, importance: 0.1, created: 1000, space_id: 'a'})",
            "CREATE (:Memory {id: 'late', kind: 'x', seed: 0.2, importance: 1.0, created: 2000, space_id: 'b'})",
            "CREATE (:Memory {id: 'other', kind: 'y', seed: 0.8, importance: 0.8, created: 1000, space_id: 'a'})",
        ] {
            database.query(statement).unwrap();
        }
        database
    }

    fn parameters(kind: &str) -> BTreeMap<String, Value> {
        BTreeMap::from([("kind".into(), Value::String(kind.into()))])
    }

    fn scoring(combination: ScoringCombination, search: f64, importance: f64) -> ScoringRequest {
        ScoringRequest::new(
            ScoringProgram::new(
                combination,
                MissingScoringFeature::Reject,
                ScoringSpec {
                    terms: vec![
                        ScoringTerm {
                            feature: ScoreFeature::SearchScore,
                            weight: search,
                        },
                        ScoringTerm {
                            feature: ScoreFeature::NodeProperty("importance".into()),
                            weight: importance,
                        },
                    ],
                    decay: vec![DecayTerm {
                        feature: ScoreFeature::TimestampProperty("created".into()),
                        half_life: 1.0,
                        min_factor: 0.0,
                    }],
                },
            )
            .unwrap(),
            "score",
            1,
        )
        .unwrap()
    }

    #[test]
    fn ordinary_scoring_rebinds_coefficients_clock_and_query_parameters_on_cache_hits() {
        let mut database = fixture();
        let x = parameters("x");
        let first =
            scoring(ScoringCombination::WeightedSum, 1.0, 0.0).with_reference_time_millis(1_000);
        let before = database.plan_cache_stats().unwrap();
        let output = database
            .query_request(
                QueryRequest::new(QUERY)
                    .with_params(&x)
                    .with_scoring(&first),
            )
            .unwrap();
        assert_eq!(output.rows.len(), 1);
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("early".into()))
        );
        assert_eq!(
            output.rows[0].get(SCORING_RERANK_SCORE_COLUMN),
            Some(&Value::Float(1.0))
        );
        let after_first = database.plan_cache_stats().unwrap();
        assert_eq!(after_first.misses, before.misses + 1);

        let rebound =
            scoring(ScoringCombination::WeightedSum, 0.0, 1.0).with_reference_time_millis(2_000);
        let output = database
            .query_request(
                QueryRequest::new(QUERY)
                    .with_params(&x)
                    .with_scoring(&rebound),
            )
            .unwrap();
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("late".into()))
        );
        assert_eq!(
            output.rows[0].get(SCORING_RERANK_SCORE_COLUMN),
            Some(&Value::Float(1.0))
        );
        assert_eq!(
            database.plan_cache_stats().unwrap().hits,
            after_first.hits + 1
        );

        let y = parameters("y");
        let output = database
            .query_request(
                QueryRequest::new(QUERY)
                    .with_params(&y)
                    .with_scoring(&rebound),
            )
            .unwrap();
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("other".into()))
        );
        let Some(Value::Float(score)) = output.rows[0].get(SCORING_RERANK_SCORE_COLUMN) else {
            panic!("missing combined score")
        };
        assert_eq!(score.to_bits(), 0.4f64.to_bits());
        assert_eq!(
            database.plan_cache_stats().unwrap().hits,
            after_first.hits + 2
        );

        let explain = database
            .query_request(
                QueryRequest::new(&format!("EXPLAIN {QUERY}"))
                    .with_params(&y)
                    .with_scoring(&rebound),
            )
            .unwrap();
        let rendered = format!("{:?}", explain.rows[0]);
        assert!(rendered.contains("ScoringProgramExec"), "{rendered}");
        assert!(rendered.contains("WeightedSum"), "{rendered}");
        assert!(rendered.contains("weight: 0.0"), "{rendered}");
        let Some(Value::Map(cost)) = explain.rows[0].get("selected_plan_cost") else {
            panic!("missing scoring plan cost")
        };
        assert_eq!(cost.get("estimated_rows"), Some(&Value::Int(1)));
        assert!(matches!(cost.get("cost"), Some(Value::Int(value)) if *value > 0));
        let Some(Value::List(cardinalities)) = explain.rows[0].get("operator_cardinalities") else {
            panic!("missing scoring cardinalities")
        };
        let scoring_estimate = cardinalities.iter().find_map(|value| match value {
            Value::Map(estimate)
                if estimate.get("operator")
                    == Some(&Value::String("ScoringProgramExec".into())) =>
            {
                Some(estimate)
            }
            _ => None,
        });
        assert_eq!(
            scoring_estimate.and_then(|estimate| estimate.get("estimated_rows")),
            Some(&Value::Int(1))
        );
    }

    #[test]
    fn ordinary_scoring_cache_separates_programs_from_each_other_and_unscored_queries() {
        let mut database = fixture();
        let params = parameters("x");
        let sum =
            scoring(ScoringCombination::WeightedSum, 2.0, 1.0).with_reference_time_millis(2_000);
        let product = scoring(ScoringCombination::WeightedProduct, 2.0, 1.0)
            .with_reference_time_millis(2_000);
        let before = database.plan_cache_stats().unwrap();
        let sum_rows = database
            .query_request(
                QueryRequest::new(QUERY)
                    .with_params(&params)
                    .with_scoring(&sum),
            )
            .unwrap();
        assert_eq!(
            sum_rows.rows[0].get("id"),
            Some(&Value::String("late".into()))
        );
        let product_rows = database
            .query_request(
                QueryRequest::new(QUERY)
                    .with_params(&params)
                    .with_scoring(&product),
            )
            .unwrap();
        assert_eq!(
            product_rows.rows[0].get("id"),
            Some(&Value::String("early".into()))
        );
        let ordinary = database.query_with_params(QUERY, &params).unwrap();
        assert_eq!(ordinary.rows.len(), 2);
        assert!(ordinary
            .rows
            .iter()
            .all(|row| row.get(SCORING_RERANK_SCORE_COLUMN).is_none()));
        assert_eq!(
            database.plan_cache_stats().unwrap().misses,
            before.misses + 3
        );
    }

    #[test]
    fn ordinary_scoring_preserves_only_explicit_candidate_windows_and_rejects_writes() {
        let mut database = fixture();
        let params = parameters("x");
        let score =
            scoring(ScoringCombination::WeightedSum, 0.0, 1.0).with_reference_time_millis(2_000);
        let query = "MATCH (m:Memory) WHERE m.kind = $kind RETURN m.id AS id, m.seed AS score, m.importance AS importance, m.created AS created ORDER BY score DESC LIMIT 1";
        assert!(matches!(
            database.query_request(
                QueryRequest::new(query)
                    .with_params(&params)
                    .with_scoring(&score)
            ),
            Err(HawDBError::Semantic(_))
        ));
        let windowed = score.clone().with_candidate_window();
        let output = database
            .query_request(
                QueryRequest::new(query)
                    .with_params(&params)
                    .with_scoring(&windowed),
            )
            .unwrap();
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("early".into()))
        );
        assert!(matches!(
            database.query_request(
                QueryRequest::new("CREATE (:Memory {id: 'rejected'})").with_scoring(&score)
            ),
            Err(HawDBError::Semantic(_))
        ));
        assert!(database
            .query("MATCH (m:Memory) WHERE m.id = 'rejected' RETURN m.id AS id")
            .unwrap()
            .rows
            .is_empty());
    }

    #[test]
    fn snapshot_scoring_preserves_authorization_and_validated_budget_cancellation() {
        let database = fixture();
        let mut snapshot = database.begin_read_transaction().unwrap();
        let params = parameters("x");
        let score =
            scoring(ScoringCombination::WeightedSum, 0.0, 1.0).with_reference_time_millis(2_000);
        let access = QueryAccessControlContext::visibility_scope(7, "space_id", "a");
        let request = QueryRequest::new(QUERY)
            .with_params(&params)
            .with_scoring(&score);
        #[cfg(feature = "acl")]
        let request = request.with_access_control(&access);
        #[cfg(not(feature = "acl"))]
        assert!(matches!(
            snapshot.query_request(request.with_access_control(&access)),
            Err(HawDBError::CapabilityUnavailable { .. })
        ));
        let output = snapshot.query_request(request).unwrap();
        assert_eq!(output.rows.len(), 1);
        #[cfg(feature = "acl")]
        assert_eq!(
            output.rows[0].get("id"),
            Some(&Value::String("early".into()))
        );

        let mut delivered = 0;
        let result = snapshot.query_request_streaming(
            request.with_output_limits(QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(1),
            }),
            |_| {
                delivered += 1;
                Ok(())
            },
        );
        assert!(result.is_err());
        assert_eq!(
            delivered, 0,
            "validate result payload before the consumer boundary"
        );

        let two = ScoringRequest::new(score.program().clone(), "score", 2)
            .unwrap()
            .with_reference_time_millis(2_000);
        let result = snapshot.query_request_streaming(
            QueryRequest::new(QUERY)
                .with_params(&params)
                .with_scoring(&two)
                .with_output_limits(QueryStreamOptions {
                    max_rows: Some(1),
                    max_payload_bytes: None,
                }),
            |_| {
                delivered += 1;
                Ok(())
            },
        );
        assert!(
            result.is_err(),
            "output caps must reject rather than truncate scoring K"
        );
        assert_eq!(delivered, 0);

        let token = hawdb_core::RuntimeCancellationToken::new();
        let context = hawdb_core::RuntimeTaskContext::without_deadline(token.clone());
        token.cancel();
        let result = snapshot.query_request_streaming(request.with_task_context(&context), |_| {
            delivered += 1;
            Ok(())
        });
        assert!(result.is_err());
        assert_eq!(delivered, 0, "cancelled scoring requests deliver no rows");
    }

    #[test]
    fn snapshot_scoring_explain_rebinds_and_analyze_obeys_request_boundaries() {
        let database = fixture();
        let mut snapshot = database.begin_read_transaction().unwrap();
        let params = parameters("x");
        let score =
            scoring(ScoringCombination::WeightedSum, 1.0, 0.0).with_reference_time_millis(1_000);
        let cypher = format!("EXPLAIN {QUERY}");
        let request = QueryRequest::new(&cypher)
            .with_params(&params)
            .with_scoring(&score);
        let output = snapshot.query_request(request).unwrap();
        let rendered = format!("{:?}", output.rows[0]);
        assert!(rendered.contains("ScoringProgramExec"), "{rendered}");
        assert!(
            rendered.contains("reference_time_millis=1000"),
            "{rendered}"
        );
        let before = snapshot.plan_cache_stats();
        let rebound =
            scoring(ScoringCombination::WeightedSum, 0.0, 1.0).with_reference_time_millis(2_000);
        let output = snapshot
            .query_request(request.with_scoring(&rebound))
            .unwrap();
        let rendered = format!("{:?}", output.rows[0]);
        assert!(
            rendered.contains("reference_time_millis=2000"),
            "{rendered}"
        );
        assert!(rendered.contains("weight: 0.0"), "{rendered}");
        assert_eq!(snapshot.plan_cache_stats().hits, before.hits + 1);

        let cypher = format!("EXPLAIN ANALYZE {QUERY}");
        let request = QueryRequest::new(&cypher)
            .with_params(&params)
            .with_scoring(&rebound);
        let output = snapshot.query_request(request).unwrap();
        assert_eq!(output.rows[0].get("row_count"), Some(&Value::Int(1)));
        assert!(snapshot
            .query_request(request.with_output_limits(QueryStreamOptions {
                max_rows: Some(1),
                max_payload_bytes: Some(1),
            }))
            .is_err());
        let token = hawdb_core::RuntimeCancellationToken::new();
        let context = hawdb_core::RuntimeTaskContext::without_deadline(token.clone());
        token.cancel();
        assert!(snapshot
            .query_request(request.with_task_context(&context))
            .is_err());
    }
}
