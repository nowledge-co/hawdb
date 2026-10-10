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

use super::PhysicalPlan;
use crate::{GraphMatchStep, ProjectionExpression};
use hawdb_core::{HawDBError, Result};
use hawdb_expression::{Predicate, SortItem, SortKey};
use std::collections::BTreeSet;

/// Engine-owned row annotations; never public result columns.
#[doc(hidden)]
pub const SCORING_PROVENANCE_PREFIX: &str = "\0hawdb.scoring.";

/// One declared retriever seed and the canonical candidate reached from it.
/// Admission proves a connected, row-preserving physical producer chain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScoringSeedGraphInput {
    kind: ScoringSeedKind,
    seed_variable: String,
    candidate_variable: String,
}

impl ScoringSeedGraphInput {
    pub fn new(
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        let input = Self {
            kind: ScoringSeedKind::Vector,
            seed_variable: seed_variable.into(),
            candidate_variable: candidate_variable.into(),
        };
        if [&input.seed_variable, &input.candidate_variable]
            .iter()
            .any(|name| name.is_empty() || name.contains('\0'))
        {
            return Err(invalid(
                "seed and candidate variables must be nonempty without NUL",
            ));
        }
        Ok(input)
    }

    pub fn new_text(
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        let mut input = Self::new(seed_variable, candidate_variable)?;
        input.kind = ScoringSeedKind::Text;
        Ok(input)
    }

    pub fn new_graph(
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        let mut input = Self::new(seed_variable, candidate_variable)?;
        input.kind = ScoringSeedKind::Graph;
        Ok(input)
    }

    pub fn kind(&self) -> ScoringSeedKind {
        self.kind
    }

    pub fn seed_variable(&self) -> &str {
        &self.seed_variable
    }
    pub fn candidate_variable(&self) -> &str {
        &self.candidate_variable
    }

    /// Fail closed rather than infer retriever provenance from returned aliases.
    pub fn validate_plan(&self, plan: &PhysicalPlan) -> Result<()> {
        let state = self.validate_stage(plan, 0)?;
        if state.variable.as_deref() != Some(self.candidate_variable()) {
            return Err(invalid(
                "candidate variable does not end the declared retriever expansion chain",
            ));
        }
        Ok(())
    }

    fn producer_stage(output_external_id: bool) -> Result<Stage> {
        let mut id_columns = BTreeSet::from(["id".to_string()]);
        if output_external_id {
            id_columns.insert("external_id".to_string());
        }
        Ok(Stage {
            id_columns,
            variable: None,
            seen: BTreeSet::new(),
        })
    }

    fn validate_stage(&self, plan: &PhysicalPlan, depth: usize) -> Result<Stage> {
        if depth > 256 {
            return Err(invalid("seed scoring chain exceeds maximum depth"));
        }
        match plan {
            PhysicalPlan::GraphSeedScan {
                variable,
                score_column,
                node_visibility_predicate,
                ..
            } if self.kind == ScoringSeedKind::Graph && variable == self.seed_variable() => {
                if !public_name(score_column)
                    || node_visibility_predicate
                        .as_ref()
                        .is_some_and(|predicate| !public_predicate(predicate, 0))
                {
                    return Err(invalid(
                        "reserved scoring annotation in graph seed visibility",
                    ));
                }
                Ok(Stage {
                    id_columns: BTreeSet::new(),
                    variable: Some(variable.clone()),
                    seen: BTreeSet::from([variable.clone()]),
                })
            }
            PhysicalPlan::VectorSeedScan {
                output_external_id, ..
            } if self.kind == ScoringSeedKind::Vector => Self::producer_stage(*output_external_id),
            PhysicalPlan::TextSeedScan {
                output_external_id, ..
            } if self.kind == ScoringSeedKind::Text => Self::producer_stage(*output_external_id),
            PhysicalPlan::ProjectExec { items, input } => {
                if items
                    .iter()
                    .map(|item| &item.name)
                    .collect::<BTreeSet<_>>()
                    .len()
                    != items.len()
                {
                    return Err(invalid(
                        "ambiguous duplicate seed scoring projection aliases",
                    ));
                }
                if items.iter().any(|item| {
                    item.name.starts_with(SCORING_PROVENANCE_PREFIX)
                        || !public_expression(&item.expression)
                }) {
                    return Err(invalid("reserved scoring annotation alias"));
                }
                let mut state = self.validate_stage(input, depth + 1)?;
                state.id_columns = items
                    .iter()
                    .filter_map(|item| match &item.expression {
                        ProjectionExpression::Column(column)
                            if state.id_columns.contains(column) =>
                        {
                            Some(item.name.clone())
                        }
                        _ => None,
                    })
                    .collect();
                Ok(state)
            }
            PhysicalPlan::NodeColumnLookupExec {
                variable,
                property,
                column,
                optional,
                node_visibility_predicate,
                input,
                ..
            } => {
                if node_visibility_predicate
                    .as_ref()
                    .is_some_and(|predicate| !public_predicate(predicate, 0))
                {
                    return Err(invalid("reserved scoring annotation in lookup predicate"));
                }
                let mut state = self.validate_stage(input, depth + 1)?;
                if *optional
                    || state.variable.is_some()
                    || variable != self.seed_variable()
                    || property != "id"
                    || !state.id_columns.contains(column)
                {
                    return Err(invalid(
                        "seed lookup must use an unmodified retriever-produced ID column",
                    ));
                }
                state.variable = Some(variable.clone());
                state.seen.insert(variable.clone());
                Ok(state)
            }
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                target_variable,
                rel_variable,
                input,
                ..
            } => {
                let mut state = self.validate_stage(input, depth + 1)?;
                if !public_name(source_variable)
                    || !public_name(target_variable)
                    || rel_variable
                        .as_deref()
                        .is_some_and(|name| !public_name(name))
                    || state.variable.as_deref() != Some(source_variable)
                    || !state.seen.insert(target_variable.clone())
                {
                    return Err(invalid(
                        "seed scoring requires a connected expansion without variable rebinding",
                    ));
                }
                state.variable = Some(target_variable.clone());
                Ok(state)
            }
            PhysicalPlan::GraphMatchExec {
                program,
                input: Some(input),
            } => {
                let mut state = self.validate_stage(input, depth + 1)?;
                if !program.imports.is_empty() {
                    return Err(invalid(
                        "seed scoring cannot infer imported MATCH provenance",
                    ));
                }
                // Runtime clears every introduced binding before MATCH executes.
                // A same-name variable after WITH is a new graph scan, not the seed.
                if program
                    .introduced
                    .iter()
                    .any(|name| state.seen.contains(name) || !public_name(name))
                {
                    return Err(invalid(
                        "seed scoring cannot reintroduce a certified graph variable",
                    ));
                }
                if program
                    .predicate
                    .as_ref()
                    .is_some_and(|predicate| !public_predicate(predicate, 0))
                {
                    return Err(invalid("reserved scoring annotation in MATCH predicate"));
                }
                for step in &program.steps {
                    match step {
                        GraphMatchStep::Node(node)
                            if state.variable.as_deref() == Some(&node.variable) => {}
                        GraphMatchStep::Expand { source, target, .. }
                            if state.variable.as_deref() == Some(source)
                                && public_name(&target.variable)
                                && state.seen.insert(target.variable.clone()) =>
                        {
                            state.variable = Some(target.variable.clone());
                        }
                        _ => {
                            return Err(invalid(
                                "seed scoring MATCH must retain one connected expansion chain",
                            ))
                        }
                    }
                }
                Ok(state)
            }
            PhysicalPlan::FilterExec { predicate, input } => {
                if !public_predicate(predicate, 0) {
                    return Err(invalid("reserved scoring annotation in filter"));
                }
                self.validate_stage(input, depth + 1)
            }
            PhysicalPlan::SortExec { items, input }
            | PhysicalPlan::TopNExec { items, input, .. } => {
                if !items.iter().all(public_sort_item) {
                    return Err(invalid("reserved scoring annotation in order expression"));
                }
                self.validate_stage(input, depth + 1)
            }
            PhysicalPlan::LimitExec { input, .. } => self.validate_stage(input, depth + 1),
            _ => Err(invalid(
                "unsupported or ambiguous seed scoring producer chain",
            )),
        }
    }
}

/// Declared retriever identity is part of physical/request cache shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScoringSeedKind {
    Vector,
    Text,
    Graph,
}

/// Compatibility name for callers of the original vector-only attachment.
pub type ScoringVectorGraphInput = ScoringSeedGraphInput;

struct Stage {
    id_columns: BTreeSet<String>,
    variable: Option<String>,
    seen: BTreeSet<String>,
}

fn invalid(message: &str) -> HawDBError {
    HawDBError::Semantic(message.into())
}

fn public_name(name: &str) -> bool {
    !name.starts_with(SCORING_PROVENANCE_PREFIX)
}

fn public_expression(expression: &ProjectionExpression) -> bool {
    public_expression_at(expression, 0)
}

fn public_expression_at(expression: &ProjectionExpression, depth: usize) -> bool {
    if depth > 256 {
        return false;
    }
    let own_names = match expression {
        ProjectionExpression::Variable { variable }
        | ProjectionExpression::Property { variable, .. }
        | ProjectionExpression::Id { variable }
        | ProjectionExpression::RelationshipType { variable }
        | ProjectionExpression::DatePart { variable, .. }
        | ProjectionExpression::DefaultIfNullOrEq { variable, .. }
        | ProjectionExpression::DefaultIfNull { variable, .. }
        | ProjectionExpression::CasePropertyNotNullOrEq { variable, .. }
        | ProjectionExpression::CasePropertyEqualsRank { variable, .. }
        | ProjectionExpression::CaseLowerPropertyDefault { variable, .. }
        | ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, .. } => {
            public_name(variable)
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => public_name(&expression.variable),
        ProjectionExpression::CaseColumnSearchRank(expression) => public_name(&expression.column),
        ProjectionExpression::ColumnDefaultIfNullOrEq { column, .. }
        | ProjectionExpression::ColumnValueDefaultIfNull { column, .. }
        | ProjectionExpression::ColumnValueCasePropertyNotNullOrEq { column, .. }
        | ProjectionExpression::Column(column)
        | ProjectionExpression::ColumnProperty { column, .. } => public_name(column),
        ProjectionExpression::Literal(_)
        | ProjectionExpression::Coalesce(_)
        | ProjectionExpression::Left { .. }
        | ProjectionExpression::Lower(_)
        | ProjectionExpression::Case { .. }
        | ProjectionExpression::Binary { .. }
        | ProjectionExpression::Not(_)
        | ProjectionExpression::IsNull { .. } => true,
    };
    own_names && expression.all_children(|child| public_expression_at(child, depth + 1))
}

fn public_predicate(predicate: &Predicate, depth: usize) -> bool {
    if depth > 256 {
        return false;
    }
    match predicate {
        Predicate::And(predicates) | Predicate::Or(predicates) => predicates
            .iter()
            .all(|child| public_predicate(child, depth + 1)),
        Predicate::Not(predicate) => public_predicate(predicate, depth + 1),
        Predicate::ConstantBool(_) => true,
        Predicate::RelationshipExists { variable, .. }
        | Predicate::IdEq { variable, .. }
        | Predicate::IdNotEq { variable, .. }
        | Predicate::IdCompare { variable, .. }
        | Predicate::IdIn { variable, .. }
        | Predicate::PropertyEq { variable, .. }
        | Predicate::PropertyNotEq { variable, .. }
        | Predicate::PropertyCompare { variable, .. }
        | Predicate::PropertyListContains { variable, .. }
        | Predicate::PropertyListContainsLower { variable, .. }
        | Predicate::PropertyContains { variable, .. }
        | Predicate::PropertyStartsWith { variable, .. }
        | Predicate::PropertyEndsWith { variable, .. }
        | Predicate::PropertyRegexMatch { variable, .. }
        | Predicate::PropertyIsNull { variable, .. }
        | Predicate::PropertyIsNotNull { variable, .. }
        | Predicate::PropertyIn { variable, .. } => public_name(variable),
        Predicate::BoundRelationshipExists {
            source_variable,
            target_variable,
            ..
        } => public_name(source_variable) && public_name(target_variable),
        Predicate::ExpressionEq { expression, value }
        | Predicate::ExpressionNotEq { expression, value }
        | Predicate::ExpressionCompare {
            expression, value, ..
        }
        | Predicate::ExpressionContains { expression, value } => {
            public_expression_at(expression, depth + 1) && public_expression_at(value, depth + 1)
        }
    }
}

fn public_sort_item(item: &SortItem) -> bool {
    match &item.key {
        SortKey::Property { variable, .. } | SortKey::Id { variable } => public_name(variable),
        SortKey::Column(column) => public_name(column),
        SortKey::Expression(expression) => public_expression(expression),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Projection, VectorCandidateSource, VectorExecutionResourceProfile, VectorPhysicalPlan,
    };
    use hawdb_core::Value;

    fn producer() -> PhysicalPlan {
        PhysicalPlan::VectorSeedScan {
            embedding_parameter: "embedding".into(),
            output_external_id: true,
            metadata_filters: Default::default(),
            resource_profile: VectorExecutionResourceProfile {
                priority: 128,
                max_parallelism: 1,
                max_working_memory_bytes: None,
            },
            vector_plan: VectorPhysicalPlan::TopK {
                limit: 8,
                input: Box::new(VectorPhysicalPlan::VectorCandidateScan {
                    source: VectorCandidateSource::Scalar,
                    embedding_dimension: 2,
                    candidate_limit: 8,
                    input: Box::new(VectorPhysicalPlan::Filter { fields: vec![] }),
                }),
            },
        }
    }
    fn lookup(items: Vec<Projection>) -> PhysicalPlan {
        PhysicalPlan::NodeColumnLookupExec {
            variable: "seed".into(),
            label: "Memory".into(),
            property: "id".into(),
            column: "seed_id".into(),
            optional: false,
            node_visibility_predicate: None,
            input: Box::new(PhysicalPlan::ProjectExec {
                items,
                input: Box::new(producer()),
            }),
        }
    }
    fn known_id() -> Projection {
        Projection {
            name: "seed_id".into(),
            expression: ProjectionExpression::Column("external_id".into()),
        }
    }

    #[test]
    fn lookup_visibility_rejects_reserved_scoring_reads_for_text_and_vector() {
        for kind in [ScoringSeedKind::Vector, ScoringSeedKind::Text] {
            let source = if kind == ScoringSeedKind::Text {
                ScoringSeedGraphInput::new_text("seed", "seed")
            } else {
                ScoringSeedGraphInput::new("seed", "seed")
            }
            .unwrap();
            let make_plan = |predicate| {
                let mut plan = lookup(vec![known_id()]);
                let PhysicalPlan::NodeColumnLookupExec {
                    node_visibility_predicate,
                    input,
                    ..
                } = &mut plan
                else {
                    unreachable!()
                };
                *node_visibility_predicate = Some(predicate);
                if kind == ScoringSeedKind::Text {
                    let PhysicalPlan::ProjectExec { input, .. } = input.as_mut() else {
                        unreachable!()
                    };
                    **input = PhysicalPlan::TextSeedScan {
                        query_parameter: "text".into(),
                        top_k: 1,
                        output_external_id: true,
                        metadata_filters: Default::default(),
                        resource_profile: VectorExecutionResourceProfile {
                            priority: 1,
                            max_parallelism: 1,
                            max_working_memory_bytes: None,
                        },
                    };
                }
                plan
            };
            source
                .validate_plan(&make_plan(Predicate::PropertyIn {
                    variable: "seed".into(),
                    property: "space_id".into(),
                    values: vec![Value::String("allowed".into())],
                }))
                .unwrap();
            for name in ["seed_score", "hops"] {
                let private =
                    ProjectionExpression::Column(format!("{SCORING_PROVENANCE_PREFIX}{name}"));
                for expression in [
                    private.clone(),
                    ProjectionExpression::Coalesce(vec![
                        ProjectionExpression::Literal(Value::Null),
                        private,
                    ]),
                ] {
                    for private_on_rhs in [false, true] {
                        let public = ProjectionExpression::Literal(Value::Float(1.0));
                        let predicate = if private_on_rhs {
                            Predicate::ExpressionEq {
                                expression: public,
                                value: expression.clone(),
                            }
                        } else {
                            Predicate::ExpressionEq {
                                expression: expression.clone(),
                                value: public,
                            }
                        };
                        for predicate in [
                            predicate.clone(),
                            Predicate::And(vec![
                                Predicate::ConstantBool(true),
                                Predicate::Not(Box::new(predicate)),
                            ]),
                        ] {
                            assert!(
                                matches!(
                                    source.validate_plan(&make_plan(predicate)),
                                    Err(HawDBError::Semantic(_))
                                ),
                                "{kind:?} lookup admitted private {name} read"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn validates_producer_id_lineage_and_rejects_replacement_or_ambiguous_aliases() {
        let source = ScoringSeedGraphInput::new("seed", "seed").unwrap();
        source.validate_plan(&lookup(vec![known_id()])).unwrap();
        let forged = Projection {
            name: "seed_id".into(),
            expression: ProjectionExpression::Literal(Value::String("other".into())),
        };
        for items in [vec![forged.clone()], vec![forged, known_id()]] {
            assert!(matches!(
                source.validate_plan(&lookup(items)),
                Err(HawDBError::Semantic(_))
            ));
        }
    }

    #[test]
    fn rejects_reserved_annotation_forgery_and_projection_reads() {
        let source = ScoringSeedGraphInput::new("seed", "seed").unwrap();
        for item in [
            Projection {
                name: format!("{SCORING_PROVENANCE_PREFIX}vector_score"),
                expression: ProjectionExpression::Literal(Value::Float(99.0)),
            },
            Projection {
                name: "public".into(),
                expression: ProjectionExpression::Column(format!(
                    "{SCORING_PROVENANCE_PREFIX}hops"
                )),
            },
        ] {
            let plan = PhysicalPlan::ProjectExec {
                items: vec![item],
                input: Box::new(lookup(vec![known_id()])),
            };
            assert!(matches!(
                source.validate_plan(&plan),
                Err(HawDBError::Semantic(_))
            ));
        }
    }

    #[test]
    fn rejects_unbound_candidate_and_collapsing_input() {
        let plan = lookup(vec![known_id()]);
        assert!(matches!(
            ScoringSeedGraphInput::new("seed", "candidate")
                .unwrap()
                .validate_plan(&plan),
            Err(HawDBError::Semantic(_))
        ));
        let source = ScoringSeedGraphInput::new("seed", "seed").unwrap();
        source.validate_plan(&plan).unwrap();
        assert!(matches!(
            source.validate_plan(&PhysicalPlan::DistinctExec {
                input: Box::new(plan)
            }),
            Err(HawDBError::Semantic(_))
        ));
    }
    fn private_read() -> ProjectionExpression {
        ProjectionExpression::ColumnValueDefaultIfNull {
            column: format!("{SCORING_PROVENANCE_PREFIX}vector_score"),
            default: Value::Float(0.0),
        }
    }

    fn reject_private_plan(plan: PhysicalPlan) {
        assert!(matches!(
            ScoringSeedGraphInput::new("seed", "seed")
                .unwrap()
                .validate_plan(&plan),
            Err(HawDBError::Semantic(_))
        ));
    }

    #[test]
    fn rejects_specialized_and_nested_reserved_projection_reads() {
        use hawdb_expression::CaseColumnSearchRankProjection;
        let column = format!("{SCORING_PROVENANCE_PREFIX}vector_score");
        for expression in [
            private_read(),
            ProjectionExpression::ColumnProperty {
                column: column.clone(),
                property: "id".into(),
            },
            ProjectionExpression::ColumnDefaultIfNullOrEq {
                column: column.clone(),
                property: "id".into(),
                empty: Value::Null,
                default: Value::Null,
            },
            ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
                column: column.clone(),
                empty: Value::Null,
                non_empty: Value::Int(1),
                null_or_empty: Value::Int(0),
            },
            ProjectionExpression::CaseColumnSearchRank(Box::new(CaseColumnSearchRankProjection {
                column: column.clone(),
                raw_query: Value::Null,
                normalized_query: Value::Null,
                exact_rank: Value::Int(1),
                contains_rank: Value::Int(1),
                fallback_rank: Value::Int(0),
            })),
            ProjectionExpression::Variable {
                variable: column.clone(),
            },
            ProjectionExpression::Coalesce(vec![
                ProjectionExpression::Literal(Value::Null),
                private_read(),
            ]),
        ] {
            reject_private_plan(PhysicalPlan::ProjectExec {
                items: vec![Projection {
                    name: "public".into(),
                    expression,
                }],
                input: Box::new(lookup(vec![known_id()])),
            });
        }
        ScoringSeedGraphInput::new("seed", "seed")
            .unwrap()
            .validate_plan(&PhysicalPlan::ProjectExec {
                items: vec![Projection {
                    name: "public".into(),
                    expression: ProjectionExpression::ColumnValueDefaultIfNull {
                        column: "seed_id".into(),
                        default: Value::Null,
                    },
                }],
                input: Box::new(lookup(vec![known_id()])),
            })
            .unwrap();
    }

    #[test]
    fn rejects_reserved_filter_and_match_predicate_reads() {
        use hawdb_expression::Predicate;
        let predicate = Predicate::Not(Box::new(Predicate::ExpressionEq {
            expression: private_read(),
            value: ProjectionExpression::Literal(Value::Float(0.0)),
        }));
        reject_private_plan(PhysicalPlan::FilterExec {
            predicate: predicate.clone(),
            input: Box::new(lookup(vec![known_id()])),
        });
        reject_private_plan(PhysicalPlan::GraphMatchExec {
            program: crate::GraphMatchProgram {
                imports: vec![],
                introduced: vec![],
                steps: vec![crate::GraphMatchStep::Node(crate::GraphMatchNode {
                    variable: "seed".into(),
                    label: "Memory".into(),
                    properties: Default::default(),
                })],
                predicate: Some(predicate),
                optional: false,
            },
            input: Some(Box::new(lookup(vec![known_id()]))),
        });
    }

    #[test]
    fn rejects_reserved_sort_reads() {
        use hawdb_expression::{SortDirection, SortItem, SortKey};
        for key in [
            SortKey::Column(format!("{SCORING_PROVENANCE_PREFIX}vector_score")),
            SortKey::Expression(private_read()),
        ] {
            reject_private_plan(PhysicalPlan::SortExec {
                items: vec![SortItem {
                    key,
                    direction: SortDirection::Asc,
                }],
                input: Box::new(lookup(vec![known_id()])),
            });
        }
    }

    #[test]
    fn rejects_reserved_topn_reads() {
        use hawdb_expression::{SortDirection, SortItem, SortKey};
        reject_private_plan(PhysicalPlan::TopNExec {
            items: vec![SortItem {
                key: SortKey::Expression(private_read()),
                direction: SortDirection::Asc,
            }],
            offset: 0,
            limit: 1,
            input: Box::new(lookup(vec![known_id()])),
        });
    }
}
