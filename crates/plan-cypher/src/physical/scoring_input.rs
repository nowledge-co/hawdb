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
use std::collections::BTreeSet;

/// Engine-owned row annotations; never public result columns.
#[doc(hidden)]
pub const SCORING_PROVENANCE_PREFIX: &str = "\0hawdb.scoring.";

/// One declared vector seed and the canonical candidate reached from it.
/// Admission proves a connected, row-preserving physical producer chain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScoringVectorGraphInput {
    seed_variable: String,
    candidate_variable: String,
}

impl ScoringVectorGraphInput {
    pub fn new(
        seed_variable: impl Into<String>,
        candidate_variable: impl Into<String>,
    ) -> Result<Self> {
        let input = Self {
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
                "candidate variable does not end the declared vector expansion chain",
            ));
        }
        Ok(())
    }

    fn validate_stage(&self, plan: &PhysicalPlan, depth: usize) -> Result<Stage> {
        if depth > 256 {
            return Err(invalid("vector scoring chain exceeds maximum depth"));
        }
        match plan {
            PhysicalPlan::VectorSeedScan {
                output_external_id, ..
            } => {
                let mut id_columns = BTreeSet::from(["id".to_string()]);
                if *output_external_id {
                    id_columns.insert("external_id".to_string());
                }
                Ok(Stage {
                    id_columns,
                    variable: None,
                    seen: BTreeSet::new(),
                })
            }
            PhysicalPlan::ProjectExec { items, input } => {
                if items
                    .iter()
                    .map(|item| &item.name)
                    .collect::<BTreeSet<_>>()
                    .len()
                    != items.len()
                {
                    return Err(invalid(
                        "ambiguous duplicate vector scoring projection aliases",
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
                input,
                ..
            } => {
                let mut state = self.validate_stage(input, depth + 1)?;
                if *optional
                    || state.variable.is_some()
                    || variable != self.seed_variable()
                    || property != "id"
                    || !state.id_columns.contains(column)
                {
                    return Err(invalid(
                        "seed lookup must use an unmodified vector-produced ID column",
                    ));
                }
                state.variable = Some(variable.clone());
                state.seen.insert(variable.clone());
                Ok(state)
            }
            PhysicalPlan::AdjacencyExpandExec {
                source_variable,
                target_variable,
                input,
                ..
            } => {
                let mut state = self.validate_stage(input, depth + 1)?;
                if state.variable.as_deref() != Some(source_variable)
                    || !state.seen.insert(target_variable.clone())
                {
                    return Err(invalid(
                        "vector scoring requires a connected expansion without variable rebinding",
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
                        "vector scoring cannot infer imported MATCH provenance",
                    ));
                }
                for step in &program.steps {
                    match step {
                        GraphMatchStep::Node(node)
                            if state.variable.as_deref() == Some(&node.variable) => {}
                        GraphMatchStep::Expand { source, target, .. }
                            if state.variable.as_deref() == Some(source)
                                && state.seen.insert(target.variable.clone()) =>
                        {
                            state.variable = Some(target.variable.clone());
                        }
                        _ => {
                            return Err(invalid(
                                "vector scoring MATCH must retain one connected expansion chain",
                            ))
                        }
                    }
                }
                Ok(state)
            }
            PhysicalPlan::FilterExec { input, .. }
            | PhysicalPlan::SortExec { input, .. }
            | PhysicalPlan::LimitExec { input, .. }
            | PhysicalPlan::TopNExec { input, .. } => self.validate_stage(input, depth + 1),
            _ => Err(invalid(
                "unsupported or ambiguous vector scoring producer chain",
            )),
        }
    }
}

struct Stage {
    id_columns: BTreeSet<String>,
    variable: Option<String>,
    seen: BTreeSet<String>,
}

fn invalid(message: &str) -> HawDBError {
    HawDBError::Semantic(message.into())
}

fn public_expression(expression: &ProjectionExpression) -> bool {
    !matches!(expression, ProjectionExpression::Column(name) if name.starts_with(SCORING_PROVENANCE_PREFIX))
        && expression.all_children(public_expression)
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
    fn validates_producer_id_lineage_and_rejects_replacement_or_ambiguous_aliases() {
        let source = ScoringVectorGraphInput::new("seed", "seed").unwrap();
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
        let source = ScoringVectorGraphInput::new("seed", "seed").unwrap();
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
            ScoringVectorGraphInput::new("seed", "candidate")
                .unwrap()
                .validate_plan(&plan),
            Err(HawDBError::Semantic(_))
        ));
        let source = ScoringVectorGraphInput::new("seed", "seed").unwrap();
        source.validate_plan(&plan).unwrap();
        assert!(matches!(
            source.validate_plan(&PhysicalPlan::DistinctExec {
                input: Box::new(plan)
            }),
            Err(HawDBError::Semantic(_))
        ));
    }
}
