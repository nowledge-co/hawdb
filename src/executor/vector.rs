//! Host vector-read adaptation for the storage-independent batch pipeline.

use super::*;
use std::cell::RefCell;

pub(super) trait BatchExternalRead {
    fn execute_vector_seed(
        &self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput>;
}

pub(super) struct BatchExternalReadAdapter<'a> {
    external: RefCell<&'a mut dyn ExternalReadOperator>,
}

impl<'a> BatchExternalReadAdapter<'a> {
    pub(super) fn new(external: &'a mut dyn ExternalReadOperator) -> Self {
        Self {
            external: RefCell::new(external),
        }
    }
}

impl BatchExternalRead for BatchExternalReadAdapter<'_> {
    fn execute_vector_seed(
        &self,
        request: VectorSeedExecutionRequest<'_>,
    ) -> Result<VectorSeedExecutionOutput> {
        self.external.borrow_mut().execute_vector_seed(request)
    }
}

pub(super) fn vector_embedding_parameter(
    parameters: &BTreeMap<String, Value>,
    name: &str,
    vector_plan: &skein_plan::VectorPhysicalPlan,
) -> Result<Vec<f32>> {
    let Some(Value::List(values)) = parameters.get(name) else {
        return Err(SkeinError::Semantic(format!(
            "vector search parameter '${name}' must be a numeric list"
        )));
    };
    let embedding = values
        .iter()
        .map(|value| match value {
            Value::Float(value) if value.is_finite() => {
                let value = *value as f32;
                if value.is_finite() {
                    Ok(value)
                } else {
                    Err(SkeinError::Semantic(format!(
                        "vector search parameter '${name}' exceeds f32 range"
                    )))
                }
            }
            Value::Int(value) => Ok(*value as f32),
            _ => Err(SkeinError::Semantic(format!(
                "vector search parameter '${name}' must contain finite numbers"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    let expected_dimension = vector_plan_embedding_dimension(vector_plan);
    if embedding.len() != expected_dimension {
        return Err(SkeinError::Semantic(format!(
            "vector search parameter '${name}' dimension changed after planning"
        )));
    }
    Ok(embedding)
}

fn vector_plan_embedding_dimension(plan: &skein_plan::VectorPhysicalPlan) -> usize {
    match plan {
        skein_plan::VectorPhysicalPlan::VectorCandidateScan {
            embedding_dimension,
            ..
        }
        | skein_plan::VectorPhysicalPlan::RawVectorRerank {
            embedding_dimension,
            ..
        } => *embedding_dimension,
        skein_plan::VectorPhysicalPlan::ResidualFilter { input, .. }
        | skein_plan::VectorPhysicalPlan::TopK { input, .. } => {
            vector_plan_embedding_dimension(input)
        }
        skein_plan::VectorPhysicalPlan::Filter { .. } => 0,
    }
}

pub(super) fn vector_plan_top_k(plan: &skein_plan::VectorPhysicalPlan) -> Option<usize> {
    match plan {
        skein_plan::VectorPhysicalPlan::TopK { limit, .. } => Some(*limit),
        skein_plan::VectorPhysicalPlan::VectorCandidateScan { input, .. }
        | skein_plan::VectorPhysicalPlan::RawVectorRerank { input, .. }
        | skein_plan::VectorPhysicalPlan::ResidualFilter { input, .. } => vector_plan_top_k(input),
        skein_plan::VectorPhysicalPlan::Filter { .. } => None,
    }
}
