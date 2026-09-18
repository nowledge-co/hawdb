use super::super::*;

#[cfg(test)]
pub use hawdb_nowledge_contracts::test_support::analytics::*;

pub(crate) struct QueryExecutionTrace {
    pub(crate) statement: cypher::Statement,
    pub(crate) optimizer_trace: Option<OptimizerTrace>,
    pub(crate) plan_cache_lookup: Option<PlanCacheLookup>,
    pub(crate) execution_profile: Option<executor::ReadExecutionProfile>,
}

impl QueryExecutionTrace {
    pub(in crate::api) fn uncached(statement: cypher::Statement) -> Self {
        Self {
            statement,
            optimizer_trace: None,
            plan_cache_lookup: None,
            execution_profile: None,
        }
    }
}

#[cfg(test)]
mod facade_tests {
    use super::*;

    #[test]
    fn facade_reexports_analytics_protocol_models_without_conversion() {
        let _: fn(KnowledgePageRankScoreUpdate) -> hawdb_nowledge_contracts::test_support::analytics::KnowledgePageRankScoreUpdate =
            |value| value;
        let _: fn(KnowledgeRelationshipDeleteRequest) -> hawdb_nowledge_contracts::test_support::analytics::KnowledgeRelationshipDeleteRequest =
            |value| value;
    }
}
