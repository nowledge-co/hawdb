use skein_analytics::ProjectedGraph;
use skein_core::{Result, Value};
use skein_executor::QueryOutput;
use std::collections::BTreeMap;

/// Internal execution boundary implemented by the embedded facade.
///
/// The borrowed session keeps query, setup, and effect execution inside one
/// session and preserves its drop/rollback boundary on early errors.
pub trait CompatibilityPrimaryEngine {
    type Session<'a>: CompatibilityPrimarySession
    where
        Self: 'a;

    fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput>;

    fn explain_plan_with_params(
        &self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<String>;

    fn project_graph(&self, rel_type: Option<&str>) -> ProjectedGraph;

    fn session(&mut self) -> Self::Session<'_>;
}

pub trait CompatibilityPrimarySession {
    fn query_with_params(
        &mut self,
        cypher: &str,
        parameters: &BTreeMap<String, Value>,
    ) -> Result<QueryOutput>;
}
