use super::{QueryOutput, Result};
use crate::cypher;
use crate::value::Value;
use std::collections::BTreeMap;

pub use skein_query_policy::QuerySystemVariables;
pub(super) use skein_query_policy::{
    query_statement_variables_for_statement, query_work_request_for_statement,
    reject_system_variable_parameters,
};

pub(super) fn apply_set_system_variable(
    variables: &mut QuerySystemVariables,
    set: &cypher::SetSystemVariable,
) -> Result<QueryOutput> {
    let update = skein_query_policy::apply_set_system_variable(variables, set)?;
    Ok(QueryOutput {
        rows: vec![BTreeMap::from([
            ("name".to_string(), Value::String(update.name)),
            ("value".to_string(), update.value),
        ])]
        .into(),
    })
}
