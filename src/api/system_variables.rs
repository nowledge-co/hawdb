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

use super::{QueryOutput, Result};
use crate::cypher;
use crate::value::Value;
use std::collections::BTreeMap;

pub use hawdb_query_policy::QuerySystemVariables;
pub(super) use hawdb_query_policy::{
    query_statement_variables_for_statement, query_work_request_for_statement,
    reject_system_variable_parameters,
};

pub(super) fn apply_set_system_variable(
    variables: &mut QuerySystemVariables,
    set: &cypher::SetSystemVariable,
) -> Result<QueryOutput> {
    let update = hawdb_query_policy::apply_set_system_variable(variables, set)?;
    Ok(QueryOutput {
        rows: vec![BTreeMap::from([
            ("name".to_string(), Value::String(update.name)),
            ("value".to_string(), update.value),
        ])]
        .into(),
    })
}
