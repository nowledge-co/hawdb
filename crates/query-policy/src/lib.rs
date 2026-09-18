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

//! Session-scoped query policy derived from Cypher system variables.
//!
//! This crate owns the typed policy and AST interpretation. The embedded
//! facade owns session state and turns accepted updates into query results.

use hawdb_core::{HawDBError, Result, Value};
use hawdb_cypher as cypher;
use hawdb_optimizer::OptimizerSearchDirective;
use hawdb_qos::{WorkClass, WorkPriority, WorkRequest};
use std::collections::BTreeSet;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuerySystemVariables {
    pub work_priority: WorkPriority,
    pub work_class: WorkClass,
    pub estimated_operations: usize,
}

impl Default for QuerySystemVariables {
    fn default() -> Self {
        Self {
            work_priority: WorkPriority::Foreground,
            work_class: WorkClass::Query,
            estimated_operations: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryStatementVariables {
    query_variables: QuerySystemVariables,
    pub optimizer_search: OptimizerSearchDirective,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemVariableUpdate {
    pub name: String,
    pub value: Value,
}

impl QueryStatementVariables {
    fn from_query_variables(query_variables: QuerySystemVariables) -> Self {
        Self {
            query_variables,
            optimizer_search: OptimizerSearchDirective::Auto,
        }
    }

    fn query_work_request(&self) -> WorkRequest {
        self.query_variables.query_work_request()
    }
}

impl QuerySystemVariables {
    pub fn query_work_request(&self) -> WorkRequest {
        match self.work_priority {
            WorkPriority::Foreground => {
                WorkRequest::foreground(self.work_class, self.estimated_operations)
            }
            WorkPriority::Background => {
                WorkRequest::background(self.work_class, self.estimated_operations)
            }
        }
    }
}

pub fn apply_set_system_variable(
    variables: &mut QuerySystemVariables,
    set: &cypher::SetSystemVariable,
) -> Result<SystemVariableUpdate> {
    let value = literal_system_variable_value(&set.value)?;
    match set.name.as_str() {
        "work_priority" => {
            let priority = string_system_variable_value(&set.name, &value)?
                .parse::<WorkPriority>()
                .map_err(|_| {
                    HawDBError::Semantic(
                        "SET system.work_priority accepts foreground or background".to_string(),
                    )
                })?;
            variables.work_priority = priority;
            Ok(SystemVariableUpdate {
                name: "system.work_priority".to_string(),
                value: Value::String(priority.as_str().to_string()),
            })
        }
        "work_class" => {
            let class = string_system_variable_value(&set.name, &value)?
                .parse::<WorkClass>()
                .map_err(|_| {
                    HawDBError::Semantic(
                        "SET system.work_class accepts query, mutation, projection, import, analytics, or shadow"
                            .to_string(),
                    )
                })?;
            variables.work_class = class;
            Ok(SystemVariableUpdate {
                name: "system.work_class".to_string(),
                value: Value::String(class.as_str().to_string()),
            })
        }
        "estimated_operations" => {
            let estimated_operations = usize_system_variable_value(&set.name, &value)?;
            variables.estimated_operations = estimated_operations;
            Ok(SystemVariableUpdate {
                name: "system.estimated_operations".to_string(),
                value: Value::Int(i64::try_from(estimated_operations).unwrap_or(i64::MAX)),
            })
        }
        _ => Err(HawDBError::Semantic(format!(
            "unknown system variable system.{}",
            set.name
        ))),
    }
}

fn apply_system_variable_hints(
    variables: &QuerySystemVariables,
    hints: &[cypher::SetSystemVariable],
) -> Result<QueryStatementVariables> {
    let mut statement_variables = QueryStatementVariables::from_query_variables(variables.clone());
    let mut seen = BTreeSet::new();
    for hint in hints {
        if !seen.insert(hint.name.as_str()) {
            return Err(HawDBError::Semantic(format!(
                "duplicate CYPHER system hint system.{}",
                hint.name
            )));
        }
        if hint.name == "optimizer_search" {
            let value = literal_system_variable_value(&hint.value)?;
            let value = string_system_variable_value(&hint.name, &value)?;
            statement_variables.optimizer_search = OptimizerSearchDirective::from_str(&value)
                .map_err(|_| {
                    HawDBError::Semantic(
                        "CYPHER system.optimizer_search accepts auto, memo, or direct_fallback"
                            .to_string(),
                    )
                })?;
        } else {
            apply_set_system_variable(&mut statement_variables.query_variables, hint)?;
        }
    }
    Ok(statement_variables)
}

pub fn reject_system_variable_parameters(
    parameters: &std::collections::BTreeMap<String, Value>,
) -> Result<()> {
    if parameters.is_empty() {
        Ok(())
    } else {
        Err(HawDBError::Semantic(
            "SET system variable does not accept parameters".to_string(),
        ))
    }
}

pub fn query_work_request_for_statement(
    variables: &QuerySystemVariables,
    statement: &cypher::Statement,
) -> Result<WorkRequest> {
    query_statement_variables_for_statement(variables, statement)
        .map(|variables| variables.query_work_request())
}

pub fn query_statement_variables_for_statement(
    variables: &QuerySystemVariables,
    statement: &cypher::Statement,
) -> Result<QueryStatementVariables> {
    match statement {
        cypher::Statement::CypherQuery(query) => {
            apply_system_variable_hints(variables, &query.system_variables)
        }
        cypher::Statement::Explain(explain) => {
            query_statement_variables_for_statement(variables, &explain.statement)
        }
        _ => Ok(QueryStatementVariables::from_query_variables(
            variables.clone(),
        )),
    }
}

fn literal_system_variable_value(value: &cypher::ValueExpression) -> Result<Value> {
    match &value.kind {
        cypher::ValueExpressionKind::Literal(value) => Ok(value.clone()),
        _ => Err(HawDBError::Semantic(
            "SET system variable requires a literal value".to_string(),
        )),
    }
}

fn string_system_variable_value(name: &str, value: &Value) -> Result<String> {
    match value {
        Value::String(value) => Ok(value.to_ascii_lowercase()),
        _ => Err(HawDBError::Semantic(format!(
            "SET system.{name} requires a string value"
        ))),
    }
}

fn usize_system_variable_value(name: &str, value: &Value) -> Result<usize> {
    match value {
        Value::Int(value) if *value >= 0 => usize::try_from(*value)
            .map_err(|_| HawDBError::Semantic(format!("SET system.{name} value is too large"))),
        _ => Err(HawDBError::Semantic(format!(
            "SET system.{name} requires a non-negative integer value"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn set_assignment(input: &str) -> cypher::SetSystemVariable {
        let cypher::Statement::SetSystemVariable(set) = cypher::parse(input).unwrap() else {
            panic!("expected SET system variable statement");
        };
        set
    }

    #[test]
    fn set_assignment_updates_typed_policy_and_returns_canonical_value() {
        let mut variables = QuerySystemVariables::default();

        let update = apply_set_system_variable(
            &mut variables,
            &set_assignment("SET system.work_class = 'analytics'"),
        )
        .unwrap();

        assert_eq!(variables.work_class, WorkClass::Analytics);
        assert_eq!(update.name, "system.work_class");
        assert_eq!(update.value, Value::String("analytics".to_string()));
    }

    #[test]
    fn invalid_set_assignment_does_not_change_policy() {
        let mut variables = QuerySystemVariables::default();
        let before = variables.clone();

        let error = apply_set_system_variable(
            &mut variables,
            &set_assignment("SET system.work_priority = 'urgent'"),
        )
        .unwrap_err();

        assert!(error.to_string().contains("foreground or background"));
        assert_eq!(variables, before);
    }

    #[test]
    fn statement_hints_override_policy_without_mutating_session_state() {
        let variables = QuerySystemVariables {
            work_priority: WorkPriority::Background,
            work_class: WorkClass::Projection,
            estimated_operations: 4,
        };
        let statement = cypher::parse(
            "CYPHER system.work_class = 'analytics' system.estimated_operations = 32 \
             system.optimizer_search = 'direct_fallback' MATCH (n) RETURN n",
        )
        .unwrap();

        let statement_variables =
            query_statement_variables_for_statement(&variables, &statement).unwrap();

        assert_eq!(
            statement_variables.query_work_request(),
            WorkRequest::background(WorkClass::Analytics, 32)
        );
        assert_eq!(
            statement_variables.optimizer_search,
            OptimizerSearchDirective::DirectFallback
        );
        assert_eq!(variables.work_class, WorkClass::Projection);
        assert_eq!(variables.estimated_operations, 4);
    }

    #[test]
    fn policy_rejects_duplicate_hints_non_literal_values_and_set_parameters() {
        let variables = QuerySystemVariables::default();

        let duplicate = cypher::parse(
            "CYPHER system.work_class = 'query' system.work_class = 'analytics' \
             MATCH (n) RETURN n",
        )
        .unwrap();
        assert!(query_work_request_for_statement(&variables, &duplicate)
            .unwrap_err()
            .to_string()
            .contains("duplicate CYPHER system hint"));

        let parameter =
            cypher::parse("CYPHER system.work_priority = $priority MATCH (n) RETURN n").unwrap();
        assert!(query_work_request_for_statement(&variables, &parameter)
            .unwrap_err()
            .to_string()
            .contains("requires a literal value"));

        let mut parameters = BTreeMap::new();
        parameters.insert("ignored".to_string(), Value::Int(1));
        assert!(reject_system_variable_parameters(&parameters)
            .unwrap_err()
            .to_string()
            .contains("does not accept parameters"));
    }
}
