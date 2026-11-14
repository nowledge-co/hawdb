// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Value-error proof for pruning a conjunctive exact-index union. Resource
//! admission remains runtime-owned; unsupported expression shapes are fences.

use super::OptimizerCatalog;
#[cfg(test)]
use hawdb_core::ValidatedRegex;
use hawdb_core::{PropertyType, SchemaObjectState, StringFunction, TableKind, Value};
use hawdb_plan_cypher::{Predicate, ProjectionExpression, ScalarBinaryOp};

const MAX_PROOF_NODES: usize = 512;
const MAX_PROOF_DEPTH: usize = 32;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Any,
    Null,
    Bool,
    String,
}

impl Kind {
    fn literal(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Bool,
            Value::String(_) => Self::String,
            _ => Self::Any,
        }
    }

    fn union(self, other: Self) -> Self {
        match (self, other) {
            (Self::Null, kind) | (kind, Self::Null) => kind,
            (left, right) if left == right => left,
            _ => Self::Any,
        }
    }

    fn boolean(self) -> bool {
        matches!(self, Self::Bool | Self::Null)
    }

    fn text(self) -> bool {
        matches!(self, Self::String | Self::Null)
    }
}

pub(super) fn predicate_is_safe(
    predicate: &Predicate,
    variable: &str,
    label: &str,
    catalog: &OptimizerCatalog,
) -> bool {
    Proof {
        variable,
        label,
        catalog,
        remaining: MAX_PROOF_NODES,
    }
    .predicate(predicate, 0)
}

struct Proof<'a> {
    variable: &'a str,
    label: &'a str,
    catalog: &'a OptimizerCatalog,
    remaining: usize,
}

impl Proof<'_> {
    fn enter(&mut self, depth: usize) -> bool {
        if depth > MAX_PROOF_DEPTH || self.remaining == 0 {
            return false;
        }
        self.remaining -= 1;
        true
    }

    fn predicate(&mut self, predicate: &Predicate, depth: usize) -> bool {
        if !self.enter(depth) {
            return false;
        }
        match predicate {
            Predicate::ConstantBool(_) => true,
            Predicate::And(parts) | Predicate::Or(parts) => {
                parts.iter().all(|part| self.predicate(part, depth + 1))
            }
            Predicate::Not(part) => self.predicate(part, depth + 1),
            Predicate::PropertyEq { variable, .. }
            | Predicate::PropertyNotEq { variable, .. }
            | Predicate::PropertyCompare { variable, .. }
            | Predicate::PropertyIn { variable, .. }
            | Predicate::PropertyIsNull { variable, .. }
            | Predicate::PropertyIsNotNull { variable, .. }
            | Predicate::PropertyContains { variable, .. }
            | Predicate::PropertyStartsWith { variable, .. }
            | Predicate::PropertyEndsWith { variable, .. }
            | Predicate::PropertyRegexMatch { variable, .. }
            | Predicate::IdEq { variable, .. }
            | Predicate::IdNotEq { variable, .. }
            | Predicate::IdCompare { variable, .. }
            | Predicate::IdIn { variable, .. } => variable == self.variable,
            Predicate::ExpressionEq { expression, value }
            | Predicate::ExpressionNotEq { expression, value }
            | Predicate::ExpressionCompare {
                expression, value, ..
            }
            | Predicate::ExpressionContains { expression, value } => {
                self.expression(expression, depth + 1).is_some()
                    && self.expression(value, depth + 1).is_some()
            }
            Predicate::ExpressionRegexMatch { expression, .. } => self
                .expression(expression, depth + 1)
                .is_some_and(Kind::text),
            Predicate::ExpressionIn { expression, .. } => {
                self.expression(expression, depth + 1).is_some()
            }
            _ => false,
        }
    }

    fn property(&self, variable: &str, property: &str) -> Option<Kind> {
        if variable != self.variable {
            return None;
        }
        let declared = || {
            let schema = self.catalog.source_schema.as_ref()?;
            let table = schema.table_id(TableKind::Node, self.label)?;
            if schema.table_descriptor(table)?.state != SchemaObjectState::Public {
                return None;
            }
            let id = schema.property_descriptor_id(table, property)?;
            let descriptor = schema.property_descriptor(id)?;
            if descriptor.state != SchemaObjectState::Public {
                return None;
            }
            Some(match descriptor.value_type {
                PropertyType::Bool => Kind::Bool,
                PropertyType::String => Kind::String,
                _ => Kind::Any,
            })
        };
        Some(declared().unwrap_or(Kind::Any))
    }

    fn expression(&mut self, expression: &ProjectionExpression, depth: usize) -> Option<Kind> {
        if !self.enter(depth) {
            return None;
        }
        match expression {
            ProjectionExpression::Literal(value) => Some(Kind::literal(value)),
            ProjectionExpression::Property { variable, property } => {
                self.property(variable, property)
            }
            ProjectionExpression::Id { variable } if variable == self.variable => Some(Kind::Any),
            ProjectionExpression::Coalesce(values) => {
                let mut result = Kind::Null;
                for value in values {
                    result = result.union(self.expression(value, depth + 1)?);
                }
                Some(result)
            }
            ProjectionExpression::DefaultIfNull {
                variable,
                property,
                default,
            }
            | ProjectionExpression::DefaultIfNullOrEq {
                variable,
                property,
                default,
                ..
            } => Some(
                self.property(variable, property)?
                    .union(Kind::literal(default)),
            ),
            ProjectionExpression::CasePropertyNotNullOrEq {
                variable,
                property,
                non_empty,
                null_or_empty,
                ..
            } => {
                self.property(variable, property)?;
                Some(Kind::literal(non_empty).union(Kind::literal(null_or_empty)))
            }
            ProjectionExpression::IsNull { expression, .. } => {
                self.expression(expression, depth + 1)?;
                Some(Kind::Bool)
            }
            ProjectionExpression::Not(expression) => self
                .expression(expression, depth + 1)?
                .boolean()
                .then_some(Kind::Bool),
            ProjectionExpression::Binary { left, op, right } => {
                let left = self.expression(left, depth + 1)?;
                let right = self.expression(right, depth + 1)?;
                match op {
                    ScalarBinaryOp::Eq
                    | ScalarBinaryOp::NotEq
                    | ScalarBinaryOp::Lt
                    | ScalarBinaryOp::Lte
                    | ScalarBinaryOp::Gt
                    | ScalarBinaryOp::Gte => Some(Kind::Bool),
                    ScalarBinaryOp::And | ScalarBinaryOp::Or
                        if left.boolean() && right.boolean() =>
                    {
                        Some(Kind::Bool)
                    }
                    _ => None,
                }
            }
            ProjectionExpression::Case {
                operand,
                branches,
                otherwise,
            } => {
                let simple = if let Some(operand) = operand {
                    self.expression(operand, depth + 1)?;
                    true
                } else {
                    false
                };
                let mut result = Kind::Null;
                for (condition, value) in branches {
                    let condition = self.expression(condition, depth + 1)?;
                    if !simple && !condition.boolean() {
                        return None;
                    }
                    result = result.union(self.expression(value, depth + 1)?);
                }
                if let Some(otherwise) = otherwise {
                    result = result.union(self.expression(otherwise, depth + 1)?);
                }
                Some(result)
            }
            ProjectionExpression::StringFunction {
                function: StringFunction::Trim,
                arguments,
                pattern: None,
            } => {
                let [argument] = arguments.as_slice() else {
                    return None;
                };
                self.expression(argument, depth + 1)?
                    .text()
                    .then_some(Kind::String)
            }
            ProjectionExpression::Lower(expression) => self
                .expression(expression, depth + 1)?
                .text()
                .then_some(Kind::String),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hawdb_core::Catalog;

    fn property() -> ProjectionExpression {
        ProjectionExpression::Property {
            variable: "t".into(),
            property: "space_id".into(),
        }
    }

    fn equals_default(expression: ProjectionExpression) -> Predicate {
        Predicate::ExpressionEq {
            expression,
            value: ProjectionExpression::Literal(Value::String("default".into())),
        }
    }

    fn catalog(kind: PropertyType, state: SchemaObjectState) -> OptimizerCatalog {
        let mut schema = Catalog::default();
        let table = schema.get_or_create_table(TableKind::Node, "Thread");
        let property = schema.get_or_create_property(table, "space_id", kind, true);
        schema.set_property_state(property, state);
        let mut catalog = OptimizerCatalog::default();
        catalog.source_schema = Some(schema);
        catalog
    }

    #[test]
    fn residual_union_trim_requires_an_enforced_public_string_domain() {
        let trim = equals_default(ProjectionExpression::StringFunction {
            function: StringFunction::Trim,
            arguments: vec![property()],
            pattern: None,
        });
        for (kind, state, expected) in [
            (PropertyType::String, SchemaObjectState::Public, true),
            (PropertyType::String, SchemaObjectState::WriteOnly, false),
            (PropertyType::Any, SchemaObjectState::Public, false),
            (PropertyType::Int, SchemaObjectState::Public, false),
        ] {
            assert_eq!(
                predicate_is_safe(&trim, "t", "Thread", &catalog(kind, state)),
                expected
            );
        }
        let coalesce = equals_default(ProjectionExpression::Coalesce(vec![
            property(),
            ProjectionExpression::Literal(Value::String("default".into())),
        ]));
        assert!(predicate_is_safe(
            &coalesce,
            "t",
            "Thread",
            &OptimizerCatalog::default()
        ));
        assert!(!predicate_is_safe(
            &coalesce,
            "other",
            "Thread",
            &OptimizerCatalog::default()
        ));
    }

    #[test]
    fn residual_union_regex_requires_a_proven_text_domain() {
        let regex = |expression| Predicate::ExpressionRegexMatch {
            expression,
            pattern: ValidatedRegex::new("^ok$").unwrap(),
        };
        for (kind, state, expected) in [
            (PropertyType::String, SchemaObjectState::Public, true),
            (PropertyType::String, SchemaObjectState::WriteOnly, false),
            (PropertyType::Any, SchemaObjectState::Public, false),
            (PropertyType::Int, SchemaObjectState::Public, false),
        ] {
            assert_eq!(
                predicate_is_safe(&regex(property()), "t", "Thread", &catalog(kind, state)),
                expected
            );
        }
        assert!(predicate_is_safe(
            &regex(ProjectionExpression::Literal(Value::Null)),
            "t",
            "Thread",
            &OptimizerCatalog::default()
        ));
        assert!(!predicate_is_safe(
            &regex(ProjectionExpression::Literal(Value::Int(1))),
            "t",
            "Thread",
            &OptimizerCatalog::default()
        ));
    }

    #[test]
    fn residual_union_boolean_types_and_proof_budgets_remain_fences() {
        let wrong_condition = equals_default(ProjectionExpression::Case {
            operand: None,
            branches: vec![(
                ProjectionExpression::Literal(Value::Int(1)),
                ProjectionExpression::Literal(Value::String("default".into())),
            )],
            otherwise: Some(Box::new(property())),
        });
        assert!(!predicate_is_safe(
            &wrong_condition,
            "t",
            "Thread",
            &OptimizerCatalog::default()
        ));
        let mut deep = property();
        for _ in 0..=MAX_PROOF_DEPTH {
            deep = ProjectionExpression::Coalesce(vec![deep]);
        }
        assert!(!predicate_is_safe(
            &equals_default(deep),
            "t",
            "Thread",
            &OptimizerCatalog::default()
        ));
        let wide = ProjectionExpression::Coalesce(vec![
            ProjectionExpression::Literal(Value::Null);
            MAX_PROOF_NODES + 1
        ]);
        assert!(!predicate_is_safe(
            &equals_default(wide),
            "t",
            "Thread",
            &OptimizerCatalog::default()
        ));
    }
}
