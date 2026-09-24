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

use super::*;

pub(super) fn write_predicate(output: &mut String, predicate: &Predicate) {
    match predicate {
        Predicate::And(predicates) => {
            output.push_str("And(");
            for (index, predicate) in predicates.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_predicate(output, predicate);
            }
            output.push(')');
        }
        Predicate::Or(predicates) => {
            output.push_str("Or(");
            for (index, predicate) in predicates.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_predicate(output, predicate);
            }
            output.push(')');
        }
        Predicate::Not(predicate) => {
            output.push_str("Not(");
            write_predicate(output, predicate);
            output.push(')');
        }
        Predicate::ConstantBool(value) => {
            output.push_str(if *value { "True" } else { "False" });
        }
        Predicate::RelationshipExists {
            variable,
            rel_type,
            direction,
            target_label,
        } => {
            output.push_str("RelationshipExists(");
            write_identifier(output, variable);
            output.push(',');
            write_identifier(output, rel_type);
            output.push(',');
            output.push_str(match direction {
                RelationshipDirection::Outgoing => "out",
                RelationshipDirection::Incoming => "in",
                RelationshipDirection::Undirected => "both",
            });
            output.push(',');
            write_identifier(output, target_label);
            output.push(')');
        }
        Predicate::BoundRelationshipExists {
            source_variable,
            rel_type,
            direction,
            target_variable,
        } => {
            output.push_str("BoundRelationshipExists(");
            write_identifier(output, source_variable);
            output.push(',');
            write_identifier(output, rel_type);
            output.push(',');
            output.push_str(match direction {
                RelationshipDirection::Outgoing => "out",
                RelationshipDirection::Incoming => "in",
                RelationshipDirection::Undirected => "both",
            });
            output.push(',');
            write_identifier(output, target_variable);
            output.push(')');
        }
        Predicate::IdEq { variable, value } => {
            output.push_str("IdEq(id(");
            write_identifier(output, variable);
            output.push_str(")=");
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdNotEq { variable, value } => {
            output.push_str("IdNotEq(id(");
            write_identifier(output, variable);
            output.push_str(")<>");
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdCompare {
            variable,
            op,
            value,
        } => {
            output.push_str("IdCompare(id(");
            write_identifier(output, variable);
            output.push(')');
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_value(output, value);
            output.push(')');
        }
        Predicate::IdIn { variable, values } => {
            output.push_str("IdIn(id(");
            write_identifier(output, variable);
            output.push_str(") in [");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push_str("])");
        }
        Predicate::PropertyEq {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyEq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push('=');
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyNotEq {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyNotEq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str("<>");
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyCompare {
            variable,
            property,
            op,
            value,
        } => {
            output.push_str("PropertyCompare(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_value(output, value);
            output.push(')');
        }
        Predicate::ExpressionEq { expression, value } => {
            output.push_str("ExpressionEq(");
            write_projection_expression(output, expression);
            output.push('=');
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionNotEq { expression, value } => {
            output.push_str("ExpressionNotEq(");
            write_projection_expression(output, expression);
            output.push_str("<>");
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionCompare {
            expression,
            op,
            value,
        } => {
            output.push_str("ExpressionCompare(");
            write_projection_expression(output, expression);
            output.push_str(match op {
                ComparisonOp::Lt => "<",
                ComparisonOp::Lte => "<=",
                ComparisonOp::Gt => ">",
                ComparisonOp::Gte => ">=",
            });
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::ExpressionContains { expression, value } => {
            output.push_str("ExpressionContains(");
            write_projection_expression(output, expression);
            output.push_str(" contains ");
            write_projection_expression(output, value);
            output.push(')');
        }
        Predicate::PropertyListContains {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyListContains(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" contains ");
            write_value(output, value);
            output.push(')');
        }
        Predicate::PropertyListContainsLower {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyListContainsLower(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" contains ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyContains {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyContains(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" contains ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyStartsWith {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyStartsWith(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" starts_with ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyEndsWith {
            variable,
            property,
            value,
        } => {
            output.push_str("PropertyEndsWith(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" ends_with ");
            write_identifier(output, value);
            output.push(')');
        }
        Predicate::PropertyRegexMatch {
            variable,
            property,
            pattern,
        } => {
            output.push_str("PropertyRegexMatch(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" =~ ");
            write_identifier(output, pattern.as_str());
            output.push(')');
        }
        Predicate::PropertyIsNull { variable, property } => {
            output.push_str("PropertyIsNull(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        Predicate::PropertyIsNotNull { variable, property } => {
            output.push_str("PropertyIsNotNull(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        Predicate::PropertyIn {
            variable,
            property,
            values,
        } => {
            output.push_str("PropertyIn(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push_str(" in [");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push_str("])");
        }
    }
}
