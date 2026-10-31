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

use hawdb_core::Result;

use super::super::ast::*;
use super::Parser;

const MAX_PROJECTED_RELATIONSHIP_PREDICATE_BYTES: usize = 16 * 1024;
const MAX_PROJECTED_RELATIONSHIP_PREDICATE_CONJUNCTS: usize = 16;

pub(super) fn parse_projected_relationship_predicate(input: &str) -> Result<PropertyPredicate> {
    if input.len() > MAX_PROJECTED_RELATIONSHIP_PREDICATE_BYTES {
        return Err(hawdb_core::HawDBError::Semantic(format!(
            "projected relationship predicate exceeds limit of {MAX_PROJECTED_RELATIONSHIP_PREDICATE_BYTES} UTF-8 bytes"
        )));
    }
    let mut parser = Parser::new(input);
    let predicate = parser.parse_predicate(false)?;
    parser.expect_eof()?;
    validate_projected_relationship_predicate(&predicate)?;
    Ok(predicate)
}

fn validate_projected_relationship_predicate(predicate: &PropertyPredicate) -> Result<usize> {
    match predicate {
        PropertyPredicate::And(predicates) if !predicates.is_empty() => {
            let mut conjuncts = 0;
            for predicate in predicates {
                conjuncts += validate_projected_relationship_predicate(predicate)?;
                if conjuncts > MAX_PROJECTED_RELATIONSHIP_PREDICATE_CONJUNCTS {
                    return Err(hawdb_core::HawDBError::Semantic(format!(
                        "projected relationship predicate exceeds limit of {MAX_PROJECTED_RELATIONSHIP_PREDICATE_CONJUNCTS} conjuncts"
                    )));
                }
            }
            Ok(conjuncts)
        }
        PropertyPredicate::Eq {
            variable, value, ..
        } if variable == "r" && matches!(&value.kind, ValueExpressionKind::Literal(_)) => Ok(1),
        PropertyPredicate::Compare {
            variable,
            op: ComparisonOp::Gte,
            value,
            ..
        } if variable == "r" && matches!(&value.kind, ValueExpressionKind::Literal(_)) => Ok(1),
        _ => Err(hawdb_core::HawDBError::Semantic(
            "projected relationship predicates support only literal r.property comparisons joined by AND"
                .to_string(),
        )),
    }
}
