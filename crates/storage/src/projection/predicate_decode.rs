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

//! Shared owned predicate-tree parsing; backends supply work and allocation.

use super::ProjectedRelationshipPredicate;
use hawdb_core::{HawDBError, Result, Value};

pub(super) trait Decoder {
    fn visit(&self) -> Result<()>;
    fn push(
        &self,
        values: &mut Vec<ProjectedRelationshipPredicate>,
        value: ProjectedRelationshipPredicate,
    ) -> Result<()>;
}

pub(super) fn decode(
    value: Value,
    backend: &impl Decoder,
) -> Result<ProjectedRelationshipPredicate> {
    backend.visit()?;
    let Value::List(fields) = value else {
        return Err(HawDBError::Storage(
            "projected relationship predicate must decode to a list".into(),
        ));
    };
    let count = fields.len();
    let mut fields = fields.into_iter();
    let Some(operator) = fields.next() else {
        return Err(HawDBError::Storage(
            "projected relationship predicate is missing its operator".into(),
        ));
    };
    let Value::String(operator) = operator else {
        return Err(HawDBError::Storage(
            "projected relationship predicate operator must be a string".into(),
        ));
    };
    if operator == "and" && count == 2 {
        if let Some(Value::List(children)) = fields.next()
            && !children.is_empty()
        {
            let mut predicates = Vec::new();
            for child in children {
                backend.push(&mut predicates, decode(child, backend)?)?;
            }
            return Ok(ProjectedRelationshipPredicate::And(predicates));
        }
    } else if matches!(operator.as_str(), "eq" | "gte")
        && count == 3
        && let Some(Value::String(property)) = fields.next()
    {
        let value = fields.next().expect("arity checked");
        return Ok(if operator == "eq" {
            ProjectedRelationshipPredicate::Eq { property, value }
        } else {
            ProjectedRelationshipPredicate::Gte { property, value }
        });
    }
    Err(HawDBError::Storage(format!(
        "invalid projected relationship predicate operator or arity: {operator}"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_nested_predicate_moves_wide_leaf_allocations_without_copying() {
        let property = "中文\0property".repeat(8193);
        let property_address = property.as_ptr();
        let payload = vec![0x7a; 257 * 1024 + 3];
        let payload_address = payload.as_ptr();
        let leaf = Value::List(vec![
            Value::String("eq".into()),
            Value::String(property),
            Value::Binary(payload),
        ]);
        let source = Value::List(vec![
            Value::String("and".into()),
            Value::List(vec![
                Value::List(vec![Value::String("and".into()), Value::List(vec![leaf])]),
                Value::List(vec![
                    Value::String("gte".into()),
                    Value::String("score".into()),
                    Value::Float(f64::from_bits(0x7ff8000000000123)),
                ]),
            ]),
        ]);
        let observation = crate::test_allocator::AllocationObservation::start();
        let decoded = super::super::projected_relationship_predicate_from_value(source).unwrap();
        assert_eq!(
            observation.finish(),
            0,
            "owned predicate conversion copied a wide allocation"
        );
        let ProjectedRelationshipPredicate::And(children) = decoded else {
            panic!("outer conjunction")
        };
        assert_eq!(children.len(), 2);
        let ProjectedRelationshipPredicate::And(nested) = &children[0] else {
            panic!("nested conjunction")
        };
        assert_eq!(nested.len(), 1);
        let ProjectedRelationshipPredicate::Eq {
            property,
            value: Value::Binary(payload),
        } = &nested[0]
        else {
            panic!("binary equality")
        };
        assert_eq!(property.as_ptr(), property_address);
        assert_eq!(property, &"中文\0property".repeat(8193));
        assert_eq!(payload.as_ptr(), payload_address);
        assert_eq!(payload.len(), 257 * 1024 + 3);
        assert!(payload.iter().all(|byte| *byte == 0x7a));
        let ProjectedRelationshipPredicate::Gte {
            property,
            value: Value::Float(value),
        } = &children[1]
        else {
            panic!("floating bound")
        };
        assert_eq!(property, "score");
        assert_eq!(value.to_bits(), 0x7ff8000000000123);
    }
}
