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

use hawdb_core::Value;
use hawdb_cypher::RelationshipDirection;
use hawdb_plan::{
    AggregateFunction, AggregateTarget, Aggregation, CompositeRangeSeek, ExactPropertySeekBranch,
    NodeProjectionAccess, PhysicalPlan, Predicate, Projection, ProjectionExpression,
};
use std::collections::BTreeMap;

pub(super) const VARIABLES: [&str; 7] = ["n", "m", "r", "s", "left", "right", "missing"];
pub(super) const PROPERTIES: [&str; 5] = ["key", "score", "x", "y", "missing"];
pub(super) const ACCESS_KINDS: usize = 18;
pub(super) const UNARY_KINDS: usize = 11;

pub(super) fn access(kind: usize, variable: &str, label: &str) -> PhysicalPlan {
    let variable = variable.to_string();
    let label = label.to_string();
    let property = "key".to_string();
    let values = vec![Value::Int(7), Value::Null];
    let lower = Some((Value::Int(3), false));
    let upper = Some((Value::Int(9), true));
    let predicates = vec![("key".into(), Value::Int(7)), ("x".into(), Value::Null)];
    let branches = vec![
        ExactPropertySeekBranch {
            property: "key".into(),
            values: values.clone(),
        },
        ExactPropertySeekBranch {
            property: "y".into(),
            values: Vec::new(),
        },
    ];
    let seek = CompositeRangeSeek {
        index_properties: vec!["key".into(), "x".into(), "score".into()],
        equality_prefix: predicates.clone(),
        range_property: "score".into(),
        lower: lower.clone(),
        upper: upper.clone(),
    };
    match kind {
        0 => PhysicalPlan::SeqNodeScan { variable, label },
        1 => PhysicalPlan::IndexNodeSeek {
            variable,
            label,
            property,
            value: Value::Int(7),
        },
        2 => PhysicalPlan::IndexNodeMultiSeek {
            variable,
            label,
            property,
            values,
        },
        3 => PhysicalPlan::IndexNodeUnionSeek {
            variable,
            label,
            branches,
        },
        4 => PhysicalPlan::IndexNodeCompositeSeek {
            variable,
            label,
            predicates,
        },
        5 => PhysicalPlan::IndexNodeCompositeRangeSeek {
            variable,
            label,
            seek,
        },
        6 => PhysicalPlan::IndexNodeRangeSeek {
            variable,
            label,
            property,
            lower,
            upper,
        },
        7 => PhysicalPlan::IndexNodeTextSeek {
            variable,
            label,
            property,
            query: "term".into(),
        },
        8..=14 => {
            let access = match kind {
                8 => NodeProjectionAccess::LabelScan,
                9 => NodeProjectionAccess::PropertyValues { property, values },
                10 => NodeProjectionAccess::PropertyUnion { branches },
                11 => NodeProjectionAccess::CompositeEquality { predicates },
                12 => NodeProjectionAccess::CompositeRange { seek },
                13 => NodeProjectionAccess::PropertyRange {
                    property,
                    lower,
                    upper,
                },
                14 => NodeProjectionAccess::FullText {
                    property,
                    query: "term".into(),
                },
                _ => unreachable!(),
            };
            PhysicalPlan::NodeProjectionScanExec {
                variable: variable.clone(),
                label,
                access,
                required_properties: vec!["key".into()],
                predicate: Some(Predicate::PropertyEq {
                    variable,
                    property: "key".into(),
                    value: Value::Int(7),
                }),
                items: Vec::new(),
            }
        }
        15 => PhysicalPlan::EmptyExec,
        16 => PhysicalPlan::SourceSegmentScan {
            variable,
            predicate: Predicate::ConstantBool(true),
        },
        17 => PhysicalPlan::NodeCountExec {
            label,
            output: variable,
        },
        _ => unreachable!("unknown access fixture"),
    }
}

pub(super) fn expand(input: PhysicalPlan, named: bool, same_endpoint: bool) -> PhysicalPlan {
    PhysicalPlan::AdjacencyExpandExec {
        source_variable: "n".into(),
        source_label: "Left".into(),
        rel_variable: named.then(|| "r".into()),
        rel_type: "LINK".into(),
        rel_properties: BTreeMap::new(),
        direction: RelationshipDirection::Outgoing,
        target_variable: if same_endpoint { "n" } else { "m" }.into(),
        target_label: "Right".into(),
        min_hops: 1,
        max_hops: 2,
        optional: false,
        graph_budget: None,
        input: Box::new(input),
    }
}

pub(super) fn wrap(kind: usize, input: PhysicalPlan) -> PhysicalPlan {
    if kind == 10 {
        return expand(input, true, false);
    }
    let input = Box::new(input);
    match kind {
        0 => PhysicalPlan::FilterExec {
            predicate: Predicate::PropertyEq {
                variable: "n".into(),
                property: "key".into(),
                value: Value::Int(7),
            },
            input,
        },
        1 => PhysicalPlan::ProjectExec {
            items: Vec::new(),
            input,
        },
        2 => PhysicalPlan::DistinctExec { input },
        3 => PhysicalPlan::SortExec {
            items: Vec::new(),
            input,
        },
        4 => PhysicalPlan::TopNExec {
            items: Vec::new(),
            offset: 2,
            limit: 7,
            input,
        },
        5 => PhysicalPlan::LimitExec {
            offset: 3,
            limit: Some(9),
            input,
        },
        6 => PhysicalPlan::AggregateExec {
            group_keys: vec![Projection {
                expression: ProjectionExpression::Property {
                    variable: "n".into(),
                    property: "key".into(),
                },
                name: "key".into(),
            }],
            items: ["n", "m", "r"]
                .into_iter()
                .map(|variable| Aggregation {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Variable(variable.into()),
                    distinct: true,
                    name: format!("count_{variable}"),
                })
                .collect(),
            input,
        },
        7 => PhysicalPlan::NodeColumnLookupExec {
            variable: "left".into(),
            label: "Lookup".into(),
            property: "score".into(),
            column: "score".into(),
            optional: true,
            input,
        },
        8 => PhysicalPlan::AdjacencyExistsExec {
            source_variable: "n".into(),
            rel_type: "LINK".into(),
            direction: RelationshipDirection::Incoming,
            target_variable: "m".into(),
            input,
        },
        9 => PhysicalPlan::OptionalDegreeExec {
            source_variable: "n".into(),
            rel_type: "LINK".into(),
            rel_properties: BTreeMap::new(),
            direction: RelationshipDirection::Undirected,
            target_label: "Right".into(),
            target_properties: BTreeMap::new(),
            alias: "degree".into(),
            input,
        },
        _ => unreachable!("unknown unary fixture"),
    }
}

pub(super) fn product(left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::NodeCartesianProductExec {
        left: Box::new(left),
        right: Box::new(right),
    }
}

pub(super) struct Random(pub(super) u64);

impl Random {
    pub(super) fn next(&mut self) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 as usize
    }

    pub(super) fn plan(&mut self, depth: usize, remaining: &mut usize) -> PhysicalPlan {
        *remaining -= 1;
        let kind = self.next() % 15;
        if depth == 0 || *remaining == 0 || kind == 14 {
            let kind = self.next() % ACCESS_KINDS;
            let variable = VARIABLES[self.next() % (VARIABLES.len() - 1)];
            let label = ["Left", "Right", "Lookup"][self.next() % 3];
            return access(kind, variable, label);
        }
        if kind == 11 && *remaining >= 2 {
            *remaining -= 1;
            let left = self.plan(depth - 1, remaining);
            *remaining += 1;
            let right = self.plan(depth - 1, remaining);
            product(left, right)
        } else {
            let input = self.plan(depth - 1, remaining);
            match kind {
                12 => expand(input, false, false),
                13 => expand(input, true, true),
                _ => wrap(kind % UNARY_KINDS, input),
            }
        }
    }
}
