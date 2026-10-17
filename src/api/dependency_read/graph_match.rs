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
use crate::planner::Predicate;
use hawdb_core::RelationshipDirection;
use hawdb_plan_cypher::{GraphMatchProgram, GraphMatchStep};

const MAX_ADJACENCY_WORK: usize = 1024;

pub(super) fn ids<'a>(predicate: &'a Predicate, variable: &str) -> Option<&'a [Value]> {
    match predicate {
        Predicate::And(parts) => parts.iter().find_map(|part| ids(part, variable)),
        Predicate::IdEq {
            variable: bound,
            value,
        } if bound == variable => Some(std::slice::from_ref(value)),
        Predicate::IdIn {
            variable: bound,
            values,
        } if bound == variable => Some(values),
        _ => None,
    }
}

pub(super) fn record_ids(
    values: &[Value],
    label: &str,
    dependencies: &mut GraphReadDependencies,
) -> Result<()> {
    if values.len() > MAX_ADJACENCY_WORK {
        return Err(refusal());
    }
    let covered = dependencies.covers_node_domain(label);
    for value in values {
        let Value::Int(value) = value else {
            return Err(refusal());
        };
        let value = u64::try_from(*value).map_err(|_| refusal())?;
        if !covered {
            dependencies
                .record(VersionKey::GraphNode(NodeId(value)))
                .map_err(|error| HawDBError::Execution(error.to_string()))?;
        }
    }
    Ok(())
}

pub(super) fn collect(
    program: &GraphMatchProgram,
    outer: Option<&Predicate>,
    catalog: &Catalog,
    store: &GraphStore,
    dependencies: &mut GraphReadDependencies,
) -> Result<()> {
    let [GraphMatchStep::Node(root), rest @ ..] = program.steps.as_slice() else {
        return Err(refusal());
    };
    if program.optional
        || !program.imports.is_empty()
        || program
            .predicate
            .as_ref()
            .is_some_and(predicate_reads_relationships)
        || outer.is_some_and(predicate_reads_relationships)
    {
        return Err(refusal());
    }
    let values = outer
        .and_then(|predicate| ids(predicate, &root.variable))
        .or_else(|| {
            program
                .predicate
                .as_ref()
                .and_then(|predicate| ids(predicate, &root.variable))
        })
        .ok_or_else(refusal)?;
    record_ids(values, &root.label, dependencies)?;
    if rest.is_empty() {
        return Ok(());
    }
    let [GraphMatchStep::Expand {
        source,
        rel_type,
        direction,
        target,
        min_hops: 1,
        max_hops: 1,
        ..
    }] = rest
    else {
        return Err(refusal());
    };
    if source != &root.variable {
        return Err(refusal());
    }
    if covered_hop(dependencies, &root.label, rel_type, &target.label) {
        return Ok(());
    }
    let kind = if rel_type.is_empty() {
        None
    } else {
        Some(catalog.rel_type_id(rel_type).ok_or_else(refusal)?)
    };
    let directions: &[AdjacencyDirection] = match direction {
        RelationshipDirection::Outgoing => &[AdjacencyDirection::Outgoing],
        RelationshipDirection::Incoming => &[AdjacencyDirection::Incoming],
        RelationshipDirection::Undirected => {
            &[AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming]
        }
    };
    let mut work = MAX_ADJACENCY_WORK;
    for value in values {
        let Value::Int(value) = value else {
            return Err(refusal());
        };
        for direction in directions {
            store.collect_prepared_adjacency_dependencies(
                dependencies,
                NodeId(*value as u64),
                kind,
                *direction,
                &mut work,
            )?;
        }
    }
    Ok(())
}

pub(super) fn collect_expand(
    plan: &PhysicalPlan,
    catalog: &Catalog,
    store: &GraphStore,
    dependencies: &mut GraphReadDependencies,
) -> Result<()> {
    let PhysicalPlan::AdjacencyExpandExec {
        source_variable,
        target_label,
        rel_type,
        direction,
        min_hops: 1,
        max_hops: 1,
        input,
        ..
    } = plan
    else {
        return Err(refusal());
    };
    let Some((variable, root_label)) = complete_node_source(input) else {
        return Err(refusal());
    };
    if variable != source_variable {
        return Err(refusal());
    }
    let values = bound_ids(input, variable).ok_or_else(refusal)?;
    record_ids(values, root_label, dependencies)?;
    if covered_hop(dependencies, root_label, rel_type, target_label) {
        return Ok(());
    }
    let kind = if rel_type.is_empty() {
        None
    } else {
        Some(catalog.rel_type_id(rel_type).ok_or_else(refusal)?)
    };
    let directions: &[AdjacencyDirection] = match direction {
        RelationshipDirection::Outgoing => &[AdjacencyDirection::Outgoing],
        RelationshipDirection::Incoming => &[AdjacencyDirection::Incoming],
        RelationshipDirection::Undirected => {
            &[AdjacencyDirection::Outgoing, AdjacencyDirection::Incoming]
        }
    };
    let mut work = MAX_ADJACENCY_WORK;
    for value in values {
        let Value::Int(value) = value else {
            return Err(refusal());
        };
        for direction in directions {
            store.collect_prepared_adjacency_dependencies(
                dependencies,
                NodeId(*value as u64),
                kind,
                *direction,
                &mut work,
            )?;
        }
    }
    Ok(())
}

fn bound_ids<'a>(plan: &'a PhysicalPlan, variable: &str) -> Option<&'a [Value]> {
    match plan {
        PhysicalPlan::FilterExec { predicate, input } => {
            ids(predicate, variable).or_else(|| bound_ids(input, variable))
        }
        PhysicalPlan::NodeProjectionScanExec {
            predicate: Some(predicate),
            ..
        } => ids(predicate, variable),
        _ => None,
    }
}

fn refusal() -> HawDBError {
    HawDBError::Execution("prepared graph MATCH requires a complete internal-ID root and a qualified single-hop shape".into())
}

// The plan qualifier above establishes a complete internal-ID root and one
// explicit hop. Both endpoint labels and the relationship type must be guarded;
// an unlabeled/untyped source requires its all-domain guard. Filters, result
// limits and which endpoints happen to exist at E do not weaken these premises.
fn covered_hop(dependencies: &GraphReadDependencies, root: &str, kind: &str, target: &str) -> bool {
    dependencies.covers_node_domain(root)
        && dependencies.covers_node_domain(target)
        && dependencies.covers_relationship_domain(kind)
}
