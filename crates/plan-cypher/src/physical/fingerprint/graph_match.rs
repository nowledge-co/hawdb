// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;
use crate::{
    GraphEntityKind, GraphMatchNode, GraphMatchProgram, GraphMatchStep, ShortestPathSelection,
};

pub(super) fn write_program(output: &mut String, program: &GraphMatchProgram) {
    output.push_str(if program.optional {
        "optional,"
    } else {
        "required,"
    });
    write_identifier_list(output, &program.introduced);
    // Projection decides which record parts are read; a cached plan keeps it.
    write_projection(output, ",projected=[", &program.projected_nodes);
    write_projection(
        output,
        ",projected_relationships=[",
        &program.projected_relationships,
    );
    // The execution strategy changes the work and the row order.
    if let Some(join) = &program.hash_join {
        output.push_str(",hash_join=");
        write_identifier(output, &join.key);
        output.push_str(&format!(
            ":{:?}:{:?}:{}:{}",
            join.build_conjuncts, join.residual_order, join.residual_placed, join.probe_conjuncts
        ));
    }
    if !program.id_set_conjuncts.is_empty() {
        output.push_str(&format!(",id_sets={:?}", program.id_set_conjuncts));
    }
    output.push_str(",imports=[");
    for import in &program.imports {
        output.push_str(match import.kind {
            GraphEntityKind::Node => "node:",
            GraphEntityKind::Relationship => "rel:",
        });
        write_identifier(output, &import.variable);
        output.push('=');
        write_identifier(output, &import.column);
        output.push(';');
    }
    output.push_str("],steps=[");
    for step in &program.steps {
        match step {
            GraphMatchStep::Node(node) => write_node(output, node),
            GraphMatchStep::Expand {
                source,
                relationship,
                rel_type,
                rel_types,
                properties,
                direction,
                min_hops,
                max_hops,
                target,
            } => {
                output.push_str("expand(");
                write_identifier(output, source);
                output.push(',');
                match relationship {
                    Some(variable) => {
                        output.push_str("bound:");
                        write_identifier(output, variable);
                    }
                    None => output.push_str("anonymous"),
                }
                output.push(',');
                write_identifier(output, rel_type);
                // A disjunction reads more types than its first; single-type
                // steps keep their earlier fingerprint text.
                if !rel_types.is_empty() {
                    output.push_str("|types=");
                    write_identifier_list(output, rel_types);
                }
                output.push(',');
                write_properties(output, properties);
                output.push(',');
                output.push_str(match direction {
                    RelationshipDirection::Outgoing => "out",
                    RelationshipDirection::Incoming => "in",
                    RelationshipDirection::Undirected => "both",
                });
                output.push(',');
                output.push_str(&min_hops.to_string());
                output.push_str("..");
                output.push_str(&max_hops.to_string());
                output.push(',');
                write_node(output, target);
                output.push(')');
            }
            GraphMatchStep::ShortestPath(step) => {
                output.push_str(match step.selection {
                    ShortestPathSelection::Any => "shortest_path(",
                    ShortestPathSelection::All => "all_shortest_paths(",
                });
                write_identifier(output, &step.path);
                output.push(',');
                write_identifier(output, &step.source);
                output.push(',');
                write_identifier(output, &step.rel_type);
                output.push(',');
                output.push_str(match step.direction {
                    RelationshipDirection::Outgoing => "out",
                    RelationshipDirection::Incoming => "in",
                    RelationshipDirection::Undirected => "both",
                });
                output.push(',');
                output.push_str(&step.min_hops.to_string());
                output.push_str("..");
                output.push_str(&step.max_hops.to_string());
                output.push_str(if step.bounded {
                    ",bounded,"
                } else {
                    ",capped,"
                });
                write_identifier(output, &step.target);
                output.push(')');
            }
        }
        output.push(';');
    }
    output.push_str("],predicate=");
    write_optional_predicate(output, program.predicate.as_ref());
    // Placement changes when work runs, not which rows match; it stays part
    // of the instance identity so a cached plan keeps its placement.
    if !program.predicate_steps.is_empty() {
        output.push_str(",predicate_steps=[");
        for step in &program.predicate_steps {
            output.push_str(&step.to_string());
            output.push(';');
        }
        output.push(']');
    }
    if !program.edge_predicates.is_empty() {
        output.push_str(",edge_predicates=[");
        for conjunct in &program.edge_predicates {
            output.push_str(&conjunct.to_string());
            output.push(';');
        }
        output.push(']');
    }
}

fn write_projection(
    output: &mut String,
    prefix: &str,
    projection: &std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
) {
    if projection.is_empty() {
        return;
    }
    output.push_str(prefix);
    for (variable, properties) in projection {
        write_identifier(output, variable);
        output.push(':');
        for property in properties {
            write_identifier(output, property);
            output.push(',');
        }
        output.push(';');
    }
    output.push(']');
}

fn write_node(output: &mut String, node: &GraphMatchNode) {
    output.push_str("node(");
    write_identifier(output, &node.variable);
    output.push(':');
    write_identifier(output, &node.label);
    output.push(',');
    write_properties(output, &node.properties);
    output.push(')');
}
