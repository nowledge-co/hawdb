use super::*;
use crate::{GraphEntityKind, GraphMatchNode, GraphMatchProgram, GraphMatchStep};

pub(super) fn write_program(output: &mut String, program: &GraphMatchProgram) {
    output.push_str(if program.optional {
        "optional,"
    } else {
        "required,"
    });
    write_identifier_list(output, &program.introduced);
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
        }
        output.push(';');
    }
    output.push_str("],predicate=");
    write_optional_predicate(output, program.predicate.as_ref());
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
