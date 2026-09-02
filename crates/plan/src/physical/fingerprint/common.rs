use super::*;

pub(super) fn write_identifier(output: &mut String, value: &str) {
    output.push_str(&value.len().to_string());
    output.push(':');
    output.push_str(value);
}

pub(super) fn write_identifier_list(output: &mut String, values: &[String]) {
    output.push('[');
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, value);
    }
    output.push(']');
}

pub(super) fn write_properties(output: &mut String, properties: &BTreeMap<String, Value>) {
    output.push('{');
    for (index, (key, value)) in properties.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, key);
        output.push('=');
        write_value(output, value);
    }
    output.push('}');
}

pub(super) fn write_relationship_on_create_properties(
    output: &mut String,
    properties: &BTreeMap<String, RelationshipOnCreateValue>,
) {
    output.push('{');
    for (index, (key, value)) in properties.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, key);
        output.push('=');
        match value {
            RelationshipOnCreateValue::Value(value) => write_value(output, value),
            RelationshipOnCreateValue::MatchedRelationshipProperty { property } => {
                output.push_str("matched_rel.");
                write_identifier(output, property);
            }
        }
    }
    output.push('}');
}

pub(super) fn write_value(output: &mut String, value: &Value) {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "bool:true" } else { "bool:false" }),
        Value::Int(value) => {
            output.push_str("int:");
            output.push_str(&value.to_string());
        }
        Value::Float(value) => {
            output.push_str("float:");
            output.push_str(&value.to_bits().to_string());
        }
        Value::String(value) => {
            output.push_str("string:");
            write_identifier(output, value);
        }
        Value::Binary(value) => {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            output.push_str("binary:");
            output.push_str(&value.len().to_string());
            output.push(':');
            for byte in value {
                output.push(HEX[usize::from(byte >> 4)] as char);
                output.push(HEX[usize::from(byte & 0x0f)] as char);
            }
        }
        Value::Uuid(value) => {
            output.push_str("uuid:");
            output.push_str(&value.to_string());
        }
        Value::List(values) => {
            output.push_str("list:[");
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(output, value);
            }
            output.push(']');
        }
        Value::Map(values) => {
            output.push_str("map:{");
            for (index, (key, value)) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_identifier(output, key);
                output.push('=');
                write_value(output, value);
            }
            output.push('}');
        }
    }
}

pub(super) fn write_set_value(output: &mut String, value: &SetValue) {
    match value {
        SetValue::Value(value) => write_value(output, value),
        SetValue::Coalesce { property, default } => {
            output.push_str("coalesce(");
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        SetValue::AddInt { property, amount } => {
            output.push_str("add_int(");
            write_identifier(output, property);
            output.push(',');
            output.push_str(&amount.to_string());
            output.push(')');
        }
        SetValue::DecrementFloorZero { property } => {
            output.push_str("dec_floor_zero(");
            write_identifier(output, property);
            output.push(')');
        }
        SetValue::PreserveNewerExisting {
            property,
            incoming,
            preserve,
        } => {
            output.push_str("preserve_newer(");
            write_identifier(output, property);
            output.push(',');
            write_value(output, incoming);
            output.push(',');
            output.push_str(&preserve.to_string());
            output.push(')');
        }
    }
}

pub(super) fn write_set_assignments(output: &mut String, assignments: &[SetAssignment]) {
    output.push('[');
    for (index, assignment) in assignments.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_identifier(output, &assignment.property);
        output.push('=');
        write_set_value(output, &assignment.value);
    }
    output.push(']');
}

pub(super) fn write_optional_predicate(output: &mut String, predicate: Option<&Predicate>) {
    match predicate {
        Some(predicate) => write_predicate(output, predicate),
        None => output.push_str("none"),
    }
}

pub(super) fn write_optional_range_bound(output: &mut String, bound: Option<&(Value, bool)>) {
    match bound {
        Some((value, inclusive)) => {
            output.push_str(if *inclusive {
                "inclusive:"
            } else {
                "exclusive:"
            });
            write_value(output, value);
        }
        None => output.push_str("none"),
    }
}
