use super::*;

pub(super) fn write_projection_list(output: &mut String, items: &[Projection]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_projection(output, item);
    }
    output.push(']');
}

pub(super) fn write_set_return_mode(output: &mut String, returns: &SetNodePropertiesReturnMode) {
    match returns {
        SetNodePropertiesReturnMode::Project(items) => write_projection_list(output, items),
        SetNodePropertiesReturnMode::Count { name } => {
            output.push_str("Count(");
            write_identifier(output, name);
            output.push(')');
        }
    }
}

pub(super) fn write_relationship_count_leg(output: &mut String, leg: &RelationshipCountLeg) {
    output.push_str("RelationshipCountLeg(");
    match leg.direction {
        RelationshipDirection::Incoming => output.push_str("in,"),
        RelationshipDirection::Outgoing => output.push_str("out,"),
        RelationshipDirection::Undirected => output.push_str("both,"),
    }
    write_identifier(output, &leg.rel_type);
    output.push_str(",distinct=");
    output.push_str(if leg.distinct { "true" } else { "false" });
    if let Some(filter) = &leg.filter {
        output.push_str(",filter=");
        match filter {
            RelationshipCountFilter::PropertyNotEqOrEmpty { property, value } => {
                output.push_str("not_eq_or_empty(");
                write_identifier(output, property);
                output.push(',');
                write_value(output, value);
                output.push(')');
            }
        }
    }
    output.push(')');
}

fn write_projection(output: &mut String, item: &Projection) {
    output.push_str("Projection(");
    write_projection_expression(output, &item.expression);
    output.push_str(" as ");
    write_identifier(output, &item.name);
    output.push(')');
}

pub fn write_projection_expression(output: &mut String, expression: &ProjectionExpression) {
    match expression {
        ProjectionExpression::Case {
            operand,
            branches,
            otherwise,
        } => {
            output.push_str("Case(");
            if let Some(operand) = operand {
                write_projection_expression(output, operand);
            }
            output.push(';');
            for (condition, result) in branches {
                write_projection_expression(output, condition);
                output.push(':');
                write_projection_expression(output, result);
                output.push(';');
            }
            if let Some(otherwise) = otherwise {
                write_projection_expression(output, otherwise);
            }
            output.push(')');
        }
        ProjectionExpression::Binary { left, op, right } => {
            output.push_str(&format!("Binary({op:?},"));
            write_projection_expression(output, left);
            output.push(',');
            write_projection_expression(output, right);
            output.push(')');
        }
        ProjectionExpression::Not(child) => {
            output.push_str("Not(");
            write_projection_expression(output, child);
            output.push(')');
        }
        ProjectionExpression::IsNull {
            expression,
            negated,
        } => {
            output.push_str(if *negated { "IsNotNull(" } else { "IsNull(" });
            write_projection_expression(output, expression);
            output.push(')');
        }

        ProjectionExpression::Variable { variable } => {
            write_identifier(output, variable);
        }
        ProjectionExpression::Property { variable, property } => {
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
        }
        ProjectionExpression::Id { variable } => {
            output.push_str("id(");
            write_identifier(output, variable);
            output.push(')');
        }
        ProjectionExpression::RelationshipType { variable } => {
            output.push_str("label(");
            write_identifier(output, variable);
            output.push(')');
        }
        ProjectionExpression::Literal(value) => {
            write_value(output, value);
        }
        ProjectionExpression::Coalesce(expressions) => {
            output.push_str("coalesce(");
            for (index, expression) in expressions.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_projection_expression(output, expression);
            }
            output.push(')');
        }
        ProjectionExpression::Left { expression, length } => {
            output.push_str("left(");
            write_projection_expression(output, expression);
            output.push(',');
            output.push_str(&length.to_string());
            output.push(')');
        }
        ProjectionExpression::Lower(expression) => {
            output.push_str("lower(");
            write_projection_expression(output, expression);
            output.push(')');
        }
        ProjectionExpression::DatePart {
            part,
            variable,
            property,
        } => {
            output.push_str("date_part(");
            output.push_str(match part {
                crate::DatePart::Year => "year",
                crate::DatePart::Month => "month",
            });
            output.push(',');
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
        ProjectionExpression::DefaultIfNullOrEq {
            variable,
            property,
            empty,
            default,
        } => {
            output.push_str("default_if_null_or_eq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::DefaultIfNull {
            variable,
            property,
            default,
        } => {
            output.push_str("default_if_null(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CasePropertyNotNullOrEq {
            variable,
            property,
            empty,
            non_empty,
            null_or_empty,
        } => {
            output.push_str("case_property_not_null_or_eq(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, non_empty);
            output.push(',');
            write_value(output, null_or_empty);
            output.push(')');
        }
        ProjectionExpression::CasePropertyEqualsRank {
            variable,
            property,
            branches,
            default,
        } => {
            output.push_str("case_property_equals_rank(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            for (candidate, rank) in branches {
                output.push(',');
                write_value(output, candidate);
                output.push_str("=>");
                write_value(output, rank);
            }
            output.push_str(",default=>");
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CaseLowerPropertyDefault {
            variable,
            property,
            default,
        } => {
            output.push_str("case_lower_property_default(");
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::CaseCoalesceDifferenceFloorZero { variable, terms } => {
            output.push_str("case_coalesce_difference_floor_zero(");
            for (index, term) in terms.iter().enumerate() {
                if index > 0 {
                    output.push('-');
                }
                output.push_str("coalesce(");
                write_identifier(output, variable);
                output.push('.');
                write_identifier(output, &term.property);
                output.push(',');
                write_value(output, &term.default);
                output.push(')');
            }
            output.push(')');
        }
        ProjectionExpression::CaseEntitySearchRank(expression) => {
            output.push_str("case_entity_search_rank(");
            write_identifier(output, &expression.variable);
            output.push('.');
            write_identifier(output, &expression.name_property);
            output.push(',');
            write_identifier(output, &expression.variable);
            output.push('.');
            write_identifier(output, &expression.aliases_property);
            output.push(',');
            write_value(output, &expression.raw_query);
            output.push(',');
            write_value(output, &expression.normalized_query);
            output.push(',');
            write_value(output, &expression.raw_input);
            output.push(',');
            write_value(output, &expression.exact_rank);
            output.push(',');
            write_value(output, &expression.alias_rank);
            output.push(',');
            write_value(output, &expression.fallback_rank);
            output.push(')');
        }
        ProjectionExpression::CaseColumnSearchRank(expression) => {
            output.push_str("case_column_search_rank(");
            write_identifier(output, &expression.column);
            output.push(',');
            write_value(output, &expression.raw_query);
            output.push(',');
            write_value(output, &expression.normalized_query);
            output.push(',');
            write_value(output, &expression.exact_rank);
            output.push(',');
            write_value(output, &expression.contains_rank);
            output.push(',');
            write_value(output, &expression.fallback_rank);
            output.push(')');
        }
        ProjectionExpression::ColumnDefaultIfNullOrEq {
            column,
            property,
            empty,
            default,
        } => {
            output.push_str("column_default_if_null_or_eq(");
            write_identifier(output, column);
            output.push('.');
            write_identifier(output, property);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::ColumnValueDefaultIfNull { column, default } => {
            output.push_str("column_value_default_if_null(");
            write_identifier(output, column);
            output.push(',');
            write_value(output, default);
            output.push(')');
        }
        ProjectionExpression::ColumnValueCasePropertyNotNullOrEq {
            column,
            empty,
            non_empty,
            null_or_empty,
        } => {
            output.push_str("column_value_case_property_not_null_or_eq(");
            write_identifier(output, column);
            output.push(',');
            write_value(output, empty);
            output.push(',');
            write_value(output, non_empty);
            output.push(',');
            write_value(output, null_or_empty);
            output.push(')');
        }
        ProjectionExpression::Column(name) => {
            output.push_str("column(");
            write_identifier(output, name);
            output.push(')');
        }
        ProjectionExpression::ColumnProperty { column, property } => {
            output.push_str("column_property(");
            write_identifier(output, column);
            output.push('.');
            write_identifier(output, property);
            output.push(')');
        }
    }
}

pub(super) fn write_aggregation_list(output: &mut String, items: &[Aggregation]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_aggregation(output, item);
    }
    output.push(']');
}

fn write_aggregation(output: &mut String, item: &Aggregation) {
    output.push_str("Aggregation(");
    match item.function {
        AggregateFunction::Count => output.push_str("count"),
        AggregateFunction::Min => output.push_str("min"),
        AggregateFunction::Max => output.push_str("max"),
        AggregateFunction::Avg => output.push_str("avg"),
        AggregateFunction::Collect => output.push_str("collect"),
    }
    output.push('(');
    if item.distinct {
        output.push_str("distinct ");
    }
    match &item.target {
        AggregateTarget::All => output.push('*'),
        AggregateTarget::Variable(variable) => write_identifier(output, variable),
        AggregateTarget::Property { variable, property } => {
            write_identifier(output, variable);
            output.push('.');
            write_identifier(output, property);
        }
    }
    output.push_str(") as ");
    write_identifier(output, &item.name);
    output.push(')');
}

pub(super) fn write_sort_list(output: &mut String, items: &[SortItem]) {
    output.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        output.push_str("Sort(");
        match &item.key {
            SortKey::Property { variable, property } => {
                write_identifier(output, variable);
                output.push('.');
                write_identifier(output, property);
            }
            SortKey::Id { variable } => {
                output.push_str("id(");
                write_identifier(output, variable);
                output.push(')');
            }
            SortKey::Expression(expression) => write_projection_expression(output, expression),
            SortKey::Column(column) => write_identifier(output, column),
        }
        output.push(' ');
        match item.direction {
            SortDirection::Asc => output.push_str("asc"),
            SortDirection::Desc => output.push_str("desc"),
        }
        output.push(')');
    }
    output.push(']');
}
