// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Compiled Cypher PEG with bounded captures and typed AST lowering.
use super::tree::{Node, ProjectionCaches};
use super::*;
pub(super) const RULE_NAMES: &[&str] = &[
    "root",
    "pipeline_root",
    "end",
    "statement",
    "multi_stage",
    "pipeline",
    "match",
    "unwind",
    "explain",
    "cypher",
    "begin",
    "checkpoint",
    "commit",
    "rollback",
    "call",
    "w_input",
    "p_where",
    "w_or",
    "w_and",
    "w_atom",
    "w_not",
    "w_group",
    "w_graph",
    "w_bound",
    "w_list_contains",
    "w_contains_fn",
    "w_reference",
    "w_parameter",
    "w_id_call",
    "w_id",
    "w_id_op",
    "w_property",
    "w_string",
    "w_string_op",
    "w_word",
    "w_scalar",
    "w_parenthesized_expression",
    "w_expression_tail",
    "w_comparisons",
    "w_expression_comparisons",
    "w_following",
    "w_right",
    "w_in",
    "x_scalar",
    "x_primary",
    "x_conversion",
    "x_value",
    "x_ref",
    "x_variable",
    "x_identity",
    "x_identity_name",
    "x_list_function",
    "x_list_function_name",
    "x_metric",
    "x_metric_name",
    "x_coalesce",
    "x_json",
    "x_left",
    "x_lower",
    "x_path_properties",
    "x_path",
    "x_path_name",
    "x_date",
    "x_list",
    "x_value_list",
    "x_expression_list",
    "x_expression_list_contents",
    "x_comprehension",
    "x_comprehension_contents",
    "x_comprehension_where",
    "x_comprehension_projection",
    "x_postfix",
    "x_postfix_contents",
    "x_dots",
    "x_slice_tail",
    "x_case_scalar",
    "x_timestamp_value",
    "x_or",
    "x_or_rest",
    "x_and",
    "x_and_rest",
    "x_condition",
    "x_not",
    "x_not_contents",
    "x_condition_base",
    "x_condition_paren",
    "x_list_contains",
    "x_condition_tail",
    "x_is_null",
    "x_negated",
    "x_comparisons",
    "x_comparison",
    "x_comparison_op",
    "x_contains",
    "x_contains_op",
    "x_case",
    "x_reserved_special_case",
    "x_special_lower",
    "x_special_case",
    "x_case_body",
    "x_searched_case",
    "x_searched_branch",
    "x_simple_case",
    "x_simple_branch",
    "x_else",
    "x_floor_case",
    "x_difference",
    "x_difference_term",
    "x_temporal_name",
    "x_temporal",
    "v_temporal",
    "x_map",
    "v_map",
    "x_sum",
    "x_product",
    "x_operand",
    "with_clause",
    "return_clause",
    "r_projection",
    "r_distinct",
    "r_item",
    "r_alias",
    "r_sum",
    "r_sum_rest",
    "r_sum_op",
    "r_product",
    "r_product_rest",
    "r_product_op",
    "r_atom",
    "r_paren",
    "r_nested_sum",
    "r_unary",
    "r_unary_contents",
    "r_path",
    "r_collect_top",
    "r_aggregate",
    "r_aggregate_name",
    "r_aggregate_contents",
    "r_star",
    "r_list_function",
    "r_list_call",
    "r_order",
    "r_order_item",
    "r_direction",
    "r_order_expression",
    "r_offset",
    "r_limit",
    "u_legacy_call",
    "u_legacy_return",
    "u_legacy_trailing_comma",
    "call_clause",
    "u_procedure",
    "u_arg",
    "u_named",
    "u_option",
    "u_map",
    "u_map_contents",
    "u_map_entry",
    "u_key",
    "u_yields",
    "u_yield",
    "u_yield_alias",
    "merge",
    "p_patterns",
    "p_pattern",
    "p_binding",
    "p_function",
    "p_chain",
    "p_node",
    "p_variable",
    "p_labels",
    "p_step",
    "p_relationship",
    "p_incoming",
    "p_outgoing",
    "p_arrow",
    "p_rel_body",
    "p_rel_type",
    "p_hops",
    "p_selector",
    "p_min",
    "p_range",
    "p_max",
    "match_clause",
    "optional_clause",
    "unwind_clause",
    "load_clause",
    "create_clause",
    "merge_clause",
    "set_clause",
    "delete_clause",
    "detach_clause",
    "v_identifier",
    "v_value",
    "v_parameter",
    "v_list",
    "v_list_contents",
    "v_string",
    "v_number",
    "v_boolean",
    "v_null",
    "v_current_timestamp",
    "v_timestamp",
    "v_cast",
    "v_call_contents",
    "v_property_value",
    "v_conversion_contents",
    "v_property",
    "v_properties",
    "v_property_entry",
    "d_create_data",
    "d_node",
    "d_variable",
    "d_edge",
    "d_merge_data",
    "d_on_sets",
    "d_on_kind",
    "d_post_sets",
    "d_sets",
    "d_assignment",
    "d_ref",
    "d_set_value",
    "d_property_value",
    "d_add",
    "d_coalesce",
    "d_case_preserve",
    "d_case_decrement",
    "hint",
    "set",
    "d_system_variable",
    "schema_create",
    "schema_identifier",
    "schema_kind",
    "schema_table",
    "schema_object_kind",
    "schema_range",
    "schema_fulltext",
    "schema_index",
    "schema_index_keys",
    "schema_property_path",
    "schema_property",
    "schema_not_null",
    "schema_type",
    "schema_dimension",
    "schema_constraint",
    "schema_constraint_key",
    "schema_node_constraint",
    "schema_relationship_constraint",
    "schema_assertion",
    "schema_alter",
    "schema_alter_property",
    "schema_alter_table",
    "schema_state_clause",
    "schema_state",
    "create",
    "alter",
];
use super::lexer::TokenKind;

enum Either<A, B> {
    Left(A),
    Right(B),
}
trait Captures {
    fn append(self, out: &mut Vec<NodeId>);
}
impl Captures for () {
    fn append(self, _: &mut Vec<NodeId>) {}
}
impl Captures for NodeId {
    fn append(self, out: &mut Vec<NodeId>) {
        if out.capacity() == 0 {
            out.reserve_exact(16);
        }
        out.push(self);
    }
}
impl<A: Captures, B: Captures> Captures for Either<A, B> {
    fn append(self, out: &mut Vec<NodeId>) {
        match self {
            Self::Left(x) => x.append(out),
            Self::Right(x) => x.append(out),
        }
    }
}
impl<A: Captures> Captures for Option<A> {
    fn append(self, out: &mut Vec<NodeId>) {
        if let Some(x) = self {
            x.append(out);
        }
    }
}
impl<A: Captures> Captures for Vec<A> {
    fn append(self, out: &mut Vec<NodeId>) {
        for x in self {
            x.append(out);
        }
    }
}
impl<T0: Captures> Captures for (T0,) {
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
    }
}
impl<T0: Captures, T1: Captures> Captures for (T0, T1) {
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
    }
}
impl<T0: Captures, T1: Captures, T2: Captures> Captures for (T0, T1, T2) {
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
    }
}
impl<T0: Captures, T1: Captures, T2: Captures, T3: Captures> Captures for (T0, T1, T2, T3) {
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
    }
}
impl<T0: Captures, T1: Captures, T2: Captures, T3: Captures, T4: Captures> Captures
    for (T0, T1, T2, T3, T4)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
    }
}
impl<T0: Captures, T1: Captures, T2: Captures, T3: Captures, T4: Captures, T5: Captures> Captures
    for (T0, T1, T2, T3, T4, T5)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6, T7)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6, T7, T8)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
    > Captures for (T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13)
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
        T17: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
        self.17.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
        T17: Captures,
        T18: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
        self.17.append(out);
        self.18.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
        T17: Captures,
        T18: Captures,
        T19: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
        self.17.append(out);
        self.18.append(out);
        self.19.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
        T17: Captures,
        T18: Captures,
        T19: Captures,
        T20: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
        self.17.append(out);
        self.18.append(out);
        self.19.append(out);
        self.20.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
        T17: Captures,
        T18: Captures,
        T19: Captures,
        T20: Captures,
        T21: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        T21,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
        self.17.append(out);
        self.18.append(out);
        self.19.append(out);
        self.20.append(out);
        self.21.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
        T17: Captures,
        T18: Captures,
        T19: Captures,
        T20: Captures,
        T21: Captures,
        T22: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        T21,
        T22,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
        self.17.append(out);
        self.18.append(out);
        self.19.append(out);
        self.20.append(out);
        self.21.append(out);
        self.22.append(out);
    }
}
impl<
        T0: Captures,
        T1: Captures,
        T2: Captures,
        T3: Captures,
        T4: Captures,
        T5: Captures,
        T6: Captures,
        T7: Captures,
        T8: Captures,
        T9: Captures,
        T10: Captures,
        T11: Captures,
        T12: Captures,
        T13: Captures,
        T14: Captures,
        T15: Captures,
        T16: Captures,
        T17: Captures,
        T18: Captures,
        T19: Captures,
        T20: Captures,
        T21: Captures,
        T22: Captures,
        T23: Captures,
    > Captures
    for (
        T0,
        T1,
        T2,
        T3,
        T4,
        T5,
        T6,
        T7,
        T8,
        T9,
        T10,
        T11,
        T12,
        T13,
        T14,
        T15,
        T16,
        T17,
        T18,
        T19,
        T20,
        T21,
        T22,
        T23,
    )
{
    fn append(self, out: &mut Vec<NodeId>) {
        self.0.append(out);
        self.1.append(out);
        self.2.append(out);
        self.3.append(out);
        self.4.append(out);
        self.5.append(out);
        self.6.append(out);
        self.7.append(out);
        self.8.append(out);
        self.9.append(out);
        self.10.append(out);
        self.11.append(out);
        self.12.append(out);
        self.13.append(out);
        self.14.append(out);
        self.15.append(out);
        self.16.append(out);
        self.17.append(out);
        self.18.append(out);
        self.19.append(out);
        self.20.append(out);
        self.21.append(out);
        self.22.append(out);
        self.23.append(out);
    }
}

#[derive(Clone, Copy)]
struct Depth {
    value: usize,
    structural: usize,
    statement: usize,
}
impl Depth {
    fn nested_statement(self) -> Self {
        Self {
            structural: self.structural + 1,
            statement: self.statement + 1,
            ..self
        }
    }
    fn descend(self, value: usize, structural: usize) -> Self {
        Self {
            value: self.value + value,
            structural: self.structural + structural,
            statement: self.statement,
        }
    }
}
#[derive(Clone, Copy)]
struct Diagnostic {
    position: usize,
    expected: &'static str,
}
#[derive(Default)]
struct ProjectionBuffer {
    items: Vec<ReturnItem>,
    error: Option<HawDBError>,
    count: usize,
}
#[derive(Default)]
struct OrderBuffer {
    items: Vec<OrderItem>,
    error: Option<HawDBError>,
    count: usize,
}
#[cfg(test)]
#[derive(Clone, Copy)]
pub(super) struct TestLimits {
    pub(super) steps: usize,
    pub(super) nodes: usize,
    pub(super) nesting: usize,
}
#[cfg(test)]
impl Default for TestLimits {
    fn default() -> Self {
        Self {
            steps: Limits::default().steps,
            nodes: Limits::default().nodes,
            nesting: MAX_CYPHER_PARSER_DEPTH,
        }
    }
}
struct Builder<'a> {
    #[cfg(test)]
    limits: TestLimits,
    input: &'a str,
    multi_stage: bool,
    tokens: &'a [Token],
    nodes: Vec<Node>,
    edges: Vec<NodeId>,
    steps: usize,
    fatal: Option<SyntaxError>,
    farthest: Option<Diagnostic>,
    peek_depth: usize,
    eager_projection: bool,
    logical_nodes: usize,
    logical_edges: usize,
    projection: Option<ProjectionBuffer>,
    order: Option<OrderBuffer>,
    projections: ProjectionCaches,
}
impl Builder<'_> {
    fn step_limit(&self) -> usize {
        #[cfg(test)]
        {
            self.limits.steps.min(Limits::default().steps)
        }
        #[cfg(not(test))]
        {
            Limits::default().steps
        }
    }
    fn node_limit(&self) -> usize {
        #[cfg(test)]
        {
            self.limits.nodes.min(Limits::default().nodes)
        }
        #[cfg(not(test))]
        {
            Limits::default().nodes
        }
    }
    fn nesting_limit(&self) -> usize {
        #[cfg(test)]
        {
            self.limits.nesting.min(MAX_CYPHER_PARSER_DEPTH)
        }
        #[cfg(not(test))]
        {
            MAX_CYPHER_PARSER_DEPTH
        }
    }
    fn rewind(&mut self, mark: (usize, usize, usize), projection: bool) {
        self.nodes.truncate(mark.0);
        self.edges.truncate(mark.1);
        if self.projections.len() > mark.2 {
            self.projections.truncate(mark.2);
        }
        if projection {
            self.projection = None;
        }
        // Failed alternatives still consume the monotonic work/node budgets.
        // Diagnostics also survive backtracking, including the first farthest error.
    }
    fn begin_projection(&mut self) {
        self.projection = Some(ProjectionBuffer::default());
    }
    fn projection_item(&mut self, mark: (usize, usize), node: NodeId, depth: usize) {
        let should_fold = self
            .projection
            .as_ref()
            .expect("projection scope")
            .error
            .is_none();
        if should_fold {
            let tree = ParseTree::from_token_matches(
                std::mem::take(&mut self.nodes),
                std::mem::take(&mut self.edges),
                node,
            );
            let result = Transformer {
                tree: &tree,
                input: self.input,
                tokens: self.tokens,
                peak_depth: 0,
            }
            .projection_item(node, depth);
            (self.nodes, self.edges) = tree.into_parts();
            let buffer = self.projection.as_mut().expect("projection scope");
            match result {
                Ok(item) => buffer.items.push(item),
                Err(error) => buffer.error = Some(error),
            }
        }
        self.projection.as_mut().expect("projection scope").count += 1;
        self.nodes.truncate(mark.0);
        self.edges.truncate(mark.1);
    }
    fn finish_projection(
        &mut self,
        rule: u16,
        start: usize,
        end: usize,
        distinct: Option<NodeId>,
    ) -> std::result::Result<NodeId, &'static str> {
        let buffer = self.projection.take().expect("projection scope");
        if buffer.count > self.node_limit().saturating_sub(self.logical_edges) {
            self.fatal = Some(
                SyntaxError::new(
                    SyntaxErrorCode::ParserLimitExceeded,
                    self.tokens[start].span(),
                )
                .expected("PEG nodes and edges within the parser limit"),
            );
            return Err("PEG resource budget");
        }
        self.logical_edges += buffer.count;
        let node = self.node(rule, start, end, distinct)?;
        self.projections
            .put(node, buffer.error.map_or_else(|| Ok(buffer.items), Err));
        Ok(node)
    }

    fn begin_order(&mut self) {
        self.order = Some(OrderBuffer::default());
    }
    fn order_item(&mut self, mark: (usize, usize), node: NodeId, depth: usize) {
        let should_fold = self.order.as_ref().expect("order scope").error.is_none();
        if should_fold {
            let tree = ParseTree::from_token_matches(
                std::mem::take(&mut self.nodes),
                std::mem::take(&mut self.edges),
                node,
            );
            let result = Transformer {
                tree: &tree,
                input: self.input,
                tokens: self.tokens,
                peak_depth: 0,
            }
            .order_item(node, depth);
            (self.nodes, self.edges) = tree.into_parts();
            let buffer = self.order.as_mut().expect("order scope");
            match result {
                Ok(item) => buffer.items.push(item),
                Err(error) => buffer.error = Some(error),
            }
        }
        self.order.as_mut().expect("order scope").count += 1;
        self.nodes.truncate(mark.0);
        self.edges.truncate(mark.1);
    }
    fn finish_order(
        &mut self,
        rule: u16,
        start: usize,
        end: usize,
    ) -> std::result::Result<NodeId, &'static str> {
        let buffer = self.order.take().expect("order scope");
        if buffer.count > self.node_limit().saturating_sub(self.logical_edges) {
            self.fatal = Some(
                SyntaxError::new(
                    SyntaxErrorCode::ParserLimitExceeded,
                    self.tokens[start].span(),
                )
                .expected("PEG nodes and edges within the parser limit"),
            );
            return Err("PEG resource budget");
        }
        self.logical_edges += buffer.count;
        let node = self.node(rule, start, end, ())?;
        self.projections
            .put_order(node, buffer.error.map_or_else(|| Ok(buffer.items), Err));
        Ok(node)
    }

    fn enter(&mut self, depth: Depth, position: usize) -> std::result::Result<(), &'static str> {
        if self.fatal.is_some() {
            return Err("PEG resource budget");
        }
        self.steps += 1;
        if depth.value > self.nesting_limit()
            || depth.structural > self.nesting_limit()
            || self.steps > self.step_limit()
        {
            self.fatal = Some(
                SyntaxError::new(
                    SyntaxErrorCode::ParserLimitExceeded,
                    self.tokens[position].span(),
                )
                .expected("language nesting within the parser limit"),
            );
            return Err("PEG resource budget");
        }
        Ok(())
    }
    fn failure<T>(
        &mut self,
        position: usize,
        expected: &'static str,
    ) -> std::result::Result<T, &'static str> {
        // Token starts are ordered by the lexer. Token indexes preserve that
        // ordering, including the matching position after the EOF token.
        if self.farthest.is_none_or(|old| position > old.position) {
            self.farthest = Some(Diagnostic { position, expected });
        }
        Err(expected)
    }
    fn restore_absence(&mut self, position: usize, saved: Option<Diagnostic>) {
        if self.farthest.is_none_or(|error| error.position <= position) {
            self.farthest = saved;
        }
    }
    fn adjacency_failure(&self, start: usize, end: usize) -> usize {
        (start.saturating_add(1)..end)
            .find(|&i| self.tokens[i - 1].span().end != self.tokens[i].span().start)
            .expect("non-adjacent pair")
    }
    fn adjacent(&self, start: usize, end: usize) -> bool {
        self.tokens[start..end]
            .windows(2)
            .all(|x| x[0].span().end == x[1].span().start)
    }
    // Gate the generic scalar parser without changing diagnostics or charging
    // its resource budget for ordinary non-regex predicates.
    fn has_scalar_regex_tail(&self, position: usize) -> bool {
        let mut parens = 0usize;
        let mut brackets = 0usize;
        let mut braces = 0usize;
        let mut index = position;
        while let Some(token) = self.tokens.get(index) {
            let nested = parens != 0 || brackets != 0 || braces != 0;
            if !nested {
                // A keyword-shaped parameter name or property key remains an
                // identifier; only standalone words can end this predicate.
                let scalar_identifier = index > position
                    && matches!(
                        self.tokens[index - 1].kind,
                        TokenKind::Parameter | TokenKind::Dot
                    )
                    && self.adjacent(index - 1, index + 1);
                match token.kind {
                    TokenKind::Equal => {
                        return self
                            .tokens
                            .get(index + 1)
                            .is_some_and(|next| next.kind == TokenKind::Tilde)
                            && self.adjacent(index, index + 2);
                    }
                    TokenKind::Less | TokenKind::Greater | TokenKind::NotEqual => return false,
                    TokenKind::Word
                        if !scalar_identifier
                            && [
                                "AND", "OR", "XOR", "IS", "IN", "CONTAINS", "STARTS", "ENDS",
                                "RETURN", "WITH", "ORDER", "LIMIT", "SKIP", "OFFSET",
                            ]
                            .iter()
                            .any(|keyword| token.is_keyword(self.input, keyword)) =>
                    {
                        return false;
                    }
                    TokenKind::Semicolon | TokenKind::End => return false,
                    _ => {}
                }
            }
            match token.kind {
                TokenKind::LeftParen => parens += 1,
                TokenKind::RightParen => {
                    let Some(next) = parens.checked_sub(1) else {
                        return false;
                    };
                    parens = next;
                }
                TokenKind::LeftBracket => brackets += 1,
                TokenKind::RightBracket => {
                    let Some(next) = brackets.checked_sub(1) else {
                        return false;
                    };
                    brackets = next;
                }
                TokenKind::LeftBrace => braces += 1,
                TokenKind::RightBrace => {
                    let Some(next) = braces.checked_sub(1) else {
                        return false;
                    };
                    braces = next;
                }
                _ => {}
            }
            index += 1;
        }
        false
    }
    fn node(
        &mut self,
        rule: u16,
        start: usize,
        end: usize,
        children: impl Captures,
    ) -> std::result::Result<NodeId, &'static str> {
        let first = self.edges.len();
        children.append(&mut self.edges);
        if self.logical_nodes >= self.node_limit()
            || self.edges.len() - first > self.node_limit().saturating_sub(self.logical_edges)
        {
            self.fatal = Some(
                SyntaxError::new(
                    SyntaxErrorCode::ParserLimitExceeded,
                    self.tokens[start].span(),
                )
                .expected("PEG nodes and edges within the parser limit"),
            );
            return Err("PEG resource budget");
        }
        self.logical_nodes += 1;
        self.logical_edges += self.edges.len() - first;
        // Lookahead consumes no syntax-tree result. Charge the same logical
        // budget, but release captures before returning an unused node handle.
        if self.peek_depth > 0 {
            self.edges.truncate(first);
            return Ok(0);
        }
        if first == self.edges.len()
            && let Some(id) = super::tree::inline_leaf(rule, start, end)
        {
            return Ok(id);
        }
        if self.edges.len() - first == 1
            && let Some(id) = super::tree::inline_alias(rule, start, end, first)
        {
            return Ok(id);
        }
        if let Some(id) = super::tree::inline_branch(rule, start, end, first..self.edges.len()) {
            return Ok(id);
        }
        if self.nodes.capacity() == 0 {
            self.nodes.reserve_exact(16);
        }
        let id = self.nodes.len();
        self.nodes.push(Node::from_token_match(
            rule,
            start,
            end,
            first..self.edges.len(),
        ));
        Ok(id)
    }
}
peg::parser! {
 grammar syntax<'a>(cx:&mut Builder<'a>) for [Token] {
 rule capture_mark() -> (usize,usize,usize) = {(cx.nodes.len(),cx.edges.len(),cx.projections.len())}
 rule rewind_capture(mark:(usize,usize,usize),projection:bool) = {cx.rewind(mark,projection);}
 rule diagnostic() -> Option<Diagnostic> = { cx.farthest }
 rule scalar_regex_tail_ahead() = position:position!() {? if cx.has_scalar_regex_tail(position) { Ok(()) } else { Err("a scalar regex predicate") } }
 rule peek_mark() -> Option<Diagnostic> = {cx.peek_depth+=1;cx.farthest}
 rule eager_projection() = {? if cx.peek_depth==0 && cx.eager_projection {Ok(())} else {Err("deferred projection fold")} }
 rule restore(saved:Option<Diagnostic>) = {cx.peek_depth=cx.peek_depth.checked_sub(1).expect("balanced lookahead");cx.farthest=saved; }
 rule restore_absence(position:usize,saved:Option<Diagnostic>) = { cx.restore_absence(position,saved); }
 rule record_failure(position:usize,expected:&'static str) = { let _:std::result::Result<(),_>=cx.failure(position,expected); }
 // A value choice that consumed no input reports its public rule contract.
 // Keep errors after a consumed prefix, and leave fatal admission untouched.
 rule value_failure(position:usize,saved:Option<Diagnostic>) = {
     if cx.farthest.is_none_or(|error| error.position <= position) {
         cx.farthest = saved.filter(|error| error.position >= position).or(Some(Diagnostic { position, expected: "value" }));
     }
 }
 // Scalar primary parsing ends with the identifier production.
 rule identifier_failure(position:usize) = {
     if cx.farthest.is_none_or(|error| error.position <= position) {
         cx.farthest = Some(Diagnostic { position, expected: "identifier" });
     }
 }
 // Unary minus is a consumed predicate prefix even when its operand fails.
 // Keep that failure at the operand token rather than losing it in lookahead.
 rule predicate_failure(position:usize) =
     ([token if token.kind == TokenKind::Minus] { () }) fail("identifier")
     / identifier_failure(position)
 // A complete statement owns an equal-position trailing-input failure.
 // Keep farther failures from genuinely incomplete constructs.
 rule record_end_failure(position:usize) = {
     if cx.farthest.is_none_or(|old| position > old.position
         || (position == old.position && matches!(old.expected,
             "a Cypher value token" | "'('" | "'.'"))) {
         cx.farthest = Some(Diagnostic { position, expected: "unexpected trailing input" });
     }
 }
 rule end_failure() = position:position!() record_end_failure(position) rejected:expected!("unexpected trailing input") { rejected }
 // The two-byte assignment token is atomic: report a failed token at its start.
 rule named_assignment() =
     (begin:position!() [colon if colon.kind == TokenKind::Colon]
      [equal if equal.kind == TokenKind::Equal] end:position!()
      {? if cx.adjacent(begin,end) { Ok(()) } else { Err(":=") } })
     / fail(":=")
 rule fail(expected:&'static str) -> () = position:position!() record_failure(position,expected) value:expected!(expected) {value}
 rule repetition_stop<T>(position:usize,saved:Option<Diagnostic>) -> T = restore_absence(position,saved) value:expected!("a consuming PEG repetition") {value}
 rule enter(depth:Depth) = position:position!() {? cx.enter(depth,position) }
 rule admit_multistage() = {? if cx.multi_stage {Ok(())} else {Err("a multi-stage pipeline")} }
 rule root(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(statement(depth.descend(0,0))) c1:(end(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(0,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule pipeline_root(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(pipeline(depth.descend(0,0))) c1:(end(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(1,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule end(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Semicolon] { () } / fail(";"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::End] { () } / end_failure())) { () })) end:position!() {? cx.node(2,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule statement(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((admit_multistage() a:multi_stage(depth) { Either::Left(a) } / b:((a:(match(depth.descend(0,0))) { Either::Left(a) } / b:((a:(unwind(depth.descend(0,0))) { Either::Left(a) } / b:((a:(explain(depth.descend(0,0))) { Either::Left(a) } / b:((a:(cypher(depth.descend(0,0))) { Either::Left(a) } / b:((a:(create(depth.descend(0,0))) { Either::Left(a) } / b:((a:(alter(depth.descend(0,0))) { Either::Left(a) } / b:((a:(merge(depth.descend(0,0))) { Either::Left(a) } / b:((a:(set(depth.descend(0,0))) { Either::Left(a) } / b:((a:(call(depth.descend(0,0))) { Either::Left(a) } / b:((a:(begin(depth.descend(0,0))) { Either::Left(a) } / b:((a:(checkpoint(depth.descend(0,0))) { Either::Left(a) } / b:((a:(commit(depth.descend(0,0))) { Either::Left(a) } / b:(rollback(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(3,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule multi_stage(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if token.is_keyword(cx.input, "MATCH")] { () } / fail("MATCH")))) restore(saved) { () } / restore(saved) fail("a non-MATCH pipeline")))) ((saved:peek_mark() ((&((c0:((saved:diagnostic() begin:position!() result:(c:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("WITH"), 6 => word.eq_ignore_ascii_case("RETURN"), _ => false } } }] { () } / fail("WITH")))) restore(saved) { () } / restore(saved) fail("a non-WITH clause")))) c1:((a:(match_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(optional_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(unwind_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(load_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(call_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(create_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(merge_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(set_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(delete_clause(depth.descend(0,0))) { Either::Left(a) } / b:(detach_clause(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) c1:(with_clause(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("WITH"), 6 => word.eq_ignore_ascii_case("RETURN"), _ => false } } }] { () } / fail("WITH")))) restore(saved) { () } / restore(saved) fail("a non-WITH clause")))) c1:((a:(match_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(optional_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(unwind_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(load_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(call_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(create_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(merge_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(set_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(delete_clause(depth.descend(0,0))) { Either::Left(a) } / b:(detach_clause(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) c3:(with_clause(depth.descend(0,0))) { (c0,c1,c2,c3) }))) restore(saved) { () } / restore(saved) fail("a multi-stage pipeline")))) c2:(pipeline(depth.descend(0,0))) { (c2,) })) end:position!() {? cx.node(4,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule pipeline(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:((saved:diagnostic() begin:position!() result:(c:((((saved:peek_mark() ((!(([token if token.is_keyword(cx.input, "RETURN")] { () } / fail("RETURN")))) restore(saved) { () } / restore(saved) fail("a non-RETURN clause")))) c1:((a:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("WITH"), 6 => word.eq_ignore_ascii_case("RETURN"), _ => false } } }] { () } / fail("WITH")))) restore(saved) { () } / restore(saved) fail("a non-WITH clause")))) c1:((a:(match_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(optional_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(unwind_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(load_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(call_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(create_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(merge_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(set_clause(depth.descend(0,0))) { Either::Left(a) } / b:((a:(delete_clause(depth.descend(0,0))) { Either::Left(a) } / b:(detach_clause(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) { Either::Left(a) } / b:(with_clause(depth.descend(0,0))) { Either::Right(b) })) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) c1:((saved:diagnostic() begin:position!() result:(c:(return_clause(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) end:position!() {? cx.node(5,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule match(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("LOAD"), 5 => word.eq_ignore_ascii_case("MATCH"), _ => false } } }] { () } / fail("MATCH")))) restore(saved) { () } / restore(saved) fail("MATCH")))) c1:(pipeline(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(6,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule unwind(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&(([token if token.is_keyword(cx.input, "UNWIND")] { () } / fail("UNWIND")))) restore(saved) { () } / restore(saved) fail("UNWIND")))) c1:(pipeline(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(7,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule explain(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "EXPLAIN")] { () } / fail("EXPLAIN"))) ((saved:diagnostic() begin:position!() result:(c:(([token if token.is_keyword(cx.input, "ANALYZE")] { () } / fail("ANALYZE"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c2:(statement(depth.nested_statement())) { (c2,) })) end:position!() {? cx.node(8,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule cypher(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CYPHER")] { () } / fail("CYPHER"))) c1:(hint(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(hint(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) c3:(statement(depth.nested_statement())) { (c1,c2,c3) })) end:position!() {? cx.node(9,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule begin(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "BEGIN")] { () } / fail("BEGIN"))) (([token if token.is_keyword(cx.input, "TRANSACTION")] { () } / fail("TRANSACTION"))) { () })) end:position!() {? cx.node(10,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule checkpoint(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "CHECKPOINT")] { () } / fail("CHECKPOINT"))) end:position!() {? cx.node(11,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule commit(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "COMMIT")] { () } / fail("COMMIT"))) end:position!() {? cx.node(12,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule rollback(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "ROLLBACK")] { () } / fail("ROLLBACK"))) end:position!() {? cx.node(13,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule call(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(u_legacy_call(depth.descend(0,0))) { Either::Left(a) } / b:((((saved:peek_mark() ((&(([token if token.is_keyword(cx.input, "CALL")] { () } / fail("CALL")))) restore(saved) { () } / restore(saved) fail("CALL")))) c1:(pipeline(depth.descend(0,0))) { (c1,) })) { Either::Right(b) })) end:position!() {? cx.node(14,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_input(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(w_or(depth.descend(0,0))) (([token if token.kind == TokenKind::End] { () } / fail("a Cypher value token"))) { (c0,) })) end:position!() {? cx.node(15,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_where(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "WHERE")] { () } / fail("WHERE"))) c1:(w_or(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(16,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_or(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(w_and(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.is_keyword(cx.input, "OR")] { () } / fail("OR"))) c1:(w_and(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(17,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_and(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(w_atom(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.is_keyword(cx.input, "AND")] { () } / fail("AND"))) c1:(w_atom(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(18,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_atom(depth:Depth) -> NodeId = mark:capture_mark() begin:position!() result:((enter(depth) start:position!() children:((a:((((saved:peek_mark() ((&(((([token if token.kind == TokenKind::Word] { () } / fail("a Cypher value token"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) (([token if token.kind == TokenKind::Word] { () } / fail("a Cypher value token"))) (((([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () } / (([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) { () } / (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { () } / (([token if token.kind == TokenKind::NotEqual] { () } / fail("'!'"))) { () } / (([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 2 => word.eq_ignore_ascii_case("IS") || word.eq_ignore_ascii_case("IN"), 4 => word.eq_ignore_ascii_case("ENDS"), 6 => word.eq_ignore_ascii_case("STARTS"), 8 => word.eq_ignore_ascii_case("CONTAINS"), _ => false } } }] { () } / fail("IS"))) { () })) { () }))) restore(saved) { () } / restore(saved) fail("NOT")))) c1:(w_property(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:((a:(w_not(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_bound(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_graph(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_parenthesized_expression(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_group(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_list_contains(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_contains_fn(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_parameter(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_id(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_scalar(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_property(depth.descend(0,0))) { Either::Left(a) } / b:(w_word(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(19,start,end,children) }) / rewind_capture(mark,false) predicate_failure(begin) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_not(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) c1:(w_atom(depth.descend(0,1))) { (c1,) })) end:position!() {? cx.node(20,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_group(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(w_or(depth.descend(0,1))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) end:position!() {? cx.node(21,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_graph(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&((c0:(p_node(depth.descend(0,0))) ((((begin:position!() children:(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () } / ((begin:position!() children:(((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () })) { (c0,) }))) restore(saved) { () } / restore(saved) fail("a relationship existence pattern")))) c1:(p_chain(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(22,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_bound(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "EXISTS")] { () } / fail("EXISTS"))) (([token if token.kind == TokenKind::LeftBrace] { () } / fail("'{'"))) (([token if token.is_keyword(cx.input, "MATCH")] { () } / fail("MATCH"))) c3:(p_chain(depth.descend(0,0))) (([token if token.kind == TokenKind::RightBrace] { () } / fail("'}'"))) { (c3,) })) end:position!() {? cx.node(23,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_list_contains(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&((begin:position!() children:(((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 13 => word.eq_ignore_ascii_case("LIST_CONTAINS"), 19 => word.eq_ignore_ascii_case("LIST_CONTAINS_LOWER"), _ => false } } }] { () } / fail("LIST_CONTAINS"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } }))) restore(saved) { () } / restore(saved) fail("a list containment predicate")))) c1:(v_identifier(depth.descend(0,0))) c2:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(w_reference(depth.descend(0,0))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(v_value(depth.descend(0,0))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,c2) })) end:position!() {? cx.node(24,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_contains_fn(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&((begin:position!() children:(((([token if token.is_keyword(cx.input, "CONTAINS")] { () } / fail("CONTAINS"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } }))) restore(saved) { () } / restore(saved) fail("a containment predicate")))) (([token if token.is_keyword(cx.input, "CONTAINS")] { () } / fail("CONTAINS"))) c2:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(x_scalar(depth.descend(0,1))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(x_scalar(depth.descend(0,1))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c2,) })) end:position!() {? cx.node(25,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_reference(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c2:(v_identifier(depth.descend(0,0))) { (c0,c2) })) end:position!() {? cx.node(26,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_parameter(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_parameter(depth.descend(0,0))) c1:((a:(x_is_null(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_in(depth.descend(0,0))) { Either::Left(a) } / b:(w_comparisons(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { (c0,c1) })) end:position!() {? cx.node(27,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_id_call(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "ID")] { () } / fail("ID"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(28,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_id(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(w_id_call(depth.descend(0,0))) c1:((a:(w_in(depth.descend(0,0))) { Either::Left(a) } / b:((c0:(w_id_op(depth.descend(0,0))) c1:(v_value(depth.descend(0,0))) { (c0,c1) })) { Either::Right(b) })) { (c0,c1) })) end:position!() {? cx.node(29,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_id_op(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { () })) { () } / (((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) { () } / (((([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) { () } / (([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) { () } / (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { () } / (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) end:position!() {? cx.node(30,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_property(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("NOT"), 4 => word.eq_ignore_ascii_case("CASE") || word.eq_ignore_ascii_case("TRUE") || word.eq_ignore_ascii_case("NULL"), 5 => word.eq_ignore_ascii_case("FALSE"), 6 => word.eq_ignore_ascii_case("EXISTS"), _ => false } } }] { () } / fail("NOT")))) restore(saved) { () } / restore(saved) fail("a predicate binding")))) c1:(w_reference(depth.descend(0,0))) c2:((a:(x_is_null(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_in(depth.descend(0,0))) { Either::Left(a) } / b:((a:(w_string(depth.descend(0,0))) { Either::Left(a) } / b:(w_comparisons(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,c2) })) end:position!() {? cx.node(31,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_string(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(w_string_op(depth.descend(0,0))) c1:(v_value(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(32,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_string_op(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CONTAINS")] { () } / fail("CONTAINS"))) { () } / (((([token if token.is_keyword(cx.input, "STARTS")] { () } / fail("STARTS"))) (([token if token.is_keyword(cx.input, "WITH")] { () } / fail("WITH"))) { () })) { () } / (((([token if token.is_keyword(cx.input, "ENDS")] { () } / fail("ENDS"))) (([token if token.is_keyword(cx.input, "WITH")] { () } / fail("WITH"))) { () })) { () } / ((begin:position!() children:(((([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) (([token if token.kind == TokenKind::Tilde] { () } / fail("a Cypher value token"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () })) end:position!() {? cx.node(33,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_word(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("NOT"), 4 => word.eq_ignore_ascii_case("CASE") || word.eq_ignore_ascii_case("TRUE") || word.eq_ignore_ascii_case("NULL"), 5 => word.eq_ignore_ascii_case("FALSE"), 6 => word.eq_ignore_ascii_case("EXISTS"), _ => false } } }] { () } / fail("NOT")))) restore(saved) { () } / restore(saved) fail("a predicate binding")))) c1:(v_identifier(depth.descend(0,0))) c2:(w_expression_tail(depth.descend(0,0))) { (c1,c2) })) end:position!() {? cx.node(34,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_scalar(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:((a:((((saved:peek_mark() ((&(((([token if matches!(token.kind, TokenKind::String | TokenKind::UnterminatedString)] { () } / fail("a Cypher value token"))) { () } / (([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) { () } / (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) { () } / (([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("TRUE") || word.eq_ignore_ascii_case("NULL"), 5 => word.eq_ignore_ascii_case("FALSE"), _ => false } } }] { () } / fail("TRUE"))) { () }))) restore(saved) { () } / restore(saved) fail("a literal operand")))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:((((saved:peek_mark() ((&((a:(([token if token.is_keyword(cx.input, "CASE")] { () } / fail("CASE"))) { Either::Left(a) } / b:((a:((c0:(x_temporal_name(depth.descend(0,0))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { (c0,) })) { Either::Left(a) } / b:((a:(((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) { Either::Left(a) } / b:((a:((c0:(w_reference(depth.descend(0,0))) c1:((a:(([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) { Either::Left(a) } / b:((a:(([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { Either::Left(a) } / b:((a:(([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { Either::Left(a) } / b:((a:(r_sum_op(depth.descend(0,0))) { Either::Left(a) } / b:(r_product_op(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c0,c1) })) { Either::Left(a) } / b:((a:((begin:position!() children:(((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("ABS"), 4 => word.eq_ignore_ascii_case("LEFT") || word.eq_ignore_ascii_case("SIZE") || word.eq_ignore_ascii_case("HEAD") || word.eq_ignore_ascii_case("LAST") || word.eq_ignore_ascii_case("TAIL") || word.eq_ignore_ascii_case("CEIL") || word.eq_ignore_ascii_case("SIGN") || word.eq_ignore_ascii_case("SQRT"), 5 => word.eq_ignore_ascii_case("LOWER") || word.eq_ignore_ascii_case("NODES") || word.eq_ignore_ascii_case("RANGE") || word.eq_ignore_ascii_case("FLOOR") || word.eq_ignore_ascii_case("ROUND"), 6 => word.eq_ignore_ascii_case("LENGTH") || word.eq_ignore_ascii_case("REDUCE"), 7 => word.eq_ignore_ascii_case("REVERSE") || word.eq_ignore_ascii_case("ENDNODE"), 8 => word.eq_ignore_ascii_case("COALESCE"), 9 => word.eq_ignore_ascii_case("STARTNODE"), 10 => word.eq_ignore_ascii_case("JSON_VALUE"), 13 => word.eq_ignore_ascii_case("RELATIONSHIPS"), _ => false } } }] { () } / fail("COALESCE"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { Either::Left(a) } / b:((a:((c0:(w_reference(depth.descend(0,0))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { (c0,) })) { Either::Left(a) } / b:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { (c0,) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) }))) restore(saved) { () } / restore(saved) fail("a scalar operand")))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) { Either::Right(b) })) c1:((saved:diagnostic() begin:position!() result:(c:(w_expression_tail(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) end:position!() {? cx.node(35,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_legacy_scalar_expression(depth:Depth) -> (NodeId, NodeId) =
     ([token if token.kind == TokenKind::LeftParen] / fail("'('"))
     &(
         [token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }]
         / [token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("CASE") || word.eq_ignore_ascii_case("LEFT") || word.eq_ignore_ascii_case("SIZE") || word.eq_ignore_ascii_case("HEAD") || word.eq_ignore_ascii_case("LAST") || word.eq_ignore_ascii_case("TAIL"), 5 => word.eq_ignore_ascii_case("LOWER") || word.eq_ignore_ascii_case("RANGE"), 7 => word.eq_ignore_ascii_case("REVERSE"), 8 => word.eq_ignore_ascii_case("COALESCE"), _ => false } } }]
         / fail("a parenthesized scalar")
     )
     scalar:x_scalar(depth.descend(0,1))
     ([token if token.kind == TokenKind::RightParen] / fail("')'"))
     &(
         [token if token.kind == TokenKind::Equal]
         / [token if token.kind == TokenKind::Less]
         / [token if token.kind == TokenKind::Greater]
         / [token if token.is_keyword(cx.input, "CONTAINS")]
         / [token if token.is_keyword(cx.input, "IN")]
         / fail("a comparison")
     )
     tail:w_expression_tail(depth.descend(0,0))
     { (scalar, tail) }

 rule w_parenthesized_expression(depth:Depth) -> NodeId =
     mark:capture_mark()
     result:(
         (enter(depth)
          start:position!()
          children:(
              legacy:w_legacy_scalar_expression(depth.descend(0,0)) { legacy }
              / (scalar_regex_tail_ahead()
                 expression:r_sum(depth.descend(0,0))
                 tail:w_regex_expression_tail(depth.descend(0,0)) { (expression,tail) })
          )
          end:position!()
          {? cx.node(36,start,end,children) })
         / rewind_capture(mark,false)
           rejected:expected!("matching Cypher rule") { rejected }
     ) { result }

 rule w_regex_expression_tail(depth:Depth) -> NodeId =
     mark:capture_mark()
     result:(
         (enter(depth)
          start:position!()
          children:(
              (begin:position!()
               children:(([token if token.kind == TokenKind::Equal]
                          [token if token.kind == TokenKind::Tilde]) { () })
               end:position!()
               {? if cx.adjacent(begin,end) {
                   Ok(children)
               } else {
                   cx.failure(cx.adjacency_failure(begin,end), "adjacent tokens")
               } })
              pattern:v_value(depth.descend(0,0)) { (pattern,) }
          )
          end:position!()
          {? cx.node(37,start,end,children) })
         / rewind_capture(mark,false)
           rejected:expected!("matching Cypher rule") { rejected }
     ) { result }

 rule w_expression_tail(depth:Depth) -> NodeId =
     mark:capture_mark()
     result:(
         (enter(depth)
          start:position!()
          children:(
              node:x_is_null(depth.descend(0,0)) { (node,) }
              / node:w_in(depth.descend(0,0)) { (node,) }
              / ((begin:position!()
                  children:(([token if token.kind == TokenKind::Equal]
                             [token if token.kind == TokenKind::Tilde]) { () })
                  end:position!()
                  {? if cx.adjacent(begin,end) {
                      Ok(children)
                  } else {
                      cx.failure(cx.adjacency_failure(begin,end), "adjacent tokens")
                  } })
                 pattern:v_value(depth.descend(0,0)) { (pattern,) })
              / ([token if token.is_keyword(cx.input, "CONTAINS")]
                 value:x_scalar(depth.descend(0,1)) { (value,) })
              / node:w_expression_comparisons(depth.descend(0,0)) { (node,) }
          )
          end:position!()
          {? cx.node(37,start,end,children) })
         / rewind_capture(mark,false)
           rejected:expected!("matching Cypher rule") { rejected }
     ) { result }

 rule w_comparisons(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_comparison_op(depth.descend(0,0))) c1:(w_right(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(w_following(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1,c2) })) end:position!() {? cx.node(38,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_expression_comparisons(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_comparison_op(depth.descend(0,0))) c1:(x_case_scalar(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(w_following(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1,c2) })) end:position!() {? cx.node(39,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_following(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_comparison_op(depth.descend(0,0))) c1:(x_case_scalar(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(40,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_right(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("CASE") || word.eq_ignore_ascii_case("TRUE") || word.eq_ignore_ascii_case("NULL"), 5 => word.eq_ignore_ascii_case("FALSE"), _ => false } } }] { () } / fail("CASE")))) restore(saved) { () } / restore(saved) fail("a property reference")))) c1:(w_reference(depth.descend(0,0))) ((saved:peek_mark() ((!((a:(([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) { Either::Left(a) } / b:((a:(([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { Either::Left(a) } / b:((a:(([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { Either::Left(a) } / b:((a:(r_sum_op(depth.descend(0,0))) { Either::Left(a) } / b:(r_product_op(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) }))) restore(saved) { () } / restore(saved) fail("the end of a property reference")))) { (c1,) })) { Either::Left(a) } / b:(x_case_scalar(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(41,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule w_in(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "IN")] { () } / fail("IN"))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(42,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_scalar(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_primary(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_postfix(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(43,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_primary(depth:Depth) -> NodeId = mark:capture_mark() begin:position!() result:((enter(depth) start:position!() children:((a:((((saved:peek_mark() ((&(((([token if token.kind == TokenKind::Word] { () } / fail("a Cypher value token"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) (([token if token.kind == TokenKind::Word] { () } / fail("a Cypher value token"))) ((saved:peek_mark() ((!(([token if token.kind == TokenKind::LeftParen] { () } / fail("'('")))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) { () }))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) ((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("CASE") || word.eq_ignore_ascii_case("TRUE") || word.eq_ignore_ascii_case("NULL") || word.eq_ignore_ascii_case("DATE"), 5 => word.eq_ignore_ascii_case("FALSE"), 8 => word.eq_ignore_ascii_case("DATETIME") || word.eq_ignore_ascii_case("DURATION"), 9 => word.eq_ignore_ascii_case("LOCALTIME"), 13 => word.eq_ignore_ascii_case("LOCALDATETIME"), _ => false } } }] { () } / fail("CASE")))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c2:(x_ref(depth.descend(0,0))) { (c2,) })) { Either::Left(a) } / b:((a:(((([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) c1:(x_condition_paren(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:((a:((((saved:peek_mark() ((&(([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['")))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:(x_list(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:((a:(x_temporal(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_map(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_value(depth.descend(0,0))) { Either::Left(a) } / b:((a:((((saved:peek_mark() ((&((a:(((([token if token.kind == TokenKind::Word] { () } / fail("identifier"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) { Either::Left(a) } / b:((a:(x_identity_name(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_list_function_name(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_metric_name(depth.descend(0,0))) { Either::Left(a) } / b:((a:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("LEFT"), 8 => word.eq_ignore_ascii_case("COALESCE"), 10 => word.eq_ignore_ascii_case("JSON_VALUE") || word.eq_ignore_ascii_case("PROPERTIES"), _ => false } } }] { () } / fail("COALESCE"))) { Either::Left(a) } / b:((a:(x_path_name(depth.descend(0,0))) { Either::Left(a) } / b:((a:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger"))) { Either::Left(a) } / b:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 5 => word.eq_ignore_ascii_case("LOWER"), 9 => word.eq_ignore_ascii_case("DATE_PART"), _ => false } } }] { () } / fail("LOWER"))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) }))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:((a:(x_identity(depth.descend(0,0))) { Either::Left(a) } / b:((a:((((saved:peek_mark() ((&(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger")))) restore(saved) { () } / restore(saved) fail("a conversion function")))) c1:(x_conversion(depth.descend(1,0))) { (c1,) })) { Either::Left(a) } / b:((a:(x_list_function(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_metric(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_coalesce(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_json(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_left(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_path_properties(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_path(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_lower(depth.descend(0,0))) { Either::Left(a) } / b:(x_date(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) { Either::Left(a) } / b:((a:(x_case(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_ref(depth.descend(0,0))) { Either::Left(a) } / b:(x_variable(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(44,start,end,children) }) / rewind_capture(mark,false) identifier_failure(begin) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_conversion(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger")))) restore(saved) { () } / restore(saved) fail("a conversion function")))) c1:(v_identifier(depth.descend(0,0))) c2:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) argument:capture_mark() c1:(a:(x_scalar(depth.descend(0,1))) &[token if token.kind == TokenKind::RightParen] { Either::Left(a) } / rewind_capture(argument,false) b:(x_case_scalar(depth.descend(0,1))) { Either::Right(b) }) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,c2) })) end:position!() {? cx.node(45,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_value(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(v_parameter(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_string(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_number(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_boolean(depth.descend(0,0))) { Either::Left(a) } / b:(v_null(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(46,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_ref(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if token.is_keyword(cx.input, "CASE")] { () } / fail("CASE")))) restore(saved) { () } / restore(saved) fail("a property binding")))) c1:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c3:(v_identifier(depth.descend(0,0))) { (c1,c3) })) end:position!() {? cx.node(47,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_variable(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if token.is_keyword(cx.input, "CASE")] { () } / fail("CASE")))) restore(saved) { () } / restore(saved) fail("a variable")))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(48,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_identity(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_identity_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(49,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_identity_name(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 2 => word.eq_ignore_ascii_case("ID"), 4 => word.eq_ignore_ascii_case("TYPE"), 5 => word.eq_ignore_ascii_case("LABEL"), 7 => word.eq_ignore_ascii_case("ENDNODE"), 9 => word.eq_ignore_ascii_case("STARTNODE"), _ => false } } }] { () } / fail("ID"))) end:position!() {? cx.node(50,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_list_function(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(a:x_reduce_call(depth) { Either::Left(a) } / rewind_capture(mark,false) b:(c0:(x_list_function_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:(x_case_scalar(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) }) { Either::Right(b) }) end:position!() {? cx.node(51,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 // `reduce(accumulator = initial, variable IN list | expression)`. Its
 // captures belong to the enclosing `x_list_function` node: the function
 // name, both identifiers and the three scalar operands, in source order.
 rule x_reduce_call(depth:Depth) -> ((NodeId, NodeId, NodeId), (NodeId, NodeId, NodeId)) =
     &[token if token.is_keyword(cx.input, "REDUCE")]
     c0:x_list_function_name(depth.descend(0,0))
     ([token if token.kind == TokenKind::LeftParen] / fail("'('"))
     c1:v_identifier(depth.descend(0,0))
     ([token if token.kind == TokenKind::Equal] / fail("'='"))
     c2:x_case_scalar(depth.descend(0,0))
     ([token if token.kind == TokenKind::Comma] / fail("','"))
     c3:v_identifier(depth.descend(0,0))
     ([token if token.is_keyword(cx.input, "IN")] / fail("IN"))
     c4:x_case_scalar(depth.descend(0,0))
     ([token if token.kind == TokenKind::Pipe] / fail("'|'"))
     c5:x_case_scalar(depth.descend(0,0))
     ([token if token.kind == TokenKind::RightParen] / fail("')'"))
     { ((c0, c1, c2), (c3, c4, c5)) }

 rule x_list_function_name(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Word && (hawdb_core::ListFunction::from_function_name(token.text(cx.input)).is_some() || hawdb_core::StringFunction::from_function_name(token.text(cx.input)).is_some() || hawdb_core::NumericFunction::from_function_name(token.text(cx.input)).is_some() || token.is_keyword(cx.input, "REDUCE"))] { () } / fail("SIZE"))) end:position!() {? cx.node(52,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_metric(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_metric_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(x_scalar(depth.descend(0,1))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(x_scalar(depth.descend(0,1))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(53,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_metric_name(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 11 => word.eq_ignore_ascii_case("L2_DISTANCE"), 15 => word.eq_ignore_ascii_case("COSINE_DISTANCE"), _ => false } } }] { () } / fail("COSINE_DISTANCE"))) end:position!() {? cx.node(54,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_coalesce(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "COALESCE")] { () } / fail("COALESCE"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(x_scalar(depth.descend(0,1))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(x_scalar(depth.descend(0,1))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(55,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_json_null_on_nonscalar() =
     ([token if token.is_keyword(cx.input, "NULL")] / fail("NULL"))
     ([token if token.is_keyword(cx.input, "ON")] / fail("ON"))
     ([token if token.is_keyword(cx.input, "NONSCALAR")] / fail("NONSCALAR"))

 rule x_json(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "JSON_VALUE")] { () } / fail("JSON_VALUE"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(x_scalar(depth.descend(0,1))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(v_value(depth.descend(0,0))) { (c0,c2) })) x_json_null_on_nonscalar()? (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(56,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_left(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "LEFT")] { () } / fail("LEFT"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(x_scalar(depth.descend(0,1))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(v_value(depth.descend(0,0))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(57,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_lower(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "LOWER")] { () } / fail("LOWER"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(x_scalar(depth.descend(0,1))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(58,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_path_properties(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "PROPERTIES")] { () } / fail("PROPERTIES"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(((([token if token.is_keyword(cx.input, "NODES")] { () } / fail("NODES"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c3:(v_value(depth.descend(0,0))) { (c1,c3) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(59,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_path(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_path_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(60,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_path_name(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 5 => word.eq_ignore_ascii_case("NODES"), 6 => word.eq_ignore_ascii_case("LENGTH"), 13 => word.eq_ignore_ascii_case("RELATIONSHIPS"), _ => false } } }] { () } / fail("LENGTH"))) end:position!() {? cx.node(61,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_date(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "DATE_PART")] { () } / fail("DATE_PART"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(v_value(depth.descend(0,0))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(x_ref(depth.descend(0,0))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(62,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_list(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(x_comprehension(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_value_list(depth.descend(0,0))) { Either::Left(a) } / b:(x_expression_list(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(63,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_value_list(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(v_list(depth.descend(0,0))) end:position!() {? cx.node(64,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_expression_list(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) c1:(x_expression_list_contents(depth.descend(1,0))) (([token if token.kind == TokenKind::RightBracket] { () } / fail("',' or ']'"))) { (c1,) })) end:position!() {? cx.node(65,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_expression_list_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((saved:diagnostic() begin:position!() result:(c:((c0:((c0:(x_or(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(x_or(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) ((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Comma] { () } / fail("','"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) end:position!() {? cx.node(66,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_comprehension(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) c1:((a:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("TRUE") || word.eq_ignore_ascii_case("NULL"), 5 => word.eq_ignore_ascii_case("FALSE"), _ => false } } }] { () } / fail("TRUE")))) restore(saved) { () } / restore(saved) fail("a comprehension variable")))) c2:(v_identifier(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IN")] { () } / fail("IN"))) c4:(x_comprehension_contents(depth.descend(1,0))) { (c2,c4) })) { Either::Left(a) } / b:((c0:(w_graph(depth.descend(1,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_comprehension_where(depth.descend(1,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c2:(x_comprehension_projection(depth.descend(1,0))) { (c0,c1,c2) })) { Either::Right(b) })) (([token if token.kind == TokenKind::RightBracket] { () } / fail("']'"))) { (c1,) })) end:position!() {? cx.node(67,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_comprehension_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_case_scalar(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_comprehension_where(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c2:((saved:diagnostic() begin:position!() result:(c:(x_comprehension_projection(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1,c2) })) end:position!() {? cx.node(68,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_comprehension_where(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "WHERE")] { () } / fail("WHERE"))) c1:(x_or(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(69,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_comprehension_projection(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Pipe] { () } / fail("'|'"))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(70,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_postfix(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((begin:position!() children:(((([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { Either::Left(a) } / b:(((([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) c1:(x_postfix_contents(depth.descend(0,0))) (([token if token.kind == TokenKind::RightBracket] { () } / fail("']'"))) { (c1,) })) { Either::Right(b) })) end:position!() {? cx.node(71,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_postfix_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((c0:(x_dots(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_case_scalar(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) { Either::Left(a) } / b:((c0:(x_case_scalar(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_slice_tail(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) { Either::Right(b) })) end:position!() {? cx.node(72,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_dots(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((begin:position!() children:(((([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) end:position!() {? cx.node(73,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_slice_tail(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_dots(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_case_scalar(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) end:position!() {? cx.node(74,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_case_scalar(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(x_sum(depth.descend(0,0))) end:position!() {? cx.node(75,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_timestamp_value(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&(((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("CAST"), 9 => word.eq_ignore_ascii_case("TIMESTAMP"), 17 => word.eq_ignore_ascii_case("CURRENT_TIMESTAMP"), _ => false } } }] { () } / fail("CURRENT_TIMESTAMP"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () }))) restore(saved) { () } / restore(saved) fail("a timestamp value")))) c1:(v_value(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(76,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_or(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_and(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_or_rest(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(77,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_or_rest(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "OR")] { () } / fail("OR"))) c1:(x_and(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(78,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_and(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_condition(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_and_rest(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(79,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_and_rest(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "AND")] { () } / fail("AND"))) c1:(x_condition(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(80,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_condition(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(x_not(depth.descend(0,0))) { Either::Left(a) } / b:((c0:(x_condition_base(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_condition_tail(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) { Either::Right(b) })) end:position!() {? cx.node(81,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_not(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) c1:(x_not_contents(depth.descend(1,0))) { (c1,) })) end:position!() {? cx.node(82,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_not_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(x_condition(depth.descend(0,0))) end:position!() {? cx.node(83,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_condition_base(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(w_graph(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_condition_paren(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_list_contains(depth.descend(0,0))) { Either::Left(a) } / b:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("NOT"), 13 => word.eq_ignore_ascii_case("LIST_CONTAINS"), _ => false } } }] { () } / fail("NOT")))) restore(saved) { () } / restore(saved) fail("a condition")))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(84,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_condition_paren(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(x_or(depth.descend(0,1))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) end:position!() {? cx.node(85,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_list_contains(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "LIST_CONTAINS")] { () } / fail("LIST_CONTAINS"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(x_case_scalar(depth.descend(0,0))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(x_case_scalar(depth.descend(0,0))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(86,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_condition_tail(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(x_is_null(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_comparisons(depth.descend(0,0))) { Either::Left(a) } / b:(x_contains(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(87,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_is_null(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) c1:((saved:diagnostic() begin:position!() result:(c:(x_negated(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { (c1,) })) end:position!() {? cx.node(88,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_negated(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) end:position!() {? cx.node(89,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_comparisons(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_comparison(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_comparison(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(90,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_comparison(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_comparison_op(depth.descend(0,0))) c1:(x_case_scalar(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(91,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_comparison_op(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((begin:position!() children:(((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () } / ((begin:position!() children:(((([token if token.kind == TokenKind::NotEqual] { () } / fail("'!'"))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () } / ((begin:position!() children:(((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () } / ((begin:position!() children:(((([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () } / (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () } / (([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) { () } / (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { () })) end:position!() {? cx.node(92,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_contains(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_contains_op(depth.descend(0,0))) c1:(x_case_scalar(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(93,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_contains_op(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 2 => word.eq_ignore_ascii_case("IN"), 8 => word.eq_ignore_ascii_case("CONTAINS"), _ => false } } }] { () } / fail("CONTAINS"))) end:position!() {? cx.node(94,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_case(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CASE")] { () } / fail("CASE"))) c1:((a:(x_floor_case(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_reserved_special_case(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_special_case(depth.descend(0,0))) { Either::Left(a) } / b:(x_case_body(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) end:position!() {? cx.node(95,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_reserved_special_case(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) (([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("true") || word.eq_ignore_ascii_case("null") || word.eq_ignore_ascii_case("CASE"), 5 => word.eq_ignore_ascii_case("false"), _ => false } } }] { () } / fail("true"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) { () }))) restore(saved) { () } / restore(saved) fail("a specialized property reference")))) c1:((a:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:((c0:(d_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { (c0,) })) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c3:(x_special_lower(depth.descend(0,0))) c4:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c6:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c3,c4,c6) })) { Either::Left(a) } / b:((a:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(d_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) (([token if token.is_keyword(cx.input, "OR")] { () } / fail("OR"))) c5:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c7:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c9:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c11:(d_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c5,c7,c9,c11) })) { Either::Left(a) } / b:((a:((c0:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c3:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c5:(v_value(depth.descend(0,0))) { (c1,c3,c5) })) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c3:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c5:(v_value(depth.descend(0,0))) { (c1,c3,c5) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c3:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c0,c1,c3) })) { Either::Left(a) } / b:((a:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:((c0:(d_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { (c0,) })) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c3:(d_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c5:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c3,c5) })) { Either::Left(a) } / b:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:((c0:(d_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { (c0,) })) (([token if token.is_keyword(cx.input, "AND")] { () } / fail("AND"))) c3:(d_ref(depth.descend(0,0))) ((((begin:position!() children:(((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () } / ((begin:position!() children:(((([token if token.kind == TokenKind::NotEqual] { () } / fail("'!'"))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () })) c5:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c7:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c9:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c3,c5,c7,c9) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) end:position!() {? cx.node(96,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_special_lower(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "LOWER")] { () } / fail("LOWER"))) end:position!() {? cx.node(97,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_special_case(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((&((a:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:((c0:(x_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { (c0,) })) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) (([token if token.is_keyword(cx.input, "LOWER")] { () } / fail("LOWER"))) c4:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(x_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c6:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c4,c6) })) { Either::Left(a) } / b:((a:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(x_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) (([token if token.is_keyword(cx.input, "OR")] { () } / fail("OR"))) c5:(x_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c7:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c9:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c11:(x_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c5,c7,c9,c11) })) { Either::Left(a) } / b:((a:((c0:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(x_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c3:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c5:(v_value(depth.descend(0,0))) { (c1,c3,c5) })) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(x_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c3:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c5:(v_value(depth.descend(0,0))) { (c1,c3,c5) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c3:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c0,c1,c3) })) { Either::Left(a) } / b:((a:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:((c0:(x_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { (c0,) })) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c3:(x_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c5:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c3,c5) })) { Either::Left(a) } / b:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:((c0:(x_ref(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS"))) (([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { (c0,) })) (([token if token.is_keyword(cx.input, "AND")] { () } / fail("AND"))) c3:(x_ref(depth.descend(0,0))) ((((begin:position!() children:(((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () } / ((begin:position!() children:(((([token if token.kind == TokenKind::NotEqual] { () } / fail("'!'"))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { () })) c5:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c7:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c9:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c3,c5,c7,c9) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) }))) restore(saved) { () } / restore(saved) fail("a specialized CASE shape")))) c1:(x_case_body(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(98,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_case_body(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(x_searched_case(depth.descend(0,0))) { Either::Left(a) } / b:(x_simple_case(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(99,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_searched_case(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_searched_branch(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_searched_branch(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) c2:((saved:diagnostic() begin:position!() result:(c:(x_else(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c0,c1,c2) })) end:position!() {? cx.node(100,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_searched_branch(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(x_or(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c3:(x_case_scalar(depth.descend(0,0))) { (c1,c3) })) end:position!() {? cx.node(101,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_simple_case(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN")))) restore(saved) { () } / restore(saved) fail("a CASE operand")))) c1:(x_or(depth.descend(0,0))) c2:(x_simple_branch(depth.descend(0,0))) c3:((saved:diagnostic() begin:position!() result:(c:(x_simple_branch(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) c4:((saved:diagnostic() begin:position!() result:(c:(x_else(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c2,c3,c4) })) end:position!() {? cx.node(102,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_simple_branch(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(x_case_scalar(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c3:(x_case_scalar(depth.descend(0,0))) { (c1,c3) })) end:position!() {? cx.node(103,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_else(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(104,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_floor_case(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c1:(x_difference(depth.descend(0,0))) (([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) c3:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c5:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c7:(x_difference(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c1,c3,c5,c7) })) end:position!() {? cx.node(105,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_difference(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_difference_term(depth.descend(0,0))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c2:(x_difference_term(depth.descend(0,0))) c3:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c1:(x_difference_term(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c2,c3) })) end:position!() {? cx.node(106,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_difference_term(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "COALESCE")] { () } / fail("COALESCE"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(v_value(depth.descend(0,0))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(107,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_temporal_name(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((((begin:position!() children:(((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("date"), 8 => word.eq_ignore_ascii_case("datetime") || word.eq_ignore_ascii_case("duration"), 9 => word.eq_ignore_ascii_case("localtime"), 13 => word.eq_ignore_ascii_case("localdatetime"), _ => false } } }] { () } / fail("date"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("date"), 8 => word.eq_ignore_ascii_case("datetime") || word.eq_ignore_ascii_case("duration"), 9 => word.eq_ignore_ascii_case("localtime"), 13 => word.eq_ignore_ascii_case("localdatetime"), _ => false } } }] { () } / fail("date"))) { Either::Right(b) })) end:position!() {? cx.node(108,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_temporal(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_temporal_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:(x_case_scalar(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(x_case_scalar(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(109,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_temporal(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_temporal_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:((a:(v_map(depth.descend(1,0))) { Either::Left(a) } / b:(v_value(depth.descend(0,0))) { Either::Right(b) })) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:((a:(v_map(depth.descend(1,0))) { Either::Left(a) } / b:(v_value(depth.descend(0,0))) { Either::Right(b) })) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(110,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_map(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBrace] { () } / fail("'{'"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c2:(x_case_scalar(depth.descend(0,0))) { (c0,c2) })) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c2:(x_case_scalar(depth.descend(0,0))) { (c0,c2) })) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) ((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Comma] { () } / fail("','"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightBrace] { () } / fail("'}'"))) { (c1,) })) end:position!() {? cx.node(111,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_map(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBrace] { () } / fail("'{'"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c2:((a:(v_map(depth.descend(1,0))) { Either::Left(a) } / b:(v_value(depth.descend(0,0))) { Either::Right(b) })) { (c0,c2) })) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c2:((a:(v_map(depth.descend(1,0))) { Either::Left(a) } / b:(v_value(depth.descend(0,0))) { Either::Right(b) })) { (c0,c2) })) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) ((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Comma] { () } / fail("','"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightBrace] { () } / fail("'}'"))) { (c1,) })) end:position!() {? cx.node(112,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_sum(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_product(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:(r_sum_op(depth.descend(0,0))) c1:(x_product(depth.descend(0,0))) { (c0,c1) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(113,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_product(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_operand(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:(r_product_op(depth.descend(0,0))) c1:(x_operand(depth.descend(0,0))) { (c0,c1) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(114,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule x_operand(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) { () }))) restore(saved) { () } / restore(saved) fail("a scalar arithmetic operand")))) c1:((a:(x_timestamp_value(depth.descend(0,0))) { Either::Left(a) } / b:((a:(x_scalar(depth.descend(0,1))) { Either::Left(a) } / b:((a:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(x_or(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { Either::Left(a) } / b:(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c1:(x_operand(depth.descend(0,0))) { (c1,) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) end:position!() {? cx.node(115,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule with_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "WITH")] { () } / fail("WITH"))) c1:(r_projection(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(p_where(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c3:((saved:diagnostic() begin:position!() result:(c:(r_order(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c4:((saved:diagnostic() begin:position!() result:(c:(r_offset(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c5:((saved:diagnostic() begin:position!() result:(c:(r_limit(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2,c3,c4,c5) })) end:position!() {? cx.node(116,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule return_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "RETURN")] { () } / fail("RETURN"))) c1:(r_projection(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(r_order(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c3:((saved:diagnostic() begin:position!() result:(c:(r_offset(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c4:((saved:diagnostic() begin:position!() result:(c:(r_limit(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2,c3,c4) })) end:position!() {? cx.node(117,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule projection_start() = {cx.begin_projection();}
 rule item_mark() -> (usize,usize) = {(cx.nodes.len(),cx.edges.len())}
 rule typed_projection_item(depth:Depth) = mark:item_mark() node:r_item(depth) {cx.projection_item(mark,node,depth.statement);}
 rule r_projection(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() result:(
  eager_projection() projection_start()
  distinct:(saved:diagnostic() begin:position!() result:(item:r_distinct(depth) {Some(item)} / restore_absence(begin,saved) {None}) {result})
  typed_projection_item(depth) (([token if token.kind==TokenKind::Comma]) typed_projection_item(depth))*
  end:position!() {? cx.finish_projection(118,start,end,distinct) }
  / !eager_projection() children:((c0:((saved:diagnostic() begin:position!() result:(c:(r_distinct(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c1:((c0:(r_item(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(r_item(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { (c0,c1) })) end:position!() {? cx.node(118,start,end,children) }
 ) { result }) / rewind_capture(mark,true) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_distinct(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "DISTINCT")] { () } / fail("DISTINCT"))) end:position!() {? cx.node(119,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_item(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_sum(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_is_null(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c2:((saved:diagnostic() begin:position!() result:(c:(r_alias(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1,c2) })) end:position!() {? cx.node(120,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_alias(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "AS")] { () } / fail("AS"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(121,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_sum(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_product(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(r_sum_rest(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(122,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_sum_rest(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_sum_op(depth.descend(0,0))) c1:(r_product(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(123,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_sum_op(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Plus] { () } / fail("'+'"))) { () } / (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) { () })) end:position!() {? cx.node(124,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_product(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_atom(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(r_product_rest(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(125,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_product_rest(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_product_op(depth.descend(0,0))) c1:(r_atom(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(126,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_product_op(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Star] { () } / fail("'*'"))) { () } / (([token if token.kind == TokenKind::Slash] { () } / fail("'/'"))) { () } / (([token if token.kind == TokenKind::Percent] { () } / fail("'%'"))) { () })) end:position!() {? cx.node(127,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_atom(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((((saved:peek_mark() ((&(([token if token.kind == TokenKind::LeftParen] { () } / fail("'('")))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:(r_paren(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:((a:((((saved:peek_mark() ((&(([token if token.kind == TokenKind::Minus] { () } / fail("'-'")))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:(r_unary(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:((a:((((saved:peek_mark() ((&((a:(((([token if token.kind == TokenKind::Word] { () } / fail("identifier"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) { Either::Left(a) } / b:((a:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 6 => word.eq_ignore_ascii_case("LENGTH"), 10 => word.eq_ignore_ascii_case("PROPERTIES"), 11 => word.eq_ignore_ascii_case("COLLECT_TOP"), _ => false } } }] { () } / fail("PROPERTIES"))) { Either::Left(a) } / b:((a:(r_aggregate_name(depth.descend(0,0))) { Either::Left(a) } / b:(x_list_function_name(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) }))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:((a:(r_path(depth.descend(0,0))) { Either::Left(a) } / b:((a:(r_collect_top(depth.descend(0,0))) { Either::Left(a) } / b:((a:(r_aggregate(depth.descend(0,0))) { Either::Left(a) } / b:(r_list_function(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { (c1,) })) { Either::Left(a) } / b:((((((saved:peek_mark() ((!(([token if token.kind == TokenKind::Minus] { () } / fail("'-'")))) restore(saved) { () } / restore(saved) fail("a scalar atom")))) { () } / ((saved:peek_mark() ((&((begin:position!() children:(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } }))) restore(saved) { () } / restore(saved) fail("a negative numeric literal")))) { () })) c1:(x_scalar(depth.descend(0,1))) { (c1,) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(128,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_paren(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(r_nested_sum(depth.descend(1,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) end:position!() {? cx.node(129,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_nested_sum(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(r_sum(depth.descend(0,0))) end:position!() {? cx.node(130,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_unary(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!((saved:peek_mark() ((&((begin:position!() children:(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } }))) restore(saved) { () } / restore(saved) fail("a negative numeric literal"))))) restore(saved) { () } / restore(saved) fail("a non-literal unary operand")))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c2:(r_unary_contents(depth.descend(1,0))) { (c2,) })) end:position!() {? cx.node(131,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_unary_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(r_atom(depth.descend(0,0))) end:position!() {? cx.node(132,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_path(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(x_path_properties(depth.descend(0,0))) { Either::Left(a) } / b:(((([token if token.is_keyword(cx.input, "LENGTH")] { () } / fail("LENGTH"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) { Either::Right(b) })) end:position!() {? cx.node(133,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_collect_top(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "COLLECT_TOP")] { () } / fail("COLLECT_TOP"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c4:(v_value(depth.descend(0,0))) { (c0,c2,c4) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(134,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_aggregate(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_aggregate_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(r_aggregate_contents(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(135,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_aggregate_name(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("SUM") || word.eq_ignore_ascii_case("MIN") || word.eq_ignore_ascii_case("MAX") || word.eq_ignore_ascii_case("AVG"), 5 => word.eq_ignore_ascii_case("COUNT"), 7 => word.eq_ignore_ascii_case("COLLECT"), _ => false } } }] { () } / fail("COUNT"))) end:position!() {? cx.node(136,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_aggregate_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:((saved:diagnostic() begin:position!() result:(c:(r_distinct(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c1:((a:(r_star(depth.descend(0,0))) { Either::Left(a) } / b:(r_sum(depth.descend(0,1))) { Either::Right(b) })) { (c0,c1) })) end:position!() {? cx.node(137,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_star(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Star] { () } / fail("'*'"))) end:position!() {? cx.node(138,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_list_function(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_list_call(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_postfix(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(139,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_list_call(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(x_list_function_name(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:(r_sum(depth.descend(0,1))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(r_sum(depth.descend(0,1))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(140,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule order_start() = {cx.begin_order();}
 rule typed_order_item(depth:Depth) = mark:item_mark() node:r_order_item(depth) {cx.order_item(mark,node,depth.statement);}
 rule r_order(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() result:(
 eager_projection() ([token if token.is_keyword(cx.input,"ORDER")] {()} / fail("ORDER"))
 ([token if token.is_keyword(cx.input,"BY")] {()} / fail("BY")) order_start()
 typed_order_item(depth) (([token if token.kind==TokenKind::Comma]) typed_order_item(depth))*
 end:position!() {? cx.finish_order(141,start,end) }
 / !eager_projection() children:((((([token if token.is_keyword(cx.input, "ORDER")] { () } / fail("ORDER"))) (([token if token.is_keyword(cx.input, "BY")] { () } / fail("BY"))) c2:((c0:(r_order_item(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(r_order_item(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { (c2,) }))) end:position!() {? cx.node(141,start,end,children) }
 ) {result}) / rewind_order(mark) rejected:expected!("matching Cypher rule") {rejected}) {result}
 rule rewind_order(mark:(usize,usize,usize)) = {cx.rewind(mark,false);cx.order=None;}

 rule r_order_item(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_order_expression(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(r_direction(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) end:position!() {? cx.node(142,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_direction(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("ASC"), 4 => word.eq_ignore_ascii_case("DESC"), _ => false } } }] { () } / fail("ASC"))) end:position!() {? cx.node(143,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_order_expression(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(r_sum(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(x_is_null(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) end:position!() {? cx.node(144,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_offset(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("SKIP"), 6 => word.eq_ignore_ascii_case("OFFSET"), _ => false } } }] { () } / fail("SKIP"))) c1:(v_value(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(145,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule r_limit(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "LIMIT")] { () } / fail("LIMIT"))) c1:(v_value(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(146,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_legacy_call(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CALL")] { () } / fail("CALL"))) ((saved:peek_mark() ((&(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("louvain"), 8 => word.eq_ignore_ascii_case("pagerank"), 9 => word.eq_ignore_ascii_case("page_rank"), _ => false } } }] { () } / fail("page_rank")))) restore(saved) { () } / restore(saved) fail("a graph algorithm")))) c2:(u_procedure(depth.descend(0,0))) c3:((saved:diagnostic() begin:position!() result:(c:(u_legacy_return(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) ((saved:peek_mark() ((&(((([token if token.kind == TokenKind::Semicolon] { () } / fail("a Cypher value token"))) { () } / (([token if token.kind == TokenKind::End] { () } / fail("a Cypher value token"))) { () }))) restore(saved) { () } / restore(saved) fail("','")))) { (c2,c3) })) end:position!() {? cx.node(147,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_legacy_return(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "RETURN")] { () } / fail("RETURN"))) c1:((c0:(v_identifier(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) c2:((saved:diagnostic() begin:position!() result:(c:(u_legacy_trailing_comma(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2) })) end:position!() {? cx.node(148,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_legacy_trailing_comma(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Comma] { () } / fail("','"))) end:position!() {? cx.node(149,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule call_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CALL")] { () } / fail("CALL"))) c1:(u_procedure(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(u_yields(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2) })) end:position!() {? cx.node(150,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_procedure(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_identifier(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:(u_arg(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(u_arg(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) end:position!() {? cx.node(151,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_arg(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(u_named(depth.descend(0,0))) { Either::Left(a) } / b:(u_option(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(152,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_named(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_identifier(depth.descend(0,0))) named_assignment() c2:(u_option(depth.descend(0,0))) { (c0,c2) })) end:position!() {? cx.node(153,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_option(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(v_value(depth.descend(0,0))) { Either::Left(a) } / b:((a:(u_map(depth.descend(0,0))) { Either::Left(a) } / b:(v_identifier(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(154,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_map(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBrace] { () } / fail("'{'"))) c1:(u_map_contents(depth.descend(1,0))) (([token if token.kind == TokenKind::RightBrace] { () } / fail("'}'"))) { (c1,) })) end:position!() {? cx.node(155,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_map_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((saved:diagnostic() begin:position!() result:(c:((c0:((c0:(u_map_entry(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(u_map_entry(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) ((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Comma] { () } / fail("','"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) end:position!() {? cx.node(156,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_map_entry(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(u_key(depth.descend(0,0))) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c2:(u_option(depth.descend(0,0))) { (c0,c2) })) end:position!() {? cx.node(157,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_key(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(v_string(depth.descend(0,0))) { Either::Left(a) } / b:(v_identifier(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(158,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_yields(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "YIELD")] { () } / fail("YIELD"))) c1:((c0:(u_yield(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(u_yield(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { (c1,) })) end:position!() {? cx.node(159,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_yield(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_identifier(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(u_yield_alias(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) end:position!() {? cx.node(160,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule u_yield_alias(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "AS")] { () } / fail("AS"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(161,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule merge(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "MERGE")] { () } / fail("MERGE"))) c1:(d_merge_data(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(162,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_patterns(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(p_pattern(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(p_pattern(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(163,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_pattern(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:((saved:diagnostic() begin:position!() result:(c:(p_binding(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c1:((a:((c0:(p_function(depth.descend(0,0))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(p_chain(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c0,c1) })) { Either::Left(a) } / b:(p_chain(depth.descend(0,0))) { Either::Right(b) })) { (c0,c1) })) end:position!() {? cx.node(164,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_binding(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) { (c0,) })) end:position!() {? cx.node(165,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_function(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 12 => word.eq_ignore_ascii_case("shortestPath"), 16 => word.eq_ignore_ascii_case("allShortestPaths"), _ => false } } }] { () } / fail("shortestPath"))) end:position!() {? cx.node(166,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_chain(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(p_node(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(p_step(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(167,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_node(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:((a:(p_variable(depth.descend(0,0))) { Either::Left(a) } / b:((saved:peek_mark() ((&(((([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) { () } / (([token if token.kind == TokenKind::LeftBrace] { () } / fail("'{'"))) { () } / (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { () }))) restore(saved) { () } / restore(saved) fail("identifier")))) { Either::Right(b) })) c1:((saved:diagnostic() begin:position!() result:(c:(p_labels(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c2:((saved:diagnostic() begin:position!() result:(c:(v_properties(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) end:position!() {? cx.node(168,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_variable(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(v_identifier(depth.descend(0,0))) end:position!() {? cx.node(169,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_labels(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c1:(v_identifier(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c1,c2) })) end:position!() {? cx.node(170,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_step(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(p_relationship(depth.descend(0,0))) c1:(p_node(depth.descend(0,0))) { (c0,c1) })) end:position!() {? cx.node(171,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_relationship(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(p_incoming(depth.descend(0,0))) { Either::Left(a) } / b:(p_outgoing(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(172,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_incoming(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c2:(p_rel_body(depth.descend(0,0))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) { (c2,) })) end:position!() {? cx.node(173,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_outgoing(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c1:(p_rel_body(depth.descend(0,0))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c3:((saved:diagnostic() begin:position!() result:(c:(p_arrow(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c3) })) end:position!() {? cx.node(174,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_arrow(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) end:position!() {? cx.node(175,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_rel_body(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) c1:((a:(p_rel_type(depth.descend(0,0))) { Either::Left(a) } / b:((a:((c0:(p_variable(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(p_rel_type(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) { Either::Left(a) } / b:((saved:peek_mark() ((&(((([token if token.kind == TokenKind::RightBracket] { () } / fail("']'"))) { () } / (([token if token.kind == TokenKind::Star] { () } / fail("'*'"))) { () }))) restore(saved) { () } / restore(saved) fail("a relationship variable or type")))) { Either::Right(b) })) { Either::Right(b) })) c2:((saved:diagnostic() begin:position!() result:(c:(p_hops(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c3:((saved:diagnostic() begin:position!() result:(c:(v_properties(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightBracket] { () } / fail("']'"))) { (c1,c2,c3) })) end:position!() {? cx.node(176,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_rel_type(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c1:(v_identifier(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Pipe] { () } / fail("'|'"))) ((saved:diagnostic() begin:position!() result:(([token if token.kind == TokenKind::Colon] { () }) { () } / restore_absence(begin,saved) { () }) { result })) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c1,c2) })) end:position!() {? cx.node(177,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_hops(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Star] { () } / fail("'*'"))) c1:((saved:diagnostic() begin:position!() result:(c:(p_selector(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c2:((saved:diagnostic() begin:position!() result:(c:(p_min(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) c3:((saved:diagnostic() begin:position!() result:(c:(p_range(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2,c3) })) end:position!() {? cx.node(178,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_selector(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("ANY") || word.eq_ignore_ascii_case("ALL"), _ => false } } }] { () } / fail("ANY"))) (([token if token.is_keyword(cx.input, "SHORTEST")] { () } / fail("SHORTEST"))) { () })) end:position!() {? cx.node(179,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_min(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) end:position!() {? cx.node(180,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_range(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c1:((a:((begin:position!() children:(((([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c1:(p_max(depth.descend(0,0))) { (c1,) })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) { Either::Left(a) } / b:(([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) { Either::Right(b) })) { (c1,) })) end:position!() {? cx.node(181,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule p_max(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) end:position!() {? cx.node(182,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule match_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "MATCH")] { () } / fail("MATCH"))) c1:(p_patterns(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(p_where(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2) })) end:position!() {? cx.node(183,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule optional_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "OPTIONAL")] { () } / fail("OPTIONAL"))) (([token if token.is_keyword(cx.input, "MATCH")] { () } / fail("MATCH"))) c2:(p_patterns(depth.descend(0,0))) c3:((saved:diagnostic() begin:position!() result:(c:(p_where(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c2,c3) })) end:position!() {? cx.node(184,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule unwind_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "UNWIND")] { () } / fail("UNWIND"))) c1:((&([token if token.kind == TokenKind::LeftBracket || token.kind == TokenKind::Parameter] / ([token if token.kind == TokenKind::Word] !([token if token.kind == TokenKind::LeftParen]))) value:v_value(depth.descend(0,0)) &([token if token.is_keyword(cx.input, "AS")]) { value } / x_case_scalar(depth.descend(0,0)))) (([token if token.is_keyword(cx.input, "AS")] { () } / fail("AS"))) c3:(v_identifier(depth.descend(0,0))) { (c1,c3) })) end:position!() {? cx.node(185,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule load_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "LOAD")] { () } / fail("LOAD"))) (([token if token.is_keyword(cx.input, "CSV")] { () } / fail("CSV"))) ((saved:diagnostic() begin:position!() result:(c:(((([token if token.is_keyword(cx.input, "WITH")] { () } / fail("WITH"))) (([token if token.is_keyword(cx.input, "HEADERS")] { () } / fail("HEADERS"))) { () })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.is_keyword(cx.input, "FROM")] { () } / fail("FROM"))) ((saved:peek_mark() ((&(((([token if token.kind == TokenKind::Parameter] { () } / fail("a Cypher value token"))) { () } / (([token if matches!(token.kind, TokenKind::String | TokenKind::UnterminatedString)] { () } / fail("a Cypher value token"))) { () }))) restore(saved) { () } / restore(saved) fail("LOAD CSV FROM accepts only a string literal or parameter source")))) c5:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "AS")] { () } / fail("AS"))) c7:(v_identifier(depth.descend(0,0))) c8:((saved:diagnostic() begin:position!() result:(c:(((([token if token.is_keyword(cx.input, "FIELDTERMINATOR")] { () } / fail("FIELDTERMINATOR"))) ((saved:peek_mark() ((&(([token if matches!(token.kind, TokenKind::String | TokenKind::UnterminatedString)] { () } / fail("a Cypher value token")))) restore(saved) { () } / restore(saved) fail("FIELDTERMINATOR requires a string literal")))) c2:(v_value(depth.descend(0,0))) { (c2,) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c5,c7,c8) })) end:position!() {? cx.node(186,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule create_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CREATE")] { () } / fail("CREATE"))) c1:(p_patterns(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(187,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule merge_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "MERGE")] { () } / fail("MERGE"))) c1:(p_pattern(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(d_on_sets(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c1,c2) })) end:position!() {? cx.node(188,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule set_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "SET")] { () } / fail("SET"))) c1:(d_sets(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(189,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule delete_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "DELETE")] { () } / fail("DELETE"))) c1:((c0:(v_identifier(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { (c1,) })) end:position!() {? cx.node(190,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule detach_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "DETACH")] { () } / fail("DETACH"))) (([token if token.is_keyword(cx.input, "DELETE")] { () } / fail("DELETE"))) c2:((c0:(v_identifier(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) { (c2,) })) end:position!() {? cx.node(191,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_identifier(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Word] { () } / fail("identifier"))) end:position!() {? cx.node(192,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_value(depth:Depth) -> NodeId = mark:capture_mark() begin:position!() saved:diagnostic() result:((enter(depth) start:position!() children:((a:(v_parameter(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_list(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_string(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_number(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_boolean(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_null(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_current_timestamp(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_timestamp(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_cast(depth.descend(0,0))) { Either::Left(a) } / b:((a:(v_temporal(depth.descend(1,0))) { Either::Left(a) } / b:(v_property(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(193,start,end,children) }) / rewind_capture(mark,false) value_failure(begin,saved) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_parameter(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Parameter] { () } / fail("a Cypher value token"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(194,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_list(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) c1:(v_list_contents(depth.descend(1,0))) (([token if token.kind == TokenKind::RightBracket] { () } / fail("',' or ']'"))) { (c1,) })) end:position!() {? cx.node(195,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_list_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((saved:diagnostic() begin:position!() result:(c:((c0:((c0:(v_value(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(v_value(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) ((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Comma] { () } / fail("','"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) end:position!() {? cx.node(196,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_string(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::String] { () } / ([token if token.kind == TokenKind::UnterminatedString] { () }) fail("unterminated string") / fail("a Cypher value token"))) end:position!() {? cx.node(197,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_number(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((begin:position!() children:((((((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) ((saved:peek_mark() ((!(([token if token.kind == TokenKind::Dot] { () } / fail("'.'")))) restore(saved) { () } / restore(saved) fail("a decimal point")))) ((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { () })) { () } / (((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) (([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) { () })) { () } / ((((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::Number] { () } / fail("integer digits"))) { () })) { () })) end:position!() {? if cx.adjacent(begin,end) { Ok(children) } else { cx.failure(cx.adjacency_failure(begin,end),"adjacent tokens") } })) end:position!() {? cx.node(198,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_boolean(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("true"), 5 => word.eq_ignore_ascii_case("false"), _ => false } } }] { () } / fail("true"))) end:position!() {? cx.node(199,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_null(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "null")] { () } / fail("null"))) end:position!() {? cx.node(200,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_current_timestamp(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CURRENT_TIMESTAMP")] { () } / fail("CURRENT_TIMESTAMP"))) (((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) (( { () })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { () })) { () })) end:position!() {? cx.node(201,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_timestamp(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "timestamp")] { () } / fail("timestamp"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(v_call_contents(depth.descend(1,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(202,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_cast(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CAST")] { () } / fail("CAST"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(v_call_contents(depth.descend(1,0))) (([token if token.is_keyword(cx.input, "AS")] { () } / fail("AS"))) (([token if token.is_keyword(cx.input, "TIMESTAMP")] { () } / fail("TIMESTAMP"))) { (c0,) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,) })) end:position!() {? cx.node(203,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_call_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(v_value(depth.descend(0,0))) end:position!() {? cx.node(204,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_property_value(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((((saved:peek_mark() ((&(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger")))) restore(saved) { () } / restore(saved) fail("a conversion function")))) c1:(v_identifier(depth.descend(0,0))) c2:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:(v_conversion_contents(depth.descend(1,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) { (c1,c2) })) { Either::Left(a) } / b:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) c2:(v_number(depth.descend(0,0))) (([token if token.kind == TokenKind::RightBracket] { () } / fail("']'"))) { (c0,c2) })) { Either::Right(b) })) end:position!() {? cx.node(205,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_conversion_contents(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((((saved:peek_mark() ((&(((((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) { () } / (((([token if token.kind == TokenKind::Word] { () } / fail("identifier"))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { () })) { () }))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:(v_property_value(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:(v_value(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(206,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_property(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("true") || word.eq_ignore_ascii_case("null") || word.eq_ignore_ascii_case("CAST"), 5 => word.eq_ignore_ascii_case("false"), 9 => word.eq_ignore_ascii_case("timestamp"), 17 => word.eq_ignore_ascii_case("CURRENT_TIMESTAMP"), _ => false } } }] { () } / fail("true")))) restore(saved) { () } / restore(saved) fail("a property binding")))) c1:(v_identifier(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2) })) end:position!() {? cx.node(207,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_properties(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftBrace] { () } / fail("'{'"))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:((c0:(v_property_entry(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(v_property_entry(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) ((saved:diagnostic() begin:position!() result:(c:(([token if token.kind == TokenKind::Comma] { () } / fail("','"))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightBrace] { () } / fail("',' or '}'"))) { (c1,) })) end:position!() {? cx.node(208,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule v_property_entry(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c2:((a:((((saved:peek_mark() ((&(((((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) { () } / (((([token if token.kind == TokenKind::Word] { () } / fail("identifier"))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { () })) { () }))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:(v_property_value(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:(v_value(depth.descend(0,0))) { Either::Right(b) })) { (c0,c2) })) end:position!() {? cx.node(209,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_create_data(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(d_node(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:((c0:(d_edge(depth.descend(0,0))) c1:(d_node(depth.descend(0,0))) { (c0,c1) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1) })) end:position!() {? cx.node(210,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_node(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:((saved:diagnostic() begin:position!() result:(c:(d_variable(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c2:(v_identifier(depth.descend(0,0))) c3:((saved:diagnostic() begin:position!() result:(c:(v_properties(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c2,c3) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) end:position!() {? cx.node(211,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_variable(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(v_identifier(depth.descend(0,0))) end:position!() {? cx.node(212,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_edge(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) (([token if token.kind == TokenKind::Colon] { () } / fail("':'"))) c3:(v_identifier(depth.descend(0,0))) c4:((saved:diagnostic() begin:position!() result:(c:(v_properties(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.kind == TokenKind::RightBracket] { () } / fail("']'"))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) { (c3,c4) })) end:position!() {? cx.node(213,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_merge_data(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:((c0:(d_node(depth.descend(0,0))) c1:(d_edge(depth.descend(0,0))) c2:(d_node(depth.descend(0,0))) { (c0,c1,c2) })) { Either::Left(a) } / b:((c0:(d_node(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(d_on_sets(depth.descend(0,0))) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) c2:((saved:diagnostic() begin:position!() result:(c:(d_post_sets(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c0,c1,c2) })) { Either::Right(b) })) end:position!() {? cx.node(214,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_on_sets(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "ON")] { () } / fail("ON"))) c1:(d_on_kind(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "SET")] { () } / fail("SET"))) c3:(d_sets(depth.descend(0,0))) { (c1,c3) })) end:position!() {? cx.node(215,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_on_kind(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 5 => word.eq_ignore_ascii_case("MATCH"), 6 => word.eq_ignore_ascii_case("CREATE"), _ => false } } }] { () } / fail("CREATE"))) end:position!() {? cx.node(216,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_post_sets(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "SET")] { () } / fail("SET"))) c1:(d_sets(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(217,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_sets(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(d_assignment(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c1:(d_assignment(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(218,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_assignment(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c2:(d_set_value(depth.descend(0,0))) { (c0,c2) })) end:position!() {? cx.node(219,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_ref(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c2:(v_identifier(depth.descend(0,0))) { (c0,c2) })) end:position!() {? cx.node(220,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_set_value(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(d_case_preserve(depth.descend(0,0))) { Either::Left(a) } / b:((a:(d_case_decrement(depth.descend(0,0))) { Either::Left(a) } / b:((a:(d_coalesce(depth.descend(0,0))) { Either::Left(a) } / b:((a:((((saved:peek_mark() ((&((c0:(x_temporal_name(depth.descend(0,0))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { (c0,) }))) restore(saved) { () } / restore(saved) fail("a temporal call")))) c1:(v_value(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:((a:(d_property_value(depth.descend(0,0))) { Either::Left(a) } / b:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("CASE"), 8 => word.eq_ignore_ascii_case("COALESCE"), _ => false } } }] { () } / fail("CASE")))) restore(saved) { () } / restore(saved) fail("a mutation value")))) c1:((a:((((saved:peek_mark() ((&(((((([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 7 => word.eq_ignore_ascii_case("toFloat"), 8 => word.eq_ignore_ascii_case("toString"), 9 => word.eq_ignore_ascii_case("toInteger") || word.eq_ignore_ascii_case("toBoolean"), _ => false } } }] { () } / fail("toInteger"))) (([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) { () })) { () } / (((([token if token.kind == TokenKind::Word] { () } / fail("identifier"))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("'['"))) { () })) { () }))) restore(saved) { () } / restore(saved) fail("a Cypher value token")))) c1:(v_property_value(depth.descend(0,0))) { (c1,) })) { Either::Left(a) } / b:(v_value(depth.descend(0,0))) { Either::Right(b) })) { (c1,) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(221,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_property_value(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((((saved:peek_mark() ((!(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("CASE"), 8 => word.eq_ignore_ascii_case("COALESCE"), _ => false } } }] { () } / fail("CASE")))) restore(saved) { () } / restore(saved) fail("a property value")))) c1:(d_ref(depth.descend(0,0))) c2:((saved:diagnostic() begin:position!() result:(c:(d_add(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2) })) end:position!() {? cx.node(222,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_add(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Plus] { () } / fail("'+'"))) c1:(v_value(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(223,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_coalesce(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "COALESCE")] { () } / fail("COALESCE"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("'('"))) c1:((c0:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Comma] { () } / fail("','"))) c2:(v_value(depth.descend(0,0))) { (c0,c2) })) (([token if token.kind == TokenKind::RightParen] { () } / fail("')'"))) { (c1,) })) c2:((saved:diagnostic() begin:position!() result:(c:(d_add(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c2) })) end:position!() {? cx.node(224,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_case_preserve(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "CASE")] { () } / fail("CASE")) ([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN")) &([token if token.kind == TokenKind::Parameter]) incoming:v_value(depth.descend(0,0)) ([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS")) ([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL")) ([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN")) property:d_ref(depth.descend(0,0)) selected_branch:(a:(([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE")) value:v_value(depth.descend(0,0)) ([token if token.is_keyword(cx.input, "END")] { () } / fail("END")) { (incoming,property,value) }) { Either::Left(a) } / b:(([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN")) preserve:v_value(depth.descend(0,0)) ([token if token.kind == TokenKind::Equal] { () } / fail("'='")) truth:v_value(depth.descend(0,0)) ([token if token.is_keyword(cx.input, "AND")] { () } / fail("AND")) existing:d_ref(depth.descend(0,0)) ([token if token.is_keyword(cx.input, "IS")] { () } / fail("IS")) ([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT")) ([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL")) ([token if token.is_keyword(cx.input, "AND")] { () } / fail("AND")) compared:d_ref(depth.descend(0,0)) (([token if token.kind == TokenKind::Greater] { () } / fail("'>'")) / ([token if token.kind == TokenKind::Less] { () } / fail("'<'"))) compared_incoming:v_value(depth.descend(0,0)) ([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN")) selected:d_ref(depth.descend(0,0)) ([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE")) fallback:v_value(depth.descend(0,0)) ([token if token.is_keyword(cx.input, "END")] { () } / fail("END")) { (incoming,property,preserve,truth,existing,compared,compared_incoming,selected,fallback) }) { Either::Right(b) }) { selected_branch }) end:position!() {? cx.node(225,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_case_decrement(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CASE")] { () } / fail("CASE"))) (([token if token.is_keyword(cx.input, "WHEN")] { () } / fail("WHEN"))) c2:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Greater] { () } / fail("'>'"))) c4:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "THEN")] { () } / fail("THEN"))) c6:(d_ref(depth.descend(0,0))) (([token if token.kind == TokenKind::Minus] { () } / fail("'-'"))) c8:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "ELSE")] { () } / fail("ELSE"))) c10:(v_value(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "END")] { () } / fail("END"))) { (c2,c4,c6,c8,c10) })) end:position!() {? cx.node(226,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule hint(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "SYSTEM")] { () } / fail("SYSTEM"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c2:(v_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c4:(v_value(depth.descend(0,0))) { (c2,c4) })) end:position!() {? cx.node(227,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule set(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "SET")] { () } / fail("SET"))) (([token if token.is_keyword(cx.input, "SYSTEM")] { () } / fail("SYSTEM"))) c2:((a:(d_system_variable(depth.descend(0,0))) { Either::Left(a) } / b:(((([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { Either::Right(b) })) (([token if token.kind == TokenKind::Equal] { () } / fail("'='"))) c4:(v_value(depth.descend(0,0))) { (c2,c4) })) end:position!() {? cx.node(228,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule d_system_variable(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "VARIABLE")] { () } / fail("VARIABLE"))) c1:((a:(((([token if token.is_keyword(cx.input, "SYSTEM")] { () } / fail("SYSTEM"))) (([token if token.kind == TokenKind::Dot] { () } / fail("'.'"))) c2:(v_identifier(depth.descend(0,0))) { (c2,) })) { Either::Left(a) } / b:((((saved:peek_mark() ((!(([token if token.is_keyword(cx.input, "SYSTEM")] { () } / fail("SYSTEM")))) restore(saved) { () } / restore(saved) fail("a variable name")))) c1:(v_identifier(depth.descend(0,0))) { (c1,) })) { Either::Right(b) })) { (c1,) })) end:position!() {? cx.node(229,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_create(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(schema_index(depth.descend(0,0))) { Either::Left(a) } / b:((a:(schema_constraint(depth.descend(0,0))) { Either::Left(a) } / b:((a:(schema_property(depth.descend(0,0))) { Either::Left(a) } / b:(schema_table(depth.descend(0,0))) { Either::Right(b) })) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(230,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_identifier(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Word] { () } / fail("a Cypher schema token"))) end:position!() {? cx.node(231,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_kind(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("NODE"), 12 => word.eq_ignore_ascii_case("RELATIONSHIP"), _ => false } } }] { () } / fail("NODE"))) end:position!() {? cx.node(232,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_table(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(schema_kind(depth.descend(0,0))) c1:(schema_object_kind(depth.descend(0,0))) c2:(schema_identifier(depth.descend(0,0))) { (c0,c1,c2) })) end:position!() {? cx.node(233,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_object_kind(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 4 => word.eq_ignore_ascii_case("TYPE"), 5 => word.eq_ignore_ascii_case("TABLE") || word.eq_ignore_ascii_case("LABEL"), _ => false } } }] { () } / fail("TABLE"))) end:position!() {? cx.node(234,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_range(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "RANGE")] { () } / fail("RANGE"))) end:position!() {? cx.node(235,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_fulltext(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.is_keyword(cx.input, "FULLTEXT")] { () } / fail("FULLTEXT"))) end:position!() {? cx.node(236,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_index(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:((saved:diagnostic() begin:position!() result:(c:((a:(schema_range(depth.descend(0,0))) { Either::Left(a) } / b:(schema_fulltext(depth.descend(0,0))) { Either::Right(b) })) { Some(c) } / restore_absence(begin,saved) { None }) { result })) (([token if token.is_keyword(cx.input, "INDEX")] { () } / fail("INDEX"))) (([token if token.is_keyword(cx.input, "ON")] { () } / fail("ON"))) (([token if token.kind == TokenKind::Colon] { () } / fail("a Cypher schema token"))) c4:(schema_identifier(depth.descend(0,0))) c5:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("a Cypher schema token"))) c1:(schema_index_keys(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("a Cypher schema token"))) { (c1,) })) { (c0,c4,c5) })) end:position!() {? cx.node(237,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_index_keys(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(schema_identifier(depth.descend(0,0))) c1:((saved:diagnostic() begin:position!() result:(c:(((([token if token.kind == TokenKind::Comma] { () } / fail("a Cypher schema token"))) c1:(schema_identifier(depth.descend(0,0))) { (c1,) })) { c } / failed:repetition_stop(begin,saved) { failed }) { result })*) { (c0,c1) })) end:position!() {? cx.node(238,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_property_path(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "ON")] { () } / fail("ON"))) c1:(schema_kind(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "TABLE")] { () } / fail("TABLE"))) c3:(schema_identifier(depth.descend(0,0))) c4:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("a Cypher schema token"))) c1:(schema_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("a Cypher schema token"))) { (c1,) })) { (c1,c3,c4) })) end:position!() {? cx.node(239,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_property(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "PROPERTY")] { () } / fail("PROPERTY"))) c1:(schema_property_path(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "TYPE")] { () } / fail("TYPE"))) c3:(schema_type(depth.descend(0,0))) c4:((saved:diagnostic() begin:position!() result:(c:(schema_not_null(depth.descend(0,0))) { Some(c) } / restore_absence(begin,saved) { None }) { result })) { (c1,c3,c4) })) end:position!() {? cx.node(240,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_not_null(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "NOT")] { () } / fail("NOT"))) (([token if token.is_keyword(cx.input, "NULL")] { () } / fail("NULL"))) { () })) end:position!() {? cx.node(241,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_type(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(((([token if token.is_keyword(cx.input, "VECTOR")] { () } / fail("VECTOR"))) c1:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("a Cypher schema token"))) c1:(schema_dimension(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("a Cypher schema token"))) { (c1,) })) { (c1,) })) { Either::Left(a) } / b:((a:(((([token if token.is_keyword(cx.input, "CHARACTER")] { () } / fail("CHARACTER"))) (([token if token.is_keyword(cx.input, "VARYING")] { () } / fail("VARYING"))) { () })) { Either::Left(a) } / b:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 3 => word.eq_ignore_ascii_case("ANY") || word.eq_ignore_ascii_case("INT"), 4 => word.eq_ignore_ascii_case("BOOL") || word.eq_ignore_ascii_case("TEXT") || word.eq_ignore_ascii_case("LIST"), 5 => word.eq_ignore_ascii_case("FLOAT"), 6 => word.eq_ignore_ascii_case("DOUBLE") || word.eq_ignore_ascii_case("STRING"), 7 => word.eq_ignore_ascii_case("BOOLEAN") || word.eq_ignore_ascii_case("INTEGER") || word.eq_ignore_ascii_case("VARCHAR"), _ => false } } }] { () } / fail("ANY"))) { Either::Right(b) })) { Either::Right(b) })) end:position!() {? cx.node(242,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_dimension(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if token.kind == TokenKind::Number] { () } / fail("a Cypher schema token"))) end:position!() {? cx.node(243,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_constraint(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CONSTRAINT")] { () } / fail("CONSTRAINT"))) (([token if token.is_keyword(cx.input, "ON")] { () } / fail("ON"))) c2:((a:(schema_relationship_constraint(depth.descend(0,0))) { Either::Left(a) } / b:(schema_node_constraint(depth.descend(0,0))) { Either::Right(b) })) (([token if token.is_keyword(cx.input, "ASSERT")] { () } / fail("ASSERT"))) c4:(schema_assertion(depth.descend(0,0))) { (c2,c4) })) end:position!() {? cx.node(244,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_constraint_key(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Colon] { () } / fail("a Cypher schema token"))) c1:(schema_identifier(depth.descend(0,0))) c2:(((([token if token.kind == TokenKind::LeftParen] { () } / fail("a Cypher schema token"))) c1:(schema_identifier(depth.descend(0,0))) (([token if token.kind == TokenKind::RightParen] { () } / fail("a Cypher schema token"))) { (c1,) })) { (c1,c2) })) end:position!() {? cx.node(245,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_node_constraint(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(schema_constraint_key(depth.descend(0,0))) end:position!() {? cx.node(246,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_relationship_constraint(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.kind == TokenKind::Minus] { () } / fail("a Cypher schema token"))) (([token if token.kind == TokenKind::LeftBracket] { () } / fail("a Cypher schema token"))) c2:(schema_constraint_key(depth.descend(0,0))) (([token if token.kind == TokenKind::RightBracket] { () } / fail("a Cypher schema token"))) (([token if token.kind == TokenKind::Minus] { () } / fail("a Cypher schema token"))) (([token if token.kind == TokenKind::Greater] { () } / fail("a Cypher schema token"))) { (c2,) })) end:position!() {? cx.node(247,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_assertion(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 6 => word.eq_ignore_ascii_case("UNIQUE") || word.eq_ignore_ascii_case("EXISTS"), _ => false } } }] { () } / fail("UNIQUE"))) { Either::Left(a) } / b:(schema_not_null(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(248,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_alter(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((a:(schema_alter_property(depth.descend(0,0))) { Either::Left(a) } / b:(schema_alter_table(depth.descend(0,0))) { Either::Right(b) })) end:position!() {? cx.node(249,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_alter_property(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "PROPERTY")] { () } / fail("PROPERTY"))) c1:(schema_property_path(depth.descend(0,0))) c2:(schema_state_clause(depth.descend(0,0))) { (c1,c2) })) end:position!() {? cx.node(250,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_alter_table(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:((c0:(schema_kind(depth.descend(0,0))) (([token if token.is_keyword(cx.input, "TABLE")] { () } / fail("TABLE"))) c2:(schema_identifier(depth.descend(0,0))) c3:(schema_state_clause(depth.descend(0,0))) { (c0,c2,c3) })) end:position!() {? cx.node(251,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_state_clause(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "SET")] { () } / fail("SET"))) (([token if token.is_keyword(cx.input, "STATE")] { () } / fail("STATE"))) c2:(schema_state(depth.descend(0,0))) { (c2,) })) end:position!() {? cx.node(252,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule schema_state(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(([token if { token.kind == TokenKind::Word && { let word = token.text(cx.input); match word.len() { 2 => word.eq_ignore_ascii_case("GC"), 6 => word.eq_ignore_ascii_case("PUBLIC"), 8 => word.eq_ignore_ascii_case("BACKFILL"), 10 => word.eq_ignore_ascii_case("WRITE_ONLY") || word.eq_ignore_ascii_case("VALIDATING"), 11 => word.eq_ignore_ascii_case("DELETE_ONLY"), _ => false } } }] { () } / fail("DELETE_ONLY"))) end:position!() {? cx.node(253,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule create(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "CREATE")] { () } / fail("CREATE"))) c1:((a:(schema_create(depth.descend(0,0))) { Either::Left(a) } / b:(d_create_data(depth.descend(0,0))) { Either::Right(b) })) { (c1,) })) end:position!() {? cx.node(254,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 rule alter(depth:Depth) -> NodeId = mark:capture_mark() result:((enter(depth) start:position!() children:(((([token if token.is_keyword(cx.input, "ALTER")] { () } / fail("ALTER"))) c1:(schema_alter(depth.descend(0,0))) { (c1,) })) end:position!() {? cx.node(255,start,end,children) }) / rewind_capture(mark,false) rejected:expected!("matching Cypher rule") {rejected}) {result}

 // These total Option entries use Builder's compatibility diagnostics.
 // Suppress only rust-peg's duplicate internal error-state bookkeeping.
 #[no_eof]
 pub rule parse_root() -> Option<NodeId> = quiet!{root(Depth {value:0,structural:0,statement:1})}?
 #[no_eof]
 pub rule parse_pipeline() -> Option<NodeId> = quiet!{pipeline_root(Depth {value:0,structural:0,statement:0})}?
 #[no_eof]
 pub rule parse_predicate() -> Option<NodeId> = quiet!{w_input(Depth {value:0,structural:0,statement:0})}?
 }
}
pub(super) fn recognize<'a>(
    input: &'a str,
    tokens: &'a [Token],
    entry: &str,
    multi_stage: bool,
) -> std::result::Result<ParseTree, SyntaxError> {
    recognize_inner(
        input,
        tokens,
        entry,
        multi_stage,
        #[cfg(test)]
        TestLimits::default(),
    )
}
#[cfg(test)]
pub(super) fn recognize_with_limits<'a>(
    input: &'a str,
    tokens: &'a [Token],
    entry: &str,
    multi_stage: bool,
    limits: TestLimits,
) -> std::result::Result<ParseTree, SyntaxError> {
    recognize_inner(input, tokens, entry, multi_stage, limits)
}
fn recognize_inner<'a>(
    input: &'a str,
    tokens: &'a [Token],
    entry: &str,
    multi_stage: bool,
    #[cfg(test)] limits: TestLimits,
) -> std::result::Result<ParseTree, SyntaxError> {
    let mut cx = Builder {
        #[cfg(test)]
        limits,
        input,
        multi_stage,
        tokens,
        nodes: Vec::new(),
        edges: Vec::new(),
        steps: 0,
        fatal: None,
        farthest: None,
        peek_depth: 0,
        eager_projection: tokens
            .iter()
            .rev()
            .find(|token| !matches!(token.kind, TokenKind::End | TokenKind::Semicolon))
            .is_some_and(|token| {
                matches!(
                    token.kind,
                    TokenKind::Word
                        | TokenKind::String
                        | TokenKind::UnterminatedString
                        | TokenKind::Number
                        | TokenKind::RightParen
                        | TokenKind::RightBracket
                        | TokenKind::RightBrace
                )
            }),
        logical_nodes: 0,
        logical_edges: 0,
        projection: None,
        order: None,
        projections: ProjectionCaches::default(),
    };
    let result = match entry {
        "root" => syntax::parse_root(tokens, &mut cx),
        "pipeline_root" => syntax::parse_pipeline(tokens, &mut cx),
        "w_input" => syntax::parse_predicate(tokens, &mut cx),
        _ => unreachable!("known Cypher entry"),
    };
    if let Some(fatal) = cx.fatal {
        return Err(fatal);
    }
    match result {
        Ok(Some(root)) => Ok(ParseTree::from_token_matches(cx.nodes, cx.edges, root)
            .with_projections(cx.projections)),
        Ok(None) => {
            let error = cx.farthest.unwrap_or(Diagnostic {
                position: 0,
                expected: "a Cypher statement",
            });
            let token = tokens
                .get(error.position)
                .copied()
                .unwrap_or(*tokens.last().expect("EOF token"));
            Err(SyntaxError::new(
                if token.kind == TokenKind::End {
                    SyntaxErrorCode::UnexpectedEnd
                } else {
                    SyntaxErrorCode::UnexpectedToken
                },
                token.span(),
            )
            .expected(error.expected)
            .found(token.text(input)))
        }
        Err(_) => unreachable!("total recognition wrapper; every root explicitly consumes EOF"),
    }
}
