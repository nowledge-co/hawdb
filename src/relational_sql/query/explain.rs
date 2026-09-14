use super::{
    predicate_is_covered_by_access, RelationalQueryLimits, RelationalQueryOutput, Result,
    SelectStatement, Value,
};

pub(super) fn format_relational_explain(
    select: &SelectStatement,
    parameters: &[Value],
    output: RelationalQueryOutput,
    analyze: bool,
    limits: RelationalQueryLimits,
) -> Result<RelationalQueryOutput> {
    skein_relational::explain::format_relational_explain(
        select,
        parameters,
        output,
        analyze,
        limits,
        predicate_is_covered_by_access,
    )
}
