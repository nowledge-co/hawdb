// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

pub(super) fn lower_source(table: &TableFactor) -> Result<Option<SqlVectorIndexSearch>> {
    let TableFactor::Table {
        name,
        args: Some(args),
        ..
    } = table
    else {
        return Ok(None);
    };
    lower_call(lower_table_name(name)?, args).map(Some)
}

pub(super) fn lower_call(
    name: SqlTableName,
    args: &sqlparser::ast::TableFunctionArgs,
) -> Result<SqlVectorIndexSearch> {
    if name.schema.is_some() || name.name != "vector_index_search" {
        return Err(HawDBError::Semantic(
            "unsupported SQL table function".into(),
        ));
    }
    if args.settings.is_some() {
        return Err(HawDBError::Semantic(
            "vector_index_search does not accept SETTINGS".into(),
        ));
    }
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(index)), FunctionArg::Unnamed(FunctionArgExpr::Expr(embedding)), options @ ..] =
        args.args.as_slice()
    else {
        return Err(HawDBError::Semantic(
            "vector_index_search requires index and embedding arguments".into(),
        ));
    };
    let mut candidates = None;
    let mut top_k = None;
    let mut mode = None;
    let mut max_work = None;
    for option in options {
        let (name, value) = match option {
            FunctionArg::Named {
                name,
                arg: FunctionArgExpr::Expr(value),
                ..
            }
            | FunctionArg::ExprNamed {
                name: Expr::Identifier(name),
                arg: FunctionArgExpr::Expr(value),
                ..
            } => (name, value),
            _ => {
                return Err(HawDBError::Semantic(
                    "vector index options must be named".into(),
                ))
            }
        };
        let slot = match normalize_ident(name).as_str() {
            "candidates" => &mut candidates,
            "top_k" => &mut top_k,
            "mode" => &mut mode,
            "max_work" => &mut max_work,
            _ => return Err(HawDBError::Semantic("unknown vector index option".into())),
        };
        if slot.replace(lower_literal_expr(value)?).is_some() {
            return Err(HawDBError::Semantic("duplicate vector index option".into()));
        }
    }
    let mode = match mode {
        None => hawdb_core::VectorIndexQueryMode::AnnWindow,
        Some(SqlValue::Literal(hawdb_core::Value::String(mode))) => {
            hawdb_core::VectorIndexQueryMode::parse(&mode)?
        }
        _ => {
            return Err(HawDBError::Semantic(
                "vector index mode must be a string literal".into(),
            ))
        }
    };
    let required = || {
        HawDBError::Semantic(
            match mode {
                hawdb_core::VectorIndexQueryMode::Exact => {
                    "exact vector search requires explicit top_k"
                }
                hawdb_core::VectorIndexQueryMode::AnnWindow
                | hawdb_core::VectorIndexQueryMode::FilteredAnn => {
                    "vector index requires explicit candidates and top_k"
                }
            }
            .into(),
        )
    };
    match mode {
        hawdb_core::VectorIndexQueryMode::AnnWindow
        | hawdb_core::VectorIndexQueryMode::FilteredAnn
            if candidates.is_none() =>
        {
            return Err(required())
        }
        hawdb_core::VectorIndexQueryMode::Exact if candidates.is_some() => {
            return Err(HawDBError::Semantic(
                "exact vector search does not accept candidates".into(),
            ));
        }
        _ => {}
    }
    if mode.requires_eligible_domain() != max_work.is_some() {
        return Err(HawDBError::Semantic(
            "max_work is required for exact and filtered_ann vector search only".into(),
        ));
    }
    Ok(SqlVectorIndexSearch {
        index: lower_literal_expr(index)?,
        embedding: lower_literal_expr(embedding)?,
        mode,
        max_work,
        candidates,
        top_k: top_k.ok_or_else(required)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_index_requires_unique_named_window_options() {
        for (arguments, diagnostic) in [
            ("'rows', $1", "explicit candidates and top_k"),
            (
                "'rows', $1, candidates => 8",
                "explicit candidates and top_k",
            ),
            ("'rows', $1, top_k => 1", "explicit candidates and top_k"),
            (
                "'rows', $1, candidates => 8, candidates => 4, top_k => 1",
                "duplicate vector index option",
            ),
            (
                "'rows', $1, candidates => 8, top_k => 1, top_k => 2",
                "duplicate vector index option",
            ),
            (
                "'rows', $1, candidates => 8, top_k => 1, limit => 2",
                "unknown vector index option",
            ),
            ("'rows', $1, 8, 1", "options must be named"),
        ] {
            let sql = format!("SELECT * FROM vector_index_search({arguments})");
            let error = crate::prepare_postgres_sql(&sql).unwrap_err();
            assert!(
                matches!(&error, HawDBError::Semantic(message) if message.contains(diagnostic)),
                "{sql}: {error:?}"
            );
        }
        crate::prepare_postgres_sql(
            "SELECT * FROM vector_index_search('rows', $1, top_k => 1, candidates => 8)",
        )
        .unwrap();
    }

    #[test]
    fn vector_index_exact_options_are_explicit_and_disjoint_from_ann() {
        for options in [
            "mode => 'exact', top_k => 1",
            "mode => 'exact', top_k => 1, max_work => 10, candidates => 2",
            "mode => $2, top_k => 1, max_work => 10",
            "mode => 'filtered_ann', top_k => 1, max_work => 10",
            "mode => 'filtered_ann', candidates => 2, top_k => 1",
            "mode => 'filtered_ann', candidates => 2, max_work => 10",
            "mode => 'ann_window', top_k => 1, candidates => 2, max_work => 10",
            "mode => 'exact', top_k => 1, max_work => 10, max_work => 20",
        ] {
            assert!(
                crate::prepare_postgres_sql(&format!(
                    "SELECT * FROM vector_index_search('rows', $1, {options})"
                ))
                .is_err(),
                "{options}"
            );
        }
        crate::prepare_postgres_sql("SELECT id FROM vector_index_search('rows', $1, mode => 'exact', top_k => 1, max_work => $2) WHERE active = true").unwrap();
        crate::prepare_postgres_sql("SELECT id FROM vector_index_search('rows', $1, mode => 'filtered_ann', candidates => $2, top_k => 1, max_work => $3) WHERE active = true").unwrap();
    }
}
