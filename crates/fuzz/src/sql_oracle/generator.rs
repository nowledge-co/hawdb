use super::{
    SqlFuzzCase, SqlJoinRewriteCase, SqlMutation, SqlPredicateRewriteCase, SqlQueryInvocation,
    SqlTlpCase, SQL_JOIN_REWRITE_SHAPE_COUNT, SQL_QUERY_SHAPE_COUNT,
};
use crate::predicate_rewrite::PredicateRewriteKind;
use crate::ResultSemantics;
use skein::{RelationalJoinPlanningStrategy, Value};

pub(super) fn generate_sql_case(seed: u64, index: usize, index_enabled: bool) -> SqlFuzzCase {
    let mut setup = vec![
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_regions (id BIGINT PRIMARY KEY, rank BIGINT UNIQUE, label TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_groups (id BIGINT PRIMARY KEY, region_id BIGINT UNIQUE REFERENCES sql_fuzz_regions(id), priority BIGINT, label TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_rows (id BIGINT PRIMARY KEY, group_id BIGINT REFERENCES sql_fuzz_groups(id), bucket BIGINT NOT NULL, score BIGINT, tag TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_notes (id BIGINT PRIMARY KEY, row_id BIGINT NOT NULL REFERENCES sql_fuzz_rows(id), value TEXT)",
        ),
    ];
    for region in 0..4_i64 {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_regions (id, rank, label) VALUES ($1, $2, $3)",
            vec![
                Value::Int(region),
                match region {
                    0 => Value::Null,
                    1 => Value::Int(10),
                    _ => Value::Int(region * 10),
                },
                Value::String(format!("region-{}", region % 2)),
            ],
        ));
    }
    for group in 0..4_i64 {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_groups (id, region_id, priority, label) VALUES ($1, $2, $3, $4)",
            vec![
                Value::Int(group),
                if group == 0 {
                    Value::Null
                } else {
                    Value::Int(group)
                },
                if group == 1 {
                    Value::Null
                } else {
                    Value::Int((group % 2) * 10)
                },
                Value::String(format!("group-{}", group % 2)),
            ],
        ));
    }
    let row_count = 12 + ((seed >> 8) as usize % 5);
    for row in 0..row_count {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_rows (id, group_id, bucket, score, tag) VALUES ($1, $2, $3, $4, $5)",
            vec![
                Value::Int(row as i64),
                if row.is_multiple_of(5) {
                    Value::Null
                } else {
                    Value::Int((row % 4) as i64)
                },
                Value::Int((row % 3) as i64),
                match row % 3 {
                    0 => Value::Null,
                    _ => Value::Int(row as i64),
                },
                match row % 3 {
                    0 => Value::Null,
                    1 => Value::String("alpha".to_string()),
                    _ => Value::String("beta".to_string()),
                },
            ],
        ));
    }
    let mut note_id = 0_i64;
    for row in 0..row_count {
        if row % 5 == 1 {
            continue;
        }
        for duplicate in 0..=row % 2 {
            setup.push(SqlMutation::data(
                "INSERT INTO sql_fuzz_notes (id, row_id, value) VALUES ($1, $2, $3)",
                vec![
                    Value::Int(note_id),
                    Value::Int(row as i64),
                    if (row + duplicate).is_multiple_of(3) {
                        Value::Null
                    } else {
                        Value::String(format!("note-{}", row % 2))
                    },
                ],
            ));
            note_id += 1;
        }
    }
    if index_enabled {
        setup.extend([
            SqlMutation::index(
                "CREATE INDEX sql_fuzz_regions_rank_idx ON sql_fuzz_regions (rank, id)",
            ),
            SqlMutation::index(
                "CREATE INDEX sql_fuzz_groups_region_idx ON sql_fuzz_groups (region_id, id)",
            ),
            SqlMutation::index("CREATE INDEX sql_fuzz_rows_score_idx ON sql_fuzz_rows (score, id)"),
            SqlMutation::index("CREATE INDEX sql_fuzz_rows_tag_idx ON sql_fuzz_rows (tag, id)"),
            SqlMutation::index(
                "CREATE INDEX sql_fuzz_rows_group_idx ON sql_fuzz_rows (group_id, id)",
            ),
            SqlMutation::index(
                "CREATE INDEX sql_fuzz_groups_priority_idx ON sql_fuzz_groups (priority, id)",
            ),
            SqlMutation::index(
                "CREATE INDEX sql_fuzz_notes_row_idx ON sql_fuzz_notes (row_id, id)",
            ),
        ]);
    }

    let specification = sql_query_spec(seed, index);
    let rewrite_kind = PredicateRewriteKind::for_case(index + index / SQL_QUERY_SHAPE_COUNT);
    SqlFuzzCase {
        seed,
        shape: specification.name.to_string(),
        setup,
        row_tlp: specification.build(false),
        aggregate_tlp: specification.build(true),
        predicate_rewrite: specification.build_predicate_rewrite(rewrite_kind),
        join_rewrite: sql_join_rewrite_case(seed, index),
        index_enabled,
    }
}

fn sql_join_rewrite_case(seed: u64, index: usize) -> SqlJoinRewriteCase {
    let rank = Value::Int(if seed & 1 == 0 { 10 } else { 20 });
    let (name, from, projection, predicate, order_by, expected_strategy) =
        match index % SQL_JOIN_REWRITE_SHAPE_COUNT {
            0 => (
                "three_inner_chain",
                "sql_fuzz_rows AS r \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id",
                "r.bucket AS bucket, r.tag AS row_tag, g.priority AS group_priority, x.rank AS region_rank",
                "x.rank = $1",
                "r.id ASC, g.id ASC, x.id ASC",
                RelationalJoinPlanningStrategy::CsgCmpMemo,
            ),
            1 => (
                "four_inner_chain",
                "sql_fuzz_rows AS r \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_notes AS n ON n.row_id = r.id",
                "r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, n.value AS note_value",
                "x.rank = $1",
                "r.id ASC, g.id ASC, x.id ASC, n.id ASC",
                RelationalJoinPlanningStrategy::CsgCmpMemo,
            ),
            2 => (
                "four_mixed_left_preserved",
                "sql_fuzz_rows AS r \
                 LEFT JOIN sql_fuzz_notes AS n ON n.row_id = r.id \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id",
                "r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, n.value AS note_value",
                "x.rank = $1",
                "r.id ASC, g.id ASC, x.id ASC, n.id ASC",
                RelationalJoinPlanningStrategy::CsgCmpMemo,
            ),
            _ => (
                "four_mixed_left_null_rejected",
                "sql_fuzz_rows AS r \
                 LEFT JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 LEFT JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_notes AS n ON n.row_id = r.id",
                "r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, n.value AS note_value",
                "x.rank = $1 AND g.id IS NOT NULL AND x.id IS NOT NULL",
                "r.id ASC, g.id ASC, x.id ASC, n.id ASC",
                RelationalJoinPlanningStrategy::CsgCmpMemo,
            ),
        };
    let base_sql = format!("SELECT {projection} FROM {from} WHERE {predicate}");
    SqlJoinRewriteCase {
        name: name.to_string(),
        optimized: SqlQueryInvocation {
            sql: format!("{base_sql} ORDER BY {order_by}"),
            parameters: vec![rank.clone()],
            result_semantics: ResultSemantics::Bag,
        },
        syntax_reference: SqlQueryInvocation {
            sql: base_sql,
            parameters: vec![rank],
            result_semantics: ResultSemantics::Bag,
        },
        expected_strategy,
    }
}

#[derive(Debug)]
struct SqlQuerySpec {
    name: &'static str,
    from: &'static str,
    projection: &'static str,
    predicate: String,
    null_predicate: String,
    predicate_parameters: Vec<Value>,
    null_parameters: Vec<Value>,
}

impl SqlQuerySpec {
    fn build(&self, aggregate: bool) -> SqlTlpCase {
        let projection = if aggregate {
            "COUNT(*) AS count"
        } else {
            self.projection
        };
        SqlTlpCase {
            name: if aggregate {
                format!("{}_count", self.name)
            } else {
                self.name.to_string()
            },
            original: self.query(projection, None, Vec::new()),
            predicate_true: self.query(
                projection,
                Some(&self.predicate),
                self.predicate_parameters.clone(),
            ),
            predicate_false: self.query(
                projection,
                Some(&format!("NOT ({})", self.predicate)),
                self.predicate_parameters.clone(),
            ),
            predicate_null: self.query(
                projection,
                Some(&self.null_predicate),
                self.null_parameters.clone(),
            ),
        }
    }

    fn build_predicate_rewrite(&self, kind: PredicateRewriteKind) -> SqlPredicateRewriteCase {
        let (original, rewritten) = match kind {
            PredicateRewriteKind::DoubleNegation => (
                self.query(
                    self.projection,
                    Some(&self.predicate),
                    self.predicate_parameters.clone(),
                ),
                self.query(
                    self.projection,
                    Some(&format!("NOT (NOT ({}))", self.predicate)),
                    self.predicate_parameters.clone(),
                ),
            ),
            PredicateRewriteKind::ConjunctionIdempotence => (
                self.query(
                    self.projection,
                    Some(&self.predicate),
                    self.predicate_parameters.clone(),
                ),
                self.query(
                    self.projection,
                    Some(&format!("({0}) AND ({0})", self.predicate)),
                    self.predicate_parameters.clone(),
                ),
            ),
            PredicateRewriteKind::DisjunctionIdempotence => (
                self.query(
                    self.projection,
                    Some(&self.predicate),
                    self.predicate_parameters.clone(),
                ),
                self.query(
                    self.projection,
                    Some(&format!("({0}) OR ({0})", self.predicate)),
                    self.predicate_parameters.clone(),
                ),
            ),
            PredicateRewriteKind::NullTotality => {
                let null_predicate = shift_positional_parameters(
                    &self.null_predicate,
                    self.predicate_parameters.len(),
                );
                let rewritten =
                    format!("({0}) OR (NOT ({0})) OR ({null_predicate})", self.predicate);
                let mut parameters = self.predicate_parameters.clone();
                parameters.extend(self.null_parameters.clone());
                (
                    self.query(self.projection, None, Vec::new()),
                    self.query(self.projection, Some(&rewritten), parameters),
                )
            }
        };

        SqlPredicateRewriteCase {
            name: kind.as_str().to_string(),
            original,
            rewritten,
        }
    }

    fn query(
        &self,
        projection: &str,
        predicate: Option<&str>,
        parameters: Vec<Value>,
    ) -> SqlQueryInvocation {
        SqlQueryInvocation {
            sql: match predicate {
                Some(predicate) => {
                    format!("SELECT {projection} FROM {} WHERE {predicate}", self.from)
                }
                None => format!("SELECT {projection} FROM {}", self.from),
            },
            parameters,
            result_semantics: ResultSemantics::Bag,
        }
    }
}

fn shift_positional_parameters(predicate: &str, offset: usize) -> String {
    if offset == 0 {
        return predicate.to_string();
    }

    let bytes = predicate.as_bytes();
    let mut output = String::with_capacity(predicate.len());
    let mut index = 0;
    let mut copied_until = 0;
    while index < bytes.len() {
        if bytes[index] != b'$' || index + 1 == bytes.len() || !bytes[index + 1].is_ascii_digit() {
            index += 1;
            continue;
        }
        output.push_str(&predicate[copied_until..index]);
        let start = index + 1;
        let mut end = start;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        let position = predicate[start..end]
            .parse::<usize>()
            .expect("generated SQL parameter positions are numeric");
        output.push('$');
        output.push_str(&(position + offset).to_string());
        index = end;
        copied_until = end;
    }
    output.push_str(&predicate[copied_until..]);
    output
}

fn sql_query_spec(seed: u64, index: usize) -> SqlQuerySpec {
    match index % SQL_QUERY_SHAPE_COUNT {
        0 => SqlQuerySpec {
            name: "nullable_score_range",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.score AS value",
            predicate: "r.score >= $1".to_string(),
            null_predicate: "r.score IS NULL".to_string(),
            predicate_parameters: vec![Value::Int((seed % 12) as i64)],
            null_parameters: Vec::new(),
        },
        1 => SqlQuerySpec {
            name: "nullable_tag_equality",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.tag AS value",
            predicate: "r.tag = $1".to_string(),
            null_predicate: "r.tag IS NULL".to_string(),
            predicate_parameters: vec![Value::String(if seed & 2 == 0 {
                "alpha".to_string()
            } else {
                "beta".to_string()
            })],
            null_parameters: Vec::new(),
        },
        2 => SqlQuerySpec {
            name: "inner_join_nullable_priority",
            from: "sql_fuzz_rows AS r INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id",
            projection: "r.bucket AS bucket, g.priority AS value",
            predicate: "g.priority >= $1".to_string(),
            null_predicate: "g.priority IS NULL".to_string(),
            predicate_parameters: vec![Value::Int(((seed % 3) as i64 + 1) * 10)],
            null_parameters: Vec::new(),
        },
        3 => SqlQuerySpec {
            name: "left_join_nullable_priority",
            from: "sql_fuzz_rows AS r LEFT JOIN sql_fuzz_groups AS g ON g.id = r.group_id",
            projection: "r.bucket AS bucket, g.priority AS value",
            predicate: "g.priority >= $1".to_string(),
            null_predicate: "g.priority IS NULL".to_string(),
            predicate_parameters: vec![Value::Int(((seed % 3) as i64 + 1) * 10)],
            null_parameters: Vec::new(),
        },
        4 => SqlQuerySpec {
            name: "nullable_score_in_list",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.score AS value",
            predicate: "r.score IN ($1, $2)".to_string(),
            null_predicate: "r.score IS NULL".to_string(),
            predicate_parameters: vec![
                Value::Int((seed % 12) as i64),
                Value::Int(((seed >> 4) % 12) as i64),
            ],
            null_parameters: Vec::new(),
        },
        5 => SqlQuerySpec {
            name: "nullable_column_comparison",
            from: "sql_fuzz_rows AS r",
            projection: "r.bucket AS bucket, r.score AS value",
            predicate: "r.score >= r.bucket".to_string(),
            null_predicate: "r.score IS NULL".to_string(),
            predicate_parameters: Vec::new(),
            null_parameters: Vec::new(),
        },
        6 => {
            let bucket = Value::Int(0);
            SqlQuerySpec {
                name: "nullable_conjunction",
                from: "sql_fuzz_rows AS r",
                projection: "r.bucket AS bucket, r.score AS value",
                predicate: "r.score >= $1 AND r.bucket = $2".to_string(),
                null_predicate: "r.score IS NULL AND r.bucket = $1".to_string(),
                predicate_parameters: vec![Value::Int((seed % 12) as i64), bucket.clone()],
                null_parameters: vec![bucket],
            }
        }
        _ => {
            let bucket = Value::Int(1);
            SqlQuerySpec {
                name: "nullable_disjunction",
                from: "sql_fuzz_rows AS r",
                projection: "r.bucket AS bucket, r.score AS value",
                predicate: "r.score >= $1 OR r.bucket = $2".to_string(),
                null_predicate: "r.score IS NULL AND NOT (r.bucket = $1)".to_string(),
                predicate_parameters: vec![Value::Int((seed % 12) as i64), bucket.clone()],
                null_parameters: vec![bucket],
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positional_parameters_are_rebased_without_touching_plain_text() {
        assert_eq!(
            shift_positional_parameters("r.score IS NULL AND r.bucket = $1", 2),
            "r.score IS NULL AND r.bucket = $3"
        );
        assert_eq!(shift_positional_parameters("$1 = $10", 3), "$4 = $13");
        assert_eq!(shift_positional_parameters("café = $1", 1), "café = $2");
        assert_eq!(crate::predicate_rewrite::PREDICATE_REWRITE_SHAPES.len(), 4);
    }
}
