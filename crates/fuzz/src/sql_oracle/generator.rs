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

use super::{
    SqlFuzzCase, SqlJoinGeneratorProfile, SqlJoinRewriteCase, SqlMutation, SqlPredicateRewriteCase,
    SqlQueryInvocation, SqlTlpCase, SQL_JOIN_REWRITE_SHAPE_COUNT, SQL_QUERY_SHAPE_COUNT,
};
use crate::predicate_rewrite::PredicateRewriteKind;
use crate::ResultSemantics;
use hawdb::{RelationalJoinPlanningStrategy, Value};
use std::collections::BTreeMap;

pub(super) fn generate_sql_case(seed: u64, index: usize, index_enabled: bool) -> SqlFuzzCase {
    let statistics = JoinStatisticsProfile::for_case(seed, index);
    let index_profile = join_index_profile(seed, index, index_enabled);
    let join_rewrite = sql_join_rewrite_case(seed, index, index_profile, statistics);
    let mut setup = vec![
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_regions (id BIGINT PRIMARY KEY, rank BIGINT, label TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_tenants (id BIGINT PRIMARY KEY, rank BIGINT, label TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_groups (id BIGINT PRIMARY KEY, region_id BIGINT REFERENCES sql_fuzz_regions(id), priority BIGINT, label TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_accounts (id BIGINT PRIMARY KEY, region_id BIGINT REFERENCES sql_fuzz_regions(id), tenant_id BIGINT REFERENCES sql_fuzz_tenants(id), tier BIGINT, label TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_rows (id BIGINT PRIMARY KEY, group_id BIGINT REFERENCES sql_fuzz_groups(id), account_id BIGINT REFERENCES sql_fuzz_accounts(id), bucket BIGINT NOT NULL, score BIGINT, tag TEXT)",
        ),
        SqlMutation::required(
            "CREATE TABLE sql_fuzz_notes (id BIGINT PRIMARY KEY, row_id BIGINT NOT NULL REFERENCES sql_fuzz_rows(id), value TEXT)",
        ),
    ];
    for region in 0..statistics.region_count as i64 {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_regions (id, rank, label) VALUES ($1, $2, $3)",
            vec![
                Value::Int(region),
                match region {
                    0 => Value::Null,
                    1 => Value::Int(10),
                    _ => Value::Int(region * 10),
                },
                Value::String(format!(
                    "region-{}",
                    region % statistics.dimension_distinct_count as i64
                )),
            ],
        ));
    }
    for tenant in 0..statistics.tenant_count as i64 {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_tenants (id, rank, label) VALUES ($1, $2, $3)",
            vec![
                Value::Int(tenant),
                if tenant == 0 {
                    Value::Null
                } else {
                    Value::Int((tenant % 4) * 10)
                },
                Value::String(format!(
                    "tenant-{}",
                    tenant % statistics.dimension_distinct_count as i64
                )),
            ],
        ));
    }
    for group in 0..statistics.group_count as i64 {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_groups (id, region_id, priority, label) VALUES ($1, $2, $3, $4)",
            vec![
                Value::Int(group),
                if group == 0 {
                    Value::Null
                } else {
                    Value::Int(join_key(group, statistics.region_count, statistics.skewed))
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
    for account in 0..statistics.account_count as i64 {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_accounts (id, region_id, tenant_id, tier, label) VALUES ($1, $2, $3, $4, $5)",
            vec![
                Value::Int(account),
                if account % 7 == 0 {
                    Value::Null
                } else {
                    Value::Int(join_key(
                        account,
                        statistics.region_count,
                        statistics.skewed,
                    ))
                },
                if account % 5 == 0 {
                    Value::Null
                } else {
                    Value::Int(join_key(
                        account,
                        statistics.tenant_count,
                        statistics.skewed,
                    ))
                },
                if account % 4 == 0 {
                    Value::Null
                } else {
                    Value::Int(account % 3)
                },
                Value::String(format!("account-{}", account % 3)),
            ],
        ));
    }
    for row in 0..statistics.row_count {
        setup.push(SqlMutation::data(
            "INSERT INTO sql_fuzz_rows (id, group_id, account_id, bucket, score, tag) VALUES ($1, $2, $3, $4, $5, $6)",
            vec![
                Value::Int(row as i64),
                if row.is_multiple_of(5) {
                    Value::Null
                } else {
                    Value::Int(join_key(
                        row as i64,
                        statistics.group_count,
                        statistics.skewed,
                    ))
                },
                if row.is_multiple_of(7) {
                    Value::Null
                } else {
                    Value::Int(join_key(
                        row as i64,
                        statistics.account_count,
                        statistics.skewed,
                    ))
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
    for row in 0..statistics.row_count {
        if row % 5 == 1 {
            continue;
        }
        let duplicate_count = statistics.note_duplicate_count(row);
        for duplicate in 0..duplicate_count {
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
    setup.extend(join_indexes(index_profile));

    let specification = sql_query_spec(seed, index);
    let rewrite_kind = PredicateRewriteKind::for_case(
        index % SQL_QUERY_SHAPE_COUNT + index / SQL_QUERY_SHAPE_COUNT,
    );
    SqlFuzzCase {
        seed,
        shape: specification.name.to_string(),
        setup,
        row_tlp: specification.build(false),
        aggregate_tlp: specification.build(true),
        predicate_rewrite: specification.build_predicate_rewrite(rewrite_kind),
        join_rewrite,
        index_enabled,
    }
}

fn sql_join_rewrite_case(
    seed: u64,
    index: usize,
    index_profile: &'static str,
    statistics: JoinStatisticsProfile,
) -> SqlJoinRewriteCase {
    let (name, from, projection, order_by, relation_count, join_graph, null_rejection) =
        match index % SQL_JOIN_REWRITE_SHAPE_COUNT {
            0 => (
                "three_inner_chain",
                "sql_fuzz_notes AS n \
                 INNER JOIN sql_fuzz_rows AS r ON r.id = n.row_id \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id",
                "n.value AS note_value, r.bucket AS bucket, g.priority AS group_priority",
                "n.id ASC, r.id ASC, g.id ASC",
                3,
                "chain",
                "none",
            ),
            1 => (
                "three_inner_reverse_chain",
                "sql_fuzz_groups AS g \
                 INNER JOIN sql_fuzz_rows AS r ON r.group_id = g.id \
                 INNER JOIN sql_fuzz_notes AS n ON n.row_id = r.id",
                "n.value AS note_value, r.bucket AS bucket, g.priority AS group_priority",
                "g.id ASC, r.id ASC, n.id ASC",
                3,
                "reverse_chain",
                "none",
            ),
            2 => (
                "four_inner_chain",
                "sql_fuzz_notes AS n \
                 INNER JOIN sql_fuzz_rows AS r ON r.id = n.row_id \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id",
                "r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, n.value AS note_value",
                "n.id ASC, r.id ASC, g.id ASC, x.id ASC",
                4,
                "chain",
                "none",
            ),
            3 => (
                "four_inner_star",
                "sql_fuzz_rows AS r \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_notes AS n ON n.row_id = r.id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id",
                "r.bucket AS bucket, g.priority AS group_priority, n.value AS note_value, a.tier AS account_tier",
                "r.id ASC, g.id ASC, n.id ASC, a.id ASC",
                4,
                "star",
                "none",
            ),
            4 => (
                "four_inner_cycle",
                "sql_fuzz_rows AS r \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id AND a.region_id = x.id",
                "r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier",
                "r.id ASC, g.id ASC, x.id ASC, a.id ASC",
                4,
                "cycle",
                "none",
            ),
            5 => (
                "five_inner_tree",
                "sql_fuzz_notes AS n \
                 INNER JOIN sql_fuzz_rows AS r ON r.id = n.row_id \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id",
                "n.value AS note_value, r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier",
                "n.id ASC, r.id ASC, g.id ASC, x.id ASC, a.id ASC",
                5,
                "tree",
                "none",
            ),
            6 => (
                "five_inner_cycle",
                "sql_fuzz_rows AS r \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id AND a.region_id = x.id \
                 INNER JOIN sql_fuzz_tenants AS t ON t.id = a.tenant_id",
                "r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier, t.rank AS tenant_rank",
                "r.id ASC, g.id ASC, x.id ASC, a.id ASC, t.id ASC",
                5,
                "cycle",
                "none",
            ),
            7 => (
                "five_mixed_left_preserved",
                "sql_fuzz_rows AS r \
                 LEFT JOIN sql_fuzz_notes AS n ON n.row_id = r.id \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id",
                "r.bucket AS bucket, n.value AS note_value, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier",
                "r.id ASC, n.id ASC, g.id ASC, x.id ASC, a.id ASC",
                5,
                "tree",
                "preserved",
            ),
            8 => (
                "six_inner_tree",
                "sql_fuzz_notes AS n \
                 INNER JOIN sql_fuzz_rows AS r ON r.id = n.row_id \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id \
                 INNER JOIN sql_fuzz_tenants AS t ON t.id = a.tenant_id",
                "n.value AS note_value, r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier, t.rank AS tenant_rank",
                "n.id ASC, r.id ASC, g.id ASC, x.id ASC, a.id ASC, t.id ASC",
                6,
                "tree",
                "none",
            ),
            9 => (
                "six_inner_cycle",
                "sql_fuzz_notes AS n \
                 INNER JOIN sql_fuzz_rows AS r ON r.id = n.row_id \
                 INNER JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 INNER JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id AND a.region_id = x.id \
                 INNER JOIN sql_fuzz_tenants AS t ON t.id = a.tenant_id",
                "n.value AS note_value, r.bucket AS bucket, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier, t.rank AS tenant_rank",
                "n.id ASC, r.id ASC, g.id ASC, x.id ASC, a.id ASC, t.id ASC",
                6,
                "cycle",
                "none",
            ),
            10 => (
                "six_mixed_left_preserved",
                "sql_fuzz_rows AS r \
                 LEFT JOIN sql_fuzz_notes AS n ON n.row_id = r.id \
                 LEFT JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 LEFT JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id \
                 INNER JOIN sql_fuzz_tenants AS t ON t.id = a.tenant_id",
                "r.bucket AS bucket, n.value AS note_value, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier, t.rank AS tenant_rank",
                "r.id ASC, n.id ASC, g.id ASC, x.id ASC, a.id ASC, t.id ASC",
                6,
                "tree",
                "preserved",
            ),
            _ => (
                "six_mixed_left_null_rejected",
                "sql_fuzz_rows AS r \
                 LEFT JOIN sql_fuzz_notes AS n ON n.row_id = r.id \
                 LEFT JOIN sql_fuzz_groups AS g ON g.id = r.group_id \
                 LEFT JOIN sql_fuzz_regions AS x ON x.id = g.region_id \
                 INNER JOIN sql_fuzz_accounts AS a ON a.id = r.account_id \
                 INNER JOIN sql_fuzz_tenants AS t ON t.id = a.tenant_id",
                "r.bucket AS bucket, n.value AS note_value, g.priority AS group_priority, x.rank AS region_rank, a.tier AS account_tier, t.rank AS tenant_rank",
                "r.id ASC, n.id ASC, g.id ASC, x.id ASC, a.id ASC, t.id ASC",
                6,
                "tree",
                "rejected",
            ),
        };
    let (selectivity, predicate, parameters) = selectivity_predicate(seed, index);
    let predicate = if null_rejection == "rejected" {
        format!("{predicate} AND n.id IS NOT NULL AND g.id IS NOT NULL AND x.id IS NOT NULL")
    } else {
        predicate
    };
    let base_sql = format!("SELECT {projection} FROM {from} WHERE {predicate}");
    SqlJoinRewriteCase {
        name: name.to_string(),
        optimized: SqlQueryInvocation {
            sql: format!("{base_sql} ORDER BY {order_by}"),
            parameters: parameters.clone(),
            result_semantics: ResultSemantics::Bag,
        },
        syntax_reference: SqlQueryInvocation {
            sql: base_sql,
            parameters,
            result_semantics: ResultSemantics::Bag,
        },
        expected_strategy: RelationalJoinPlanningStrategy::CsgCmpMemo,
        generator_profile: SqlJoinGeneratorProfile {
            relation_count,
            join_graph: join_graph.to_string(),
            null_rejection: null_rejection.to_string(),
            selectivity: selectivity.to_string(),
            index_profile: index_profile.to_string(),
            statistics_profile: statistics.name.to_string(),
            table_cardinalities: BTreeMap::from([
                ("accounts".to_string(), statistics.account_count),
                ("groups".to_string(), statistics.group_count),
                ("notes".to_string(), statistics.note_count()),
                ("regions".to_string(), statistics.region_count),
                ("rows".to_string(), statistics.row_count),
                ("tenants".to_string(), statistics.tenant_count),
            ]),
            dimension_distinct_count: statistics.dimension_distinct_count,
            skewed_join_keys: statistics.skewed,
        },
    }
}

#[derive(Debug, Clone, Copy)]
struct JoinStatisticsProfile {
    name: &'static str,
    region_count: usize,
    tenant_count: usize,
    group_count: usize,
    account_count: usize,
    row_count: usize,
    dimension_distinct_count: usize,
    skewed: bool,
}

impl JoinStatisticsProfile {
    fn for_case(seed: u64, index: usize) -> Self {
        match ((seed >> 13) as usize + index) % 3 {
            0 => Self {
                name: "compact",
                region_count: 4,
                tenant_count: 4,
                group_count: 4,
                account_count: 4,
                row_count: 12,
                dimension_distinct_count: 2,
                skewed: false,
            },
            1 => Self {
                name: "skewed",
                region_count: 4,
                tenant_count: 3,
                group_count: 9,
                account_count: 8,
                row_count: 28,
                dimension_distinct_count: 1,
                skewed: true,
            },
            _ => Self {
                name: "wide",
                region_count: 8,
                tenant_count: 8,
                group_count: 12,
                account_count: 16,
                row_count: 40,
                dimension_distinct_count: 4,
                skewed: false,
            },
        }
    }

    fn note_count(self) -> usize {
        (0..self.row_count)
            .filter(|row| row % 5 != 1)
            .map(|row| self.note_duplicate_count(row))
            .sum()
    }

    fn note_duplicate_count(self, row: usize) -> usize {
        if self.skewed && row.is_multiple_of(3) {
            3
        } else {
            1 + row % 2
        }
    }
}

fn join_key(value: i64, cardinality: usize, skewed: bool) -> i64 {
    if skewed && value % 4 != 0 {
        1.min(cardinality.saturating_sub(1)) as i64
    } else {
        value % cardinality as i64
    }
}

fn join_index_profile(seed: u64, index: usize, index_enabled: bool) -> &'static str {
    if !index_enabled {
        return "none";
    }
    match ((seed >> 21) as usize + index) % 3 {
        0 => "join_keys",
        1 => "filter",
        _ => "composite",
    }
}

fn join_indexes(profile: &str) -> Vec<SqlMutation> {
    let statements: &[&str] = match profile {
        "none" => &[],
        "join_keys" => &[
            "CREATE INDEX sql_fuzz_groups_region_idx ON sql_fuzz_groups (region_id, id)",
            "CREATE INDEX sql_fuzz_accounts_region_idx ON sql_fuzz_accounts (region_id, id)",
            "CREATE INDEX sql_fuzz_accounts_tenant_idx ON sql_fuzz_accounts (tenant_id, id)",
            "CREATE INDEX sql_fuzz_rows_group_idx ON sql_fuzz_rows (group_id, id)",
            "CREATE INDEX sql_fuzz_rows_account_idx ON sql_fuzz_rows (account_id, id)",
            "CREATE INDEX sql_fuzz_notes_row_idx ON sql_fuzz_notes (row_id, id)",
        ],
        "filter" => &[
            "CREATE INDEX sql_fuzz_regions_rank_idx ON sql_fuzz_regions (rank, id)",
            "CREATE INDEX sql_fuzz_groups_priority_idx ON sql_fuzz_groups (priority, id)",
            "CREATE INDEX sql_fuzz_rows_bucket_idx ON sql_fuzz_rows (bucket, id)",
            "CREATE INDEX sql_fuzz_rows_score_idx ON sql_fuzz_rows (score, id)",
            "CREATE INDEX sql_fuzz_rows_tag_idx ON sql_fuzz_rows (tag, id)",
        ],
        "composite" => &[
            "CREATE INDEX sql_fuzz_groups_region_priority_idx ON sql_fuzz_groups (region_id, priority, id)",
            "CREATE INDEX sql_fuzz_accounts_region_tenant_idx ON sql_fuzz_accounts (region_id, tenant_id, id)",
            "CREATE INDEX sql_fuzz_rows_group_bucket_idx ON sql_fuzz_rows (group_id, bucket, id)",
            "CREATE INDEX sql_fuzz_rows_account_bucket_idx ON sql_fuzz_rows (account_id, bucket, id)",
            "CREATE INDEX sql_fuzz_notes_row_idx ON sql_fuzz_notes (row_id, id)",
        ],
        _ => unreachable!("generated join index profile is known"),
    };
    statements
        .iter()
        .map(|statement| SqlMutation::index(*statement))
        .collect()
}

fn selectivity_predicate(seed: u64, index: usize) -> (&'static str, String, Vec<Value>) {
    match ((seed >> 29) as usize + index) % 3 {
        0 => ("rare", "r.id = $1".to_string(), vec![Value::Int(2)]),
        1 => (
            "medium",
            "r.bucket = $1".to_string(),
            vec![Value::Int((seed % 3) as i64)],
        ),
        _ => ("broad", "r.score IS NOT NULL".to_string(), Vec::new()),
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
            PredicateRewriteKind::ConjunctionAbsorption
            | PredicateRewriteKind::DisjunctionAbsorption => {
                let secondary = shift_positional_parameters(
                    &self.null_predicate,
                    self.predicate_parameters.len(),
                );
                let rewritten = match kind {
                    PredicateRewriteKind::ConjunctionAbsorption => {
                        format!("({0}) AND (({0}) OR ({secondary}))", self.predicate)
                    }
                    PredicateRewriteKind::DisjunctionAbsorption => {
                        format!("({0}) OR (({0}) AND ({secondary}))", self.predicate)
                    }
                    _ => unreachable!("matched predicate absorption variants"),
                };
                let mut parameters = self.predicate_parameters.clone();
                parameters.extend(self.null_parameters.clone());
                (
                    self.query(
                        self.projection,
                        Some(&self.predicate),
                        self.predicate_parameters.clone(),
                    ),
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
    }
}
