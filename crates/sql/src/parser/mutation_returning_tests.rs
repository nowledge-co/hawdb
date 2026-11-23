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

use crate::{parse_postgres_sql, prepare_postgres_sql, SqlCommonTableExpressionBody, SqlStatement};

#[test]
fn parses_update_delete_and_upsert_returning_columns() {
    let SqlStatement::Update(update) =
        parse_postgres_sql("UPDATE pages AS p SET body = $1 WHERE p.id = $2 RETURNING p.id, body")
            .unwrap()
    else {
        panic!("UPDATE")
    };
    assert_eq!(update.returning.len(), 2);
    assert_eq!(update.returning[0].qualifier.as_deref(), Some("p"));

    let SqlStatement::Delete(delete) =
        parse_postgres_sql("DELETE FROM pages AS p WHERE p.id = $1 RETURNING p.id, body").unwrap()
    else {
        panic!("DELETE")
    };
    assert_eq!(delete.returning.len(), 2);

    let SqlStatement::Insert(insert) = parse_postgres_sql(
        "INSERT INTO pages (id, body) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET body = EXCLUDED.body RETURNING id, body",
    )
    .unwrap()
    else {
        panic!("INSERT")
    };
    assert_eq!(insert.returning.len(), 2);
}

#[test]
fn parses_data_modifying_ctes_and_collects_parameters() {
    let prepared = prepare_postgres_sql(
        "WITH changed(id, body) AS (UPDATE pages SET body = $1 WHERE id = $2 RETURNING id, body), \
         removed AS (DELETE FROM archive WHERE id = $3 RETURNING id) \
         SELECT id, body FROM changed UNION ALL SELECT id, $4 FROM removed ORDER BY id",
    )
    .unwrap();
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|parameter| parameter.position)
            .collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    let SqlStatement::Query(query) = prepared.statement else {
        panic!("query")
    };
    assert_eq!(query.with.len(), 2);
    assert!(matches!(
        query.with[0].body,
        SqlCommonTableExpressionBody::Mutation(_)
    ));
    assert!(matches!(
        query.with[1].body,
        SqlCommonTableExpressionBody::Mutation(_)
    ));
}
