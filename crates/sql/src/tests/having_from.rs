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

use super::*;
use crate::RelationalPlanTemplateCache;

#[test]
fn comma_from_retains_join_precedence_and_on_namespace_boundaries() {
    for (sql, scopes) in [
        (
            "SELECT a.id FROM a CROSS JOIN b JOIN c ON a.id = c.id",
            vec![0, 0],
        ),
        (
            "SELECT a.id FROM a, b INNER JOIN c ON a.id = c.id",
            vec![1, 1],
        ),
        (
            "SELECT a.id FROM a LEFT JOIN b ON a.id = b.id, c JOIN d ON c.id = d.id, e",
            vec![0, 2, 2, 4],
        ),
    ] {
        let SqlStatement::Select(select) = prepare_postgres_sql(sql).unwrap().statement else {
            panic!("expected SELECT");
        };
        assert_eq!(
            select
                .joins
                .iter()
                .map(|join| join.on_scope_start)
                .collect::<Vec<_>>(),
            scopes
        );
    }
    // Schema binding rejects the first item's reference in the second item's ON.
    // Parsing must retain that scope instead of silently giving it global visibility.
    assert!(prepare_postgres_sql("SELECT * FROM a, b WHERE a.id = b.id").is_ok());
}

#[test]
fn having_parameters_and_source_metadata_survive_template_caching() {
    let sql = "SELECT a.owner, SUM($1) AS total FROM items a, owners b WHERE a.owner = b.owner AND a.id >= $2 GROUP BY a.owner HAVING COUNT(*) FILTER (WHERE a.enabled = $3) >= $4 LIMIT $5 OFFSET $6";
    let prepared = prepare_postgres_sql(sql).unwrap();
    assert_eq!(
        prepared
            .parameters
            .iter()
            .map(|parameter| parameter.position)
            .collect::<Vec<_>>(),
        (1..=6).collect::<Vec<_>>()
    );
    assert!(prepare_postgres_sql(&sql.replace("$4", "$7"))
        .unwrap_err()
        .to_string()
        .contains("must be dense"));
    let cache = RelationalPlanTemplateCache::new(Some(2));
    let first = cache.prepare(sql).unwrap();
    let mut owned = first.statement().clone();
    let SqlStatement::Select(select) = &mut owned else {
        panic!("expected SELECT")
    };
    let having = select.having.as_mut().unwrap();
    let mut parameters = Vec::new();
    having.visit(&mut |node| {
        if let Some(SqlValue::Parameter(position)) = node.as_value() {
            parameters.push(*position);
            assert_ne!(node.span, Default::default());
        }
    });
    assert_eq!(parameters, vec![3, 4]);
    having
        .try_visit_mut(&mut |node| {
            if let ExprKind::Value(SqlValue::Parameter(_)) = node.kind {
                node.kind = ExprKind::Value(SqlValue::Literal(Value::Int(99)));
            }
            Ok::<_, ()>(())
        })
        .unwrap();
    select.joins[0].on_scope_start = 0;
    let second = cache.prepare(sql).unwrap();
    assert!(std::sync::Arc::ptr_eq(&first.template, &second.template));
    assert_eq!(*second.template, prepared);
    assert_ne!(owned, prepared.statement);
}

#[test]
fn having_lowers_group_predicates_without_expanding_where_or_join_profiles() {
    for sql in [
        "SELECT a.id FROM a HAVING COUNT(*) > 1",
        "SELECT owner, COUNT(*) FROM items GROUP BY owner HAVING COUNT(*) > 1",
        "SELECT 1 FROM items HAVING TRUE",
        "SELECT owner FROM items GROUP BY owner HAVING SUM(amount) IN (MAX(amount), NULL)",
        "SELECT owner FROM items GROUP BY owner HAVING NOT (COALESCE(SUM(amount), 0) > $1) OR owner ILIKE $2",
    ] {
        let SqlStatement::Select(select) = prepare_postgres_sql(sql).unwrap().statement else { panic!("expected SELECT") };
        assert!(select.having.is_some());
    }
    for sql in [
        "SELECT owner FROM items WHERE COUNT(*) > 1",
        "SELECT owner FROM items JOIN owners ON COUNT(*) > 1",
        "SELECT owner FROM items HAVING COUNT(*) + 1 > 2",
        "SELECT owner FROM items HAVING EXISTS (SELECT id FROM owners)",
        "SELECT owner FROM items HAVING owner LIKE ANY ('a%')",
    ] {
        assert!(prepare_postgres_sql(sql).is_err(), "accepted {sql}");
    }
}
