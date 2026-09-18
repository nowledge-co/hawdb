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

const MEMORY_PREFIX_OWNERSHIP_QUERY: &str = "MATCH (m:Memory) \
     WHERE m.id STARTS WITH $prefix \
     RETURN m.id AS memory_id, id(m) AS memory_node_id, m.space_id AS space_id \
     ORDER BY memory_id ASC LIMIT $limit";

#[test]
fn memory_prefix_ownership_uses_one_fixed_bounded_query() {
    let mut db = Database::new_with_config(DatabaseConfig {
        max_plan_cache_entries: Some(8),
        statement_summary_capacity: 8,
        ..DatabaseConfig::default()
    });
    db.query("CREATE (:Memory {id: 'skill:alpha:1', space_id: ''})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'skill:alpha:2', space_id: 'team'})")
        .unwrap();
    db.query("CREATE (:Memory {id: 'skill:beta:1', space_id: 'team'})")
        .unwrap();
    db.query("CREATE (:Entity {id: 'skill:alpha:entity', space_id: 'team'})")
        .unwrap();
    let parameters = BTreeMap::from([
        (
            "prefix".to_string(),
            Value::String("skill:alpha:".to_string()),
        ),
        ("limit".to_string(), Value::Int(2)),
    ]);
    let mut read = db.begin_read_transaction();
    db.query("CREATE (:Memory {id: 'skill:alpha:3', space_id: 'late'})")
        .unwrap();

    let first = read
        .query_with_params_bounded(MEMORY_PREFIX_OWNERSHIP_QUERY, &parameters, Some(2))
        .unwrap();
    let second = read
        .query_with_params_bounded(MEMORY_PREFIX_OWNERSHIP_QUERY, &parameters, Some(2))
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(first.rows.len(), 2);
    assert_eq!(
        first.rows[0].get("memory_id"),
        Some(&Value::String("skill:alpha:1".to_string()))
    );
    assert_eq!(
        first.rows[0].get("space_id"),
        Some(&Value::String(String::new()))
    );
    assert_eq!(
        first.rows[1].get("space_id"),
        Some(&Value::String("team".to_string()))
    );
    assert_eq!(read_test_plan_cache_metric(&read, "entries"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "misses"), 1);
    assert_eq!(read_test_plan_cache_metric(&read, "hits"), 1);
}
