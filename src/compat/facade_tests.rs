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

#[test]
fn compatibility_facade_preserves_types_and_query_output() {
    let fixture: hawdb_compat::CompatibilityFixture = crate::nowledge_memory_core_fixture();
    let _: crate::CompatibilityFixture = fixture;
    let inventory: hawdb_compat::CompatibilityQueryInventory =
        crate::nowledge_memory_core_inventory();
    let _: crate::CompatibilityQueryInventory = inventory;
    let _: fn(&mut Database, &CompatibilityFixture) -> Result<CompatibilityReport> =
        crate::run_compatibility_fixture;
    let output: hawdb_executor::QueryOutput =
        crate::QueryOutput::from_rows(vec![BTreeMap::from([("value".to_string(), Value::Int(7))])]);
    let facade: crate::QueryOutput = output.clone();
    assert_eq!(facade, output);
    assert_eq!(facade.schema().columns(), ["value"]);
    assert_eq!(facade.payload_bytes(), output.rows.payload_bytes());
    assert_eq!(facade.value_rows().count(), 1);
}

#[test]
fn compatibility_session_adapter_rolls_back_on_validation_failure() {
    for reject_effect in [false, true] {
        let mut database = Database::new();
        database.query("CREATE NODE LABEL AdapterRollback").unwrap();
        let check = CypherFixtureCheck::expect_rows(
            "validate uncommitted rows",
            CypherFixtureStatement::new("MATCH (n:AdapterRollback) RETURN n.id AS id"),
            ExpectedRows::RowCount(usize::from(reject_effect)),
        )
        .with_setup_query(CypherFixtureStatement::new("BEGIN TRANSACTION"))
        .with_setup_query(CypherFixtureStatement::with_parameters(
            "CREATE (:AdapterRollback {id: $id})",
            BTreeMap::from([("id".to_string(), Value::Int(7))]),
        ))
        .with_effect_query(
            CypherFixtureStatement::new("MATCH (n:AdapterRollback) RETURN n.id AS id"),
            ExpectedRows::RowCount(0),
        )
        .with_session_execution();
        let fixture = CompatibilityFixture {
            name: "adapter-rollback".to_string(),
            setup: Vec::new(),
            checks: vec![CompatibilityCheck::Cypher(check)],
        };
        let error = run_compatibility_fixture(&mut database, &fixture).unwrap_err();
        assert!(error.to_string().contains("expected 0 rows"));
        assert!(database
            .query("MATCH (n:AdapterRollback) RETURN n.id AS id")
            .unwrap()
            .rows
            .is_empty());
    }
}
