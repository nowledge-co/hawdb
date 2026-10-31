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

use crate::CascadesOptimizer;
use hawdb_plan_cypher::{
    plan_with_params, BatchMutationOperation, CsvLocation, PhysicalPlan, PhysicalPlanKind,
};
use std::collections::BTreeMap;

fn optimize(query: &str) -> PhysicalPlan {
    let statement = hawdb_cypher::parse(query).unwrap();
    let logical = plan_with_params(&statement, &BTreeMap::new()).unwrap();
    CascadesOptimizer::default().optimize(&logical)
}

#[test]
fn read_sources_lower_to_a_streaming_leaf_under_ordinary_operators() {
    let physical = optimize(
        "LOAD CSV WITH HEADERS FROM $f AS row FIELDTERMINATOR '|' \
         WITH row WHERE toInteger(row.age) > 30 RETURN count(*) AS c",
    );
    let mut kinds = Vec::new();
    hawdb_plan_cypher::visit_plan(&physical, &mut |node| kinds.push(node.kind()));
    assert!(kinds.contains(&PhysicalPlanKind::LoadCsvExec), "{kinds:?}");
    assert!(
        kinds.contains(&PhysicalPlanKind::AggregateExec),
        "{kinds:?}"
    );
    let explain = physical.explain(0);
    assert!(
        explain.contains("LoadCsvExec source=$f headers=true field_terminator='|' variable=row"),
        "{explain}"
    );
    assert!(
        physical
            .instance_fingerprint()
            .contains("LoadCsvExec(source=$1:f,headers=true,terminator=124"),
        "{}",
        physical.instance_fingerprint()
    );
    assert_eq!(
        PhysicalPlanKind::LoadCsvExec.class(),
        hawdb_plan_cypher::PhysicalPlanClass::Procedure
    );
}

#[test]
fn mutations_lower_to_one_atomic_csv_mutation() {
    let physical = optimize(
        "LOAD CSV WITH HEADERS FROM '/import/knows.csv' AS row \
         MATCH (a:Person {id: toInteger(row.src)}), (b:Person {id: toInteger(row.dst)}) \
         CREATE (a)-[:KNOWS]->(b)",
    );
    let PhysicalPlan::LoadCsvMutation { source, operation } = &physical else {
        panic!("{physical:?}");
    };
    assert_eq!(
        source.location,
        CsvLocation::Literal("/import/knows.csv".to_string())
    );
    assert!(matches!(
        operation,
        BatchMutationOperation::CreateMatchedRelationship(_)
    ));
    assert_eq!(
        physical.class(),
        hawdb_plan_cypher::PhysicalPlanClass::Mutation
    );
    let fingerprint = physical.instance_fingerprint();
    assert!(
        fingerprint.contains("toInteger(row.3:src)") && fingerprint.contains("5:KNOWS"),
        "{fingerprint}"
    );
}
