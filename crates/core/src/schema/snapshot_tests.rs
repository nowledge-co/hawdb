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

fn populated() -> Catalog {
    let mut catalog = Catalog::default();
    for id in 0..1025 {
        let name = format!("Memory{id}🦀{}", "x".repeat(128));
        let label = catalog.get_or_create_label(&name);
        let relationship = catalog.get_or_create_rel_type(&format!("LINK{id}"));
        let table = catalog.get_or_create_table(TableKind::Node, &name);
        catalog.get_or_create_property(table, "id", PropertyType::Int, false);
        catalog.get_or_create_property_index_with_kind(label, "id", IndexKind::Equality);
        catalog.get_or_create_composite_property_index(label, &["id".into(), "name".into()]);
        catalog.get_or_create_unique_constraint(label, "id");
        catalog.get_or_create_relationship_property_exists_constraint(relationship, "weight");
    }
    catalog
}

#[test]
fn catalog_snapshots_preserve_complete_old_schema_across_independent_fork_mutations() {
    let mut serving = populated();
    let original = format!("{serving:?}");
    let source = serving.clone();
    let mut candidate = source.clone();
    let name = format!("Memory0🦀{}", "x".repeat(128));
    let label = source.label_id(&name).unwrap();
    let table = source.table_id(TableKind::Node, &name).unwrap();
    let property = source.property_descriptor_id(table, "id").unwrap();
    let old_index = source
        .property_index_id_with_kind(label, "id", IndexKind::Equality)
        .unwrap();
    let old_composite = source
        .composite_property_index_id(label, &["id".into(), "name".into()])
        .unwrap();
    assert_ne!(old_index, old_composite);

    serving.import_label(label, "serving-name".into());
    serving.import_rel_type(RelTypeId(0), "SERVING_LINK".into());
    serving.get_or_create_label("serving-new");
    serving.get_or_create_rel_type("SERVING_NEW_LINK");
    let new_table = serving.get_or_create_table(TableKind::Node, "serving-new");
    serving.get_or_create_property(new_table, "name", PropertyType::Text, true);
    serving.get_or_create_property_index_with_kind(label, "serving", IndexKind::Range);
    serving.get_or_create_composite_property_index(label, &["serving".into(), "second".into()]);
    serving.get_or_create_unique_constraint(label, "serving");
    serving.get_or_create_relationship_unique_constraint(RelTypeId(0), "serving");
    assert!(serving.remove_property_descriptor(property));
    assert!(serving.remove_table_descriptor(table));

    candidate.import_label(label, "candidate-name".into());
    candidate.import_rel_type(RelTypeId(0), "CANDIDATE_LINK".into());
    candidate.get_or_create_label("candidate-new");
    candidate.get_or_create_rel_type("CANDIDATE_NEW_LINK");
    let candidate_table = candidate.get_or_create_table(TableKind::Relationship, "candidate-new");
    candidate.get_or_create_property(candidate_table, "weight", PropertyType::Float, false);
    candidate.get_or_create_property_index_with_kind(label, "candidate", IndexKind::FullText);
    candidate.get_or_create_composite_property_index(label, &["candidate".into(), "third".into()]);
    candidate.get_or_create_node_property_exists_constraint(label, "candidate");
    candidate.get_or_create_relationship_unique_constraint(RelTypeId(0), "candidate");

    assert_eq!(format!("{source:?}"), original);
    assert_eq!(source.label_name(label), Some(name.as_str()));
    assert_eq!(source.rel_type_name(RelTypeId(0)), Some("LINK0"));
    assert_eq!(source.table_id(TableKind::Node, &name), Some(table));
    assert_eq!(source.property_descriptor_id(table, "id"), Some(property));
    assert_eq!(
        source.property_index_id_with_kind(label, "id", IndexKind::Equality),
        Some(old_index)
    );
    assert_eq!(
        source.composite_property_index_id(label, &["id".into(), "name".into()]),
        Some(old_composite)
    );
    assert_eq!(source.labels().count(), 1025);
    assert_eq!(source.rel_types().count(), 1025);
    assert_eq!(source.table_descriptors().count(), 1025);
    assert_eq!(source.property_descriptors().count(), 1025);
    assert_eq!(source.property_indexes().count(), 1025);
    assert_eq!(source.composite_property_indexes().count(), 1025);
    assert_eq!(source.unique_constraints().count(), 1025);
    assert_eq!(
        source.relationship_property_exists_constraints().count(),
        1025
    );
    assert_eq!(source.label_id("serving-new"), None);
    assert_eq!(source.label_id("candidate-new"), None);
    assert_eq!(serving.label_id("candidate-new"), None);
    assert_eq!(candidate.label_id("serving-new"), None);
    assert_eq!(candidate.table_id(TableKind::Node, &name), Some(table));
    assert_eq!(
        candidate.property_descriptor_id(table, "id"),
        Some(property)
    );
    assert_eq!(
        serving.table_id(TableKind::Relationship, "candidate-new"),
        None
    );
    assert_eq!(source.unique_constraint_id(label, "serving"), None);
    assert_eq!(
        source.node_property_exists_constraint_id(label, "candidate"),
        None
    );
    assert_eq!(
        serving.node_property_exists_constraint_id(label, "candidate"),
        None
    );
    assert_eq!(candidate.unique_constraint_id(label, "serving"), None);
    drop(serving);
    drop(candidate);
    assert_eq!(format!("{source:?}"), original);
}
