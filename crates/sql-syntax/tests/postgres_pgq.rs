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

//! Syntax cases adapted from PostgreSQL's property-graph regression suite.
//!
//! Reference revision: PostgreSQL 3d00537feb565c410baf41bb301eee338e4b2317.
//! Reference files: src/test/regress/sql/create_property_graph.sql and
//! src/test/regress/sql/graph_table.sql. The cases are reduced and renamed for
//! HawDB; PostgreSQL expected-output text is not copied.

use hawdb_sql_syntax::{
    parse_graph_table, parse_pgq_statement, ElementLabel, GraphEdgeDirection,
    GraphPathPrimarySyntax, PgqStatement, PropertyExposure, SyntaxErrorCode,
};

#[test]
fn preserves_postgres_source_spans_and_table_aliases() {
    let sql = "GRAPH_TABLE (
        app.knowledge
        MATCH (memory IS Memory WHERE memory.id = $1)-[IS MENTIONS]->(entity)
        COLUMNS (memory.id AS memory_id, entity.name)
    ) AS matched(memory_id, entity_name)";
    let table = parse_graph_table(sql).expect("PostgreSQL GRAPH_TABLE syntax");
    assert_eq!(table.graph.parts.len(), 2);
    assert_eq!(table.columns.len(), 2);
    assert_eq!(
        &sql[table.pattern.span.start..table.pattern.span.end],
        "(memory IS Memory WHERE memory.id = $1)-[IS MENTIONS]->(entity)"
    );
    assert_eq!(table.alias.expect("table alias").columns.len(), 2);
}

#[test]
fn parses_postgres_property_graph_label_and_property_forms() {
    let PgqStatement::CreatePropertyGraph(graph) = parse_pgq_statement(
        "CREATE PROPERTY GRAPH knowledge
         VERTEX TABLES (
             documents KEY (id) NO PROPERTIES,
             entities DEFAULT LABEL,
             chunks KEY (id)
                 LABEL searchable PROPERTIES (content, rank + 1 AS boosted_rank)
                 LABEL stored PROPERTIES ALL COLUMNS
         )
         EDGE TABLES (
             mentions KEY (document_id, entity_id)
                 SOURCE KEY (document_id) REFERENCES documents (id)
                 DESTINATION KEY (entity_id) REFERENCES entities (id)
                 DEFAULT LABEL
                 LABEL weighted PROPERTIES (confidence)
         )",
    )
    .expect("PostgreSQL property graph syntax");

    assert_eq!(graph.vertex_tables.len(), 3);
    assert_eq!(graph.edge_tables.len(), 1);

    let documents = graph.vertex_tables[0].exposure.as_ref().expect("exposure");
    assert!(matches!(
        documents.labels[0].properties,
        PropertyExposure::NoProperties
    ));

    let entities = graph.vertex_tables[1].exposure.as_ref().expect("exposure");
    assert!(matches!(entities.labels[0].label, ElementLabel::Default));
    assert!(matches!(
        entities.labels[0].properties,
        PropertyExposure::AllColumns
    ));

    let chunks = graph.vertex_tables[2].exposure.as_ref().expect("exposure");
    assert_eq!(chunks.labels.len(), 2);
    let PropertyExposure::Expressions(properties) = &chunks.labels[0].properties else {
        panic!("explicit properties");
    };
    assert_eq!(properties.len(), 2);
    assert!(properties[1].alias.is_some());

    let mentions = graph.edge_tables[0].exposure.as_ref().expect("exposure");
    assert_eq!(mentions.labels.len(), 2);
    assert!(matches!(mentions.labels[0].label, ElementLabel::Default));
}

#[test]
fn parses_postgres_property_graph_endpoint_shorthand() {
    let PgqStatement::CreatePropertyGraph(graph) = parse_pgq_statement(
        "CREATE PROPERTY GRAPH knowledge
         VERTEX TABLES (documents, entities)
         EDGE TABLES (mentions SOURCE documents DESTINATION entities)",
    )
    .expect("endpoint shorthand");

    let edge = &graph.edge_tables[0];
    assert!(edge.source.key.is_empty());
    assert!(edge.source.vertex_key.is_empty());
    assert!(edge.destination.key.is_empty());
}

#[test]
fn rejects_postgres_property_graph_syntax_errors() {
    for sql in [
        "CREATE UNLOGGED PROPERTY GRAPH knowledge",
        "CREATE PROPERTY GRAPH knowledge INVALID",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES ()",
        "CREATE PROPERTY GRAPH knowledge EDGE TABLES ()",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (documents LABEL)",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (documents DEFAULT)",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (documents UNKNOWN EXPOSURE)",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (documents PROPERTIES ())",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (documents PROPERTIES ALL)",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (documents KEY ())",
        "CREATE PROPERTY GRAPH knowledge EDGE TABLES (mentions SOURCE KEY () REFERENCES documents (id) DESTINATION entities)",
        "CREATE PROPERTY GRAPH knowledge EDGE TABLES (mentions SOURCE documents DESTINATION entities) VERTEX TABLES (documents, entities)",
        "CREATE PROPERTY GRAPH knowledge VERTEX TABLES (documents) VERTEX TABLES (entities)",
    ] {
        assert!(
            parse_pgq_statement(sql).is_err(),
            "invalid PostgreSQL syntax was accepted: {sql}"
        );
    }
}

#[test]
fn parses_postgres_graph_path_shapes() {
    let cases = [
        (
            "GRAPH_TABLE (knowledge MATCH (d IS document) COLUMNS (d.id))",
            1,
        ),
        (
            "GRAPH_TABLE (knowledge MATCH (d)-[m IS mentions]->(e IS entity) COLUMNS (d.id, e.name))",
            3,
        ),
        (
            "GRAPH_TABLE (knowledge MATCH (e)<-[m]-(d) COLUMNS (d.id))",
            3,
        ),
        (
            "GRAPH_TABLE (knowledge MATCH (d)-[m]-(e) COLUMNS (d.id))",
            3,
        ),
        (
            "GRAPH_TABLE (knowledge MATCH (d)->(e)<-(x)-(y) COLUMNS (d.id))",
            7,
        ),
        (
            "GRAPH_TABLE (knowledge MATCH (d IS document | archived WHERE d.id = $1)->(e) WHERE e.active COLUMNS (d.id AS document_id)) AS matched",
            3,
        ),
    ];

    for (sql, factor_count) in cases {
        let table = parse_graph_table(sql).expect("PostgreSQL graph path syntax");
        assert_eq!(table.pattern.paths.len(), 1);
        assert_eq!(table.pattern.paths[0].factors.len(), factor_count);
    }
}

#[test]
fn preserves_edge_direction_and_quantifier_syntax() {
    let table = parse_graph_table("GRAPH_TABLE (knowledge MATCH (d)-[m]->{1,3}(e) COLUMNS (d.id))")
        .expect("quantified edge syntax");
    let factor = &table.pattern.paths[0].factors[1];
    let GraphPathPrimarySyntax::Edge(edge) = &factor.primary else {
        panic!("edge factor");
    };
    assert_eq!(edge.direction, GraphEdgeDirection::Right);
    let quantifier = factor.quantifier.as_ref().expect("quantifier");
    assert_eq!((quantifier.min, quantifier.max), (1, 3));
}

#[test]
fn keeps_postgres_transform_rejections_out_of_the_syntax_layer() {
    for sql in [
        "GRAPH_TABLE (knowledge MATCH ()() COLUMNS (1 AS one))",
        "GRAPH_TABLE (knowledge MATCH -> COLUMNS (1 AS one))",
        "GRAPH_TABLE (knowledge MATCH ()-[]- COLUMNS (1 AS one))",
        "GRAPH_TABLE (knowledge MATCH ()-> ->() COLUMNS (1 AS one))",
        "GRAPH_TABLE (knowledge MATCH (a), (b) COLUMNS (1 AS one))",
        "GRAPH_TABLE (knowledge MATCH ((a)->(b)) COLUMNS (a.id))",
    ] {
        parse_graph_table(sql).expect("raw syntax accepted before semantic binding");
    }
}

#[test]
fn rejects_malformed_graph_table_syntax() {
    let cases = [
        "GRAPH_TABLE (knowledge MATCH COLUMNS (1 AS one))",
        "GRAPH_TABLE (knowledge MATCH (d IS) COLUMNS (d.id))",
        "GRAPH_TABLE (knowledge MATCH (d WHERE) COLUMNS (d.id))",
        "GRAPH_TABLE (knowledge MATCH (d)-[m IS]->(e) COLUMNS (d.id))",
        "GRAPH_TABLE (knowledge MATCH (d)-[m](e) COLUMNS (d.id))",
        "GRAPH_TABLE (knowledge MATCH (d)->{1,}(e) COLUMNS (d.id))",
        "GRAPH_TABLE (knowledge MATCH (d) COLUMNS ())",
    ];
    for sql in cases {
        let error = parse_graph_table(sql).expect_err("malformed graph syntax must fail");
        assert!(matches!(
            error.code,
            SyntaxErrorCode::UnexpectedToken | SyntaxErrorCode::UnexpectedEnd
        ));
    }
}

#[test]
fn bounds_recursive_graph_pattern_nesting() {
    let accepted_nesting = 128;
    let accepted = format!(
        "GRAPH_TABLE (knowledge MATCH {}(node){} COLUMNS (node.id))",
        "(".repeat(accepted_nesting),
        ")".repeat(accepted_nesting)
    );
    parse_graph_table(&accepted).expect("nesting at the limit");

    let rejected_nesting = accepted_nesting + 1;
    let sql = format!(
        "GRAPH_TABLE (knowledge MATCH {}(node){} COLUMNS (node.id))",
        "(".repeat(rejected_nesting),
        ")".repeat(rejected_nesting)
    );
    let error = parse_graph_table(&sql).expect_err("graph nesting must be bounded");
    assert_eq!(
        error.code,
        SyntaxErrorCode::GraphPatternNestingLimitExceeded
    );
}
