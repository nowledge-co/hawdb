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

use crate::{parse, ClauseKind, Statement};

#[test]
fn public_match_parser_retains_ordered_clauses_and_original_spans() {
    let cases = [
        (
            "MATCH (m:Memory {id: $id}) RETURN m.id",
            vec!["MATCH", "RETURN"],
        ),
        (
            "MATCH (m:Memory) WITH m RETURN m.id",
            vec!["MATCH", "WITH", "RETURN"],
        ),
        (
            "MATCH (m:Memory) WITH m WITH m RETURN m.id",
            vec!["MATCH", "WITH", "WITH", "RETURN"],
        ),
        (
            "MATCH (m:Memory {id: $id}) SET m.title = $title RETURN m.title",
            vec!["MATCH", "SET", "RETURN"],
        ),
        (
            "MATCH (m:Memory {id: $id}) DETACH DELETE m",
            vec!["MATCH", "DETACH DELETE"],
        ),
        (
            "MATCH (a:Memory {id: $a}), (b:Memory {id: $b}) CREATE (a)-[:EVOLVES]->(b)",
            vec!["MATCH", "CREATE"],
        ),
        (
            "MATCH (a:Memory {id: $a}), (b:Memory {id: $b}) MERGE (a)-[:EVOLVES]->(b)",
            vec!["MATCH", "MERGE"],
        ),
    ];
    for (source, expected) in cases {
        let Statement::Pipeline(query) = parse(source).unwrap() else {
            panic!("public MATCH parser must use the clause pipeline: {source}");
        };
        assert_eq!(query.span.unwrap().text(source), Some(source));
        assert_eq!(query.clauses.len(), expected.len(), "{source}");
        let mut previous_end = 0;
        for (clause, expected) in query.clauses.iter().zip(expected) {
            let keyword = match clause.kind {
                ClauseKind::Match {
                    optional: false, ..
                } => "MATCH",
                ClauseKind::With(_) => "WITH",
                ClauseKind::Return(_) => "RETURN",
                ClauseKind::Create(_) => "CREATE",
                ClauseKind::Merge { .. } => "MERGE",
                ClauseKind::Set(_) => "SET",
                ClauseKind::Delete { detach: true, .. } => "DETACH DELETE",
                _ => panic!("unexpected clause: {clause:?}"),
            };
            assert_eq!(keyword, expected, "{source}");
            let span = clause.span.expect("parsed clauses require source ranges");
            assert!(
                span.start >= previous_end,
                "clause ranges must preserve lexical order: {source}"
            );
            assert!(span.text(source).unwrap().starts_with(expected), "{source}");
            previous_end = span.end;
        }
    }
}

#[test]
fn public_query_wrappers_preserve_the_pipeline_body() {
    for source in [
        "EXPLAIN MATCH (m:Memory {id: $id}) RETURN m.id",
        "CYPHER SYSTEM.execution = 'auto' MATCH (m:Memory {id: $id}) RETURN m.id",
    ] {
        let statement = parse(source).unwrap();
        let body = match &statement {
            Statement::Explain(explain) => &explain.statement,
            Statement::CypherQuery(query) => &query.statement,
            _ => panic!("expected a query wrapper"),
        };
        let Statement::Pipeline(pipeline) = body else {
            panic!("query wrappers must preserve the clause pipeline: {source}");
        };
        let span = pipeline.span.unwrap();
        assert_eq!(
            span.text(source),
            Some("MATCH (m:Memory {id: $id}) RETURN m.id")
        );
    }
}

#[test]
fn public_multi_with_parser_retains_bounded_optional_admission() {
    let body = "MATCH (a:Node {id: $id}) WITH a WITH a OPTIONAL MATCH (a)-[:LINK*1..2]->(b:Node) RETURN b.id";
    for source in [
        body.to_string(),
        format!("EXPLAIN {body}"),
        format!("CYPHER SYSTEM.execution = 'auto' {body}"),
    ] {
        let statement = parse(&source).unwrap();
        let statement = match &statement {
            Statement::Explain(explain) => &explain.statement,
            Statement::CypherQuery(query) => &query.statement,
            statement => statement,
        };
        let Statement::Pipeline(pipeline) = statement else {
            panic!("previously admitted multi-WITH reads require the pipeline");
        };
        let ClauseKind::Match {
            optional, patterns, ..
        } = &pipeline.clauses[3].kind
        else {
            panic!("expected OPTIONAL MATCH");
        };
        assert!(*optional);
        assert_eq!(patterns[0].steps[0].relationship.min_hops, 1);
        assert_eq!(patterns[0].steps[0].relationship.max_hops, 2);
    }
    for prefix in ["", "WITH a "] {
        let source =
            format!("MATCH (a:Node) {prefix}OPTIONAL MATCH (a)-[:LINK*1..2]->(b:Node) RETURN b.id");
        assert!(parse(&source).is_err(), "{source}");
    }
}
