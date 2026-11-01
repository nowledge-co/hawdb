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

use crate::{
    parse, ClauseKind, LoadCsvClause, QueryPipeline, ScalarExpressionKind, SetValueExpression,
    Statement, ValueExpression, ValueExpressionKind,
};
use hawdb_core::{ConversionFunction, HawDBError, Value};

fn pipeline(query: &str) -> QueryPipeline {
    match parse(query).unwrap() {
        Statement::Pipeline(parsed) => *parsed,
        other => panic!("expected a pipeline, got {other:?}"),
    }
}

fn load(query: &QueryPipeline) -> &LoadCsvClause {
    let ClauseKind::LoadCsv(load) = &query.clauses[0].kind else {
        panic!("expected LOAD CSV first: {query:?}");
    };
    load
}

fn kind(value: &ValueExpression) -> &ValueExpressionKind {
    &value.kind
}

#[test]
fn load_csv_clause_parses_sources_headers_and_terminators() {
    let query = pipeline("LOAD CSV FROM $file AS line RETURN line");
    let clause = load(&query);
    assert!(!clause.with_headers);
    assert_eq!(clause.variable, "line");
    assert_eq!(clause.field_terminator, ',');
    assert_eq!(
        kind(&clause.source),
        &ValueExpressionKind::Parameter("file".to_string())
    );
    assert!(matches!(query.clauses[1].kind, ClauseKind::Return(_)));

    let query = pipeline(
        "load csv with headers from 'file:///data/people.csv' as row fieldterminator '|' \
         RETURN row.id AS id",
    );
    let clause = load(&query);
    assert!(clause.with_headers);
    assert_eq!(clause.field_terminator, '|');
    assert_eq!(
        kind(&clause.source),
        &ValueExpressionKind::Literal(Value::String("file:///data/people.csv".to_string()))
    );
    for (literal, expected) in [("'\\t'", '\t'), ("\";\"", ';'), ("' '", ' ')] {
        let query = pipeline(&format!(
            "LOAD CSV FROM $f AS line FIELDTERMINATOR {literal} RETURN line"
        ));
        assert_eq!(load(&query).field_terminator, expected, "{literal}");
    }
}

#[test]
fn load_csv_statements_are_pipelines_through_every_statement_wrapper() {
    for query in [
        "EXPLAIN LOAD CSV FROM $f AS line RETURN line",
        "CYPHER SYSTEM.cpu = 1 LOAD CSV FROM $f AS line RETURN line",
    ] {
        let statement = parse(query).unwrap();
        let body = match &statement {
            Statement::Explain(explain) => &explain.statement,
            Statement::CypherQuery(query) => &query.statement,
            other => panic!("{other:?}"),
        };
        assert!(matches!(body, Statement::Pipeline(_)), "{query}: {body:?}");
    }
    // `WITH HEADERS` is not a projection: two later WITH clauses still parse
    // through the multi-stage grammar, and one WITH through the simple one.
    for query in [
        "LOAD CSV WITH HEADERS FROM $f AS row WITH row WITH row RETURN count(*) AS c",
        "LOAD CSV WITH HEADERS FROM $f AS row WITH row WHERE row.id IS NOT NULL RETURN row",
    ] {
        let parsed = pipeline(query);
        assert!(load(&parsed).with_headers, "{query}");
        assert!(
            matches!(parsed.clauses[1].kind, ClauseKind::With(_)),
            "{query}"
        );
    }
}

#[test]
fn load_csv_rejects_invalid_sources_terminators_and_shapes() {
    for (query, message) in [
        (
            "LOAD CSV FROM 42 AS line RETURN line",
            "string literal or parameter source",
        ),
        (
            "LOAD CSV FROM ['a.csv'] AS line RETURN line",
            "string literal or parameter source",
        ),
        (
            "LOAD CSV FROM $f AS line FIELDTERMINATOR '||' RETURN line",
            "FIELDTERMINATOR must be one ASCII character",
        ),
        (
            "LOAD CSV FROM $f AS line FIELDTERMINATOR '\"' RETURN line",
            "FIELDTERMINATOR must be one ASCII character",
        ),
        (
            "LOAD CSV FROM $f AS line FIELDTERMINATOR 'é' RETURN line",
            "FIELDTERMINATOR must be one ASCII character",
        ),
        (
            "LOAD CSV FROM $f AS line FIELDTERMINATOR $t RETURN line",
            "FIELDTERMINATOR requires a string literal",
        ),
        ("LOAD CSV FROM $f line RETURN line", "expected AS"),
        (
            "LOAD CSV HEADERS FROM $f AS line RETURN line",
            "expected FROM",
        ),
        ("LOAD CSV FROM $f AS line", "query requires RETURN"),
    ] {
        let error = parse(query).unwrap_err();
        assert!(matches!(error, HawDBError::Parse(_)), "{query}: {error:?}");
        assert!(error.to_string().contains(message), "{query}: {error}");
    }
}

#[test]
fn property_maps_and_set_values_accept_conversions_and_list_indexes() {
    let query = pipeline(
        "LOAD CSV FROM $f AS line MERGE (c:City {name: line[1], code: toInteger(line[-1])}) \
         ON CREATE SET c.rank = toFloat(toString(line[0]))",
    );
    let ClauseKind::Merge {
        pattern, on_create, ..
    } = &query.clauses[1].kind
    else {
        panic!("expected MERGE");
    };
    assert_eq!(
        kind(&pattern.first.properties["name"]),
        &ValueExpressionKind::BindingIndex {
            variable: "line".to_string(),
            index: 1
        }
    );
    let ValueExpressionKind::Conversion { function, argument } =
        kind(&pattern.first.properties["code"])
    else {
        panic!("expected a conversion");
    };
    assert_eq!(*function, ConversionFunction::ToInteger);
    assert_eq!(
        kind(argument),
        &ValueExpressionKind::BindingIndex {
            variable: "line".to_string(),
            index: -1
        }
    );
    let SetValueExpression::Value(value) = &on_create[0].value else {
        panic!("expected a value assignment");
    };
    let ValueExpressionKind::Conversion {
        function: ConversionFunction::ToFloat,
        argument,
    } = kind(value)
    else {
        panic!("expected toFloat: {value:?}");
    };
    assert!(matches!(
        kind(argument),
        ValueExpressionKind::Conversion {
            function: ConversionFunction::ToString,
            ..
        }
    ));
    // Function names do not shadow ordinary row properties.
    let query =
        pipeline("LOAD CSV WITH HEADERS FROM $f AS toInteger CREATE (:N {v: toInteger.id})");
    let ClauseKind::Create(patterns) = &query.clauses[1].kind else {
        panic!("expected CREATE");
    };
    assert_eq!(
        kind(&patterns[0].first.properties["v"]),
        &ValueExpressionKind::BindingProperty {
            variable: "toInteger".to_string(),
            property: "id".to_string()
        }
    );
}

#[test]
fn scalar_conversions_parse_in_projections_and_predicates() {
    let query = pipeline(
        "LOAD CSV WITH HEADERS FROM $f AS row WITH row WHERE toInteger(row.age) > 30 \
         RETURN TOINTEGER(row.id) AS id, toBoolean(row.flag) AS flag",
    );
    let ClauseKind::Return(projection) = &query.clauses[2].kind else {
        panic!("expected RETURN");
    };
    let functions = projection
        .items
        .iter()
        .map(|item| match &item.expression.kind {
            crate::ReturnExpressionKind::Value(value) => match &value.kind {
                ScalarExpressionKind::Conversion { function, argument } => {
                    assert!(matches!(
                        argument.kind,
                        ScalarExpressionKind::Property { .. }
                    ));
                    *function
                }
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        functions,
        [ConversionFunction::ToInteger, ConversionFunction::ToBoolean]
    );
    let ClauseKind::With(projection) = &query.clauses[1].kind else {
        panic!("expected WITH");
    };
    assert!(projection.predicate.is_some());
}

#[test]
fn record_indexes_and_conversions_of_row_reads_are_row_bound() {
    let node = |kind| ValueExpression::synthetic(kind);
    let index = || {
        node(ValueExpressionKind::BindingIndex {
            variable: "row".into(),
            index: 0,
        })
    };
    let property = || {
        node(ValueExpressionKind::BindingProperty {
            variable: "row".into(),
            property: "id".into(),
        })
    };
    let conversion = |argument| ValueExpressionKind::Conversion {
        function: ConversionFunction::ToInteger,
        argument: Box::new(argument),
    };
    // List literals of these elements are per-row lists, not constant values.
    assert!(index().kind.reads_row_binding());
    assert!(conversion(index()).reads_row_binding());
    assert!(conversion(property()).reads_row_binding());
    assert!(ValueExpressionKind::List(vec![node(conversion(property()))]).reads_row_binding());
    assert!(!conversion(node(ValueExpressionKind::Literal(Value::String(
        "1".into()
    ))))
    .reads_row_binding());
    assert!(!conversion(node(ValueExpressionKind::Parameter("p".into()))).reads_row_binding());
}
