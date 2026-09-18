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

//! Migration evidence for the two existing frontends, without changing routing.

use crate::{prepare_postgres_sql, SqlStatement};
use hawdb_core::HawDBError;
use hawdb_sql_syntax::{
    parse_postgres_statement, tokenize, PostgresFromItemSyntax, PostgresStatementSyntax, TokenKind,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

mod checks;

const CASES: &str = include_str!("../../fixtures/frontend_corpus_v1.jsonl");
const MANIFEST: &str = include_str!("../../fixtures/frontend_corpus_manifest_v1.json");
const LITERALS: &str = include_str!("../../fixtures/frontend_source_inventory_v1.json");
const WORKLOAD: &str = include_str!(
    "../../../qualification/fixtures/nowledge_content_store/postgres_statement_corpus_v1.json"
);
const SCHEMA: &str = include_str!(
    "../../../qualification/fixtures/nowledge_content_store/content_store_schema_v1.sql"
);

type Check = Result<(), String>;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum Outcome {
    Accept { parameters: Vec<usize> },
    Reject { code: String },
}

impl Outcome {
    fn accepts(&self) -> bool {
        matches!(self, Self::Accept { .. })
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    path: String,
    symbol: String,
    line: usize,
    adaptation: String,
    reference: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    family: String,
    source: Source,
    sql: String,
    production: Outcome,
    owned: Outcome,
    waiver: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceInventory {
    path: String,
    sha256: String,
    case_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Waiver {
    id: String,
    families: Vec<String>,
    direction: String,
    issue: String,
    reason: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    protocol: String,
    audited_base_revision: String,
    case_count: usize,
    literal_count: usize,
    owned_families: Vec<String>,
    sources: Vec<SourceInventory>,
    waivers: Vec<Waiver>,
    local_case_ids: Vec<String>,
}

#[derive(Clone)]
struct Corpus {
    manifest: Manifest,
    cases: Vec<Case>,
}

impl Corpus {
    fn load() -> Self {
        Self {
            manifest: serde_json::from_str(MANIFEST).expect("typed corpus manifest"),
            cases: CASES
                .lines()
                .map(|line| serde_json::from_str(line).expect("typed SQL corpus entry"))
                .collect(),
        }
    }
}

const SOURCE_FILES: &[(&str, &[u8])] = &[
    ("crates/sql/src/tests.rs", include_bytes!("../tests.rs")),
    (
        "crates/sql/src/tests/cross_join.rs",
        include_bytes!("cross_join.rs"),
    ),
    (
        "crates/sql/src/tests/having_from.rs",
        include_bytes!("having_from.rs"),
    ),
    (
        "crates/sql/src/tests/clause_diagnostics.rs",
        include_bytes!("clause_diagnostics.rs"),
    ),
    (
        "crates/sql/src/parser/clause_tests.rs",
        include_bytes!("../parser/clause_tests.rs"),
    ),
    (
        "crates/sql/src/pgq/tests.rs",
        include_bytes!("../pgq/tests.rs"),
    ),
    (
        "crates/sql/src/pgq/lowering/tests.rs",
        include_bytes!("../pgq/lowering/tests.rs"),
    ),
    (
        "crates/sql-syntax/tests/postgres_pgq.rs",
        include_bytes!("../../../sql-syntax/tests/postgres_pgq.rs"),
    ),
    (
        "crates/sql-syntax/tests/postgres_select_pgq.rs",
        include_bytes!("../../../sql-syntax/tests/postgres_select_pgq.rs"),
    ),
    (
        "crates/sql-syntax/tests/support/alias_boundaries.rs",
        include_bytes!("../../../sql-syntax/tests/support/alias_boundaries.rs"),
    ),
    (
        "crates/qualification/fixtures/nowledge_content_store/postgres_statement_corpus_v1.json",
        WORKLOAD.as_bytes(),
    ),
    (
        "crates/qualification/fixtures/nowledge_content_store/content_store_schema_v1.sql",
        SCHEMA.as_bytes(),
    ),
];

fn source_bytes(path: &str) -> Option<&'static [u8]> {
    SOURCE_FILES
        .iter()
        .find_map(|(source, bytes)| (*source == path).then_some(*bytes))
}

fn production(sql: &str) -> Result<(Outcome, Option<&'static str>), String> {
    match prepare_postgres_sql(sql) {
        Ok(prepared) => {
            let family = match prepared.statement {
                SqlStatement::Select(_) => "select",
                SqlStatement::Explain(_) => "explain",
                SqlStatement::Insert(_) => "insert",
                SqlStatement::Update(_) => "update",
                SqlStatement::Delete(_) => "delete",
                SqlStatement::CreateTable(_) => "create_table",
                SqlStatement::CreateIndex(_) => "create_index",
                SqlStatement::AlterTableAddColumn(_) => "alter_table",
            };
            Ok((
                Outcome::Accept {
                    parameters: prepared.parameters.iter().map(|p| p.position).collect(),
                },
                Some(family),
            ))
        }
        Err(error) => {
            let code = match error {
                HawDBError::Parse(_) => "Parse",
                HawDBError::Semantic(_) => "Semantic",
                other => {
                    return Err(format!(
                        "unexpected production preparation error: {other:?}"
                    ))
                }
            };
            Ok((Outcome::Reject { code: code.into() }, None))
        }
    }
}

fn owned(sql: &str) -> Result<(Outcome, Option<&'static str>), String> {
    match parse_postgres_statement(sql) {
        Ok(statement) => {
            let family = match statement {
                PostgresStatementSyntax::CreatePropertyGraph(_) => "create_property_graph",
                PostgresStatementSyntax::Select(select) => {
                    if select.from.iter().any(|from| {
                        matches!(from.relation, PostgresFromItemSyntax::GraphTable(_))
                            || from.joins.iter().any(|join| {
                                matches!(join.relation, PostgresFromItemSyntax::GraphTable(_))
                            })
                    }) {
                        "select_graph_table"
                    } else {
                        "select"
                    }
                }
            };
            // This is lexical parameter inventory, not owned semantic preparation.
            let parameters = tokenize(sql)
                .map_err(|error| error.to_string())?
                .into_iter()
                .filter_map(|token| match token.kind {
                    TokenKind::Parameter(position) => Some(position as usize),
                    _ => None,
                })
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            Ok((Outcome::Accept { parameters }, Some(family)))
        }
        Err(error) => {
            if error.span.start > error.span.end
                || error.span.end > sql.len()
                || !sql.is_char_boundary(error.span.start)
                || !sql.is_char_boundary(error.span.end)
            {
                return Err(format!("owned error has an invalid source span: {error:?}"));
            }
            Ok((
                Outcome::Reject {
                    code: format!("{:?}", error.code),
                },
                None,
            ))
        }
    }
}

fn check_case(case: &Case) -> Check {
    for (frontend, expected, observed) in [
        ("production", &case.production, production(&case.sql)?),
        ("owned", &case.owned, owned(&case.sql)?),
    ] {
        if *expected != observed.0 {
            return Err(format!(
                "{} {frontend}: expected {expected:?}, got {:?}; {}::{}",
                case.id, observed.0, case.source.path, case.source.symbol
            ));
        }
        if observed.1.is_some_and(|family| family != case.family) {
            return Err(format!(
                "{} {frontend}: incorrect statement family",
                case.id
            ));
        }
    }
    if case.production.accepts() && case.owned.accepts() && case.production != case.owned {
        return Err(format!("{}: parameter inventory differs", case.id));
    }
    Ok(())
}

#[test]
fn shared_frontend_corpus_preserves_outcomes_parameters_and_inventory() {
    let corpus = Corpus::load();
    checks::manifest(&corpus).unwrap();
    checks::frozen_inputs(&corpus).unwrap();
    checks::literal_inventory(&corpus).unwrap();
    for case in &corpus.cases {
        check_case(case).unwrap();
    }
}

#[test]
#[ignore = "bounded local frontend corpus differential campaign"]
fn frontend_corpus_differential_campaign() {
    let corpus = Corpus::load();
    checks::manifest(&corpus).unwrap();
    let mut state = 0x159c_0a5eu64;
    for iteration in 0..4_096 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let mut case = corpus.cases[state as usize % corpus.cases.len()].clone();
        let prefix = [" ", "\t\n", "/* corpus */\n", "-- corpus\n"][iteration % 4];
        case.sql = format!("{prefix}{}\n", case.sql);
        check_case(&case).unwrap_or_else(|error| panic!("iteration {iteration}: {error}"));
    }
}
