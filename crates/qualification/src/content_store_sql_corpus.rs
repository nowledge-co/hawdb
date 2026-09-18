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

use hawdb::{HawDBError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub const NOWLEDGE_CONTENT_STORE_SQL_CORPUS_PROTOCOL: &str =
    "hawdb-nowledge-content-store-sql-corpus-v1";
pub const NOWLEDGE_CONTENT_STORE_SQL_CORPUS_REVISION: &str = "nowledge-content-store-postgres-v1";
pub const NOWLEDGE_CONTENT_STORE_SCHEMA_PROTOCOL: &str = "hawdb-nowledge-content-store-schema-v1";
pub const NOWLEDGE_CONTENT_STORE_SCHEMA_REVISION: &str = "nowledge-content-store-schema-v1";

const CORPUS_JSON: &str =
    include_str!("../fixtures/nowledge_content_store/postgres_statement_corpus_v1.json");
const SCHEMA_SQL: &str =
    include_str!("../fixtures/nowledge_content_store/content_store_schema_v1.sql");
const REQUIRED_TABLES: &[&str] = &[
    "content_documents",
    "thread_messages",
    "content_chunks",
    "content_anchors",
    "content_migration_state",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStoreSqlStatementKind {
    Read,
    Mutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStoreSqlStatementClassification {
    Required,
    Rewritten,
    RetainedOnSqlite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentStoreSqlCallerCoverage {
    Covered,
    Partial,
    RetainedOnSqlite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreSqlCallerSpec {
    pub source_path: String,
    pub symbol: String,
    pub coverage: ContentStoreSqlCallerCoverage,
    pub statements: Vec<String>,
    pub note: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreSqlStatementSpec {
    pub name: String,
    pub kind: ContentStoreSqlStatementKind,
    pub classification: ContentStoreSqlStatementClassification,
    pub transaction_group: Option<String>,
    pub sql: String,
    pub parameters: Vec<String>,
    pub result_columns: Vec<String>,
    pub ordering: Vec<String>,
    pub max_rows: usize,
    pub max_payload_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentStoreSqlCorpus {
    pub protocol: String,
    pub revision: String,
    pub schema_protocol: String,
    pub schema_revision: String,
    pub source_engine: String,
    pub target_dialect: String,
    pub tables: Vec<String>,
    pub source_inventory: Vec<ContentStoreSqlCallerSpec>,
    pub statements: Vec<ContentStoreSqlStatementSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreSqlCorpusIdentity {
    pub protocol: String,
    pub revision: String,
    pub sha256: String,
    pub schema_protocol: String,
    pub schema_revision: String,
    pub schema_sha256: String,
    pub schema_statement_count: usize,
    pub statement_count: usize,
    pub required_statement_count: usize,
    pub rewritten_statement_count: usize,
    pub retained_on_sqlite_statement_count: usize,
    pub caller_count: usize,
    pub covered_caller_count: usize,
    pub partial_caller_count: usize,
    pub retained_on_sqlite_caller_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ContentStoreSchemaIdentity {
    pub protocol: String,
    pub revision: String,
    pub sha256: String,
    pub statement_count: usize,
}

impl ContentStoreSqlCorpus {
    pub fn identity(&self) -> ContentStoreSqlCorpusIdentity {
        let schema_identity = nowledge_content_store_schema_identity();
        ContentStoreSqlCorpusIdentity {
            protocol: self.protocol.clone(),
            revision: self.revision.clone(),
            sha256: sha256_hex(CORPUS_JSON.as_bytes()),
            schema_protocol: schema_identity.protocol,
            schema_revision: schema_identity.revision,
            schema_sha256: schema_identity.sha256,
            schema_statement_count: schema_identity.statement_count,
            statement_count: self.statements.len(),
            required_statement_count: self
                .statements
                .iter()
                .filter(|statement| {
                    statement.classification == ContentStoreSqlStatementClassification::Required
                })
                .count(),
            rewritten_statement_count: self
                .statements
                .iter()
                .filter(|statement| {
                    statement.classification == ContentStoreSqlStatementClassification::Rewritten
                })
                .count(),
            retained_on_sqlite_statement_count: self
                .statements
                .iter()
                .filter(|statement| {
                    statement.classification
                        == ContentStoreSqlStatementClassification::RetainedOnSqlite
                })
                .count(),
            caller_count: self.source_inventory.len(),
            covered_caller_count: self
                .source_inventory
                .iter()
                .filter(|caller| caller.coverage == ContentStoreSqlCallerCoverage::Covered)
                .count(),
            partial_caller_count: self
                .source_inventory
                .iter()
                .filter(|caller| caller.coverage == ContentStoreSqlCallerCoverage::Partial)
                .count(),
            retained_on_sqlite_caller_count: self
                .source_inventory
                .iter()
                .filter(|caller| caller.coverage == ContentStoreSqlCallerCoverage::RetainedOnSqlite)
                .count(),
        }
    }

    pub fn statement(&self, name: &str) -> Option<&ContentStoreSqlStatementSpec> {
        self.statements
            .iter()
            .find(|statement| statement.name == name)
    }

    pub fn validate(&self) -> Result<()> {
        if self.protocol != NOWLEDGE_CONTENT_STORE_SQL_CORPUS_PROTOCOL {
            return Err(HawDBError::Semantic(format!(
                "content-store SQL corpus protocol mismatch: expected {NOWLEDGE_CONTENT_STORE_SQL_CORPUS_PROTOCOL}, got {}",
                self.protocol
            )));
        }
        if self.revision != NOWLEDGE_CONTENT_STORE_SQL_CORPUS_REVISION {
            return Err(HawDBError::Semantic(format!(
                "content-store SQL corpus revision mismatch: expected {NOWLEDGE_CONTENT_STORE_SQL_CORPUS_REVISION}, got {}",
                self.revision
            )));
        }
        if self.schema_protocol != NOWLEDGE_CONTENT_STORE_SCHEMA_PROTOCOL {
            return Err(HawDBError::Semantic(format!(
                "content-store schema protocol mismatch: expected {NOWLEDGE_CONTENT_STORE_SCHEMA_PROTOCOL}, got {}",
                self.schema_protocol
            )));
        }
        if self.schema_revision != NOWLEDGE_CONTENT_STORE_SCHEMA_REVISION {
            return Err(HawDBError::Semantic(format!(
                "content-store schema revision mismatch: expected {NOWLEDGE_CONTENT_STORE_SCHEMA_REVISION}, got {}",
                self.schema_revision
            )));
        }
        if self.source_engine != "sqlite" || self.target_dialect != "postgresql" {
            return Err(HawDBError::Semantic(
                "content-store SQL corpus must map sqlite to postgresql".to_string(),
            ));
        }

        let tables = self
            .tables
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let required_tables = REQUIRED_TABLES.iter().copied().collect::<BTreeSet<_>>();
        if tables != required_tables {
            return Err(HawDBError::Semantic(
                "content-store SQL corpus table set does not match the v1 scope".to_string(),
            ));
        }

        let mut names = BTreeSet::new();
        for statement in &self.statements {
            validate_statement(statement)?;
            if !names.insert(statement.name.as_str()) {
                return Err(HawDBError::Semantic(format!(
                    "content-store SQL corpus contains duplicate statement name {}",
                    statement.name
                )));
            }
        }

        if self.source_inventory.is_empty() {
            return Err(HawDBError::Semantic(
                "content-store SQL corpus source inventory must not be empty".to_string(),
            ));
        }
        let mut callers = BTreeSet::new();
        for caller in &self.source_inventory {
            if caller.source_path.is_empty()
                || caller.symbol.is_empty()
                || caller.statements.is_empty()
                || caller.note.is_empty()
            {
                return Err(HawDBError::Semantic(
                    "content-store caller inventory fields must not be empty".to_string(),
                ));
            }
            if !callers.insert((caller.source_path.as_str(), caller.symbol.as_str())) {
                return Err(HawDBError::Semantic(format!(
                    "content-store SQL corpus contains duplicate caller {}::{}",
                    caller.source_path, caller.symbol
                )));
            }
            for statement in &caller.statements {
                if !names.contains(statement.as_str()) {
                    return Err(HawDBError::Semantic(format!(
                        "content-store caller {} references unknown statement {statement}",
                        caller.symbol
                    )));
                }
            }
        }

        for table in REQUIRED_TABLES {
            let data_owned = self
                .statements
                .iter()
                .any(|statement| contains_sql_identifier(&statement.sql, table));
            if !data_owned {
                return Err(HawDBError::Semantic(format!(
                    "content-store SQL corpus does not own data behavior for {table}"
                )));
            }
        }
        Ok(())
    }

    pub fn json(&self) -> Result<serde_json::Value> {
        Ok(serde_json::json!({
            "identity": self.identity(),
            "schema": {
                "identity": nowledge_content_store_schema_identity(),
                "statements": nowledge_content_store_schema_statements()?,
            },
            "corpus": self,
        }))
    }
}

pub fn nowledge_content_store_schema_identity() -> ContentStoreSchemaIdentity {
    ContentStoreSchemaIdentity {
        protocol: NOWLEDGE_CONTENT_STORE_SCHEMA_PROTOCOL.to_string(),
        revision: NOWLEDGE_CONTENT_STORE_SCHEMA_REVISION.to_string(),
        sha256: sha256_hex(SCHEMA_SQL.as_bytes()),
        statement_count: SCHEMA_SQL
            .lines()
            .filter(|line| !line.trim().is_empty())
            .count(),
    }
}

pub fn nowledge_content_store_schema_statements() -> Result<Vec<&'static str>> {
    SCHEMA_SQL
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            line.trim()
                .strip_suffix(';')
                .filter(|statement| !statement.trim().is_empty())
                .ok_or_else(|| {
                    HawDBError::Parse(
                        "content-store schema statements must be one non-empty semicolon-terminated line"
                            .to_string(),
                    )
                })
        })
        .collect()
}

pub fn nowledge_content_store_sql_corpus() -> Result<ContentStoreSqlCorpus> {
    let corpus = serde_json::from_str::<ContentStoreSqlCorpus>(CORPUS_JSON).map_err(|error| {
        HawDBError::Parse(format!(
            "failed to parse embedded content-store SQL corpus: {error}"
        ))
    })?;
    corpus.validate()?;
    validate_content_store_schema()?;
    Ok(corpus)
}

pub fn nowledge_content_store_sql_corpus_json() -> Result<serde_json::Value> {
    nowledge_content_store_sql_corpus()?.json()
}

fn validate_content_store_schema() -> Result<()> {
    let statements = nowledge_content_store_schema_statements()?;
    let mut tables = BTreeSet::new();
    let mut database = hawdb::Database::new();
    let mut transaction = database.begin_transaction();
    for statement in statements {
        let lowered = hawdb::sql::parse_postgres_sql(statement)?;
        if let hawdb::sql::SqlStatement::CreateTable(create) = &lowered {
            tables.insert(create.table.name.clone());
        } else if !matches!(lowered, hawdb::sql::SqlStatement::CreateIndex(_)) {
            return Err(HawDBError::Semantic(
                "content-store schema may contain only CREATE TABLE and CREATE INDEX statements"
                    .to_string(),
            ));
        }
        transaction.query_sql(statement)?;
    }
    transaction.commit()?;
    let required_tables = REQUIRED_TABLES
        .iter()
        .map(|table| (*table).to_string())
        .collect::<BTreeSet<_>>();
    if tables != required_tables {
        return Err(HawDBError::Semantic(
            "content-store schema table set does not match the v1 scope".to_string(),
        ));
    }
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn validate_statement(statement: &ContentStoreSqlStatementSpec) -> Result<()> {
    if statement.name.is_empty() || statement.sql.trim().is_empty() {
        return Err(HawDBError::Semantic(
            "content-store SQL corpus statement name and SQL must be non-empty".to_string(),
        ));
    }
    if statement.sql.trim_end().ends_with(';') {
        return Err(HawDBError::Semantic(format!(
            "content-store SQL statement {} must not contain a trailing semicolon",
            statement.name
        )));
    }
    validate_parameter_positions(statement)?;
    match statement.kind {
        ContentStoreSqlStatementKind::Read => {
            if statement.max_rows == 0 || statement.max_payload_bytes == 0 {
                return Err(HawDBError::Semantic(format!(
                    "content-store read statement {} must declare non-zero row and payload budgets",
                    statement.name
                )));
            }
            if statement.result_columns.is_empty() {
                return Err(HawDBError::Semantic(format!(
                    "content-store read statement {} must declare its result schema",
                    statement.name
                )));
            }
        }
        ContentStoreSqlStatementKind::Mutation => {
            if statement.transaction_group.is_none() {
                return Err(HawDBError::Semantic(format!(
                    "content-store write statement {} must declare a transaction group",
                    statement.name
                )));
            }
            if statement.max_rows != 0 || statement.max_payload_bytes != 0 {
                return Err(HawDBError::Semantic(format!(
                    "content-store write statement {} must not declare read-result budgets",
                    statement.name
                )));
            }
        }
    }
    Ok(())
}

fn validate_parameter_positions(statement: &ContentStoreSqlStatementSpec) -> Result<()> {
    let positions = postgres_parameter_positions(&statement.sql)?;
    let expected = (1..=statement.parameters.len()).collect::<BTreeSet<_>>();
    if positions != expected {
        return Err(HawDBError::Semantic(format!(
            "content-store SQL statement {} parameter positions do not match its parameter schema",
            statement.name
        )));
    }
    Ok(())
}

fn postgres_parameter_positions(sql: &str) -> Result<BTreeSet<usize>> {
    let bytes = sql.as_bytes();
    let mut positions = BTreeSet::new();
    let mut offset = 0usize;
    let mut single_quoted = false;
    while offset < bytes.len() {
        match bytes[offset] {
            b'\'' => {
                if single_quoted && bytes.get(offset + 1) == Some(&b'\'') {
                    offset += 2;
                    continue;
                }
                single_quoted = !single_quoted;
                offset += 1;
            }
            b'$' if !single_quoted => {
                let start = offset + 1;
                let mut end = start;
                while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                    end += 1;
                }
                if end == start {
                    offset += 1;
                    continue;
                }
                let raw = std::str::from_utf8(&bytes[start..end]).map_err(|_| {
                    HawDBError::Parse("invalid PostgreSQL parameter position".to_string())
                })?;
                let position = raw.parse::<usize>().map_err(|_| {
                    HawDBError::Parse("invalid PostgreSQL parameter position".to_string())
                })?;
                if position == 0 {
                    return Err(HawDBError::Semantic(
                        "PostgreSQL parameters are one-based".to_string(),
                    ));
                }
                positions.insert(position);
                offset = end;
            }
            _ => offset += 1,
        }
    }
    if single_quoted {
        return Err(HawDBError::Parse(
            "content-store SQL statement contains an unterminated string literal".to_string(),
        ));
    }
    Ok(positions)
}

fn contains_sql_identifier(sql: &str, identifier: &str) -> bool {
    sql.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| token.eq_ignore_ascii_case(identifier))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_content_store_sql_corpus_is_valid_and_identity_bound() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let identity = corpus.identity();

        assert_eq!(corpus.tables.len(), REQUIRED_TABLES.len());
        assert_eq!(
            identity.protocol,
            NOWLEDGE_CONTENT_STORE_SQL_CORPUS_PROTOCOL
        );
        assert_eq!(
            identity.revision,
            NOWLEDGE_CONTENT_STORE_SQL_CORPUS_REVISION
        );
        assert_eq!(identity.statement_count, corpus.statements.len());
        assert_eq!(identity.sha256.len(), 64);
        assert_eq!(
            identity.schema_protocol,
            NOWLEDGE_CONTENT_STORE_SCHEMA_PROTOCOL
        );
        assert_eq!(
            identity.schema_revision,
            NOWLEDGE_CONTENT_STORE_SCHEMA_REVISION
        );
        assert_eq!(identity.schema_sha256.len(), 64);
        assert_eq!(identity.schema_statement_count, 13);
        assert_eq!(identity.retained_on_sqlite_statement_count, 0);
        assert!(identity.required_statement_count > identity.rewritten_statement_count);
        assert_eq!(identity.caller_count, 25);
        assert!(identity.covered_caller_count > 0);
        assert_eq!(identity.partial_caller_count, 0);
        assert_eq!(identity.covered_caller_count, identity.caller_count);
        assert_eq!(identity.retained_on_sqlite_caller_count, 0);
        assert_eq!(
            identity.caller_count,
            identity.covered_caller_count
                + identity.partial_caller_count
                + identity.retained_on_sqlite_caller_count
        );
    }

    #[test]
    fn embedded_content_store_sql_corpus_lowers_through_postgres_frontend() {
        let corpus = nowledge_content_store_sql_corpus().expect("valid embedded SQL corpus");
        for statement in &corpus.statements {
            let lowered = hawdb::sql::parse_postgres_sql(&statement.sql)
                .unwrap_or_else(|error| panic!("{} failed to lower: {error}", statement.name));
            let kind_matches = matches!(
                (&statement.kind, lowered),
                (
                    ContentStoreSqlStatementKind::Read,
                    hawdb::sql::SqlStatement::Select(_)
                ) | (
                    ContentStoreSqlStatementKind::Mutation,
                    hawdb::sql::SqlStatement::Insert(_)
                        | hawdb::sql::SqlStatement::Update(_)
                        | hawdb::sql::SqlStatement::Delete(_)
                )
            );
            assert!(
                kind_matches,
                "{} lowered to the wrong statement kind",
                statement.name
            );
        }
    }

    #[test]
    fn embedded_content_store_schema_is_executable_ddl() {
        validate_content_store_schema().expect("valid content-store schema");
        let identity = nowledge_content_store_schema_identity();

        assert_eq!(identity.protocol, NOWLEDGE_CONTENT_STORE_SCHEMA_PROTOCOL);
        assert_eq!(identity.revision, NOWLEDGE_CONTENT_STORE_SCHEMA_REVISION);
        assert_eq!(identity.sha256.len(), 64);
        assert_eq!(identity.statement_count, 13);
    }

    #[test]
    fn content_store_reads_execute_through_the_public_sql_path() {
        let corpus = nowledge_content_store_sql_corpus().expect("valid content-store corpus");
        let mut database = materialized_content_store();

        for statement in corpus
            .statements
            .iter()
            .filter(|statement| statement.kind == ContentStoreSqlStatementKind::Read)
        {
            let parameters = statement
                .parameters
                .iter()
                .enumerate()
                .map(|(position, data_type)| sample_parameter(data_type, position + 1))
                .collect::<Vec<_>>();
            database
                .query_sql_with_params(&statement.sql, &parameters)
                .unwrap_or_else(|error| panic!("{} did not execute: {error}", statement.name));
        }
    }

    #[test]
    fn source_chunk_replacement_is_bound_to_mixed_transaction_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "upsert_source_chunks")
            .expect("source chunk replacement caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller.note.contains("whole-document"));
        assert!(caller.note.contains("mixed transaction"));
    }

    #[test]
    fn thread_message_upsert_is_bound_to_mixed_transaction_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "upsert_thread_messages")
            .expect("thread message upsert caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller.note.contains("identity separation"));
        assert!(caller.note.contains("checkpoint/reopen identity"));
    }

    #[test]
    fn thread_message_reconcile_is_bound_to_occurrence_preserving_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "reconcile_thread_messages_preserving")
            .expect("thread message reconcile caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller
            .statements
            .iter()
            .any(|name| name == "thread_messages_page"));
        assert!(caller.note.contains("exact preserve-mapping validation"));
        assert!(caller.note.contains("legacy anchor reordering"));
        assert!(caller.note.contains("checkpoint/reopen identity"));
    }

    #[test]
    fn thread_tail_delete_is_bound_to_mixed_transaction_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "delete_thread_tail")
            .expect("thread tail delete caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller
            .statements
            .iter()
            .any(|name| name == "thread_tail_delete_candidates"));
        assert!(caller.note.contains("negative-start clamping"));
        assert!(caller.note.contains("one mixed durable transaction"));
        assert!(caller.note.contains("live tombstones"));
        assert!(caller.note.contains("checkpoint/reopen identity"));
    }

    #[test]
    fn thread_delete_is_bound_to_mixed_transaction_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "delete_thread_messages")
            .expect("whole-thread delete caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller
            .statements
            .iter()
            .any(|name| name == "thread_owned_document_ids"));
        assert!(caller.note.contains("bounded document discovery"));
        assert!(caller.note.contains("empty owned document"));
        assert!(caller.note.contains("epoch-preserving preflight"));
        assert!(caller.note.contains("one mixed durable transaction"));
        assert!(caller.note.contains("live tombstones"));
        assert!(caller.note.contains("checkpoint/reopen identity"));
    }

    #[test]
    fn source_ownership_move_is_bound_to_mixed_transaction_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "patch_source_chunks_space")
            .expect("source ownership caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller.note.contains("ownership move atomically"));
        assert!(caller.note.contains("checkpoint/reopen"));
    }

    #[test]
    fn thread_ownership_move_is_bound_to_guarded_batch_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "patch_thread_space_ownership")
            .expect("thread ownership caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller.note.contains("mixed-source guarded moves"));
        assert!(caller.note.contains("stale-preview preservation"));
    }

    #[test]
    fn space_merge_ownership_is_bound_to_cross_owner_evidence() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let caller = corpus
            .source_inventory
            .iter()
            .find(|caller| caller.symbol == "patch_moved_space_ownership")
            .expect("space merge ownership caller must be inventoried");
        assert_eq!(caller.coverage, ContentStoreSqlCallerCoverage::Covered);
        assert!(caller.note.contains("graph Threads and Sources"));
        assert!(caller.note.contains("stale-selection preservation"));
    }

    #[test]
    fn content_store_mutations_stage_through_the_public_sql_path() {
        let corpus = nowledge_content_store_sql_corpus().expect("valid content-store corpus");
        let mut database = materialized_content_store();
        seed_parent_documents(&mut database);

        for statement in corpus
            .statements
            .iter()
            .filter(|statement| statement.kind == ContentStoreSqlStatementKind::Mutation)
        {
            let parameters = statement
                .parameters
                .iter()
                .enumerate()
                .map(|(position, data_type)| {
                    sample_mutation_parameter(&statement.name, data_type, position + 1)
                })
                .collect::<Vec<_>>();
            let mut transaction = database.begin_transaction();
            transaction
                .query_sql_with_params(&statement.sql, &parameters)
                .unwrap_or_else(|error| panic!("{} did not stage: {error}", statement.name));
        }
    }

    #[test]
    fn parameter_validation_ignores_dollar_text_inside_string_literals() {
        assert_eq!(
            postgres_parameter_positions("SELECT '$7', $1, 'it''s $9'").unwrap(),
            BTreeSet::from([1])
        );
    }

    #[test]
    fn corpus_exposes_bounded_ordered_payload_reads() {
        let corpus = nowledge_content_store_sql_corpus().unwrap();
        let page = corpus.statement("thread_messages_page").unwrap();

        assert_eq!(page.kind, ContentStoreSqlStatementKind::Read);
        assert!(page.max_rows > 0);
        assert!(page.max_payload_bytes > 0);
        assert_eq!(page.ordering, ["order_index ASC", "content_message_id ASC"]);
    }

    fn materialized_content_store() -> hawdb::Database {
        let mut database = hawdb::Database::new();
        let mut transaction = database.begin_transaction();
        for statement in
            nowledge_content_store_schema_statements().expect("valid schema statements")
        {
            transaction
                .query_sql(statement)
                .unwrap_or_else(|error| panic!("schema statement did not stage: {error}"));
        }
        transaction
            .commit()
            .expect("materialized content-store schema");
        database
    }

    fn seed_parent_documents(database: &mut hawdb::Database) {
        for position in 1..=32 {
            database
                .query_sql_with_params(
                    "INSERT INTO content_documents \
                     (content_doc_id, owner_kind, owner_id, space_id, media_type, schema_version, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                    &[
                        hawdb::Value::String(format!("value-{position}")),
                        hawdb::Value::String("seed".to_string()),
                        hawdb::Value::String(format!("seed-{position}")),
                        hawdb::Value::String("default".to_string()),
                        hawdb::Value::String("text/plain".to_string()),
                        hawdb::Value::Int(1),
                        hawdb::Value::String("t0".to_string()),
                        hawdb::Value::String("t0".to_string()),
                    ],
                )
                .expect("seed parent document");
        }
    }

    fn sample_parameter(data_type: &str, position: usize) -> hawdb::Value {
        match data_type {
            "BOOLEAN" => hawdb::Value::Bool(false),
            "BIGINT" => hawdb::Value::Int(position as i64),
            "DOUBLE PRECISION" => hawdb::Value::Float(position as f64),
            "TEXT" => hawdb::Value::String(format!("value-{position}")),
            other => panic!("unsupported fixture parameter type {other}"),
        }
    }

    fn sample_mutation_parameter(
        statement_name: &str,
        data_type: &str,
        position: usize,
    ) -> hawdb::Value {
        if statement_name == "upsert_content_document" && data_type == "TEXT" {
            return hawdb::Value::String(format!("new-value-{position}"));
        }
        sample_parameter(data_type, position)
    }
}
