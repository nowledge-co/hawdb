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

//! Executable non-SELECT SQL is recognized by token PEG and lowered directly.
use super::{owned::Lowerer, ExpressionPosition};
use crate::ast::*;
use hawdb_core::{HawDBError, Result, Value};
use hawdb_sql_syntax::{
    self as syntax,
    peg::{Grammar, Limits, NodeId, ParseTree},
    Token, TokenKind,
};
use std::sync::OnceLock;
mod grammar;

fn semantic(message: impl Into<String>) -> HawDBError {
    HawDBError::Semantic(message.into())
}
pub(super) fn parse(input: &str) -> Result<SqlStatement> {
    static GRAMMAR: OnceLock<Grammar<()>> = OnceLock::new();
    let grammar =
        GRAMMAR.get_or_init(|| grammar::build().expect("valid executable SQL PEG grammar"));
    // Keep the established DDL/DML byte admission (including large single
    // literals). Token, work, capture/memo and language-depth admission apply.
    let tokens = syntax::tokenize_with_config(
        input,
        syntax::LexerConfig {
            max_input_bytes: usize::MAX,
            ..syntax::LexerConfig::default()
        },
    )
    .map_err(parse_error)?;
    let tree = grammar
        .parse(
            input,
            &tokens,
            "HawInput",
            Limits {
                depth: 8192,
                ..Limits::default()
            },
        )
        .map_err(parse_error)?;
    let fold = Fold {
        input,
        tokens: &tokens,
        tree: &tree,
        lowerer: Lowerer::new(input),
    };
    fold.statement(fold.first(fold.first(tree.root)))
}
fn parse_error(error: syntax::SyntaxError) -> HawDBError {
    if error.code == syntax::SyntaxErrorCode::InvalidParameter {
        semantic("PostgreSQL parameters must use one-based $n syntax")
    } else {
        HawDBError::Parse(error.to_string())
    }
}
struct Fold<'a> {
    input: &'a str,
    tokens: &'a [Token],
    tree: &'a ParseTree<()>,
    lowerer: Lowerer<'a>,
}
impl Fold<'_> {
    fn first(&self, node: NodeId) -> NodeId {
        self.tree.children(node)[0]
    }
    fn child(&self, node: NodeId, name: &str) -> NodeId {
        self.tree.required(node, name)
    }
    fn has(&self, node: NodeId, name: &str) -> bool {
        self.tree.child(node, name).is_some()
    }
    fn children(&self, node: NodeId, name: &str) -> Vec<NodeId> {
        self.tree
            .children(node)
            .iter()
            .copied()
            .filter(|id| self.tree.nodes[*id].name == Some(name))
            .collect()
    }
    fn text(&self, node: NodeId) -> &str {
        let span = self.tree.span(node, self.tokens);
        &self.input[span.start..span.end]
    }
    fn ident(&self, node: NodeId) -> String {
        let token = self.tokens[self.tree.nodes[node].start];
        self.lowerer.ident(&syntax::Identifier {
            span: token.span,
            quoted: token.kind == TokenKind::QuotedIdentifier,
        })
    }
    fn name(&self, node: NodeId) -> syntax::QualifiedName {
        syntax::QualifiedName {
            span: self.tree.span(node, self.tokens),
            parts: self
                .tree
                .children(node)
                .iter()
                .map(|id| {
                    let token = self.tokens[self.tree.nodes[*id].start];
                    syntax::Identifier {
                        span: token.span,
                        quoted: token.kind == TokenKind::QuotedIdentifier,
                    }
                })
                .collect(),
        }
    }
    fn table(&self, node: NodeId) -> Result<SqlTableName> {
        self.lowerer.name(&self.name(node))
    }
    fn unqualified(&self, node: NodeId, message: &str) -> Result<String> {
        let name = self.name(node);
        let [part] = name.parts.as_slice() else {
            return Err(semantic(message));
        };
        Ok(self.lowerer.ident(part))
    }
    fn names(&self, node: NodeId, message: &str) -> Result<Vec<String>> {
        self.tree
            .children(node)
            .iter()
            .map(|id| self.unqualified(*id, message))
            .collect()
    }
    fn expr(&self, node: NodeId) -> syntax::ExpressionSyntax {
        syntax::peg::sql_expression(self.tree, self.tokens, node)
    }
    fn alias(&self, node: NodeId) -> Result<Option<String>> {
        self.tree
            .child(node, "Alias")
            .or_else(|| self.tree.child(node, "RelationalAsAlias"))
            .map(|alias| {
                if self.has(alias, "Identifiers") {
                    return Err(semantic(
                        "PostgreSQL table column aliases are not supported",
                    ));
                }
                Ok(self.ident(self.child(alias, "Identifier")))
            })
            .transpose()
    }
    fn selection(&self, node: NodeId) -> Result<Option<crate::Expr>> {
        self.tree
            .child(node, "Where")
            .map(|id| {
                self.lowerer.lower(
                    &self.expr(self.child(id, "Or")),
                    ExpressionPosition::Predicate,
                )
            })
            .transpose()
    }
    fn statement(&self, node: NodeId) -> Result<SqlStatement> {
        match self.tree.nodes[node].name {
            Some("HawInsert") => self.insert(node),
            Some("HawUpdate") => self.update(node),
            Some("HawDelete") => self.delete(node),
            Some("HawCreateTable") => self.create_table(node),
            Some("HawCreateIndex") => self.create_index(node),
            Some("HawAlter") => self.alter(node),
            Some("HawCreateView") => self.create_view(node),
            Some("HawDrop") => self.drop_view(node),
            _ => Err(semantic("unsupported PostgreSQL statement kind")),
        }
    }
    fn assignments(&self, node: NodeId) -> Result<Vec<SqlAssignment>> {
        self.tree
            .children(node)
            .iter()
            .map(|id| {
                if self.has(*id, "HawTupleTarget") {
                    return Err(semantic("tuple assignments are not supported"));
                }
                let column = self.unqualified(
                    self.child(*id, "Name"),
                    "assignment targets must be unqualified columns",
                )?;
                let value = self.assignment_value(&self.expr(self.child(*id, "Or")))?;
                Ok(SqlAssignment { column, value })
            })
            .collect()
    }
    fn assignment_operand(&self, expr: &syntax::ExpressionSyntax) -> Result<SqlArithmeticOperand> {
        if matches!(expr.kind, syntax::ExpressionKindSyntax::Column(_)) {
            self.lowerer.column(expr).map(SqlArithmeticOperand::Column)
        } else {
            self.lowerer.literal(expr).map(SqlArithmeticOperand::Value)
        }
    }
    fn assignment_value(&self, expr: &syntax::ExpressionSyntax) -> Result<SqlAssignmentValue> {
        use syntax::{BinaryOperatorSyntax as B, ExpressionKindSyntax as E};
        match &expr.kind {
            E::Column(_) => self.lowerer.column(expr).map(SqlAssignmentValue::Column),
            E::Binary {
                left,
                operator,
                right,
            } => {
                let invalid = || {
                    semantic("UPDATE arithmetic supports column + value, value + column, and column - value")
                };
                if !matches!(operator, B::Add | B::Subtract) {
                    return Err(invalid());
                }
                let left = self.assignment_operand(left)?;
                let right = self.assignment_operand(right)?;
                if !matches!(
                    (operator, &left, &right),
                    (
                        B::Add,
                        SqlArithmeticOperand::Column(_),
                        SqlArithmeticOperand::Value(_)
                    ) | (
                        B::Add,
                        SqlArithmeticOperand::Value(_),
                        SqlArithmeticOperand::Column(_)
                    ) | (
                        B::Subtract,
                        SqlArithmeticOperand::Column(_),
                        SqlArithmeticOperand::Value(_)
                    )
                ) {
                    return Err(invalid());
                }
                Ok(SqlAssignmentValue::Arithmetic {
                    left,
                    operator: if *operator == B::Add {
                        SqlArithmeticOperator::Add
                    } else {
                        SqlArithmeticOperator::Subtract
                    },
                    right,
                })
            }
            _ => self.lowerer.literal(expr).map(SqlAssignmentValue::Value),
        }
    }
    fn insert(&self, node: NodeId) -> Result<SqlStatement> {
        let mut unsupported = Vec::new();
        for (name, diagnostic) in [
            ("HawTableKeyword", "TABLE keyword"),
            ("HawInsertAlias", "table alias"),
            ("HawMutationTail", "additional clause"),
        ] {
            if self.has(node, name) {
                unsupported.push(diagnostic);
            }
        }
        if let Some(modifier) = self.tree.child(node, "HawInsertModifier") {
            let token = self.tokens[self.tree.nodes[modifier].start];
            unsupported.push(if token.is_keyword(self.input, "IGNORE") {
                "IGNORE"
            } else if token.is_keyword(self.input, "OVERWRITE") {
                "OVERWRITE"
            } else {
                "OR conflict action"
            });
        }
        if !self.has(node, "HawInto") {
            unsupported.push("missing INTO");
        }
        super::clauses::reject("INSERT", &unsupported)?;
        let source = self.first(self.child(node, "HawInsertSource"));
        if self.tree.nodes[source].name == Some("HawDefaultValues") && self.has(node, "HawNames") {
            return Err(HawDBError::Parse(
                "explicit INSERT columns require VALUES or SELECT".into(),
            ));
        }
        if self.tree.nodes[source].name != Some("HawValues") {
            return Err(semantic("INSERT supports VALUES rows only"));
        }
        let columns = self
            .tree
            .child(node, "HawNames")
            .map(|id| self.names(id, "INSERT columns must be unqualified identifiers"))
            .transpose()?
            .unwrap_or_default();
        let rows = self
            .tree
            .children(source)
            .iter()
            .map(|row| {
                if self.has(*row, "HawExplicitRow") {
                    return Err(semantic("INSERT requires the PostgreSQL VALUES form"));
                }
                self.children(*row, "Or")
                    .iter()
                    .map(|id| self.lowerer.literal(&self.expr(*id)))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        if rows.is_empty() || rows.iter().any(|row| row.len() != columns.len()) {
            return Err(semantic(
                "INSERT VALUES rows must match the explicit column list",
            ));
        }
        let on_conflict = self
            .tree
            .child(node, "HawConflict")
            .map(|id| self.conflict(id))
            .transpose()?;
        let returning = self
            .tree
            .child(node, "HawReturning")
            .map(|id| {
                self.tree
                    .children(id)
                    .iter()
                    .map(|item| {
                        if self.has(*item, "HawReturnAlias") {
                            return Err(semantic(
                                "INSERT RETURNING supports column references only",
                            ));
                        }
                        self.lowerer
                            .column(&self.expr(self.child(*item, "Or")))
                            .map_err(|_| {
                                semantic("INSERT RETURNING supports column references only")
                            })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(SqlStatement::Insert(InsertStatement {
            table: self.table(self.child(node, "Name"))?,
            columns,
            rows,
            on_conflict,
            returning,
        }))
    }
    fn conflict(&self, node: NodeId) -> Result<SqlOnConflict> {
        let Some(target) = self.tree.child(node, "HawNames") else {
            return Err(semantic("ON CONFLICT requires an explicit column target"));
        };
        let action = if self.has(node, "HawNothing") {
            SqlConflictAction::DoNothing
        } else {
            let update = self.child(node, "HawConflictUpdate");
            if self.has(update, "Where") {
                return Err(semantic("ON CONFLICT DO UPDATE WHERE is not supported"));
            }
            SqlConflictAction::DoUpdate(self.assignments(self.child(update, "HawAssignments"))?)
        };
        Ok(SqlOnConflict {
            columns: self.names(target, "ON CONFLICT columns must be unqualified")?,
            action,
        })
    }
    fn update(&self, node: NodeId) -> Result<SqlStatement> {
        if [
            "RelationalJoin",
            "HawMutationFrom",
            "HawReturning",
            "Order",
            "Limit",
            "HawMutationTail",
        ]
        .iter()
        .any(|name| self.has(node, name))
        {
            return Err(semantic("unsupported PostgreSQL UPDATE clause"));
        }
        let alias = self
            .tree
            .child(node, "HawUpdateAlias")
            .map(|id| self.alias(id))
            .transpose()?
            .flatten();
        Ok(SqlStatement::Update(UpdateStatement {
            table: self.table(self.child(node, "Name"))?,
            alias,
            assignments: self.assignments(self.child(node, "HawAssignments"))?,
            selection: self.selection(node)?,
        }))
    }
    fn delete(&self, node: NodeId) -> Result<SqlStatement> {
        if [
            "HawDeleteTargets",
            "HawUsing",
            "HawReturning",
            "Order",
            "Limit",
        ]
        .iter()
        .any(|name| self.has(node, name))
        {
            return Err(semantic("unsupported PostgreSQL DELETE clause"));
        }
        let sources = self.tree.children(self.child(node, "RelationalFrom"));
        let [source] = sources else {
            return Err(semantic("DELETE supports exactly one base table"));
        };
        if self.has(*source, "RelationalJoin") {
            return Err(semantic("DELETE joins are not supported"));
        }
        let relation = self.first(self.child(*source, "RelationalFromItem"));
        if self.has(relation, "TableArguments") {
            return Err(semantic(
                "PostgreSQL relational SQL supports base tables only",
            ));
        }
        Ok(SqlStatement::Delete(DeleteStatement {
            table: self.table(self.child(relation, "Name"))?,
            alias: self.alias(relation)?,
            selection: self.selection(node)?,
        }))
    }
    fn table_clause(&self, node: NodeId) -> &'static str {
        match self.tokens[self.tree.nodes[node].start]
            .text(self.input)
            .to_ascii_uppercase()
            .as_str()
        {
            "TEMP" | "TEMPORARY" => "TEMPORARY",
            "OR" => "OR REPLACE",
            "AS" => "AS query",
            "ON" => "ON COMMIT",
            "INHERITS" => "INHERITS",
            "PARTITION" => "PARTITION BY",
            "CLUSTER" => "CLUSTER BY",
            "ORDER" => "ORDER BY",
            "LIKE" => "LIKE",
            "CLONE" => "CLONE",
            "WITHOUT" => "WITHOUT ROWID",
            "COMMENT" => "COMMENT",
            "UNLOGGED" => "UNLOGGED",
            "EXTERNAL" => "EXTERNAL",
            "DYNAMIC" => "DYNAMIC",
            "TRANSIENT" => "TRANSIENT",
            "VOLATILE" => "VOLATILE",
            "ICEBERG" => "ICEBERG",
            "GLOBAL" => "GLOBAL",
            "LOCAL" => "LOCAL",
            _ => unreachable!("recognized CREATE TABLE clause"),
        }
    }
    fn create_table(&self, node: NodeId) -> Result<SqlStatement> {
        let mut unsupported = self
            .children(node, "HawTableModifier")
            .iter()
            .map(|id| self.table_clause(*id))
            .collect::<Vec<_>>();
        unsupported.extend(
            self.children(node, "HawTableTail")
                .iter()
                .map(|id| self.first(*id))
                .filter(|id| self.tree.nodes[*id].name != Some("HawStorage"))
                .map(|id| self.table_clause(id)),
        );
        super::clauses::reject("CREATE TABLE", &unsupported)?;
        let mut storage = None;
        for tail in self.children(node, "HawTableTail") {
            let tail = self.first(tail);
            if self.tree.nodes[tail].name != Some("HawStorage") {
                return Err(semantic(format!(
                    "unsupported PostgreSQL CREATE TABLE clause: {}",
                    self.table_clause(tail)
                )));
            }
            if storage.replace(self.storage(tail)?).is_some() {
                return Err(semantic("duplicate CREATE TABLE storage options"));
            }
        }
        let (mut columns, mut constraints) = (Vec::new(), Vec::new());
        if let Some(items) = self.tree.child(node, "HawTableColumns") {
            for item in self.tree.children(items) {
                let item = self.first(*item);
                if self.tree.nodes[item].name == Some("HawColumn") {
                    columns.push(self.column(item)?);
                } else {
                    constraints.push(self.constraint(item)?);
                }
            }
        }
        Ok(SqlStatement::CreateTable(CreateTableStatement {
            table: self.table(self.child(node, "Name"))?,
            if_not_exists: self.has(node, "HawIfNotExists"),
            columns,
            constraints,
            storage: storage.unwrap_or(SqlTableStorage::RowPage),
        }))
    }
    fn data_type(&self, node: NodeId) -> Result<SqlDataType> {
        if self.has(node, "HawArrayType") {
            return Err(semantic("unsupported PostgreSQL array data type"));
        }
        let kind = self.first(node);
        let raw = self.tokens[self.tree.nodes[kind].start]
            .text(self.input)
            .to_ascii_uppercase();
        match self.tree.nodes[kind].name {
            Some("HawIntegerType") => Ok(SqlDataType::BigInt),
            Some("HawDoubleType") => Ok(SqlDataType::DoublePrecision),
            Some("HawSimpleType") => Ok(match raw.as_str() {
                "BOOLEAN" | "BOOL" => SqlDataType::Boolean,
                "FLOAT8" => SqlDataType::DoublePrecision,
                "TEXT" => SqlDataType::Text,
                "BYTEA" => SqlDataType::Bytea,
                "UUID" => SqlDataType::Uuid,
                _ => unreachable!("simple data type"),
            }),
            _ => {
                let name = self.name(self.child(kind, "Name"));
                if name.parts.len() != 1 || self.lowerer.ident(&name.parts[0]) != "vector" {
                    return Err(semantic(format!(
                        "unsupported PostgreSQL data type {}",
                        self.text(node)
                    )));
                }
                let invalid = || semantic("VECTOR requires one dimension between 1 and 4294967295");
                let args = self
                    .tree
                    .child(kind, "HawTypeArguments")
                    .ok_or_else(invalid)?;
                let begin = self.tree.nodes[args].start;
                let end = self.tree.nodes[args].end;
                if end - begin != 3 || self.tokens[begin + 1].kind != TokenKind::Number {
                    return Err(invalid());
                }
                let raw = self.tokens[begin + 1].text(self.input);
                if !raw.bytes().all(|b| b.is_ascii_digit()) {
                    return Err(invalid());
                }
                raw.parse::<u32>()
                    .ok()
                    .and_then(hawdb_core::VectorDimension::new)
                    .map(SqlDataType::Vector)
                    .ok_or_else(invalid)
            }
        }
    }
    fn column(&self, node: NodeId) -> Result<SqlColumnDefinition> {
        let mut column = SqlColumnDefinition {
            name: self.ident(self.child(node, "Identifier")),
            data_type: self.data_type(self.child(node, "HawType"))?,
            nullable: true,
            default: None,
            primary_key: false,
            unique: false,
            references: None,
        };
        for option in self.children(node, "HawColumnOption") {
            if self.has(option, "HawNamedConstraint") {
                return Err(semantic("named column constraints are not supported"));
            }
            let option = self.first(option);
            match self.tree.nodes[option].name {
                Some("HawNotNull") => column.nullable = false,
                Some("HawNull") => column.nullable = true,
                Some("HawPrimaryColumn") => {
                    column.primary_key = true;
                    column.nullable = false;
                }
                Some("HawUnique") => column.unique = true,
                Some("HawReference") => column.references = Some(self.reference(option)?),
                Some("HawDefault") => {
                    let expr = self.expr(self.child(option, "Or"));
                    column.default = Some(
                        match self.lowerer.lower(&expr, ExpressionPosition::Scalar)?.kind {
                            ExprKind::Value(value) => SqlColumnDefault::Literal(value),
                            ExprKind::Function {
                                name,
                                arguments,
                                distinct: false,
                                filter: None,
                            } if name == "uuidv7" && arguments.is_empty() => {
                                SqlColumnDefault::UuidV7
                            }
                            ExprKind::Function { name, .. } => {
                                return Err(semantic(format!(
                                    "unsupported PostgreSQL column default function {name}"
                                )))
                            }
                            ExprKind::Column(_) => {
                                return Err(semantic("column defaults cannot reference a column"))
                            }
                            _ => {
                                return Err(semantic(
                                    "unsupported PostgreSQL column default expression",
                                ))
                            }
                        },
                    );
                }
                _ => return Err(semantic("unsupported PostgreSQL column option")),
            }
        }
        Ok(column)
    }
    fn reference(&self, node: NodeId) -> Result<SqlForeignKeyReference> {
        if self.has(node, "HawReferenceUnsupported") {
            return Err(semantic("unsupported FOREIGN KEY clause"));
        }
        let mut on_delete = None;
        let mut on_update = None;
        for action in self.children(node, "HawReferenceAction") {
            let action = self.first(action);
            let slot = if self.tree.nodes[action].name == Some("HawOnDelete") {
                &mut on_delete
            } else {
                &mut on_update
            };
            let tokens = &self.tokens[self.tree.nodes[self.child(action, "HawAction")].start
                ..self.tree.nodes[self.child(action, "HawAction")].end];
            let value = if tokens[0].is_keyword(self.input, "NO") {
                SqlReferentialAction::NoAction
            } else if tokens[0].is_keyword(self.input, "RESTRICT") {
                SqlReferentialAction::Restrict
            } else if tokens[0].is_keyword(self.input, "CASCADE") {
                SqlReferentialAction::Cascade
            } else if tokens[1].is_keyword(self.input, "NULL") {
                SqlReferentialAction::SetNull
            } else {
                return Err(semantic("ON DELETE/UPDATE SET DEFAULT is not supported"));
            };
            if slot.replace(value).is_some() {
                return Err(HawDBError::Parse("duplicate foreign-key action".into()));
            }
        }
        Ok(SqlForeignKeyReference {
            table: self.table(self.child(node, "Name"))?,
            columns: self
                .tree
                .child(node, "HawNames")
                .map(|id| self.names(id, "reference columns must be unqualified"))
                .transpose()?
                .unwrap_or_default(),
            on_delete: on_delete.unwrap_or(SqlReferentialAction::NoAction),
            on_update: on_update.unwrap_or(SqlReferentialAction::NoAction),
        })
    }
    fn constraint(&self, node: NodeId) -> Result<SqlTableConstraint> {
        if self.has(node, "HawNamedConstraint") {
            return Err(semantic("named table constraints are not supported"));
        }
        let node = self.first(node);
        match self.tree.nodes[node].name {
            Some("HawPrimaryConstraint") | Some("HawUniqueConstraint") => {
                if self.has(node, "HawNullsDistinct") {
                    return Err(semantic("unsupported UNIQUE clause"));
                }
                let columns = self.index_columns(self.child(node, "HawIndexColumns"))?;
                if columns.iter().any(|c| {
                    c.direction != SqlOrderDirection::Asc || c.nulls != SqlNullOrder::DialectDefault
                }) {
                    return Err(semantic(
                        "constraint columns do not support ordering options",
                    ));
                }
                let columns = columns.into_iter().map(|c| c.column.name).collect();
                Ok(
                    if self.tree.nodes[node].name == Some("HawPrimaryConstraint") {
                        SqlTableConstraint::PrimaryKey(columns)
                    } else {
                        SqlTableConstraint::Unique(columns)
                    },
                )
            }
            Some("HawForeignConstraint") => Ok(SqlTableConstraint::ForeignKey {
                columns: self.names(
                    self.child(node, "HawNames"),
                    "foreign-key columns must be unqualified",
                )?,
                reference: self.reference(self.child(node, "HawReference"))?,
            }),
            _ => Err(semantic(
                "CHECK and inline index constraints are not supported",
            )),
        }
    }
    fn storage(&self, node: NodeId) -> Result<SqlTableStorage> {
        if self.tree.children(node).is_empty() {
            return Ok(SqlTableStorage::RowPage);
        }
        let (mut mode, mut partition, mut order, mut generated) = (None, None, None, None);
        for option in self.tree.children(node) {
            let name = self.ident(self.child(*option, "Identifier"));
            let expr = self.expr(self.child(*option, "Or"));
            let syntax::ExpressionKindSyntax::Literal(syntax::LiteralSyntax::String(span)) =
                &expr.kind
            else {
                return Err(semantic(format!(
                    "CREATE TABLE {name} must be a string literal"
                )));
            };
            if !self.input[span.start..span.end].starts_with('\'') {
                return Err(semantic(format!(
                    "CREATE TABLE {name} must be a string literal"
                )));
            }
            let SqlValue::Literal(Value::String(value)) = self.lowerer.literal(&expr)? else {
                unreachable!("string literal");
            };
            let slot = match name.as_str() {
                "storage_mode" => &mut mode,
                "partition_key" => &mut partition,
                "order_key" => &mut order,
                "generated_order" => &mut generated,
                _ => {
                    return Err(semantic(format!(
                        "unsupported PostgreSQL CREATE TABLE storage option {name}"
                    )))
                }
            };
            if slot.replace(value).is_some() {
                return Err(semantic(format!(
                    "PostgreSQL CREATE TABLE storage option {name} is specified more than once"
                )));
            }
        }
        let generated = generated
            .map(|v| {
                if v.eq_ignore_ascii_case("commit_sequence") {
                    Ok(SqlGeneratedOrder::CommitSequence)
                } else {
                    Err(semantic(format!(
                        "unsupported strict_append generated_order {v}"
                    )))
                }
            })
            .transpose()?;
        match mode.as_deref().map(str::to_ascii_lowercase).as_deref() {
            Some("strict_append")=>{
                let key=|value:Option<String>,name:&str|->Result<Vec<String>> {let value=value.ok_or_else(||semantic(format!("strict_append storage requires {name}")))?;if value.is_empty() {return Err(semantic(format!("CREATE TABLE {name} must not contain an empty column name")));}Ok(vec![value])};
                Ok(SqlTableStorage::StrictAppend {partition_key:key(partition,"partition_key")?,order_key:key(order,"order_key")?,generated_order:generated.unwrap_or_default()})
            },
            Some(mode)=>Err(semantic(format!("unsupported PostgreSQL CREATE TABLE storage_mode {mode}"))),
            None if partition.is_none()&&order.is_none()&&generated.is_none()=>Ok(SqlTableStorage::RowPage),
            None=>Err(semantic("partition_key, order_key, and generated_order require storage_mode = 'strict_append'")),
        }
    }
    fn index_columns(&self, node: NodeId) -> Result<Vec<SqlIndexColumn>> {
        self.tree
            .children(node)
            .iter()
            .map(|id| {
                if self.has(*id, "HawOperatorClass") {
                    return Err(semantic("index operator classes are not supported"));
                }
                let nulls = self
                    .tree
                    .child(*id, "NullOrder")
                    .map(|n| {
                        if self.has(n, "First") {
                            SqlNullOrder::First
                        } else {
                            SqlNullOrder::Last
                        }
                    })
                    .unwrap_or(SqlNullOrder::DialectDefault);
                Ok(SqlIndexColumn {
                    column: self.lowerer.column(&self.expr(self.child(*id, "Or")))?,
                    direction: if self.has(*id, "Descending") {
                        SqlOrderDirection::Desc
                    } else {
                        SqlOrderDirection::Asc
                    },
                    nulls,
                })
            })
            .collect()
    }
    fn create_index(&self, node: NodeId) -> Result<SqlStatement> {
        if ["HawConcurrently", "HawIndexUsing", "HawIndexTail"]
            .iter()
            .any(|name| self.has(node, name))
        {
            return Err(semantic("unsupported PostgreSQL CREATE INDEX clause"));
        }
        let name = self
            .tree
            .child(node, "HawIndexName")
            .ok_or_else(|| semantic("CREATE INDEX requires an explicit name"))?;
        Ok(SqlStatement::CreateIndex(CreateIndexStatement {
            name: self.unqualified(
                self.child(name, "Name"),
                "CREATE INDEX names must be unqualified",
            )?,
            table: self.table(self.child(node, "Name"))?,
            columns: self.index_columns(self.child(node, "HawIndexColumns"))?,
            unique: self.has(node, "HawUnique"),
            if_not_exists: self.has(node, "HawIfNotExists"),
        }))
    }
    fn alter(&self, node: NodeId) -> Result<SqlStatement> {
        if self.has(node, "HawIfExists") || self.has(node, "HawOnly") {
            return Err(semantic("unsupported PostgreSQL ALTER TABLE clause"));
        }
        let operations = self.children(node, "HawAlterOperation");
        let [operation] = operations.as_slice() else {
            return Err(semantic("ALTER TABLE supports one ADD COLUMN operation"));
        };
        let operation = self.first(*operation);
        if self.tree.nodes[operation].name != Some("HawAddColumn") {
            return Err(semantic("ALTER TABLE supports one ADD COLUMN operation"));
        }
        if self.has(operation, "HawColumnPosition") {
            return Err(semantic("ALTER TABLE column positioning is not supported"));
        }
        Ok(SqlStatement::AlterTableAddColumn(
            AlterTableAddColumnStatement {
                table: self.table(self.child(node, "Name"))?,
                if_not_exists: self.has(operation, "HawIfNotExists"),
                column: self.column(self.child(operation, "HawColumn"))?,
            },
        ))
    }
    fn create_view(&self, node: NodeId) -> Result<SqlStatement> {
        if !self.has(node, "HawMaterialized") {
            return Err(semantic("only MATERIALIZED VIEW is supported"));
        }
        if ["HawOrReplace", "HawNames", "HawStorage", "HawViewTail"]
            .iter()
            .any(|name| self.has(node, name))
        {
            return Err(semantic(
                "unsupported PostgreSQL CREATE MATERIALIZED VIEW clause",
            ));
        }
        let select = syntax::peg::sql_select(
            self.input,
            self.tree,
            self.tokens,
            self.child(node, "RelationalSelect"),
        );
        let SqlStatement::Select(query) = self.lowerer.select(&select)? else {
            unreachable!("SELECT AST");
        };
        Ok(SqlStatement::CreateMaterializedView(
            CreateMaterializedViewStatement {
                name: self.table(self.child(node, "Name"))?,
                if_not_exists: self.has(node, "HawIfNotExists"),
                query: Box::new(query),
            },
        ))
    }
    fn drop_view(&self, node: NodeId) -> Result<SqlStatement> {
        if !self.has(node, "HawMaterialized")
            || !self
                .text(self.child(node, "HawDropKind"))
                .eq_ignore_ascii_case("VIEW")
        {
            return Err(semantic("unsupported PostgreSQL statement kind"));
        }
        let names = self.children(node, "Name");
        if names.len() != 1
            || self
                .tree
                .child(node, "HawDropOption")
                .is_some_and(|id| !self.text(id).eq_ignore_ascii_case("RESTRICT"))
        {
            return Err(semantic(
                "unsupported PostgreSQL DROP MATERIALIZED VIEW clause",
            ));
        }
        Ok(SqlStatement::DropMaterializedView(
            DropMaterializedViewStatement {
                name: self.table(names[0])?,
                if_exists: self.has(node, "HawIfExists"),
            },
        ))
    }
}
