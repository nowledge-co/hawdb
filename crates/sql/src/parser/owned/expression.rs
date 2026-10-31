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

//! Lower matched syntax directly into relational expressions. Source locations
//! describe contributing tokens, retaining the established expression contract.
use super::super::ExpressionPosition as P;
use super::{syntax, Lowerer};
use crate::ast::*;
use hawdb_core::{HawDBError, Result, Value};
use syntax::{ExpressionKindSyntax as E, ExpressionSyntax as S, LiteralSyntax as L};

fn semantic(message: impl Into<String>) -> HawDBError {
    HawDBError::Semantic(message.into())
}
fn union(left: SqlSourceSpan, right: SqlSourceSpan) -> SqlSourceSpan {
    if left == SqlSourceSpan::default() {
        return right;
    }
    if right == SqlSourceSpan::default() {
        return left;
    }
    let before =
        |a: SqlSourceLocation, b: SqlSourceLocation| (a.line, a.column) < (b.line, b.column);
    SqlSourceSpan {
        start: if before(left.start, right.start) {
            left.start
        } else {
            right.start
        },
        end: if before(left.end, right.end) {
            right.end
        } else {
            left.end
        },
    }
}
fn expression(kind: ExprKind, span: SqlSourceSpan) -> crate::Expr {
    crate::Expr { kind, span }
}
impl Lowerer<'_> {
    pub(in crate::parser) fn ident(&self, ident: &syntax::Identifier) -> String {
        let raw = &self.input[ident.span.start..ident.span.end];
        if ident.quoted {
            raw[1..raw.len() - 1].replace("\"\"", "\"")
        } else {
            raw.to_ascii_lowercase()
        }
    }
    pub(in crate::parser) fn name(&self, name: &syntax::QualifiedName) -> Result<SqlTableName> {
        let parts: Vec<_> = name.parts.iter().map(|part| self.ident(part)).collect();
        match parts.as_slice() {
            [name] => Ok(SqlTableName {
                schema: None,
                name: name.clone(),
            }),
            [schema, name] => Ok(SqlTableName {
                schema: Some(schema.clone()),
                name: name.clone(),
            }),
            _ => Err(semantic(
                "PostgreSQL SELECT currently supports one- or two-part table names",
            )),
        }
    }
    pub(in crate::parser) fn column(&self, value: &S) -> Result<SqlColumnRef> {
        let E::Column(name) = &value.kind else {
            return Err(semantic("expected a column reference"));
        };
        let parts: Vec<_> = name.parts.iter().map(|part| self.ident(part)).collect();
        match parts.as_slice() {
            [name] => Ok(SqlColumnRef {
                qualifier: None,
                name: name.clone(),
            }),
            [qualifier, name] => Ok(SqlColumnRef {
                qualifier: Some(qualifier.clone()),
                name: name.clone(),
            }),
            _ => Err(semantic(
                "PostgreSQL SELECT currently supports one- or two-part column names",
            )),
        }
    }
    pub(in crate::parser) fn literal(&self, value: &S) -> Result<SqlValue> {
        match &value.kind {
            E::Parameter(position) => Ok(SqlValue::Parameter(*position as usize)),
            E::Literal(L::Null) => Ok(SqlValue::Literal(Value::Null)),
            E::Literal(L::Boolean(v)) => Ok(SqlValue::Literal(Value::Bool(*v))),
            E::Literal(L::Number(span)) => {
                super::super::lower_number(&self.input[span.start..span.end]).map(SqlValue::Literal)
            }
            E::Literal(L::String(span)) => decode_string(&self.input[span.start..span.end])
                .map(|v| SqlValue::Literal(Value::String(v))),
            E::Parenthesized(inner) => self.literal(inner),
            E::Unary {
                operator: syntax::UnaryOperatorSyntax::Minus,
                expression,
            } => match self.literal(expression)? {
                SqlValue::Literal(Value::Int(v)) => v
                    .checked_neg()
                    .map(|v| SqlValue::Literal(Value::Int(v)))
                    .ok_or_else(|| semantic("cannot negate integer literal outside its range")),
                SqlValue::Literal(Value::Float(v)) => Ok(SqlValue::Literal(Value::Float(-v))),
                _ => Err(semantic("cannot negate literal value")),
            },
            _ => Err(semantic("expected a literal value")),
        }
    }
    fn literal_span(&self, value: &S) -> SqlSourceSpan {
        match &value.kind {
            E::Unary { expression, .. } | E::Parenthesized(expression) => {
                self.literal_span(expression)
            }
            _ => self.span(value.span),
        }
    }
    pub(in crate::parser) fn bound(&self, value: &S) -> Result<SqlBound> {
        match self.literal(value)? {
            SqlValue::Literal(Value::Int(v)) if v >= 0 => Ok(SqlBound::Literal(v as u64)),
            SqlValue::Parameter(p) => Ok(SqlBound::Parameter(p)),
            _ => Err(semantic(
                "LIMIT/OFFSET must be non-negative integers or PostgreSQL parameters",
            )),
        }
    }
    pub(in crate::parser) fn lower(&self, value: &S, position: P) -> Result<crate::Expr> {
        use P::{Column, Having, HavingScalar, Predicate, Scalar, Value as Literal};
        let having = matches!(position, Having | HavingScalar);
        let operand = if having { HavingScalar } else { Column };
        match position {
            Column => Ok(expression(
                ExprKind::Column(self.column(value)?),
                self.span(value.span),
            )),
            Literal => Ok(expression(
                ExprKind::Value(self.literal(value)?),
                self.literal_span(value),
            )),
            Scalar | HavingScalar => match &value.kind {
                E::Parenthesized(inner) if having => self.lower(inner, HavingScalar),
                E::Column(_) => self.lower(value, Column),
                E::Parameter(_) | E::Literal(_) | E::Parenthesized(_) | E::Unary { .. } => {
                    self.lower(value, Literal)
                }
                E::Function { .. } | E::Filtered { .. } => self.function(value, position),
                _ => Err(semantic("unsupported PostgreSQL projection expression")),
            },
            Predicate | Having => match &value.kind {
                E::Parenthesized(inner) => self.lower(inner, position),
                E::Unary {
                    operator: syntax::UnaryOperatorSyntax::Not,
                    expression: inner,
                } => {
                    let inner = self.lower(inner, position)?;
                    let span = inner.span;
                    Ok(expression(ExprKind::Not(Box::new(inner)), span))
                }
                E::Binary {
                    left,
                    operator,
                    right,
                } => {
                    use syntax::BinaryOperatorSyntax as B;
                    if matches!(operator, B::And | B::Or) {
                        let left = self.lower(left, position)?;
                        let right = self.lower(right, position)?;
                        let span = union(left.span, right.span);
                        let kind = if *operator == B::And {
                            ExprKind::And(Box::new(left), Box::new(right))
                        } else {
                            ExprKind::Or(Box::new(left), Box::new(right))
                        };
                        return Ok(expression(kind, span));
                    }
                    let op = match operator {
                        B::Equal => SqlComparisonOp::Eq,
                        B::NotEqual => SqlComparisonOp::NotEq,
                        B::Less => SqlComparisonOp::Lt,
                        B::LessOrEqual => SqlComparisonOp::Lte,
                        B::Greater => SqlComparisonOp::Gt,
                        B::GreaterOrEqual => SqlComparisonOp::Gte,
                        _ => return Err(semantic("unsupported PostgreSQL predicate operator")),
                    };
                    let left = self.lower(left, operand)?;
                    let right = self.lower(
                        right,
                        if having {
                            HavingScalar
                        } else if matches!(right.kind, E::Column(_)) {
                            Column
                        } else {
                            Literal
                        },
                    )?;
                    let span = union(left.span, right.span);
                    Ok(expression(
                        ExprKind::Compare {
                            left: Box::new(left),
                            op,
                            right: Box::new(right),
                        },
                        span,
                    ))
                }
                E::InList {
                    expression: left,
                    values,
                    negated,
                } => {
                    let left = self.lower(left, operand)?;
                    let values = values
                        .iter()
                        .map(|v| self.lower(v, if having { HavingScalar } else { Literal }))
                        .collect::<Result<Vec<_>>>()?;
                    let span = values.iter().fold(left.span, |span, v| union(span, v.span));
                    Ok(expression(
                        ExprKind::InList {
                            left: Box::new(left),
                            values,
                            negated: *negated,
                        },
                        span,
                    ))
                }
                E::Like {
                    expression: left,
                    pattern,
                    escape,
                    negated,
                    case_insensitive,
                } => {
                    let left = self.lower(left, operand)?;
                    let pattern =
                        self.lower(pattern, if having { HavingScalar } else { Literal })?;
                    let escape = match escape.as_deref() {
                        None => SqlLikeEscape::Character('\\'),
                        Some(S {
                            kind: E::Literal(L::String(span)),
                            ..
                        }) => {
                            let raw = decode_string(&self.input[span.start..span.end])?;
                            let mut chars = raw.chars();
                            match (chars.next(), chars.next()) {
                                (None, _) => SqlLikeEscape::Disabled,
                                (Some(c), None) => SqlLikeEscape::Character(c),
                                _ => {
                                    return Err(semantic(
                                        "LIKE ESCAPE must contain at most one Unicode scalar",
                                    ))
                                }
                            }
                        }
                        _ => return Err(semantic("LIKE ESCAPE must be a string literal")),
                    };
                    let span = union(left.span, pattern.span);
                    Ok(expression(
                        ExprKind::Like {
                            left: Box::new(left),
                            pattern: Box::new(pattern),
                            case_insensitive: *case_insensitive,
                            negated: *negated,
                            escape,
                        },
                        span,
                    ))
                }
                E::IsNull {
                    expression: inner,
                    negated,
                } => {
                    let inner = self.lower(inner, operand)?;
                    let span = inner.span;
                    Ok(expression(
                        ExprKind::IsNull {
                            expression: Box::new(inner),
                            negated: *negated,
                        },
                        span,
                    ))
                }
                _ if having => self.lower(value, HavingScalar),
                _ => Err(semantic("unsupported PostgreSQL predicate expression")),
            },
        }
    }
    fn function(&self, value: &S, position: P) -> Result<crate::Expr> {
        let (function, filter) = match &value.kind {
            E::Filtered {
                function,
                predicate,
            } => (function.as_ref(), Some(predicate.as_ref())),
            _ => (value, None),
        };
        let E::Function {
            name,
            arguments,
            distinct,
        } = &function.kind
        else {
            return Err(semantic("FILTER requires a function"));
        };
        let [identifier] = name.parts.as_slice() else {
            return Err(semantic(
                "qualified PostgreSQL function names are not supported",
            ));
        };
        let name = self.ident(identifier);
        let mut span = self.span(identifier.span);
        let arguments = arguments
            .iter()
            .map(|arg| match &arg.kind {
                E::Wildcard(None) => Ok(SqlFunctionArgument::Wildcard),
                E::Wildcard(Some(_)) => Err(semantic(
                    "named and qualified-wildcard PostgreSQL function arguments are not supported",
                )),
                _ => {
                    let arg = self.lower(arg, position)?;
                    span = union(span, arg.span);
                    Ok(SqlFunctionArgument::Expression(arg))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let filter = filter
            .map(|f| self.lower(f, P::Predicate).map(Box::new))
            .transpose()?;
        if let Some(filter) = &filter {
            span = union(span, filter.span);
        }
        if hawdb_core::VectorDistance::from_function_name(&name).is_some() {
            if *distinct
                || filter.is_some()
                || arguments.len() != 2
                || arguments
                    .iter()
                    .any(|arg| matches!(arg, SqlFunctionArgument::Wildcard))
            {
                return Err(semantic("vector distance requires exactly two scalar arguments without DISTINCT or FILTER"));
            }
        } else {
            match name.as_str() {
                "count" | "sum" => {}
                "min" | "max" | "coalesce" | "octet_length" | "uuidv7" | "version"
                    if filter.is_none() => {}
                "min" | "max" | "coalesce" | "octet_length" | "uuidv7" | "version" => {
                    return Err(semantic(
                        "FILTER is supported only for COUNT and SUM aggregates",
                    ))
                }
                _ => return Err(semantic(format!("unsupported PostgreSQL function {name}"))),
            }
        }
        Ok(expression(
            ExprKind::Function {
                name,
                arguments,
                distinct: *distinct,
                filter,
            },
            span,
        ))
    }
}

/// Decode a single already matched SQL string token, without a SQL parser or
/// a second tokenizer. PostgreSQL E-byte escapes retain the existing ASCII and
/// NUL admission policy; U& literals use four or six hexadecimal digits.
fn decode_string(raw: &str) -> Result<String> {
    if raw.starts_with('$') {
        return Err(semantic("unsupported PostgreSQL dollar-quoted literal"));
    }
    let (prefix, body) = if raw.starts_with('e') || raw.starts_with('E') {
        (1, &raw[2..raw.len() - 1])
    } else if raw.starts_with("U&") || raw.starts_with("u&") {
        (2, &raw[3..raw.len() - 1])
    } else {
        (0, &raw[1..raw.len() - 1])
    };
    let mut chars = body.chars().peekable();
    let mut out = String::with_capacity(body.len());
    let invalid = || HawDBError::Parse("invalid escaped string literal".into());
    while let Some(c) = chars.next() {
        if c == '\'' && chars.peek() == Some(&'\'') {
            chars.next();
            out.push('\'');
            continue;
        }
        if c != '\\' || prefix == 0 {
            out.push(c);
            continue;
        }
        let next = chars.next().ok_or_else(invalid)?;
        let decoded = if prefix == 2 {
            if next == '\\' {
                '\\'
            } else {
                let (digits, first) = if next == '+' {
                    (6, None)
                } else {
                    (4, Some(next))
                };
                read_hex(&mut chars, digits, first)
                    .and_then(char::from_u32)
                    .ok_or_else(invalid)?
            }
        } else {
            match next {
                'b' => '\u{8}',
                'f' => '\u{c}',
                'n' => '\n',
                'r' => '\r',
                't' => '\t',
                'u' | 'U' => read_hex(&mut chars, if next == 'u' { 4 } else { 8 }, None)
                    .and_then(char::from_u32)
                    .ok_or_else(invalid)?,
                'x' => {
                    let mut value = 0;
                    let mut count = 0;
                    while count < 2 && chars.peek().is_some_and(|c| c.is_ascii_hexdigit()) {
                        value = value * 16 + chars.next().unwrap().to_digit(16).unwrap();
                        count += 1;
                    }
                    if count == 0 {
                        'x'
                    } else {
                        char::from_u32(value)
                            .filter(|_| value < 128)
                            .ok_or_else(invalid)?
                    }
                }
                '0'..='7' => {
                    let mut value = next.to_digit(8).unwrap();
                    let mut count = 1;
                    while count < 3 && chars.peek().is_some_and(|c| matches!(c, '0'..='7')) {
                        value = value * 8 + chars.next().unwrap().to_digit(8).unwrap();
                        count += 1;
                    }
                    let byte = value & 255;
                    char::from_u32(byte)
                        .filter(|_| byte < 128)
                        .ok_or_else(invalid)?
                }
                c => c,
            }
        };
        if prefix == 1 && decoded == '\0' {
            return Err(invalid());
        }
        out.push(decoded);
    }
    Ok(out)
}
fn read_hex(
    chars: &mut impl Iterator<Item = char>,
    digits: usize,
    first: Option<char>,
) -> Option<u32> {
    let mut value = 0;
    let mut consumed = 0;
    if let Some(c) = first {
        value = c.to_digit(16)?;
        consumed = 1;
    }
    while consumed < digits {
        value = value
            .checked_mul(16)?
            .checked_add(chars.next()?.to_digit(16)?)?;
        consumed += 1;
    }
    Some(value)
}

impl Lowerer<'_> {
    pub(in crate::parser) fn vector_call(
        &self,
        name: &syntax::QualifiedName,
        args: &[syntax::TableFunctionArgumentSyntax],
    ) -> Result<SqlVectorIndexSearch> {
        let name = self.name(name)?;
        if name.schema.is_some() || name.name != "vector_index_search" {
            return Err(semantic("unsupported SQL table function"));
        }
        let [index, embedding, options @ ..] = args else {
            return Err(semantic(
                "vector_index_search requires index and embedding arguments",
            ));
        };
        if index.name.is_some() || embedding.name.is_some() {
            return Err(semantic(
                "vector_index_search requires index and embedding arguments",
            ));
        }
        let (mut candidates, mut top_k, mut mode, mut max_work) = (None, None, None, None);
        for arg in options {
            let Some(name) = &arg.name else {
                return Err(semantic("vector index options must be named"));
            };
            let slot = match self.ident(name).as_str() {
                "candidates" => &mut candidates,
                "top_k" => &mut top_k,
                "mode" => &mut mode,
                "max_work" => &mut max_work,
                _ => return Err(semantic("unknown vector index option")),
            };
            if slot.replace(self.literal(&arg.expression)?).is_some() {
                return Err(semantic("duplicate vector index option"));
            }
        }
        use hawdb_core::VectorIndexQueryMode as Mode;
        let mode = match mode {
            None => Mode::AnnWindow,
            Some(SqlValue::Literal(Value::String(mode))) => Mode::parse(&mode)?,
            _ => return Err(semantic("vector index mode must be a string literal")),
        };
        let required = || {
            semantic(if mode == Mode::Exact {
                "exact vector search requires explicit top_k"
            } else {
                "vector index requires explicit candidates and top_k"
            })
        };
        match mode {
            Mode::AnnWindow | Mode::FilteredAnn if candidates.is_none() => return Err(required()),
            Mode::Exact if candidates.is_some() => {
                return Err(semantic("exact vector search does not accept candidates"))
            }
            _ => {}
        }
        if mode.requires_eligible_domain() != max_work.is_some() {
            return Err(semantic(
                "max_work is required for exact and filtered_ann vector search only",
            ));
        }
        Ok(SqlVectorIndexSearch {
            index: self.literal(&index.expression)?,
            embedding: self.literal(&embedding.expression)?,
            mode,
            max_work,
            candidates,
            top_k: top_k.ok_or_else(required)?,
        })
    }
}
