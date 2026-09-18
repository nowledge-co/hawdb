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

use hawdb_sql_syntax::{
    BinaryOperatorSyntax as Binary, ExpressionKindSyntax as Syntax, ExpressionSyntax,
    LiteralSyntax, UnaryOperatorSyntax as Unary,
};

use super::*;

const MAX_EXPRESSION_DEPTH: usize = 128;

// Private, span-free identity for checking a property shared by several labels.
// Keeping casts and operand order avoids claiming algebraic equivalence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BoundExpression {
    pub data_type: PgqDataType,
    kind: Kind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Kind {
    Column(usize),
    Literal(Literal),
    Unary(Unary, Box<BoundExpression>),
    Binary(Box<BoundExpression>, Binary, Box<BoundExpression>),
    IsNull(Box<BoundExpression>, bool),
    InList(Box<BoundExpression>, Vec<BoundExpression>, bool),
    Between(
        Box<BoundExpression>,
        Box<BoundExpression>,
        Box<BoundExpression>,
        bool,
    ),
    Function(String, Vec<BoundExpression>),
    Cast(Box<BoundExpression>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Literal {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(u64),
    String(String),
    Binary(Vec<u8>),
    Uuid(u128),
}

impl BoundExpression {
    pub(super) fn column(index: usize, data_type: PgqDataType) -> Self {
        Self {
            data_type,
            kind: Kind::Column(index),
        }
    }

    pub(super) fn implicit_column_name(
        &self,
        source: &PgqSourceTableSchema,
        mut syntax: &ExpressionSyntax,
    ) -> Option<String> {
        // Binding erases identity casts, but they still require an explicit name.
        while let Syntax::Parenthesized(inner) = &syntax.kind {
            syntax = inner;
        }
        if !matches!(syntax.kind, Syntax::Column(_)) {
            return None;
        }
        match self.kind {
            Kind::Column(index) => source.columns.get(index).map(|column| column.name.clone()),
            _ => None,
        }
    }

    fn literal(literal: Literal, data_type: PgqDataType) -> Self {
        Self {
            data_type,
            kind: Kind::Literal(literal),
        }
    }
}

fn unknown(data_type: PgqDataType) -> bool {
    matches!(data_type, PgqDataType::Unknown | PgqDataType::Null)
}

fn numeric(data_type: PgqDataType) -> bool {
    matches!(data_type, PgqDataType::Int64 | PgqDataType::Float64)
}

impl Binder<'_> {
    pub(super) fn expression(
        &self,
        source: &PgqSourceTableSchema,
        expression: &ExpressionSyntax,
        depth: usize,
    ) -> Result<BoundExpression> {
        let span = expression.span;
        self.text(span)?;
        if depth >= MAX_EXPRESSION_DEPTH {
            return Err(self.error(
                Code::UnsupportedExpression,
                span,
                "creation expression nesting exceeds 128 levels",
            ));
        }
        let bind = |expression| self.expression(source, expression, depth + 1);
        let (kind, data_type) = match &expression.kind {
            Syntax::Column(name) => {
                let parts = self.name(name)?;
                let (column, qualifier) = parts.split_last().ok_or_else(|| {
                    self.error(Code::InvalidSyntaxTree, name.span, "empty column reference")
                })?;
                if !source.name.ends_with(qualifier) {
                    return Err(self.error(
                        Code::UnknownColumn,
                        span,
                        "property qualifier must identify its source relation",
                    ));
                }
                let index = source
                    .columns
                    .iter()
                    .position(|candidate| &candidate.name == column)
                    .ok_or_else(|| {
                        self.error(
                            Code::UnknownColumn,
                            span,
                            format!("unknown source column {column}"),
                        )
                    })?;
                return Ok(BoundExpression::column(
                    index,
                    scalar_type(source.columns[index].data_type),
                ));
            }
            Syntax::Literal(literal) => return self.literal(literal, span),
            Syntax::TypedString { data_type, value } => {
                let target = self.type_name(&[self.identifier(data_type)?], data_type.span)?;
                return self.typed_literal(&self.string(*value)?, target, span);
            }
            Syntax::Parenthesized(inner) => return bind(inner),
            Syntax::Unary {
                operator,
                expression,
            } => {
                if *operator == Unary::Minus
                    && let Syntax::Literal(LiteralSyntax::Number(number)) = &expression.kind
                {
                    self.text(expression.span)?;
                    if self.text(*number)? == "9223372036854775808" {
                        return Ok(BoundExpression::literal(
                            Literal::Int64(i64::MIN),
                            PgqDataType::Int64,
                        ));
                    }
                }
                let mut inner = bind(expression)?;
                if *operator == Unary::Not {
                    self.require(&mut inner, PgqDataType::Boolean, span)?;
                } else if !numeric(inner.data_type) {
                    return Err(self.error(
                        Code::TypeMismatch,
                        span,
                        "numeric unary operator requires a resolved numeric type",
                    ));
                }
                let data_type = inner.data_type;
                (Kind::Unary(*operator, Box::new(inner)), data_type)
            }
            Syntax::Binary {
                left,
                operator,
                right,
            } => {
                let mut left = bind(left)?;
                let mut right = bind(right)?;
                let data_type = match operator {
                    Binary::Or | Binary::And => {
                        self.require(&mut left, PgqDataType::Boolean, span)?;
                        self.require(&mut right, PgqDataType::Boolean, span)?;
                        PgqDataType::Boolean
                    }
                    Binary::Concat => {
                        self.require(&mut left, PgqDataType::String, span)?;
                        self.require(&mut right, PgqDataType::String, span)?;
                        PgqDataType::String
                    }
                    Binary::Equal
                    | Binary::NotEqual
                    | Binary::Less
                    | Binary::LessOrEqual
                    | Binary::Greater
                    | Binary::GreaterOrEqual => {
                        self.common_expressions(&mut [&mut left, &mut right], span)?;
                        PgqDataType::Boolean
                    }
                    Binary::Add
                    | Binary::Subtract
                    | Binary::Multiply
                    | Binary::Divide
                    | Binary::Modulo => {
                        let common = self.common_expressions(&mut [&mut left, &mut right], span)?;
                        if !numeric(common)
                            || (*operator == Binary::Modulo && common != PgqDataType::Int64)
                        {
                            return Err(self.error(
                                Code::TypeMismatch,
                                span,
                                "arithmetic operator has no supported overload",
                            ));
                        }
                        common
                    }
                };
                (
                    Kind::Binary(Box::new(left), *operator, Box::new(right)),
                    data_type,
                )
            }
            Syntax::IsNull {
                expression,
                negated,
            } => {
                let mut inner = bind(expression)?;
                self.resolve_unknown(&mut inner, PgqDataType::String, span)?;
                (
                    Kind::IsNull(Box::new(inner), *negated),
                    PgqDataType::Boolean,
                )
            }
            Syntax::InList {
                expression,
                values,
                negated,
            } => {
                if values.is_empty() {
                    return Err(self.error(Code::InvalidSyntaxTree, span, "empty IN list"));
                }
                let mut inner = bind(expression)?;
                let mut values = values.iter().map(bind).collect::<Result<Vec<_>>>()?;
                let mut all = vec![&mut inner];
                all.extend(values.iter_mut());
                self.common_expressions(&mut all, span)?;
                (
                    Kind::InList(Box::new(inner), values, *negated),
                    PgqDataType::Boolean,
                )
            }
            Syntax::Between {
                expression,
                low,
                high,
                negated,
            } => {
                let mut inner = bind(expression)?;
                let mut low = bind(low)?;
                let mut high = bind(high)?;
                self.common_expressions(&mut [&mut inner, &mut low, &mut high], span)?;
                (
                    Kind::Between(Box::new(inner), Box::new(low), Box::new(high), *negated),
                    PgqDataType::Boolean,
                )
            }
            Syntax::Function {
                name,
                arguments,
                distinct,
            } => {
                let name = self.name(name)?;
                let function = self.builtin_name(&name, span)?;
                if *distinct || !matches!(function, "lower" | "upper" | "abs") {
                    return Err(self.error(
                        Code::UnsupportedExpression,
                        span,
                        "unsupported creation function or DISTINCT call",
                    ));
                }
                if arguments.len() != 1 {
                    return Err(self.error(
                        Code::TypeMismatch,
                        span,
                        "creation scalar function requires one argument",
                    ));
                }
                let mut argument = bind(&arguments[0])?;
                if function == "abs" {
                    self.resolve_unknown(&mut argument, PgqDataType::Float64, span)?;
                    if !numeric(argument.data_type) {
                        return Err(self.error(
                            Code::TypeMismatch,
                            span,
                            "abs requires a numeric argument",
                        ));
                    }
                } else {
                    self.require(&mut argument, PgqDataType::String, span)?;
                }
                let data_type = argument.data_type;
                (
                    Kind::Function(function.to_owned(), vec![argument]),
                    data_type,
                )
            }
            Syntax::Cast {
                expression,
                data_type,
            } => {
                let target = self.type_name(&self.name(data_type)?, data_type.span)?;
                let mut inner = bind(expression)?;
                if unknown(inner.data_type) {
                    self.resolve_unknown(&mut inner, target, span)?;
                    return Ok(inner);
                }
                if !cast_allowed(inner.data_type, target) {
                    return Err(self.error(Code::TypeMismatch, span, "unsupported scalar cast"));
                }
                if inner.data_type == target {
                    return Ok(inner);
                }
                (Kind::Cast(Box::new(inner)), target)
            }
            Syntax::Wildcard(_) | Syntax::Parameter(_) | Syntax::Collate { .. } => {
                return Err(self.error(
                    Code::UnsupportedExpression,
                    span,
                    "wildcards, parameters and COLLATE are not creation properties",
                ));
            }
        };
        Ok(BoundExpression { kind, data_type })
    }

    fn literal(&self, literal: &LiteralSyntax, span: Span) -> Result<BoundExpression> {
        Ok(match literal {
            LiteralSyntax::Null => BoundExpression::literal(Literal::Null, PgqDataType::Null),
            LiteralSyntax::Boolean(value) => {
                BoundExpression::literal(Literal::Boolean(*value), PgqDataType::Boolean)
            }
            LiteralSyntax::String(span) => {
                BoundExpression::literal(Literal::String(self.string(*span)?), PgqDataType::Unknown)
            }
            LiteralSyntax::Number(number) => {
                let value = self.text(*number)?;
                if value.contains(['.', 'e', 'E']) {
                    let value = value
                        .parse::<f64>()
                        .ok()
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| {
                            self.error(
                                Code::InvalidLiteral,
                                span,
                                "floating literal outside the supported range",
                            )
                        })?;
                    BoundExpression::literal(
                        Literal::Float64(float_bits(value)),
                        PgqDataType::Float64,
                    )
                } else {
                    let value = value.parse::<i64>().map_err(|_| {
                        self.error(
                            Code::InvalidLiteral,
                            span,
                            "integer literal outside Int64 range",
                        )
                    })?;
                    BoundExpression::literal(Literal::Int64(value), PgqDataType::Int64)
                }
            }
        })
    }

    fn string(&self, span: Span) -> Result<String> {
        let text = self.text(span)?;
        let decoded = if let Some(inner) = text
            .strip_prefix('\'')
            .and_then(|text| text.strip_suffix('\''))
        {
            (!inner.replace("''", "").contains('\'')).then(|| inner.replace("''", "'"))
        } else if let Some(inner) = text.strip_prefix('$') {
            inner.find('$').and_then(|end| {
                let delimiter = &text[..end + 2];
                text.get(delimiter.len()..)?
                    .strip_suffix(delimiter)
                    .map(str::to_owned)
            })
        } else {
            None
        };
        decoded
            .filter(|value| !value.contains('\0'))
            .ok_or_else(|| self.error(Code::InvalidLiteral, span, "invalid SQL string literal"))
    }

    fn builtin_name<'a>(&self, name: &'a [String], span: Span) -> Result<&'a str> {
        match name {
            [name] => Ok(name),
            [schema, name] if schema == "pg_catalog" => Ok(name),
            _ => Err(self.error(
                Code::UnsupportedExpression,
                span,
                "only unqualified or pg_catalog builtins are supported",
            )),
        }
    }

    fn type_name(&self, name: &[String], span: Span) -> Result<PgqDataType> {
        match self.builtin_name(name, span)? {
            "bool" | "boolean" => Ok(PgqDataType::Boolean),
            "bigint" | "int8" => Ok(PgqDataType::Int64),
            "float8" => Ok(PgqDataType::Float64),
            "text" => Ok(PgqDataType::String),
            "bytea" => Ok(PgqDataType::Binary),
            "uuid" => Ok(PgqDataType::Uuid),
            _ => Err(self.error(
                Code::UnsupportedExpression,
                span,
                "type is outside the six-scalar creation profile",
            )),
        }
    }

    pub(super) fn resolve_unknown(
        &self,
        expression: &mut BoundExpression,
        target: PgqDataType,
        span: Span,
    ) -> Result<()> {
        if !unknown(expression.data_type) {
            return Ok(());
        }
        match &expression.kind {
            Kind::Literal(Literal::Null) => expression.data_type = target,
            Kind::Literal(Literal::String(value)) => {
                *expression = self.typed_literal(value, target, span)?
            }
            _ => {
                return Err(self.error(Code::TypeMismatch, span, "unresolved creation expression"))
            }
        }
        Ok(())
    }

    fn require(
        &self,
        expression: &mut BoundExpression,
        target: PgqDataType,
        span: Span,
    ) -> Result<()> {
        self.resolve_unknown(expression, target, span)?;
        if expression.data_type != target {
            return Err(self.error(Code::TypeMismatch, span, "incompatible expression type"));
        }
        Ok(())
    }

    fn common_expressions(
        &self,
        expressions: &mut [&mut BoundExpression],
        span: Span,
    ) -> Result<PgqDataType> {
        let mut common = PgqDataType::Unknown;
        for expression in expressions.iter() {
            let candidate = expression.data_type;
            if unknown(candidate) {
                continue;
            }
            if unknown(common) {
                common = candidate;
            } else if common != candidate {
                if numeric(common) && numeric(candidate) {
                    common = PgqDataType::Float64;
                } else {
                    return Err(self.error(
                        Code::TypeMismatch,
                        span,
                        "incompatible expression types",
                    ));
                }
            }
        }
        if unknown(common) {
            common = PgqDataType::String;
        }
        for expression in expressions {
            self.resolve_unknown(expression, common, span)?;
            if expression.data_type != common {
                let inner =
                    std::mem::replace(*expression, BoundExpression::literal(Literal::Null, common));
                expression.kind = Kind::Cast(Box::new(inner));
            }
        }
        Ok(common)
    }

    fn typed_literal(
        &self,
        value: &str,
        target: PgqDataType,
        span: Span,
    ) -> Result<BoundExpression> {
        let invalid = || {
            self.error(
                Code::InvalidLiteral,
                span,
                "invalid literal for the resolved scalar type",
            )
        };
        let literal = match target {
            PgqDataType::String => Literal::String(value.to_owned()),
            PgqDataType::Boolean => {
                let value = value.trim().to_ascii_lowercase();
                let boolean = match value.as_str() {
                    "1" => Some(true),
                    "0" => Some(false),
                    _ if !value.is_empty() => {
                        let mut candidates = [
                            ("true", true),
                            ("false", false),
                            ("yes", true),
                            ("no", false),
                            ("on", true),
                            ("off", false),
                        ]
                        .into_iter()
                        .filter(|(name, _)| name.starts_with(&value));
                        candidates
                            .next()
                            .filter(|_| candidates.next().is_none())
                            .map(|(_, value)| value)
                    }
                    _ => None,
                };
                Literal::Boolean(boolean.ok_or_else(invalid)?)
            }
            PgqDataType::Int64 => Literal::Int64(value.trim().parse().map_err(|_| invalid())?),
            PgqDataType::Float64 => {
                let text = value.trim();
                let value = match text.to_ascii_lowercase().as_str() {
                    "infinity" | "+infinity" | "inf" | "+inf" => f64::INFINITY,
                    "-infinity" | "-inf" => f64::NEG_INFINITY,
                    "nan" => f64::NAN,
                    _ => text
                        .parse::<f64>()
                        .ok()
                        .filter(|value| value.is_finite())
                        .ok_or_else(invalid)?,
                };
                Literal::Float64(float_bits(value))
            }
            PgqDataType::Binary => Literal::Binary(decode_bytea(value).ok_or_else(invalid)?),
            PgqDataType::Uuid => Literal::Uuid(decode_uuid(value).ok_or_else(invalid)?),
            _ => {
                return Err(self.error(
                    Code::TypeMismatch,
                    span,
                    "literal requires a known scalar type",
                ))
            }
        };
        Ok(BoundExpression::literal(literal, target))
    }
}

fn float_bits(value: f64) -> u64 {
    if value.is_nan() {
        f64::NAN.to_bits()
    } else if value == 0.0 {
        0.0_f64.to_bits()
    } else {
        value.to_bits()
    }
}

fn cast_allowed(source: PgqDataType, target: PgqDataType) -> bool {
    use PgqDataType::*;
    source == target
        || source == String
        || target == String
        || (numeric(source) && numeric(target))
        || matches!(
            (source, target),
            (Int64, Binary) | (Binary, Int64) | (Binary, Uuid) | (Uuid, Binary)
        )
}

fn decode_bytea(value: &str) -> Option<Vec<u8>> {
    if let Some(hex) = value.strip_prefix("\\x") {
        let mut digits = hex.bytes().peekable();
        let mut output = Vec::new();
        while let Some(first) = digits.next() {
            if first.is_ascii_whitespace() {
                continue;
            }
            let high = (first as char).to_digit(16)?;
            let low = (digits.next()? as char).to_digit(16)?;
            output.push((high * 16 + low) as u8);
        }
        return Some(output);
    }
    let mut bytes = value.bytes();
    let mut output = Vec::new();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            output.push(byte);
            continue;
        }
        let first = bytes.next()?;
        if first == b'\\' {
            output.push(b'\\');
            continue;
        }
        let second = bytes.next()?;
        let third = bytes.next()?;
        if !(b'0'..=b'3').contains(&first)
            || !(b'0'..=b'7').contains(&second)
            || !(b'0'..=b'7').contains(&third)
        {
            return None;
        }
        output.push((first - b'0') * 64 + (second - b'0') * 8 + third - b'0');
    }
    Some(output)
}

fn decode_uuid(value: &str) -> Option<u128> {
    let value = if value.starts_with('{') {
        value.strip_prefix('{')?.strip_suffix('}')?
    } else {
        value
    };
    let mut digits = 0;
    let mut output = 0_u128;
    let mut previous_hyphen = false;
    for byte in value.bytes() {
        if byte == b'-' {
            if digits == 0 || digits % 4 != 0 || previous_hyphen {
                return None;
            }
            previous_hyphen = true;
        } else {
            if digits == 32 {
                return None;
            }
            output = (output << 4) | (byte as char).to_digit(16)? as u128;
            digits += 1;
            previous_hyphen = false;
        }
    }
    (digits == 32 && !previous_hyphen).then_some(output)
}
