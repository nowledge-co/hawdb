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
    peg::{Expression as E, Grammar},
    TokenKind as T,
};
fn s(items: Vec<E>) -> E {
    E::Sequence(items)
}
fn c(items: Vec<E>) -> E {
    E::Choice(items)
}
fn o(item: E) -> E {
    E::Optional(Box::new(item))
}
fn many(item: E) -> E {
    E::Repeat(Box::new(item))
}
fn r(name: &'static str) -> E {
    E::Rule(name)
}
fn k(name: &'static str) -> E {
    E::Keyword(name)
}
fn t(kind: T) -> E {
    E::Token(kind, "a SQL statement token")
}
fn kw(names: &[&'static str]) -> E {
    c(names.iter().copied().map(k).collect())
}
fn list(name: &'static str) -> E {
    s(vec![r(name), many(s(vec![t(T::Comma), r(name)]))])
}
fn group(item: E) -> E {
    s(vec![t(T::LeftParen), item, t(T::RightParen)])
}
fn not_keyword(word: &'static str) -> E {
    E::Not(Box::new(k(word)), "an identifier")
}

pub(super) fn build() -> Result<Grammar, String> {
    let rules = vec![
        (
            "HawInput",
            s(vec![r("HawStatement"), o(t(T::Semicolon)), t(T::End)]),
        ),
        (
            "HawStatement",
            c(vec![
                r("HawInsert"),
                r("HawUpdate"),
                r("HawDelete"),
                r("HawCreateTable"),
                r("HawCreateIndex"),
                r("HawAlter"),
                r("HawCreateView"),
                r("HawDrop"),
                r("HawUnsupportedStatement"),
            ]),
        ),
        ("HawNames", group(list("Name"))),
        ("HawIfNotExists", s(vec![k("IF"), k("NOT"), k("EXISTS")])),
        ("HawIfExists", s(vec![k("IF"), k("EXISTS")])),
        ("HawUnique", k("UNIQUE")),
        (
            "HawInsert",
            s(vec![
                k("INSERT"),
                o(r("HawInsertModifier")),
                o(r("HawInto")),
                o(r("HawTableKeyword")),
                r("Name"),
                o(r("HawInsertAlias")),
                o(r("HawNames")),
                r("HawInsertSource"),
                o(r("HawConflict")),
                o(r("HawReturning")),
                o(r("HawMutationTail")),
            ]),
        ),
        ("HawInto", k("INTO")),
        ("HawTableKeyword", k("TABLE")),
        (
            "HawInsertModifier",
            c(vec![
                kw(&["IGNORE", "OVERWRITE"]),
                s(vec![
                    k("OR"),
                    kw(&["REPLACE", "IGNORE", "ABORT", "FAIL", "ROLLBACK"]),
                ]),
            ]),
        ),
        ("HawInsertAlias", s(vec![k("AS"), r("Identifier")])),
        (
            "HawInsertSource",
            c(vec![
                r("HawValues"),
                r("RelationalSelect"),
                r("HawDefaultValues"),
            ]),
        ),
        ("HawValues", s(vec![k("VALUES"), list("HawRow")])),
        ("HawRow", s(vec![o(r("HawExplicitRow")), group(list("Or"))])),
        ("HawExplicitRow", k("ROW")),
        ("HawDefaultValues", s(vec![k("DEFAULT"), k("VALUES")])),
        (
            "HawConflict",
            s(vec![
                k("ON"),
                k("CONFLICT"),
                o(c(vec![r("HawNames"), r("HawConflictConstraint")])),
                k("DO"),
                c(vec![r("HawNothing"), r("HawConflictUpdate")]),
            ]),
        ),
        (
            "HawConflictConstraint",
            s(vec![k("ON"), k("CONSTRAINT"), r("Identifier")]),
        ),
        ("HawNothing", k("NOTHING")),
        (
            "HawConflictUpdate",
            s(vec![
                k("UPDATE"),
                k("SET"),
                r("HawAssignments"),
                o(r("Where")),
            ]),
        ),
        ("HawAssignments", list("HawAssignment")),
        (
            "HawAssignment",
            s(vec![c(vec![
                s(vec![r("HawTupleTarget"), t(T::Equal), group(list("Or"))]),
                s(vec![r("Name"), t(T::Equal), r("Or")]),
            ])]),
        ),
        ("HawTupleTarget", r("HawNames")),
        (
            "HawReturning",
            s(vec![k("RETURNING"), list("HawReturningItem")]),
        ),
        ("HawReturningItem", s(vec![r("Or"), o(r("HawReturnAlias"))])),
        (
            "HawReturnAlias",
            c(vec![r("AsAlias"), r("ProjectionAlias")]),
        ),
        (
            "HawUpdate",
            s(vec![
                k("UPDATE"),
                r("Name"),
                o(r("HawUpdateAlias")),
                many(r("RelationalJoin")),
                k("SET"),
                r("HawAssignments"),
                o(r("HawMutationFrom")),
                o(r("Where")),
                o(r("HawReturning")),
                o(r("Order")),
                o(r("Limit")),
                o(r("HawMutationTail")),
            ]),
        ),
        ("HawUpdateAlias", s(vec![not_keyword("SET"), r("Alias")])),
        ("HawMutationFrom", s(vec![k("FROM"), r("RelationalFrom")])),
        (
            "HawDelete",
            s(vec![
                k("DELETE"),
                o(r("HawDeleteTargets")),
                k("FROM"),
                r("RelationalFrom"),
                o(r("HawUsing")),
                o(r("Where")),
                o(r("HawReturning")),
                o(r("Order")),
                o(r("Limit")),
            ]),
        ),
        (
            "HawDeleteTargets",
            s(vec![not_keyword("FROM"), list("Name")]),
        ),
        ("HawUsing", s(vec![k("USING"), r("RelationalFrom")])),
        (
            "HawMutationTail",
            s(vec![
                kw(&["OUTPUT", "SETTINGS", "FORMAT", "PARTITION"]),
                many(r("HawRawAtom")),
            ]),
        ),
        (
            "HawCreateTable",
            s(vec![
                k("CREATE"),
                many(r("HawTableModifier")),
                k("TABLE"),
                o(r("HawIfNotExists")),
                r("Name"),
                o(r("HawTableColumns")),
                many(r("HawTableTail")),
            ]),
        ),
        (
            "HawTableModifier",
            c(vec![
                kw(&[
                    "TEMP",
                    "TEMPORARY",
                    "UNLOGGED",
                    "EXTERNAL",
                    "DYNAMIC",
                    "TRANSIENT",
                    "VOLATILE",
                    "ICEBERG",
                    "GLOBAL",
                    "LOCAL",
                ]),
                s(vec![k("OR"), k("REPLACE")]),
            ]),
        ),
        ("HawTableColumns", group(o(list("HawTableItem")))),
        ("HawTableItem", c(vec![r("HawConstraint"), r("HawColumn")])),
        (
            "HawColumn",
            s(vec![
                r("Identifier"),
                r("HawType"),
                many(r("HawColumnOption")),
            ]),
        ),
        (
            "HawType",
            s(vec![
                c(vec![
                    r("HawIntegerType"),
                    r("HawDoubleType"),
                    r("HawSimpleType"),
                    r("HawCustomType"),
                ]),
                many(r("HawArrayType")),
            ]),
        ),
        (
            "HawIntegerType",
            s(vec![kw(&["BIGINT", "INT8"]), o(group(r("Number")))]),
        ),
        ("HawDoubleType", s(vec![k("DOUBLE"), k("PRECISION")])),
        (
            "HawSimpleType",
            kw(&["BOOLEAN", "BOOL", "FLOAT8", "TEXT", "BYTEA", "UUID"]),
        ),
        (
            "HawCustomType",
            s(vec![r("Name"), o(r("HawTypeArguments"))]),
        ),
        ("HawTypeArguments", group(many(r("HawRawAtom")))),
        (
            "HawArrayType",
            s(vec![t(T::LeftBracket), o(r("Number")), t(T::RightBracket)]),
        ),
        (
            "HawColumnOption",
            s(vec![
                o(r("HawNamedConstraint")),
                c(vec![
                    r("HawNotNull"),
                    r("HawNull"),
                    r("HawDefault"),
                    r("HawPrimaryColumn"),
                    r("HawUnique"),
                    r("HawReference"),
                    r("HawColumnUnsupported"),
                ]),
            ]),
        ),
        (
            "HawNamedConstraint",
            s(vec![k("CONSTRAINT"), r("Identifier")]),
        ),
        ("HawNotNull", s(vec![k("NOT"), k("NULL")])),
        ("HawNull", k("NULL")),
        ("HawDefault", s(vec![k("DEFAULT"), r("Or")])),
        ("HawPrimaryColumn", s(vec![k("PRIMARY"), k("KEY")])),
        (
            "HawColumnUnsupported",
            c(vec![
                s(vec![k("CHECK"), group(r("Or"))]),
                s(vec![k("COLLATE"), r("Name")]),
                s(vec![k("GENERATED"), many(r("HawRawAtom"))]),
            ]),
        ),
        (
            "HawReference",
            s(vec![
                k("REFERENCES"),
                r("Name"),
                o(r("HawNames")),
                many(r("HawReferenceAction")),
                o(r("HawReferenceUnsupported")),
            ]),
        ),
        (
            "HawReferenceAction",
            s(vec![k("ON"), c(vec![r("HawOnDelete"), r("HawOnUpdate")])]),
        ),
        ("HawOnDelete", s(vec![k("DELETE"), r("HawAction")])),
        ("HawOnUpdate", s(vec![k("UPDATE"), r("HawAction")])),
        (
            "HawAction",
            c(vec![
                s(vec![k("NO"), k("ACTION")]),
                kw(&["RESTRICT", "CASCADE"]),
                s(vec![k("SET"), kw(&["NULL", "DEFAULT"])]),
            ]),
        ),
        (
            "HawReferenceUnsupported",
            s(vec![
                kw(&["MATCH", "DEFERRABLE", "INITIALLY"]),
                many(r("HawRawAtom")),
            ]),
        ),
        (
            "HawConstraint",
            s(vec![
                o(r("HawNamedConstraint")),
                c(vec![
                    r("HawPrimaryConstraint"),
                    r("HawUniqueConstraint"),
                    r("HawForeignConstraint"),
                    r("HawCheckConstraint"),
                ]),
            ]),
        ),
        (
            "HawPrimaryConstraint",
            s(vec![k("PRIMARY"), k("KEY"), r("HawIndexColumns")]),
        ),
        (
            "HawUniqueConstraint",
            s(vec![
                k("UNIQUE"),
                o(r("HawNullsDistinct")),
                r("HawIndexColumns"),
            ]),
        ),
        (
            "HawNullsDistinct",
            s(vec![k("NULLS"), o(k("NOT")), k("DISTINCT")]),
        ),
        (
            "HawForeignConstraint",
            s(vec![
                k("FOREIGN"),
                k("KEY"),
                r("HawNames"),
                r("HawReference"),
            ]),
        ),
        ("HawCheckConstraint", s(vec![k("CHECK"), group(r("Or"))])),
        (
            "HawStorage",
            s(vec![k("WITH"), group(o(list("HawStorageOption")))]),
        ),
        (
            "HawStorageOption",
            s(vec![r("Identifier"), t(T::Equal), r("Or")]),
        ),
        (
            "HawTableTail",
            c(vec![r("HawStorage"), r("HawTableUnsupportedTail")]),
        ),
        (
            "HawTableUnsupportedTail",
            c(vec![
                s(vec![k("AS"), r("RelationalSelect")]),
                s(vec![
                    k("ON"),
                    k("COMMIT"),
                    c(vec![
                        k("DROP"),
                        s(vec![kw(&["DELETE", "PRESERVE"]), k("ROWS")]),
                    ]),
                ]),
                s(vec![k("INHERITS"), r("HawNames")]),
                s(vec![
                    kw(&["PARTITION", "CLUSTER", "ORDER"]),
                    k("BY"),
                    r("Or"),
                ]),
                s(vec![kw(&["LIKE", "CLONE"]), r("Name")]),
                s(vec![k("WITHOUT"), k("ROWID")]),
                s(vec![k("COMMENT"), t(T::String)]),
            ]),
        ),
        (
            "HawCreateIndex",
            s(vec![
                k("CREATE"),
                o(r("HawUnique")),
                k("INDEX"),
                o(r("HawConcurrently")),
                o(r("HawIfNotExists")),
                o(r("HawIndexName")),
                k("ON"),
                r("Name"),
                o(r("HawIndexUsing")),
                r("HawIndexColumns"),
                many(r("HawIndexTail")),
            ]),
        ),
        ("HawConcurrently", k("CONCURRENTLY")),
        ("HawIndexName", s(vec![not_keyword("ON"), r("Name")])),
        ("HawIndexUsing", s(vec![k("USING"), r("Name")])),
        ("HawIndexColumns", group(list("HawIndexColumn"))),
        (
            "HawIndexColumn",
            s(vec![
                r("Or"),
                o(r("HawOperatorClass")),
                o(c(vec![r("Ascending"), r("Descending")])),
                o(r("NullOrder")),
            ]),
        ),
        (
            "HawOperatorClass",
            s(vec![
                E::Not(
                    Box::new(kw(&["ASC", "DESC", "NULLS", "USING"])),
                    "an operator class",
                ),
                r("Name"),
            ]),
        ),
        (
            "HawIndexTail",
            c(vec![
                s(vec![k("INCLUDE"), r("HawNames")]),
                r("HawNullsDistinct"),
                r("HawStorage"),
                r("Where"),
            ]),
        ),
        (
            "HawAlter",
            s(vec![
                k("ALTER"),
                k("TABLE"),
                o(r("HawIfExists")),
                o(r("HawOnly")),
                r("Name"),
                list("HawAlterOperation"),
            ]),
        ),
        ("HawOnly", k("ONLY")),
        (
            "HawAlterOperation",
            c(vec![r("HawAddColumn"), r("HawAlterUnsupported")]),
        ),
        (
            "HawAddColumn",
            s(vec![
                k("ADD"),
                o(k("COLUMN")),
                o(r("HawIfNotExists")),
                r("HawColumn"),
                o(r("HawColumnPosition")),
            ]),
        ),
        (
            "HawColumnPosition",
            c(vec![k("FIRST"), s(vec![k("AFTER"), r("Identifier")])]),
        ),
        (
            "HawAlterUnsupported",
            s(vec![
                kw(&["DROP", "ALTER", "RENAME", "SET", "RESET"]),
                many(r("HawRawAtom")),
            ]),
        ),
        (
            "HawCreateView",
            s(vec![
                k("CREATE"),
                o(r("HawOrReplace")),
                o(r("HawMaterialized")),
                k("VIEW"),
                o(r("HawIfNotExists")),
                r("Name"),
                o(r("HawNames")),
                o(r("HawStorage")),
                k("AS"),
                r("RelationalSelect"),
                o(r("HawViewTail")),
            ]),
        ),
        ("HawOrReplace", s(vec![k("OR"), k("REPLACE")])),
        ("HawMaterialized", k("MATERIALIZED")),
        (
            "HawViewTail",
            s(vec![
                k("WITH"),
                o(k("NO")),
                kw(&["DATA", "SCHEMA"]),
                o(k("BINDING")),
            ]),
        ),
        (
            "HawDrop",
            s(vec![
                k("DROP"),
                o(r("HawMaterialized")),
                r("HawDropKind"),
                o(r("HawIfExists")),
                list("Name"),
                o(r("HawDropOption")),
            ]),
        ),
        (
            "HawDropKind",
            kw(&["VIEW", "TABLE", "INDEX", "SCHEMA", "SEQUENCE"]),
        ),
        ("HawDropOption", kw(&["CASCADE", "RESTRICT", "PURGE"])),
        (
            "HawUnsupportedStatement",
            c(vec![
                s(vec![
                    kw(&["BEGIN", "COMMIT", "ROLLBACK"]),
                    o(kw(&["TRANSACTION", "WORK"])),
                ]),
                s(vec![k("TRUNCATE"), o(k("TABLE")), list("Name")]),
                s(vec![k("SHOW"), k("TABLES")]),
                s(vec![k("SET"), r("Name"), t(T::Equal), r("Or")]),
                s(vec![k("WITH"), list("HawCte"), r("RelationalSelect")]),
            ]),
        ),
        (
            "HawCte",
            s(vec![
                r("Identifier"),
                o(r("HawNames")),
                k("AS"),
                group(r("RelationalSelect")),
            ]),
        ),
        // Balanced lexical groups are used only for recognized unsupported
        // syntax and custom type modifiers; they never become executable ASTs.
        (
            "HawRawAtom",
            c(vec![
                group(many(r("HawRawAtom"))),
                s(vec![
                    t(T::LeftBracket),
                    many(r("HawRawAtom")),
                    t(T::RightBracket),
                ]),
                r("HawRawToken"),
            ]),
        ),
        (
            "HawRawToken",
            c([
                T::Word,
                T::QuotedIdentifier,
                T::String,
                T::Number,
                T::Comma,
                T::Dot,
                T::Colon,
                T::DoubleColon,
                T::Plus,
                T::Minus,
                T::Star,
                T::Slash,
                T::Percent,
                T::Equal,
                T::NotEqual,
                T::Less,
                T::LessOrEqual,
                T::Greater,
                T::GreaterOrEqual,
                T::Concat,
                T::Pipe,
                T::Ampersand,
                T::Caret,
                T::Question,
            ]
            .into_iter()
            .map(t)
            .chain(std::iter::once(E::Parameter))
            .collect()),
        ),
    ];
    hawdb_sql_syntax::peg::sql_grammar(rules, "HawInput")
}
