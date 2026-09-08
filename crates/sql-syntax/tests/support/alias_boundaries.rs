//! Local regressions and a deterministic campaign for table-alias boundaries.

use skein_sql_syntax::{
    parse_graph_table, parse_postgres_select, parse_postgres_statement, PostgresFromItemSyntax,
    PostgresStatementSyntax, Span, SyntaxError, SyntaxErrorCode, TableAlias,
};

const UNSUPPORTED_BOUNDARIES: [&str; 7] = [
    "NATURAL",
    "UNION",
    "EXCEPT",
    "INTERSECT",
    "LATERAL",
    "TABLESAMPLE",
    "WINDOW",
];
const FROM_BOUNDARY_EXPECTED: &str = "a join, comma, or supported SELECT clause after FROM item";

#[derive(Clone, Copy)]
struct Envelope {
    prefix: &'static str,
    standalone: bool,
}

const ENVELOPES: [Envelope; 4] = [
    Envelope {
        prefix: "SELECT * FROM source",
        standalone: false,
    },
    Envelope {
        prefix: "SELECT * FROM schema_name.\"source_\u{e9}\"",
        standalone: false,
    },
    Envelope {
        prefix: "SELECT * FROM GRAPH_TABLE (knowledge MATCH (node) COLUMNS (node.id))",
        standalone: false,
    },
    Envelope {
        prefix: "GRAPH_TABLE (knowledge MATCH (node) COLUMNS (node.id))",
        standalone: true,
    },
];

fn prefix(envelope: Envelope, gap: &str, explicit_as: bool) -> String {
    let mut prefix = format!("{}{gap}", envelope.prefix);
    if explicit_as {
        prefix.push_str(&format!("AS{gap}"));
    }
    prefix
}

fn assert_boundary_error(
    input: &str,
    keyword: &str,
    start: usize,
    explicit_as: bool,
    standalone: bool,
) {
    let expected = SyntaxError::new(
        SyntaxErrorCode::UnexpectedToken,
        Span::new(start, start + keyword.len()),
    )
    .expected(if explicit_as {
        "a table alias"
    } else if standalone {
        "end of GRAPH_TABLE clause"
    } else {
        FROM_BOUNDARY_EXPECTED
    })
    .found(keyword);
    if standalone {
        let actual = parse_graph_table(input).expect_err("unsupported graph alias boundary");
        assert_eq!(actual, expected, "{input}");
    } else {
        let actual = parse_postgres_select(input).expect_err("unsupported SELECT boundary");
        assert_eq!(actual, expected, "{input}");
        let actual = parse_postgres_statement(input).expect_err("unsupported statement boundary");
        assert_eq!(actual, expected, "{input}");
    }
}

fn assert_alias(alias: &TableAlias, name: &str, start: usize, quoted: bool, with_columns: bool) {
    let name_end = start + name.len();
    assert_eq!(alias.name.span, Span::new(start, name_end));
    assert_eq!(alias.name.quoted, quoted);
    let columns_len = if with_columns { "(renamed)".len() } else { 0 };
    assert_eq!(alias.span, Span::new(start, name_end + columns_len));
    assert_eq!(alias.columns.len(), usize::from(with_columns));
    if with_columns {
        assert_eq!(
            alias.columns[0].span,
            Span::new(name_end + 1, name_end + 1 + "renamed".len())
        );
        assert!(!alias.columns[0].quoted);
    }
}

fn assert_accepted_alias(
    prefix: &str,
    name: &str,
    quoted: bool,
    with_columns: bool,
    standalone: bool,
) {
    let columns = if with_columns { "(renamed)" } else { "" };
    let tail = if standalone {
        ";"
    } else {
        " WHERE 1 = 1 ORDER BY 1 LIMIT 1;"
    };
    let input = format!("{prefix}{name}{columns}{tail}");
    if standalone {
        let table = parse_graph_table(&input).expect("valid standalone graph alias");
        assert_alias(
            table.alias.as_ref().expect("alias retained"),
            name,
            prefix.len(),
            quoted,
            with_columns,
        );
    } else {
        let select = parse_postgres_select(&input).expect("valid SELECT alias");
        let PostgresStatementSyntax::Select(statement) =
            parse_postgres_statement(&input).expect("valid statement alias")
        else {
            panic!("SELECT statement expected");
        };
        assert_eq!(select, *statement, "entrypoint AST parity: {input}");
        assert!(select.selection.is_some());
        assert_eq!(select.order_by.len(), 1);
        assert!(select.limit.is_some());
        let alias = match &select.from[0].relation {
            PostgresFromItemSyntax::Relation(table) => table.alias.as_ref(),
            PostgresFromItemSyntax::GraphTable(table) => table.alias.as_ref(),
        };
        assert_alias(
            alias.expect("alias retained"),
            name,
            prefix.len(),
            quoted,
            with_columns,
        );
    }
}

fn legal_aliases(keyword: &str) -> [(String, bool); 4] {
    [
        (format!("\"{keyword}\""), true),
        (format!("{keyword}_alias"), false),
        (format!("alias_{keyword}"), false),
        (format!("\"{keyword}\"\"alias\""), true),
    ]
}

#[test]
fn natural_join_reports_the_unsupported_keyword_not_missing_on() {
    let prefix = "SELECT * FROM source ";
    let input = format!("{prefix}NATURAL JOIN target");
    assert_boundary_error(&input, "NATURAL", prefix.len(), false, false);
}

#[test]
fn natural_join_with_on_is_not_accepted_as_an_aliased_inner_join() {
    let prefix = "SELECT * FROM source ";
    let input = format!("{prefix}NATURAL JOIN target ON source.id = target.id");
    assert_boundary_error(&input, "NATURAL", prefix.len(), false, false);
}

#[test]
fn unsupported_boundaries_are_not_bare_or_explicit_aliases() {
    for keyword in UNSUPPORTED_BOUNDARIES {
        for envelope in ENVELOPES {
            for explicit_as in [false, true] {
                let prefix = prefix(envelope, " ", explicit_as);
                let input = format!("{prefix}{keyword}");
                assert_boundary_error(
                    &input,
                    keyword,
                    prefix.len(),
                    explicit_as,
                    envelope.standalone,
                );
            }
        }
    }
}

#[test]
fn quoted_and_keyword_prefixed_aliases_keep_names_and_column_lists() {
    for keyword in UNSUPPORTED_BOUNDARIES {
        for envelope in ENVELOPES {
            for explicit_as in [false, true] {
                let prefix = prefix(envelope, " ", explicit_as);
                for (name, quoted) in legal_aliases(keyword) {
                    for with_columns in [false, true] {
                        assert_accepted_alias(
                            &prefix,
                            &name,
                            quoted,
                            with_columns,
                            envelope.standalone,
                        );
                    }
                }
            }
        }
    }
}

#[test]
#[ignore = "deterministic local alias-boundary campaign"]
fn table_alias_boundary_differential_campaign() {
    let mut rejected = 0;
    let mut accepted = 0;
    for keyword in UNSUPPORTED_BOUNDARIES {
        let alternating_case: String = keyword
            .chars()
            .enumerate()
            .map(|(index, ch)| {
                if index % 2 == 0 {
                    ch
                } else {
                    ch.to_ascii_lowercase()
                }
            })
            .collect();
        for word in [
            keyword.to_owned(),
            keyword.to_ascii_lowercase(),
            alternating_case,
        ] {
            for gap in [" ", "\t", "\n-- alias boundary\n", "/* \u{03bb} */"] {
                for envelope in ENVELOPES {
                    for explicit_as in [false, true] {
                        let prefix = prefix(envelope, gap, explicit_as);
                        for tail in [
                            "",
                            ";",
                            "(renamed)",
                            " JOIN target ON source.id = target.id",
                        ] {
                            let input = format!("{prefix}{word}{tail}");
                            assert_boundary_error(
                                &input,
                                &word,
                                prefix.len(),
                                explicit_as,
                                envelope.standalone,
                            );
                            rejected += 1;
                        }
                        for (name, quoted) in legal_aliases(&word) {
                            for with_columns in [false, true] {
                                assert_accepted_alias(
                                    &prefix,
                                    &name,
                                    quoted,
                                    with_columns,
                                    envelope.standalone,
                                );
                                accepted += 1;
                            }
                        }
                    }
                }
            }
        }
    }
    assert_eq!(rejected, 2_688);
    assert_eq!(accepted, 5_376);
    eprintln!("alias boundary campaign: {rejected} exact rejections, {accepted} valid aliases");
}
