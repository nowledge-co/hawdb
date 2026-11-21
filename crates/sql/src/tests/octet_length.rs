// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

use super::*;

#[test]
fn parses_octet_length_in_predicate_scalar_positions() {
    for predicate in [
        "octet_length(body) <= $1",
        "octet_length(payload) IS NULL",
        "NOT (octet_length(body) + 1 IN (2, 3))",
    ] {
        parse_postgres_sql(&format!("SELECT id FROM records WHERE {predicate}"))
            .unwrap_or_else(|error| panic!("{predicate}: {error}"));
    }
    assert!(parse_postgres_sql("SELECT id FROM records WHERE SUM(id) = 1").is_err());
}
