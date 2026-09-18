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

use super::{Parser, MAX_CYPHER_PARSER_DEPTH};

#[test]
fn checkpoint_restores_prior_names_after_a_failed_nested_parse() {
    let mut parser = Parser::new("(:Source) (:Memory) RETURN");
    assert_eq!(parser.parse_match_node_pattern().unwrap().0, "__anon0");
    let checkpoint = parser.checkpoint();
    let error = parser
        .with_recursion(|parser| {
            assert_eq!(parser.parse_match_node_pattern()?.0, "__anon1");
            parser.expect_keyword("WHERE")
        })
        .unwrap_err();
    assert!(error.to_string().contains("expected WHERE"));
    assert_eq!(parser.recursion_depth, 0);
    assert_eq!(parser.anonymous_variable_id, 2);
    parser.restore(checkpoint);
    assert_eq!(parser.checkpoint(), checkpoint);
    assert_eq!(parser.parse_match_node_pattern().unwrap().0, "__anon1");
    parser.expect_keyword("RETURN").unwrap();
    parser.expect_eof().unwrap();
}

#[test]
fn nested_checkpoints_preserve_the_active_recursion_budget() {
    let mut parser = Parser::new("(:Source) (:Memory)");
    parser
        .with_recursion(|parser| {
            let outer = parser.checkpoint();
            assert_eq!(parser.parse_match_node_pattern()?.0, "__anon0");
            let inner = parser.checkpoint();
            assert_eq!(parser.parse_match_node_pattern()?.0, "__anon1");
            parser.restore(inner);
            assert_eq!(parser.parse_match_node_pattern()?.0, "__anon1");
            parser.restore(outer);
            assert_eq!(parser.recursion_depth, 1);
            assert_eq!(parser.parse_match_node_pattern()?.0, "__anon0");
            Ok(())
        })
        .unwrap();
    assert_eq!(parser.recursion_depth, 0);

    parser.recursion_depth = MAX_CYPHER_PARSER_DEPTH;
    let checkpoint = parser.checkpoint();
    assert!(parser.with_recursion(|_| Ok(())).is_err());
    parser.restore(checkpoint);
    assert_eq!(parser.recursion_depth, MAX_CYPHER_PARSER_DEPTH);
    assert!(parser.with_recursion(|_| Ok(())).is_err());
}
