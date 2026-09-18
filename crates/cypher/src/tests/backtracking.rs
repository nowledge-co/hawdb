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

use super::*;

#[test]
fn failed_thread_repair_probe_preserves_anonymous_variable_numbering() {
    for (prefix, next_id) in [
        ("MATCH (e:Entity)", 0),
        ("MATCH (:Source)-[:LINKS]->(e:Entity)", 1),
    ] {
        let anonymous =
            format!("{prefix} OPTIONAL MATCH (:Memory)-[r:MENTIONS]->(e) RETURN COUNT(r)");
        let explicit_prefix = prefix.replace("(:Source)", "(__anon0:Source)");
        let explicit = format!(
            "{explicit_prefix} OPTIONAL MATCH (__anon{next_id}:Memory)-[r:MENTIONS]->(e) RETURN COUNT(r)"
        );
        assert_eq!(
            parse(&anonymous).unwrap(),
            parse(&explicit).unwrap(),
            "failed probe changed the AST for {anonymous}"
        );
    }
}

#[test]
fn successful_thread_repair_probe_keeps_its_parsed_statement() {
    let statement = parse(
        "MATCH (t:Thread) OPTIONAL MATCH (ti:ThreadIdentity) WHERE ti.thread_node_id = t.id \
         WITH t, COUNT(ti) AS identity_refs \
         OPTIONAL MATCH (t)-[:CONTAINS]->(msg:Message) \
         WITH t, identity_refs, COUNT(msg) AS legacy_messages \
         OPTIONAL MATCH (t)-[:COMPACTS_TO]->(m:Memory) \
         RETURN t.id, t.thread_id, \
         CASE WHEN t.space_id IS NULL OR t.space_id = '' THEN 'default' ELSE t.space_id END, \
         COALESCE(t.message_count, 0), identity_refs, legacy_messages, COUNT(m) \
         ORDER BY t.id ASC",
    )
    .unwrap();
    let Statement::MatchThreadRepairStats(query) = statement else {
        panic!("successful probe fell through: {statement:?}");
    };
    assert_eq!(query.identity_variable, "ti");
    assert_eq!(query.message_label, "Message");
    assert_eq!(query.memory_label, "Memory");
}

fn variant(index: &mut usize, count: usize) -> usize {
    let choice = *index % count;
    *index /= count;
    choice
}

#[test]
#[ignore = "local-only deterministic parser backtracking campaign"]
fn parser_backtracking_differential_campaign() {
    const SHAPES: usize = 4 * 3 * 2 * 3 * 2 * 4 * 3;
    let mut cases = 0;
    for seed in [160_u64, 7, 0x5eed] {
        let mut state = seed;
        for index in 0..SHAPES {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let mut choices = index;
            let prefix_kind = variant(&mut choices, 4);
            let edge =
                ["-[r:MENTIONS]->", "<-[r:MENTIONS]-", "-[r:MENTIONS]-"][variant(&mut choices, 3)];
            let anonymous_first = variant(&mut choices, 2) == 0;
            let label = ["", ":Memory", ":Source"][variant(&mut choices, 3)];
            let properties = if variant(&mut choices, 2) == 0 {
                String::new()
            } else {
                format!(" {{id: {}, note: 'e\u{301}\u{1f4da}'}}", state % 10_000)
            };
            let whitespace = [" ", "\n", "\t", "\u{2003}"][variant(&mut choices, 4)];
            let suffix = [
                "RETURN COUNT(r) AS total",
                "RETURN e.id, COUNT(r) AS total ORDER BY total DESC LIMIT 3",
                "WITH e, COUNT(r) AS degree RETURN e.id, degree ORDER BY degree DESC LIMIT 3",
            ][variant(&mut choices, 3)];
            assert_eq!(choices, 0);

            let (prefix, explicit_prefix, next_id) = match prefix_kind {
                0 => ("MATCH (e:Entity)", "MATCH (e:Entity)", 0),
                1 => (
                    "MATCH (:Source)-[:LINKS]->(e:Entity)",
                    "MATCH (__anon0:Source)-[:LINKS]->(e:Entity)",
                    1,
                ),
                2 => (
                    "MATCH (e:Entity) WHERE (e)-[:LINKS]->()",
                    "MATCH (e:Entity) WHERE (e)-[:LINKS]->(__anon0)",
                    1,
                ),
                3 => (
                    "MATCH (:Source)-[:LINKS]->(e:Entity) WHERE (e)-[:LINKS]->()",
                    "MATCH (__anon0:Source)-[:LINKS]->(e:Entity) WHERE (e)-[:LINKS]->(__anon1)",
                    2,
                ),
                _ => unreachable!(),
            };
            let node = format!("({label}{properties})");
            let explicit_node = format!("(__anon{next_id}{label}{properties})");
            let (pattern, explicit_pattern) = if anonymous_first {
                (
                    format!("{node}{edge}(e)"),
                    format!("{explicit_node}{edge}(e)"),
                )
            } else {
                (
                    format!("(e){edge}{node}"),
                    format!("(e){edge}{explicit_node}"),
                )
            };
            let anonymous =
                format!("{prefix} OPTIONAL MATCH {pattern} {suffix}").replace(' ', whitespace);
            let explicit = format!("{explicit_prefix} OPTIONAL MATCH {explicit_pattern} {suffix}")
                .replace(' ', whitespace);
            let actual = parse(&anonymous)
                .unwrap_or_else(|error| panic!("seed={seed} index={index} {anonymous:?}: {error}"));
            let expected = parse(&explicit)
                .unwrap_or_else(|error| panic!("seed={seed} index={index} {explicit:?}: {error}"));
            assert_eq!(actual, expected, "seed={seed} index={index} {anonymous:?}");
            assert_eq!(parse(&anonymous).unwrap(), actual);
            let invalid = anonymous.replace("[r:MENTIONS]", "[r:MENTIONS*1..2]");
            let error = parse(&invalid).expect_err("multi-hop OPTIONAL MATCH must stay rejected");
            assert!(
                error
                    .to_string()
                    .contains("OPTIONAL MATCH supports only one-hop relationships"),
                "seed={seed} index={index} unexpected rejection: {error}"
            );
            cases += 1;
        }
    }
    assert_eq!(cases, SHAPES * 3);
    println!("Parser backtracking differential: {cases} full-AST comparisons, {cases} rejections");
}
