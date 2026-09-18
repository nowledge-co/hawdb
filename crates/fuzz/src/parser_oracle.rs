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

use std::thread;

pub const PARSER_FUZZ_PROTOCOL: &str = "hawdb-parser-fuzz-v1";
const MAX_INPUT_BYTES: usize = 16 * 1024;
const PARSER_WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;

const STATIC_SEEDS: &[(&str, &[u8])] = &[
    ("empty", b""),
    ("cypher_match", b"MATCH (n:Memory) RETURN n.id"),
    (
        "cypher_predicate",
        b"MATCH (m:Memory)-[:MENTIONS]->(e:Entity) WHERE m.id = $id AND e.kind IN ['person', 'place'] RETURN e.name",
    ),
    (
        "cypher_mutation",
        b"CREATE (:Memory {id: 'm1', title: 'hello'})",
    ),
    ("cypher_non_ascii_token", b"\xe6\x97\xa5\xe6\x9c\xac"),
    (
        "cypher_non_ascii_predicate",
        b"MATCH (n) WHERE (\xe6\x97\xa5\xe6\x9c\xac) = 1 RETURN n",
    ),
    (
        "postgres_select",
        b"SELECT source.id, source.score + 1 FROM source WHERE NOT source.deleted AND source.score >= $1 ORDER BY source.id LIMIT 10",
    ),
    (
        "postgres_schema",
        b"CREATE TABLE public.events (id BIGINT PRIMARY KEY, payload TEXT NOT NULL)",
    ),
    (
        "postgres_mutation",
        b"INSERT INTO public.events (id, payload) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
    ),
    (
        "pgq_create",
        b"CREATE PROPERTY GRAPH knowledge VERTEX TABLES (memory KEY (id))",
    ),
    (
        "pgq_select",
        b"SELECT * FROM GRAPH_TABLE (knowledge MATCH (node IS memory) COLUMNS (node.id AS id))",
    ),
    ("unterminated_string", b"SELECT 'unterminated"),
    ("unterminated_comment", b"MATCH (n) /* unterminated"),
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParserFuzzCase {
    pub campaign_seed: u64,
    pub index: usize,
    pub case_seed: u64,
    pub source_kind: &'static str,
    pub seed_name: Option<&'static str>,
    pub mutation_count: usize,
    pub input: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParserFuzzObservation {
    pub used_lossy_utf8: bool,
    pub cypher_accepted: bool,
    pub relational_sql_accepted: bool,
    pub postgres_syntax_accepted: bool,
    pub pgq_accepted: bool,
}

pub fn parser_fuzz_seed_count() -> usize {
    STATIC_SEEDS.len() + 2
}

pub fn generate_parser_fuzz_case(campaign_seed: u64, index: usize) -> ParserFuzzCase {
    let case_seed = mix_seed(campaign_seed, index as u64);
    if index < STATIC_SEEDS.len() {
        let (name, input) = STATIC_SEEDS[index];
        return ParserFuzzCase {
            campaign_seed,
            index,
            case_seed,
            source_kind: "seed_corpus",
            seed_name: Some(name),
            mutation_count: 0,
            input: input.to_vec(),
        };
    }

    if index == STATIC_SEEDS.len() {
        return ParserFuzzCase {
            campaign_seed,
            index,
            case_seed,
            source_kind: "seed_corpus",
            seed_name: Some("cypher_deep_nesting"),
            mutation_count: 0,
            input: format!(
                "MATCH (n) WHERE {}n.id = 1{} RETURN n",
                "(".repeat(256),
                ")".repeat(256)
            )
            .into_bytes(),
        };
    }

    if index == STATIC_SEEDS.len() + 1 {
        return ParserFuzzCase {
            campaign_seed,
            index,
            case_seed,
            source_kind: "seed_corpus",
            seed_name: Some("postgres_deep_nesting"),
            mutation_count: 0,
            input: format!(
                "SELECT * FROM source WHERE {}1{}",
                "(".repeat(256),
                ")".repeat(256)
            )
            .into_bytes(),
        };
    }

    let mut rng = DeterministicRng::new(case_seed);
    if (index - parser_fuzz_seed_count()).is_multiple_of(4) {
        let length = rng.bounded(1025);
        let input = (0..length).map(|_| rng.next_u64() as u8).collect();
        return ParserFuzzCase {
            campaign_seed,
            index,
            case_seed,
            source_kind: "random_bytes",
            seed_name: None,
            mutation_count: 0,
            input,
        };
    }

    let seed_index = rng.bounded(STATIC_SEEDS.len());
    let (seed_name, seed) = STATIC_SEEDS[seed_index];
    let mut input = seed.to_vec();
    let mutation_count = 1 + rng.bounded(16);
    for _ in 0..mutation_count {
        mutate_bytes(&mut input, &mut rng);
    }
    input.truncate(MAX_INPUT_BYTES);
    ParserFuzzCase {
        campaign_seed,
        index,
        case_seed,
        source_kind: "mutated_seed",
        seed_name: Some(seed_name),
        mutation_count,
        input,
    }
}

pub fn run_parser_fuzz_case(case: &ParserFuzzCase) -> Result<ParserFuzzObservation, String> {
    let input = case.input.clone();
    let worker = thread::Builder::new()
        .name(format!("hawdb-parser-fuzz-{}", case.index))
        .stack_size(PARSER_WORKER_STACK_BYTES)
        .spawn(move || {
            let used_lossy_utf8 = std::str::from_utf8(&input).is_err();
            let input = String::from_utf8_lossy(&input);
            ParserFuzzObservation {
                used_lossy_utf8,
                cypher_accepted: hawdb::cypher::parse(&input).is_ok(),
                relational_sql_accepted: hawdb::sql::prepare_postgres_sql(&input).is_ok(),
                postgres_syntax_accepted: hawdb::sql::syntax::parse_postgres_statement(&input)
                    .is_ok(),
                pgq_accepted: hawdb::sql::syntax::parse_pgq_statement(&input).is_ok(),
            }
        })
        .map_err(|error| format!("failed to spawn bounded parser worker: {error}"))?;
    worker
        .join()
        .map_err(|_| "bounded parser worker panicked".to_string())
}

pub fn parser_input_fingerprint(input: &[u8]) -> String {
    let hash = input.iter().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    format!("{hash:016x}")
}

fn mutate_bytes(input: &mut Vec<u8>, rng: &mut DeterministicRng) {
    match rng.bounded(7) {
        0 if !input.is_empty() => {
            let index = rng.bounded(input.len());
            input[index] = rng.next_u64() as u8;
        }
        1 if input.len() < MAX_INPUT_BYTES => {
            let index = rng.bounded(input.len() + 1);
            input.insert(index, rng.next_u64() as u8);
        }
        2 if !input.is_empty() => {
            let start = rng.bounded(input.len());
            let length = 1 + rng.bounded(input.len() - start);
            input.drain(start..start + length);
        }
        3 if !input.is_empty() && input.len() < MAX_INPUT_BYTES => {
            let start = rng.bounded(input.len());
            let length = 1 + rng.bounded((input.len() - start).min(64));
            let duplicate = input[start..start + length].to_vec();
            let destination = rng.bounded(input.len() + 1);
            input.splice(destination..destination, duplicate);
        }
        4 if !input.is_empty() => {
            input.truncate(rng.bounded(input.len() + 1));
        }
        5 if input.len() < MAX_INPUT_BYTES => {
            let length = 1 + rng.bounded(64);
            input.extend((0..length).map(|_| rng.next_u64() as u8));
        }
        _ => {
            let index = rng.bounded(input.len() + 1);
            let punctuation = b"()[]{}',;$/*-+=";
            input.insert(index, punctuation[rng.bounded(punctuation.len())]);
        }
    }
    input.truncate(MAX_INPUT_BYTES);
}

fn mix_seed(seed: u64, index: u64) -> u64 {
    let mut value = seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15);
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng(u64);

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0xa076_1d64_78bd_642f)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0 = self.0.wrapping_mul(0x2545_f491_4f6c_dd1d);
        self.0
    }

    fn bounded(&mut self, upper: usize) -> usize {
        if upper == 0 {
            0
        } else {
            (self.next_u64() as usize) % upper
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seed_corpus_covers_known_parser_regressions() {
        let cases = (0..parser_fuzz_seed_count())
            .map(|index| generate_parser_fuzz_case(7, index))
            .collect::<Vec<_>>();

        assert!(cases.iter().any(|case| {
            case.seed_name == Some("cypher_non_ascii_predicate")
                && std::str::from_utf8(&case.input).is_ok()
        }));
        assert!(cases
            .iter()
            .any(|case| case.seed_name == Some("cypher_deep_nesting")));
        assert!(cases
            .iter()
            .any(|case| case.seed_name == Some("postgres_deep_nesting")));
        for case in &cases {
            run_parser_fuzz_case(case).expect("seeded parser case must not panic");
        }
    }

    #[test]
    fn byte_mutation_is_bounded_and_replayable() {
        for index in parser_fuzz_seed_count()..parser_fuzz_seed_count() + 128 {
            let left = generate_parser_fuzz_case(19, index);
            let right = generate_parser_fuzz_case(19, index);
            assert_eq!(left, right);
            assert!(left.input.len() <= MAX_INPUT_BYTES);
            run_parser_fuzz_case(&left).expect("mutated parser case must not panic");
        }
    }

    #[test]
    fn fingerprints_preserve_byte_identity() {
        assert_eq!(
            parser_input_fingerprint(b"abc"),
            parser_input_fingerprint(b"abc")
        );
        assert_ne!(
            parser_input_fingerprint(b"abc"),
            parser_input_fingerprint(b"abd")
        );
    }
}
