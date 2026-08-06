use lrlex::lrlex_mod;
use lrpar::lrpar_mod;
use serde_json::json;
use std::hint::black_box;
use std::time::Instant;

lrlex_mod!("exact_lookup.l");
lrpar_mod!("exact_lookup.y");

const QUERY: &str = "MATCH (m:Memory) WHERE m.id = $id RETURN m.title AS title LIMIT $limit";
const ITERATIONS: usize = 50_000;
const SAMPLES: usize = 11;

fn main() {
    let lexer_definition = exact_lookup_l::lexerdef();
    let expected = skein_cypher::parse(QUERY).expect("hand-written parser must accept the probe");
    let actual =
        parse_yacc_with(&lexer_definition, QUERY).expect("Yacc parser must accept the probe");
    assert_eq!(actual, expected, "Yacc and hand-written ASTs diverged");

    let hand_written_ns = median_sample(|| {
        black_box(skein_cypher::parse(black_box(QUERY)).expect("probe must parse"));
    });
    let yacc_ns = median_sample(|| {
        black_box(parse_yacc_with(&lexer_definition, black_box(QUERY)).expect("probe must parse"));
    });
    println!(
        "cypher_yacc_experiment {}",
        json!({
            "scope": "parameterized_exact_lookup",
            "input_bytes": QUERY.len(),
            "iterations": ITERATIONS,
            "samples": SAMPLES,
            "hand_written_median_ns_per_query": hand_written_ns,
            "yacc_median_ns_per_query": yacc_ns,
            "yacc_to_hand_written_ratio": yacc_ns as f64 / hand_written_ns as f64,
            "migration_gate_passed": yacc_ns.saturating_mul(5) <= hand_written_ns.saturating_mul(4),
            "ast_equal": true,
        })
    );
}

#[cfg(test)]
fn parse_yacc(input: &str) -> Result<skein_cypher::Statement, String> {
    let lexer_definition = exact_lookup_l::lexerdef();
    parse_yacc_with(&lexer_definition, input)
}

fn parse_yacc_with(
    lexer_definition: &lrlex::LRNonStreamingLexerDef<lrlex::defaults::DefaultLexerTypes>,
    input: &str,
) -> Result<skein_cypher::Statement, String> {
    let lexer = lexer_definition.lexer(input);
    let (result, errors) = exact_lookup_y::parse(&lexer);
    if !errors.is_empty() {
        return Err(errors
            .into_iter()
            .map(|error| error.pp(&lexer, &exact_lookup_y::token_epp))
            .collect::<Vec<_>>()
            .join("\n"));
    }
    result
        .ok_or_else(|| "Yacc parser returned no AST".to_string())?
        .map(|statement| *statement)
}

fn median_sample(mut operation: impl FnMut()) -> u128 {
    operation();
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let started = Instant::now();
        for _ in 0..ITERATIONS {
            operation();
        }
        samples.push(started.elapsed().as_nanos() / ITERATIONS as u128);
    }
    samples.sort_unstable();
    samples[SAMPLES / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yacc_probe_matches_production_ast() {
        assert_eq!(
            parse_yacc(QUERY).unwrap(),
            skein_cypher::parse(QUERY).unwrap()
        );
    }

    #[test]
    fn yacc_probe_rejects_repaired_input() {
        let error =
            parse_yacc("MATCH (m:Memory) WHERE m.id = RETURN m.title AS title LIMIT $limit")
                .unwrap_err();
        assert!(!error.is_empty());
    }
}
