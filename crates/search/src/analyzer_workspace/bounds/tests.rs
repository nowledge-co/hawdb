use super::*;
use regex_automata::hybrid::dfa::{Config, DFA};
use regex_automata::nfa::thompson::{self, State, WhichCaptures, NFA};
use regex_automata::util::start;
use regex_automata::{Anchored, MatchKind, PatternID};
use regex_syntax::hir::{Class, Hir, HirKind};
use regex_syntax::utf8::Utf8Sequences;
use std::collections::HashSet;

const PATTERN: &str = REGEX_PATTERN;

#[test]
fn syntax_and_compiler_shape_match_the_pinned_construction_envelope() {
    let hir = regex_syntax::Parser::new().parse(PATTERN).unwrap();
    let mut observed = [0usize; 4];
    fn visit(hir: &Hir, observed: &mut [usize; 4]) {
        observed[0] += 1;
        match hir.kind() {
            HirKind::Class(Class::Unicode(class)) => {
                observed[1] += class.ranges().len();
                for range in class.ranges() {
                    for sequence in Utf8Sequences::new(range.start(), range.end()) {
                        observed[2] += sequence.as_slice().len();
                    }
                }
            }
            HirKind::Class(Class::Bytes(_)) => panic!("unexpected byte class"),
            HirKind::Literal(literal) => observed[3] += literal.0.len(),
            HirKind::Capture(capture) => visit(&capture.sub, observed),
            HirKind::Repetition(repetition) => {
                assert!(repetition.min <= 1);
                assert!(repetition.max.is_none_or(|max| max <= 1));
                visit(&repetition.sub, observed);
            }
            HirKind::Concat(children) | HirKind::Alternation(children) => {
                for child in children {
                    visit(child, observed);
                }
            }
            _ => {}
        }
    }
    visit(&hir, &mut observed);
    assert_eq!(observed, [HIR_NODES, CLASS_RANGES, UTF8_EDGES, 1]);
    assert_eq!(PATTERN.len(), REGEX_PATTERN_BYTES);
    assert!(std::mem::size_of::<jieba_rs::Token<'_>>() <= 6 * WORD);
    assert!(std::mem::size_of::<State>() <= 4 * WORD);
    assert!(std::mem::size_of::<thompson::Transition>() <= 8);
}

#[test]
fn complete_lazy_state_space_stays_below_cache_clear_and_fallback_thresholds() {
    let meta = regex_automata::meta::Regex::new(PATTERN).unwrap();
    assert!(!meta.is_accelerated());
    assert!(
        FORWARD.nfa_states
            > regex_automata::meta::Config::new()
                .get_dfa_state_limit()
                .unwrap()
    );
    let onepass_nfa = NFA::compiler()
        .configure(thompson::Config::new().shrink(false))
        .build(PATTERN)
        .unwrap();
    assert!(regex_automata::dfa::onepass::DFA::builder()
        .configure(regex_automata::dfa::onepass::Config::new().starts_for_each_pattern(true))
        .build_from_nfa(onepass_nfa)
        .is_err());
    for (reverse, qualified) in [(false, FORWARD), (true, REVERSE)] {
        let nfa = NFA::compiler()
            .configure(
                thompson::Config::new()
                    .shrink(false)
                    .reverse(reverse)
                    .which_captures(if reverse {
                        WhichCaptures::None
                    } else {
                        WhichCaptures::All
                    }),
            )
            .build(PATTERN)
            .unwrap();
        assert_eq!(nfa.states().len(), qualified.nfa_states);
        let epsilon_edges = nfa
            .states()
            .iter()
            .map(|state| match state {
                State::Look { .. } | State::Capture { .. } => 1,
                State::BinaryUnion { .. } => 2,
                State::Union { alternates } => alternates.len(),
                _ => 0,
            })
            .sum::<usize>();
        assert_eq!(epsilon_edges, qualified.epsilon_edges);
        let config = Config::new()
            .match_kind(if reverse {
                MatchKind::All
            } else {
                MatchKind::LeftmostFirst
            })
            .starts_for_each_pattern(true)
            .byte_classes(true)
            .unicode_word_boundary(true)
            .specialize_start_states(false)
            .cache_capacity(2 * (1 << 20))
            .skip_cache_capacity_check(false)
            .minimum_cache_clear_count(Some(3))
            .minimum_bytes_per_state(Some(10));
        let dfa = DFA::builder()
            .configure(config)
            .build_from_nfa(nfa)
            .unwrap();
        assert_eq!(dfa.byte_classes().alphabet_len().next_power_of_two(), 128);
        let mut scratch = dfa.create_cache();
        let mut states = Vec::new();
        let mut seen = HashSet::new();
        for anchored in [
            Anchored::No,
            Anchored::Yes,
            Anchored::Pattern(PatternID::ZERO),
        ] {
            for look in std::iter::once(None).chain((0..=255).map(Some)) {
                let state = dfa
                    .start_state(
                        &mut scratch,
                        &start::Config::new().anchored(anchored).look_behind(look),
                    )
                    .unwrap();
                if seen.insert(state) {
                    states.push(state);
                }
            }
        }
        let mut cursor = 0;
        while cursor < states.len() {
            let state = states[cursor];
            cursor += 1;
            for byte in 0..=255 {
                let next = dfa.next_state(&mut scratch, state, byte).unwrap();
                assert!(!next.is_unknown() && !next.is_quit());
                if seen.insert(next) {
                    states.push(next);
                }
            }
            let next = dfa.next_eoi_state(&mut scratch, state).unwrap();
            assert!(!next.is_unknown() && !next.is_quit());
            if seen.insert(next) {
                states.push(next);
            }
            assert_eq!(scratch.clear_count(), 0);
        }
        assert_eq!(states.len(), qualified.dfa_states);
        assert!(scratch.memory_usage() <= cache(qualified).unwrap());
        assert!(cache(qualified).unwrap() < 2 * (1 << 20));
    }
}

#[test]
fn source_dimensions_and_overflow_fail_closed() {
    assert!(invocation(4, 5).is_none());
    assert!(invocation(usize::MAX, usize::MAX).is_none());
    assert!(hmm_retained(usize::MAX).is_none());
    assert!(invocation(0, 0).is_some());
}
