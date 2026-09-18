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

//! Capacity envelopes for the pinned immutable Jieba analyzer and skip regex.
//!
//! These model Rust allocation requests, including replacement overlap. Process
//! dictionary residency, allocator overhead and OS thread metadata are separate.
//!
//! A flat workspace allowance cannot bound Jieba's input-sized DAG and HMM
//! buffers. Keep their capacity-dependent envelopes separate from the fixed
//! regex envelope. Dependency upgrades must rerun the allocator qualification
//! in `tests/analyzer_workspace_allocation.rs` before changing the exact pins.

const WORD: usize = 8;
pub(super) const REGEX_PATTERN: &str = r"([a-zA-Z0-9]+(?:.\d+)?%?)";
const REGEX_PATTERN_BYTES: usize = REGEX_PATTERN.len();
const HIR_NODES: usize = 11;
const CLASS_RANGES: usize = 76;
const UTF8_EDGES: usize = 273;
const COMPILER_STATES: usize = 4 * HIR_NODES + UTF8_EDGES + 1 + 8;
const MAX_DICTIONARY_PREFIXES: usize = 7;

#[derive(Clone, Copy)]
struct Automaton {
    nfa_states: usize,
    dfa_states: usize,
    epsilon_edges: usize,
}

const FORWARD: Automaton = Automaton {
    nfa_states: 46,
    dfa_states: 54,
    epsilon_edges: 14,
};
const REVERSE: Automaton = Automaton {
    nfa_states: 188,
    dfa_states: 356,
    epsilon_edges: 91,
};

// Rust's pinned Vec growth needs at most twice the requested length (or its
// initial/minimum capacity). Count an old and replacement allocation together.
fn growing(elements: usize, element_bytes: usize) -> Option<usize> {
    elements.max(8).checked_mul(4)?.checked_mul(element_bytes)
}

fn cache(automaton: Automaton) -> Option<usize> {
    // Add the unknown/dead/quit sentinels even when enumeration already reached
    // one. The alphabet stride includes end-of-input and is 128 in both NFAs.
    let states = automaton.dfa_states.checked_add(3)?;
    let encoded = automaton.nfa_states.checked_mul(5)?.checked_add(17)?;
    let terms = [
        growing(states.checked_mul(128)?, 4)?,
        growing(states, 2 * WORD)?,
        // HashMap load factor, power-of-two buckets and replacement overlap;
        // each bucket holds Arc<[u8]>, a u32 ID, alignment and a control byte.
        states.checked_mul(8 * (3 * WORD + 1))?.checked_add(16)?,
        // One shared Arc payload per distinct state, including its refcounts.
        states.checked_mul(encoded.checked_add(2 * WORD)?)?,
        automaton.nfa_states.checked_mul(4 * 4)?,
        growing(automaton.epsilon_edges.checked_add(1)?, 4)?,
        growing(encoded, 1)?,
        // Three anchoring modes and all six start-context classes.
        growing(3 * 6, 4)?,
    ];
    sum(terms)
}

pub(super) fn regex_retained() -> Option<usize> {
    // Immutable forward/reverse NFA storage and captured-pattern metadata.
    // The compiler can emit no more than COMPILER_STATES intermediate states;
    // UTF8_EDGES plus structural nodes bound the sparse/union payloads.
    let immutable = sum([
        growing(COMPILER_STATES, 4 * WORD)?,
        growing(UTF8_EDGES + 4 * HIR_NODES, 8)?,
        (HIR_NODES + 2).checked_mul(256)?,
    ])?;
    sum([cache(FORWARD)?, cache(REVERSE)?, 2 * immutable])
}

pub(super) fn regex_construction() -> Option<usize> {
    let syntax = sum([
        // AST/class nodes and parser visit state for this fixed pattern.
        (4 * REGEX_PATTERN_BYTES).checked_mul(256 * 2)?,
        (4 * HIR_NODES).checked_mul(256)?,
        (4 * CLASS_RANGES).checked_mul(8)?,
        (4 * REGEX_PATTERN_BYTES).checked_add(16 * 256)?,
    ])?;
    let compiler = sum([
        // Utf8BoundedEntry (Vec, version, state ID) and Utf8SuffixEntry.
        10_000 * 4 * WORD,
        1_000 * 2 * WORD,
        // Mutable and immutable state arrays can coexist during conversion.
        2 * growing(COMPILER_STATES, 4 * WORD)?,
        // Cache, unfinished UTF-8 node, mutable state and immutable state.
        4 * growing(UTF8_EDGES + 4 * COMPILER_STATES, 8)?,
        // Remapping IDs and pending empty-state rewrites.
        growing(COMPILER_STATES, 4)?,
        growing(COMPILER_STATES, 2 * WORD)?,
        (HIR_NODES + 2).checked_mul(256)?,
    ])?;
    // The full DFA is skipped before construction: 46 forward NFA states
    // exceed the meta engine's default limit of 30, regardless of Cargo features.
    // The optional one-pass attempt has at most one state per NFA state plus
    // DEAD. Include its table, ID worklist/map/remap, DFS stack and sparse set.
    let onepass = sum([
        growing((FORWARD.nfa_states + 1) * 128, 8)?,
        3 * growing(FORWARD.nfa_states + 1, 4)?,
        growing(FORWARD.nfa_states, 2 * WORD)?,
        FORWARD.nfa_states * 2 * 4,
        growing(2, 4)?,
    ])?;
    sum([syntax, 2 * compiler, onepass])
}

pub(super) fn hmm_retained(characters: usize) -> Option<usize> {
    let states = characters.checked_mul(4)?;
    sum([
        states.max(8).checked_mul(2 * 8)?,
        // State/Option<State> have four variants in pinned Jieba; a machine
        // word is a conservative layout bound on supported 32/64-bit targets.
        states.max(8).checked_mul(2 * WORD)?,
        characters.max(8).checked_mul(2 * WORD)?,
        characters.max(8).checked_mul(2 * 2 * WORD)?,
    ])
}

pub(super) fn invocation(bytes: usize, characters: usize) -> Option<usize> {
    if characters > bytes {
        return None;
    }
    let initial = bytes / 2;
    let character_capacity = initial.max(characters);
    let dag_initial = initial.checked_mul(4)?.clamp(32, 4_000_000);
    let dag_entries = characters.checked_mul(MAX_DICTIONARY_PREFIXES + 1)?;
    sum([
        growing(character_capacity, 2 * WORD)?, // str_words
        growing(character_capacity, 6 * WORD)?, // cut output
        growing(initial.max(bytes.checked_add(1)?), 2 * WORD)?, // route
        growing(dag_initial.max(dag_entries), 8)?,
        growing(bytes.checked_add(1)?, WORD)?, // byte-indexed starts
        growing(32.max(characters), WORD)?,    // touched starts
        growing(characters.checked_mul(2)?, 6 * WORD)?, // search output
        growing(characters, WORD)?,            // search word character offsets
        // The retained lease is grown before calling Jieba. This extra new
        // workspace covers coexistence while old HMM buffers are replaced.
        hmm_retained(characters)?,
    ])
}

fn sum(values: impl IntoIterator<Item = usize>) -> Option<usize> {
    values.into_iter().try_fold(0usize, usize::checked_add)
}

#[cfg(test)]
#[path = "bounds/tests.rs"]
mod tests;
