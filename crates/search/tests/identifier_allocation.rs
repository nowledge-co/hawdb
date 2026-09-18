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

//! Isolate allocator instrumentation from the library and its other tests.

#[path = "../src/identifier.rs"]
mod identifier;

#[path = "support/allocation.rs"]
mod allocation;
use allocation::measure;

#[test]
fn splitter_removes_source_sized_character_scratch() {
    // Measure allocator-requested bytes, including reallocations, not RSS or
    // live memory. Construct the input outside the measurement window.
    for (name, raw) in [
        ("ascii", "a".repeat(1024 * 1024)),
        ("camel_digits", "HTTPServer42".repeat(8192)),
        ("unicode_expansion", "\u{130}\u{4e2d}\u{e9}".repeat(32768)),
    ] {
        let (expected, old_bytes) = measure(|| identifier::reference::identifier_parts(&raw));
        let (actual, new_bytes) = measure(|| identifier::identifier_parts(&raw));
        assert_eq!(actual, expected, "{name}");
        let char_bytes = raw.chars().count() * std::mem::size_of::<char>();
        assert!(
            new_bytes + char_bytes <= old_bytes,
            "{name}: old={old_bytes}, new={new_bytes}, character scratch={char_bytes}"
        );
        println!(
            "{name}: old={old_bytes}, new={new_bytes}, removed={}",
            old_bytes - new_bytes
        );
    }
}

#[test]
fn borrowed_part_cursors_do_not_allocate_input_sized_scratch() {
    for raw in [
        "a".repeat(1024 * 1024),
        "HTTPServer42".repeat(8192),
        "\u{39f}\u{3a3}_\u{130}Index42".repeat(4096),
    ] {
        let expected = identifier::reference::identifier_parts(&raw);
        let (matches, bytes) = measure(|| {
            let mut parts = identifier::part_slices(&raw);
            for expected in &expected {
                let Some(part) = parts.next() else {
                    return false;
                };
                if !part
                    .chars()
                    .flat_map(char::to_lowercase)
                    .eq(expected.chars())
                {
                    return false;
                }
            }
            parts.next().is_none()
        });
        assert!(matches);
        assert_eq!(
            bytes, 0,
            "borrowed boundaries must not allocate per input byte or part"
        );
    }
}
