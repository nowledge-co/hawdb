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
use crate::build_memory::BuildMemory;
use hawdb_core::{RuntimeMemoryReservation, RuntimeTaskContext};

pub(super) struct ShortReads<'a> {
    pub(super) remaining: &'a [u8],
    pub(super) first: usize,
    pub(super) width: usize,
}

impl Read for ShortReads<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let width = if self.first != 0 {
            std::mem::take(&mut self.first)
        } else {
            self.width
        };
        let count = width.min(output.len()).min(self.remaining.len());
        output[..count].copy_from_slice(&self.remaining[..count]);
        self.remaining = &self.remaining[count..];
        Ok(count)
    }
}

fn events(text: &str) -> Vec<(String, TokenOccurrence)> {
    let mut result = Vec::new();
    visit_token_list(
        text,
        &SearchAnalyzerLexicon::default(),
        |term, occurrence| {
            result.push((term, occurrence));
            Ok(())
        },
    )
    .unwrap();
    result
}

#[test]
fn every_byte_split_preserves_exact_field_events() {
    for text in [
        "alpha beta alpha_beta alpha beta __ HTTPServer getURLValue running runners",
        "  \t\n __a___b___ __ __ c a_b a b __ ",
        "caf\u{e9} \u{130}stanbul \u{1f642} Stra\u{df}e \u{3a3}\u{391}\u{3a3}",
        "\u{4e2d}\u{6587}\u{641c}\u{7d22} \u{5927}\u{6587}\u{6863}HTTPServer \u{5317}\u{4eac}\u{5927}\u{5b66}",
        "", "_", "...", "only_identifier",
    ] {
        let expected = events(text);
        for split in 1..=text.len().max(1) {
            for width in [1, 2, 3, 7, 32] {
                let mut reader = ShortReads { remaining: text.as_bytes(), first: split, width };
                let mut actual = Vec::new();
                let bytes = visit_reader(&mut reader, &SearchAnalyzerLexicon::default(), Control::default(), u64::MAX, 1024, |term, occurrence| {
                    actual.push((term.into_untracked()?, occurrence));
                    Ok(())
                }).unwrap();
                assert_eq!(bytes, text.len() as u64);
                assert_eq!(actual, expected, "split={split}, width={width}, text={text:?}");
            }
        }
    }
}

#[test]
fn physical_split_does_not_create_or_lose_phrase_events() {
    let mut input = ShortReads {
        remaining: b"left right",
        first: 2,
        width: 1,
    };
    let mut actual = Vec::new();
    visit_reader(
        &mut input,
        &SearchAnalyzerLexicon::default(),
        Control::default(),
        10,
        5,
        |term, occurrence| {
            actual.push((term.into_untracked()?, occurrence));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(
        actual,
        vec![
            ("left".into(), TokenOccurrence::Repeated),
            ("left_right".into(), TokenOccurrence::UniqueInField),
            ("right".into(), TokenOccurrence::Repeated),
        ]
    );
}

#[test]
fn invalid_and_incomplete_utf8_are_rejected_at_every_split() {
    for bytes in [
        &b"ok \xff tail"[..],
        &b"\xf0\x9f\x99"[..],
        &b"\xc0\xaf"[..],
        &b"\xed\xa0\x80"[..],
    ] {
        for width in 1..=bytes.len() {
            let mut input = ShortReads {
                remaining: bytes,
                first: 0,
                width,
            };
            let error = visit_reader(
                &mut input,
                &SearchAnalyzerLexicon::default(),
                Control::default(),
                u64::MAX,
                1024,
                |_, _| Ok(()),
            )
            .unwrap_err();
            assert!(error.to_string().contains("UTF-8"), "{error}");
        }
    }
}

#[test]
fn minimum_identifier_unit_and_source_bytes_have_independent_bounds() {
    for (source, unit, success) in [(6, 6, true), (6, 5, false), (5, 6, false)] {
        let result = visit_reader(
            &mut &b"abcdef"[..],
            &SearchAnalyzerLexicon::default(),
            Control::default(),
            source,
            unit,
            |_, _| Ok(()),
        );
        assert_eq!(result.is_ok(), success);
    }
    let mut emitted = 0;
    let error = visit_reader(
        &mut &b"\xe4\xb8\xad\xe6\x96\x87"[..],
        &SearchAnalyzerLexicon::default(),
        Control::default(),
        6,
        5,
        |_, _| {
            emitted += 1;
            Ok(())
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("minimum unit requires 6"));
    assert_eq!(emitted, 0);
}

struct GeneratedBody {
    remaining: usize,
    offset: usize,
}

impl Read for GeneratedBody {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let count = output.len().min(self.remaining);
        for byte in &mut output[..count] {
            *byte = b"cat dog "[self.offset % 8];
            self.offset += 1;
        }
        self.remaining -= count;
        Ok(count)
    }
}

#[test]
fn generated_body_exceeds_operation_memory_without_materialization() {
    let bytes = 8 * 1024 * 1024;
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(64 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let mut input = GeneratedBody {
        remaining: bytes,
        offset: 0,
    };
    let mut repeated = 0;
    let mut phrases = 0;
    let count = visit_reader(
        &mut input,
        &SearchAnalyzerLexicon::default(),
        Control {
            memory: Some(crate::analyzer_memory::Memory::Build(&memory)),
            task: Some(&task),
            ..Control::default()
        },
        bytes as u64,
        32,
        |_, occurrence| {
            match occurrence {
                TokenOccurrence::Repeated => repeated += 1,
                TokenOccurrence::UniqueInField => phrases += 1,
            }
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(count, bytes as u64);
    assert_eq!(repeated, bytes / 4);
    assert_eq!(phrases, repeated - 1);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}

#[test]
fn emission_failure_and_cancellation_release_admission() {
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(64 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    for cancel in [false, true] {
        let mut emitted = 0;
        let error = visit_reader(
            &mut &b"left right tail"[..],
            &SearchAnalyzerLexicon::default(),
            Control {
                memory: Some(crate::analyzer_memory::Memory::Build(&memory)),
                task: Some(&task),
                ..Control::default()
            },
            100,
            32,
            |_, _| {
                emitted += 1;
                if cancel {
                    task.cancellation().cancel();
                    Ok(())
                } else {
                    Err(HawDBError::Execution("consumer denied".into()))
                }
            },
        )
        .unwrap_err();
        assert_eq!(emitted, 1);
        assert!(error
            .to_string()
            .contains(if cancel { "cancel" } else { "consumer denied" }));
        assert_eq!(memory.ledger.snapshot().used_bytes, 0);
    }
}

#[test]
fn streamed_chinese_analysis_retains_qualified_workspace_until_worker_exit() {
    let text = "\u{4e2d}\u{6587}\u{641c}\u{7d22} \u{5317}\u{4eac}\u{5927}\u{5b66} HTTPServer";
    let expected = events(text);
    let task = RuntimeTaskContext::default()
        .with_memory_reservation(RuntimeMemoryReservation::new(256 * 1024 * 1024, 0));
    let memory = BuildMemory::new(&task).unwrap();
    let actual = crate::analyzer_workspace::run(&memory, &task, |workspace| {
        let mut result = Vec::new();
        let mut input = ShortReads {
            remaining: text.as_bytes(),
            first: 0,
            width: 1,
        };
        visit_reader(
            &mut input,
            &SearchAnalyzerLexicon::default(),
            Control {
                memory: Some(crate::analyzer_memory::Memory::Build(&memory)),
                task: Some(&task),
                workspace: Some(workspace),
                checkpoint_throttle: None,
            },
            text.len() as u64,
            64,
            |term, occurrence| {
                result.push((term.as_str().to_owned(), occurrence));
                Ok(())
            },
        )?;
        Ok(result)
    })
    .unwrap();
    assert_eq!(actual, expected);
    assert_eq!(memory.ledger.snapshot().used_bytes, 0);
}
