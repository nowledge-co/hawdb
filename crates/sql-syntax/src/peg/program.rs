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

//! Lower resolved PEG expressions to instructions without expanding rule calls.
//! Scope entry preserves logical expression budgets; branches target matching
//! cleanup instructions so capture rollback runs on both success and mismatch.

use super::Expression;
use crate::TokenKind;

pub(super) const HALT: u8 = 0;
pub(super) const CALL: u8 = 1;
pub(super) const RETURN: u8 = 2;
pub(super) const ENTER: u8 = 3;
pub(super) const LEAVE: u8 = 4;
pub(super) const KEYWORD: u8 = 5;
pub(super) const TOKEN: u8 = 6;
pub(super) const PARAMETER: u8 = 7;
pub(super) const IF_FAILED: u8 = 8;
pub(super) const CHOICE_NEXT: u8 = 9;
pub(super) const OPTIONAL_END: u8 = 10;
pub(super) const REPEAT_NEXT: u8 = 11;
pub(super) const TIGHT_END: u8 = 12;
pub(super) const LOOK_END: u8 = 13;
pub(super) const CALL_EXPRESSION: u8 = 14;
pub(super) const KEYWORD_CHOICE: u8 = 15;
pub(super) const ROOT_CALL: usize = super::MAX_RULES;
pub(super) const CHOICE_SCOPE: usize = 1;
pub(super) const LOOK_SCOPE: usize = 2;
const SHIFT: u32 = 60;
const ARGUMENT: u64 = (1_u64 << SHIFT) - 1;

pub(super) struct Program {
    pub instructions: Vec<u64>,
    pub entries: Vec<usize>,
    pub keywords: Vec<&'static str>,
    pub keyword_choices: Vec<std::ops::Range<usize>>,
    pub tokens: Vec<(TokenKind, &'static str)>,
    pub looks: Vec<(&'static str, bool)>,
}

impl Program {
    pub fn compile(rules: Vec<(&'static str, Expression<usize>)>) -> (Self, Vec<&'static str>) {
        let mut program = Self {
            instructions: Vec::new(),
            entries: Vec::with_capacity(rules.len()),
            keywords: Vec::new(),
            keyword_choices: Vec::new(),
            tokens: Vec::new(),
            looks: Vec::new(),
        };
        program.emit(CALL, ROOT_CALL);
        program.emit(HALT, 0);
        let mut names = Vec::with_capacity(rules.len());
        for (name, body) in rules {
            names.push(name);
            program.entries.push(program.instructions.len());
            program.expression(&body);
            program.emit(RETURN, 0);
        }
        program.instructions.shrink_to_fit();
        program.keywords.shrink_to_fit();
        program.keyword_choices.shrink_to_fit();
        program.tokens.shrink_to_fit();
        program.looks.shrink_to_fit();
        (program, names)
    }

    pub fn decode(&self, pc: usize) -> (u8, usize) {
        let instruction = self.instructions[pc];
        (
            (instruction >> SHIFT) as u8,
            (instruction & ARGUMENT) as usize,
        )
    }

    fn emit(&mut self, opcode: u8, argument: usize) -> usize {
        // All operands are rule IDs, small scope flags, or indices into
        // non-zero-sized program vectors. Their allocation bounds fit 60 bits.
        let argument = argument as u64;
        assert!(
            argument <= ARGUMENT,
            "program operand fits allocation bounds"
        );
        let pc = self.instructions.len();
        self.instructions
            .push((u64::from(opcode) << SHIFT) | argument);
        pc
    }

    fn patch(&mut self, pc: usize, target: usize) {
        let (opcode, _) = self.decode(pc);
        assert!(
            (target as u64) <= ARGUMENT,
            "program target fits allocation bounds"
        );
        self.instructions[pc] = (u64::from(opcode) << SHIFT) | target as u64;
    }

    // While compiling a branch, its jump operands form a temporary linked
    // list (zero ends it). Patching consumes the list before parsing can begin,
    // avoiding a separate allocation for every sequence and choice.
    fn link_jump(&mut self, opcode: u8, jumps: &mut Option<usize>) {
        *jumps = Some(self.emit(opcode, jumps.map_or(0, |pc| pc + 1)));
    }

    fn patch_jumps(&mut self, mut jumps: Option<usize>, target: usize) {
        while let Some(pc) = jumps {
            let (_, next) = self.decode(pc);
            self.patch(pc, target);
            jumps = next.checked_sub(1);
        }
    }

    fn expression(&mut self, expression: &Expression<usize>) {
        match expression {
            Expression::Keyword(keyword) => {
                let id = self.keywords.len();
                self.keywords.push(keyword);
                self.emit(KEYWORD, id);
            }
            Expression::Token(kind, expected) => {
                let id = self.tokens.len();
                self.tokens.push((*kind, expected));
                self.emit(TOKEN, id);
            }
            Expression::Parameter => {
                self.emit(PARAMETER, 0);
            }
            Expression::Rule(rule) => {
                self.emit(CALL_EXPRESSION, *rule);
            }
            Expression::Sequence(items) => {
                self.emit(ENTER, 0);
                let mut jumps = None;
                for (index, item) in items.iter().enumerate() {
                    self.expression(item);
                    if index + 1 < items.len() {
                        self.link_jump(IF_FAILED, &mut jumps);
                    }
                }
                let exit = self.emit(LEAVE, 0);
                self.patch_jumps(jumps, exit);
            }
            Expression::Choice(items)
                if !items.is_empty()
                    && items
                        .iter()
                        .all(|item| matches!(item, Expression::Keyword(_))) =>
            {
                let start = self.keywords.len();
                self.keywords.extend(items.iter().map(|item| {
                    let Expression::Keyword(keyword) = item else {
                        unreachable!()
                    };
                    *keyword
                }));
                let id = self.keyword_choices.len();
                self.keyword_choices.push(start..self.keywords.len());
                self.emit(KEYWORD_CHOICE, id);
            }
            Expression::Choice(items) => {
                self.emit(ENTER, CHOICE_SCOPE);
                let mut jumps = None;
                for (index, item) in items.iter().enumerate() {
                    self.expression(item);
                    if index + 1 < items.len() {
                        self.link_jump(CHOICE_NEXT, &mut jumps);
                    }
                }
                let exit = self.emit(LEAVE, 0);
                self.patch_jumps(jumps, exit);
            }
            Expression::Optional(inner) => {
                self.emit(ENTER, 0);
                self.expression(inner);
                self.emit(OPTIONAL_END, 0);
                self.emit(LEAVE, 0);
            }
            Expression::Repeat(inner) => {
                self.emit(ENTER, 0);
                let begin = self.instructions.len();
                self.expression(inner);
                self.emit(REPEAT_NEXT, begin);
                self.emit(LEAVE, 0);
            }
            Expression::Tight(inner) => {
                self.emit(ENTER, 0);
                self.expression(inner);
                self.emit(TIGHT_END, 0);
                self.emit(LEAVE, 0);
            }
            Expression::Ahead(inner, expected) | Expression::Not(inner, expected) => {
                self.emit(ENTER, LOOK_SCOPE);
                self.expression(inner);
                let id = self.looks.len();
                self.looks
                    .push((expected, matches!(expression, Expression::Ahead(..))));
                self.emit(LOOK_END, id);
                self.emit(LEAVE, 0);
            }
        }
    }
}
