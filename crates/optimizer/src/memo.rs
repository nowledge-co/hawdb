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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(usize);

impl GroupId {
    pub fn new(index: usize) -> Self {
        Self(index)
    }

    pub fn index(self) -> usize {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoGroup<E> {
    expressions: Vec<E>,
}

impl<E> MemoGroup<E> {
    pub fn new(expression: E) -> Self {
        Self {
            expressions: vec![expression],
        }
    }

    pub fn expressions(&self) -> &[E] {
        &self.expressions
    }

    pub fn first_expression(&self) -> Option<&E> {
        self.expressions.first()
    }

    pub fn expression_count(&self) -> usize {
        self.expressions.len()
    }

    pub fn push(&mut self, expression: E) {
        self.expressions.push(expression);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Memo<E> {
    groups: Vec<MemoGroup<E>>,
}

impl<E> Default for Memo<E> {
    fn default() -> Self {
        Self { groups: Vec::new() }
    }
}

impl<E> Memo<E> {
    pub fn insert_group(&mut self, expression: E) -> GroupId {
        let id = GroupId::new(self.groups.len());
        self.groups.push(MemoGroup::new(expression));
        id
    }

    pub fn group(&self, id: GroupId) -> Option<&MemoGroup<E>> {
        self.groups.get(id.index())
    }

    pub fn group_mut(&mut self, id: GroupId) -> Option<&mut MemoGroup<E>> {
        self.groups.get_mut(id.index())
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    pub fn groups(&self) -> &[MemoGroup<E>] {
        &self.groups
    }
}

#[cfg(test)]
mod tests {
    use super::Memo;

    #[test]
    fn memo_interns_group_ids_by_insertion_order() {
        let mut memo = Memo::default();

        let first = memo.insert_group("scan");
        let second = memo.insert_group("filter");

        assert_eq!(first.index(), 0);
        assert_eq!(second.index(), 1);
        assert_eq!(memo.group_count(), 2);
        assert!(!memo.is_empty());
        assert_eq!(memo.group(first).unwrap().expressions(), &["scan"]);
        assert_eq!(memo.group(first).unwrap().first_expression(), Some(&"scan"));
    }

    #[test]
    fn memo_group_can_hold_equivalent_expressions() {
        let mut memo = Memo::default();
        let group = memo.insert_group("seq_scan");

        memo.group_mut(group).unwrap().push("index_scan");

        assert_eq!(
            memo.group(group).unwrap().expressions(),
            &["seq_scan", "index_scan"]
        );
        assert_eq!(memo.group(group).unwrap().expression_count(), 2);
    }
}
