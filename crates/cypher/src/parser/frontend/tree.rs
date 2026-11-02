// Copyright 2026 Nowledge
// SPDX-License-Identifier: Apache-2.0

//! Cypher captures use token offsets within the existing 16 MiB input and
//! 262144 node/edge budgets. Shared matcher memo heights are not stored here.
use super::rust_peg::RULE_NAMES;
use crate::{OrderItem, ReturnItem};
use hawdb_core::Result;
use hawdb_sql_syntax::peg::NodeId;
use hawdb_sql_syntax::{Span, Token};
use std::cell::RefCell;
use std::ops::Range;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct RuleId(u8);
pub(super) const fn rule_id(name: &str) -> RuleId {
    let needle = name.as_bytes();
    let mut rule = 0;
    while rule < RULE_NAMES.len() {
        let candidate = RULE_NAMES[rule].as_bytes();
        if candidate.len() == needle.len() {
            let mut i = 0;
            while i < needle.len() && candidate[i] == needle[i] {
                i += 1;
            }
            if i == needle.len() {
                assert!(rule <= u8::MAX as usize);
                return RuleId(rule as u8);
            }
        }
        rule += 1;
    }
    panic!("unknown Cypher rule")
}

pub(super) struct Node {
    rule_and_children_len: u32,
    start: u32,
    end: u32,
    children_start: u32,
}
pub(super) struct NodeView {
    pub name: Option<&'static str>,
    pub start: usize,
    pub end: usize,
}
// On 64-bit hosts small captures fit in the handle itself. Smaller
// hosts retain ordinary arena nodes; representation never limits admission.
const INLINE_LEAF: u64 = 1 << 63;
const INLINE_ALIAS: u64 = 1 << 62;
const INLINE_BRANCH: u64 = 1 << 53;
const BRANCH_LENGTH_BITS: u32 = 8;
const BRANCH_LENGTH_MASK: u64 = (1 << BRANCH_LENGTH_BITS) - 1;
const ALIAS_EDGE_BITS: u32 = 18;
const ALIAS_LENGTH_BITS: u32 = 11;
const ALIAS_EDGE_MASK: u64 = (1 << ALIAS_EDGE_BITS) - 1;
const ALIAS_LENGTH_MASK: u64 = (1 << ALIAS_LENGTH_BITS) - 1;
const TOKEN_BITS: u32 = 25;
const TOKEN_MASK: u64 = (1 << TOKEN_BITS) - 1;
pub(super) fn inline_leaf(rule: u16, start: usize, end: usize) -> Option<NodeId> {
    if usize::BITS < 64
        || rule > u16::from(u8::MAX)
        || start as u64 > TOKEN_MASK
        || end as u64 > TOKEN_MASK
    {
        return None;
    }
    usize::try_from(
        INLINE_LEAF | (u64::from(rule) << 54) | ((start as u64) << TOKEN_BITS) | end as u64,
    )
    .ok()
}
pub(super) fn inline_alias(rule: u16, start: usize, end: usize, edge: usize) -> Option<NodeId> {
    let len = end.checked_sub(start)?;
    if usize::BITS < 64
        || rule > u16::from(u8::MAX)
        || start as u64 > TOKEN_MASK
        || len as u64 > ALIAS_LENGTH_MASK
        || edge as u64 > ALIAS_EDGE_MASK
    {
        return None;
    }
    usize::try_from(
        INLINE_LEAF
            | INLINE_ALIAS
            | (u64::from(rule) << 54)
            | ((edge as u64) << 36)
            | ((start as u64) << ALIAS_LENGTH_BITS)
            | len as u64,
    )
    .ok()
}
pub(super) fn inline_branch(
    rule: u16,
    start: usize,
    end: usize,
    edges: Range<usize>,
) -> Option<NodeId> {
    let len = end.checked_sub(start)?;
    let count = edges.end.checked_sub(edges.start)?;
    if usize::BITS < 64
        || rule > u16::from(u8::MAX)
        || start as u64 > TOKEN_MASK
        || len as u64 > BRANCH_LENGTH_MASK
        || edges.start as u64 > ALIAS_EDGE_MASK
        || !(2..=5).contains(&count)
    {
        return None;
    }
    usize::try_from(
        INLINE_LEAF
            | INLINE_BRANCH
            | (u64::from(rule) << 54)
            | (((count - 2) as u64) << 51)
            | ((len as u64) << 43)
            | ((edges.start as u64) << TOKEN_BITS)
            | start as u64,
    )
    .ok()
}
fn is_inline_leaf(id: NodeId) -> bool {
    id as u64 & INLINE_LEAF != 0
}
impl Node {
    pub(super) fn from_token_match(
        rule: u16,
        start: usize,
        end: usize,
        children: Range<usize>,
    ) -> Self {
        Self {
            rule_and_children_len: u32::from(u8::try_from(rule).expect("bounded rule id"))
                | (u32::try_from(children.len()).expect("bounded child count") << 8),
            start: u32::try_from(start).expect("bounded token offset"),
            end: u32::try_from(end).expect("bounded token offset"),
            children_start: u32::try_from(children.start).expect("bounded child offset"),
        }
    }
}

enum FoldedList {
    Projection(Result<Vec<ReturnItem>>),
    Order(Result<Vec<OrderItem>>),
}
#[derive(Default)]
pub(super) struct ProjectionCaches {
    first: Option<(NodeId, FoldedList)>,
    second: Option<(NodeId, FoldedList)>,
    more: Vec<(NodeId, FoldedList)>,
}
impl ProjectionCaches {
    pub(super) fn len(&self) -> usize {
        usize::from(self.first.is_some()) + usize::from(self.second.is_some()) + self.more.len()
    }
    pub(super) fn truncate(&mut self, len: usize) {
        if len == 0 {
            self.first = None;
        }
        if len < 2 {
            self.second = None;
        }
        self.more.truncate(len.saturating_sub(2));
    }
    fn put_list(&mut self, id: NodeId, value: FoldedList) {
        if self.first.is_none() {
            self.first = Some((id, value));
        } else if self.second.is_none() {
            self.second = Some((id, value));
        } else {
            self.more.push((id, value));
        }
    }
    fn take_list(&mut self, id: NodeId) -> Option<FoldedList> {
        if self.first.as_ref().is_some_and(|entry| entry.0 == id) {
            return self.first.take().map(|entry| entry.1);
        }
        if self.second.as_ref().is_some_and(|entry| entry.0 == id) {
            return self.second.take().map(|entry| entry.1);
        }
        self.more
            .iter()
            .position(|entry| entry.0 == id)
            .map(|index| self.more.swap_remove(index).1)
    }
    pub(super) fn put(&mut self, id: NodeId, value: Result<Vec<ReturnItem>>) {
        self.put_list(id, FoldedList::Projection(value));
    }
    fn take(&mut self, id: NodeId) -> Option<Result<Vec<ReturnItem>>> {
        self.take_list(id).map(|value| match value {
            FoldedList::Projection(items) => items,
            FoldedList::Order(_) => unreachable!("projection rule cache"),
        })
    }
    pub(super) fn put_order(&mut self, id: NodeId, value: Result<Vec<OrderItem>>) {
        self.put_list(id, FoldedList::Order(value));
    }
    fn take_order(&mut self, id: NodeId) -> Option<Result<Vec<OrderItem>>> {
        self.take_list(id).map(|value| match value {
            FoldedList::Order(items) => items,
            FoldedList::Projection(_) => unreachable!("order rule cache"),
        })
    }
}

pub(super) struct ParseTree {
    pub nodes: Vec<Node>,
    edges: Vec<NodeId>,
    pub root: NodeId,
    projections: RefCell<ProjectionCaches>,
}
impl ParseTree {
    pub(super) fn with_projections(mut self, projections: ProjectionCaches) -> Self {
        self.projections = RefCell::new(projections);
        self
    }
    pub(super) fn take_projection(&self, node: NodeId) -> Option<Result<Vec<ReturnItem>>> {
        self.projections.borrow_mut().take(node)
    }
    pub(super) fn take_order(&self, node: NodeId) -> Option<Result<Vec<OrderItem>>> {
        self.projections.borrow_mut().take_order(node)
    }
    pub(super) fn into_parts(self) -> (Vec<Node>, Vec<NodeId>) {
        (self.nodes, self.edges)
    }

    pub(super) fn from_token_matches(nodes: Vec<Node>, edges: Vec<NodeId>, root: NodeId) -> Self {
        Self {
            nodes,
            edges,
            root,
            projections: RefCell::default(),
        }
    }
    #[inline]
    pub(super) fn node(&self, id: NodeId) -> NodeView {
        if is_inline_leaf(id) {
            let packed = id as u64;
            let (start, end) = if packed & INLINE_ALIAS != 0 {
                let start = ((packed >> ALIAS_LENGTH_BITS) & TOKEN_MASK) as usize;
                (start, start + (packed & ALIAS_LENGTH_MASK) as usize)
            } else if packed & INLINE_BRANCH != 0 {
                let start = (packed & TOKEN_MASK) as usize;
                (
                    start,
                    start + ((packed >> 43) & BRANCH_LENGTH_MASK) as usize,
                )
            } else {
                (
                    ((packed >> TOKEN_BITS) & TOKEN_MASK) as usize,
                    (packed & TOKEN_MASK) as usize,
                )
            };
            return NodeView {
                name: Some(RULE_NAMES[((packed >> 54) & 255) as usize]),
                start,
                end,
            };
        }
        let node = &self.nodes[id];
        NodeView {
            name: Some(RULE_NAMES[(node.rule_and_children_len & 255) as usize]),
            start: node.start as usize,
            end: node.end as usize,
        }
    }
    pub(super) fn children(&self, node: NodeId) -> &[NodeId] {
        if is_inline_leaf(node) {
            let packed = node as u64;
            if packed & INLINE_ALIAS != 0 {
                let edge = ((packed >> 36) & ALIAS_EDGE_MASK) as usize;
                return &self.edges[edge..edge + 1];
            }
            if packed & INLINE_BRANCH != 0 {
                let edge = ((packed >> TOKEN_BITS) & ALIAS_EDGE_MASK) as usize;
                let count = ((packed >> 51) & 3) as usize + 2;
                return &self.edges[edge..edge + count];
            }
            return &[];
        }
        let n = &self.nodes[node];
        let start = n.children_start as usize;
        &self.edges[start..start + (n.rule_and_children_len >> 8) as usize]
    }
    #[inline]
    pub(super) fn rule_id(&self, node: NodeId) -> RuleId {
        if is_inline_leaf(node) {
            RuleId(((node as u64 >> 54) & 255) as u8)
        } else {
            RuleId((self.nodes[node].rule_and_children_len & 255) as u8)
        }
    }
    pub(super) fn child(&self, node: NodeId, name: RuleId) -> Option<NodeId> {
        self.children(node)
            .iter()
            .copied()
            .find(|id| self.rule_id(*id) == name)
    }
    pub(super) fn required(&self, node: NodeId, name: RuleId) -> NodeId {
        self.child(node, name)
            .expect("matched grammar has the required child")
    }
    pub(super) fn span(&self, node: NodeId, tokens: &[Token]) -> Span {
        let node = self.node(node);
        let start = tokens[node.start].span.start;
        let end = if node.start == node.end {
            start
        } else {
            tokens[node.end - 1].span.end
        };
        Span::new(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_branches_preserve_spans_and_borrowed_children() {
        if usize::BITS < 64 {
            assert!(inline_branch(0, 0, 1, 0..2).is_none());
            return;
        }
        let mut edges = vec![0; ALIAS_EDGE_MASK as usize + 5];
        let first = ALIAS_EDGE_MASK as usize;
        edges[first..].copy_from_slice(&[3, 7, 11, 17, 23]);
        let tree = ParseTree::from_token_matches(Vec::new(), edges, 0);
        for count in 2..=5 {
            for len in [0, 1, BRANCH_LENGTH_MASK as usize] {
                let start = TOKEN_MASK as usize - len;
                let id = inline_branch(0, start, start + len, first..first + count).unwrap();
                let view = tree.node(id);
                assert_eq!((view.start, view.end), (start, start + len));
                assert_eq!(view.name, Some(RULE_NAMES[0]));
                assert!(tree.rule_id(id) == RuleId(0));
                assert_eq!(tree.children(id), &tree.edges[first..first + count]);
                assert_eq!(tree.children(id).as_ptr(), tree.edges[first..].as_ptr());
            }
        }
        // Alias edge bits overlap the branch marker; alias decoding takes priority.
        let alias = inline_alias(0, 42, 43, first).unwrap();
        assert_eq!(tree.node(alias).start, 42);
        assert_eq!(tree.children(alias), &[3]);
        let leaf = inline_leaf(0, TOKEN_MASK as usize, TOKEN_MASK as usize).unwrap();
        assert_eq!(tree.node(leaf).end, TOKEN_MASK as usize);
        assert!(tree.children(leaf).is_empty());
    }

    #[test]
    fn inline_branch_limits_fall_back_to_arena_nodes() {
        assert!(inline_branch(0, 0, 256, 0..2).is_none());
        assert!(inline_branch(0, 2, 1, 0..2).is_none());
        assert!(inline_branch(0, 0, 1, 0..1).is_none());
        assert!(inline_branch(0, 0, 1, 0..6).is_none());
        assert!(inline_branch(256, 0, 1, 0..2).is_none());
        let edge = ALIAS_EDGE_MASK as usize + 1;
        assert!(inline_branch(0, 0, 1, edge..edge + 2).is_none());
        let start = TOKEN_MASK as usize + 1;
        assert!(inline_branch(0, start, start, 0..2).is_none());
    }
}
