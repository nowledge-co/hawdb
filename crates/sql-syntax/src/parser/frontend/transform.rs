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

use super::super::expression::Fold;
use super::{Output, Transformer};
use crate::peg::NodeId;
use crate::*;

impl Transformer<'_> {
    fn span(&self, node: NodeId) -> Span {
        self.tree.span(node, self.tokens)
    }
    fn first(&self, node: NodeId) -> NodeId {
        self.tree.children(node)[0]
    }
    fn child(&self, node: NodeId, name: &str) -> NodeId {
        self.tree.required(node, name)
    }
    fn fold(&self) -> Fold<'_> {
        Fold {
            tree: &self.tree,
            tokens: self.tokens,
        }
    }
    fn expr(&self, node: NodeId) -> ExpressionSyntax {
        self.fold().expression(node)
    }
    fn name(&self, node: NodeId) -> QualifiedName {
        self.fold().name(node)
    }
    fn ident(&self, node: NodeId) -> Identifier {
        super::super::identifier_from_token(self.tokens[self.tree.nodes[node].start])
    }
    fn identifiers(&self, node: NodeId) -> Vec<Identifier> {
        self.tree
            .children(node)
            .iter()
            .map(|id| self.ident(*id))
            .collect()
    }
    fn key(&self, node: NodeId) -> Vec<Identifier> {
        self.tree.child(node, "Key").map_or_else(Vec::new, |key| {
            self.identifiers(self.child(key, "Identifiers"))
        })
    }
    pub(super) fn output(&self, root: &str) -> Output {
        let mut node = self.first(self.tree.root);
        if matches!(root, "SqlInput" | "RelationalStatementInput") {
            node = self.first(node);
        }
        match self.tree.nodes[node].name {
            Some("RelationalExplain") => Output::Explain {
                analyze: self.tree.child(node, "Analyze").is_some(),
                select: Box::new(self.select(self.child(node, "RelationalSelect"))),
            },
            Some("Select" | "RelationalSelect") => Output::Select(Box::new(self.select(node))),
            Some("CreatePropertyGraph") => Output::Create(Box::new(self.create(node))),
            Some("GraphTable") => Output::GraphTable(Box::new(self.graph_table(node))),
            _ => unreachable!("statement CST alternative"),
        }
    }
    fn labeled(&self, node: NodeId) -> LabeledExpressionSyntax {
        let alias = self
            .tree
            .child(node, "AsAlias")
            .or_else(|| self.tree.child(node, "ProjectionAlias"));
        LabeledExpressionSyntax {
            expression: self.expr(self.child(node, "Or")),
            alias: alias.map(|id| self.ident(self.child(id, "Identifier"))),
            span: self.span(node),
        }
    }
    fn clause(&self, node: NodeId, name: &str) -> Option<ExpressionSyntax> {
        self.tree
            .child(node, name)
            .and_then(|clause| self.tree.child(clause, "Or"))
            .map(|id| self.expr(id))
    }
    fn select(&self, node: NodeId) -> PostgresSelectSyntax {
        let relational = self.tree.nodes[node].name == Some("RelationalSelect");
        let projection = self.child(
            node,
            if relational {
                "RelationalProjection"
            } else {
                "Projection"
            },
        );
        let from = self
            .tree
            .child(node, "From")
            .or_else(|| self.tree.child(node, "RelationalFrom"));
        let group_by = self.tree.child(node, "Group").map_or_else(Vec::new, |id| {
            self.tree
                .children(id)
                .iter()
                .map(|id| self.expr(*id))
                .collect()
        });
        let order_by = self.tree.child(node, "Order").map_or_else(Vec::new, |id| {
            self.tree
                .children(id)
                .iter()
                .map(|id| self.order(*id))
                .collect()
        });
        let locking = self
            .tree
            .child(node, "Locking")
            .or_else(|| self.tree.child(node, "RelationalLocking"))
            .map(|id| {
                let strength_node = self.tree.child(id, "Locking").unwrap_or(id);
                LockingClauseSyntax {
                    nonblocking: self.tree.child(id, "Nonblocking").is_some(),
                    strength: if self.tree.child(strength_node, "Share").is_some() {
                        LockStrengthSyntax::Share
                    } else {
                        LockStrengthSyntax::Update
                    },
                    span: self.span(id),
                }
            });
        PostgresSelectSyntax {
            distinct: self
                .tree
                .child(node, "Quantifier")
                .is_some_and(|id| self.tree.child(id, "Distinct").is_some()),
            projection: self
                .tree
                .children(projection)
                .iter()
                .map(|id| self.labeled(*id))
                .collect(),
            from: from.map_or_else(Vec::new, |id| {
                self.tree
                    .children(id)
                    .iter()
                    .map(|id| self.source(*id))
                    .collect()
            }),
            selection: self.clause(node, "Where"),
            group_by,
            having: self.clause(node, "Having"),
            order_by,
            limit: self
                .clause(node, "Limit")
                .or_else(|| self.clause(node, "RelationalLimit")),
            offset: self
                .clause(node, "Offset")
                .or_else(|| self.clause(node, "RelationalOffset")),
            locking,
            span: self.span(node),
        }
    }
    fn order(&self, node: NodeId) -> OrderBySyntax {
        OrderBySyntax {
            expression: self.expr(self.child(node, "Or")),
            direction: if self.tree.child(node, "Ascending").is_some() {
                Some(OrderDirectionSyntax::Ascending)
            } else if self.tree.child(node, "Descending").is_some() {
                Some(OrderDirectionSyntax::Descending)
            } else {
                None
            },
            nulls: self.tree.child(node, "NullOrder").map(|id| {
                if self.tree.child(id, "First").is_some() {
                    NullOrderSyntax::First
                } else {
                    NullOrderSyntax::Last
                }
            }),
            span: self.span(node),
        }
    }
    fn source(&self, node: NodeId) -> PostgresFromSyntax {
        let children = self.tree.children(node);
        PostgresFromSyntax {
            relation: self.transform_from_item(children[0]),
            joins: children[1..].iter().map(|id| self.join(*id)).collect(),
            span: self.span(node),
        }
    }
    fn transform_from_item(&self, node: NodeId) -> PostgresFromItemSyntax {
        let node = self.first(node);
        if self.tree.nodes[node].name == Some("GraphTable") {
            return PostgresFromItemSyntax::GraphTable(self.graph_table(node));
        }
        let args = self.tree.child(node, "TableArguments").map(|id| {
            self.tree
                .children(id)
                .iter()
                .map(|id| TableFunctionArgumentSyntax {
                    name: self
                        .tree
                        .child(*id, "ArgumentName")
                        .map(|id| self.ident(self.child(id, "Identifier"))),
                    expression: self.expr(self.child(*id, "Or")),
                })
                .collect()
        });
        PostgresFromItemSyntax::Relation(RelationTableSyntax {
            name: self.name(self.child(node, "Name")),
            arguments: args,
            alias: self.alias(node),
            span: self.span(node),
        })
    }
    fn alias(&self, node: NodeId) -> Option<TableAlias> {
        self.tree
            .child(node, "Alias")
            .or_else(|| self.tree.child(node, "RelationalAsAlias"))
            .map(|id| {
                let name = self.ident(self.child(id, "Identifier"));
                TableAlias {
                    span: Span::new(name.span.start, self.span(id).end),
                    name,
                    columns: self
                        .tree
                        .child(id, "Identifiers")
                        .map_or_else(Vec::new, |id| self.identifiers(id)),
                }
            })
    }
    fn join(&self, node: NodeId) -> PostgresJoinSyntax {
        let node = self.first(node);
        PostgresJoinSyntax {
            kind: match self.tree.nodes[node].name {
                Some("InnerJoin" | "RelationalInnerJoin") => PostgresJoinKind::Inner,
                Some("LeftJoin" | "RelationalLeftJoin") => PostgresJoinKind::Left,
                _ => PostgresJoinKind::Cross,
            },
            relation: self.transform_from_item(
                self.tree
                    .child(node, "FromItem")
                    .or_else(|| self.tree.child(node, "RelationalFromItem"))
                    .expect("join source"),
            ),
            condition: self.tree.child(node, "Or").map(|id| self.expr(id)),
            span: self.span(node),
        }
    }
    fn graph_table(&self, node: NodeId) -> GraphTable {
        GraphTable {
            graph: self.name(self.child(node, "Name")),
            pattern: self.pattern(self.child(node, "Pattern")),
            columns: self
                .tree
                .children(node)
                .iter()
                .filter(|id| self.tree.nodes[**id].name == Some("Labeled"))
                .map(|id| {
                    let item = self.labeled(*id);
                    GraphTableColumn {
                        expression: item.expression,
                        alias: item.alias,
                        span: item.span,
                    }
                })
                .collect(),
            alias: self.alias(node),
            span: self.span(node),
        }
    }
    fn pattern(&self, node: NodeId) -> GraphPatternSyntax {
        GraphPatternSyntax {
            paths: self
                .tree
                .children(node)
                .iter()
                .filter(|id| self.tree.nodes[**id].name == Some("Path"))
                .map(|id| self.path(*id))
                .collect(),
            predicate: self.clause(node, "Where"),
            span: self.span(node),
        }
    }
    fn path(&self, node: NodeId) -> GraphPathSyntax {
        GraphPathSyntax {
            factors: self
                .tree
                .children(node)
                .iter()
                .map(|id| self.factor(*id))
                .collect(),
            span: self.span(node),
        }
    }
    fn factor(&self, node: NodeId) -> GraphPathFactorSyntax {
        let primary = self.first(self.child(node, "Primary"));
        let primary = match self.tree.nodes[primary].name {
            Some("NestedPath") => {
                let contents = self.child(primary, "NestedContents");
                GraphPathPrimarySyntax::Parenthesized {
                    path: Box::new(self.path(self.child(contents, "Path"))),
                    predicate: self.clause(contents, "Where"),
                    span: self.span(primary),
                }
            }
            Some("Vertex") => {
                let fields = self.child(primary, "Fields");
                GraphPathPrimarySyntax::Vertex(GraphElementPatternSyntax {
                    variable: self.variable(fields),
                    labels: self.labels(fields),
                    predicate: self.clause(fields, "Where"),
                    span: self.span(primary),
                })
            }
            Some("Edge") => GraphPathPrimarySyntax::Edge(self.edge(primary)),
            _ => unreachable!("graph primary"),
        };
        GraphPathFactorSyntax {
            primary,
            quantifier: self
                .tree
                .child(node, "GraphQuantifier")
                .map(|id| self.quantifier(id)),
            span: self.span(node),
        }
    }
    fn variable(&self, node: NodeId) -> Option<Identifier> {
        self.tree
            .child(node, "Variable")
            .map(|id| self.ident(self.child(id, "Identifier")))
    }
    fn labels(&self, node: NodeId) -> Vec<Identifier> {
        self.tree
            .child(node, "Labels")
            .map_or_else(Vec::new, |id| self.identifiers(id))
    }
    fn edge(&self, node: NodeId) -> GraphEdgePatternSyntax {
        let span = self.span(node);
        let edge = self.first(node);
        let fields = self
            .tree
            .child(edge, "EdgeFields")
            .map(|id| self.child(id, "Fields"));
        let last = self.tokens[self.tree.nodes[node].end - 1].kind;
        let direction = if self.tree.nodes[edge].name == Some("LeftEdge") {
            GraphEdgeDirection::Left
        } else if matches!(last, TokenKind::Greater | TokenKind::ArrowRight) {
            GraphEdgeDirection::Right
        } else {
            GraphEdgeDirection::Any
        };
        GraphEdgePatternSyntax {
            direction,
            variable: fields.and_then(|id| self.variable(id)),
            labels: fields.map_or_else(Vec::new, |id| self.labels(id)),
            predicate: fields.and_then(|id| self.clause(id, "Where")),
            abbreviated: fields.is_none(),
            span,
        }
    }
    fn quantifier(&self, node: NodeId) -> GraphPatternQuantifier {
        let values: Vec<u32> = self
            .tree
            .children(node)
            .iter()
            .map(|id| {
                self.tokens[self.tree.nodes[*id].start]
                    .text(self.input)
                    .parse()
                    .expect("validated quantifier integer")
            })
            .collect();
        let (min, max) = if self.tokens[self.tree.nodes[node].start + 1].kind == TokenKind::Comma {
            (0, values[0])
        } else {
            (values[0], *values.last().unwrap())
        };
        GraphPatternQuantifier {
            min,
            max,
            span: self.span(node),
        }
    }
    fn create(&self, node: NodeId) -> CreatePropertyGraph {
        CreatePropertyGraph {
            temporary: self.tree.child(node, "Temporary").is_some(),
            name: self.name(self.child(node, "Name")),
            vertex_tables: self
                .tree
                .child(node, "Vertices")
                .map_or_else(Vec::new, |id| {
                    self.tree
                        .children(id)
                        .iter()
                        .map(|id| self.vertex_definition(*id))
                        .collect()
                }),
            edge_tables: self.tree.child(node, "Edges").map_or_else(Vec::new, |id| {
                self.tree
                    .children(id)
                    .iter()
                    .map(|id| self.edge_definition(*id))
                    .collect()
            }),
            span: self.span(node),
        }
    }
    fn exposure(&self, node: NodeId) -> Option<ElementExposure> {
        self.tree.child(node, "Exposure").map(|id| {
            let children = self.tree.children(id);
            let labels = if self.tree.nodes[children[0]].name == Some("Properties") {
                vec![ElementLabelExposure {
                    label: ElementLabel::Implicit,
                    properties: self.properties(children[0]),
                    span: self.span(id),
                }]
            } else {
                children
                    .iter()
                    .map(|id| {
                        let label = self
                            .tree
                            .child(*id, "NamedLabel")
                            .map_or(ElementLabel::Default, |id| {
                                ElementLabel::Named(self.ident(self.child(id, "Identifier")))
                            });
                        ElementLabelExposure {
                            label,
                            properties: self
                                .tree
                                .child(*id, "Properties")
                                .map_or(PropertyExposure::AllColumns, |id| self.properties(id)),
                            span: self.span(*id),
                        }
                    })
                    .collect()
            };
            ElementExposure {
                labels,
                span: self.span(id),
            }
        })
    }
    fn properties(&self, node: NodeId) -> PropertyExposure {
        let node = self.first(node);
        match self.tree.nodes[node].name {
            Some("NoProperties") => PropertyExposure::NoProperties,
            Some("AllProperties") => PropertyExposure::AllColumns,
            _ => PropertyExposure::Expressions(
                self.tree
                    .children(node)
                    .iter()
                    .map(|id| self.labeled(*id))
                    .collect(),
            ),
        }
    }
    fn vertex_definition(&self, node: NodeId) -> VertexTableDefinition {
        VertexTableDefinition {
            table: self.name(self.child(node, "Name")),
            alias: self
                .tree
                .child(node, "AsAlias")
                .map(|id| self.ident(self.child(id, "Identifier"))),
            key: self.key(node),
            exposure: self.exposure(node),
            span: self.span(node),
        }
    }
    fn endpoint(&self, node: NodeId) -> EdgeEndpoint {
        EdgeEndpoint {
            key: self.key(node),
            vertex: self.ident(self.child(node, "Identifier")),
            vertex_key: self
                .tree
                .child(node, "Identifiers")
                .map_or_else(Vec::new, |id| self.identifiers(id)),
            span: self.span(node),
        }
    }
    fn edge_definition(&self, node: NodeId) -> EdgeTableDefinition {
        let endpoints: Vec<_> = self
            .tree
            .children(node)
            .iter()
            .filter(|id| self.tree.nodes[**id].name == Some("Endpoint"))
            .collect();
        EdgeTableDefinition {
            table: self.name(self.child(node, "Name")),
            alias: self
                .tree
                .child(node, "AsAlias")
                .map(|id| self.ident(self.child(id, "Identifier"))),
            key: self.key(node),
            source: self.endpoint(*endpoints[0]),
            destination: self.endpoint(*endpoints[1]),
            exposure: self.exposure(node),
            span: self.span(node),
        }
    }
}
