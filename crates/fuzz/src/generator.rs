use crate::predicate_rewrite::PredicateRewriteKind;
use crate::query_ast::{
    GeneratedSchema, MatchPattern, NodePattern, OrderItem, PatternDirection, PropertyExpression,
    QueryAst, QueryPredicate, ReturnItem,
};
use crate::{
    FuzzCase, GraphPredicateRewriteCase, GraphTlpCase, MetamorphicCase, MetamorphicRelation,
    Mutation, Parameters, QueryInvocation, ResultSemantics,
};
use skein::Value;

const ENTITY_COUNT: usize = 6;

#[derive(Debug, Clone, Copy)]
pub(crate) struct StateAwareCaseGenerator {
    rng: DeterministicRng,
}

impl StateAwareCaseGenerator {
    pub(crate) fn new(seed: u64) -> Self {
        Self {
            rng: DeterministicRng::new(seed),
        }
    }

    pub(crate) fn case(&mut self, index: usize) -> FuzzCase {
        let seed = self.rng.next_u64();
        let index_enabled = seed & 1 == 0;
        let graph = GeneratedGraphState::from_seed(seed);
        let query = graph.plan_differential_query(seed, index);
        let metamorphic = graph.metamorphic_case(index_enabled, &query.ast);
        let rewrite_kind = PredicateRewriteKind::for_case(index + index / crate::QUERY_SHAPE_COUNT);
        let graph_tlp = graph.graph_tlp_cases(seed, rewrite_kind);

        FuzzCase {
            seed,
            shape: query.name.clone(),
            mutations: graph.mutations(index_enabled),
            query: query.invocation,
            query_ast: query.ast,
            graph_tlp: graph_tlp.rows,
            graph_tlp_aggregate: graph_tlp.aggregate,
            graph_predicate_rewrite: graph_tlp.predicate_rewrite,
            metamorphic,
            index_enabled,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GeneratedGraphState {
    memory_count: usize,
}

impl GeneratedGraphState {
    fn from_seed(seed: u64) -> Self {
        Self {
            memory_count: 12 + ((seed >> 8) as usize % 5),
        }
    }

    fn mutations(self, index_enabled: bool) -> Vec<Mutation> {
        self.transformed_mutations(index_enabled, "", false)
    }

    fn transformed_mutations(
        self,
        index_enabled: bool,
        identifier_prefix: &str,
        reverse_relationships: bool,
    ) -> Vec<Mutation> {
        let mut mutations = Vec::new();
        for index in 0..self.memory_count {
            let kind = memory_kind(index);
            let optional_note = match index % 3 {
                0 => ", optional_note: null",
                1 => ", optional_note: 'present'",
                _ => "",
            };
            let optional_score = match index % 3 {
                0 => ", optional_score: null".to_string(),
                1 => format!(", optional_score: {index}"),
                _ => String::new(),
            };
            let id = format!("{identifier_prefix}mem-{index}");
            mutations.push(Mutation::new(format!(
                "CREATE (:Memory {{id: '{id}', kind: '{kind}', title: 'Memory {index}', importance: {index}{optional_note}{optional_score}}})"
            )));
        }
        for index in 0..ENTITY_COUNT {
            let id = format!("{identifier_prefix}entity-{index}");
            mutations.push(Mutation::new(format!(
                "CREATE (:Entity {{id: '{id}', name: 'Entity {index}'}})"
            )));
        }
        for index in 0..self.memory_count {
            let memory_id = format!("{identifier_prefix}mem-{index}");
            let entity_id = format!("{identifier_prefix}entity-{}", index % ENTITY_COUNT);
            let relationship = if reverse_relationships {
                format!("(e)-[:MENTIONS {{weight: {}}}]->(m)", index % 4)
            } else {
                format!("(m)-[:MENTIONS {{weight: {}}}]->(e)", index % 4)
            };
            let matched = if reverse_relationships {
                format!("(e:Entity {{id: '{entity_id}'}}), (m:Memory {{id: '{memory_id}'}})")
            } else {
                format!("(m:Memory {{id: '{memory_id}'}}), (e:Entity {{id: '{entity_id}'}})")
            };
            mutations.push(Mutation::new(format!(
                "MATCH {matched} CREATE {relationship}",
            )));
        }
        mutations.push(relationship_mutation(
            identifier_prefix,
            0,
            0,
            Some(0),
            reverse_relationships,
        ));
        for weight in [1, 2] {
            mutations.push(relationship_mutation(
                identifier_prefix,
                1,
                2,
                Some(weight),
                reverse_relationships,
            ));
        }
        mutations.push(relationship_mutation(
            identifier_prefix,
            2,
            3,
            Some(3),
            reverse_relationships,
        ));
        mutations.push(nullable_relationship_mutation(
            identifier_prefix,
            4,
            5,
            "weight: null",
            reverse_relationships,
        ));
        mutations.push(nullable_relationship_mutation(
            identifier_prefix,
            5,
            4,
            "",
            reverse_relationships,
        ));
        if index_enabled {
            mutations.push(Mutation::new("CREATE INDEX ON :Memory(id)"));
            mutations.push(Mutation::new("CREATE RANGE INDEX ON :Memory(importance)"));
        }
        mutations
    }

    fn metamorphic_case(self, index_enabled: bool, query: &QueryAst) -> MetamorphicCase {
        let isomorphic_query = query.map_identifier_parameters("iso-");
        let graph_isomorphism = MetamorphicRelation {
            name: "graph_isomorphism",
            applicability_guard: "identifier values are projected as scalar values only",
            mutations: self.transformed_mutations(index_enabled, "iso-", false),
            query: isomorphic_query.invocation(),
            identifier_prefix_to_strip: Some("iso-".to_string()),
        };
        let direction_reversal =
            query
                .reversed_directions()
                .map(|reversed_query| MetamorphicRelation {
                    name: "direction_reversal",
                    applicability_guard:
                        "query contains at least one directed relationship pattern",
                    mutations: self.transformed_mutations(index_enabled, "", true),
                    query: reversed_query.invocation(),
                    identifier_prefix_to_strip: None,
                });
        MetamorphicCase {
            graph_isomorphism,
            direction_reversal,
        }
    }

    fn plan_differential_query(self, seed: u64, index: usize) -> GeneratedQuery {
        let selected_memory = (seed as usize) % self.memory_count;
        let alternate_memory = ((seed >> 16) as usize) % self.memory_count;
        let kind = if seed & 2 == 0 { "note" } else { "thread" };
        let mut parameters = Parameters::new();
        let memory = || MatchPattern::Node(NodePattern::new("m", "Memory"));
        let entity = || MatchPattern::Node(NodePattern::new("e", "Entity"));

        let (name, ast) = match index % crate::QUERY_SHAPE_COUNT {
            0 => (
                "node_scan",
                QueryAst {
                    matches: vec![memory()],
                    predicate: None,
                    returns: vec![
                        ReturnItem::property("m", "id", "id"),
                        ReturnItem::property("m", "kind", "kind"),
                    ],
                    distinct: false,
                    order_by: vec![OrderItem::ascending("id")],
                    limit: None,
                    parameters,
                },
            ),
            1 => {
                parameters.insert("kind".to_string(), Value::String(kind.to_string()));
                (
                    "equality_filter",
                    QueryAst {
                        matches: vec![memory()],
                        predicate: Some(QueryPredicate::Equal(
                            PropertyExpression::new("m", "kind"),
                            "kind",
                        )),
                        returns: vec![ReturnItem::property("m", "id", "id")],
                        distinct: false,
                        order_by: Vec::new(),
                        limit: None,
                        parameters,
                    },
                )
            }
            2 => {
                parameters.insert(
                    "ids".to_string(),
                    Value::List(vec![
                        memory_id(selected_memory),
                        memory_id(alternate_memory),
                        memory_id(selected_memory),
                    ]),
                );
                (
                    "in_filter",
                    QueryAst {
                        matches: vec![memory()],
                        predicate: Some(QueryPredicate::In(
                            PropertyExpression::new("m", "id"),
                            "ids",
                        )),
                        returns: vec![ReturnItem::property("m", "id", "id")],
                        distinct: false,
                        order_by: vec![OrderItem::ascending("id")],
                        limit: None,
                        parameters,
                    },
                )
            }
            3 => {
                parameters.insert(
                    "minimum".to_string(),
                    Value::Int((seed % self.memory_count as u64) as i64),
                );
                (
                    "range_filter",
                    QueryAst {
                        matches: vec![memory()],
                        predicate: Some(QueryPredicate::GreaterThanOrEqual(
                            PropertyExpression::new("m", "importance"),
                            "minimum",
                        )),
                        returns: vec![
                            ReturnItem::property("m", "id", "id"),
                            ReturnItem::property("m", "importance", "importance"),
                        ],
                        distinct: false,
                        order_by: vec![
                            OrderItem::ascending("importance"),
                            OrderItem::ascending("id"),
                        ],
                        limit: None,
                        parameters,
                    },
                )
            }
            4 => {
                parameters.insert("id".to_string(), memory_id(selected_memory));
                (
                    "one_hop_expand",
                    QueryAst {
                        matches: vec![MatchPattern::Relationship {
                            source: NodePattern::new("m", "Memory")
                                .with_property_parameter("id", "id"),
                            relationship_variable: "r",
                            relationship_type: "MENTIONS",
                            direction: PatternDirection::Outgoing,
                            target: NodePattern::new("e", "Entity"),
                        }],
                        predicate: None,
                        returns: vec![
                            ReturnItem::property("m", "id", "memory_id"),
                            ReturnItem::property("e", "id", "entity_id"),
                        ],
                        distinct: false,
                        order_by: vec![OrderItem::ascending("entity_id")],
                        limit: None,
                        parameters,
                    },
                )
            }
            5 => (
                "self_loop",
                QueryAst {
                    matches: vec![MatchPattern::Relationship {
                        source: NodePattern::new("e", "Entity"),
                        relationship_variable: "r",
                        relationship_type: "RELATES_TO",
                        direction: PatternDirection::Outgoing,
                        target: NodePattern::new("e", "Entity"),
                    }],
                    predicate: None,
                    returns: vec![ReturnItem::property("e", "id", "id")],
                    distinct: false,
                    order_by: Vec::new(),
                    limit: None,
                    parameters,
                },
            ),
            6 => {
                parameters.insert("source".to_string(), entity_id(1));
                parameters.insert("target".to_string(), entity_id(2));
                (
                    "parallel_edges",
                    QueryAst {
                        matches: vec![MatchPattern::Relationship {
                            source: NodePattern::new("a", "Entity")
                                .with_property_parameter("id", "source"),
                            relationship_variable: "r",
                            relationship_type: "RELATES_TO",
                            direction: PatternDirection::Outgoing,
                            target: NodePattern::new("b", "Entity")
                                .with_property_parameter("id", "target"),
                        }],
                        predicate: None,
                        returns: vec![
                            ReturnItem::property("a", "id", "source"),
                            ReturnItem::property("b", "id", "target"),
                        ],
                        distinct: false,
                        order_by: Vec::new(),
                        limit: None,
                        parameters,
                    },
                )
            }
            7 => (
                "cartesian_product",
                QueryAst {
                    matches: vec![memory(), entity()],
                    predicate: None,
                    returns: vec![
                        ReturnItem::property("m", "id", "memory_id"),
                        ReturnItem::property("e", "id", "entity_id"),
                    ],
                    distinct: false,
                    order_by: Vec::new(),
                    limit: None,
                    parameters,
                },
            ),
            8 => (
                "distinct_projection",
                QueryAst {
                    matches: vec![memory()],
                    predicate: None,
                    returns: vec![ReturnItem::property("m", "kind", "kind")],
                    distinct: true,
                    order_by: vec![OrderItem::ascending("kind")],
                    limit: None,
                    parameters,
                },
            ),
            9 => (
                "aggregate",
                QueryAst {
                    matches: vec![memory()],
                    predicate: None,
                    returns: vec![
                        ReturnItem::property("m", "kind", "kind"),
                        ReturnItem::count("m", "count"),
                    ],
                    distinct: false,
                    order_by: vec![OrderItem::ascending("kind")],
                    limit: None,
                    parameters,
                },
            ),
            10 => (
                "top_n",
                QueryAst {
                    matches: vec![memory()],
                    predicate: None,
                    returns: vec![
                        ReturnItem::property("m", "id", "id"),
                        ReturnItem::property("m", "importance", "importance"),
                    ],
                    distinct: false,
                    order_by: vec![
                        OrderItem::descending("importance"),
                        OrderItem::ascending("id"),
                    ],
                    limit: Some(5),
                    parameters,
                },
            ),
            _ => (
                "missing_or_null",
                QueryAst {
                    matches: vec![memory()],
                    predicate: Some(QueryPredicate::IsNull(PropertyExpression::new(
                        "m",
                        if seed & 4 == 0 {
                            "optional_note"
                        } else {
                            "optional_score"
                        },
                    ))),
                    returns: vec![ReturnItem::property("m", "id", "id")],
                    distinct: false,
                    order_by: vec![OrderItem::ascending("id")],
                    limit: None,
                    parameters,
                },
            ),
        };

        let schema = GeneratedSchema::nowledge_fixture();
        ast.validate(&schema)
            .unwrap_or_else(|error| panic!("generated schema-invalid query AST: {error}"));
        let invocation = ast.invocation();

        GeneratedQuery {
            name: name.to_string(),
            ast,
            invocation,
        }
    }

    fn graph_tlp_cases(
        self,
        seed: u64,
        rewrite_kind: PredicateRewriteKind,
    ) -> GeneratedGraphTlpCases {
        match (seed >> 3) % 3 {
            0 => GraphTlpBuilder::new(
                "nullable_node_property",
                "MATCH (m:Memory)",
                GeneratedPredicate::compare(
                    PropertyRef::new("m", "optional_note"),
                    ComparisonOperator::Equal,
                    "tlp_value",
                ),
                "m.id AS id, m.optional_note AS optional_note",
                "m",
            )
            .with_parameter("tlp_value", Value::String("present".to_string()))
            .build(rewrite_kind),
            1 => GraphTlpBuilder::new(
                "node_range",
                "MATCH (m:Memory)",
                GeneratedPredicate::compare(
                    PropertyRef::new("m", "optional_score"),
                    ComparisonOperator::GreaterThanOrEqual,
                    "tlp_value",
                ),
                "m.id AS id, m.optional_score AS optional_score",
                "m",
            )
            .with_parameter(
                "tlp_value",
                Value::Int((seed % self.memory_count as u64) as i64),
            )
            .build(rewrite_kind),
            _ => GraphTlpBuilder::new(
                "relationship_range",
                "MATCH (a:Entity)-[r:RELATES_TO]->(b:Entity)",
                GeneratedPredicate::compare(
                    PropertyRef::new("r", "weight"),
                    ComparisonOperator::GreaterThanOrEqual,
                    "tlp_value",
                ),
                "a.id AS source, b.id AS target, r.weight AS weight",
                "r",
            )
            .with_parameter("tlp_value", Value::Int((seed % 4) as i64))
            .build(rewrite_kind),
        }
    }
}

#[derive(Debug)]
struct GeneratedQuery {
    name: String,
    ast: QueryAst,
    invocation: QueryInvocation,
}

#[derive(Debug)]
struct GeneratedGraphTlpCases {
    rows: GraphTlpCase,
    aggregate: GraphTlpCase,
    predicate_rewrite: GraphPredicateRewriteCase,
}

#[derive(Debug)]
struct GraphTlpBuilder {
    name: &'static str,
    match_clause: &'static str,
    predicate: GeneratedPredicate,
    projection: &'static str,
    count_variable: &'static str,
    parameters: Parameters,
}

impl GraphTlpBuilder {
    fn new(
        name: &'static str,
        match_clause: &'static str,
        predicate: GeneratedPredicate,
        projection: &'static str,
        count_variable: &'static str,
    ) -> Self {
        Self {
            name,
            match_clause,
            predicate,
            projection,
            count_variable,
            parameters: Parameters::new(),
        }
    }

    fn with_parameter(mut self, name: &str, value: Value) -> Self {
        self.parameters.insert(name.to_string(), value);
        self
    }

    fn build(self, rewrite_kind: PredicateRewriteKind) -> GeneratedGraphTlpCases {
        let query = |projection: &str, predicate: Option<&GeneratedPredicate>| QueryInvocation {
            cypher: match predicate {
                Some(predicate) => format!(
                    "{} WHERE {} RETURN {projection}",
                    self.match_clause,
                    predicate.render(),
                ),
                None => format!("{} RETURN {projection}", self.match_clause),
            },
            parameters: self.parameters.clone(),
            result_semantics: ResultSemantics::Bag,
        };
        let build_case = |name: String, projection: String| GraphTlpCase {
            name,
            original: query(&projection, None),
            predicate_true: query(&projection, Some(&self.predicate)),
            predicate_false: query(&projection, Some(&self.predicate.clone().negated())),
            predicate_null: query(
                &projection,
                Some(&GeneratedPredicate::IsNull(self.predicate.property())),
            ),
        };

        let predicate = self.predicate.clone();
        let (original, rewritten) = match rewrite_kind {
            PredicateRewriteKind::DoubleNegation => (
                query(self.projection, Some(&predicate)),
                query(
                    self.projection,
                    Some(&predicate.clone().negated().negated()),
                ),
            ),
            PredicateRewriteKind::ConjunctionIdempotence => (
                query(self.projection, Some(&predicate)),
                query(
                    self.projection,
                    Some(&GeneratedPredicate::And(
                        Box::new(predicate.clone()),
                        Box::new(predicate),
                    )),
                ),
            ),
            PredicateRewriteKind::DisjunctionIdempotence => (
                query(self.projection, Some(&predicate)),
                query(
                    self.projection,
                    Some(&GeneratedPredicate::Or(
                        Box::new(predicate.clone()),
                        Box::new(predicate),
                    )),
                ),
            ),
            PredicateRewriteKind::NullTotality => {
                let nullable_property = predicate.property();
                let rewritten = GeneratedPredicate::Or(
                    Box::new(GeneratedPredicate::Or(
                        Box::new(predicate.clone()),
                        Box::new(predicate.negated()),
                    )),
                    Box::new(GeneratedPredicate::IsNull(nullable_property)),
                );
                (
                    query(self.projection, None),
                    query(self.projection, Some(&rewritten)),
                )
            }
            PredicateRewriteKind::ConjunctionAbsorption => (
                query(self.projection, Some(&predicate)),
                query(
                    self.projection,
                    Some(&GeneratedPredicate::And(
                        Box::new(predicate.clone()),
                        Box::new(GeneratedPredicate::Or(
                            Box::new(predicate.clone()),
                            Box::new(GeneratedPredicate::IsNull(predicate.property())),
                        )),
                    )),
                ),
            ),
            PredicateRewriteKind::DisjunctionAbsorption => (
                query(self.projection, Some(&predicate)),
                query(
                    self.projection,
                    Some(&GeneratedPredicate::Or(
                        Box::new(predicate.clone()),
                        Box::new(GeneratedPredicate::And(
                            Box::new(predicate.clone()),
                            Box::new(GeneratedPredicate::IsNull(predicate.property())),
                        )),
                    )),
                ),
            ),
        };

        GeneratedGraphTlpCases {
            rows: build_case(self.name.to_string(), self.projection.to_string()),
            aggregate: build_case(
                format!("{}_count", self.name),
                format!("count({}) AS count", self.count_variable),
            ),
            predicate_rewrite: GraphPredicateRewriteCase {
                name: rewrite_kind.as_str().to_string(),
                original,
                rewritten,
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct PropertyRef {
    variable: &'static str,
    property: &'static str,
}

impl PropertyRef {
    const fn new(variable: &'static str, property: &'static str) -> Self {
        Self { variable, property }
    }

    fn render(self) -> String {
        format!("{}.{}", self.variable, self.property)
    }
}

#[derive(Debug, Clone, Copy)]
enum ComparisonOperator {
    Equal,
    GreaterThanOrEqual,
}

impl ComparisonOperator {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::GreaterThanOrEqual => ">=",
        }
    }
}

#[derive(Debug, Clone)]
enum GeneratedPredicate {
    Compare {
        property: PropertyRef,
        operator: ComparisonOperator,
        parameter: &'static str,
    },
    IsNull(PropertyRef),
    Not(Box<Self>),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
}

impl GeneratedPredicate {
    const fn compare(
        property: PropertyRef,
        operator: ComparisonOperator,
        parameter: &'static str,
    ) -> Self {
        Self::Compare {
            property,
            operator,
            parameter,
        }
    }

    fn property(&self) -> PropertyRef {
        match self {
            Self::Compare { property, .. } | Self::IsNull(property) => *property,
            Self::Not(predicate) => predicate.property(),
            Self::And(left, _) | Self::Or(left, _) => left.property(),
        }
    }

    fn negated(self) -> Self {
        Self::Not(Box::new(self))
    }

    fn render(&self) -> String {
        match self {
            Self::Compare {
                property,
                operator,
                parameter,
            } => format!("{} {} ${parameter}", property.render(), operator.as_str()),
            Self::IsNull(property) => format!("{} IS NULL", property.render()),
            Self::Not(predicate) => format!("NOT ({})", predicate.render()),
            Self::And(left, right) => format!("({}) AND ({})", left.render(), right.render()),
            Self::Or(left, right) => format!("({}) OR ({})", left.render(), right.render()),
        }
    }
}

fn relationship_mutation(
    identifier_prefix: &str,
    source: usize,
    target: usize,
    weight: Option<usize>,
    reverse: bool,
) -> Mutation {
    let weight = weight
        .map(|weight| format!(" {{weight: {weight}}}"))
        .unwrap_or_default();
    nullable_relationship_mutation(identifier_prefix, source, target, weight.trim(), reverse)
}

fn nullable_relationship_mutation(
    identifier_prefix: &str,
    source: usize,
    target: usize,
    properties: &str,
    reverse: bool,
) -> Mutation {
    let source_id = format!("{identifier_prefix}entity-{source}");
    let target_id = format!("{identifier_prefix}entity-{target}");
    let properties = if properties.is_empty() {
        String::new()
    } else if properties.starts_with('{') {
        format!(" {properties}")
    } else {
        format!(" {{{properties}}}")
    };
    let relationship = if reverse {
        format!("(b)-[:RELATES_TO{properties}]->(a)")
    } else {
        format!("(a)-[:RELATES_TO{properties}]->(b)")
    };
    let matched = if reverse {
        format!("(b:Entity {{id: '{target_id}'}}), (a:Entity {{id: '{source_id}'}})")
    } else {
        format!("(a:Entity {{id: '{source_id}'}}), (b:Entity {{id: '{target_id}'}})")
    };
    Mutation::new(format!("MATCH {matched} CREATE {relationship}"))
}

fn memory_kind(index: usize) -> &'static str {
    if index.is_multiple_of(2) {
        "note"
    } else {
        "thread"
    }
}

fn memory_id(index: usize) -> Value {
    Value::String(format!("mem-{index}"))
}

fn entity_id(index: usize) -> Value {
    Value::String(format!("entity-{index}"))
}

#[derive(Debug, Clone, Copy)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.state
    }
}
