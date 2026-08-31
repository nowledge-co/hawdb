//! Mutation transaction commit paths and out-of-core delta admission for [`GraphStore`].

use super::*;
use skein_storage::RelationalError;

impl GraphStore {
    pub fn commit_mutations(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
    ) -> Result<MutationSummary> {
        self.commit_mutations_with_limits(catalog, mutations, MutationLimits::default())
    }

    pub(crate) fn begin_mutation_transaction(&self, catalog: &Catalog) -> GraphMutationTransaction {
        GraphMutationTransaction {
            base_commit_epoch: self.commit_epoch,
            catalog: catalog.clone(),
            store: self.snapshot(),
            ops: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn commit_mutation_transaction_and_relational(
        &mut self,
        catalog: &mut Catalog,
        transaction: GraphMutationTransaction,
        relational_transaction: RelationalTransaction,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutation_transaction_and_relational_internal(
            catalog,
            transaction,
            relational_transaction,
            AppendTransaction::default(),
            limits,
            false,
        )
    }

    pub(crate) fn commit_mutation_transaction_relational_and_append(
        &mut self,
        catalog: &mut Catalog,
        transaction: GraphMutationTransaction,
        relational_transaction: RelationalTransaction,
        append_transaction: AppendTransaction,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutation_transaction_and_relational_internal(
            catalog,
            transaction,
            relational_transaction,
            append_transaction,
            limits,
            false,
        )
    }

    pub(crate) fn commit_rebased_mutation_transaction_relational_and_append(
        &mut self,
        catalog: &mut Catalog,
        transaction: GraphMutationTransaction,
        relational_transaction: RelationalTransaction,
        append_transaction: AppendTransaction,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutation_transaction_and_relational_internal(
            catalog,
            transaction,
            relational_transaction,
            append_transaction,
            limits,
            true,
        )
    }

    fn commit_mutation_transaction_and_relational_internal(
        &mut self,
        catalog: &mut Catalog,
        transaction: GraphMutationTransaction,
        relational_transaction: RelationalTransaction,
        append_transaction: AppendTransaction,
        limits: MutationLimits,
        allow_stale_rebase: bool,
    ) -> Result<MutationSummary> {
        let read_only = transaction.ops.is_empty()
            && relational_transaction.writes.is_empty()
            && append_transaction.writes.is_empty();
        if !read_only && !allow_stale_rebase && self.commit_epoch != transaction.base_commit_epoch {
            return Err(SkeinError::Execution(format!(
                "transaction snapshot is stale: started at commit epoch {}, current epoch is {}",
                transaction.base_commit_epoch, self.commit_epoch
            )));
        }
        let ops = compact_transaction_graph_ops(transaction.ops);
        self.commit_prepared_mutation_ops(
            catalog,
            transaction.catalog,
            ops,
            transaction.rows,
            limits,
            MutationCommitOptions {
                relational: Some(relational_transaction),
                append: Some(append_transaction),
                ..MutationCommitOptions::default()
            },
        )
    }

    pub fn commit_mutation_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutation: GraphMutation,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutations_internal(
            catalog,
            vec![mutation],
            limits,
            MutationCommitOptions {
                preserve_single_create_wal: true,
                ..MutationCommitOptions::default()
            },
        )
    }

    pub fn commit_mutations_with_limits(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutations_internal(catalog, mutations, limits, MutationCommitOptions::default())
    }

    pub(crate) fn commit_relational_transaction(
        &mut self,
        catalog: &mut Catalog,
        transaction: RelationalTransaction,
    ) -> Result<MutationSummary> {
        self.commit_mutations_and_relational(
            catalog,
            Vec::new(),
            transaction,
            MutationLimits::default(),
        )
    }

    pub(crate) fn commit_mutations_and_relational(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
        transaction: RelationalTransaction,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutations_internal(
            catalog,
            mutations,
            limits,
            MutationCommitOptions {
                relational: Some(transaction),
                ..MutationCommitOptions::default()
            },
        )
    }

    pub fn commit_kernel_write_batch(
        &mut self,
        catalog: &mut Catalog,
        batch: KernelWriteBatch,
        limits: MutationLimits,
    ) -> Result<MutationSummary> {
        self.commit_mutations_internal(
            catalog,
            batch.graph,
            limits,
            MutationCommitOptions {
                relational: Some(batch.relational),
                append: Some(batch.append),
                ..MutationCommitOptions::default()
            },
        )
    }

    pub(super) fn commit_mutations_internal(
        &mut self,
        catalog: &mut Catalog,
        mutations: Vec<GraphMutation>,
        limits: MutationLimits,
        options: MutationCommitOptions<'_>,
    ) -> Result<MutationSummary> {
        let mut next_node_id = self.next_node_id;
        let mut next_rel_id = self.next_rel_id;
        let mut working_catalog = catalog.clone();
        let mut ops = Vec::new();
        let mut rows = Vec::new();
        let mut pending_nodes = Vec::new();
        let mut pending_relationships = Vec::new();

        for mutation in mutations {
            match mutation {
                GraphMutation::CreateNodeLabel { label } => {
                    if let Some(id) = working_catalog.label_id(&label) {
                        rows.push(BTreeMap::from([
                            ("label_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateNodeLabel {
                            label: label.clone(),
                        });
                        let id = working_catalog.get_or_create_label(&label);
                        rows.push(BTreeMap::from([
                            ("label_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipType { rel_type } => {
                    if let Some(id) = working_catalog.rel_type_id(&rel_type) {
                        rows.push(BTreeMap::from([
                            ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRelationshipType {
                            rel_type: rel_type.clone(),
                        });
                        let id = working_catalog.get_or_create_rel_type(&rel_type);
                        rows.push(BTreeMap::from([
                            ("rel_type_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNodeTable { name } => {
                    if let Some(id) = working_catalog.table_id(TableKind::Node, &name) {
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateNodeTable { name: name.clone() });
                        working_catalog.get_or_create_label(&name);
                        let id = working_catalog.get_or_create_table(TableKind::Node, &name);
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipTable { name } => {
                    if let Some(id) = working_catalog.table_id(TableKind::Relationship, &name) {
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRelationshipTable { name: name.clone() });
                        working_catalog.get_or_create_rel_type(&name);
                        let id =
                            working_catalog.get_or_create_table(TableKind::Relationship, &name);
                        rows.push(BTreeMap::from([
                            ("table_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateProperty {
                    table_kind,
                    table,
                    property,
                    value_type,
                    nullable,
                } => {
                    let table_id =
                        ensure_table_descriptor(&mut working_catalog, table_kind, &table);
                    if let Some(id) = working_catalog.property_descriptor_id(table_id, &property) {
                        rows.push(BTreeMap::from([
                            ("property_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        validate_property_descriptor(
                            &working_catalog,
                            self,
                            table_id,
                            &property,
                            value_type,
                            nullable,
                        )?;
                        ops.push(WalOp::CreateProperty {
                            table_kind,
                            table: table.clone(),
                            property: property.clone(),
                            value_type,
                            nullable,
                        });
                        let id = working_catalog
                            .get_or_create_property(table_id, &property, value_type, nullable);
                        rows.push(BTreeMap::from([
                            ("property_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::AlterTableState {
                    table_kind,
                    table,
                    state,
                } => {
                    let Some(id) = working_catalog.table_id(table_kind, &table) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let Some(descriptor) = working_catalog.table_descriptor(id) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let changed = descriptor.state != state;
                    if changed {
                        ops.push(WalOp::AlterTableState {
                            table_kind,
                            table: table.clone(),
                            state,
                        });
                        working_catalog.set_table_state(id, state);
                    }
                    rows.push(BTreeMap::from([
                        ("table_id".to_string(), Value::Int(id.0 as i64)),
                        ("changed".to_string(), Value::Bool(changed)),
                    ]));
                }
                GraphMutation::AlterPropertyState {
                    table_kind,
                    table,
                    property,
                    state,
                } => {
                    let Some(table_id) = working_catalog.table_id(table_kind, &table) else {
                        return Err(SkeinError::Storage(format!(
                            "schema table '{table}' does not exist"
                        )));
                    };
                    let Some(id) = working_catalog.property_descriptor_id(table_id, &property)
                    else {
                        return Err(SkeinError::Storage(format!(
                            "schema property '{table}.{property}' does not exist"
                        )));
                    };
                    let Some(descriptor) = working_catalog.property_descriptor(id) else {
                        return Err(SkeinError::Storage(format!(
                            "schema property '{table}.{property}' does not exist"
                        )));
                    };
                    let changed = descriptor.state != state;
                    if changed {
                        if state == SchemaObjectState::Public {
                            validate_property_descriptor(
                                &working_catalog,
                                self,
                                table_id,
                                &property,
                                descriptor.value_type,
                                descriptor.nullable,
                            )?;
                        }
                        ops.push(WalOp::AlterPropertyState {
                            table_kind,
                            table: table.clone(),
                            property: property.clone(),
                            state,
                        });
                        working_catalog.set_property_state(id, state);
                    }
                    rows.push(BTreeMap::from([
                        ("property_id".to_string(), Value::Int(id.0 as i64)),
                        ("changed".to_string(), Value::Bool(changed)),
                    ]));
                }
                GraphMutation::CreateIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id(label_id, &property) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateCompositeIndex { label, properties } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) =
                        working_catalog.composite_property_index_id(label_id, &properties)
                    {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateCompositeIndex {
                            label: label.clone(),
                            properties: properties.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_composite_property_index(label_id, &properties);
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRangeIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id_with_kind(
                        label_id,
                        &property,
                        IndexKind::Range,
                    ) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateRangeIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index_with_kind(
                            label_id,
                            &property,
                            IndexKind::Range,
                        );
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateFullTextIndex { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.property_index_id_with_kind(
                        label_id,
                        &property,
                        IndexKind::FullText,
                    ) {
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        ops.push(WalOp::CreateFullTextIndex {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog.get_or_create_property_index_with_kind(
                            label_id,
                            &property,
                            IndexKind::FullText,
                        );
                        rows.push(BTreeMap::from([
                            ("index_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateUniqueConstraint { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) = working_catalog.unique_constraint_id(label_id, &property) {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_unique_constraint(&working_catalog, label_id, &property)?;
                        ops.push(WalOp::CreateUniqueConstraint {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id =
                            working_catalog.get_or_create_unique_constraint(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNodePropertyExistsConstraint { label, property } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    if let Some(id) =
                        working_catalog.node_property_exists_constraint_id(label_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_node_property_exists_constraint(
                            &working_catalog,
                            label_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateNodePropertyExistsConstraint {
                            label: label.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_node_property_exists_constraint(label_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipUniqueConstraint { rel_type, property } => {
                    let rel_type_id = working_catalog.get_or_create_rel_type(&rel_type);
                    if let Some(id) =
                        working_catalog.relationship_unique_constraint_id(rel_type_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_relationship_unique_constraint(
                            &working_catalog,
                            rel_type_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateRelationshipUniqueConstraint {
                            rel_type: rel_type.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_relationship_unique_constraint(rel_type_id, &property);
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateRelationshipPropertyExistsConstraint {
                    rel_type,
                    property,
                } => {
                    let rel_type_id = working_catalog.get_or_create_rel_type(&rel_type);
                    if let Some(id) = working_catalog
                        .relationship_property_exists_constraint_id(rel_type_id, &property)
                    {
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        self.validate_relationship_property_exists_constraint(
                            &working_catalog,
                            rel_type_id,
                            &property,
                        )?;
                        ops.push(WalOp::CreateRelationshipPropertyExistsConstraint {
                            rel_type: rel_type.clone(),
                            property: property.clone(),
                        });
                        let id = working_catalog
                            .get_or_create_relationship_property_exists_constraint(
                                rel_type_id,
                                &property,
                            );
                        rows.push(BTreeMap::from([
                            ("constraint_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateNode { label, properties } => {
                    let id = NodeId(next_node_id);
                    next_node_id += 1;
                    let label_id = working_catalog.get_or_create_label(&label);
                    ops.push(WalOp::CreateNode {
                        id,
                        label,
                        properties: properties.clone(),
                    });
                    pending_nodes.push((id, label_id, properties));
                    rows.push(BTreeMap::from([(
                        "node_id".to_string(),
                        Value::Int(id.0 as i64),
                    )]));
                }
                GraphMutation::MergeNode {
                    label,
                    match_properties,
                    on_create_properties,
                    on_match_assignments,
                    post_merge_assignments,
                } => {
                    let label_id = working_catalog.get_or_create_label(&label);
                    let current =
                        self.find_node_by_label_and_properties(label_id, &match_properties)?;
                    let pending = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == label_id
                                && properties_contain_all(pending_properties, &match_properties)
                        })
                        .map(|(id, _, _)| *id);
                    if let Some(id) = current {
                        if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                            let mut assignments = Vec::with_capacity(
                                on_match_assignments.len() + post_merge_assignments.len(),
                            );
                            assignments.extend(on_match_assignments);
                            assignments.extend(post_merge_assignments);
                            let set_ops = self.node_set_property_ops(&[id], &assignments)?;
                            ops.extend(set_ops);
                        }
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else if let Some(id) = pending {
                        if !on_match_assignments.is_empty() || !post_merge_assignments.is_empty() {
                            let mut assignments = Vec::with_capacity(
                                on_match_assignments.len() + post_merge_assignments.len(),
                            );
                            assignments.extend(on_match_assignments);
                            assignments.extend(post_merge_assignments);
                            Self::apply_pending_node_assignments(
                                &mut ops,
                                &mut pending_nodes,
                                id,
                                &assignments,
                            )?;
                        }
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(false)),
                        ]));
                    } else {
                        let mut properties = match_properties;
                        for (property, value) in on_create_properties {
                            properties.insert(property, value);
                        }
                        apply_node_assignments_to_properties(
                            &mut properties,
                            &post_merge_assignments,
                        )?;
                        let id = NodeId(next_node_id);
                        next_node_id += 1;
                        ops.push(WalOp::CreateNode {
                            id,
                            label,
                            properties: properties.clone(),
                        });
                        pending_nodes.push((id, label_id, properties));
                        rows.push(BTreeMap::from([
                            ("node_id".to_string(), Value::Int(id.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::MergeConnectedNodes(request) => {
                    let source_label_id =
                        working_catalog.get_or_create_label(&request.source_label);
                    let target_label_id =
                        working_catalog.get_or_create_label(&request.target_label);
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let current_source = self.find_node_by_label_and_properties(
                        source_label_id,
                        &request.source_properties,
                    )?;
                    let current_target = self.find_node_by_label_and_properties(
                        target_label_id,
                        &request.target_properties,
                    )?;
                    let pending_source = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == source_label_id
                                && pending_properties == &request.source_properties
                        })
                        .map(|(id, _, _)| *id);
                    let pending_target = pending_nodes
                        .iter()
                        .find(|(_, pending_label, pending_properties)| {
                            *pending_label == target_label_id
                                && pending_properties == &request.target_properties
                        })
                        .map(|(id, _, _)| *id);
                    let source = current_source.or(pending_source);
                    let target = current_target.or(pending_target);
                    if let (Some(source), Some(target)) = (source, target) {
                        let current_relationship = self.find_relationship_by_properties(
                            source,
                            target,
                            rel_type_id,
                            &request.rel_properties,
                        )?;
                        let pending_relationship = pending_relationships
                            .iter()
                            .find(
                                |(_, pending_source, pending_target, pending_type, properties)| {
                                    *pending_source == source
                                        && *pending_target == target
                                        && *pending_type == rel_type_id
                                        && properties == &request.rel_properties
                                },
                            )
                            .map(|(id, _, _, _, _)| *id);
                        if let Some(relationship) = current_relationship.or(pending_relationship) {
                            rows.push(merge_relationship_row(source, relationship, target, false));
                            continue;
                        }
                    }

                    let source_created = source.is_none();
                    let source = source.unwrap_or(NodeId(next_node_id));
                    if source.0 == next_node_id {
                        next_node_id += 1;
                        pending_nodes.push((
                            source,
                            source_label_id,
                            request.source_properties.clone(),
                        ));
                    }
                    let target_created = target.is_none();
                    let target = target.unwrap_or(NodeId(next_node_id));
                    if target.0 == next_node_id {
                        next_node_id += 1;
                        pending_nodes.push((
                            target,
                            target_label_id,
                            request.target_properties.clone(),
                        ));
                    }
                    let relationship = RelId(next_rel_id);
                    next_rel_id += 1;
                    if source_created {
                        ops.push(WalOp::CreateNode {
                            id: source,
                            label: request.source_label.clone(),
                            properties: request.source_properties.clone(),
                        });
                    }
                    if target_created {
                        ops.push(WalOp::CreateNode {
                            id: target,
                            label: request.target_label.clone(),
                            properties: request.target_properties.clone(),
                        });
                    }
                    ops.push(WalOp::CreateRelationship {
                        id: relationship,
                        source,
                        target,
                        rel_type: request.rel_type.clone(),
                        properties: request.rel_properties.clone(),
                    });
                    pending_relationships.push((
                        relationship,
                        source,
                        target,
                        rel_type_id,
                        request.rel_properties.clone(),
                    ));
                    rows.push(merge_relationship_row(source, relationship, target, true));
                }
                GraphMutation::SetNodeProperty {
                    label,
                    filter,
                    property,
                    value,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let assignment = [NodeSetAssignment {
                            property: property.clone(),
                            value: NodeSetValue::Value(value.clone()),
                        }];
                        self.apply_set_node_properties_mutation(
                            &mut ops,
                            &mut rows,
                            &mut pending_nodes,
                            label_id,
                            filter.as_ref(),
                            &assignment,
                            limits,
                        )?;
                    }
                }
                GraphMutation::SetNodePropertyAddInt {
                    label,
                    filter,
                    property,
                    amount,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let assignment = [NodeSetAssignment {
                            property: property.clone(),
                            value: NodeSetValue::AddInt { amount },
                        }];
                        self.apply_set_node_properties_mutation(
                            &mut ops,
                            &mut rows,
                            &mut pending_nodes,
                            label_id,
                            filter.as_ref(),
                            &assignment,
                            limits,
                        )?;
                    }
                }
                GraphMutation::SetNodeProperties {
                    label,
                    filter,
                    assignments,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        self.apply_set_node_properties_mutation(
                            &mut ops,
                            &mut rows,
                            &mut pending_nodes,
                            label_id,
                            filter.as_ref(),
                            &assignments,
                            limits,
                        )?;
                    }
                }
                GraphMutation::SetRelationshipProperty {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                    property,
                    value,
                } => {
                    let assignments = vec![RelationshipSetAssignment { property, value }];
                    apply_set_relationship_properties_mutation(
                        self,
                        &working_catalog,
                        &mut ops,
                        &mut rows,
                        &pending_nodes,
                        &mut pending_relationships,
                        RelationshipPropertiesUpdate {
                            source_label,
                            filter,
                            rel_type,
                            target_label,
                            target_filter,
                            rel_filter,
                            assignments,
                        },
                        limits,
                    )?;
                }
                GraphMutation::SetRelationshipProperties {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                    assignments,
                } => {
                    apply_set_relationship_properties_mutation(
                        self,
                        &working_catalog,
                        &mut ops,
                        &mut rows,
                        &pending_nodes,
                        &mut pending_relationships,
                        RelationshipPropertiesUpdate {
                            source_label,
                            filter,
                            rel_type,
                            target_label,
                            target_filter,
                            rel_filter,
                            assignments,
                        },
                        limits,
                    )?;
                }
                GraphMutation::DeleteNode {
                    label,
                    filter,
                    detach,
                } => {
                    let label_id = optional_label_id(&working_catalog, &label);
                    if label.is_empty() || label_id.is_some() {
                        let committed_ids = self.matching_node_ids_bounded(
                            label_id,
                            filter.as_ref(),
                            remaining_mutation_affected_rows(rows.len(), limits)?,
                            "max_mutation_affected_rows",
                        )?;
                        let pending_ids = Self::pending_node_ids_matching(
                            label_id,
                            filter.as_ref(),
                            &pending_nodes,
                        );
                        let mut delete_ids = committed_ids.clone();
                        delete_ids.extend(pending_ids.iter().copied());
                        let incident_pending_relationship_ids =
                            pending_relationship_ids_for_nodes(&pending_relationships, &delete_ids);
                        if !detach && !incident_pending_relationship_ids.is_empty() {
                            let id = delete_ids.first().copied().unwrap_or(NodeId(0));
                            return Err(SkeinError::Storage(format!(
                                "node {} has relationships; use DETACH DELETE",
                                id.0
                            )));
                        }
                        ensure_additional_mutation_limits(
                            ops.len(),
                            rows.len(),
                            0,
                            delete_ids.len(),
                            limits,
                        )?;
                        let delete_ops = self.delete_node_ops_bounded(
                            &committed_ids,
                            detach,
                            remaining_mutation_operations(ops.len(), limits)?,
                        )?;
                        if detach {
                            for relationship_id in incident_pending_relationship_ids {
                                remove_pending_relationship(
                                    &mut ops,
                                    &mut pending_relationships,
                                    relationship_id,
                                );
                            }
                        }
                        for id in &pending_ids {
                            remove_pending_node(&mut ops, &mut pending_nodes, *id);
                        }
                        for id in delete_ids {
                            rows.push(BTreeMap::from([(
                                "node_id".to_string(),
                                Value::Int(id.0 as i64),
                            )]));
                        }
                        ops.extend(delete_ops);
                    }
                }
                GraphMutation::DeleteRelationship {
                    source_label,
                    filter,
                    rel_type,
                    target_label,
                    target_filter,
                    rel_filter,
                } => {
                    if let (Some(source_label_id), Some(target_label_id), Some(rel_type_id)) = (
                        working_catalog.label_id(&source_label),
                        working_catalog.label_id(&target_label),
                        working_catalog.rel_type_id(&rel_type),
                    ) {
                        let source_ids = self
                            .matching_node_ids_with_pending_bounded(
                                Some(source_label_id),
                                filter.as_ref(),
                                &pending_nodes,
                                remaining_mutation_affected_rows(rows.len(), limits)?,
                                "max_mutation_affected_rows",
                            )?
                            .into_iter()
                            .collect::<BTreeSet<_>>();
                        let target_ids = target_filter
                            .as_ref()
                            .map(|filter| {
                                self.matching_node_ids_with_pending_bounded(
                                    Some(target_label_id),
                                    Some(filter),
                                    &pending_nodes,
                                    remaining_mutation_affected_rows(rows.len(), limits)?,
                                    "max_mutation_affected_rows",
                                )
                                .map(|ids| ids.into_iter().collect::<BTreeSet<_>>())
                            })
                            .transpose()?;
                        if target_ids.as_ref().is_some_and(BTreeSet::is_empty) {
                            continue;
                        }
                        for relationship in self.relationship_records_owned() {
                            let relationship = relationship?;
                            if relationship.rel_type != rel_type_id
                                || !source_ids.contains(&relationship.source)
                            {
                                continue;
                            }
                            if rel_filter
                                .as_ref()
                                .map(|filter| {
                                    !property_filter_matches(
                                        filter,
                                        relationship.id.0,
                                        &relationship.properties,
                                    )
                                })
                                .unwrap_or(false)
                            {
                                continue;
                            }
                            let target_matches = self
                                .node_owned(relationship.target)?
                                .map(|target| {
                                    target.labels.contains(&target_label_id)
                                        && target_ids
                                            .as_ref()
                                            .map(|ids| ids.contains(&relationship.target))
                                            .unwrap_or(true)
                                })
                                .unwrap_or(false);
                            if target_matches {
                                ensure_additional_mutation_limits(
                                    ops.len(),
                                    rows.len(),
                                    1,
                                    1,
                                    limits,
                                )?;
                                ops.push(WalOp::DeleteRelationship {
                                    id: relationship.id,
                                });
                                rows.push(BTreeMap::from([(
                                    "rel_id".to_string(),
                                    Value::Int(relationship.id.0 as i64),
                                )]));
                            }
                        }
                        let mut pending_delete_ids = Vec::new();
                        for (relationship_id, source, target, pending_rel_type_id, properties) in
                            &pending_relationships
                        {
                            if *pending_rel_type_id != rel_type_id
                                || !source_ids.contains(source)
                                || rel_filter.as_ref().is_some_and(|filter| {
                                    !property_filter_matches(filter, relationship_id.0, properties)
                                })
                                || !node_matches_label_and_filter(
                                    self,
                                    &pending_nodes,
                                    *target,
                                    target_label_id,
                                    target_filter.as_ref(),
                                )?
                            {
                                continue;
                            }
                            pending_delete_ids.push(*relationship_id);
                        }
                        for relationship_id in pending_delete_ids {
                            ensure_additional_mutation_limits(ops.len(), rows.len(), 0, 1, limits)?;
                            remove_pending_relationship(
                                &mut ops,
                                &mut pending_relationships,
                                relationship_id,
                            );
                            rows.push(BTreeMap::from([(
                                "rel_id".to_string(),
                                Value::Int(relationship_id.0 as i64),
                            )]));
                        }
                    }
                }
                GraphMutation::DeleteRelationshipTargetNodes(request) => {
                    let ids = self.relationship_target_node_ids_with_pending_bounded(
                        &working_catalog,
                        &request,
                        &pending_nodes,
                        &pending_relationships,
                        remaining_mutation_affected_rows(rows.len(), limits)?,
                    )?;
                    let mut committed_ids = Vec::new();
                    for id in &ids {
                        if self.node_owned(*id)?.is_some() {
                            committed_ids.push(*id);
                        }
                    }
                    let pending_ids = ids
                        .iter()
                        .copied()
                        .filter(|id| {
                            pending_nodes
                                .iter()
                                .any(|(pending_id, _, _)| pending_id == id)
                        })
                        .collect::<Vec<_>>();
                    let incident_pending_relationship_ids =
                        pending_relationship_ids_for_nodes(&pending_relationships, &ids);
                    if !request.detach && !incident_pending_relationship_ids.is_empty() {
                        let id = ids.first().copied().unwrap_or(NodeId(0));
                        return Err(SkeinError::Storage(format!(
                            "node {} has relationships; use DETACH DELETE",
                            id.0
                        )));
                    }
                    ensure_additional_mutation_limits(ops.len(), rows.len(), 0, ids.len(), limits)?;
                    let delete_ops = self.delete_node_ops_bounded(
                        &committed_ids,
                        request.detach,
                        remaining_mutation_operations(ops.len(), limits)?,
                    )?;
                    if request.detach {
                        for relationship_id in incident_pending_relationship_ids {
                            remove_pending_relationship(
                                &mut ops,
                                &mut pending_relationships,
                                relationship_id,
                            );
                        }
                    }
                    for id in &pending_ids {
                        remove_pending_node(&mut ops, &mut pending_nodes, *id);
                    }
                    ops.extend(delete_ops);
                    rows.extend(ids.into_iter().map(|id| {
                        BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))])
                    }));
                }
                GraphMutation::CreateRelationshipsBetweenMatches(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let sources = self.matching_node_ids_with_pending_bounded(
                        source_label_id,
                        request.source_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let targets = self.matching_node_ids_with_pending_bounded(
                        target_label_id,
                        request.target_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let pair_count = sources.len().checked_mul(targets.len()).ok_or_else(|| {
                        SkeinError::Execution("mutation Cartesian product overflow".to_string())
                    })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in sources {
                        for target in &targets {
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.rel_type.clone(),
                                properties: request.rel_properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                rel_type_id,
                                request.rel_properties.clone(),
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsBetweenMatches(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let sources = self.matching_node_ids_with_pending_bounded(
                        source_label_id,
                        request.source_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let targets = self.matching_node_ids_with_pending_bounded(
                        target_label_id,
                        request.target_filter.as_ref(),
                        &pending_nodes,
                        remaining_rows,
                        "max_mutation_affected_rows",
                    )?;
                    let pair_count = sources.len().checked_mul(targets.len()).ok_or_else(|| {
                        SkeinError::Execution("mutation Cartesian product overflow".to_string())
                    })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in sources {
                        for target in &targets {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                rel_type_id,
                                &request.rel_match_properties,
                            )?;
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.rel_match_properties.clone();
                            for (property, value) in &request.on_create_properties {
                                properties.insert(property.clone(), value.clone());
                            }
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsToMatchedTarget(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    if !request.source_label.is_empty() && source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_target_label_id) =
                        working_catalog.label_id(&request.old_target_label)
                    else {
                        continue;
                    };
                    let Some(new_target_label_id) =
                        working_catalog.label_id(&request.new_target_label)
                    else {
                        continue;
                    };
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let source_ids = relationships_with_pending_matching_bounded(
                        self,
                        &pending_nodes,
                        &pending_relationships,
                        RelationshipMatchRequest {
                            rel_type_id: old_rel_type_id,
                            source_label_id,
                            source_filter: request.source_filter.as_ref(),
                            target_label_id: Some(old_target_label_id),
                            target_filter: request.old_target_filter.as_ref(),
                            rel_properties: &request.old_rel_filter,
                        },
                        remaining_rows,
                    )?
                    .into_iter()
                    .map(|relationship| relationship.source)
                    .collect::<BTreeSet<_>>();
                    let target_ids = self
                        .matching_node_ids_with_pending_bounded(
                            Some(new_target_label_id),
                            request.new_target_filter.as_ref(),
                            &pending_nodes,
                            remaining_rows,
                            "max_mutation_affected_rows",
                        )?
                        .into_iter()
                        .collect::<Vec<_>>();
                    let pair_count =
                        source_ids
                            .len()
                            .checked_mul(target_ids.len())
                            .ok_or_else(|| {
                                SkeinError::Execution(
                                    "mutation Cartesian product overflow".to_string(),
                                )
                            })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in source_ids {
                        for target in &target_ids {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                new_rel_type_id,
                                &request.new_rel_match_properties,
                            )?;
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == new_rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.new_rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.new_rel_match_properties.clone();
                            properties.extend(request.on_create_properties.clone());
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.new_rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                new_rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsFromMatchedTarget(request) => {
                    let old_source_label_id =
                        optional_label_id(&working_catalog, &request.old_source_label);
                    if !request.old_source_label.is_empty() && old_source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_target_label_id) =
                        working_catalog.label_id(&request.old_target_label)
                    else {
                        continue;
                    };
                    let new_source_label_id =
                        optional_label_id(&working_catalog, &request.new_source_label);
                    if !request.new_source_label.is_empty() && new_source_label_id.is_none() {
                        continue;
                    }
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
                    let target_ids = relationships_with_pending_matching_bounded(
                        self,
                        &pending_nodes,
                        &pending_relationships,
                        RelationshipMatchRequest {
                            rel_type_id: old_rel_type_id,
                            source_label_id: old_source_label_id,
                            source_filter: request.old_source_filter.as_ref(),
                            target_label_id: Some(old_target_label_id),
                            target_filter: request.old_target_filter.as_ref(),
                            rel_properties: &request.old_rel_filter,
                        },
                        remaining_rows,
                    )?
                    .into_iter()
                    .map(|relationship| relationship.target)
                    .collect::<BTreeSet<_>>();
                    let source_ids = self
                        .matching_node_ids_with_pending_bounded(
                            new_source_label_id,
                            request.new_source_filter.as_ref(),
                            &pending_nodes,
                            remaining_rows,
                            "max_mutation_affected_rows",
                        )?
                        .into_iter()
                        .collect::<Vec<_>>();
                    let pair_count =
                        source_ids
                            .len()
                            .checked_mul(target_ids.len())
                            .ok_or_else(|| {
                                SkeinError::Execution(
                                    "mutation Cartesian product overflow".to_string(),
                                )
                            })?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        pair_count,
                        pair_count,
                        limits,
                    )?;
                    for source in source_ids {
                        for target in &target_ids {
                            let current = self.find_relationship_by_property_subset(
                                source,
                                *target,
                                new_rel_type_id,
                                &request.new_rel_match_properties,
                            )?;
                            let pending = pending_relationships
                                .iter()
                                .find(
                                    |(
                                        _,
                                        pending_source,
                                        pending_target,
                                        pending_type,
                                        properties,
                                    )| {
                                        *pending_source == source
                                            && *pending_target == *target
                                            && *pending_type == new_rel_type_id
                                            && properties_contain_all(
                                                properties,
                                                &request.new_rel_match_properties,
                                            )
                                    },
                                )
                                .map(|(id, _, _, _, _)| *id);
                            if let Some(relationship) = current.or(pending) {
                                rows.push(BTreeMap::from([
                                    ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                    ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                    ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                    ("created".to_string(), Value::Bool(false)),
                                ]));
                                continue;
                            }
                            let relationship = RelId(next_rel_id);
                            next_rel_id += 1;
                            let mut properties = request.new_rel_match_properties.clone();
                            properties.extend(request.on_create_properties.clone());
                            ops.push(WalOp::CreateRelationship {
                                id: relationship,
                                source,
                                target: *target,
                                rel_type: request.new_rel_type.clone(),
                                properties: properties.clone(),
                            });
                            pending_relationships.push((
                                relationship,
                                source,
                                *target,
                                new_rel_type_id,
                                properties,
                            ));
                            rows.push(BTreeMap::from([
                                ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                                ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(true)),
                            ]));
                        }
                    }
                }
                GraphMutation::MergeRelationshipsFromMatchedRelationships(request) => {
                    let source_label_id =
                        optional_label_id(&working_catalog, &request.source_label);
                    let target_label_id =
                        optional_label_id(&working_catalog, &request.target_label);
                    if (!request.source_label.is_empty() && source_label_id.is_none())
                        || (!request.target_label.is_empty() && target_label_id.is_none())
                    {
                        continue;
                    }
                    let Some(old_rel_type_id) = working_catalog.rel_type_id(&request.old_rel_type)
                    else {
                        continue;
                    };
                    let new_rel_type_id =
                        working_catalog.get_or_create_rel_type(&request.new_rel_type);
                    let old_relationships = relationships_with_pending_matching_bounded(
                        self,
                        &pending_nodes,
                        &pending_relationships,
                        RelationshipMatchRequest {
                            rel_type_id: old_rel_type_id,
                            source_label_id,
                            source_filter: request.source_filter.as_ref(),
                            target_label_id,
                            target_filter: request.target_filter.as_ref(),
                            rel_properties: &request.old_rel_filter,
                        },
                        remaining_mutation_affected_rows(rows.len(), limits)?,
                    )?;
                    ensure_additional_mutation_limits(
                        ops.len(),
                        rows.len(),
                        old_relationships.len(),
                        old_relationships.len(),
                        limits,
                    )?;
                    for old_relationship in old_relationships {
                        let current = self.find_relationship_by_property_subset(
                            old_relationship.source,
                            old_relationship.target,
                            new_rel_type_id,
                            &request.new_rel_match_properties,
                        )?;
                        let pending = pending_relationships
                            .iter()
                            .find(
                                |(_, pending_source, pending_target, pending_type, properties)| {
                                    *pending_source == old_relationship.source
                                        && *pending_target == old_relationship.target
                                        && *pending_type == new_rel_type_id
                                        && properties_contain_all(
                                            properties,
                                            &request.new_rel_match_properties,
                                        )
                                },
                            )
                            .map(|(id, _, _, _, _)| *id);
                        if let Some(relationship) = current.or(pending) {
                            rows.push(BTreeMap::from([
                                (
                                    "source_node_id".to_string(),
                                    Value::Int(old_relationship.source.0 as i64),
                                ),
                                (
                                    "target_node_id".to_string(),
                                    Value::Int(old_relationship.target.0 as i64),
                                ),
                                ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                                ("created".to_string(), Value::Bool(false)),
                            ]));
                            continue;
                        }
                        let relationship = RelId(next_rel_id);
                        next_rel_id += 1;
                        let mut properties = request.new_rel_match_properties.clone();
                        for (property, value) in &request.on_create_properties {
                            let value = match value {
                                RelationshipOnCreatePropertyValue::Value(value) => value.clone(),
                                RelationshipOnCreatePropertyValue::MatchedRelationshipProperty {
                                    property,
                                } => old_relationship
                                    .properties
                                    .get(property)
                                    .cloned()
                                    .unwrap_or(Value::Null),
                            };
                            properties.insert(property.clone(), value);
                        }
                        ops.push(WalOp::CreateRelationship {
                            id: relationship,
                            source: old_relationship.source,
                            target: old_relationship.target,
                            rel_type: request.new_rel_type.clone(),
                            properties: properties.clone(),
                        });
                        pending_relationships.push((
                            relationship,
                            old_relationship.source,
                            old_relationship.target,
                            new_rel_type_id,
                            properties,
                        ));
                        rows.push(BTreeMap::from([
                            (
                                "source_node_id".to_string(),
                                Value::Int(old_relationship.source.0 as i64),
                            ),
                            (
                                "target_node_id".to_string(),
                                Value::Int(old_relationship.target.0 as i64),
                            ),
                            ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                            ("created".to_string(), Value::Bool(true)),
                        ]));
                    }
                }
                GraphMutation::CreateConnectedNodes(request) => {
                    let source_label_id =
                        working_catalog.get_or_create_label(&request.source_label);
                    let target_label_id =
                        working_catalog.get_or_create_label(&request.target_label);
                    let rel_type_id = working_catalog.get_or_create_rel_type(&request.rel_type);
                    let source_properties = request.source_properties.clone();
                    let target_properties = request.target_properties.clone();
                    let rel_properties = request.rel_properties.clone();
                    let source = NodeId(next_node_id);
                    let target = NodeId(next_node_id + 1);
                    let relationship = RelId(next_rel_id);
                    next_node_id += 2;
                    next_rel_id += 1;
                    ops.push(WalOp::CreateNode {
                        id: source,
                        label: request.source_label,
                        properties: request.source_properties,
                    });
                    ops.push(WalOp::CreateNode {
                        id: target,
                        label: request.target_label,
                        properties: request.target_properties,
                    });
                    ops.push(WalOp::CreateRelationship {
                        id: relationship,
                        source,
                        target,
                        rel_type: request.rel_type,
                        properties: request.rel_properties,
                    });
                    pending_relationships.push((
                        relationship,
                        source,
                        target,
                        rel_type_id,
                        rel_properties,
                    ));
                    pending_nodes.push((source, source_label_id, source_properties));
                    pending_nodes.push((target, target_label_id, target_properties));
                    rows.push(BTreeMap::from([
                        ("source_node_id".to_string(), Value::Int(source.0 as i64)),
                        ("target_node_id".to_string(), Value::Int(target.0 as i64)),
                        ("rel_id".to_string(), Value::Int(relationship.0 as i64)),
                    ]));
                }
            }
            ensure_mutation_commit_limits(&ops, &rows, limits)?;
        }

        self.commit_prepared_mutation_ops(catalog, working_catalog, ops, rows, limits, options)
    }

    fn commit_prepared_mutation_ops(
        &mut self,
        catalog: &mut Catalog,
        working_catalog: Catalog,
        mut ops: Vec<WalOp>,
        rows: Vec<BTreeMap<String, Value>>,
        limits: MutationLimits,
        options: MutationCommitOptions<'_>,
    ) -> Result<MutationSummary> {
        let MutationCommitOptions {
            relational: relational_transaction,
            append: append_transaction,
            preserve_single_create_wal,
            captured_graph_ops,
        } = options;
        self.ensure_usable()?;
        ensure_mutation_commit_limits(&ops, &rows, limits)?;
        if let Some(captured_graph_ops) = captured_graph_ops {
            captured_graph_ops.extend(ops.iter().cloned());
        }
        let mut staged_relational_state = None;
        let mut staged_relational_index_capture = None;
        let mut staged_relational_row_capture = None;
        let mut staged_relational_primary_key_changes = None;
        let mut relational_mutation_outcomes = Vec::new();
        let mut staged_append_state = None;
        let next_commit_epoch = self
            .commit_epoch
            .checked_add(1)
            .ok_or_else(|| SkeinError::Storage("commit epoch overflow".to_string()))?;
        if let Some(transaction) = relational_transaction.filter(|value| !value.writes.is_empty()) {
            let authoritative_index = self.authoritative_relational_constraint_index()?;
            let index_limits = self.relational_index_live_capture_limits();
            let row_limits = self.relational_row_live_capture_limits();
            let staged = if self.relational_state.canonical_row_metadata_only() {
                let index = authoritative_index.as_ref().ok_or_else(|| {
                    SkeinError::StorageIntegrity(
                        "sparse relational live staging requires an authoritative constraint index"
                            .to_string(),
                    )
                })?;
                let index_limits = index_limits.ok_or_else(|| {
                    SkeinError::StorageIntegrity(
                        "sparse relational live staging requires index capture limits".to_string(),
                    )
                })?;
                let row_limits = row_limits.ok_or_else(|| {
                    SkeinError::StorageIntegrity(
                        "sparse relational live staging requires row capture limits".to_string(),
                    )
                })?;
                let hydrated_workspace = self
                    .hydrate_sparse_relational_live_workspace(
                        &transaction,
                        index_limits,
                        row_limits,
                        index,
                    )
                    .map_err(map_relational_staging_error)?;
                let proven_constraint_index =
                    crate::store::relational_row_pages::RelationalProvenAbsenceConstraintIndex::new(
                        index,
                        &hydrated_workspace.proven_absent_primary_keys,
                    );
                self.relational_state
                    .stage_sparse_transaction_with_primary_key_changes(
                        RelationalSparseLiveStage {
                            transaction: transaction.clone(),
                            hydrated_workspace: hydrated_workspace.rows,
                            mutation_limits: self.relational_mutation_limits,
                            overflow_config: self.relational_overflow_config,
                            index_capture_limits: index_limits,
                            row_capture_limits: row_limits,
                            constraint_index: &proven_constraint_index,
                        },
                        self.search_projection_primary_key_capture_limits,
                    )
                    .map_err(map_relational_staging_error)?
            } else {
                self.relational_state
                    .stage_transaction_with_primary_key_changes(
                        transaction.clone(),
                        self.relational_mutation_limits,
                        self.relational_overflow_config,
                        index_limits,
                        row_limits,
                        self.search_projection_primary_key_capture_limits,
                        authoritative_index
                            .as_ref()
                            .map(|index| index as &dyn skein_storage::RelationalConstraintIndex),
                    )
                    .map_err(map_relational_staging_error)?
            };
            staged_relational_state = Some(staged.state);
            staged_relational_index_capture = staged.index_capture;
            staged_relational_row_capture = staged.row_capture;
            relational_mutation_outcomes = staged.mutation_outcomes;
            let replay_access = staged.replay_access.filter(|_| {
                matches!(
                    staged_relational_row_capture.as_ref(),
                    Some(skein_storage::RelationalRowChangeCapture::Captured { .. })
                )
            });
            let encoded = skein_storage::encode_relational_wal_batch_with_captures(
                next_commit_epoch,
                &transaction,
                replay_access.as_ref(),
                &staged.primary_key_changes,
            )
            .map_err(|error| SkeinError::Storage(error.to_string()))?;
            staged_relational_primary_key_changes = Some(encoded.primary_key_changes);
            ops.push(WalOp::Relational {
                record: Arc::from(encoded.record),
            });
        }
        if let Some(transaction) = append_transaction.filter(|value| !value.writes.is_empty()) {
            staged_append_state = Some(
                self.append_state
                    .stage_transaction(&transaction, self.append_mutation_limits)
                    .map_err(|error| SkeinError::Storage(error.to_string()))?,
            );
            let record = encode_append_wal_batch(next_commit_epoch, &transaction)
                .map_err(|error| SkeinError::Storage(error.to_string()))?;
            ops.push(WalOp::Append {
                record: Arc::from(record),
            });
        }
        if ops.is_empty() {
            return Ok(MutationSummary {
                rows,
                relational_mutation_outcomes,
            });
        }
        let staged_relational_index_publication = self.stage_relational_index_live_publication(
            next_commit_epoch,
            staged_relational_index_capture,
        );
        let staged_relational_row_publication = self.stage_relational_row_live_publication(
            next_commit_epoch,
            staged_relational_row_capture,
        );
        self.require_authoritative_relational_index_live_publication(
            next_commit_epoch,
            &staged_relational_index_publication,
        )?;
        let row_publication_requirement = self.require_relational_row_live_publication(
            next_commit_epoch,
            &staged_relational_row_publication,
        );
        self.poison_on_storage_error(&row_publication_requirement);
        row_publication_requirement?;
        self.validate_constraints_for_ops(&working_catalog, &ops)?;
        if let Some(durable) = &mut self.durable {
            if preserve_single_create_wal
                && let [WalOp::CreateNode {
                    id,
                    label,
                    properties,
                }] = ops.as_slice()
            {
                durable.append_create_node(*id, label, properties)?;
            } else {
                durable.append_batch(ops.clone())?;
            }
        }
        *catalog = working_catalog;
        self.record_search_projection_changes_for_ops(
            catalog,
            next_commit_epoch,
            &ops,
            staged_relational_primary_key_changes,
        );
        for op in ops {
            if matches!(op, WalOp::Relational { .. }) {
                self.relational_state = staged_relational_state
                    .take()
                    .expect("relational WAL operation must have staged state");
            } else if matches!(op, WalOp::Append { .. }) {
                self.append_state = staged_append_state
                    .take()
                    .expect("append WAL operation must have staged state");
            } else {
                self.apply_wal_op(catalog, op)?;
            }
        }
        self.commit_epoch = next_commit_epoch;
        self.publish_relational_index_live_view(staged_relational_index_publication);
        self.publish_relational_row_live_view(staged_relational_row_publication);
        Ok(MutationSummary {
            rows,
            relational_mutation_outcomes,
        })
    }

    fn apply_pending_node_assignments(
        ops: &mut [WalOp],
        pending_nodes: &mut [PendingNode],
        id: NodeId,
        assignments: &[NodeSetAssignment],
    ) -> Result<()> {
        let Some((_, _, properties)) = pending_nodes
            .iter_mut()
            .find(|(pending_id, _, _)| *pending_id == id)
        else {
            return Ok(());
        };
        for assignment in assignments {
            let value = evaluate_node_set_value(properties, assignment)?;
            properties.insert(assignment.property.clone(), value.clone());
            for op in ops.iter_mut() {
                if let WalOp::CreateNode {
                    id: create_id,
                    properties,
                    ..
                } = op
                    && *create_id == id
                {
                    properties.insert(assignment.property.clone(), value.clone());
                    break;
                }
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_set_node_properties_mutation(
        &self,
        ops: &mut Vec<WalOp>,
        rows: &mut Vec<BTreeMap<String, Value>>,
        pending_nodes: &mut [PendingNode],
        label_id: Option<LabelId>,
        filter: Option<&PropertyFilter>,
        assignments: &[NodeSetAssignment],
        limits: MutationLimits,
    ) -> Result<()> {
        let remaining_rows = remaining_mutation_affected_rows(rows.len(), limits)?;
        let committed_ids = self.matching_node_ids_bounded(
            label_id,
            filter,
            remaining_rows,
            "max_mutation_affected_rows",
        )?;
        let pending_ids = Self::pending_node_ids_matching(label_id, filter, pending_nodes);
        ensure_additional_mutation_limits(
            ops.len(),
            rows.len(),
            committed_ids.len().saturating_mul(assignments.len()),
            committed_ids.len().saturating_add(pending_ids.len()),
            limits,
        )?;
        ops.extend(self.node_set_property_ops(&committed_ids, assignments)?);
        for id in &pending_ids {
            Self::apply_pending_node_assignments(ops, pending_nodes, *id, assignments)?;
        }
        rows.extend(
            committed_ids
                .into_iter()
                .chain(pending_ids)
                .map(|id| BTreeMap::from([("node_id".to_string(), Value::Int(id.0 as i64))])),
        );
        Ok(())
    }

    pub(super) fn ensure_out_of_core_delta_admission(&self, ops: &[WalOp]) -> Result<()> {
        self.ensure_out_of_core_delta_admission_mode(ops, true)
    }

    pub(super) fn ensure_out_of_core_delta_replay_admission(&self, ops: &[WalOp]) -> Result<()> {
        self.ensure_out_of_core_delta_admission_mode(ops, false)
    }

    fn ensure_out_of_core_delta_admission_mode(
        &self,
        ops: &[WalOp],
        apply_live_backpressure: bool,
    ) -> Result<()> {
        if !self.canonical_base_out_of_core {
            return Ok(());
        }
        let Some(limit) = self.max_out_of_core_delta_bytes else {
            return Ok(());
        };
        let current = self.estimated_delta_resident_bytes();
        let mut touched_nodes = BTreeSet::new();
        let mut touched_relationships = BTreeSet::new();
        let additional = self.estimated_mutation_delta_bytes(
            ops,
            &mut touched_nodes,
            &mut touched_relationships,
        )?;
        let projected = current.saturating_add(additional);
        if projected > limit {
            return Err(SkeinError::Storage(format!(
                "out-of-core mutation delta admission rejected {projected} estimated bytes under the {limit} byte limit; checkpoint the database or raise max_out_of_core_delta_bytes"
            )));
        }
        let pressure = StorageDebtController.evaluate(StoragePressureSignals {
            delta_bytes: projected,
            max_delta_bytes: Some(limit),
            ..StoragePressureSignals::default()
        });
        if apply_live_backpressure && pressure.state == StoragePressureState::DelayMutation {
            return Err(SkeinError::Storage(format!(
                "out-of-core mutation delayed by storage pressure at {projected} estimated bytes under the {limit} byte limit; checkpoint the database before retrying"
            )));
        }
        Ok(())
    }

    fn estimated_mutation_delta_bytes(
        &self,
        ops: &[WalOp],
        touched_nodes: &mut BTreeSet<NodeId>,
        touched_relationships: &mut BTreeSet<RelId>,
    ) -> Result<u64> {
        let mut bytes = 0u64;
        for op in ops {
            match op {
                WalOp::CreateNode { id, properties, .. } => {
                    if touched_nodes.insert(*id) && !self.nodes.contains_key(id) {
                        bytes = bytes.saturating_add(64).saturating_add(
                            estimated_properties_bytes(properties).saturating_mul(2),
                        );
                    }
                }
                WalOp::SetNodeProperty {
                    id,
                    property,
                    value,
                } => {
                    if touched_nodes.insert(*id)
                        && !self.nodes.contains_key(id)
                        && let Some(node) = self.node_owned(*id)?
                    {
                        bytes = bytes.saturating_add(estimated_node_record_bytes(&node));
                    }
                    bytes = bytes
                        .saturating_add(64)
                        .saturating_add(property.len() as u64)
                        .saturating_add(estimated_value_bytes(value).saturating_mul(2));
                }
                WalOp::DeleteNode { id } => {
                    if touched_nodes.insert(*id)
                        && !self.nodes.contains_key(id)
                        && let Some(node) = self.node_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimated_node_record_bytes(&node))
                            .saturating_add(32);
                    }
                }
                WalOp::CreateRelationship { id, properties, .. } => {
                    if touched_relationships.insert(*id) && !self.relationships.contains_key(id) {
                        bytes = bytes.saturating_add(160).saturating_add(
                            estimated_properties_bytes(properties).saturating_mul(2),
                        );
                    }
                }
                WalOp::SetRelationshipProperty {
                    id,
                    property,
                    value,
                } => {
                    if touched_relationships.insert(*id)
                        && !self.relationships.contains_key(id)
                        && let Some(relationship) = self.relationship_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimated_relationship_record_bytes(&relationship))
                            .saturating_add(96);
                    }
                    bytes = bytes
                        .saturating_add(64)
                        .saturating_add(property.len() as u64)
                        .saturating_add(estimated_value_bytes(value).saturating_mul(2));
                }
                WalOp::DeleteRelationship { id } => {
                    if touched_relationships.insert(*id)
                        && !self.relationships.contains_key(id)
                        && let Some(relationship) = self.relationship_owned(*id)?
                    {
                        bytes = bytes
                            .saturating_add(estimated_relationship_record_bytes(&relationship))
                            .saturating_add(128);
                    }
                }
                WalOp::Batch(batch) => {
                    bytes = bytes.saturating_add(self.estimated_mutation_delta_bytes(
                        batch,
                        touched_nodes,
                        touched_relationships,
                    )?);
                }
                WalOp::CreateNodeLabel { .. }
                | WalOp::CreateRelationshipType { .. }
                | WalOp::CreateNodeTable { .. }
                | WalOp::CreateRelationshipTable { .. }
                | WalOp::CreateProperty { .. }
                | WalOp::AlterTableState { .. }
                | WalOp::AlterPropertyState { .. }
                | WalOp::GcTableDescriptor { .. }
                | WalOp::GcPropertyDescriptor { .. }
                | WalOp::CreateIndex { .. }
                | WalOp::CreateCompositeIndex { .. }
                | WalOp::CreateRangeIndex { .. }
                | WalOp::CreateFullTextIndex { .. }
                | WalOp::CreateUniqueConstraint { .. }
                | WalOp::CreateNodePropertyExistsConstraint { .. }
                | WalOp::CreateRelationshipUniqueConstraint { .. }
                | WalOp::CreateRelationshipPropertyExistsConstraint { .. }
                | WalOp::ProjectGraph { .. }
                | WalOp::MarkInitialImportSource { .. }
                | WalOp::Relational { .. }
                | WalOp::RelationalSnapshot { .. }
                | WalOp::Append { .. } => {}
            }
        }
        Ok(bytes)
    }

    pub(super) fn validate_out_of_core_record_changes(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
    ) -> Result<()> {
        let mut node_changes = BTreeMap::<NodeId, Option<NodeRecord>>::new();
        let mut relationship_changes = BTreeMap::<RelId, Option<RelRecord>>::new();
        self.collect_out_of_core_record_changes(
            catalog,
            ops,
            &mut node_changes,
            &mut relationship_changes,
        )?;

        for node in node_changes.values().flatten() {
            validate_node_record_constraints(catalog, node)?;
        }
        for relationship in relationship_changes.values().flatten() {
            validate_relationship_record_constraints(catalog, relationship)?;
        }
        validate_changed_node_uniqueness(self, catalog, &node_changes)?;
        validate_changed_relationship_uniqueness(self, catalog, &relationship_changes)
    }

    fn collect_out_of_core_record_changes(
        &self,
        catalog: &Catalog,
        ops: &[WalOp],
        nodes: &mut BTreeMap<NodeId, Option<NodeRecord>>,
        relationships: &mut BTreeMap<RelId, Option<RelRecord>>,
    ) -> Result<()> {
        for op in ops {
            match op {
                WalOp::CreateNode {
                    id,
                    label,
                    properties,
                } => {
                    let label_id = catalog.label_id(label).ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "node label '{label}' is missing during out-of-core validation"
                        ))
                    })?;
                    nodes.insert(
                        *id,
                        Some(NodeRecord {
                            id: *id,
                            labels: BTreeSet::from([label_id]),
                            properties: properties.clone(),
                        }),
                    );
                }
                WalOp::SetNodeProperty {
                    id,
                    property,
                    value,
                } => {
                    if !nodes.contains_key(id) {
                        nodes.insert(*id, self.node_owned(*id)?);
                    }
                    if let Some(node) = nodes.get_mut(id).and_then(Option::as_mut) {
                        node.properties.insert(property.clone(), value.clone());
                    }
                }
                WalOp::DeleteNode { id } => {
                    nodes.insert(*id, None);
                }
                WalOp::CreateRelationship {
                    id,
                    source,
                    target,
                    rel_type,
                    properties,
                } => {
                    let rel_type = catalog.rel_type_id(rel_type).ok_or_else(|| {
                        SkeinError::Storage(format!(
                            "relationship type '{rel_type}' is missing during out-of-core validation"
                        ))
                    })?;
                    relationships.insert(
                        *id,
                        Some(RelRecord {
                            id: *id,
                            source: *source,
                            target: *target,
                            rel_type,
                            properties: properties.clone(),
                        }),
                    );
                }
                WalOp::SetRelationshipProperty {
                    id,
                    property,
                    value,
                } => {
                    if !relationships.contains_key(id) {
                        relationships.insert(*id, self.relationship_owned(*id)?);
                    }
                    if let Some(relationship) = relationships.get_mut(id).and_then(Option::as_mut) {
                        relationship
                            .properties
                            .insert(property.clone(), value.clone());
                    }
                }
                WalOp::DeleteRelationship { id } => {
                    relationships.insert(*id, None);
                }
                WalOp::Batch(ops) => {
                    self.collect_out_of_core_record_changes(catalog, ops, nodes, relationships)?
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn map_relational_staging_error(error: RelationalError) -> SkeinError {
    match error {
        RelationalError::Corruption(_) => SkeinError::StorageIntegrity(error.to_string()),
        RelationalError::Admission(_)
        | RelationalError::Schema(_)
        | RelationalError::Constraint(_)
        | RelationalError::Durability(_) => SkeinError::Storage(error.to_string()),
    }
}
