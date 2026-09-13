use skein_sql_syntax::EdgeEndpoint;

use super::*;

impl Binder<'_> {
    pub(super) fn source<'a>(
        &self,
        catalog: &'a dyn PgqSourceCatalog,
        name: &QualifiedName,
    ) -> Result<&'a PgqSourceTableSchema> {
        let name_parts = self.name(name)?;
        let source = catalog.source_table(&name_parts).ok_or_else(|| {
            self.error(
                Code::UnknownTable,
                name.span,
                format!("source table {} does not exist", name_parts.join(".")),
            )
        })?;
        self.validate_source(source, name.span)?;
        Ok(source)
    }

    fn validate_source(&self, source: &PgqSourceTableSchema, span: Span) -> Result<()> {
        let invalid = |message| self.error(Code::InvalidSourceSchema, span, message);
        if source.name.is_empty()
            || source
                .name
                .iter()
                .any(|name| name.is_empty() || name.contains('\0'))
        {
            return Err(invalid("source has no valid canonical identity"));
        }
        let mut columns = BTreeMap::new();
        for column in &source.columns {
            if column.name.is_empty()
                || column.name.contains('\0')
                || columns.insert(&column.name, column).is_some()
            {
                return Err(invalid("source column names must be nonempty and unique"));
            }
        }
        self.source_columns(source, &source.primary_key, span, Code::InvalidSourceSchema)?;
        for name in &source.primary_key {
            if columns[name].nullable {
                return Err(invalid(
                    "source primary key metadata contains a nullable column",
                ));
            }
        }
        for foreign_key in &source.foreign_keys {
            self.source_columns(
                source,
                &foreign_key.columns,
                span,
                Code::InvalidSourceSchema,
            )?;
            if foreign_key.columns.is_empty()
                || foreign_key.columns.len() != foreign_key.referenced_columns.len()
                || foreign_key.referenced_table.is_empty()
                || foreign_key
                    .referenced_table
                    .iter()
                    .any(|name| name.is_empty() || name.contains('\0'))
                || foreign_key
                    .referenced_columns
                    .iter()
                    .any(|name| name.is_empty() || name.contains('\0'))
                || foreign_key
                    .referenced_columns
                    .iter()
                    .collect::<BTreeSet<_>>()
                    .len()
                    != foreign_key.referenced_columns.len()
            {
                return Err(invalid("inconsistent source foreign key metadata"));
            }
        }
        Ok(())
    }

    pub(super) fn alias(
        &self,
        source: &PgqSourceTableSchema,
        alias: Option<&Identifier>,
        span: Span,
    ) -> Result<String> {
        alias
            .map(|alias| self.identifier(alias))
            .unwrap_or_else(|| {
                source.name.last().cloned().ok_or_else(|| {
                    self.error(Code::InvalidSourceSchema, span, "missing source name")
                })
            })
    }

    pub(super) fn key(
        &self,
        source: &PgqSourceTableSchema,
        key: &[Identifier],
        span: Span,
    ) -> Result<()> {
        if key.is_empty() {
            if source.primary_key.is_empty() {
                return Err(self.error(
                    Code::MissingKey,
                    span,
                    "omitted KEY requires a source primary key",
                ));
            }
        } else {
            let columns = key
                .iter()
                .map(|column| self.identifier(column))
                .collect::<Result<Vec<_>>>()?;
            self.source_columns(source, &columns, span, Code::InvalidKey)?;
        }
        Ok(())
    }

    fn source_columns<'a>(
        &self,
        source: &'a PgqSourceTableSchema,
        names: &[String],
        span: Span,
        code: Code,
    ) -> Result<Vec<&'a PgqSourceColumnSchema>> {
        let mut seen = BTreeSet::new();
        names
            .iter()
            .map(|name| {
                if !seen.insert(name) {
                    return Err(self.error(code, span, format!("duplicate key column {name}")));
                }
                source
                    .columns
                    .iter()
                    .find(|column| &column.name == name)
                    .ok_or_else(|| {
                        self.error(
                            if code == Code::InvalidSourceSchema {
                                code
                            } else {
                                Code::UnknownColumn
                            },
                            span,
                            format!("source column {name} does not exist"),
                        )
                    })
            })
            .collect()
    }

    pub(super) fn endpoint(
        &self,
        source: &PgqSourceTableSchema,
        endpoint: &EdgeEndpoint,
        vertices: &BTreeMap<String, &PgqSourceTableSchema>,
    ) -> Result<()> {
        self.text(endpoint.span)?;
        let alias = self.identifier(&endpoint.vertex)?;
        let vertex = vertices.get(&alias).ok_or_else(|| {
            self.error(
                Code::UnknownVertex,
                endpoint.vertex.span,
                format!("vertex alias {alias} does not exist"),
            )
        })?;
        let inferred = endpoint.key.is_empty() && endpoint.vertex_key.is_empty();
        let (edge_names, vertex_names) = if inferred {
            // Separate constraints remain separate candidates, including duplicates.
            let mut candidates = source
                .foreign_keys
                .iter()
                .filter(|key| key.referenced_table == vertex.name);
            let key = candidates.next().ok_or_else(|| {
                self.error(
                    Code::MissingForeignKey,
                    endpoint.span,
                    format!("no foreign key references vertex {alias}"),
                )
            })?;
            if candidates.next().is_some() {
                return Err(self.error(
                    Code::AmbiguousForeignKey,
                    endpoint.span,
                    format!("multiple foreign keys reference vertex {alias}"),
                ));
            }
            (key.columns.clone(), key.referenced_columns.clone())
        } else {
            (
                endpoint
                    .key
                    .iter()
                    .map(|id| self.identifier(id))
                    .collect::<Result<Vec<_>>>()?,
                endpoint
                    .vertex_key
                    .iter()
                    .map(|id| self.identifier(id))
                    .collect::<Result<Vec<_>>>()?,
            )
        };
        if edge_names.is_empty() || edge_names.len() != vertex_names.len() {
            return Err(self.error(
                Code::InvalidKey,
                endpoint.span,
                "endpoint column lists require equal nonzero arity",
            ));
        }
        let metadata_code = if inferred {
            Code::InvalidSourceSchema
        } else {
            Code::InvalidKey
        };
        let edge_columns =
            self.source_columns(source, &edge_names, endpoint.span, metadata_code)?;
        let vertex_columns =
            self.source_columns(vertex, &vertex_names, endpoint.span, metadata_code)?;
        for (edge, vertex) in edge_columns.iter().zip(vertex_columns) {
            // PostgreSQL resolves equality with the referenced vertex on the left.
            let compatible = edge.data_type == vertex.data_type
                || (!inferred
                    && edge.data_type == SqlDataType::BigInt
                    && vertex.data_type == SqlDataType::DoublePrecision);
            if !compatible {
                return Err(self.error(
                    if inferred {
                        Code::InvalidSourceSchema
                    } else {
                        Code::TypeMismatch
                    },
                    endpoint.span,
                    "incompatible endpoint column types",
                ));
            }
        }
        Ok(())
    }
}
