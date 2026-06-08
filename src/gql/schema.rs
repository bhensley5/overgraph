use crate::error::EngineError;
use crate::gql::ast::*;
use crate::schema::{
    validate_edge_schema_shape, validate_node_schema_shape, DenseVectorSchema, EdgeSchema,
    EdgeSchemaInfo, EdgeValiditySchema, EndpointLabelSchema, GraphSchema, GraphSchemaCheckOptions,
    GraphSchemaOperation, GraphSchemaSetOptions, NodeLabelConstraintSchema, NodeSchema,
    NodeSchemaInfo, NumericFieldSchema, PropertySchema, SchemaAdditionalProperties,
    SchemaNumericBound, SchemaTargetKind, SchemaValueType, SchemaVectorPresence, SchemaViolation,
    SchemaViolationTarget, SparseVectorSchema, StringFieldSchema,
};
use crate::types::{
    validate_label_token_name, GqlParamValue, GqlParams, GqlSemanticErrorCode, GqlValue, PropValue,
    SourceSpan,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlSchemaSemanticPlan {
    Alter(GqlBoundAlterGraphTypeStatement),
    DropCurrentGraphType {
        span: SourceSpan,
        parameters: Vec<String>,
        parameter_spans: BTreeMap<String, SourceSpan>,
    },
    Check(GqlBoundCheckGraphTypeStatement),
    Show {
        kind: GqlShowSchemaKind,
        span: SourceSpan,
        parameters: Vec<String>,
        parameter_spans: BTreeMap<String, SourceSpan>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundAlterGraphTypeStatement {
    pub(crate) mode: GqlGraphTypeAlterMode,
    pub(crate) schema: Option<GraphSchema>,
    pub(crate) operations: Vec<GraphSchemaOperation>,
    pub(crate) options: GraphSchemaSetOptions,
    pub(crate) span: SourceSpan,
    pub(crate) parameters: Vec<String>,
    pub(crate) parameter_spans: BTreeMap<String, SourceSpan>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundCheckGraphTypeStatement {
    pub(crate) mode: GqlGraphTypeCheckMode,
    pub(crate) schema: GraphSchema,
    pub(crate) options: GraphSchemaCheckOptions,
    pub(crate) span: SourceSpan,
    pub(crate) parameters: Vec<String>,
    pub(crate) parameter_spans: BTreeMap<String, SourceSpan>,
}

pub(crate) fn bind_schema_statement(
    statement: GqlSchemaStatement,
    params: &GqlParams,
) -> Result<GqlSchemaSemanticPlan, EngineError> {
    let mut binder = GqlSchemaBinder {
        params,
        parameters: BTreeSet::new(),
        parameter_spans: BTreeMap::new(),
    };
    binder.bind_statement(statement)
}

pub(crate) fn schema_statement_is_mutating(statement: &GqlSchemaStatement) -> bool {
    matches!(
        statement,
        GqlSchemaStatement::AlterGraphType(_) | GqlSchemaStatement::DropCurrentGraphType { .. }
    )
}

#[derive(Clone, Debug, PartialEq)]
struct SchemaValue {
    kind: SchemaValueKind,
    span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
enum SchemaValueKind {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<SchemaValue>),
    Map(Vec<SchemaField>),
}

#[derive(Clone, Debug, PartialEq)]
struct SchemaField {
    key: String,
    key_span: SourceSpan,
    value: SchemaValue,
}

struct GqlSchemaBinder<'a> {
    params: &'a GqlParams,
    parameters: BTreeSet<String>,
    parameter_spans: BTreeMap<String, SourceSpan>,
}

impl GqlSchemaBinder<'_> {
    fn bind_statement(
        &mut self,
        statement: GqlSchemaStatement,
    ) -> Result<GqlSchemaSemanticPlan, EngineError> {
        let plan = match statement {
            GqlSchemaStatement::AlterGraphType(statement) => {
                GqlSchemaSemanticPlan::Alter(self.bind_alter_graph_type(statement)?)
            }
            GqlSchemaStatement::DropCurrentGraphType { span } => {
                GqlSchemaSemanticPlan::DropCurrentGraphType {
                    span,
                    parameters: Vec::new(),
                    parameter_spans: BTreeMap::new(),
                }
            }
            GqlSchemaStatement::CheckGraphType(statement) => {
                GqlSchemaSemanticPlan::Check(self.bind_check_graph_type(statement)?)
            }
            GqlSchemaStatement::Show(statement) => {
                self.validate_show_schema_kind(&statement.kind)?;
                GqlSchemaSemanticPlan::Show {
                    kind: statement.kind,
                    span: statement.span,
                    parameters: Vec::new(),
                    parameter_spans: BTreeMap::new(),
                }
            }
        };
        Ok(plan)
    }

    fn bind_alter_graph_type(
        &mut self,
        statement: GqlAlterGraphTypeStatement,
    ) -> Result<GqlBoundAlterGraphTypeStatement, EngineError> {
        let (schema, operations) = match statement.mode {
            GqlGraphTypeAlterMode::Add => {
                if statement.items.is_empty() {
                    return Err(gql_schema_semantic_error(
                        "ALTER CURRENT GRAPH TYPE ADD requires at least one schema item",
                        statement.span.clone(),
                    ));
                }
                let schema = self.graph_schema_from_items(&statement.items)?;
                let operations = graph_schema_set_operations(&schema);
                (Some(schema), operations)
            }
            GqlGraphTypeAlterMode::Set => {
                let schema = self.graph_schema_from_items(&statement.items)?;
                let operations = graph_schema_set_operations(&schema);
                (Some(schema), operations)
            }
            GqlGraphTypeAlterMode::Drop => {
                if statement.drop_items.is_empty() {
                    return Err(gql_schema_semantic_error(
                        "ALTER CURRENT GRAPH TYPE DROP requires at least one schema item",
                        statement.span.clone(),
                    ));
                }
                (
                    None,
                    self.drop_operations_from_items(&statement.drop_items)?,
                )
            }
        };
        let options = match statement.options.as_ref() {
            Some(options) => self.parse_set_options(options)?,
            None => GraphSchemaSetOptions::default(),
        };
        Ok(GqlBoundAlterGraphTypeStatement {
            mode: statement.mode,
            schema,
            operations,
            options,
            span: statement.span,
            parameters: self.parameters.iter().cloned().collect(),
            parameter_spans: self.parameter_spans.clone(),
        })
    }

    fn bind_check_graph_type(
        &mut self,
        statement: GqlCheckGraphTypeStatement,
    ) -> Result<GqlBoundCheckGraphTypeStatement, EngineError> {
        if statement.mode == GqlGraphTypeCheckMode::Add && statement.items.is_empty() {
            return Err(gql_schema_semantic_error(
                "CHECK CURRENT GRAPH TYPE ADD requires at least one schema item",
                statement.span.clone(),
            ));
        }
        let schema = self.graph_schema_from_items(&statement.items)?;
        let options = match statement.options.as_ref() {
            Some(options) => self.parse_check_options(options)?,
            None => GraphSchemaCheckOptions::default(),
        };
        Ok(GqlBoundCheckGraphTypeStatement {
            mode: statement.mode,
            schema,
            options,
            span: statement.span,
            parameters: self.parameters.iter().cloned().collect(),
            parameter_spans: self.parameter_spans.clone(),
        })
    }

    fn graph_schema_from_items(
        &mut self,
        items: &[GqlSchemaItem],
    ) -> Result<GraphSchema, EngineError> {
        let mut node_labels = BTreeSet::new();
        let mut edge_labels = BTreeSet::new();
        let mut node_schemas = Vec::new();
        let mut edge_schemas = Vec::new();
        for item in items {
            match item {
                GqlSchemaItem::Node {
                    label,
                    schema,
                    span,
                } => {
                    validate_schema_label(label)?;
                    reject_duplicate_target(&mut node_labels, &label.name, "node", &label.span)?;
                    let value = self.schema_literal_to_value(schema, "node schema")?;
                    let schema = self.parse_node_schema(&value)?;
                    validate_node_schema_shape(&schema)
                        .map_err(|err| schema_shape_error(err, span.clone()))?;
                    node_schemas.push(NodeSchemaInfo {
                        label: label.name.clone(),
                        schema,
                    });
                }
                GqlSchemaItem::Edge {
                    label,
                    schema,
                    span,
                } => {
                    validate_schema_label(label)?;
                    reject_duplicate_target(&mut edge_labels, &label.name, "edge", &label.span)?;
                    let value = self.schema_literal_to_value(schema, "edge schema")?;
                    let schema = self.parse_edge_schema(&value)?;
                    validate_edge_schema_shape(&schema)
                        .map_err(|err| schema_shape_error(err, span.clone()))?;
                    edge_schemas.push(EdgeSchemaInfo {
                        label: label.name.clone(),
                        schema,
                    });
                }
            }
        }
        Ok(GraphSchema {
            node_schemas,
            edge_schemas,
        })
    }

    fn drop_operations_from_items(
        &self,
        items: &[GqlSchemaDropItem],
    ) -> Result<Vec<GraphSchemaOperation>, EngineError> {
        let mut node_labels = BTreeSet::new();
        let mut edge_labels = BTreeSet::new();
        let mut operations = Vec::with_capacity(items.len());
        for item in items {
            match item {
                GqlSchemaDropItem::Node { label, .. } => {
                    validate_schema_label(label)?;
                    reject_duplicate_target(
                        &mut node_labels,
                        &label.name,
                        "node drop",
                        &label.span,
                    )?;
                    operations.push(GraphSchemaOperation::DropNode {
                        label: label.name.clone(),
                    });
                }
                GqlSchemaDropItem::Edge { label, .. } => {
                    validate_schema_label(label)?;
                    reject_duplicate_target(
                        &mut edge_labels,
                        &label.name,
                        "edge drop",
                        &label.span,
                    )?;
                    operations.push(GraphSchemaOperation::DropEdge {
                        label: label.name.clone(),
                    });
                }
            }
        }
        Ok(operations)
    }

    fn validate_show_schema_kind(&self, kind: &GqlShowSchemaKind) -> Result<(), EngineError> {
        match kind {
            GqlShowSchemaKind::NodeSchema { label } | GqlShowSchemaKind::EdgeSchema { label } => {
                validate_schema_label(label)
            }
            GqlShowSchemaKind::CurrentGraphType
            | GqlShowSchemaKind::NodeSchemas
            | GqlShowSchemaKind::EdgeSchemas => Ok(()),
        }
    }

    fn schema_literal_to_value(
        &mut self,
        literal: &GqlSchemaLiteral,
        expected: &str,
    ) -> Result<SchemaValue, EngineError> {
        match literal {
            GqlSchemaLiteral::Map(map) => self.map_literal_to_value(map),
            GqlSchemaLiteral::Parameter { name, span } => {
                self.record_parameter(name, span);
                let value = self
                    .params
                    .get(name)
                    .ok_or_else(|| EngineError::GqlParameter {
                        name: name.clone(),
                        expected: expected.to_string(),
                        message: format!("missing parameter '${name}'"),
                        span: span.clone(),
                    })?;
                self.param_value_to_schema_value(value, span)
            }
        }
    }

    fn map_literal_to_value(&mut self, map: &MapLiteral) -> Result<SchemaValue, EngineError> {
        let mut fields = Vec::with_capacity(map.entries.len());
        for entry in &map.entries {
            fields.push(SchemaField {
                key: entry.key.name.clone(),
                key_span: entry.key.span.clone(),
                value: self.expr_to_schema_value(&entry.value)?,
            });
        }
        Ok(SchemaValue {
            kind: SchemaValueKind::Map(fields),
            span: map.span.clone(),
        })
    }

    fn expr_to_schema_value(&mut self, expr: &Expr) -> Result<SchemaValue, EngineError> {
        match &expr.kind {
            ExprKind::Literal(literal) => match literal {
                Literal::Null => Ok(SchemaValue {
                    kind: SchemaValueKind::Null,
                    span: expr.span.clone(),
                }),
                Literal::Bool(value) => Ok(SchemaValue {
                    kind: SchemaValueKind::Bool(*value),
                    span: expr.span.clone(),
                }),
                Literal::Int(value) => Ok(SchemaValue {
                    kind: SchemaValueKind::Int(*value),
                    span: expr.span.clone(),
                }),
                Literal::Float(value) if value.is_finite() => Ok(SchemaValue {
                    kind: SchemaValueKind::Float(*value),
                    span: expr.span.clone(),
                }),
                Literal::Float(_) => Err(gql_schema_semantic_error(
                    "schema float literals must be finite",
                    expr.span.clone(),
                )),
                Literal::String(value) => Ok(SchemaValue {
                    kind: SchemaValueKind::String(value.clone()),
                    span: expr.span.clone(),
                }),
            },
            ExprKind::Parameter(name) => {
                self.record_parameter(name, &expr.span);
                let value = self
                    .params
                    .get(name)
                    .ok_or_else(|| EngineError::GqlParameter {
                        name: name.clone(),
                        expected: "literal-compatible schema value".to_string(),
                        message: format!("missing parameter '${name}'"),
                        span: expr.span.clone(),
                    })?;
                self.param_value_to_schema_value(value, &expr.span)
            }
            ExprKind::Unary {
                op: UnaryOp::Neg,
                expr: inner,
            } => self.negated_schema_value(inner, &expr.span),
            ExprKind::List(items) => {
                let values = items
                    .iter()
                    .map(|item| self.expr_to_schema_value(item))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(SchemaValue {
                    kind: SchemaValueKind::List(values),
                    span: expr.span.clone(),
                })
            }
            ExprKind::Map(map) => self.map_literal_to_value(map),
            _ => Err(gql_schema_semantic_error(
                "schema maps and OPTIONS accept only literal values and parameters",
                expr.span.clone(),
            )),
        }
    }

    fn negated_schema_value(
        &mut self,
        inner: &Expr,
        span: &SourceSpan,
    ) -> Result<SchemaValue, EngineError> {
        match &inner.kind {
            ExprKind::Literal(Literal::Int(value)) => {
                let value = value.checked_neg().ok_or_else(|| {
                    gql_schema_semantic_error("negative integer literal overflow", span.clone())
                })?;
                Ok(SchemaValue {
                    kind: SchemaValueKind::Int(value),
                    span: span.clone(),
                })
            }
            ExprKind::Literal(Literal::Float(value)) if value.is_finite() => Ok(SchemaValue {
                kind: SchemaValueKind::Float(-value),
                span: span.clone(),
            }),
            ExprKind::Literal(Literal::Float(_)) => Err(gql_schema_semantic_error(
                "schema float literals must be finite",
                span.clone(),
            )),
            _ => Err(gql_schema_semantic_error(
                "schema maps and OPTIONS accept only literal values and parameters",
                span.clone(),
            )),
        }
    }

    fn param_value_to_schema_value(
        &mut self,
        value: &GqlParamValue,
        span: &SourceSpan,
    ) -> Result<SchemaValue, EngineError> {
        let kind = match value {
            GqlParamValue::Null => SchemaValueKind::Null,
            GqlParamValue::Bool(value) => SchemaValueKind::Bool(*value),
            GqlParamValue::Int(value) => SchemaValueKind::Int(*value),
            GqlParamValue::UInt(value) => SchemaValueKind::UInt(*value),
            GqlParamValue::Float(value) if value.is_finite() => SchemaValueKind::Float(*value),
            GqlParamValue::Float(_) => {
                return Err(gql_schema_semantic_error(
                    "schema parameter floats must be finite",
                    span.clone(),
                ));
            }
            GqlParamValue::String(value) => SchemaValueKind::String(value.clone()),
            GqlParamValue::Bytes(value) => SchemaValueKind::Bytes(value.clone()),
            GqlParamValue::List(values) => SchemaValueKind::List(
                values
                    .iter()
                    .map(|value| self.param_value_to_schema_value(value, span))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            GqlParamValue::Map(values) => SchemaValueKind::Map(
                values
                    .iter()
                    .map(|(key, value)| {
                        Ok(SchemaField {
                            key: key.clone(),
                            key_span: span.clone(),
                            value: self.param_value_to_schema_value(value, span)?,
                        })
                    })
                    .collect::<Result<Vec<_>, EngineError>>()?,
            ),
        };
        Ok(SchemaValue {
            kind,
            span: span.clone(),
        })
    }

    fn record_parameter(&mut self, name: &str, span: &SourceSpan) {
        self.parameters.insert(name.to_string());
        self.parameter_spans
            .entry(name.to_string())
            .or_insert_with(|| span.clone());
    }

    fn parse_node_schema(&self, value: &SchemaValue) -> Result<NodeSchema, EngineError> {
        let fields = object_fields(
            value,
            "node schema",
            &[
                "additional_properties",
                "properties",
                "key",
                "label_constraints",
                "weight",
                "dense_vector",
                "sparse_vector",
            ],
        )?;
        let mut schema = NodeSchema::default();
        if let Some(field) = fields.get("additional_properties") {
            schema.additional_properties = parse_additional_properties(&field.value)?;
        }
        if let Some(field) = fields.get("properties") {
            schema.properties = self.parse_property_map(&field.value)?;
        }
        if let Some(field) = fields.get("key") {
            schema.key = parse_optional_map(&field.value, "node schema key", |value| {
                self.parse_string_field_schema(value)
            })?;
        }
        if let Some(field) = fields.get("label_constraints") {
            schema.label_constraints =
                parse_optional_map(&field.value, "node schema label_constraints", |value| {
                    self.parse_node_label_constraint_schema(value)
                })?;
        }
        if let Some(field) = fields.get("weight") {
            schema.weight = parse_optional_map(&field.value, "node schema weight", |value| {
                self.parse_numeric_field_schema(value)
            })?;
        }
        if let Some(field) = fields.get("dense_vector") {
            schema.dense_vector =
                parse_optional_map(&field.value, "node schema dense_vector", |value| {
                    self.parse_dense_vector_schema(value)
                })?;
        }
        if let Some(field) = fields.get("sparse_vector") {
            schema.sparse_vector =
                parse_optional_map(&field.value, "node schema sparse_vector", |value| {
                    self.parse_sparse_vector_schema(value)
                })?;
        }
        Ok(schema)
    }

    fn parse_edge_schema(&self, value: &SchemaValue) -> Result<EdgeSchema, EngineError> {
        let fields = object_fields(
            value,
            "edge schema",
            &[
                "additional_properties",
                "properties",
                "from",
                "to",
                "allow_self_loops",
                "weight",
                "validity",
            ],
        )?;
        let mut schema = EdgeSchema::default();
        if let Some(field) = fields.get("additional_properties") {
            schema.additional_properties = parse_additional_properties(&field.value)?;
        }
        if let Some(field) = fields.get("properties") {
            schema.properties = self.parse_property_map(&field.value)?;
        }
        if let Some(field) = fields.get("from") {
            schema.from = parse_optional_map(&field.value, "edge schema from", |value| {
                self.parse_endpoint_label_schema(value)
            })?;
        }
        if let Some(field) = fields.get("to") {
            schema.to = parse_optional_map(&field.value, "edge schema to", |value| {
                self.parse_endpoint_label_schema(value)
            })?;
        }
        if let Some(field) = fields.get("allow_self_loops") {
            schema.allow_self_loops = expect_bool(&field.value, "allow_self_loops")?;
        }
        if let Some(field) = fields.get("weight") {
            schema.weight = parse_optional_map(&field.value, "edge schema weight", |value| {
                self.parse_numeric_field_schema(value)
            })?;
        }
        if let Some(field) = fields.get("validity") {
            schema.validity = parse_optional_map(&field.value, "edge schema validity", |value| {
                self.parse_edge_validity_schema(value)
            })?;
        }
        Ok(schema)
    }

    fn parse_property_map(
        &self,
        value: &SchemaValue,
    ) -> Result<BTreeMap<String, PropertySchema>, EngineError> {
        let fields = expect_map(value, "properties")?;
        let mut properties = BTreeMap::new();
        for field in fields {
            if properties.contains_key(&field.key) {
                return Err(gql_schema_semantic_error(
                    format!("duplicate property schema '{}'", field.key),
                    field.key_span.clone(),
                ));
            }
            properties.insert(field.key.clone(), self.parse_property_schema(&field.value)?);
        }
        Ok(properties)
    }

    fn parse_property_schema(&self, value: &SchemaValue) -> Result<PropertySchema, EngineError> {
        let fields = object_fields(
            value,
            "property schema",
            &[
                "required",
                "nullable",
                "types",
                "numeric_min",
                "numeric_max",
                "string_min_bytes",
                "string_max_bytes",
                "bytes_min_len",
                "bytes_max_len",
                "array_min_items",
                "array_max_items",
                "map_min_entries",
                "map_max_entries",
                "enum_values",
            ],
        )?;
        let mut schema = PropertySchema::default();
        if let Some(field) = fields.get("required") {
            schema.required = expect_bool(&field.value, "required")?;
        }
        if let Some(field) = fields.get("nullable") {
            schema.nullable = expect_bool(&field.value, "nullable")?;
        }
        if let Some(field) = fields.get("types") {
            schema.types = parse_schema_value_types(&field.value)?;
        }
        if let Some(field) = fields.get("numeric_min") {
            schema.numeric_min =
                parse_optional_map(&field.value, "numeric_min", parse_schema_numeric_bound)?;
        }
        if let Some(field) = fields.get("numeric_max") {
            schema.numeric_max =
                parse_optional_map(&field.value, "numeric_max", parse_schema_numeric_bound)?;
        }
        schema.string_min_bytes = parse_optional_usize_field(&fields, "string_min_bytes")?;
        schema.string_max_bytes = parse_optional_usize_field(&fields, "string_max_bytes")?;
        schema.bytes_min_len = parse_optional_usize_field(&fields, "bytes_min_len")?;
        schema.bytes_max_len = parse_optional_usize_field(&fields, "bytes_max_len")?;
        schema.array_min_items = parse_optional_usize_field(&fields, "array_min_items")?;
        schema.array_max_items = parse_optional_usize_field(&fields, "array_max_items")?;
        schema.map_min_entries = parse_optional_usize_field(&fields, "map_min_entries")?;
        schema.map_max_entries = parse_optional_usize_field(&fields, "map_max_entries")?;
        if let Some(field) = fields.get("enum_values") {
            schema.enum_values = parse_prop_value_list(&field.value)?;
        }
        Ok(schema)
    }

    fn parse_string_field_schema(
        &self,
        value: &SchemaValue,
    ) -> Result<StringFieldSchema, EngineError> {
        let fields = object_fields(
            value,
            "string field schema",
            &["min_bytes", "max_bytes", "enum_values"],
        )?;
        Ok(StringFieldSchema {
            min_bytes: parse_optional_usize_field(&fields, "min_bytes")?,
            max_bytes: parse_optional_usize_field(&fields, "max_bytes")?,
            enum_values: fields
                .get("enum_values")
                .map(|field| parse_string_list(&field.value, "enum_values"))
                .transpose()?
                .unwrap_or_default(),
        })
    }

    fn parse_numeric_field_schema(
        &self,
        value: &SchemaValue,
    ) -> Result<NumericFieldSchema, EngineError> {
        let fields = object_fields(value, "numeric field schema", &["min", "max", "finite"])?;
        let mut schema = NumericFieldSchema::default();
        if let Some(field) = fields.get("min") {
            schema.min = parse_optional_map(
                &field.value,
                "numeric field min",
                parse_schema_numeric_bound,
            )?;
        }
        if let Some(field) = fields.get("max") {
            schema.max = parse_optional_map(
                &field.value,
                "numeric field max",
                parse_schema_numeric_bound,
            )?;
        }
        if let Some(field) = fields.get("finite") {
            schema.finite = expect_bool(&field.value, "finite")?;
        }
        Ok(schema)
    }

    fn parse_node_label_constraint_schema(
        &self,
        value: &SchemaValue,
    ) -> Result<NodeLabelConstraintSchema, EngineError> {
        let fields = object_fields(
            value,
            "node label constraint schema",
            &["all_of", "any_of", "none_of"],
        )?;
        Ok(NodeLabelConstraintSchema {
            all_of: parse_optional_string_list_field(&fields, "all_of")?,
            any_of: parse_optional_string_list_field(&fields, "any_of")?,
            none_of: parse_optional_string_list_field(&fields, "none_of")?,
        })
    }

    fn parse_endpoint_label_schema(
        &self,
        value: &SchemaValue,
    ) -> Result<EndpointLabelSchema, EngineError> {
        let fields = object_fields(
            value,
            "endpoint label schema",
            &["all_of", "any_of", "none_of"],
        )?;
        Ok(EndpointLabelSchema {
            all_of: parse_optional_string_list_field(&fields, "all_of")?,
            any_of: parse_optional_string_list_field(&fields, "any_of")?,
            none_of: parse_optional_string_list_field(&fields, "none_of")?,
        })
    }

    fn parse_dense_vector_schema(
        &self,
        value: &SchemaValue,
    ) -> Result<DenseVectorSchema, EngineError> {
        let fields = object_fields(value, "dense vector schema", &["presence", "dimension"])?;
        let mut schema = DenseVectorSchema::default();
        if let Some(field) = fields.get("presence") {
            schema.presence = parse_vector_presence(&field.value)?;
        }
        schema.dimension = parse_optional_usize_field(&fields, "dimension")?;
        Ok(schema)
    }

    fn parse_sparse_vector_schema(
        &self,
        value: &SchemaValue,
    ) -> Result<SparseVectorSchema, EngineError> {
        let fields = object_fields(
            value,
            "sparse vector schema",
            &["presence", "min_entries", "max_entries", "max_dimension_id"],
        )?;
        let mut schema = SparseVectorSchema::default();
        if let Some(field) = fields.get("presence") {
            schema.presence = parse_vector_presence(&field.value)?;
        }
        schema.min_entries = parse_optional_usize_field(&fields, "min_entries")?;
        schema.max_entries = parse_optional_usize_field(&fields, "max_entries")?;
        schema.max_dimension_id = parse_optional_u32_field(&fields, "max_dimension_id")?;
        Ok(schema)
    }

    fn parse_edge_validity_schema(
        &self,
        value: &SchemaValue,
    ) -> Result<EdgeValiditySchema, EngineError> {
        let fields = object_fields(
            value,
            "edge validity schema",
            &[
                "require_valid_from_before_valid_to",
                "valid_from_min",
                "valid_from_max",
                "valid_to_min",
                "valid_to_max",
                "allow_open_ended_valid_to",
            ],
        )?;
        let mut schema = EdgeValiditySchema::default();
        if let Some(field) = fields.get("require_valid_from_before_valid_to") {
            schema.require_valid_from_before_valid_to =
                expect_bool(&field.value, "require_valid_from_before_valid_to")?;
        }
        schema.valid_from_min = parse_optional_i64_field(&fields, "valid_from_min")?;
        schema.valid_from_max = parse_optional_i64_field(&fields, "valid_from_max")?;
        schema.valid_to_min = parse_optional_i64_field(&fields, "valid_to_min")?;
        schema.valid_to_max = parse_optional_i64_field(&fields, "valid_to_max")?;
        if let Some(field) = fields.get("allow_open_ended_valid_to") {
            schema.allow_open_ended_valid_to =
                expect_bool(&field.value, "allow_open_ended_valid_to")?;
        }
        Ok(schema)
    }

    fn parse_set_options(
        &mut self,
        literal: &GqlSchemaLiteral,
    ) -> Result<GraphSchemaSetOptions, EngineError> {
        let value = self.schema_literal_to_value(literal, "schema OPTIONS map")?;
        let fields = object_fields(
            &value,
            "schema OPTIONS",
            &["max_violations", "chunk_size", "scan_limit"],
        )?;
        let mut options = GraphSchemaSetOptions::default();
        if let Some(field) = fields.get("max_violations") {
            options.max_violations = expect_usize(&field.value, "max_violations")?;
        }
        if let Some(field) = fields.get("chunk_size") {
            options.chunk_size = expect_usize(&field.value, "chunk_size")?;
            if options.chunk_size == 0 {
                return Err(gql_schema_semantic_error(
                    "OPTIONS chunk_size must be positive",
                    field.value.span.clone(),
                ));
            }
        }
        if let Some(field) = fields.get("scan_limit") {
            options.scan_limit = expect_optional_u64(&field.value, "scan_limit")?;
        }
        Ok(options)
    }

    fn parse_check_options(
        &mut self,
        literal: &GqlSchemaLiteral,
    ) -> Result<GraphSchemaCheckOptions, EngineError> {
        let value = self.schema_literal_to_value(literal, "schema OPTIONS map")?;
        let fields = object_fields(
            &value,
            "schema OPTIONS",
            &["max_violations", "chunk_size", "scan_limit"],
        )?;
        let mut options = GraphSchemaCheckOptions::default();
        if let Some(field) = fields.get("max_violations") {
            options.max_violations = expect_usize(&field.value, "max_violations")?;
        }
        if let Some(field) = fields.get("chunk_size") {
            options.chunk_size = expect_usize(&field.value, "chunk_size")?;
            if options.chunk_size == 0 {
                return Err(gql_schema_semantic_error(
                    "OPTIONS chunk_size must be positive",
                    field.value.span.clone(),
                ));
            }
        }
        if let Some(field) = fields.get("scan_limit") {
            options.scan_limit = expect_optional_u64(&field.value, "scan_limit")?;
        }
        Ok(options)
    }
}

fn graph_schema_set_operations(schema: &GraphSchema) -> Vec<GraphSchemaOperation> {
    schema
        .node_schemas
        .iter()
        .map(|info| GraphSchemaOperation::SetNode {
            label: info.label.clone(),
            schema: info.schema.clone(),
        })
        .chain(
            schema
                .edge_schemas
                .iter()
                .map(|info| GraphSchemaOperation::SetEdge {
                    label: info.label.clone(),
                    schema: info.schema.clone(),
                }),
        )
        .collect()
}

fn validate_schema_label(label: &GqlSchemaLabel) -> Result<(), EngineError> {
    validate_label_token_name(&label.name).map_err(|err| match err {
        EngineError::InvalidOperation(message) => {
            gql_schema_semantic_error(message, label.span.clone())
        }
        other => other,
    })
}

fn reject_duplicate_target(
    seen: &mut BTreeSet<String>,
    label: &str,
    target_kind: &str,
    span: &SourceSpan,
) -> Result<(), EngineError> {
    if !seen.insert(label.to_string()) {
        return Err(gql_schema_semantic_error(
            format!("duplicate {target_kind} schema target '{label}'"),
            span.clone(),
        ));
    }
    Ok(())
}

type ObjectFields<'a> = BTreeMap<String, &'a SchemaField>;

fn object_fields<'a>(
    value: &'a SchemaValue,
    context: &str,
    allowed: &[&str],
) -> Result<ObjectFields<'a>, EngineError> {
    let fields = expect_map(value, context)?;
    let mut by_name = BTreeMap::new();
    for field in fields {
        if !allowed.contains(&field.key.as_str()) {
            return Err(gql_schema_semantic_error(
                format!("unknown {context} field '{}'", field.key),
                field.key_span.clone(),
            ));
        }
        if by_name.insert(field.key.clone(), field).is_some() {
            return Err(gql_schema_semantic_error(
                format!("duplicate {context} field '{}'", field.key),
                field.key_span.clone(),
            ));
        }
    }
    Ok(by_name)
}

fn expect_map<'a>(value: &'a SchemaValue, context: &str) -> Result<&'a [SchemaField], EngineError> {
    match &value.kind {
        SchemaValueKind::Map(fields) => Ok(fields),
        _ => Err(gql_schema_semantic_error(
            format!("{context} must be a map"),
            value.span.clone(),
        )),
    }
}

fn parse_optional_map<T>(
    value: &SchemaValue,
    context: &str,
    parse: impl FnOnce(&SchemaValue) -> Result<T, EngineError>,
) -> Result<Option<T>, EngineError> {
    if matches!(&value.kind, SchemaValueKind::Null) {
        return Ok(None);
    }
    expect_map(value, context)?;
    parse(value).map(Some)
}

fn parse_additional_properties(
    value: &SchemaValue,
) -> Result<SchemaAdditionalProperties, EngineError> {
    match expect_string(value, "additional_properties")? {
        "allow" => Ok(SchemaAdditionalProperties::Allow),
        "reject" => Ok(SchemaAdditionalProperties::Reject),
        other => Err(gql_schema_semantic_error(
            format!("invalid additional_properties value '{other}'"),
            value.span.clone(),
        )),
    }
}

fn parse_schema_value_types(value: &SchemaValue) -> Result<Vec<SchemaValueType>, EngineError> {
    parse_string_list(value, "types")?
        .into_iter()
        .map(|name| {
            Ok(match name.as_str() {
                "bool" => SchemaValueType::Bool,
                "int" => SchemaValueType::Int,
                "uint" => SchemaValueType::UInt,
                "float" => SchemaValueType::Float,
                "number" => SchemaValueType::Number,
                "string" => SchemaValueType::String,
                "bytes" => SchemaValueType::Bytes,
                "array" => SchemaValueType::Array,
                "map" => SchemaValueType::Map,
                _ => {
                    return Err(gql_schema_semantic_error(
                        format!("invalid schema value type '{name}'"),
                        value.span.clone(),
                    ));
                }
            })
        })
        .collect()
}

fn parse_vector_presence(value: &SchemaValue) -> Result<SchemaVectorPresence, EngineError> {
    match expect_string(value, "presence")? {
        "optional" => Ok(SchemaVectorPresence::Optional),
        "required" => Ok(SchemaVectorPresence::Required),
        "forbidden" => Ok(SchemaVectorPresence::Forbidden),
        other => Err(gql_schema_semantic_error(
            format!("invalid vector presence '{other}'"),
            value.span.clone(),
        )),
    }
}

fn parse_schema_numeric_bound(value: &SchemaValue) -> Result<SchemaNumericBound, EngineError> {
    let fields = object_fields(value, "numeric bound", &["value", "inclusive"])?;
    let value_field = fields.get("value").ok_or_else(|| {
        gql_schema_semantic_error("numeric bound requires value", value.span.clone())
    })?;
    let inclusive = fields
        .get("inclusive")
        .map(|field| expect_bool(&field.value, "inclusive"))
        .transpose()?
        .unwrap_or(true);
    Ok(SchemaNumericBound {
        value: schema_value_to_prop_value(&value_field.value)?,
        inclusive,
    })
}

fn parse_prop_value_list(value: &SchemaValue) -> Result<Vec<PropValue>, EngineError> {
    match &value.kind {
        SchemaValueKind::List(values) => values.iter().map(schema_value_to_prop_value).collect(),
        _ => Err(gql_schema_semantic_error(
            "enum_values must be a list",
            value.span.clone(),
        )),
    }
}

fn schema_value_to_prop_value(value: &SchemaValue) -> Result<PropValue, EngineError> {
    match &value.kind {
        SchemaValueKind::Null => Ok(PropValue::Null),
        SchemaValueKind::Bool(value) => Ok(PropValue::Bool(*value)),
        SchemaValueKind::Int(value) => Ok(PropValue::Int(*value)),
        SchemaValueKind::UInt(value) => Ok(PropValue::UInt(*value)),
        SchemaValueKind::Float(value) if value.is_finite() => Ok(PropValue::Float(*value)),
        SchemaValueKind::Float(_) => Err(gql_schema_semantic_error(
            "schema property float values must be finite",
            value.span.clone(),
        )),
        SchemaValueKind::String(value) => Ok(PropValue::String(value.clone())),
        SchemaValueKind::Bytes(value) => Ok(PropValue::Bytes(value.clone())),
        SchemaValueKind::List(values) => values
            .iter()
            .map(schema_value_to_prop_value)
            .collect::<Result<Vec<_>, _>>()
            .map(PropValue::Array),
        SchemaValueKind::Map(fields) => {
            if let Some(tagged) = try_tagged_prop_value(fields, &value.span)? {
                return Ok(tagged);
            }
            let mut map = BTreeMap::new();
            for field in fields {
                if map.contains_key(&field.key) {
                    return Err(gql_schema_semantic_error(
                        format!("duplicate map literal field '{}'", field.key),
                        field.key_span.clone(),
                    ));
                }
                map.insert(field.key.clone(), schema_value_to_prop_value(&field.value)?);
            }
            Ok(PropValue::Map(map))
        }
    }
}

fn try_tagged_prop_value(
    fields: &[SchemaField],
    span: &SourceSpan,
) -> Result<Option<PropValue>, EngineError> {
    let Some(type_field) = fields.iter().find(|field| field.key == "type") else {
        return Ok(None);
    };
    let SchemaValueKind::String(tag) = &type_field.value.kind else {
        return Ok(None);
    };
    match tag.as_str() {
        "uint" => parse_tagged_uint(fields, span).map(Some),
        "bytes" => parse_tagged_bytes(fields, span).map(Some),
        _ => Ok(None),
    }
}

fn parse_tagged_uint(fields: &[SchemaField], span: &SourceSpan) -> Result<PropValue, EngineError> {
    let value = tagged_value_field(fields, "uint", span)?;
    let SchemaValueKind::String(value) = &value.kind else {
        return Err(gql_schema_semantic_error(
            "tagged uint value must be a decimal string",
            value.span.clone(),
        ));
    };
    let parsed = value.parse::<u64>().map_err(|_| {
        gql_schema_semantic_error("tagged uint value must fit in u64", span.clone())
    })?;
    Ok(PropValue::UInt(parsed))
}

fn parse_tagged_bytes(fields: &[SchemaField], span: &SourceSpan) -> Result<PropValue, EngineError> {
    let value = tagged_value_field(fields, "bytes", span)?;
    let SchemaValueKind::List(items) = &value.kind else {
        return Err(gql_schema_semantic_error(
            "tagged bytes value must be a list",
            value.span.clone(),
        ));
    };
    let mut bytes = Vec::with_capacity(items.len());
    for item in items {
        let byte = match &item.kind {
            SchemaValueKind::Int(value) if (0..=255).contains(value) => *value as u8,
            SchemaValueKind::UInt(value) if *value <= 255 => *value as u8,
            _ => {
                return Err(gql_schema_semantic_error(
                    "tagged bytes values must be integers in [0, 255]",
                    item.span.clone(),
                ));
            }
        };
        bytes.push(byte);
    }
    Ok(PropValue::Bytes(bytes))
}

fn tagged_value_field<'a>(
    fields: &'a [SchemaField],
    tag: &str,
    span: &SourceSpan,
) -> Result<&'a SchemaValue, EngineError> {
    if fields.len() != 2 {
        return Err(gql_schema_semantic_error(
            format!("tagged {tag} literal must contain exactly type and value"),
            span.clone(),
        ));
    }
    fields
        .iter()
        .find(|field| field.key == "value")
        .map(|field| &field.value)
        .ok_or_else(|| {
            gql_schema_semantic_error(format!("tagged {tag} literal requires value"), span.clone())
        })
}

fn parse_optional_usize_field(
    fields: &ObjectFields<'_>,
    name: &str,
) -> Result<Option<usize>, EngineError> {
    fields
        .get(name)
        .map(|field| expect_optional_usize(&field.value, name))
        .transpose()
        .map(Option::flatten)
}

fn parse_optional_u32_field(
    fields: &ObjectFields<'_>,
    name: &str,
) -> Result<Option<u32>, EngineError> {
    fields
        .get(name)
        .map(|field| expect_optional_u32(&field.value, name))
        .transpose()
        .map(Option::flatten)
}

fn parse_optional_i64_field(
    fields: &ObjectFields<'_>,
    name: &str,
) -> Result<Option<i64>, EngineError> {
    fields
        .get(name)
        .map(|field| expect_optional_i64(&field.value, name))
        .transpose()
        .map(Option::flatten)
}

fn parse_optional_string_list_field(
    fields: &ObjectFields<'_>,
    name: &str,
) -> Result<Vec<String>, EngineError> {
    fields
        .get(name)
        .map(|field| parse_string_list(&field.value, name))
        .transpose()
        .map(Option::unwrap_or_default)
}

fn parse_string_list(value: &SchemaValue, name: &str) -> Result<Vec<String>, EngineError> {
    match &value.kind {
        SchemaValueKind::List(values) => values
            .iter()
            .map(|value| expect_string(value, name).map(str::to_string))
            .collect(),
        _ => Err(gql_schema_semantic_error(
            format!("{name} must be a list of strings"),
            value.span.clone(),
        )),
    }
}

fn expect_string<'a>(value: &'a SchemaValue, name: &str) -> Result<&'a str, EngineError> {
    match &value.kind {
        SchemaValueKind::String(value) => Ok(value),
        _ => Err(gql_schema_semantic_error(
            format!("{name} must be a string"),
            value.span.clone(),
        )),
    }
}

fn expect_bool(value: &SchemaValue, name: &str) -> Result<bool, EngineError> {
    match &value.kind {
        SchemaValueKind::Bool(value) => Ok(*value),
        _ => Err(gql_schema_semantic_error(
            format!("{name} must be a boolean"),
            value.span.clone(),
        )),
    }
}

fn expect_optional_usize(value: &SchemaValue, name: &str) -> Result<Option<usize>, EngineError> {
    if matches!(&value.kind, SchemaValueKind::Null) {
        return Ok(None);
    }
    expect_usize(value, name).map(Some)
}

fn expect_usize(value: &SchemaValue, name: &str) -> Result<usize, EngineError> {
    match &value.kind {
        SchemaValueKind::Int(inner) if *inner >= 0 => usize::try_from(*inner).map_err(|_| {
            gql_schema_semantic_error(format!("{name} must fit in usize"), value.span.clone())
        }),
        SchemaValueKind::UInt(inner) => usize::try_from(*inner).map_err(|_| {
            gql_schema_semantic_error(format!("{name} must fit in usize"), value.span.clone())
        }),
        _ => Err(gql_schema_semantic_error(
            format!("{name} must be a non-negative integer"),
            value.span.clone(),
        )),
    }
}

fn expect_optional_u32(value: &SchemaValue, name: &str) -> Result<Option<u32>, EngineError> {
    if matches!(&value.kind, SchemaValueKind::Null) {
        return Ok(None);
    }
    match &value.kind {
        SchemaValueKind::Int(inner) if *inner >= 0 => {
            u32::try_from(*inner).map(Some).map_err(|_| {
                gql_schema_semantic_error(format!("{name} must fit in u32"), value.span.clone())
            })
        }
        SchemaValueKind::UInt(inner) => u32::try_from(*inner).map(Some).map_err(|_| {
            gql_schema_semantic_error(format!("{name} must fit in u32"), value.span.clone())
        }),
        _ => Err(gql_schema_semantic_error(
            format!("{name} must be a non-negative integer"),
            value.span.clone(),
        )),
    }
}

fn expect_optional_i64(value: &SchemaValue, name: &str) -> Result<Option<i64>, EngineError> {
    if matches!(&value.kind, SchemaValueKind::Null) {
        return Ok(None);
    }
    match &value.kind {
        SchemaValueKind::Int(inner) => Ok(Some(*inner)),
        SchemaValueKind::UInt(inner) => i64::try_from(*inner).map(Some).map_err(|_| {
            gql_schema_semantic_error(format!("{name} must fit in i64"), value.span.clone())
        }),
        _ => Err(gql_schema_semantic_error(
            format!("{name} must be an integer"),
            value.span.clone(),
        )),
    }
}

fn expect_optional_u64(value: &SchemaValue, name: &str) -> Result<Option<u64>, EngineError> {
    if matches!(&value.kind, SchemaValueKind::Null) {
        return Ok(None);
    }
    match &value.kind {
        SchemaValueKind::Int(inner) if *inner >= 0 => Ok(Some(*inner as u64)),
        SchemaValueKind::UInt(inner) => Ok(Some(*inner)),
        _ => Err(gql_schema_semantic_error(
            format!("{name} must be null or a non-negative integer"),
            value.span.clone(),
        )),
    }
}

fn schema_shape_error(err: EngineError, span: SourceSpan) -> EngineError {
    match err {
        EngineError::InvalidOperation(message) => gql_schema_semantic_error(message, span),
        other => other,
    }
}

fn gql_schema_semantic_error(message: impl Into<String>, span: SourceSpan) -> EngineError {
    EngineError::GqlSemantic {
        code: GqlSemanticErrorCode::InvalidParameter,
        message: message.into(),
        span,
    }
}

pub(crate) fn gql_value_from_node_schema(schema: &NodeSchema) -> GqlValue {
    gql_schema_map([
        (
            "additional_properties",
            GqlValue::String(schema_additional_properties_name(
                schema.additional_properties,
            )),
        ),
        (
            "properties",
            gql_value_from_property_map(&schema.properties),
        ),
        (
            "key",
            gql_value_from_option(schema.key.as_ref(), gql_value_from_string_field_schema),
        ),
        (
            "label_constraints",
            gql_value_from_option(
                schema.label_constraints.as_ref(),
                gql_value_from_node_label_constraints,
            ),
        ),
        (
            "weight",
            gql_value_from_option(schema.weight.as_ref(), gql_value_from_numeric_field_schema),
        ),
        (
            "dense_vector",
            gql_value_from_option(
                schema.dense_vector.as_ref(),
                gql_value_from_dense_vector_schema,
            ),
        ),
        (
            "sparse_vector",
            gql_value_from_option(
                schema.sparse_vector.as_ref(),
                gql_value_from_sparse_vector_schema,
            ),
        ),
    ])
}

pub(crate) fn gql_value_from_edge_schema(schema: &EdgeSchema) -> GqlValue {
    gql_schema_map([
        (
            "additional_properties",
            GqlValue::String(schema_additional_properties_name(
                schema.additional_properties,
            )),
        ),
        (
            "properties",
            gql_value_from_property_map(&schema.properties),
        ),
        (
            "from",
            gql_value_from_option(schema.from.as_ref(), gql_value_from_endpoint_label_schema),
        ),
        (
            "to",
            gql_value_from_option(schema.to.as_ref(), gql_value_from_endpoint_label_schema),
        ),
        ("allow_self_loops", GqlValue::Bool(schema.allow_self_loops)),
        (
            "weight",
            gql_value_from_option(schema.weight.as_ref(), gql_value_from_numeric_field_schema),
        ),
        (
            "validity",
            gql_value_from_option(
                schema.validity.as_ref(),
                gql_value_from_edge_validity_schema,
            ),
        ),
    ])
}

pub(crate) fn gql_value_from_schema_violation(violation: &SchemaViolation) -> GqlValue {
    gql_schema_map([
        (
            "target",
            gql_value_from_schema_violation_target(&violation.target),
        ),
        ("path", GqlValue::String(violation.path.clone())),
        ("message", GqlValue::String(violation.message.clone())),
    ])
}

pub(crate) fn gql_schema_target_kind_name(value: SchemaTargetKind) -> &'static str {
    match value {
        SchemaTargetKind::Node => "node",
        SchemaTargetKind::Edge => "edge",
    }
}

fn gql_schema_map<const N: usize>(entries: [(&str, GqlValue); N]) -> GqlValue {
    GqlValue::Map(
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    )
}

fn gql_value_from_option<T>(value: Option<&T>, convert: impl FnOnce(&T) -> GqlValue) -> GqlValue {
    value.map(convert).unwrap_or(GqlValue::Null)
}

fn gql_value_from_usize(value: usize) -> GqlValue {
    GqlValue::UInt(value as u64)
}

fn gql_value_from_optional_usize(value: Option<usize>) -> GqlValue {
    value.map(gql_value_from_usize).unwrap_or(GqlValue::Null)
}

fn gql_value_from_optional_u32(value: Option<u32>) -> GqlValue {
    value
        .map(|value| GqlValue::UInt(value as u64))
        .unwrap_or(GqlValue::Null)
}

fn gql_value_from_optional_i64(value: Option<i64>) -> GqlValue {
    value.map(GqlValue::Int).unwrap_or(GqlValue::Null)
}

fn gql_value_from_string_list(values: &[String]) -> GqlValue {
    GqlValue::List(values.iter().cloned().map(GqlValue::String).collect())
}

fn gql_value_from_property_map(properties: &BTreeMap<String, PropertySchema>) -> GqlValue {
    GqlValue::Map(
        properties
            .iter()
            .map(|(key, schema)| (key.clone(), gql_value_from_property_schema(schema)))
            .collect(),
    )
}

fn gql_value_from_property_schema(schema: &PropertySchema) -> GqlValue {
    gql_schema_map([
        ("required", GqlValue::Bool(schema.required)),
        ("nullable", GqlValue::Bool(schema.nullable)),
        (
            "types",
            GqlValue::List(
                schema
                    .types
                    .iter()
                    .map(|kind| GqlValue::String(schema_value_type_name(*kind)))
                    .collect(),
            ),
        ),
        (
            "numeric_min",
            gql_value_from_option(schema.numeric_min.as_ref(), gql_value_from_numeric_bound),
        ),
        (
            "numeric_max",
            gql_value_from_option(schema.numeric_max.as_ref(), gql_value_from_numeric_bound),
        ),
        (
            "string_min_bytes",
            gql_value_from_optional_usize(schema.string_min_bytes),
        ),
        (
            "string_max_bytes",
            gql_value_from_optional_usize(schema.string_max_bytes),
        ),
        (
            "bytes_min_len",
            gql_value_from_optional_usize(schema.bytes_min_len),
        ),
        (
            "bytes_max_len",
            gql_value_from_optional_usize(schema.bytes_max_len),
        ),
        (
            "array_min_items",
            gql_value_from_optional_usize(schema.array_min_items),
        ),
        (
            "array_max_items",
            gql_value_from_optional_usize(schema.array_max_items),
        ),
        (
            "map_min_entries",
            gql_value_from_optional_usize(schema.map_min_entries),
        ),
        (
            "map_max_entries",
            gql_value_from_optional_usize(schema.map_max_entries),
        ),
        (
            "enum_values",
            GqlValue::List(
                schema
                    .enum_values
                    .iter()
                    .map(gql_schema_value_from_prop)
                    .collect(),
            ),
        ),
    ])
}

fn gql_value_from_numeric_bound(bound: &SchemaNumericBound) -> GqlValue {
    gql_schema_map([
        ("value", gql_schema_value_from_prop(&bound.value)),
        ("inclusive", GqlValue::Bool(bound.inclusive)),
    ])
}

fn gql_value_from_string_field_schema(schema: &StringFieldSchema) -> GqlValue {
    gql_schema_map([
        ("min_bytes", gql_value_from_optional_usize(schema.min_bytes)),
        ("max_bytes", gql_value_from_optional_usize(schema.max_bytes)),
        (
            "enum_values",
            gql_value_from_string_list(&schema.enum_values),
        ),
    ])
}

fn gql_value_from_numeric_field_schema(schema: &NumericFieldSchema) -> GqlValue {
    gql_schema_map([
        (
            "min",
            gql_value_from_option(schema.min.as_ref(), gql_value_from_numeric_bound),
        ),
        (
            "max",
            gql_value_from_option(schema.max.as_ref(), gql_value_from_numeric_bound),
        ),
        ("finite", GqlValue::Bool(schema.finite)),
    ])
}

fn gql_value_from_node_label_constraints(schema: &NodeLabelConstraintSchema) -> GqlValue {
    gql_schema_map([
        ("all_of", gql_value_from_string_list(&schema.all_of)),
        ("any_of", gql_value_from_string_list(&schema.any_of)),
        ("none_of", gql_value_from_string_list(&schema.none_of)),
    ])
}

fn gql_value_from_dense_vector_schema(schema: &DenseVectorSchema) -> GqlValue {
    gql_schema_map([
        (
            "presence",
            GqlValue::String(schema_vector_presence_name(schema.presence)),
        ),
        ("dimension", gql_value_from_optional_usize(schema.dimension)),
    ])
}

fn gql_value_from_sparse_vector_schema(schema: &SparseVectorSchema) -> GqlValue {
    gql_schema_map([
        (
            "presence",
            GqlValue::String(schema_vector_presence_name(schema.presence)),
        ),
        (
            "min_entries",
            gql_value_from_optional_usize(schema.min_entries),
        ),
        (
            "max_entries",
            gql_value_from_optional_usize(schema.max_entries),
        ),
        (
            "max_dimension_id",
            gql_value_from_optional_u32(schema.max_dimension_id),
        ),
    ])
}

fn gql_value_from_endpoint_label_schema(schema: &EndpointLabelSchema) -> GqlValue {
    gql_schema_map([
        ("all_of", gql_value_from_string_list(&schema.all_of)),
        ("any_of", gql_value_from_string_list(&schema.any_of)),
        ("none_of", gql_value_from_string_list(&schema.none_of)),
    ])
}

fn gql_value_from_edge_validity_schema(schema: &EdgeValiditySchema) -> GqlValue {
    gql_schema_map([
        (
            "require_valid_from_before_valid_to",
            GqlValue::Bool(schema.require_valid_from_before_valid_to),
        ),
        (
            "valid_from_min",
            gql_value_from_optional_i64(schema.valid_from_min),
        ),
        (
            "valid_from_max",
            gql_value_from_optional_i64(schema.valid_from_max),
        ),
        (
            "valid_to_min",
            gql_value_from_optional_i64(schema.valid_to_min),
        ),
        (
            "valid_to_max",
            gql_value_from_optional_i64(schema.valid_to_max),
        ),
        (
            "allow_open_ended_valid_to",
            GqlValue::Bool(schema.allow_open_ended_valid_to),
        ),
    ])
}

fn gql_schema_value_from_prop(value: &PropValue) -> GqlValue {
    match value {
        PropValue::Null => GqlValue::Null,
        PropValue::Bool(value) => GqlValue::Bool(*value),
        PropValue::Int(value) => GqlValue::Int(*value),
        PropValue::UInt(value) => gql_tagged_uint(*value),
        PropValue::Float(value) => GqlValue::Float(*value),
        PropValue::String(value) => GqlValue::String(value.clone()),
        PropValue::Bytes(value) => gql_tagged_bytes(value),
        PropValue::Array(values) => {
            GqlValue::List(values.iter().map(gql_schema_value_from_prop).collect())
        }
        PropValue::Map(values) => GqlValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), gql_schema_value_from_prop(value)))
                .collect(),
        ),
    }
}

fn gql_value_from_schema_violation_target(target: &SchemaViolationTarget) -> GqlValue {
    match target {
        SchemaViolationTarget::Node { id, labels, key } => gql_schema_map([
            ("kind", GqlValue::String("node".to_string())),
            ("id", gql_tagged_uint(*id)),
            ("labels", gql_value_from_string_list(labels)),
            ("key", GqlValue::String(key.clone())),
        ]),
        SchemaViolationTarget::Edge {
            id,
            label,
            from,
            to,
        } => gql_schema_map([
            ("kind", GqlValue::String("edge".to_string())),
            ("id", gql_tagged_uint(*id)),
            ("label", GqlValue::String(label.clone())),
            ("from", gql_tagged_uint(*from)),
            ("to", gql_tagged_uint(*to)),
        ]),
    }
}

fn gql_tagged_uint(value: u64) -> GqlValue {
    gql_schema_map([
        ("type", GqlValue::String("uint".to_string())),
        ("value", GqlValue::String(value.to_string())),
    ])
}

fn gql_tagged_bytes(value: &[u8]) -> GqlValue {
    gql_schema_map([
        ("type", GqlValue::String("bytes".to_string())),
        (
            "value",
            GqlValue::List(
                value
                    .iter()
                    .map(|byte| GqlValue::Int(i64::from(*byte)))
                    .collect(),
            ),
        ),
    ])
}

fn schema_additional_properties_name(value: SchemaAdditionalProperties) -> String {
    match value {
        SchemaAdditionalProperties::Allow => "allow",
        SchemaAdditionalProperties::Reject => "reject",
    }
    .to_string()
}

fn schema_value_type_name(value: SchemaValueType) -> String {
    match value {
        SchemaValueType::Bool => "bool",
        SchemaValueType::Int => "int",
        SchemaValueType::UInt => "uint",
        SchemaValueType::Float => "float",
        SchemaValueType::Number => "number",
        SchemaValueType::String => "string",
        SchemaValueType::Bytes => "bytes",
        SchemaValueType::Array => "array",
        SchemaValueType::Map => "map",
    }
    .to_string()
}

fn schema_vector_presence_name(value: SchemaVectorPresence) -> String {
    match value {
        SchemaVectorPresence::Optional => "optional",
        SchemaVectorPresence::Required => "required",
        SchemaVectorPresence::Forbidden => "forbidden",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::parser::{parse_statement, GqlParseOptions};

    fn bind(source: &str) -> Result<GqlSchemaSemanticPlan, EngineError> {
        bind_with_params(source, GqlParams::new())
    }

    fn bind_with_params(
        source: &str,
        params: GqlParams,
    ) -> Result<GqlSchemaSemanticPlan, EngineError> {
        let statement = parse_statement(source, &GqlParseOptions::default())?;
        let GqlStatementBody::Schema(schema) = statement.body else {
            panic!("expected schema statement");
        };
        bind_schema_statement(schema, &params)
    }

    fn schema_map(entries: &[(&str, GqlParamValue)]) -> GqlParamValue {
        GqlParamValue::Map(
            entries
                .iter()
                .map(|(key, value)| ((*key).to_string(), value.clone()))
                .collect(),
        )
    }

    fn schema_list(values: Vec<GqlParamValue>) -> GqlParamValue {
        GqlParamValue::List(values)
    }

    fn expect_gql_schema_semantic_err(source: &str, expected: &str) {
        let err = bind(source).expect_err("schema statement should fail semantic binding");
        match err {
            EngineError::GqlSemantic { message, .. } => assert!(
                message.contains(expected),
                "expected message to contain {expected:?}, got {message:?}"
            ),
            other => panic!("expected GQL semantic error, got {other:?}"),
        }
    }

    fn expect_gql_schema_error(source: &str) {
        assert!(
            bind(source).is_err() || parse_statement(source, &GqlParseOptions::default()).is_err(),
            "expected schema source to fail: {source}"
        );
    }

    #[test]
    fn gql_schema_semantic_accepts_empty_set_and_check_set() {
        let alter = bind("ALTER CURRENT GRAPH TYPE SET {}").unwrap();
        let GqlSchemaSemanticPlan::Alter(alter) = alter else {
            panic!("expected alter plan");
        };
        assert_eq!(alter.mode, GqlGraphTypeAlterMode::Set);
        assert!(alter.schema.as_ref().unwrap().node_schemas.is_empty());
        assert!(alter.operations.is_empty());

        let check = bind("CHECK CURRENT GRAPH TYPE SET {}").unwrap();
        let GqlSchemaSemanticPlan::Check(check) = check else {
            panic!("expected check plan");
        };
        assert_eq!(check.mode, GqlGraphTypeCheckMode::Set);
        assert!(check.schema.node_schemas.is_empty());
        assert!(check.schema.edge_schemas.is_empty());
    }

    #[test]
    fn gql_schema_semantic_rejects_empty_add_check_add_and_drop() {
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE ADD {}",
            "ADD requires at least one",
        );
        expect_gql_schema_semantic_err(
            "CHECK CURRENT GRAPH TYPE ADD {}",
            "ADD requires at least one",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE DROP {}",
            "DROP requires at least one",
        );
    }

    #[test]
    fn gql_schema_semantic_rejects_duplicate_schema_targets() {
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE ADD { NODE Person = {}, NODE Person = {} }",
            "duplicate node schema target",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE ADD { EDGE KNOWS = {}, EDGE KNOWS = {} }",
            "duplicate edge schema target",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE DROP { NODE Person, NODE Person }",
            "duplicate node drop schema target",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE DROP { EDGE KNOWS, EDGE KNOWS }",
            "duplicate edge drop schema target",
        );
    }

    #[test]
    fn gql_schema_semantic_rejects_unknown_and_camel_case_fields() {
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { unknown_field: true } }",
            "unknown node schema field",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { additionalProperties: 'allow' } }",
            "unknown node schema field",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET { EDGE KNOWS = { allowSelfLoops: false } }",
            "unknown edge schema field",
        );
    }

    #[test]
    fn gql_schema_semantic_rejects_unknown_and_invalid_options() {
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET {} OPTIONS { bogus: 1 }",
            "unknown schema OPTIONS field",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET {} OPTIONS { chunk_size: 0 }",
            "chunk_size must be positive",
        );
        expect_gql_schema_semantic_err(
            "CHECK CURRENT GRAPH TYPE SET {} OPTIONS { max_violations: -1 }",
            "max_violations must be a non-negative integer",
        );
        expect_gql_schema_semantic_err(
            "CHECK CURRENT GRAPH TYPE SET {} OPTIONS { scan_limit: -1 }",
            "scan_limit must be null or a non-negative integer",
        );
        expect_gql_schema_semantic_err(
            "CHECK CURRENT GRAPH TYPE SET {} OPTIONS { scan_limit: 1.5 }",
            "scan_limit must be null or a non-negative integer",
        );
    }

    #[test]
    fn gql_schema_semantic_converts_full_node_and_edge_dtos() {
        let plan = bind(
            "ALTER CURRENT GRAPH TYPE ADD {
                NODE Person = {
                    additional_properties: 'reject',
                    key: { min_bytes: 1, max_bytes: 64, enum_values: ['p1'] },
                    label_constraints: { all_of: ['Base'], any_of: ['Person'], none_of: ['Archived'] },
                    weight: { min: { value: -1, inclusive: true }, max: { value: 100, inclusive: false }, finite: false },
                    dense_vector: { presence: 'required', dimension: 3 },
                    sparse_vector: { presence: 'optional', min_entries: 0, max_entries: 3, max_dimension_id: 99 },
                    properties: {
                        name: { required: true, nullable: false, types: ['string'], string_min_bytes: 1, string_max_bytes: 128 },
                        score: {
                            required: false,
                            nullable: true,
                            types: ['number', 'uint', 'bytes', 'array', 'map', 'bool', 'int', 'float'],
                            numeric_min: { value: { type: 'uint', value: '0' }, inclusive: true },
                            numeric_max: { value: 99.5, inclusive: false },
                            bytes_min_len: 1,
                            bytes_max_len: 8,
                            array_min_items: 0,
                            array_max_items: 4,
                            map_min_entries: 0,
                            map_max_entries: 4,
                            enum_values: [{ type: 'bytes', value: [0, 1, 255] }, { type: 'uint', value: '18446744073709551615' }]
                        }
                    }
                },
                EDGE WORKS_ON = {
                    additional_properties: 'allow',
                    from: { all_of: ['Person'], any_of: ['Employee'], none_of: ['Archived'] },
                    to: { all_of: ['Project'], any_of: ['Team'], none_of: ['Closed'] },
                    allow_self_loops: false,
                    weight: { min: { value: 0, inclusive: true }, max: { value: 10, inclusive: true }, finite: true },
                    validity: {
                        require_valid_from_before_valid_to: true,
                        valid_from_min: -10,
                        valid_from_max: 10,
                        valid_to_min: 0,
                        valid_to_max: 20,
                        allow_open_ended_valid_to: false
                    },
                    properties: {
                        role: { required: false, nullable: false, types: ['string'], enum_values: ['lead'] }
                    }
                }
            } OPTIONS { max_violations: 2, chunk_size: 8, scan_limit: 0 }",
        )
        .unwrap();
        let GqlSchemaSemanticPlan::Alter(alter) = plan else {
            panic!("expected alter plan");
        };
        assert_eq!(alter.mode, GqlGraphTypeAlterMode::Add);
        assert_eq!(alter.options.max_violations, 2);
        assert_eq!(alter.options.chunk_size, 8);
        assert_eq!(alter.options.scan_limit, Some(0));
        let schema = alter.schema.as_ref().unwrap();
        assert_eq!(schema.node_schemas.len(), 1);
        assert_eq!(schema.edge_schemas.len(), 1);
        let node = &schema.node_schemas[0].schema;
        assert_eq!(
            node.additional_properties,
            SchemaAdditionalProperties::Reject
        );
        assert_eq!(node.key.as_ref().unwrap().min_bytes, Some(1));
        assert_eq!(
            node.label_constraints.as_ref().unwrap().none_of,
            vec!["Archived"]
        );
        assert_eq!(
            node.dense_vector.as_ref().unwrap().presence,
            SchemaVectorPresence::Required
        );
        let score = node.properties.get("score").unwrap();
        assert!(matches!(
            score.enum_values.as_slice(),
            [PropValue::Bytes(bytes), PropValue::UInt(u)] if bytes == &vec![0, 1, 255] && *u == u64::MAX
        ));
        let edge = &schema.edge_schemas[0].schema;
        assert!(!edge.allow_self_loops);
        assert_eq!(edge.from.as_ref().unwrap().all_of, vec!["Person"]);
        assert!(
            edge.validity
                .as_ref()
                .unwrap()
                .require_valid_from_before_valid_to
        );
        assert_eq!(alter.operations.len(), 2);
    }

    #[test]
    fn gql_schema_semantic_converts_schema_and_options_parameters() {
        let mut params = GqlParams::new();
        params.insert(
            "person".to_string(),
            schema_map(&[(
                "properties",
                schema_map(&[(
                    "name",
                    schema_map(&[(
                        "types",
                        schema_list(vec![GqlParamValue::String("string".to_string())]),
                    )]),
                )]),
            )]),
        );
        params.insert(
            "options".to_string(),
            schema_map(&[
                ("max_violations", GqlParamValue::UInt(7)),
                ("chunk_size", GqlParamValue::Int(16)),
                ("scan_limit", GqlParamValue::Null),
            ]),
        );
        let plan = bind_with_params(
            "CHECK CURRENT GRAPH TYPE ADD { NODE Person = $person } OPTIONS $options",
            params,
        )
        .unwrap();
        let GqlSchemaSemanticPlan::Check(check) = plan else {
            panic!("expected check plan");
        };
        assert_eq!(check.options.max_violations, 7);
        assert_eq!(check.options.chunk_size, 16);
        assert_eq!(check.options.scan_limit, None);
        assert_eq!(check.parameters, vec!["options", "person"]);
        assert_eq!(check.schema.node_schemas[0].label, "Person");
    }

    #[test]
    fn gql_schema_semantic_rejects_missing_and_incompatible_parameters() {
        let err = bind("ALTER CURRENT GRAPH TYPE SET { NODE Person = $person }").unwrap_err();
        assert!(matches!(err, EngineError::GqlParameter { name, .. } if name == "person"));

        let mut params = GqlParams::new();
        params.insert(
            "person".to_string(),
            GqlParamValue::String("bad".to_string()),
        );
        let err = bind_with_params(
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = $person }",
            params,
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::GqlSemantic { .. }));

        let mut params = GqlParams::new();
        params.insert("bad_float".to_string(), GqlParamValue::Float(f64::INFINITY));
        let err = bind_with_params(
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { n: { enum_values: [$bad_float] } } } }",
            params,
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::GqlSemantic { .. }));
    }

    #[test]
    fn gql_schema_semantic_rejects_non_literal_schema_map_expressions() {
        for source in [
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [n] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [n.name] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [coalesce(1, 2)] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [1 + 2] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [EXISTS { MATCH (n) RETURN n }] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [MATCH] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [RETURN] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [WITH] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [CALL] } } } }",
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [UNION] } } } }",
        ] {
            expect_gql_schema_error(source);
        }
    }

    #[test]
    fn gql_schema_semantic_rejects_malformed_tagged_literals() {
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [{ type: 'uint', value: 'not-a-u64' }] } } } }",
            "tagged uint value",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [{ type: 'bytes', value: [256] }] } } } }",
            "tagged bytes values",
        );
        expect_gql_schema_semantic_err(
            "ALTER CURRENT GRAPH TYPE SET { NODE Person = { properties: { p: { enum_values: [{ type: 'bytes' }] } } } }",
            "tagged bytes literal",
        );
    }
}
