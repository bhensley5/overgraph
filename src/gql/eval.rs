use crate::error::EngineError;
use crate::gql::ast::{BinaryOp, Expr, ExprKind, Literal, MapLiteral, UnaryOp};
use crate::gql::metadata::{GqlEndpointFunction, GqlMetadataFunction};
use crate::gql::semantic::{
    edge_endpoint_id_call, gql_semantic_error, variable_name, GqlAliasKind, GqlReturnPlan,
    GqlSemanticPlan,
};
use crate::graph_row::{
    eval_graph_binary_values, eval_graph_scalar_function_values, eval_graph_unary_value,
    GraphEvalValue,
};
use crate::property_value_semantics::{
    compare_numeric_keys, numeric_key_from_f64, numeric_key_from_i64, numeric_key_from_u64,
    NumericScalarKey,
};
#[cfg(test)]
use crate::row_projection::ProjectionNeeds;
use crate::row_projection::{
    EdgeOutputProjection, EdgeProjectionField, NodeOutputProjection, NodeProjectionField,
    ProjectedRow, ProjectedValue, ProjectionColumn, ProjectionNeedClass, RowProjectionPlan,
};
use crate::types::{
    GqlParamValue, GqlParams, GqlSemanticErrorCode, GraphBinaryOp, GraphFunction, GraphUnaryOp,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlReturnExpr {
    pub(crate) expr: Expr,
    pub(crate) output_name: String,
}

#[derive(Clone, Debug)]
pub(crate) struct GqlRuntimeProjection {
    #[allow(dead_code)]
    pub(crate) plan: RowProjectionPlan,
    pub(crate) keys: Vec<GqlRuntimeValueKey>,
    key_indexes: BTreeMap<GqlRuntimeValueKey, usize>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum GqlRuntimeValueKey {
    NodeElement {
        alias: String,
    },
    EdgeElement {
        alias: String,
    },
    NodeProperty {
        alias: String,
        key: String,
    },
    EdgeProperty {
        alias: String,
        key: String,
    },
    NodeMetadata {
        alias: String,
        field: NodeProjectionField,
    },
    EdgeMetadata {
        alias: String,
        field: EdgeProjectionField,
    },
}

pub(crate) fn return_exprs(plan: &GqlSemanticPlan) -> Vec<GqlReturnExpr> {
    match &plan.returns {
        GqlReturnPlan::Star {
            expanded_aliases, ..
        } => expanded_aliases
            .iter()
            .map(|alias| GqlReturnExpr {
                expr: Expr {
                    kind: ExprKind::Variable(alias.clone()),
                    span: plan
                        .aliases
                        .get(alias)
                        .map(|binding| binding.span.clone())
                        .unwrap_or_else(|| plan.query.return_clause.span.clone()),
                },
                output_name: alias.clone(),
            })
            .collect(),
        GqlReturnPlan::Items(items) => items
            .iter()
            .map(|item| GqlReturnExpr {
                expr: item.expr.clone(),
                output_name: item.output_name.clone(),
            })
            .collect(),
    }
}

pub(crate) fn build_runtime_projection(
    exprs: &[Expr],
    plan: &GqlSemanticPlan,
    alias_projection: &BTreeMap<String, String>,
    include_variable_elements: bool,
    include_vectors: bool,
) -> Result<GqlRuntimeProjection, EngineError> {
    build_runtime_projection_excluding(
        exprs,
        plan,
        alias_projection,
        include_variable_elements,
        include_vectors,
        &BTreeSet::new(),
    )
}

#[cfg(test)]
pub(crate) fn build_runtime_projection_for_need_class(
    exprs: &[Expr],
    plan: &GqlSemanticPlan,
    alias_projection: &BTreeMap<String, String>,
    include_variable_elements: bool,
    include_vectors: bool,
    need_class: ProjectionNeedClass,
) -> Result<GqlRuntimeProjection, EngineError> {
    build_runtime_projection_excluding_for_need_class(
        exprs,
        plan,
        alias_projection,
        include_variable_elements,
        include_vectors,
        &BTreeSet::new(),
        need_class,
    )
}

pub(crate) fn build_runtime_projection_excluding(
    exprs: &[Expr],
    plan: &GqlSemanticPlan,
    alias_projection: &BTreeMap<String, String>,
    include_variable_elements: bool,
    include_vectors: bool,
    excluded_keys: &BTreeSet<GqlRuntimeValueKey>,
) -> Result<GqlRuntimeProjection, EngineError> {
    build_runtime_projection_excluding_for_need_class(
        exprs,
        plan,
        alias_projection,
        include_variable_elements,
        include_vectors,
        excluded_keys,
        ProjectionNeedClass::Output,
    )
}

pub(crate) fn build_runtime_projection_excluding_for_need_class(
    exprs: &[Expr],
    plan: &GqlSemanticPlan,
    alias_projection: &BTreeMap<String, String>,
    include_variable_elements: bool,
    include_vectors: bool,
    excluded_keys: &BTreeSet<GqlRuntimeValueKey>,
    need_class: ProjectionNeedClass,
) -> Result<GqlRuntimeProjection, EngineError> {
    let refs = runtime_projection_refs(
        exprs,
        plan,
        alias_projection,
        include_variable_elements,
        include_vectors,
        excluded_keys,
    )?;
    runtime_projection_from_refs_for_need_class(refs, need_class)
}

#[cfg(test)]
pub(crate) struct GqlRuntimeProjectionExprs<'a> {
    pub(crate) exprs: &'a [Expr],
    pub(crate) need_class: ProjectionNeedClass,
}

#[cfg(test)]
pub(crate) fn build_runtime_projection_for_need_classes(
    groups: &[GqlRuntimeProjectionExprs<'_>],
    plan: &GqlSemanticPlan,
    alias_projection: &BTreeMap<String, String>,
    include_variable_elements: bool,
    include_vectors: bool,
) -> Result<GqlRuntimeProjection, EngineError> {
    let mut all_refs = BTreeMap::new();
    let mut needs = ProjectionNeeds::default();
    for group in groups {
        let refs = runtime_projection_refs(
            group.exprs,
            plan,
            alias_projection,
            include_variable_elements,
            include_vectors,
            &BTreeSet::new(),
        )?;
        let group_plan = RowProjectionPlan::from_columns_for_need_class(
            refs.values().cloned().collect(),
            group.need_class,
        )?;
        let group_needs = match group.need_class {
            ProjectionNeedClass::Verifier => &group_plan.needs.verifier,
            ProjectionNeedClass::Residual => &group_plan.needs.residual,
            ProjectionNeedClass::Order => &group_plan.needs.order,
            ProjectionNeedClass::Output => &group_plan.needs.output,
        };
        needs.merge_class_needs(group.need_class, group_needs)?;
        for (key, column) in refs {
            all_refs.entry(key).or_insert(column);
        }
    }
    runtime_projection_from_refs_with_needs(all_refs, needs)
}

fn runtime_projection_refs(
    exprs: &[Expr],
    plan: &GqlSemanticPlan,
    alias_projection: &BTreeMap<String, String>,
    include_variable_elements: bool,
    include_vectors: bool,
    excluded_keys: &BTreeSet<GqlRuntimeValueKey>,
) -> Result<BTreeMap<GqlRuntimeValueKey, ProjectionColumn>, EngineError> {
    let mut refs = BTreeMap::new();
    for expr in exprs {
        collect_expr_refs(
            expr,
            plan,
            alias_projection,
            include_variable_elements,
            include_vectors,
            &mut refs,
        )?;
    }
    for key in excluded_keys {
        refs.remove(key);
    }
    Ok(refs)
}

fn runtime_projection_from_refs_for_need_class(
    refs: BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
    need_class: ProjectionNeedClass,
) -> Result<GqlRuntimeProjection, EngineError> {
    let (keys, columns) = runtime_projection_parts(refs);
    let plan = RowProjectionPlan::from_columns_for_need_class(columns, need_class)?;
    Ok(runtime_projection_from_plan(keys, plan))
}

#[cfg(test)]
fn runtime_projection_from_refs_with_needs(
    refs: BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
    needs: ProjectionNeeds,
) -> Result<GqlRuntimeProjection, EngineError> {
    let (keys, columns) = runtime_projection_parts(refs);
    let plan = RowProjectionPlan::with_explicit_needs(columns, needs)?;
    Ok(runtime_projection_from_plan(keys, plan))
}

fn runtime_projection_parts(
    refs: BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
) -> (Vec<GqlRuntimeValueKey>, Vec<ProjectionColumn>) {
    let mut keys = Vec::with_capacity(refs.len());
    let mut columns = Vec::with_capacity(refs.len());
    for (key, column) in refs {
        keys.push(key);
        columns.push(column);
    }
    (keys, columns)
}

fn runtime_projection_from_plan(
    keys: Vec<GqlRuntimeValueKey>,
    plan: RowProjectionPlan,
) -> GqlRuntimeProjection {
    let key_indexes = keys
        .iter()
        .cloned()
        .enumerate()
        .map(|(index, key)| (key, index))
        .collect();
    GqlRuntimeProjection {
        plan,
        keys,
        key_indexes,
    }
}

pub(crate) fn eval_expr_against_context(
    expr: &Expr,
    context: &GqlEvalContext<'_>,
) -> Result<ProjectedValue, EngineError> {
    match eval_expr(expr, context)? {
        RuntimeValue::Value(value) => Ok(value),
        RuntimeValue::Binding { .. } => Err(invalid_expression_error(
            expr,
            "bound node and edge aliases are valid return values only when projected as elements",
        )),
    }
}

#[cfg(test)]
pub(crate) fn eval_predicate_against_projected_row(
    expr: &Expr,
    projected: &GqlRuntimeProjection,
    row: &ProjectedRow,
    plan: &GqlSemanticPlan,
    params: &GqlParams,
) -> Result<bool, EngineError> {
    let context = GqlEvalContext::new(projected, row, plan, params);
    eval_predicate_against_context(expr, &context)
}

#[cfg(test)]
pub(crate) fn eval_predicate_against_context(
    expr: &Expr,
    context: &GqlEvalContext<'_>,
) -> Result<bool, EngineError> {
    match eval_expr(expr, context)? {
        RuntimeValue::Value(ProjectedValue::Bool(value)) => Ok(value),
        RuntimeValue::Value(ProjectedValue::Null) => Ok(false),
        RuntimeValue::Value(_) | RuntimeValue::Binding { .. } => Err(invalid_expression_error(
            expr,
            "residual WHERE expressions must evaluate to a boolean or null",
        )),
    }
}

pub(crate) struct GqlEvalContext<'a> {
    primary: &'a GqlRuntimeProjection,
    primary_row: &'a ProjectedRow,
    fallback: Option<(&'a GqlRuntimeProjection, &'a ProjectedRow)>,
    aliases: &'a BTreeMap<String, crate::gql::semantic::GqlAliasBinding>,
    params: &'a GqlParams,
}

impl<'a> GqlEvalContext<'a> {
    pub(crate) fn new(
        projection: &'a GqlRuntimeProjection,
        row: &'a ProjectedRow,
        plan: &'a GqlSemanticPlan,
        params: &'a GqlParams,
    ) -> Self {
        Self {
            primary: projection,
            primary_row: row,
            fallback: None,
            aliases: &plan.aliases.by_name,
            params,
        }
    }

    fn alias_kind(&self, alias: &str) -> Option<GqlAliasKind> {
        self.aliases.get(alias).map(|binding| binding.kind)
    }

    fn value_ref(&self, key: &GqlRuntimeValueKey) -> Option<&'a ProjectedValue> {
        projection_value_ref(self.primary, self.primary_row, key).or_else(|| {
            self.fallback
                .and_then(|(projection, row)| projection_value_ref(projection, row, key))
        })
    }

    fn value(&self, key: GqlRuntimeValueKey) -> ProjectedValue {
        self.value_ref(&key)
            .cloned()
            .unwrap_or(ProjectedValue::Null)
    }
}

fn projection_value_ref<'a>(
    projection: &'a GqlRuntimeProjection,
    row: &'a ProjectedRow,
    key: &GqlRuntimeValueKey,
) -> Option<&'a ProjectedValue> {
    projection
        .key_indexes
        .get(key)
        .and_then(|&index| row.values.get(index))
}

enum RuntimeValue {
    Value(ProjectedValue),
    Binding { alias: String, kind: GqlAliasKind },
}

fn collect_expr_refs(
    expr: &Expr,
    plan: &GqlSemanticPlan,
    alias_projection: &BTreeMap<String, String>,
    include_variable_elements: bool,
    include_vectors: bool,
    refs: &mut BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
) -> Result<(), EngineError> {
    match &expr.kind {
        ExprKind::Variable(alias) => {
            if include_variable_elements {
                if let Some(kind) = plan.aliases.get(alias).map(|binding| binding.kind) {
                    add_element_ref(alias, kind, alias_projection, include_vectors, refs)?;
                }
            }
        }
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                if let Some(kind) = plan.aliases.get(alias).map(|binding| binding.kind) {
                    add_property_ref(alias, &property.name, kind, alias_projection, refs)?;
                    return Ok(());
                }
            }
            collect_expr_refs(
                object,
                plan,
                alias_projection,
                include_variable_elements,
                include_vectors,
                refs,
            )?;
        }
        ExprKind::FunctionCall { name, args } => {
            if let Some((endpoint, endpoint_arg)) = edge_endpoint_id_call(name, args) {
                let alias = variable_name(endpoint_arg).expect("endpoint call shape checked");
                if plan
                    .aliases
                    .get(alias)
                    .is_some_and(|binding| binding.kind == GqlAliasKind::Edge)
                {
                    add_edge_metadata_ref(
                        alias,
                        projection_alias(alias, alias_projection),
                        edge_endpoint_projection_field(endpoint),
                        refs,
                    );
                    return Ok(());
                }
            }
            if args.len() == 1 {
                if let ExprKind::Variable(alias) = &args[0].kind {
                    if let Some(kind) = plan.aliases.get(alias).map(|binding| binding.kind) {
                        add_function_ref(&name.name, alias, kind, alias_projection, refs)?;
                        return Ok(());
                    }
                }
            }
            for arg in args {
                collect_expr_refs(
                    arg,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
            }
        }
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                collect_expr_refs(
                    arg,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
            }
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => collect_expr_refs(
            expr,
            plan,
            alias_projection,
            include_variable_elements,
            include_vectors,
            refs,
        )?,
        ExprKind::Binary { left, right, .. } => {
            collect_expr_refs(
                left,
                plan,
                alias_projection,
                include_variable_elements,
                include_vectors,
                refs,
            )?;
            collect_expr_refs(
                right,
                plan,
                alias_projection,
                include_variable_elements,
                include_vectors,
                refs,
            )?;
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand {
                collect_expr_refs(
                    operand,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
            }
            for branch in branches {
                collect_expr_refs(
                    &branch.when,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
                collect_expr_refs(
                    &branch.then,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
            }
            if let Some(else_expr) = else_expr {
                collect_expr_refs(
                    else_expr,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
            }
        }
        ExprKind::List(items) => {
            for item in items {
                collect_expr_refs(
                    item,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
            }
        }
        ExprKind::Map(map) => {
            for entry in &map.entries {
                collect_expr_refs(
                    &entry.value,
                    plan,
                    alias_projection,
                    include_variable_elements,
                    include_vectors,
                    refs,
                )?;
            }
        }
        ExprKind::ExistsSubquery(_) => {}
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
    Ok(())
}

fn add_element_ref(
    alias: &str,
    kind: GqlAliasKind,
    alias_projection: &BTreeMap<String, String>,
    include_vectors: bool,
    refs: &mut BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
) -> Result<(), EngineError> {
    let projection_alias = projection_alias(alias, alias_projection);
    match kind {
        GqlAliasKind::Node => {
            let key = GqlRuntimeValueKey::NodeElement {
                alias: alias.to_string(),
            };
            refs.entry(key)
                .or_insert_with(|| ProjectionColumn::NodeAlias {
                    alias: projection_alias,
                    projection: if include_vectors {
                        NodeOutputProjection::full_with_vectors()
                    } else {
                        NodeOutputProjection::full_without_vectors()
                    },
                    output_name: internal_output_name(alias, "node"),
                });
        }
        GqlAliasKind::Edge => {
            let key = GqlRuntimeValueKey::EdgeElement {
                alias: alias.to_string(),
            };
            refs.entry(key)
                .or_insert_with(|| ProjectionColumn::EdgeAlias {
                    alias: projection_alias,
                    projection: EdgeOutputProjection::full(),
                    output_name: internal_output_name(alias, "edge"),
                });
        }
        GqlAliasKind::Path | GqlAliasKind::Scalar => {}
    }
    Ok(())
}

fn add_property_ref(
    alias: &str,
    property: &str,
    kind: GqlAliasKind,
    alias_projection: &BTreeMap<String, String>,
    refs: &mut BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
) -> Result<(), EngineError> {
    let projection_alias = projection_alias(alias, alias_projection);
    match kind {
        GqlAliasKind::Node => {
            let key = GqlRuntimeValueKey::NodeProperty {
                alias: alias.to_string(),
                key: property.to_string(),
            };
            refs.entry(key)
                .or_insert_with(|| ProjectionColumn::NodeProperty {
                    alias: projection_alias,
                    key: property.to_string(),
                    output_name: internal_output_name(alias, property),
                });
        }
        GqlAliasKind::Edge => {
            let key = GqlRuntimeValueKey::EdgeProperty {
                alias: alias.to_string(),
                key: property.to_string(),
            };
            refs.entry(key)
                .or_insert_with(|| ProjectionColumn::EdgeProperty {
                    alias: projection_alias,
                    key: property.to_string(),
                    output_name: internal_output_name(alias, property),
                });
        }
        GqlAliasKind::Path | GqlAliasKind::Scalar => {}
    }
    Ok(())
}

fn add_function_ref(
    function: &str,
    alias: &str,
    kind: GqlAliasKind,
    alias_projection: &BTreeMap<String, String>,
    refs: &mut BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
) -> Result<(), EngineError> {
    let projection_alias = projection_alias(alias, alias_projection);
    let lower = function.to_ascii_lowercase();
    match lower.as_str() {
        "labels" => {
            add_node_metadata_ref(alias, projection_alias, NodeProjectionField::Labels, refs);
            return Ok(());
        }
        "type" => {
            add_edge_metadata_ref(alias, projection_alias, EdgeProjectionField::Label, refs);
            return Ok(());
        }
        _ => {}
    }
    if let Some(metadata) = GqlMetadataFunction::from_lower(&lower) {
        match kind {
            GqlAliasKind::Node => {
                if let Some(field) = node_metadata_projection_field(metadata) {
                    add_node_metadata_ref(alias, projection_alias, field, refs);
                }
            }
            GqlAliasKind::Edge => {
                if let Some(field) = edge_metadata_projection_field(metadata) {
                    add_edge_metadata_ref(alias, projection_alias, field, refs);
                }
            }
            GqlAliasKind::Path | GqlAliasKind::Scalar => {}
        }
    }
    Ok(())
}

fn add_node_metadata_ref(
    alias: &str,
    projection_alias: String,
    field: NodeProjectionField,
    refs: &mut BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
) {
    let key = GqlRuntimeValueKey::NodeMetadata {
        alias: alias.to_string(),
        field,
    };
    refs.entry(key)
        .or_insert_with(|| ProjectionColumn::NodeMetadata {
            alias: projection_alias,
            field,
            output_name: internal_output_name(alias, node_field_name(field)),
        });
}

fn add_edge_metadata_ref(
    alias: &str,
    projection_alias: String,
    field: EdgeProjectionField,
    refs: &mut BTreeMap<GqlRuntimeValueKey, ProjectionColumn>,
) {
    let key = GqlRuntimeValueKey::EdgeMetadata {
        alias: alias.to_string(),
        field,
    };
    refs.entry(key)
        .or_insert_with(|| ProjectionColumn::EdgeMetadata {
            alias: projection_alias,
            field,
            output_name: internal_output_name(alias, edge_field_name(field)),
        });
}

fn eval_expr(expr: &Expr, context: &GqlEvalContext<'_>) -> Result<RuntimeValue, EngineError> {
    match &expr.kind {
        ExprKind::Literal(literal) => Ok(RuntimeValue::Value(literal_to_value(literal))),
        ExprKind::Parameter(name) => {
            let value = context
                .params
                .get(name)
                .ok_or_else(|| EngineError::GqlParameter {
                    name: name.clone(),
                    expected: "GqlParamValue".to_string(),
                    message: format!("missing parameter '${name}'"),
                    span: expr.span.clone(),
                })?;
            Ok(RuntimeValue::Value(param_to_value(value)))
        }
        ExprKind::Variable(alias) => {
            if let Some(kind) = context.alias_kind(alias) {
                let key = match kind {
                    GqlAliasKind::Node => GqlRuntimeValueKey::NodeElement {
                        alias: alias.clone(),
                    },
                    GqlAliasKind::Edge => GqlRuntimeValueKey::EdgeElement {
                        alias: alias.clone(),
                    },
                    GqlAliasKind::Path | GqlAliasKind::Scalar => {
                        return Ok(RuntimeValue::Binding {
                            alias: alias.clone(),
                            kind,
                        });
                    }
                };
                if let Some(value) = context.value_ref(&key) {
                    Ok(RuntimeValue::Value(value.clone()))
                } else {
                    Ok(RuntimeValue::Binding {
                        alias: alias.clone(),
                        kind,
                    })
                }
            } else {
                Err(invalid_expression_error(
                    expr,
                    "unknown alias during GQL expression evaluation",
                ))
            }
        }
        ExprKind::PropertyAccess { object, property } => {
            if let ExprKind::Variable(alias) = &object.kind {
                if let Some(kind) = context.alias_kind(alias) {
                    return Ok(RuntimeValue::Value(property_value_for_alias(
                        alias,
                        &property.name,
                        kind,
                        context,
                    )));
                }
            }
            match eval_expr(object, context)? {
                RuntimeValue::Value(ProjectedValue::Map(values)) => Ok(RuntimeValue::Value(
                    values
                        .get(&property.name)
                        .cloned()
                        .unwrap_or(ProjectedValue::Null),
                )),
                RuntimeValue::Value(ProjectedValue::Null) => {
                    Ok(RuntimeValue::Value(ProjectedValue::Null))
                }
                RuntimeValue::Value(_) | RuntimeValue::Binding { .. } => Err(
                    invalid_expression_error(expr, "property access requires a map or bound alias"),
                ),
            }
        }
        ExprKind::Unary { op, expr } => {
            let value = value_only(expr, eval_expr(expr, context)?)?;
            let graph_value = projected_value_to_graph_eval_scalar(&value)?.ok_or_else(|| {
                invalid_expression_error(expr, "unary expression requires scalar or null input")
            })?;
            let graph_op = match op {
                UnaryOp::Not => GraphUnaryOp::Not,
                UnaryOp::Neg => GraphUnaryOp::Neg,
            };
            Ok(RuntimeValue::Value(graph_eval_to_projected_scalar(
                eval_graph_unary_value(graph_op, &graph_value)?,
                &expr.span,
            )?))
        }
        ExprKind::Binary { op, left, right } => eval_binary(*op, left, right, context),
        ExprKind::IsNull { expr, negated } => {
            let value = eval_expr(expr, context)?;
            let is_null = matches!(value, RuntimeValue::Value(ProjectedValue::Null));
            Ok(RuntimeValue::Value(ProjectedValue::Bool(if *negated {
                !is_null
            } else {
                is_null
            })))
        }
        ExprKind::FunctionCall { name, args } => {
            let lower = name.name.to_ascii_lowercase();
            if let Some(function) = gql_eval_scalar_function_name(&lower) {
                return eval_scalar_function(function, &name.name, args, context, &expr.span);
            }
            if let Some((endpoint, endpoint_arg)) = edge_endpoint_id_call(name, args) {
                let alias = variable_name(endpoint_arg).expect("endpoint call shape checked");
                if context.alias_kind(alias) != Some(GqlAliasKind::Edge) {
                    return Err(invalid_expression_error(
                        expr,
                        "startNode()/endNode() inside id() expects an edge alias",
                    ));
                }
                return Ok(RuntimeValue::Value(context.value(
                    GqlRuntimeValueKey::EdgeMetadata {
                        alias: alias.to_string(),
                        field: edge_endpoint_projection_field(endpoint),
                    },
                )));
            }
            if args.len() != 1 {
                return Err(invalid_expression_error(
                    expr,
                    "GQL scalar functions expect one argument",
                ));
            }
            let ExprKind::Variable(alias) = &args[0].kind else {
                return Err(invalid_expression_error(
                    expr,
                    "GQL scalar functions expect a bound alias argument",
                ));
            };
            let Some(kind) = context.alias_kind(alias) else {
                return Err(invalid_expression_error(
                    expr,
                    "unknown alias during GQL function evaluation",
                ));
            };
            let value = match lower.as_str() {
                "labels" => context.value(GqlRuntimeValueKey::NodeMetadata {
                    alias: alias.clone(),
                    field: NodeProjectionField::Labels,
                }),
                "type" => context.value(GqlRuntimeValueKey::EdgeMetadata {
                    alias: alias.clone(),
                    field: EdgeProjectionField::Label,
                }),
                _ => {
                    let Some(metadata) = GqlMetadataFunction::from_lower(&lower) else {
                        return Err(invalid_expression_error(
                            expr,
                            "unsupported GQL scalar function",
                        ));
                    };
                    let field_value = match kind {
                        GqlAliasKind::Node => node_metadata_projection_field(metadata).map(
                            |field| {
                                context.value(GqlRuntimeValueKey::NodeMetadata {
                                    alias: alias.clone(),
                                    field,
                                })
                            },
                        ),
                        GqlAliasKind::Edge => edge_metadata_projection_field(metadata).map(
                            |field| {
                                context.value(GqlRuntimeValueKey::EdgeMetadata {
                                    alias: alias.clone(),
                                    field,
                                })
                            },
                        ),
                        GqlAliasKind::Path | GqlAliasKind::Scalar => None,
                    };
                    let Some(value) = field_value else {
                        return Err(invalid_expression_error(
                            expr,
                            "metadata function target does not support this metadata field",
                        ));
                    };
                    value
                }
            };
            Ok(RuntimeValue::Value(value))
        }
        ExprKind::AggregateCall { .. } => Err(invalid_expression_error(
            expr,
            "aggregate functions require projection pipeline evaluation",
        )),
        ExprKind::ExistsSubquery(_) => Err(invalid_expression_error(
            expr,
            "EXISTS subqueries require native pipeline evaluation",
        )),
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => eval_case(
            operand.as_deref(),
            branches,
            else_expr.as_deref(),
            context,
            &expr.span,
        ),
        ExprKind::List(items) => {
            let mut values = Vec::with_capacity(items.len());
            for item in items {
                values.push(value_only(item, eval_expr(item, context)?)?);
            }
            Ok(RuntimeValue::Value(ProjectedValue::List(values)))
        }
        ExprKind::Map(map) => eval_map(map, context),
    }
}

fn eval_binary(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    context: &GqlEvalContext<'_>,
) -> Result<RuntimeValue, EngineError> {
    match op {
        BinaryOp::And => eval_and(left, right, context),
        BinaryOp::Or => eval_or(left, right, context),
        BinaryOp::Eq
        | BinaryOp::Neq
        | BinaryOp::Lt
        | BinaryOp::Le
        | BinaryOp::Gt
        | BinaryOp::Ge
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::StartsWith
        | BinaryOp::EndsWith
        | BinaryOp::Contains
        | BinaryOp::In => {
            let left_value = value_only(left, eval_expr(left, context)?)?;
            let right_value = value_only(right, eval_expr(right, context)?)?;
            if matches!(
                op,
                BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::StartsWith
                    | BinaryOp::EndsWith
                    | BinaryOp::Contains
            ) {
                return Ok(RuntimeValue::Value(eval_shared_binary(
                    op,
                    &left_value,
                    &right_value,
                    &left.span,
                )?));
            }
            if let Some(value) = try_eval_shared_binary(op, &left_value, &right_value, &left.span)?
            {
                return Ok(RuntimeValue::Value(value));
            }
            Ok(RuntimeValue::Value(compare_values(
                op,
                left_value,
                right_value,
            )))
        }
    }
}

fn eval_and(
    left: &Expr,
    right: &Expr,
    context: &GqlEvalContext<'_>,
) -> Result<RuntimeValue, EngineError> {
    let left_value = bool_or_null(left, eval_expr(left, context)?)?;
    if left_value == Some(false) {
        return Ok(RuntimeValue::Value(ProjectedValue::Bool(false)));
    }
    let right_value = bool_or_null(right, eval_expr(right, context)?)?;
    Ok(RuntimeValue::Value(match (left_value, right_value) {
        (_, Some(false)) => ProjectedValue::Bool(false),
        (Some(true), Some(true)) => ProjectedValue::Bool(true),
        _ => ProjectedValue::Null,
    }))
}

fn eval_or(
    left: &Expr,
    right: &Expr,
    context: &GqlEvalContext<'_>,
) -> Result<RuntimeValue, EngineError> {
    let left_value = bool_or_null(left, eval_expr(left, context)?)?;
    if left_value == Some(true) {
        return Ok(RuntimeValue::Value(ProjectedValue::Bool(true)));
    }
    let right_value = bool_or_null(right, eval_expr(right, context)?)?;
    Ok(RuntimeValue::Value(match (left_value, right_value) {
        (_, Some(true)) => ProjectedValue::Bool(true),
        (Some(false), Some(false)) => ProjectedValue::Bool(false),
        _ => ProjectedValue::Null,
    }))
}

fn eval_case(
    operand: Option<&Expr>,
    branches: &[crate::gql::ast::CaseBranch],
    else_expr: Option<&Expr>,
    context: &GqlEvalContext<'_>,
    span: &crate::types::SourceSpan,
) -> Result<RuntimeValue, EngineError> {
    if let Some(operand) = operand {
        let operand_value = value_only(operand, eval_expr(operand, context)?)?;
        for branch in branches {
            let when_value = value_only(&branch.when, eval_expr(&branch.when, context)?)?;
            if let Some(value) =
                try_eval_shared_binary(BinaryOp::Eq, &operand_value, &when_value, span)?
            {
                match value {
                    ProjectedValue::Bool(true) => return eval_expr(&branch.then, context),
                    ProjectedValue::Bool(false) | ProjectedValue::Null => {}
                    _ => unreachable!("equality returns bool or null"),
                }
            } else if matches!(
                compare_values(BinaryOp::Eq, operand_value.clone(), when_value),
                ProjectedValue::Bool(true)
            ) {
                return eval_expr(&branch.then, context);
            }
        }
    } else {
        for branch in branches {
            if let Some(true) = bool_or_null(&branch.when, eval_expr(&branch.when, context)?)? {
                return eval_expr(&branch.then, context);
            }
        }
    }
    else_expr
        .map(|expr| eval_expr(expr, context))
        .unwrap_or(Ok(RuntimeValue::Value(ProjectedValue::Null)))
}

fn eval_scalar_function(
    function: GraphFunction,
    display: &str,
    args: &[Expr],
    context: &GqlEvalContext<'_>,
    span: &crate::types::SourceSpan,
) -> Result<RuntimeValue, EngineError> {
    validate_eval_scalar_function_arity(&display.to_ascii_lowercase(), display, args.len(), span)?;
    if function == GraphFunction::Coalesce {
        for arg in args {
            let value = value_only(arg, eval_expr(arg, context)?)?;
            let graph_value = projected_value_to_graph_eval_scalar(&value)?.ok_or_else(|| {
                invalid_expression_error(
                    arg,
                    "scalar function expects scalar, list, map, or null input",
                )
            })?;
            if !graph_value.is_null() {
                let checked = eval_graph_scalar_function_values(
                    GraphFunction::Coalesce,
                    std::slice::from_ref(&graph_value),
                )?;
                return Ok(RuntimeValue::Value(graph_eval_to_projected_scalar(
                    checked, &arg.span,
                )?));
            }
        }
        return Ok(RuntimeValue::Value(ProjectedValue::Null));
    }
    let values = args
        .iter()
        .map(|arg| {
            let value = value_only(arg, eval_expr(arg, context)?)?;
            projected_value_to_graph_eval_scalar(&value)?.ok_or_else(|| {
                invalid_expression_error(
                    arg,
                    "scalar function expects scalar, list, map, or null input",
                )
            })
        })
        .collect::<Result<Vec<_>, EngineError>>()?;
    Ok(RuntimeValue::Value(graph_eval_to_projected_scalar(
        eval_graph_scalar_function_values(function, &values)?,
        span,
    )?))
}

fn eval_shared_binary(
    op: BinaryOp,
    left: &ProjectedValue,
    right: &ProjectedValue,
    span: &crate::types::SourceSpan,
) -> Result<ProjectedValue, EngineError> {
    let left = projected_value_to_graph_eval_scalar(left)?.ok_or_else(|| {
        gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "scalar operator requires scalar, list, map, or null operands".to_string(),
            span.clone(),
        )
    })?;
    let right = projected_value_to_graph_eval_scalar(right)?.ok_or_else(|| {
        gql_semantic_error(
            GqlSemanticErrorCode::InvalidReturnExpression,
            "scalar operator requires scalar, list, map, or null operands".to_string(),
            span.clone(),
        )
    })?;
    graph_eval_to_projected_scalar(
        eval_graph_binary_values(gql_eval_binary_op_to_graph_op(op), &left, &right)?,
        span,
    )
}

fn try_eval_shared_binary(
    op: BinaryOp,
    left: &ProjectedValue,
    right: &ProjectedValue,
    span: &crate::types::SourceSpan,
) -> Result<Option<ProjectedValue>, EngineError> {
    let Some(left) = projected_value_to_graph_eval_scalar(left)? else {
        return Ok(None);
    };
    let Some(right) = projected_value_to_graph_eval_scalar(right)? else {
        return Ok(None);
    };
    graph_eval_to_projected_scalar(
        eval_graph_binary_values(gql_eval_binary_op_to_graph_op(op), &left, &right)?,
        span,
    )
    .map(Some)
}

fn gql_eval_binary_op_to_graph_op(op: BinaryOp) -> GraphBinaryOp {
    match op {
        BinaryOp::Or => GraphBinaryOp::Or,
        BinaryOp::And => GraphBinaryOp::And,
        BinaryOp::Add => GraphBinaryOp::Add,
        BinaryOp::Sub => GraphBinaryOp::Sub,
        BinaryOp::Mul => GraphBinaryOp::Mul,
        BinaryOp::Div => GraphBinaryOp::Div,
        BinaryOp::Eq => GraphBinaryOp::Eq,
        BinaryOp::Neq => GraphBinaryOp::Neq,
        BinaryOp::Lt => GraphBinaryOp::Lt,
        BinaryOp::Le => GraphBinaryOp::Le,
        BinaryOp::Gt => GraphBinaryOp::Gt,
        BinaryOp::Ge => GraphBinaryOp::Ge,
        BinaryOp::In => GraphBinaryOp::In,
        BinaryOp::StartsWith => GraphBinaryOp::StartsWith,
        BinaryOp::EndsWith => GraphBinaryOp::EndsWith,
        BinaryOp::Contains => GraphBinaryOp::Contains,
    }
}

fn gql_eval_scalar_function_name(lower: &str) -> Option<GraphFunction> {
    match lower {
        "coalesce" => Some(GraphFunction::Coalesce),
        "tostring" => Some(GraphFunction::ToString),
        "tointeger" => Some(GraphFunction::ToInteger),
        "tofloat" => Some(GraphFunction::ToFloat),
        "abs" => Some(GraphFunction::Abs),
        "floor" => Some(GraphFunction::Floor),
        "ceil" => Some(GraphFunction::Ceil),
        "round" => Some(GraphFunction::Round),
        "lower" => Some(GraphFunction::Lower),
        "upper" => Some(GraphFunction::Upper),
        "trim" => Some(GraphFunction::Trim),
        "substring" => Some(GraphFunction::Substring),
        "size" => Some(GraphFunction::Size),
        "head" => Some(GraphFunction::Head),
        "last" => Some(GraphFunction::Last),
        _ => None,
    }
}

fn validate_eval_scalar_function_arity(
    lower: &str,
    display: &str,
    arg_count: usize,
    span: &crate::types::SourceSpan,
) -> Result<(), EngineError> {
    let valid = match lower {
        "coalesce" => arg_count >= 1,
        "substring" => matches!(arg_count, 2 | 3),
        "tostring" | "tointeger" | "tofloat" | "abs" | "floor" | "ceil" | "round" | "lower"
        | "upper" | "trim" | "size" | "head" | "last" => arg_count == 1,
        _ => false,
    };
    if valid {
        return Ok(());
    }
    let expected = match lower {
        "coalesce" => "at least one argument",
        "substring" => "two or three arguments",
        _ => "exactly one argument",
    };
    Err(gql_semantic_error(
        GqlSemanticErrorCode::InvalidReturnExpression,
        format!("function '{display}' expects {expected}"),
        span.clone(),
    ))
}

fn projected_value_to_graph_eval_scalar(
    value: &ProjectedValue,
) -> Result<Option<GraphEvalValue>, EngineError> {
    Ok(match value {
        ProjectedValue::Null => Some(GraphEvalValue::Null),
        ProjectedValue::Bool(value) => Some(GraphEvalValue::Bool(*value)),
        ProjectedValue::Int(value) => Some(GraphEvalValue::Int(*value)),
        ProjectedValue::UInt(value) => Some(GraphEvalValue::UInt(*value)),
        ProjectedValue::Float(value) => Some(GraphEvalValue::Float(*value)),
        ProjectedValue::String(value) => Some(GraphEvalValue::String(value.clone())),
        ProjectedValue::Bytes(value) => Some(GraphEvalValue::Bytes(value.clone())),
        ProjectedValue::List(values) => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                let Some(value) = projected_value_to_graph_eval_scalar(value)? else {
                    return Ok(None);
                };
                out.push(value);
            }
            Some(GraphEvalValue::List(out))
        }
        ProjectedValue::Map(values) => {
            let mut out = BTreeMap::new();
            for (key, value) in values {
                let Some(value) = projected_value_to_graph_eval_scalar(value)? else {
                    return Ok(None);
                };
                out.insert(key.clone(), value);
            }
            Some(GraphEvalValue::Map(out))
        }
        ProjectedValue::Node(_) | ProjectedValue::Edge(_) | ProjectedValue::Path(_) => None,
    })
}

fn graph_eval_to_projected_scalar(
    value: GraphEvalValue,
    span: &crate::types::SourceSpan,
) -> Result<ProjectedValue, EngineError> {
    Ok(match value {
        GraphEvalValue::Null => ProjectedValue::Null,
        GraphEvalValue::Bool(value) => ProjectedValue::Bool(value),
        GraphEvalValue::Int(value) => ProjectedValue::Int(value),
        GraphEvalValue::UInt(value) => ProjectedValue::UInt(value),
        GraphEvalValue::Float(value) => ProjectedValue::Float(value),
        GraphEvalValue::String(value) => ProjectedValue::String(value),
        GraphEvalValue::Bytes(value) => ProjectedValue::Bytes(value),
        GraphEvalValue::List(values) => ProjectedValue::List(
            values
                .into_iter()
                .map(|value| graph_eval_to_projected_scalar(value, span))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        GraphEvalValue::Map(values) => ProjectedValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, graph_eval_to_projected_scalar(value, span)?)))
                .collect::<Result<BTreeMap<_, _>, EngineError>>()?,
        ),
        GraphEvalValue::Node(_) | GraphEvalValue::Edge(_) | GraphEvalValue::Path(_) => {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "scalar expression produced a graph element value".to_string(),
                span.clone(),
            ));
        }
    })
}

fn eval_map(map: &MapLiteral, context: &GqlEvalContext<'_>) -> Result<RuntimeValue, EngineError> {
    let mut values = BTreeMap::new();
    for entry in &map.entries {
        values.insert(
            entry.key.name.clone(),
            value_only(&entry.value, eval_expr(&entry.value, context)?)?,
        );
    }
    Ok(RuntimeValue::Value(ProjectedValue::Map(values)))
}

fn property_value_for_alias(
    alias: &str,
    property: &str,
    kind: GqlAliasKind,
    context: &GqlEvalContext<'_>,
) -> ProjectedValue {
    match kind {
        GqlAliasKind::Node => context.value(GqlRuntimeValueKey::NodeProperty {
            alias: alias.to_string(),
            key: property.to_string(),
        }),
        GqlAliasKind::Edge => context.value(GqlRuntimeValueKey::EdgeProperty {
            alias: alias.to_string(),
            key: property.to_string(),
        }),
        GqlAliasKind::Path | GqlAliasKind::Scalar => ProjectedValue::Null,
    }
}

fn compare_values(op: BinaryOp, left: ProjectedValue, right: ProjectedValue) -> ProjectedValue {
    if left.is_null() || right.is_null() {
        return ProjectedValue::Null;
    }
    match op {
        BinaryOp::Eq => ProjectedValue::Bool(values_equal_for_gql(&left, &right)),
        BinaryOp::Neq => ProjectedValue::Bool(!values_equal_for_gql(&left, &right)),
        BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge => {
            match partial_cmp_projected_values(&left, &right) {
                Some(ordering) => ProjectedValue::Bool(match op {
                    BinaryOp::Lt => ordering == Ordering::Less,
                    BinaryOp::Le => matches!(ordering, Ordering::Less | Ordering::Equal),
                    BinaryOp::Gt => ordering == Ordering::Greater,
                    BinaryOp::Ge => matches!(ordering, Ordering::Greater | Ordering::Equal),
                    _ => unreachable!(),
                }),
                None => ProjectedValue::Null,
            }
        }
        BinaryOp::In => match right {
            ProjectedValue::List(items) => {
                let mut saw_null = false;
                for item in items {
                    if item.is_null() {
                        saw_null = true;
                    } else if values_equal_for_gql(&item, &left) {
                        return ProjectedValue::Bool(true);
                    }
                }
                if saw_null {
                    ProjectedValue::Null
                } else {
                    ProjectedValue::Bool(false)
                }
            }
            _ => ProjectedValue::Null,
        },
        BinaryOp::And
        | BinaryOp::Or
        | BinaryOp::Add
        | BinaryOp::Sub
        | BinaryOp::Mul
        | BinaryOp::Div
        | BinaryOp::StartsWith
        | BinaryOp::EndsWith
        | BinaryOp::Contains => unreachable!(),
    }
}

fn values_equal_for_gql(left: &ProjectedValue, right: &ProjectedValue) -> bool {
    match partial_cmp_numeric_values(left, right) {
        Some(ordering) => ordering == Ordering::Equal,
        None => left == right,
    }
}

fn partial_cmp_projected_values(left: &ProjectedValue, right: &ProjectedValue) -> Option<Ordering> {
    if let Some(ordering) = partial_cmp_numeric_values(left, right) {
        return Some(ordering);
    }
    match (left, right) {
        (ProjectedValue::String(left), ProjectedValue::String(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

pub(crate) fn partial_cmp_numeric_values(
    left: &ProjectedValue,
    right: &ProjectedValue,
) -> Option<Ordering> {
    Some(compare_numeric_keys(
        numeric_key_for_projected_value(left)?,
        numeric_key_for_projected_value(right)?,
    ))
}

fn numeric_key_for_projected_value(value: &ProjectedValue) -> Option<NumericScalarKey> {
    match value {
        ProjectedValue::Int(value) => Some(numeric_key_from_i64(*value)),
        ProjectedValue::UInt(value) => Some(numeric_key_from_u64(*value)),
        ProjectedValue::Float(value) => numeric_key_from_f64(*value),
        _ => None,
    }
}

fn value_only(expr: &Expr, value: RuntimeValue) -> Result<ProjectedValue, EngineError> {
    match value {
        RuntimeValue::Value(value) => Ok(value),
        RuntimeValue::Binding { alias, kind } => Err(invalid_expression_error(
            expr,
            &format!(
                "alias '{}' ({kind:?}) cannot be used as an implicit scalar value",
                alias
            ),
        )),
    }
}

fn bool_or_null(expr: &Expr, value: RuntimeValue) -> Result<Option<bool>, EngineError> {
    match value_only(expr, value)? {
        ProjectedValue::Bool(value) => Ok(Some(value)),
        ProjectedValue::Null => Ok(None),
        _ => Err(invalid_expression_error(
            expr,
            "boolean operators require boolean or null operands",
        )),
    }
}

fn literal_to_value(literal: &Literal) -> ProjectedValue {
    match literal {
        Literal::Null => ProjectedValue::Null,
        Literal::Bool(value) => ProjectedValue::Bool(*value),
        Literal::Int(value) => ProjectedValue::Int(*value),
        Literal::Float(value) => ProjectedValue::Float(*value),
        Literal::String(value) => ProjectedValue::String(value.clone()),
    }
}

fn param_to_value(value: &GqlParamValue) -> ProjectedValue {
    match value {
        GqlParamValue::Null => ProjectedValue::Null,
        GqlParamValue::Bool(value) => ProjectedValue::Bool(*value),
        GqlParamValue::Int(value) => ProjectedValue::Int(*value),
        GqlParamValue::UInt(value) => ProjectedValue::UInt(*value),
        GqlParamValue::Float(value) => ProjectedValue::Float(*value),
        GqlParamValue::String(value) => ProjectedValue::String(value.clone()),
        GqlParamValue::Bytes(value) => ProjectedValue::Bytes(value.clone()),
        GqlParamValue::List(values) => {
            ProjectedValue::List(values.iter().map(param_to_value).collect())
        }
        GqlParamValue::Map(values) => ProjectedValue::Map(
            values
                .iter()
                .map(|(key, value)| (key.clone(), param_to_value(value)))
                .collect(),
        ),
    }
}

fn projection_alias(alias: &str, alias_projection: &BTreeMap<String, String>) -> String {
    alias_projection
        .get(alias)
        .cloned()
        .unwrap_or_else(|| alias.to_string())
}

fn node_metadata_projection_field(metadata: GqlMetadataFunction) -> Option<NodeProjectionField> {
    match metadata {
        GqlMetadataFunction::Id => Some(NodeProjectionField::Id),
        GqlMetadataFunction::ElementKey => Some(NodeProjectionField::Key),
        GqlMetadataFunction::Weight => Some(NodeProjectionField::Weight),
        GqlMetadataFunction::CreatedAt => Some(NodeProjectionField::CreatedAt),
        GqlMetadataFunction::UpdatedAt => Some(NodeProjectionField::UpdatedAt),
        GqlMetadataFunction::ValidFrom | GqlMetadataFunction::ValidTo => None,
    }
}

fn edge_metadata_projection_field(metadata: GqlMetadataFunction) -> Option<EdgeProjectionField> {
    match metadata {
        GqlMetadataFunction::Id => Some(EdgeProjectionField::Id),
        GqlMetadataFunction::Weight => Some(EdgeProjectionField::Weight),
        GqlMetadataFunction::CreatedAt => Some(EdgeProjectionField::CreatedAt),
        GqlMetadataFunction::UpdatedAt => Some(EdgeProjectionField::UpdatedAt),
        GqlMetadataFunction::ValidFrom => Some(EdgeProjectionField::ValidFrom),
        GqlMetadataFunction::ValidTo => Some(EdgeProjectionField::ValidTo),
        GqlMetadataFunction::ElementKey => None,
    }
}

fn edge_endpoint_projection_field(endpoint: GqlEndpointFunction) -> EdgeProjectionField {
    match endpoint {
        GqlEndpointFunction::StartNode => EdgeProjectionField::From,
        GqlEndpointFunction::EndNode => EdgeProjectionField::To,
    }
}

fn node_field_name(field: NodeProjectionField) -> &'static str {
    match field {
        NodeProjectionField::Id => "id",
        NodeProjectionField::Labels => "labels",
        NodeProjectionField::Key => "key",
        NodeProjectionField::Weight => "weight",
        NodeProjectionField::CreatedAt => "created_at",
        NodeProjectionField::UpdatedAt => "updated_at",
    }
}

fn edge_field_name(field: EdgeProjectionField) -> &'static str {
    match field {
        EdgeProjectionField::Id => "id",
        EdgeProjectionField::From => "from",
        EdgeProjectionField::To => "to",
        EdgeProjectionField::Label => "label",
        EdgeProjectionField::Weight => "weight",
        EdgeProjectionField::CreatedAt => "created_at",
        EdgeProjectionField::UpdatedAt => "updated_at",
        EdgeProjectionField::ValidFrom => "valid_from",
        EdgeProjectionField::ValidTo => "valid_to",
    }
}

fn internal_output_name(alias: &str, suffix: &str) -> String {
    format!("__gql_{alias}_{suffix}")
}

fn invalid_expression_error(expr: &Expr, message: &str) -> EngineError {
    gql_semantic_error(
        GqlSemanticErrorCode::InvalidReturnExpression,
        message.to_string(),
        expr.span.clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::parser::{parse_query, GqlParseOptions};
    use crate::gql::semantic::bind_query;

    fn bound_plan(source: &str) -> GqlSemanticPlan {
        bind_query(
            parse_query(source, &GqlParseOptions::default()).unwrap(),
            &BTreeMap::new(),
        )
        .unwrap()
    }

    #[test]
    fn return_star_exprs_expand_in_bound_order() {
        let plan = bound_plan("MATCH (a)-[r:KNOWS]->(b) RETURN *");
        let exprs = return_exprs(&plan);
        assert_eq!(
            exprs
                .iter()
                .map(|expr| expr.output_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "r", "b"]
        );
    }

    #[test]
    fn variable_only_predicate_is_rejected_as_implicit_truthiness() {
        let plan = bound_plan("MATCH (n:Person) WHERE n RETURN n");
        let projection = build_runtime_projection(
            &[plan.query.match_clauses[0].where_clause.clone().unwrap()],
            &plan,
            &BTreeMap::new(),
            false,
            false,
        )
        .unwrap();
        let row = ProjectedRow { values: Vec::new() };
        let err = eval_predicate_against_projected_row(
            plan.query.match_clauses[0].where_clause.as_ref().unwrap(),
            &projection,
            &row,
            &plan,
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(matches!(err, EngineError::GqlSemantic { .. }));
    }

    #[test]
    fn numeric_comparisons_do_not_use_lossy_boundary_casts() {
        assert_eq!(
            compare_values(
                BinaryOp::Eq,
                ProjectedValue::UInt(u64::MAX),
                ProjectedValue::Float(18_446_744_073_709_551_616.0),
            ),
            ProjectedValue::Bool(false)
        );
        assert_eq!(
            compare_values(
                BinaryOp::Lt,
                ProjectedValue::UInt(u64::MAX),
                ProjectedValue::Float(18_446_744_073_709_551_616.0),
            ),
            ProjectedValue::Bool(true)
        );
        assert_eq!(
            compare_values(
                BinaryOp::Eq,
                ProjectedValue::Int(i64::MAX),
                ProjectedValue::Float(9_223_372_036_854_775_808.0),
            ),
            ProjectedValue::Bool(false)
        );
        assert_eq!(
            compare_values(
                BinaryOp::Lt,
                ProjectedValue::Int(i64::MAX),
                ProjectedValue::Float(9_223_372_036_854_775_808.0),
            ),
            ProjectedValue::Bool(true)
        );
    }

    #[test]
    fn numeric_equality_in_and_ranges_share_property_semantics() {
        assert_eq!(
            compare_values(
                BinaryOp::Eq,
                ProjectedValue::Int(1),
                ProjectedValue::Float(1.0),
            ),
            ProjectedValue::Bool(true)
        );
        assert_eq!(
            compare_values(
                BinaryOp::In,
                ProjectedValue::UInt(1),
                ProjectedValue::List(vec![ProjectedValue::Float(1.0)]),
            ),
            ProjectedValue::Bool(true)
        );
        assert_eq!(
            compare_values(
                BinaryOp::Gt,
                ProjectedValue::Float(1.5),
                ProjectedValue::Int(1),
            ),
            ProjectedValue::Bool(true)
        );
        assert_eq!(
            compare_values(
                BinaryOp::Eq,
                ProjectedValue::Float(-0.0),
                ProjectedValue::UInt(0),
            ),
            ProjectedValue::Bool(true)
        );
        assert_eq!(
            compare_values(
                BinaryOp::Eq,
                ProjectedValue::Float(f64::NAN),
                ProjectedValue::Float(f64::NAN),
            ),
            ProjectedValue::Bool(false)
        );
    }

    #[test]
    fn lazy_coalesce_uses_shared_scalar_domain_validation() {
        let params = GqlParams::from([("bad".to_string(), GqlParamValue::Float(f64::NAN))]);
        let plan = bind_query(
            parse_query(
                "MATCH (n:Person) RETURN coalesce($bad, 1)",
                &GqlParseOptions::default(),
            )
            .unwrap(),
            &params,
        )
        .unwrap();
        let expr = return_exprs(&plan).pop().unwrap().expr;
        let projection = build_runtime_projection(
            std::slice::from_ref(&expr),
            &plan,
            &BTreeMap::new(),
            false,
            false,
        )
        .unwrap();
        let row = ProjectedRow { values: Vec::new() };
        let context = GqlEvalContext::new(&projection, &row, &plan, &params);

        let err = eval_expr_against_context(&expr, &context).unwrap_err();
        assert!(matches!(
            err,
            EngineError::InvalidOperation(message)
                if message.contains("scalar function result must be finite")
        ));
    }
}
