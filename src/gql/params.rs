use crate::error::EngineError;
use crate::gql::ast::{Expr, ExprKind, GqlQuery, MapLiteral, Pattern, ReturnBody};
use crate::gql::parser::{parse_query, GqlParseOptions};
use crate::gql::semantic::GqlSemanticPlan;
use crate::types::{GqlParamValue, GqlParams, GqlQueryOptions, SourceSpan};
use std::collections::BTreeMap;

pub(crate) fn referenced_param_names_for_query(
    query: &str,
    options: &GqlQueryOptions,
) -> Result<Vec<String>, EngineError> {
    let ast = parse_query(
        query,
        &GqlParseOptions {
            max_query_bytes: options.max_query_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        },
    )?;
    Ok(collect_query_parameter_spans(&ast).into_keys().collect())
}

pub(crate) fn validate_referenced_gql_params(
    semantic: &GqlSemanticPlan,
    params: &GqlParams,
    options: &GqlQueryOptions,
) -> Result<(), EngineError> {
    let mut total_items = 0usize;
    let mut total_bytes = 0usize;
    for name in &semantic.parameters {
        let span = semantic
            .parameter_spans
            .get(name)
            .cloned()
            .unwrap_or_else(|| SourceSpan::new(0, 0, 1, 1));
        let value = params.get(name).ok_or_else(|| EngineError::GqlParameter {
            name: name.clone(),
            expected: "GqlParamValue".to_string(),
            message: format!("missing parameter '${name}'"),
            span: span.clone(),
        })?;
        validate_param_value(
            name,
            &span,
            value,
            options,
            &mut total_items,
            &mut total_bytes,
        )?;
    }
    Ok(())
}

fn collect_query_parameter_spans(query: &GqlQuery) -> BTreeMap<String, SourceSpan> {
    let mut spans = BTreeMap::new();
    for clause in &query.match_clauses {
        for pattern in &clause.patterns {
            collect_pattern_parameter_spans(pattern, &mut spans);
        }
        if let Some(where_clause) = clause.where_clause.as_ref() {
            collect_expr_parameter_spans(where_clause, &mut spans);
        }
    }
    if let ReturnBody::Items(items) = &query.return_clause.body {
        for item in items {
            collect_expr_parameter_spans(&item.expr, &mut spans);
        }
    }
    for item in &query.order_by {
        collect_expr_parameter_spans(&item.expr, &mut spans);
    }
    if let Some(skip) = query.skip.as_ref() {
        collect_expr_parameter_spans(skip, &mut spans);
    }
    if let Some(limit) = query.limit.as_ref() {
        collect_expr_parameter_spans(limit, &mut spans);
    }
    spans
}

fn collect_pattern_parameter_spans(pattern: &Pattern, spans: &mut BTreeMap<String, SourceSpan>) {
    if let Some(properties) = pattern.start.properties.as_ref() {
        collect_map_parameter_spans(properties, spans);
    }
    for chain in &pattern.chains {
        if let Some(properties) = chain.relationship.properties.as_ref() {
            collect_map_parameter_spans(properties, spans);
        }
        if let Some(properties) = chain.node.properties.as_ref() {
            collect_map_parameter_spans(properties, spans);
        }
    }
}

fn collect_map_parameter_spans(map: &MapLiteral, spans: &mut BTreeMap<String, SourceSpan>) {
    for entry in &map.entries {
        collect_expr_parameter_spans(&entry.value, spans);
    }
}

fn collect_expr_parameter_spans(expr: &Expr, spans: &mut BTreeMap<String, SourceSpan>) {
    let mut stack = vec![expr];
    while let Some(expr) = stack.pop() {
        match &expr.kind {
            ExprKind::Literal(_) | ExprKind::Variable(_) => {}
            ExprKind::Parameter(name) => {
                spans
                    .entry(name.clone())
                    .or_insert_with(|| expr.span.clone());
            }
            ExprKind::PropertyAccess { object, .. } => stack.push(object),
            ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => stack.push(expr),
            ExprKind::Binary { left, right, .. } => {
                stack.push(right);
                stack.push(left);
            }
            ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
                for arg in args.iter().rev() {
                    stack.push(arg);
                }
            }
            ExprKind::Map(map) => {
                for entry in map.entries.iter().rev() {
                    stack.push(&entry.value);
                }
            }
        }
    }
}

fn validate_param_value(
    name: &str,
    span: &SourceSpan,
    value: &GqlParamValue,
    options: &GqlQueryOptions,
    total_items: &mut usize,
    total_bytes: &mut usize,
) -> Result<(), EngineError> {
    let mut stack = vec![(value, 0usize)];
    while let Some((value, container_depth)) = stack.pop() {
        match value {
            GqlParamValue::Null
            | GqlParamValue::Bool(_)
            | GqlParamValue::Int(_)
            | GqlParamValue::UInt(_)
            | GqlParamValue::Float(_) => {}
            GqlParamValue::String(value) => {
                add_param_bytes(name, span, value.len(), "string", total_bytes, options)?;
            }
            GqlParamValue::Bytes(value) => {
                add_param_bytes(name, span, value.len(), "bytes", total_bytes, options)?;
            }
            GqlParamValue::List(values) => {
                let depth = container_depth.saturating_add(1);
                check_container_depth(name, span, depth, options)?;
                add_param_items(name, span, values.len(), "list", total_items, options)?;
                for item in values.iter().rev() {
                    stack.push((item, depth));
                }
            }
            GqlParamValue::Map(values) => {
                let depth = container_depth.saturating_add(1);
                check_container_depth(name, span, depth, options)?;
                add_param_items(name, span, values.len(), "map", total_items, options)?;
                for (key, value) in values.iter().rev() {
                    add_param_bytes(name, span, key.len(), "map key", total_bytes, options)?;
                    stack.push((value, depth));
                }
            }
        }
    }
    Ok(())
}

fn check_container_depth(
    name: &str,
    span: &SourceSpan,
    depth: usize,
    options: &GqlQueryOptions,
) -> Result<(), EngineError> {
    if depth > options.max_ast_depth {
        return Err(param_resource_error(
            name,
            span,
            format!("max_ast_depth <= {}", options.max_ast_depth),
            format!(
                "parameter '${name}' nested list/map depth exceeds max_ast_depth of {}",
                options.max_ast_depth
            ),
        ));
    }
    Ok(())
}

fn add_param_items(
    name: &str,
    span: &SourceSpan,
    count: usize,
    container_kind: &str,
    total_items: &mut usize,
    options: &GqlQueryOptions,
) -> Result<(), EngineError> {
    if count > options.max_literal_items {
        return Err(param_resource_error(
            name,
            span,
            format!("max_literal_items <= {}", options.max_literal_items),
            format!(
                "parameter '${name}' {container_kind} contains {count} items, exceeding max_literal_items of {}",
                options.max_literal_items
            ),
        ));
    }
    *total_items = total_items
        .checked_add(count)
        .filter(|total| *total <= options.max_literal_items)
        .ok_or_else(|| {
            param_resource_error(
                name,
                span,
                format!("max_literal_items <= {}", options.max_literal_items),
                format!(
                    "referenced GQL parameters contain more than max_literal_items={} total list/map items",
                    options.max_literal_items
                ),
            )
        })?;
    Ok(())
}

fn add_param_bytes(
    name: &str,
    span: &SourceSpan,
    bytes: usize,
    value_kind: &str,
    total_bytes: &mut usize,
    options: &GqlQueryOptions,
) -> Result<(), EngineError> {
    if bytes > options.max_param_bytes {
        return Err(param_resource_error(
            name,
            span,
            format!("max_param_bytes <= {}", options.max_param_bytes),
            format!(
                "parameter '${name}' {value_kind} is {bytes} bytes, exceeding max_param_bytes of {}",
                options.max_param_bytes
            ),
        ));
    }
    *total_bytes = total_bytes
        .checked_add(bytes)
        .filter(|total| *total <= options.max_param_bytes)
        .ok_or_else(|| {
            param_resource_error(
                name,
                span,
                format!("max_param_bytes <= {}", options.max_param_bytes),
                format!(
                    "referenced GQL parameters contain more than max_param_bytes={} total string/bytes/map-key bytes",
                    options.max_param_bytes
                ),
            )
        })?;
    Ok(())
}

fn param_resource_error(
    name: &str,
    span: &SourceSpan,
    expected: String,
    message: String,
) -> EngineError {
    EngineError::GqlParameter {
        name: name.to_string(),
        expected,
        message,
        span: span.clone(),
    }
}
