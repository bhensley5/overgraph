use crate::error::EngineError;
use crate::gql::ast::{
    Expr, ExprKind, GqlMutationStatement, GqlPipelineClause, GqlQuery, GqlReadPipeline,
    GqlSchemaDropItem, GqlSchemaItem, GqlSchemaLiteral, GqlSchemaStatement, GqlStatementBody,
    MapLiteral, MutationClause, Pattern, RemoveItem, ReturnBody, SetItem,
};
use crate::gql::parser::{parse_statement, GqlParseOptions};
use crate::gql::semantic::{GqlMutationSemanticPlan, GqlSemanticPlan};
use crate::types::{GqlExecutionOptions, GqlParamValue, GqlParams, SourceSpan};
use std::collections::BTreeMap;

pub(crate) fn referenced_param_names_for_query(
    query: &str,
    options: &GqlExecutionOptions,
) -> Result<Vec<String>, EngineError> {
    let statement = parse_statement(
        query,
        &GqlParseOptions {
            max_query_bytes: options.max_query_bytes,
            max_ast_depth: options.max_ast_depth,
            max_literal_items: options.max_literal_items,
        },
    )?;
    let spans = match &statement.body {
        GqlStatementBody::Query(query) => collect_query_parameter_spans(query),
        GqlStatementBody::Mutation(mutation) => collect_mutation_parameter_spans(mutation),
        GqlStatementBody::Schema(schema) => collect_schema_parameter_spans(schema),
        GqlStatementBody::Index(_) => BTreeMap::new(),
    };
    Ok(spans.into_keys().collect())
}

pub(crate) fn validate_referenced_gql_params(
    semantic: &GqlSemanticPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<(), EngineError> {
    validate_referenced_param_set(
        &semantic.parameters,
        &semantic.parameter_spans,
        params,
        options,
    )
}

pub(crate) fn validate_referenced_gql_mutation_params(
    semantic: &GqlMutationSemanticPlan,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<(), EngineError> {
    validate_referenced_param_set(
        &semantic.parameters,
        &semantic.parameter_spans,
        params,
        options,
    )
}

pub(crate) fn validate_referenced_gql_schema_ast_params(
    schema: &GqlSchemaStatement,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<(), EngineError> {
    let spans = collect_schema_parameter_spans(schema);
    let parameters = spans.keys().cloned().collect::<Vec<_>>();
    validate_referenced_param_set(&parameters, &spans, params, options)
}

fn validate_referenced_param_set(
    parameters: &[String],
    parameter_spans: &BTreeMap<String, SourceSpan>,
    params: &GqlParams,
    options: &GqlExecutionOptions,
) -> Result<(), EngineError> {
    let mut total_items = 0usize;
    let mut total_bytes = 0usize;
    for name in parameters {
        let span = parameter_spans
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
    collect_read_pipeline_parameter_spans(&query.pipeline, &mut spans);
    spans
}

fn collect_read_pipeline_parameter_spans(
    pipeline: &GqlReadPipeline,
    spans: &mut BTreeMap<String, SourceSpan>,
) {
    for clause in &pipeline.clauses {
        match clause {
            GqlPipelineClause::Match(match_clauses) => {
                for clause in match_clauses {
                    for pattern in &clause.patterns {
                        collect_pattern_parameter_spans(pattern, spans);
                    }
                    if let Some(where_clause) = clause.where_clause.as_ref() {
                        collect_expr_parameter_spans(where_clause, spans);
                    }
                }
            }
            GqlPipelineClause::ShortestPath(shortest) => {
                collect_pattern_parameter_spans(&shortest.pattern, spans);
            }
            GqlPipelineClause::Call(call) => {
                collect_read_pipeline_parameter_spans(&call.pipeline, spans);
            }
            GqlPipelineClause::Projection(projection) => {
                collect_return_body_parameter_spans(&projection.body, spans);
                if let Some(where_clause) = projection.where_clause.as_ref() {
                    collect_expr_parameter_spans(where_clause, spans);
                }
                for item in &projection.order_by {
                    collect_expr_parameter_spans(&item.expr, spans);
                }
                if let Some(skip) = projection.skip.as_ref() {
                    collect_expr_parameter_spans(skip, spans);
                }
                if let Some(limit) = projection.limit.as_ref() {
                    collect_expr_parameter_spans(limit, spans);
                }
            }
        }
    }
}

fn collect_mutation_parameter_spans(
    mutation: &GqlMutationStatement,
) -> BTreeMap<String, SourceSpan> {
    let mut spans = BTreeMap::new();
    if let Some(pipeline) = mutation.read_prefix_pipeline.as_ref() {
        collect_read_pipeline_parameter_spans(pipeline, &mut spans);
    } else {
        for clause in &mutation.read_prefix {
            for pattern in &clause.patterns {
                collect_pattern_parameter_spans(pattern, &mut spans);
            }
            if let Some(where_clause) = clause.where_clause.as_ref() {
                collect_expr_parameter_spans(where_clause, &mut spans);
            }
        }
    }
    for clause in &mutation.mutation_clauses {
        match clause {
            MutationClause::Create(create) => {
                for pattern in &create.patterns {
                    collect_pattern_parameter_spans(pattern, &mut spans);
                }
            }
            MutationClause::Merge(merge) => {
                collect_pattern_parameter_spans(&merge.pattern, &mut spans);
                if let Some(on_create) = merge.on_create.as_ref() {
                    collect_set_parameter_spans(on_create, &mut spans);
                }
                if let Some(on_match) = merge.on_match.as_ref() {
                    collect_set_parameter_spans(on_match, &mut spans);
                }
            }
            MutationClause::Set(set) => {
                collect_set_parameter_spans(set, &mut spans);
            }
            MutationClause::Remove(remove) => {
                for item in &remove.items {
                    match item {
                        RemoveItem::Property { .. } | RemoveItem::NodeLabel { .. } => {}
                    }
                }
            }
            MutationClause::Delete(delete) => {
                for target in &delete.targets {
                    collect_expr_parameter_spans(target, &mut spans);
                }
            }
        }
    }
    if let Some(tail) = mutation.return_tail.as_ref() {
        collect_return_body_parameter_spans(&tail.return_clause.body, &mut spans);
        for item in &tail.order_by {
            collect_expr_parameter_spans(&item.expr, &mut spans);
        }
        if let Some(skip) = tail.skip.as_ref() {
            collect_expr_parameter_spans(skip, &mut spans);
        }
        if let Some(limit) = tail.limit.as_ref() {
            collect_expr_parameter_spans(limit, &mut spans);
        }
    }
    spans
}

fn collect_schema_parameter_spans(schema: &GqlSchemaStatement) -> BTreeMap<String, SourceSpan> {
    let mut spans = BTreeMap::new();
    match schema {
        GqlSchemaStatement::AlterGraphType(statement) => {
            for item in &statement.items {
                collect_schema_item_parameter_spans(item, &mut spans);
            }
            if let Some(options) = statement.options.as_ref() {
                collect_schema_literal_parameter_spans(options, &mut spans);
            }
            for item in &statement.drop_items {
                match item {
                    GqlSchemaDropItem::Node { .. } | GqlSchemaDropItem::Edge { .. } => {}
                }
            }
        }
        GqlSchemaStatement::CheckGraphType(statement) => {
            for item in &statement.items {
                collect_schema_item_parameter_spans(item, &mut spans);
            }
            if let Some(options) = statement.options.as_ref() {
                collect_schema_literal_parameter_spans(options, &mut spans);
            }
        }
        GqlSchemaStatement::DropCurrentGraphType { .. } | GqlSchemaStatement::Show(_) => {}
    }
    spans
}

fn collect_schema_item_parameter_spans(
    item: &GqlSchemaItem,
    spans: &mut BTreeMap<String, SourceSpan>,
) {
    match item {
        GqlSchemaItem::Node { schema, .. } | GqlSchemaItem::Edge { schema, .. } => {
            collect_schema_literal_parameter_spans(schema, spans);
        }
    }
}

fn collect_schema_literal_parameter_spans(
    literal: &GqlSchemaLiteral,
    spans: &mut BTreeMap<String, SourceSpan>,
) {
    match literal {
        GqlSchemaLiteral::Map(map) => collect_map_parameter_spans(map, spans),
        GqlSchemaLiteral::Parameter { name, span } => {
            spans.entry(name.clone()).or_insert_with(|| span.clone());
        }
    }
}

fn collect_set_parameter_spans(
    set: &crate::gql::ast::SetClause,
    spans: &mut BTreeMap<String, SourceSpan>,
) {
    for item in &set.items {
        match item {
            SetItem::Property { value, .. }
            | SetItem::Metadata { value, .. }
            | SetItem::MapMerge { value, .. } => {
                collect_expr_parameter_spans(value, spans);
            }
            SetItem::NodeLabel { .. } => {}
        }
    }
}

fn collect_return_body_parameter_spans(
    body: &ReturnBody,
    spans: &mut BTreeMap<String, SourceSpan>,
) {
    match body {
        ReturnBody::All(_) => {}
        ReturnBody::AllAndItems { items, .. } | ReturnBody::Items(items) => {
            for item in items {
                collect_expr_parameter_spans(&item.expr, spans);
            }
        }
    }
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
            ExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(else_expr) = else_expr {
                    stack.push(else_expr);
                }
                for branch in branches.iter().rev() {
                    stack.push(&branch.then);
                    stack.push(&branch.when);
                }
                if let Some(operand) = operand {
                    stack.push(operand);
                }
            }
            ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
                for arg in args.iter().rev() {
                    stack.push(arg);
                }
            }
            ExprKind::AggregateCall { arg, .. } => {
                if let Some(arg) = arg.as_ref() {
                    stack.push(arg);
                }
            }
            ExprKind::ExistsSubquery(pipeline) => {
                collect_read_pipeline_parameter_spans(pipeline, spans);
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
    options: &GqlExecutionOptions,
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
    options: &GqlExecutionOptions,
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
    options: &GqlExecutionOptions,
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
    options: &GqlExecutionOptions,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn referenced(source: &str) -> Vec<String> {
        referenced_param_names_for_query(source, &GqlExecutionOptions::default()).unwrap()
    }

    #[test]
    fn gql_schema_params_collect_whole_maps_and_nested_values() {
        let params = referenced(
            "ALTER CURRENT GRAPH TYPE ADD {
                NODE Person = $person_schema,
                EDGE WORKS_ON = {
                    properties: {
                        role: { enum_values: [$role, $fallback_role] }
                    }
                }
            } OPTIONS { max_violations: $max_violations, chunk_size: $chunk_size, scan_limit: $scan_limit }",
        );
        assert_eq!(
            params,
            vec![
                "chunk_size",
                "fallback_role",
                "max_violations",
                "person_schema",
                "role",
                "scan_limit"
            ]
        );
    }

    #[test]
    fn gql_schema_params_collect_options_parameter() {
        let params = referenced(
            "CHECK CURRENT GRAPH TYPE SET { NODE Person = { properties: { name: $name_schema } } } OPTIONS $options",
        );
        assert_eq!(params, vec!["name_schema", "options"]);
    }

    #[test]
    fn gql_schema_params_preserve_query_and_mutation_behavior() {
        assert_eq!(
            referenced("MATCH (n:Person {elementKey: $key}) RETURN n.name"),
            vec!["key"]
        );
        assert_eq!(
            referenced("MATCH (n:Person) SET n.name = $name RETURN n"),
            vec!["name"]
        );
    }
}
