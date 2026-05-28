#![allow(dead_code)]

use crate::error::EngineError;
use crate::gql::ast::*;
use crate::row_projection::{DIRECT_EDGE_ALIAS, DIRECT_NODE_ALIAS};
use crate::types::{
    validate_label_token_name, GqlParams, GqlSemanticErrorCode, SourceSpan,
    MAX_NODE_LABELS_PER_NODE,
};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlAliasKind {
    Node,
    Edge,
    Path,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlAliasBinding {
    pub(crate) name: String,
    pub(crate) kind: GqlAliasKind,
    pub(crate) span: SourceSpan,
    pub(crate) user_visible: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GqlAliasTable {
    pub(crate) by_name: BTreeMap<String, GqlAliasBinding>,
    pub(crate) user_order: Vec<String>,
}

impl GqlAliasTable {
    pub(crate) fn get(&self, name: &str) -> Option<&GqlAliasBinding> {
        self.by_name.get(name)
    }

    pub(crate) fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundNodePattern {
    pub(crate) alias: String,
    pub(crate) user_alias: Option<String>,
    pub(crate) labels: Vec<Ident>,
    pub(crate) properties: Option<MapLiteral>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundEdgePattern {
    pub(crate) alias: Option<String>,
    pub(crate) user_alias: Option<String>,
    pub(crate) from_alias: String,
    pub(crate) to_alias: String,
    pub(crate) rel_types: Vec<Ident>,
    pub(crate) direction: RelationshipDirection,
    pub(crate) quantifier: Option<RelationshipQuantifier>,
    pub(crate) properties: Option<MapLiteral>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundPattern {
    pub(crate) path_alias: Option<String>,
    pub(crate) user_path_alias: Option<String>,
    pub(crate) path_span: Option<SourceSpan>,
    pub(crate) nodes: Vec<GqlBoundNodePattern>,
    pub(crate) edges: Vec<GqlBoundEdgePattern>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundMatchClause {
    pub(crate) optional: bool,
    pub(crate) patterns: Vec<GqlBoundPattern>,
    pub(crate) where_clause: Option<Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlReturnItemBinding {
    pub(crate) expr: Expr,
    pub(crate) explicit_alias: Option<String>,
    pub(crate) output_name: String,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlReturnPlan {
    Star {
        span: SourceSpan,
        expanded_aliases: Vec<String>,
    },
    Items(Vec<GqlReturnItemBinding>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlSemanticPlan {
    pub(crate) query: GqlQuery,
    pub(crate) aliases: GqlAliasTable,
    pub(crate) clauses: Vec<GqlBoundMatchClause>,
    pub(crate) returns: GqlReturnPlan,
    pub(crate) parameters: Vec<String>,
    pub(crate) parameter_spans: BTreeMap<String, SourceSpan>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlAliasOrigin {
    ReadPrefix,
    Created,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlMutationAliasBinding {
    pub(crate) name: String,
    pub(crate) kind: GqlAliasKind,
    pub(crate) origin: GqlAliasOrigin,
    pub(crate) nullable: bool,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationSemanticPlan {
    pub(crate) statement: GqlMutationStatement,
    pub(crate) read_prefix: Option<GqlSemanticPlan>,
    pub(crate) aliases: BTreeMap<String, GqlMutationAliasBinding>,
    pub(crate) user_order: Vec<String>,
    pub(crate) clauses: Vec<GqlBoundMutationClause>,
    pub(crate) returns: Option<GqlReturnPlan>,
    pub(crate) parameters: Vec<String>,
    pub(crate) parameter_spans: BTreeMap<String, SourceSpan>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlBoundMutationClause {
    Create(GqlBoundCreateClause),
    Set(GqlBoundSetClause),
    Remove(GqlBoundRemoveClause),
    Delete(GqlBoundDeleteClause),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundCreateClause {
    pub(crate) patterns: Vec<GqlBoundCreatePattern>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundCreatePattern {
    pub(crate) nodes: Vec<GqlBoundCreateNode>,
    pub(crate) edges: Vec<GqlBoundCreateEdge>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundCreateNode {
    pub(crate) alias: String,
    pub(crate) labels: Vec<Ident>,
    pub(crate) properties: Option<MapLiteral>,
    pub(crate) created: bool,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundCreateEdge {
    pub(crate) alias: Option<String>,
    pub(crate) from_alias: String,
    pub(crate) to_alias: String,
    pub(crate) rel_type: Ident,
    pub(crate) properties: Option<MapLiteral>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundSetClause {
    pub(crate) items: Vec<GqlBoundSetItem>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlBoundSetItem {
    Property {
        alias: String,
        target_kind: GqlAliasKind,
        property: Ident,
        value: Expr,
        span: SourceSpan,
    },
    MapMerge {
        alias: String,
        target_kind: GqlAliasKind,
        value: Expr,
        span: SourceSpan,
    },
    NodeLabel {
        alias: String,
        label: Ident,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundRemoveClause {
    pub(crate) items: Vec<GqlBoundRemoveItem>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlBoundRemoveItem {
    Property {
        alias: String,
        target_kind: GqlAliasKind,
        property: Ident,
        span: SourceSpan,
    },
    NodeLabel {
        alias: String,
        label: Ident,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundDeleteClause {
    pub(crate) detach: bool,
    pub(crate) targets: Vec<GqlBoundDeleteTarget>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundDeleteTarget {
    pub(crate) alias: String,
    pub(crate) kind: GqlAliasKind,
    pub(crate) span: SourceSpan,
}

pub(crate) fn bind_query(
    query: GqlQuery,
    params: &GqlParams,
) -> Result<GqlSemanticPlan, EngineError> {
    let mut binder = SemanticBinder {
        aliases: GqlAliasTable::default(),
        anonymous_node_counter: 0,
        parameters: BTreeSet::new(),
        parameter_spans: BTreeMap::new(),
        params,
    };

    let clauses = binder.bind_match_clauses(&query.match_clauses)?;
    binder.aliases.user_order = semantic_binding_order(&clauses);

    let returns = binder.bind_return_clause(&query.return_clause)?;
    let mut return_aliases = BTreeSet::new();
    if let GqlReturnPlan::Items(items) = &returns {
        for item in items {
            if let Some(alias) = item.explicit_alias.as_ref() {
                return_aliases.insert(alias.clone());
            }
        }
    }
    for item in &query.order_by {
        binder.validate_expr(&item.expr, &return_aliases)?;
    }
    if let Some(skip) = query.skip.as_ref() {
        binder.validate_expr(skip, &return_aliases)?;
    }
    if let Some(limit) = query.limit.as_ref() {
        binder.validate_expr(limit, &return_aliases)?;
    }

    let parameters = binder.parameters.into_iter().collect();
    Ok(GqlSemanticPlan {
        query,
        aliases: binder.aliases,
        clauses,
        returns,
        parameters,
        parameter_spans: binder.parameter_spans,
    })
}

pub(crate) fn bind_mutation(
    statement: GqlMutationStatement,
    params: &GqlParams,
) -> Result<GqlMutationSemanticPlan, EngineError> {
    for clause in &statement.read_prefix {
        if clause.patterns.len() != 1 {
            return Err(EngineError::GqlUnsupported {
                feature: "comma-separated mutation read-prefix pattern lists".to_string(),
                message: "mutation read-prefix MATCH clauses support exactly one pattern; use repeated MATCH clauses instead".to_string(),
                span: clause.span.clone(),
            });
        }
    }

    let read_prefix = if statement.read_prefix.is_empty() {
        None
    } else {
        Some(bind_query(
            synthetic_read_prefix_query(&statement.read_prefix, &statement.span),
            params,
        )?)
    };
    let (aliases, user_order) = read_prefix
        .as_ref()
        .map(read_prefix_mutation_aliases)
        .unwrap_or_default();
    let mut binder = MutationSemanticBinder {
        aliases,
        user_order,
        created_internal_counter: 0,
        deleted_aliases: BTreeSet::new(),
        incident_edges: read_prefix
            .as_ref()
            .map(read_prefix_incident_edges)
            .unwrap_or_default(),
        parameters: read_prefix
            .as_ref()
            .map(|plan| plan.parameters.iter().cloned().collect())
            .unwrap_or_default(),
        parameter_spans: read_prefix
            .as_ref()
            .map(|plan| plan.parameter_spans.clone())
            .unwrap_or_default(),
        params,
    };

    let mut bound_clauses = Vec::with_capacity(statement.mutation_clauses.len());
    let mut has_delete = false;
    for clause in &statement.mutation_clauses {
        let bound = binder.bind_mutation_clause(clause)?;
        if matches!(bound, GqlBoundMutationClause::Delete(_)) {
            has_delete = true;
        }
        bound_clauses.push(bound);
    }
    if has_delete {
        if let Some(return_tail) = statement.return_tail.as_ref() {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "RETURN after DELETE or DETACH DELETE is not supported".to_string(),
                return_tail.return_clause.span.clone(),
            ));
        }
    }

    let returns = statement
        .return_tail
        .as_ref()
        .map(|tail| binder.bind_mutation_return_tail(tail))
        .transpose()?;

    let parameters = binder.parameters.iter().cloned().collect();
    Ok(GqlMutationSemanticPlan {
        statement,
        read_prefix,
        aliases: binder.aliases,
        user_order: binder.user_order,
        clauses: bound_clauses,
        returns,
        parameters,
        parameter_spans: binder.parameter_spans,
    })
}

struct SemanticBinder<'a> {
    aliases: GqlAliasTable,
    anonymous_node_counter: usize,
    parameters: BTreeSet<String>,
    parameter_spans: BTreeMap<String, SourceSpan>,
    params: &'a GqlParams,
}

impl SemanticBinder<'_> {
    fn bind_match_clauses(
        &mut self,
        clauses: &[MatchClause],
    ) -> Result<Vec<GqlBoundMatchClause>, EngineError> {
        clauses
            .iter()
            .map(|clause| self.bind_match_clause(clause))
            .collect()
    }

    fn bind_match_clause(
        &mut self,
        clause: &MatchClause,
    ) -> Result<GqlBoundMatchClause, EngineError> {
        let patterns = clause
            .patterns
            .iter()
            .map(|pattern| self.bind_pattern(pattern))
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(where_clause) = clause.where_clause.as_ref() {
            self.validate_expr(where_clause, &BTreeSet::new())?;
        }
        for pattern in &clause.patterns {
            self.collect_pattern_parameters(pattern)?;
        }
        Ok(GqlBoundMatchClause {
            optional: clause.optional,
            patterns,
            where_clause: clause.where_clause.clone(),
            span: clause.span.clone(),
        })
    }

    fn bind_pattern(&mut self, pattern: &Pattern) -> Result<GqlBoundPattern, EngineError> {
        let (path_alias, user_path_alias, path_span) =
            if let Some(path_variable) = pattern.path_variable.as_ref() {
                self.bind_user_alias(path_variable, GqlAliasKind::Path)?;
                (
                    Some(path_variable.name.clone()),
                    Some(path_variable.name.clone()),
                    Some(path_variable.span.clone()),
                )
            } else {
                (None, None, None)
            };
        let mut nodes = Vec::with_capacity(pattern.chains.len() + 1);
        let mut edges = Vec::with_capacity(pattern.chains.len());
        let start = self.bind_node_pattern(&pattern.start)?;
        let mut previous_alias = start.alias.clone();
        nodes.push(start);

        for chain in &pattern.chains {
            let next = self.bind_node_pattern(&chain.node)?;
            let edge = self.bind_edge_pattern(
                &chain.relationship,
                previous_alias.clone(),
                next.alias.clone(),
            )?;
            previous_alias = next.alias.clone();
            edges.push(edge);
            nodes.push(next);
        }

        Ok(GqlBoundPattern {
            path_alias,
            user_path_alias,
            path_span,
            nodes,
            edges,
            span: pattern.span.clone(),
        })
    }

    fn bind_node_pattern(
        &mut self,
        pattern: &NodePattern,
    ) -> Result<GqlBoundNodePattern, EngineError> {
        let (alias, user_alias) = if let Some(variable) = pattern.variable.as_ref() {
            self.bind_node_alias(variable)?;
            (variable.name.clone(), Some(variable.name.clone()))
        } else {
            (self.next_internal_node_alias(), None)
        };

        Ok(GqlBoundNodePattern {
            alias,
            user_alias,
            labels: pattern.labels.clone(),
            properties: pattern.properties.clone(),
            span: pattern.span.clone(),
        })
    }

    fn bind_edge_pattern(
        &mut self,
        pattern: &RelationshipPattern,
        from_alias: String,
        to_alias: String,
    ) -> Result<GqlBoundEdgePattern, EngineError> {
        let (alias, user_alias) = if let Some(variable) = pattern.variable.as_ref() {
            if pattern
                .quantifier
                .as_ref()
                .is_some_and(|quantifier| quantifier.min_hops != 1 || quantifier.max_hops != 1)
            {
                return Err(EngineError::GqlUnsupported {
                    feature: "multi-hop relationship-list aliases".to_string(),
                    message: "relationship aliases on variable-length patterns are supported only for exactly 1..1; return the path alias and inspect edge_ids instead".to_string(),
                    span: variable.span.clone(),
                });
            }
            self.bind_user_alias(variable, GqlAliasKind::Edge)?;
            (Some(variable.name.clone()), Some(variable.name.clone()))
        } else {
            (None, None)
        };

        Ok(GqlBoundEdgePattern {
            alias,
            user_alias,
            from_alias,
            to_alias,
            rel_types: pattern.rel_types.clone(),
            direction: pattern.direction,
            quantifier: pattern.quantifier.clone(),
            properties: pattern.properties.clone(),
            span: pattern.span.clone(),
        })
    }

    fn bind_node_alias(&mut self, ident: &Ident) -> Result<(), EngineError> {
        if let Some(existing) = self.aliases.by_name.get(&ident.name) {
            return if existing.kind == GqlAliasKind::Node {
                Ok(())
            } else {
                Err(gql_semantic_error(
                    GqlSemanticErrorCode::DuplicateAlias,
                    format!(
                        "alias '{}' is already bound as {:?}",
                        ident.name, existing.kind
                    ),
                    ident.span.clone(),
                ))
            };
        }
        self.bind_user_alias(ident, GqlAliasKind::Node)
    }

    fn bind_user_alias(&mut self, ident: &Ident, kind: GqlAliasKind) -> Result<(), EngineError> {
        if is_reserved_user_alias(&ident.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DuplicateAlias,
                format!("'{}' is reserved for internal GQL projection", ident.name),
                ident.span.clone(),
            ));
        }
        if self.aliases.by_name.contains_key(&ident.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DuplicateAlias,
                format!("duplicate alias '{}'", ident.name),
                ident.span.clone(),
            ));
        }
        self.aliases.by_name.insert(
            ident.name.clone(),
            GqlAliasBinding {
                name: ident.name.clone(),
                kind,
                span: ident.span.clone(),
                user_visible: true,
            },
        );
        self.aliases.user_order.push(ident.name.clone());
        Ok(())
    }

    fn next_internal_node_alias(&mut self) -> String {
        loop {
            let alias = format!("__gql_anon_node_{}", self.anonymous_node_counter);
            self.anonymous_node_counter += 1;
            if !self.aliases.by_name.contains_key(&alias) {
                self.aliases.by_name.insert(
                    alias.clone(),
                    GqlAliasBinding {
                        name: alias.clone(),
                        kind: GqlAliasKind::Node,
                        span: SourceSpan::new(0, 0, 1, 1),
                        user_visible: false,
                    },
                );
                return alias;
            }
        }
    }

    fn collect_pattern_parameters(&mut self, pattern: &Pattern) -> Result<(), EngineError> {
        if let Some(properties) = pattern.start.properties.as_ref() {
            self.collect_map_parameters(properties)?;
        }
        for chain in &pattern.chains {
            if let Some(properties) = chain.relationship.properties.as_ref() {
                self.collect_map_parameters(properties)?;
            }
            if let Some(properties) = chain.node.properties.as_ref() {
                self.collect_map_parameters(properties)?;
            }
        }
        Ok(())
    }

    fn collect_map_parameters(&mut self, literal: &MapLiteral) -> Result<(), EngineError> {
        for entry in &literal.entries {
            self.validate_expr(&entry.value, &BTreeSet::new())?;
        }
        Ok(())
    }

    fn bind_return_clause(&mut self, clause: &ReturnClause) -> Result<GqlReturnPlan, EngineError> {
        match &clause.body {
            ReturnBody::All(span) => Ok(GqlReturnPlan::Star {
                span: span.clone(),
                expanded_aliases: self.aliases.user_order.clone(),
            }),
            ReturnBody::Items(items) => {
                let mut bound = Vec::with_capacity(items.len());
                for item in items {
                    self.validate_expr(&item.expr, &BTreeSet::new())?;
                    let explicit_alias = item.alias.as_ref().map(|alias| alias.name.clone());
                    if let Some(alias) = item.alias.as_ref() {
                        if is_reserved_user_alias(&alias.name) {
                            return Err(gql_semantic_error(
                                GqlSemanticErrorCode::DuplicateAlias,
                                format!("'{}' is reserved for internal GQL projection", alias.name),
                                alias.span.clone(),
                            ));
                        }
                    }
                    let output_name = explicit_alias
                        .clone()
                        .unwrap_or_else(|| expression_output_name(&item.expr));
                    bound.push(GqlReturnItemBinding {
                        expr: item.expr.clone(),
                        explicit_alias,
                        output_name,
                        span: item.span.clone(),
                    });
                }
                Ok(GqlReturnPlan::Items(bound))
            }
        }
    }

    fn validate_expr(
        &mut self,
        expr: &Expr,
        return_aliases: &BTreeSet<String>,
    ) -> Result<(), EngineError> {
        match &expr.kind {
            ExprKind::Literal(_) => Ok(()),
            ExprKind::Parameter(name) => self.validate_parameter(name, &expr.span),
            ExprKind::Variable(name) => {
                if self.aliases.contains(name) || return_aliases.contains(name) {
                    Ok(())
                } else {
                    Err(gql_semantic_error(
                        GqlSemanticErrorCode::UnknownVariable,
                        format!("unknown variable '{}'", name),
                        expr.span.clone(),
                    ))
                }
            }
            ExprKind::PropertyAccess { object, property } => {
                self.validate_expr(object, return_aliases)?;
                if let ExprKind::Variable(alias) = &object.kind {
                    if self
                        .aliases
                        .get(alias)
                        .is_some_and(|binding| binding.kind == GqlAliasKind::Path)
                        && !is_supported_path_property(&property.name)
                    {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::InvalidPropertyAccess,
                            format!("unsupported path property '{}'", property.name),
                            property.span.clone(),
                        ));
                    }
                }
                Ok(())
            }
            ExprKind::Unary { expr, .. } => self.validate_expr(expr, return_aliases),
            ExprKind::Binary { left, right, .. } => {
                self.validate_expr(left, return_aliases)?;
                self.validate_expr(right, return_aliases)
            }
            ExprKind::IsNull { expr, .. } => self.validate_expr(expr, return_aliases),
            ExprKind::FunctionCall { name, args } => self.validate_function_call(name, args),
            ExprKind::List(items) => {
                for item in items {
                    self.validate_expr(item, return_aliases)?;
                }
                Ok(())
            }
            ExprKind::Map(map) => self.collect_map_parameters(map),
        }
    }

    fn validate_parameter(&mut self, name: &str, span: &SourceSpan) -> Result<(), EngineError> {
        if !self.params.contains_key(name) {
            return Err(EngineError::GqlParameter {
                name: name.to_string(),
                expected: "GqlParamValue".to_string(),
                message: format!("missing parameter '${name}'"),
                span: span.clone(),
            });
        }
        self.parameters.insert(name.to_string());
        self.parameter_spans
            .entry(name.to_string())
            .or_insert_with(|| span.clone());
        Ok(())
    }

    fn validate_function_call(&mut self, name: &Ident, args: &[Expr]) -> Result<(), EngineError> {
        let function = name.name.to_ascii_lowercase();
        match function.as_str() {
            "id" | "labels" | "type" | "length" | "start_node" | "end_node" | "nodes"
            | "relationships" | "node_ids" | "edge_ids" => {}
            _ => {
                return Err(EngineError::GqlUnsupported {
                    feature: "function".to_string(),
                    message: format!("function '{}' is not supported in Phase 31", name.name),
                    span: name.span.clone(),
                });
            }
        }
        if args.len() != 1 {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("function '{}' expects exactly one argument", name.name),
                name.span.clone(),
            ));
        }
        self.validate_expr(&args[0], &BTreeSet::new())?;
        let Some(alias) = variable_name(&args[0]) else {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("function '{}' expects a bound alias argument", name.name),
                args[0].span.clone(),
            ));
        };
        let binding = self.aliases.get(alias).expect("alias validated above");
        match (function.as_str(), binding.kind) {
            ("labels", GqlAliasKind::Node)
            | ("type", GqlAliasKind::Edge)
            | ("id", GqlAliasKind::Node | GqlAliasKind::Edge)
            | ("length", GqlAliasKind::Path)
            | ("start_node", GqlAliasKind::Path)
            | ("end_node", GqlAliasKind::Path)
            | ("nodes", GqlAliasKind::Path)
            | ("relationships", GqlAliasKind::Path)
            | ("node_ids", GqlAliasKind::Path)
            | ("edge_ids", GqlAliasKind::Path) => Ok(()),
            ("labels", GqlAliasKind::Edge | GqlAliasKind::Path) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "labels() expects a node alias".to_string(),
                args[0].span.clone(),
            )),
            ("type", GqlAliasKind::Node | GqlAliasKind::Path) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "type() expects an edge alias".to_string(),
                args[0].span.clone(),
            )),
            ("id", GqlAliasKind::Path) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "id() expects a node or edge alias".to_string(),
                args[0].span.clone(),
            )),
            (
                "length" | "start_node" | "end_node" | "nodes" | "relationships" | "node_ids"
                | "edge_ids",
                GqlAliasKind::Node | GqlAliasKind::Edge,
            ) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("{}() expects a path alias", name.name),
                args[0].span.clone(),
            )),
            _ => Err(EngineError::GqlUnsupported {
                feature: "function".to_string(),
                message: format!("function '{}' is not supported", name.name),
                span: name.span.clone(),
            }),
        }
    }
}

struct MutationSemanticBinder<'a> {
    aliases: BTreeMap<String, GqlMutationAliasBinding>,
    user_order: Vec<String>,
    created_internal_counter: usize,
    deleted_aliases: BTreeSet<String>,
    incident_edges: BTreeMap<String, BTreeSet<String>>,
    parameters: BTreeSet<String>,
    parameter_spans: BTreeMap<String, SourceSpan>,
    params: &'a GqlParams,
}

impl MutationSemanticBinder<'_> {
    fn bind_mutation_clause(
        &mut self,
        clause: &MutationClause,
    ) -> Result<GqlBoundMutationClause, EngineError> {
        match clause {
            MutationClause::Create(create) => self
                .bind_create_clause(create)
                .map(GqlBoundMutationClause::Create),
            MutationClause::Set(set) => self.bind_set_clause(set).map(GqlBoundMutationClause::Set),
            MutationClause::Remove(remove) => self
                .bind_remove_clause(remove)
                .map(GqlBoundMutationClause::Remove),
            MutationClause::Delete(delete) => self
                .bind_delete_clause(delete)
                .map(GqlBoundMutationClause::Delete),
        }
    }

    fn bind_create_clause(
        &mut self,
        create: &CreateClause,
    ) -> Result<GqlBoundCreateClause, EngineError> {
        let patterns = create
            .patterns
            .iter()
            .map(|pattern| self.bind_create_pattern(pattern))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GqlBoundCreateClause {
            patterns,
            span: create.span.clone(),
        })
    }

    fn bind_create_pattern(
        &mut self,
        pattern: &Pattern,
    ) -> Result<GqlBoundCreatePattern, EngineError> {
        if let Some(path_variable) = pattern.path_variable.as_ref() {
            return Err(EngineError::GqlUnsupported {
                feature: "CREATE path assignment".to_string(),
                message: "CREATE path assignment is not supported".to_string(),
                span: path_variable.span.clone(),
            });
        }

        let mut nodes = Vec::with_capacity(pattern.chains.len() + 1);
        let mut edges = Vec::with_capacity(pattern.chains.len());
        let mut pattern_created_aliases = BTreeSet::new();
        let has_relationships = !pattern.chains.is_empty();
        let start = self.bind_create_node_pattern(
            &pattern.start,
            has_relationships,
            &pattern_created_aliases,
        )?;
        let mut previous_alias = start.alias.clone();
        if start.created {
            pattern_created_aliases.insert(start.alias.clone());
        }
        nodes.push(start);

        for chain in &pattern.chains {
            let next =
                self.bind_create_node_pattern(&chain.node, true, &pattern_created_aliases)?;
            let next_alias = next.alias.clone();
            let edge =
                self.bind_create_edge_pattern(&chain.relationship, &previous_alias, &next_alias)?;
            previous_alias = next_alias;
            if next.created {
                pattern_created_aliases.insert(next.alias.clone());
            }
            edges.push(edge);
            nodes.push(next);
        }

        Ok(GqlBoundCreatePattern {
            nodes,
            edges,
            span: pattern.span.clone(),
        })
    }

    fn bind_create_node_pattern(
        &mut self,
        pattern: &NodePattern,
        relationship_endpoint: bool,
        pattern_created_aliases: &BTreeSet<String>,
    ) -> Result<GqlBoundCreateNode, EngineError> {
        if let Some(variable) = pattern.variable.as_ref() {
            if let Some(existing) = self.aliases.get(&variable.name).cloned() {
                if existing.kind != GqlAliasKind::Node {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::DuplicateAlias,
                        format!(
                            "CREATE endpoint alias '{}' is bound as {:?}, not a node",
                            variable.name, existing.kind
                        ),
                        variable.span.clone(),
                    ));
                }
                if self.deleted_aliases.contains(&variable.name) {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!(
                            "CREATE endpoint alias '{}' was deleted earlier in this statement",
                            variable.name
                        ),
                        variable.span.clone(),
                    ));
                }
                if !pattern.labels.is_empty() || pattern.properties.is_some() {
                    if existing.origin == GqlAliasOrigin::Created {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::DuplicateAlias,
                            format!("created node alias '{}' is already bound", variable.name),
                            variable.span.clone(),
                        ));
                    }
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!(
                            "bound CREATE endpoint '{}' must be bare; use SET for labels or properties",
                            variable.name
                        ),
                        pattern.span.clone(),
                    ));
                }
                if !relationship_endpoint {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!(
                            "existing CREATE endpoint '{}' must be incident to a relationship",
                            variable.name
                        ),
                        pattern.span.clone(),
                    ));
                }
                if existing.origin == GqlAliasOrigin::Created
                    && !pattern_created_aliases.contains(&variable.name)
                {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::DuplicateAlias,
                        format!(
                            "created node alias '{}' cannot be reused outside its CREATE pattern chain",
                            variable.name
                        ),
                        variable.span.clone(),
                    ));
                }
                return Ok(GqlBoundCreateNode {
                    alias: variable.name.clone(),
                    labels: Vec::new(),
                    properties: None,
                    created: false,
                    span: pattern.span.clone(),
                });
            }

            self.validate_new_create_node(pattern)?;
            self.insert_created_alias(variable, GqlAliasKind::Node)?;
            Ok(GqlBoundCreateNode {
                alias: variable.name.clone(),
                labels: pattern.labels.clone(),
                properties: pattern.properties.clone(),
                created: true,
                span: pattern.span.clone(),
            })
        } else {
            self.validate_new_create_node(pattern)?;
            Ok(GqlBoundCreateNode {
                alias: self.next_internal_created_alias("node"),
                labels: pattern.labels.clone(),
                properties: pattern.properties.clone(),
                created: true,
                span: pattern.span.clone(),
            })
        }
    }

    fn bind_create_edge_pattern(
        &mut self,
        pattern: &RelationshipPattern,
        previous_alias: &str,
        next_alias: &str,
    ) -> Result<GqlBoundCreateEdge, EngineError> {
        if pattern.direction == RelationshipDirection::Undirected {
            return Err(EngineError::GqlUnsupported {
                feature: "undirected CREATE relationship".to_string(),
                message: "CREATE relationship patterns must be directed".to_string(),
                span: pattern.span.clone(),
            });
        }
        if pattern.quantifier.is_some() {
            return Err(EngineError::GqlUnsupported {
                feature: "variable-length CREATE relationship".to_string(),
                message: "variable-length relationship patterns are not supported in CREATE"
                    .to_string(),
                span: pattern.span.clone(),
            });
        }
        if pattern.rel_types.len() != 1 {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DynamicRelationshipTypeNotSupported,
                "CREATE relationship patterns require exactly one static relationship label"
                    .to_string(),
                pattern.span.clone(),
            ));
        }
        for label in &pattern.rel_types {
            validate_label_token_name(&label.name).map_err(|err| match err {
                EngineError::InvalidOperation(message) => gql_semantic_error(
                    GqlSemanticErrorCode::DynamicRelationshipTypeNotSupported,
                    message,
                    label.span.clone(),
                ),
                other => other,
            })?;
        }
        if let Some(properties) = pattern.properties.as_ref() {
            self.validate_create_edge_map(properties)?;
        }

        let alias = if let Some(variable) = pattern.variable.as_ref() {
            if self.aliases.contains_key(&variable.name) {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::DuplicateAlias,
                    format!(
                        "CREATE relationship alias '{}' is already bound",
                        variable.name
                    ),
                    variable.span.clone(),
                ));
            }
            self.insert_created_alias(variable, GqlAliasKind::Edge)?;
            self.record_incident_edge(previous_alias, next_alias, &variable.name);
            Some(variable.name.clone())
        } else {
            None
        };
        let (from_alias, to_alias) = match pattern.direction {
            RelationshipDirection::LeftToRight => {
                (previous_alias.to_string(), next_alias.to_string())
            }
            RelationshipDirection::RightToLeft => {
                (next_alias.to_string(), previous_alias.to_string())
            }
            RelationshipDirection::Undirected => unreachable!("rejected above"),
        };
        Ok(GqlBoundCreateEdge {
            alias,
            from_alias,
            to_alias,
            rel_type: pattern.rel_types[0].clone(),
            properties: pattern.properties.clone(),
            span: pattern.span.clone(),
        })
    }

    fn bind_set_clause(&mut self, set: &SetClause) -> Result<GqlBoundSetClause, EngineError> {
        let items = set
            .items
            .iter()
            .map(|item| self.bind_set_item(item))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GqlBoundSetClause {
            items,
            span: set.span.clone(),
        })
    }

    fn bind_set_item(&mut self, item: &SetItem) -> Result<GqlBoundSetItem, EngineError> {
        match item {
            SetItem::Property {
                alias,
                property,
                value,
                span,
            } => {
                let binding = self.require_target_alias(alias)?;
                reject_reserved_set_property(binding.kind, property)?;
                self.validate_expr(value, &BTreeSet::new(), false)?;
                self.reject_statically_element_property_value(value)?;
                Ok(GqlBoundSetItem::Property {
                    alias: alias.name.clone(),
                    target_kind: binding.kind,
                    property: property.clone(),
                    value: value.clone(),
                    span: span.clone(),
                })
            }
            SetItem::MapMerge { alias, value, span } => {
                let binding = self.require_target_alias(alias)?;
                self.validate_expr(value, &BTreeSet::new(), false)?;
                self.reject_statically_element_property_value(value)?;
                Ok(GqlBoundSetItem::MapMerge {
                    alias: alias.name.clone(),
                    target_kind: binding.kind,
                    value: value.clone(),
                    span: span.clone(),
                })
            }
            SetItem::NodeLabel { alias, label, span } => {
                let binding = self.require_target_alias(alias)?;
                if binding.kind != GqlAliasKind::Node {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        "SET node labels require a node alias".to_string(),
                        alias.span.clone(),
                    ));
                }
                validate_label_token_name(&label.name).map_err(|err| match err {
                    EngineError::InvalidOperation(message) => gql_semantic_error(
                        GqlSemanticErrorCode::DynamicLabelNotSupported,
                        message,
                        label.span.clone(),
                    ),
                    other => other,
                })?;
                Ok(GqlBoundSetItem::NodeLabel {
                    alias: alias.name.clone(),
                    label: label.clone(),
                    span: span.clone(),
                })
            }
        }
    }

    fn bind_remove_clause(
        &mut self,
        remove: &RemoveClause,
    ) -> Result<GqlBoundRemoveClause, EngineError> {
        let items = remove
            .items
            .iter()
            .map(|item| self.bind_remove_item(item))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GqlBoundRemoveClause {
            items,
            span: remove.span.clone(),
        })
    }

    fn bind_remove_item(&mut self, item: &RemoveItem) -> Result<GqlBoundRemoveItem, EngineError> {
        match item {
            RemoveItem::Property {
                alias,
                property,
                span,
            } => {
                let binding = self.require_target_alias(alias)?;
                reject_reserved_remove_property(binding.kind, property)?;
                Ok(GqlBoundRemoveItem::Property {
                    alias: alias.name.clone(),
                    target_kind: binding.kind,
                    property: property.clone(),
                    span: span.clone(),
                })
            }
            RemoveItem::NodeLabel { alias, label, span } => {
                let binding = self.require_target_alias(alias)?;
                if binding.kind != GqlAliasKind::Node {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        "REMOVE node labels require a node alias".to_string(),
                        alias.span.clone(),
                    ));
                }
                validate_label_token_name(&label.name).map_err(|err| match err {
                    EngineError::InvalidOperation(message) => gql_semantic_error(
                        GqlSemanticErrorCode::DynamicLabelNotSupported,
                        message,
                        label.span.clone(),
                    ),
                    other => other,
                })?;
                Ok(GqlBoundRemoveItem::NodeLabel {
                    alias: alias.name.clone(),
                    label: label.clone(),
                    span: span.clone(),
                })
            }
        }
    }

    fn bind_delete_clause(
        &mut self,
        delete: &DeleteClause,
    ) -> Result<GqlBoundDeleteClause, EngineError> {
        let mut targets = Vec::with_capacity(delete.targets.len());
        for target in &delete.targets {
            self.validate_expr(target, &BTreeSet::new(), true)?;
            let Some(alias) = variable_name(target) else {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "DELETE targets must be bound node or edge aliases".to_string(),
                    target.span.clone(),
                ));
            };
            let Some(binding) = self.aliases.get(alias) else {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::UnknownVariable,
                    format!("unknown DELETE target alias '{alias}'"),
                    target.span.clone(),
                ));
            };
            match (delete.detach, binding.kind) {
                (false, GqlAliasKind::Edge) | (true, GqlAliasKind::Node) => {}
                (false, GqlAliasKind::Node) => {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        "DELETE of node aliases requires DETACH DELETE".to_string(),
                        target.span.clone(),
                    ));
                }
                (true, GqlAliasKind::Edge) => {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        "DETACH DELETE accepts node aliases only".to_string(),
                        target.span.clone(),
                    ));
                }
                (_, GqlAliasKind::Path) => {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        "path aliases cannot be deleted".to_string(),
                        target.span.clone(),
                    ));
                }
            }
            targets.push(GqlBoundDeleteTarget {
                alias: alias.to_string(),
                kind: binding.kind,
                span: target.span.clone(),
            });
            self.mark_deleted_alias(alias, binding.kind);
        }
        Ok(GqlBoundDeleteClause {
            detach: delete.detach,
            targets,
            span: delete.span.clone(),
        })
    }

    fn bind_mutation_return_tail(
        &mut self,
        tail: &MutationReturnTail,
    ) -> Result<GqlReturnPlan, EngineError> {
        let returns = self.bind_return_clause(&tail.return_clause)?;
        let mut explicit_return_aliases = BTreeSet::new();
        if let GqlReturnPlan::Items(items) = &returns {
            for item in items {
                if let Some(alias) = item.explicit_alias.as_ref() {
                    explicit_return_aliases.insert(alias.clone());
                }
            }
        }
        for item in &tail.order_by {
            self.validate_expr(&item.expr, &explicit_return_aliases, true)?;
        }
        if let Some(skip) = tail.skip.as_ref() {
            self.validate_expr(skip, &explicit_return_aliases, true)?;
        }
        if let Some(limit) = tail.limit.as_ref() {
            self.validate_expr(limit, &explicit_return_aliases, true)?;
        }
        Ok(returns)
    }

    fn bind_return_clause(&mut self, clause: &ReturnClause) -> Result<GqlReturnPlan, EngineError> {
        match &clause.body {
            ReturnBody::All(span) => Ok(GqlReturnPlan::Star {
                span: span.clone(),
                expanded_aliases: self.user_order.clone(),
            }),
            ReturnBody::Items(items) => {
                let mut bound = Vec::with_capacity(items.len());
                let mut output_names = BTreeSet::new();
                for item in items {
                    self.validate_expr(&item.expr, &BTreeSet::new(), true)?;
                    let explicit_alias = item.alias.as_ref().map(|alias| alias.name.clone());
                    if let Some(alias) = item.alias.as_ref() {
                        if is_reserved_user_alias(&alias.name) {
                            return Err(gql_semantic_error(
                                GqlSemanticErrorCode::DuplicateAlias,
                                format!("'{}' is reserved for internal GQL projection", alias.name),
                                alias.span.clone(),
                            ));
                        }
                    }
                    let output_name = explicit_alias
                        .clone()
                        .unwrap_or_else(|| expression_output_name(&item.expr));
                    if !output_names.insert(output_name.clone()) {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::DuplicateAlias,
                            format!("duplicate mutation RETURN alias '{}'", output_name),
                            item.span.clone(),
                        ));
                    }
                    bound.push(GqlReturnItemBinding {
                        expr: item.expr.clone(),
                        explicit_alias,
                        output_name,
                        span: item.span.clone(),
                    });
                }
                Ok(GqlReturnPlan::Items(bound))
            }
        }
    }

    fn validate_new_create_node(&mut self, pattern: &NodePattern) -> Result<(), EngineError> {
        if pattern.labels.is_empty() {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "CREATE node patterns require at least one static label".to_string(),
                pattern.span.clone(),
            ));
        }
        if pattern.labels.len() > MAX_NODE_LABELS_PER_NODE {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!(
                    "CREATE node patterns may contain at most {} labels",
                    MAX_NODE_LABELS_PER_NODE
                ),
                pattern.span.clone(),
            ));
        }
        let mut labels = BTreeSet::new();
        for label in &pattern.labels {
            validate_label_token_name(&label.name).map_err(|err| match err {
                EngineError::InvalidOperation(message) => gql_semantic_error(
                    GqlSemanticErrorCode::DynamicLabelNotSupported,
                    message,
                    label.span.clone(),
                ),
                other => other,
            })?;
            if !labels.insert(label.name.clone()) {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::DuplicateAlias,
                    format!("duplicate CREATE node label '{}'", label.name),
                    label.span.clone(),
                ));
            }
        }
        let Some(properties) = pattern.properties.as_ref() else {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "CREATE node patterns require a property map containing key".to_string(),
                pattern.span.clone(),
            ));
        };
        let mut has_key = false;
        for entry in &properties.entries {
            if entry.key.name == "key" {
                has_key = true;
            }
            if is_reserved_create_node_property(&entry.key.name) {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidPropertyAccess,
                    format!(
                        "CREATE node property '{}' is reserved metadata and cannot be set here",
                        entry.key.name
                    ),
                    entry.key.span.clone(),
                ));
            }
            self.validate_expr(&entry.value, &BTreeSet::new(), false)?;
            self.reject_statically_element_property_value(&entry.value)?;
        }
        if !has_key {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "CREATE node property map must contain key".to_string(),
                properties.span.clone(),
            ));
        }
        Ok(())
    }

    fn validate_create_edge_map(&mut self, properties: &MapLiteral) -> Result<(), EngineError> {
        for entry in &properties.entries {
            if is_reserved_create_edge_property(&entry.key.name) {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidPropertyAccess,
                    format!(
                        "CREATE relationship property '{}' is reserved metadata and cannot be set here",
                        entry.key.name
                    ),
                    entry.key.span.clone(),
                ));
            }
            self.validate_expr(&entry.value, &BTreeSet::new(), false)?;
            self.reject_statically_element_property_value(&entry.value)?;
        }
        Ok(())
    }

    fn require_target_alias(&self, alias: &Ident) -> Result<GqlMutationAliasBinding, EngineError> {
        let Some(binding) = self.aliases.get(&alias.name) else {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::UnknownVariable,
                format!("unknown mutation target alias '{}'", alias.name),
                alias.span.clone(),
            ));
        };
        if self.deleted_aliases.contains(&alias.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!(
                    "mutation target alias '{}' was deleted earlier in this statement",
                    alias.name
                ),
                alias.span.clone(),
            ));
        }
        if binding.kind == GqlAliasKind::Path {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidPropertyAccess,
                "path aliases cannot be mutation targets".to_string(),
                alias.span.clone(),
            ));
        }
        Ok(binding.clone())
    }

    fn validate_expr(
        &mut self,
        expr: &Expr,
        return_aliases: &BTreeSet<String>,
        allow_created_sources: bool,
    ) -> Result<(), EngineError> {
        match &expr.kind {
            ExprKind::Literal(_) => Ok(()),
            ExprKind::Parameter(name) => self.validate_parameter(name, &expr.span),
            ExprKind::Variable(name) => {
                if let Some(binding) = self.aliases.get(name) {
                    if binding.origin == GqlAliasOrigin::Created && !allow_created_sources {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::InvalidReturnExpression,
                            format!(
                                "created alias '{}' cannot be used as a mutation expression source before commit",
                                name
                            ),
                            expr.span.clone(),
                        ));
                    }
                    Ok(())
                } else if return_aliases.contains(name) {
                    Ok(())
                } else {
                    Err(gql_semantic_error(
                        GqlSemanticErrorCode::UnknownVariable,
                        format!("unknown variable '{}'", name),
                        expr.span.clone(),
                    ))
                }
            }
            ExprKind::PropertyAccess { object, property } => {
                self.validate_expr(object, return_aliases, allow_created_sources)?;
                if let ExprKind::Variable(alias) = &object.kind {
                    if self
                        .aliases
                        .get(alias)
                        .is_some_and(|binding| binding.kind == GqlAliasKind::Path)
                        && !is_supported_path_property(&property.name)
                    {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::InvalidPropertyAccess,
                            format!("unsupported path property '{}'", property.name),
                            property.span.clone(),
                        ));
                    }
                }
                Ok(())
            }
            ExprKind::Unary { expr, .. } => {
                self.validate_expr(expr, return_aliases, allow_created_sources)
            }
            ExprKind::Binary { left, right, .. } => {
                self.validate_expr(left, return_aliases, allow_created_sources)?;
                self.validate_expr(right, return_aliases, allow_created_sources)
            }
            ExprKind::IsNull { expr, .. } => {
                self.validate_expr(expr, return_aliases, allow_created_sources)
            }
            ExprKind::FunctionCall { name, args } => {
                self.validate_function_call(name, args, allow_created_sources)
            }
            ExprKind::List(items) => {
                for item in items {
                    self.validate_expr(item, return_aliases, allow_created_sources)?;
                }
                Ok(())
            }
            ExprKind::Map(map) => {
                for entry in &map.entries {
                    self.validate_expr(&entry.value, return_aliases, allow_created_sources)?;
                }
                Ok(())
            }
        }
    }

    fn validate_parameter(&mut self, name: &str, span: &SourceSpan) -> Result<(), EngineError> {
        if !self.params.contains_key(name) {
            return Err(EngineError::GqlParameter {
                name: name.to_string(),
                expected: "GqlParamValue".to_string(),
                message: format!("missing parameter '${name}'"),
                span: span.clone(),
            });
        }
        self.parameters.insert(name.to_string());
        self.parameter_spans
            .entry(name.to_string())
            .or_insert_with(|| span.clone());
        Ok(())
    }

    fn validate_function_call(
        &mut self,
        name: &Ident,
        args: &[Expr],
        allow_created_sources: bool,
    ) -> Result<(), EngineError> {
        let function = name.name.to_ascii_lowercase();
        match function.as_str() {
            "id" | "labels" | "type" | "length" | "start_node" | "end_node" | "nodes"
            | "relationships" | "node_ids" | "edge_ids" => {}
            _ => {
                return Err(EngineError::GqlUnsupported {
                    feature: "function".to_string(),
                    message: format!("function '{}' is not supported", name.name),
                    span: name.span.clone(),
                });
            }
        }
        if args.len() != 1 {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("function '{}' expects exactly one argument", name.name),
                name.span.clone(),
            ));
        }
        self.validate_expr(&args[0], &BTreeSet::new(), allow_created_sources)?;
        let Some(alias) = variable_name(&args[0]) else {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("function '{}' expects a bound alias argument", name.name),
                args[0].span.clone(),
            ));
        };
        let binding = self.aliases.get(alias).expect("alias validated above");
        match (function.as_str(), binding.kind) {
            ("labels", GqlAliasKind::Node)
            | ("type", GqlAliasKind::Edge)
            | ("id", GqlAliasKind::Node | GqlAliasKind::Edge)
            | ("length", GqlAliasKind::Path)
            | ("start_node", GqlAliasKind::Path)
            | ("end_node", GqlAliasKind::Path)
            | ("nodes", GqlAliasKind::Path)
            | ("relationships", GqlAliasKind::Path)
            | ("node_ids", GqlAliasKind::Path)
            | ("edge_ids", GqlAliasKind::Path) => Ok(()),
            ("labels", GqlAliasKind::Edge | GqlAliasKind::Path) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "labels() expects a node alias".to_string(),
                args[0].span.clone(),
            )),
            ("type", GqlAliasKind::Node | GqlAliasKind::Path) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "type() expects an edge alias".to_string(),
                args[0].span.clone(),
            )),
            ("id", GqlAliasKind::Path) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "id() expects a node or edge alias".to_string(),
                args[0].span.clone(),
            )),
            (
                "length" | "start_node" | "end_node" | "nodes" | "relationships" | "node_ids"
                | "edge_ids",
                GqlAliasKind::Node | GqlAliasKind::Edge,
            ) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("{}() expects a path alias", name.name),
                args[0].span.clone(),
            )),
            _ => Err(EngineError::GqlUnsupported {
                feature: "function".to_string(),
                message: format!("function '{}' is not supported", name.name),
                span: name.span.clone(),
            }),
        }
    }

    fn reject_statically_element_property_value(&self, expr: &Expr) -> Result<(), EngineError> {
        match &expr.kind {
            ExprKind::Variable(name) => {
                if let Some(binding) = self.aliases.get(name) {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!(
                            "{:?} alias '{}' cannot be used as a property value",
                            binding.kind, name
                        ),
                        expr.span.clone(),
                    ));
                }
                Ok(())
            }
            ExprKind::FunctionCall { name, args: _ } => {
                let function = name.name.to_ascii_lowercase();
                if matches!(
                    function.as_str(),
                    "start_node" | "end_node" | "nodes" | "relationships"
                ) {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        format!(
                            "function '{}' returns graph elements and cannot be used as a property value",
                            name.name
                        ),
                        name.span.clone(),
                    ));
                }
                Ok(())
            }
            ExprKind::List(items) => {
                for item in items {
                    self.reject_statically_element_property_value(item)?;
                }
                Ok(())
            }
            ExprKind::Map(map) => {
                for entry in &map.entries {
                    self.reject_statically_element_property_value(&entry.value)?;
                }
                Ok(())
            }
            ExprKind::PropertyAccess { .. }
            | ExprKind::Literal(_)
            | ExprKind::Parameter(_)
            | ExprKind::Unary { .. }
            | ExprKind::Binary { .. }
            | ExprKind::IsNull { .. } => Ok(()),
        }
    }

    fn insert_created_alias(
        &mut self,
        ident: &Ident,
        kind: GqlAliasKind,
    ) -> Result<(), EngineError> {
        if is_reserved_user_alias(&ident.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DuplicateAlias,
                format!("'{}' is reserved for internal GQL projection", ident.name),
                ident.span.clone(),
            ));
        }
        if self.aliases.contains_key(&ident.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DuplicateAlias,
                format!("created alias '{}' is already bound", ident.name),
                ident.span.clone(),
            ));
        }
        self.aliases.insert(
            ident.name.clone(),
            GqlMutationAliasBinding {
                name: ident.name.clone(),
                kind,
                origin: GqlAliasOrigin::Created,
                nullable: false,
                span: ident.span.clone(),
            },
        );
        self.user_order.push(ident.name.clone());
        Ok(())
    }

    fn record_incident_edge(&mut self, from_alias: &str, to_alias: &str, edge_alias: &str) {
        self.incident_edges
            .entry(from_alias.to_string())
            .or_default()
            .insert(edge_alias.to_string());
        self.incident_edges
            .entry(to_alias.to_string())
            .or_default()
            .insert(edge_alias.to_string());
    }

    fn mark_deleted_alias(&mut self, alias: &str, kind: GqlAliasKind) {
        self.deleted_aliases.insert(alias.to_string());
        if kind == GqlAliasKind::Node {
            if let Some(edges) = self.incident_edges.get(alias) {
                self.deleted_aliases.extend(edges.iter().cloned());
            }
        }
    }

    fn next_internal_created_alias(&mut self, kind: &str) -> String {
        loop {
            let alias = format!("__gql_create_{kind}_{}", self.created_internal_counter);
            self.created_internal_counter += 1;
            if !self.aliases.contains_key(&alias) {
                return alias;
            }
        }
    }
}

pub(crate) fn gql_semantic_error(
    code: GqlSemanticErrorCode,
    message: String,
    span: SourceSpan,
) -> EngineError {
    EngineError::GqlSemantic {
        code,
        message,
        span,
    }
}

pub(crate) fn is_reserved_user_alias(name: &str) -> bool {
    name == DIRECT_NODE_ALIAS || name == DIRECT_EDGE_ALIAS || name.starts_with("__gql_")
}

fn semantic_binding_order(clauses: &[GqlBoundMatchClause]) -> Vec<String> {
    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    for clause in clauses {
        for pattern in &clause.patterns {
            if let Some(alias) = pattern.user_path_alias.as_ref() {
                push_user_alias_once(alias, &mut seen, &mut order);
            }
            if let Some(alias) = pattern
                .nodes
                .first()
                .and_then(|node| node.user_alias.as_ref())
            {
                push_user_alias_once(alias, &mut seen, &mut order);
            }
            for (index, edge) in pattern.edges.iter().enumerate() {
                if let Some(alias) = edge.user_alias.as_ref() {
                    push_user_alias_once(alias, &mut seen, &mut order);
                }
                if let Some(alias) = pattern
                    .nodes
                    .get(index + 1)
                    .and_then(|node| node.user_alias.as_ref())
                {
                    push_user_alias_once(alias, &mut seen, &mut order);
                }
            }
        }
    }
    order
}

fn push_user_alias_once(alias: &str, seen: &mut BTreeSet<String>, order: &mut Vec<String>) {
    if seen.insert(alias.to_string()) {
        order.push(alias.to_string());
    }
}

fn is_supported_path_property(property: &str) -> bool {
    matches!(property, "node_ids" | "edge_ids" | "length")
}

pub(crate) fn variable_name(expr: &Expr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Variable(name) => Some(name.as_str()),
        _ => None,
    }
}

fn synthetic_read_prefix_query(read_prefix: &[MatchClause], span: &SourceSpan) -> GqlQuery {
    GqlQuery {
        match_clauses: read_prefix.to_vec(),
        return_clause: ReturnClause {
            body: ReturnBody::All(span.clone()),
            span: span.clone(),
        },
        order_by: Vec::new(),
        skip: None,
        limit: None,
        span: span.clone(),
    }
}

fn read_prefix_mutation_aliases(
    plan: &GqlSemanticPlan,
) -> (BTreeMap<String, GqlMutationAliasBinding>, Vec<String>) {
    let mut aliases = BTreeMap::new();
    let mut user_order = Vec::new();
    for clause in &plan.clauses {
        for (alias, nullable) in clause_user_aliases(clause) {
            if aliases.contains_key(&alias) {
                continue;
            }
            let Some(binding) = plan.aliases.get(&alias) else {
                continue;
            };
            aliases.insert(
                alias.clone(),
                GqlMutationAliasBinding {
                    name: alias.clone(),
                    kind: binding.kind,
                    origin: GqlAliasOrigin::ReadPrefix,
                    nullable,
                    span: binding.span.clone(),
                },
            );
            user_order.push(alias);
        }
    }
    (aliases, user_order)
}

fn read_prefix_incident_edges(plan: &GqlSemanticPlan) -> BTreeMap<String, BTreeSet<String>> {
    let mut incident_edges = BTreeMap::new();
    for clause in &plan.clauses {
        for pattern in &clause.patterns {
            for edge in &pattern.edges {
                let Some(edge_alias) = edge.user_alias.as_ref() else {
                    continue;
                };
                incident_edges
                    .entry(edge.from_alias.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(edge_alias.clone());
                incident_edges
                    .entry(edge.to_alias.clone())
                    .or_insert_with(BTreeSet::new)
                    .insert(edge_alias.clone());
            }
        }
    }
    incident_edges
}

fn clause_user_aliases(clause: &GqlBoundMatchClause) -> Vec<(String, bool)> {
    let mut aliases = Vec::new();
    let nullable = clause.optional;
    for pattern in &clause.patterns {
        if let Some(alias) = pattern.user_path_alias.as_ref() {
            aliases.push((alias.clone(), nullable));
        }
        if let Some(alias) = pattern
            .nodes
            .first()
            .and_then(|node| node.user_alias.as_ref())
        {
            aliases.push((alias.clone(), nullable));
        }
        for (index, edge) in pattern.edges.iter().enumerate() {
            if let Some(alias) = edge.user_alias.as_ref() {
                aliases.push((alias.clone(), nullable));
            }
            if let Some(alias) = pattern
                .nodes
                .get(index + 1)
                .and_then(|node| node.user_alias.as_ref())
            {
                aliases.push((alias.clone(), nullable));
            }
        }
    }
    aliases
}

fn is_reserved_create_node_property(property: &str) -> bool {
    matches!(
        property,
        "id" | "labels" | "created_at" | "updated_at" | "dense_vector" | "sparse_vector"
    )
}

fn is_reserved_create_edge_property(property: &str) -> bool {
    matches!(
        property,
        "id" | "from" | "to" | "label" | "type" | "created_at" | "updated_at"
    )
}

fn reject_reserved_set_property(kind: GqlAliasKind, property: &Ident) -> Result<(), EngineError> {
    let reserved = match kind {
        GqlAliasKind::Node => matches!(
            property.name.as_str(),
            "id" | "labels"
                | "key"
                | "created_at"
                | "updated_at"
                | "dense_vector"
                | "sparse_vector"
        ),
        GqlAliasKind::Edge => matches!(
            property.name.as_str(),
            "id" | "from" | "to" | "label" | "type" | "created_at" | "updated_at"
        ),
        GqlAliasKind::Path => true,
    };
    if reserved {
        return Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidPropertyAccess,
            format!(
                "SET target '{}.{}' is reserved metadata",
                kind_name(kind),
                property.name
            ),
            property.span.clone(),
        ));
    }
    Ok(())
}

fn reject_reserved_remove_property(
    kind: GqlAliasKind,
    property: &Ident,
) -> Result<(), EngineError> {
    let reserved = match kind {
        GqlAliasKind::Node => matches!(
            property.name.as_str(),
            "id" | "labels"
                | "key"
                | "created_at"
                | "updated_at"
                | "dense_vector"
                | "sparse_vector"
        ),
        GqlAliasKind::Edge => matches!(
            property.name.as_str(),
            "id" | "from"
                | "to"
                | "label"
                | "type"
                | "created_at"
                | "updated_at"
                | "weight"
                | "valid_from"
                | "valid_to"
        ),
        GqlAliasKind::Path => true,
    };
    if reserved {
        return Err(gql_semantic_error(
            GqlSemanticErrorCode::InvalidPropertyAccess,
            format!(
                "REMOVE target '{}.{}' is reserved metadata",
                kind_name(kind),
                property.name
            ),
            property.span.clone(),
        ));
    }
    Ok(())
}

fn kind_name(kind: GqlAliasKind) -> &'static str {
    match kind {
        GqlAliasKind::Node => "node",
        GqlAliasKind::Edge => "edge",
        GqlAliasKind::Path => "path",
    }
}

pub(crate) fn expression_output_name(expr: &Expr) -> String {
    match &expr.kind {
        ExprKind::Variable(name) => name.clone(),
        ExprKind::PropertyAccess { object, property } => {
            format!("{}.{}", expression_output_name(object), property.name)
        }
        ExprKind::FunctionCall { name, args } => {
            let args = args
                .iter()
                .map(expression_output_name)
                .collect::<Vec<_>>()
                .join(", ");
            format!("{}({})", name.name, args)
        }
        ExprKind::Parameter(name) => format!("${name}"),
        ExprKind::Literal(Literal::Null) => "null".to_string(),
        ExprKind::Literal(Literal::Bool(value)) => value.to_string(),
        ExprKind::Literal(Literal::Int(value)) => value.to_string(),
        ExprKind::Literal(Literal::Float(value)) => value.to_string(),
        ExprKind::Literal(Literal::String(value)) => value.clone(),
        ExprKind::List(_) => "list".to_string(),
        ExprKind::Map(_) => "map".to_string(),
        ExprKind::Unary { .. } | ExprKind::Binary { .. } | ExprKind::IsNull { .. } => {
            "expr".to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gql::parser::{parse_query, parse_statement, GqlParseOptions};

    fn bind(source: &str) -> Result<GqlSemanticPlan, EngineError> {
        bind_query(
            parse_query(source, &GqlParseOptions::default()).unwrap(),
            &GqlParams::new(),
        )
    }

    fn bind_mut(source: &str) -> Result<GqlMutationSemanticPlan, EngineError> {
        let statement = parse_statement(source, &GqlParseOptions::default()).unwrap();
        let GqlStatementBody::Mutation(mutation) = statement.body else {
            panic!("expected mutation statement");
        };
        bind_mutation(mutation, &GqlParams::new())
    }

    fn expect_mut_semantic_code(err: EngineError, code: GqlSemanticErrorCode) {
        match err {
            EngineError::GqlSemantic { code: actual, .. } => assert_eq!(actual, code),
            other => panic!("expected semantic error {code:?}, got {other:?}"),
        }
    }

    #[test]
    fn binds_ordered_optional_clauses_and_path_aliases() {
        let plan = bind("MATCH (a) OPTIONAL MATCH p = (a)-[:KNOWS*0..2]->(b) RETURN *").unwrap();
        assert_eq!(plan.clauses.len(), 2);
        assert!(!plan.clauses[0].optional);
        assert!(plan.clauses[1].optional);
        assert_eq!(plan.aliases.get("a").unwrap().kind, GqlAliasKind::Node);
        assert_eq!(plan.aliases.get("p").unwrap().kind, GqlAliasKind::Path);
        assert_eq!(plan.aliases.get("b").unwrap().kind, GqlAliasKind::Node);
        let GqlReturnPlan::Star {
            expanded_aliases, ..
        } = plan.returns
        else {
            panic!("expected RETURN *");
        };
        assert_eq!(expanded_aliases, vec!["a", "p", "b"]);
    }

    #[test]
    fn one_hop_vlp_may_bind_edge_and_path_aliases() {
        let plan = bind("MATCH p = (a)-[r:KNOWS*1..1]->(b) RETURN p, r").unwrap();
        assert_eq!(plan.aliases.get("p").unwrap().kind, GqlAliasKind::Path);
        assert_eq!(plan.aliases.get("r").unwrap().kind, GqlAliasKind::Edge);
    }

    #[test]
    fn rejects_multi_hop_relationship_aliases_and_wrong_path_function_kinds() {
        let rel_alias = bind("MATCH p = (a)-[r:KNOWS*1..2]->(b) RETURN p")
            .expect_err("multi-hop relationship alias should fail");
        assert!(matches!(
            rel_alias,
            EngineError::GqlUnsupported { ref feature, .. }
                if feature == "multi-hop relationship-list aliases"
        ));

        let wrong_kind = bind("MATCH p = (a)-[:KNOWS*1..2]->(b) RETURN length(a)")
            .expect_err("path function on node should fail");
        assert!(matches!(
            wrong_kind,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidReturnExpression,
                ..
            }
        ));

        let unknown_path_property = bind("MATCH p = (a)-[:KNOWS*1..2]->(b) RETURN p.foo")
            .expect_err("unknown path property should fail");
        assert!(matches!(
            unknown_path_property,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidPropertyAccess,
                ..
            }
        ));
    }

    #[test]
    fn mutation_binds_read_prefix_nullable_and_created_aliases() {
        let plan = bind_mut(
            "MATCH (a:Person {key: 'a'}) OPTIONAL MATCH p = (a)-[r:KNOWS*1..1]->(b) CREATE (c:Person {key: 'c'})-[e:LINK]->(a) RETURN a, p, b, c, e",
        )
        .unwrap();
        let a = plan.aliases.get("a").unwrap();
        assert_eq!(a.origin, GqlAliasOrigin::ReadPrefix);
        assert!(!a.nullable);
        let p = plan.aliases.get("p").unwrap();
        assert_eq!(p.kind, GqlAliasKind::Path);
        assert_eq!(p.origin, GqlAliasOrigin::ReadPrefix);
        assert!(p.nullable);
        let b = plan.aliases.get("b").unwrap();
        assert_eq!(b.kind, GqlAliasKind::Node);
        assert!(b.nullable);
        let c = plan.aliases.get("c").unwrap();
        assert_eq!(c.kind, GqlAliasKind::Node);
        assert_eq!(c.origin, GqlAliasOrigin::Created);
        let e = plan.aliases.get("e").unwrap();
        assert_eq!(e.kind, GqlAliasKind::Edge);
        assert_eq!(e.origin, GqlAliasOrigin::Created);
    }

    #[test]
    fn mutation_rejects_create_alias_collisions_and_invalid_create_shapes() {
        let duplicate = bind_mut("CREATE (n:Person {key: 'a'}), (n:Person {key: 'b'})")
            .expect_err("duplicate created alias should fail");
        expect_mut_semantic_code(duplicate, GqlSemanticErrorCode::DuplicateAlias);

        let bound_endpoint = bind_mut("MATCH (n:Person {key: 'a'}) CREATE (n:Other {key: 'b'})")
            .expect_err("bound endpoint with labels should fail");
        expect_mut_semantic_code(
            bound_endpoint,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let bound_endpoint_with_props =
            bind_mut("MATCH (n:Person {key: 'a'}) CREATE (n {key: 'b'})")
                .expect_err("bound endpoint with properties should fail");
        expect_mut_semantic_code(
            bound_endpoint_with_props,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let standalone_existing = bind_mut("MATCH (n:Person {key: 'a'}) CREATE (n)")
            .expect_err("standalone existing CREATE endpoint should fail");
        expect_mut_semantic_code(
            standalone_existing,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let cross_pattern_created =
            bind_mut("CREATE (a:Person {key: 'a'}), (a)-[:R]->(b:Person {key: 'b'})")
                .expect_err("created alias reuse across CREATE patterns should fail");
        expect_mut_semantic_code(cross_pattern_created, GqlSemanticErrorCode::DuplicateAlias);

        let no_label =
            bind_mut("CREATE (n {key: 'a'})").expect_err("new node without label should fail");
        expect_mut_semantic_code(no_label, GqlSemanticErrorCode::InvalidReturnExpression);

        let no_key = bind_mut("CREATE (n:Person {name: 'Ada'})")
            .expect_err("new node without key should fail");
        expect_mut_semantic_code(no_key, GqlSemanticErrorCode::InvalidReturnExpression);

        let path_create = bind_mut("CREATE p = (n:Person {key: 'a'})")
            .expect_err("CREATE path assignment should fail");
        assert!(matches!(
            path_create,
            EngineError::GqlUnsupported { feature, .. } if feature == "CREATE path assignment"
        ));

        let reserved_node = bind_mut("CREATE (n:Person {key: 'a', id: 1})")
            .expect_err("reserved CREATE node metadata should fail");
        expect_mut_semantic_code(reserved_node, GqlSemanticErrorCode::InvalidPropertyAccess);

        let reserved_edge =
            bind_mut("CREATE (a:Person {key: 'a'})-[r:R {from: 1}]->(b:Person {key: 'b'})")
                .expect_err("reserved CREATE edge metadata should fail");
        expect_mut_semantic_code(reserved_edge, GqlSemanticErrorCode::InvalidPropertyAccess);
    }

    #[test]
    fn mutation_validates_relationship_create_shape() {
        for source in [
            "CREATE (a:Person {key: 'a'})-[r]-(b:Person {key: 'b'})",
            "CREATE (a:Person {key: 'a'})-[r*1..1]->(b:Person {key: 'b'})",
            "CREATE (a:Person {key: 'a'})-[r:A|B]->(b:Person {key: 'b'})",
            "CREATE (a:Person {key: 'a'})-[r]->(b:Person {key: 'b'})",
        ] {
            assert!(
                bind_mut(source).is_err(),
                "invalid relationship CREATE should fail: {source}"
            );
        }
        let ok = bind_mut("CREATE (a:Person {key: 'a'})-[r:KNOWS]->(b:Person {key: 'b'})").unwrap();
        assert_eq!(ok.aliases.get("r").unwrap().kind, GqlAliasKind::Edge);

        let duplicate_edge = bind_mut(
            "CREATE (a:Person {key: 'a'})-[r:R]->(b:Person {key: 'b'})-[r:S]->(c:Person {key: 'c'})",
        )
        .expect_err("duplicate relationship CREATE alias should fail");
        expect_mut_semantic_code(duplicate_edge, GqlSemanticErrorCode::DuplicateAlias);
    }

    #[test]
    fn mutation_rejects_created_alias_rhs_sources_but_allows_targets_and_return() {
        let ok = bind_mut("CREATE (n:Person {key: 'a'}) SET n.name = 'Ada' RETURN n").unwrap();
        assert!(ok.aliases.contains_key("n"));

        let rhs = bind_mut("CREATE (n:Person {key: 'a'}) SET n.name = n.key")
            .expect_err("created alias RHS should fail");
        expect_mut_semantic_code(rhs, GqlSemanticErrorCode::InvalidReturnExpression);

        let element_rhs = bind_mut("MATCH (n)-[r:KNOWS]->(m) SET n.friend = m")
            .expect_err("element-valued SET RHS should fail");
        expect_mut_semantic_code(element_rhs, GqlSemanticErrorCode::InvalidReturnExpression);

        let element_list_rhs = bind_mut("MATCH p = (n)-[r:KNOWS]->(m) SET n.friends = nodes(p)")
            .expect_err("element-list SET RHS should fail");
        expect_mut_semantic_code(
            element_list_rhs,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );
    }

    #[test]
    fn mutation_rejects_path_targets_and_validates_delete_targets() {
        let path_set = bind_mut("MATCH p = (a)-[r:KNOWS]->(b) SET p.name = 'x'")
            .expect_err("path SET target should fail");
        expect_mut_semantic_code(path_set, GqlSemanticErrorCode::InvalidPropertyAccess);

        let path_remove = bind_mut("MATCH p = (a)-[r:KNOWS]->(b) REMOVE p.name")
            .expect_err("path REMOVE target should fail");
        expect_mut_semantic_code(path_remove, GqlSemanticErrorCode::InvalidPropertyAccess);

        bind_mut("MATCH p = (a)-[r:KNOWS]->(b) SET a.name = 'x' RETURN p").unwrap();

        bind_mut("MATCH (a)-[r:KNOWS]->(b) DELETE r").unwrap();
        bind_mut("MATCH (a)-[r:KNOWS]->(b) DELETE r DELETE r").unwrap();
        bind_mut("MATCH (n:Person {key: 'a'}) DETACH DELETE n").unwrap();

        let set_deleted = bind_mut("MATCH (a)-[r:KNOWS]->(b) DELETE r SET r.weight = 1")
            .expect_err("SET after DELETE of same alias should fail");
        expect_mut_semantic_code(set_deleted, GqlSemanticErrorCode::InvalidReturnExpression);

        let remove_deleted = bind_mut("MATCH (n:Person {key: 'a'}) DETACH DELETE n REMOVE n.name")
            .expect_err("REMOVE after DETACH DELETE of same alias should fail");
        expect_mut_semantic_code(
            remove_deleted,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let detach_then_set_incident =
            bind_mut("MATCH (a)-[r:KNOWS]->(b) DETACH DELETE a SET r.weight = 1")
                .expect_err("SET after DETACH DELETE of incident edge should fail");
        expect_mut_semantic_code(
            detach_then_set_incident,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let detach_then_remove_incident =
            bind_mut("MATCH (a)-[r:KNOWS]->(b) DETACH DELETE a REMOVE r.weight")
                .expect_err("REMOVE after DETACH DELETE of incident edge should fail");
        expect_mut_semantic_code(
            detach_then_remove_incident,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        bind_mut("MATCH (a)-[r:KNOWS]->(b) DETACH DELETE a DELETE r").unwrap();

        let detach_then_create_from_deleted = bind_mut(
            "MATCH (a)-[r:KNOWS]->(b) DETACH DELETE a CREATE (a)-[:NEXT]->(c:Person {key: 'c'})",
        )
        .expect_err("CREATE endpoint deleted earlier should fail");
        expect_mut_semantic_code(
            detach_then_create_from_deleted,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let created_incident_deleted = bind_mut(
            "CREATE (a:Person {key: 'a'})-[r:R]->(b:Person {key: 'b'}) DETACH DELETE a SET r.weight = 1",
        )
        .expect_err("SET created edge after DETACH DELETE of endpoint should fail");
        expect_mut_semantic_code(
            created_incident_deleted,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let delete_node = bind_mut("MATCH (n:Person {key: 'a'}) DELETE n")
            .expect_err("DELETE node without DETACH should fail");
        expect_mut_semantic_code(delete_node, GqlSemanticErrorCode::InvalidReturnExpression);

        let detach_edge = bind_mut("MATCH (a)-[r:KNOWS]->(b) DETACH DELETE r")
            .expect_err("DETACH DELETE edge should fail");
        expect_mut_semantic_code(detach_edge, GqlSemanticErrorCode::InvalidReturnExpression);

        let return_after_delete = bind_mut("MATCH (a)-[r:KNOWS]->(b) DELETE r RETURN r")
            .expect_err("RETURN after DELETE should fail");
        expect_mut_semantic_code(
            return_after_delete,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );
    }

    #[test]
    fn mutation_validates_set_remove_reserved_fields_and_optional_targets() {
        for source in [
            "MATCH (n:Person {key: 'a'}) SET n.id = 1",
            "MATCH (n:Person {key: 'a'}) SET n.key = 'b'",
            "MATCH (n:Person {key: 'a'}) SET n.dense_vector = []",
            "MATCH (a)-[r:KNOWS]->(b) SET r.from = 1",
            "MATCH (a)-[r:KNOWS]->(b) SET r.type = 'X'",
            "MATCH (n:Person {key: 'a'}) REMOVE n.id",
            "MATCH (a)-[r:KNOWS]->(b) REMOVE r.weight",
            "MATCH (a)-[r:KNOWS]->(b) REMOVE r.valid_from",
        ] {
            let err = bind_mut(source).expect_err("reserved field should fail");
            expect_mut_semantic_code(err, GqlSemanticErrorCode::InvalidPropertyAccess);
        }

        bind_mut("MATCH (n:Person {key: 'a'}) SET n.weight = 1.0").unwrap();
        bind_mut("MATCH (a)-[r:KNOWS]->(b) SET r.weight = 1.0").unwrap();
        bind_mut("MATCH (a)-[r:KNOWS]->(b) SET r.valid_from = 1").unwrap();
        bind_mut("MATCH (a)-[r:KNOWS]->(b) SET r.valid_to = 2").unwrap();
        bind_mut(
            "MATCH (a:Person {key: 'a'}) OPTIONAL MATCH (a)-[r:KNOWS]->(b) SET b.name = 'optional'",
        )
        .unwrap();
    }
}
