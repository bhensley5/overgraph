#![allow(dead_code)]

use crate::error::EngineError;
use crate::gql::ast::*;
use crate::row_projection::{DIRECT_EDGE_ALIAS, DIRECT_NODE_ALIAS};
use crate::types::{GqlParams, GqlSemanticErrorCode, SourceSpan};
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

fn expression_output_name(expr: &Expr) -> String {
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
    use crate::gql::parser::{parse_query, GqlParseOptions};

    fn bind(source: &str) -> Result<GqlSemanticPlan, EngineError> {
        bind_query(
            parse_query(source, &GqlParseOptions::default()).unwrap(),
            &GqlParams::new(),
        )
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
}
