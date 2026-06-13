#![allow(dead_code)]

use crate::error::EngineError;
use crate::gql::ast::*;
use crate::gql::metadata::{GqlElementMapMetadataKey, GqlEndpointFunction, GqlMetadataFunction};
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
    Scalar,
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
pub(crate) struct GqlBoundShortestPathClause {
    pub(crate) optional: bool,
    pub(crate) output_path_alias: String,
    pub(crate) mode: GqlShortestPathMode,
    pub(crate) from_alias: String,
    pub(crate) to_alias: String,
    pub(crate) direction: RelationshipDirection,
    pub(crate) rel_types: Vec<Ident>,
    pub(crate) min_hops: u8,
    pub(crate) max_hops: u8,
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
    pub(crate) pipeline: GqlBoundReadPipeline,
    pub(crate) returns: GqlReturnPlan,
    pub(crate) parameters: Vec<String>,
    pub(crate) parameter_spans: BTreeMap<String, SourceSpan>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundReadPipeline {
    pub(crate) clauses: Vec<GqlBoundPipelineClause>,
    pub(crate) union_branches: Vec<GqlBoundUnionBranch>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundUnionBranch {
    pub(crate) modifier: GqlUnionModifier,
    pub(crate) clauses: Vec<GqlBoundPipelineClause>,
    pub(crate) returns: GqlReturnPlan,
    pub(crate) span: SourceSpan,
    pub(crate) union_span: SourceSpan,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlBoundPipelineClause {
    Match(Vec<GqlBoundMatchClause>),
    ShortestPath(GqlBoundShortestPathClause),
    Call(GqlBoundCallSubquery),
    Projection(GqlBoundProjectionClause),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundCallSubquery {
    pub(crate) pipeline: GqlBoundReadPipeline,
    pub(crate) import_aliases: Vec<String>,
    pub(crate) output_aliases: Vec<GqlProjectionAlias>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundProjectionClause {
    pub(crate) kind: GqlProjectionKind,
    pub(crate) distinct: bool,
    pub(crate) distinct_span: Option<SourceSpan>,
    pub(crate) returns: GqlReturnPlan,
    pub(crate) output_aliases: Vec<GqlProjectionAlias>,
    pub(crate) where_clause: Option<Expr>,
    pub(crate) order_by: Vec<OrderItem>,
    pub(crate) skip: Option<Expr>,
    pub(crate) limit: Option<Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlProjectionAlias {
    pub(crate) name: String,
    pub(crate) kind: GqlAliasKind,
    pub(crate) span: SourceSpan,
}

struct BoundProjectionItem {
    return_binding: GqlReturnItemBinding,
    output_alias: GqlProjectionAlias,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlAliasOrigin {
    ReadPrefix,
    Created,
    Merged,
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

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlBoundMutationClause {
    Create(GqlBoundCreateClause),
    Merge(GqlBoundMergeClause),
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
pub(crate) struct GqlBoundMergeClause {
    pub(crate) pattern: GqlBoundMergePattern,
    pub(crate) on_create: GqlBoundSetClause,
    pub(crate) on_match: GqlBoundSetClause,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlBoundMergePattern {
    Node(GqlBoundMergeNode),
    Relationship(GqlBoundMergeRelationship),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundMergeNode {
    pub(crate) alias: String,
    pub(crate) label: Ident,
    pub(crate) key: Expr,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlBoundMergeRelationship {
    pub(crate) alias: String,
    pub(crate) from_alias: String,
    pub(crate) to_alias: String,
    pub(crate) rel_type: Ident,
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
    Metadata {
        alias: String,
        target_kind: GqlAliasKind,
        field: GqlMetadataFunction,
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

fn terminal_return_plan(clauses: &[GqlBoundPipelineClause]) -> Result<&GqlReturnPlan, EngineError> {
    clauses
        .iter()
        .rev()
        .find_map(|clause| match clause {
            GqlBoundPipelineClause::Projection(projection)
                if projection.kind == GqlProjectionKind::Return =>
            {
                Some(&projection.returns)
            }
            _ => None,
        })
        .ok_or_else(|| {
            EngineError::InvalidOperation("GQL read pipeline must end in RETURN".to_string())
        })
}

fn terminal_return_columns(clauses: &[GqlBoundPipelineClause]) -> Result<Vec<String>, EngineError> {
    terminal_return_plan(clauses).map(return_plan_columns)
}

fn return_plan_columns(plan: &GqlReturnPlan) -> Vec<String> {
    match plan {
        GqlReturnPlan::Star {
            expanded_aliases, ..
        } => expanded_aliases.clone(),
        GqlReturnPlan::Items(items) => items.iter().map(|item| item.output_name.clone()).collect(),
    }
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

    let pipeline = binder.bind_read_pipeline(&query.pipeline)?;
    let clauses = if query.is_legacy_single_block() {
        pipeline
            .clauses
            .iter()
            .flat_map(|clause| match clause {
                GqlBoundPipelineClause::Match(clauses) => clauses.clone(),
                GqlBoundPipelineClause::ShortestPath(_) => Vec::new(),
                GqlBoundPipelineClause::Call(_) => Vec::new(),
                GqlBoundPipelineClause::Projection(_) => Vec::new(),
            })
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let returns = terminal_return_plan(&pipeline.clauses)?.clone();

    let parameters = binder.parameters.into_iter().collect();
    Ok(GqlSemanticPlan {
        query,
        aliases: binder.aliases,
        clauses,
        pipeline,
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

    let read_prefix = if mutation_statement_has_read_prefix(&statement) {
        Some(bind_query(synthetic_read_prefix_query(&statement), params)?)
    } else {
        None
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
    fn bind_read_pipeline(
        &mut self,
        pipeline: &GqlReadPipeline,
    ) -> Result<GqlBoundReadPipeline, EngineError> {
        let base_aliases = self.aliases.clone();
        let clauses = self.bind_pipeline_clauses(&pipeline.clauses)?;
        let first_columns = terminal_return_columns(&clauses)?;
        let mut union_branches = Vec::with_capacity(pipeline.union_branches.len());
        for branch in &pipeline.union_branches {
            let mut branch_binder = SemanticBinder {
                aliases: base_aliases.clone(),
                anonymous_node_counter: 0,
                parameters: BTreeSet::new(),
                parameter_spans: BTreeMap::new(),
                params: self.params,
            };
            let branch_clauses = branch_binder.bind_pipeline_clauses(&branch.clauses)?;
            let branch_returns = terminal_return_plan(&branch_clauses)?.clone();
            let branch_columns = return_plan_columns(&branch_returns);
            if branch_columns.len() != first_columns.len() {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!(
                        "UNION branch returns {} column(s), expected {}",
                        branch_columns.len(),
                        first_columns.len()
                    ),
                    branch.span.clone(),
                ));
            }
            if branch_columns != first_columns {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!(
                        "UNION branch columns {:?} do not match {:?}",
                        branch_columns, first_columns
                    ),
                    branch.span.clone(),
                ));
            }
            self.parameters.extend(branch_binder.parameters);
            self.parameter_spans.extend(branch_binder.parameter_spans);
            union_branches.push(GqlBoundUnionBranch {
                modifier: branch.modifier,
                clauses: branch_clauses,
                returns: branch_returns,
                span: branch.span.clone(),
                union_span: branch.union_span.clone(),
            });
        }
        Ok(GqlBoundReadPipeline {
            clauses,
            union_branches,
        })
    }

    fn bind_pipeline_clauses(
        &mut self,
        clauses: &[GqlPipelineClause],
    ) -> Result<Vec<GqlBoundPipelineClause>, EngineError> {
        let mut bound = Vec::with_capacity(clauses.len());
        for clause in clauses {
            match clause {
                GqlPipelineClause::Match(clauses) => {
                    let previous_order = self.aliases.user_order.clone();
                    let clauses = self.bind_match_clauses(clauses)?;
                    self.reconcile_match_binding_order(&previous_order, &clauses);
                    bound.push(GqlBoundPipelineClause::Match(clauses));
                }
                GqlPipelineClause::ShortestPath(shortest) => {
                    let shortest = self.bind_shortest_path_clause(shortest)?;
                    bound.push(GqlBoundPipelineClause::ShortestPath(shortest));
                }
                GqlPipelineClause::Call(call) => {
                    let call = self.bind_call_subquery(call)?;
                    bound.push(GqlBoundPipelineClause::Call(call));
                }
                GqlPipelineClause::Projection(projection) => {
                    let projection = self.bind_projection_clause(projection)?;
                    bound.push(GqlBoundPipelineClause::Projection(projection));
                }
            }
        }
        Ok(bound)
    }

    fn reconcile_match_binding_order(
        &mut self,
        previous_order: &[String],
        clauses: &[GqlBoundMatchClause],
    ) {
        let mut seen = previous_order.iter().cloned().collect::<BTreeSet<_>>();
        let mut order = previous_order.to_vec();
        for alias in semantic_binding_order(clauses) {
            let Some(binding) = self.aliases.get(&alias) else {
                continue;
            };
            if binding.user_visible && seen.insert(alias.clone()) {
                order.push(alias);
            }
        }
        self.aliases.user_order = order;
    }

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
            self.validate_predicate_expr(where_clause, &BTreeSet::new())?;
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

    fn bind_shortest_path_clause(
        &mut self,
        clause: &GqlShortestPathClause,
    ) -> Result<GqlBoundShortestPathClause, EngineError> {
        let from_alias = self.bind_shortest_path_endpoint(&clause.pattern.start)?;
        let chain = clause
            .pattern
            .chains
            .first()
            .expect("parser validated shortest-path relationship count");
        let to_alias = self.bind_shortest_path_endpoint(&chain.node)?;
        let quantifier = chain
            .relationship
            .quantifier
            .as_ref()
            .expect("parser validated shortest-path hop bounds");
        self.bind_user_alias(&clause.output_path_alias, GqlAliasKind::Path)?;
        Ok(GqlBoundShortestPathClause {
            optional: clause.optional,
            output_path_alias: clause.output_path_alias.name.clone(),
            mode: clause.mode,
            from_alias,
            to_alias,
            direction: chain.relationship.direction,
            rel_types: chain.relationship.rel_types.clone(),
            min_hops: quantifier.min_hops,
            max_hops: quantifier.max_hops,
            span: clause.span.clone(),
        })
    }

    fn bind_shortest_path_endpoint(&self, pattern: &NodePattern) -> Result<String, EngineError> {
        if !pattern.labels.is_empty() || pattern.properties.is_some() {
            return Err(EngineError::GqlUnsupported {
                feature: "shortest-path endpoint lookup".to_string(),
                message:
                    "shortest-path endpoints must be bound node aliases; bind label/key endpoints in an earlier MATCH"
                        .to_string(),
                span: pattern.span.clone(),
            });
        }
        let Some(variable) = pattern.variable.as_ref() else {
            return Err(EngineError::GqlUnsupported {
                feature: "shortest-path endpoint scan".to_string(),
                message:
                    "shortest-path endpoints must be bound node aliases; broad endpoint scans are not supported"
                        .to_string(),
                span: pattern.span.clone(),
            });
        };
        let Some(binding) = self.aliases.get(&variable.name) else {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::UnknownVariable,
                format!(
                    "shortest-path endpoint '{}' must be bound before shortest-path MATCH",
                    variable.name
                ),
                variable.span.clone(),
            ));
        };
        if binding.kind != GqlAliasKind::Node {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!(
                    "shortest-path endpoint '{}' must be a node alias",
                    variable.name
                ),
                variable.span.clone(),
            ));
        }
        Ok(variable.name.clone())
    }

    fn bind_call_subquery(
        &mut self,
        call: &GqlCallSubquery,
    ) -> Result<GqlBoundCallSubquery, EngineError> {
        let outer_aliases = self.aliases.clone();
        let (pipeline, import_aliases, output_aliases, parameters, parameter_spans) =
            bind_subquery_pipeline_parts(&call.pipeline, &outer_aliases, self.params)?;
        for output in &output_aliases {
            if self.aliases.contains(&output.name) {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::DuplicateAlias,
                    format!(
                        "CALL subquery output '{}' collides with an outer alias",
                        output.name
                    ),
                    output.span.clone(),
                ));
            }
        }
        for output in &output_aliases {
            self.bind_user_alias(
                &Ident {
                    name: output.name.clone(),
                    span: output.span.clone(),
                },
                output.kind,
            )?;
        }
        self.parameters.extend(parameters);
        self.parameter_spans.extend(parameter_spans);
        Ok(GqlBoundCallSubquery {
            pipeline,
            import_aliases,
            output_aliases,
            span: call.span.clone(),
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
        self.bind_return_body(&clause.body)
    }

    fn bind_projection_clause(
        &mut self,
        clause: &GqlProjectionClause,
    ) -> Result<GqlBoundProjectionClause, EngineError> {
        let previous_aliases = if clause.kind == GqlProjectionKind::Return {
            Some(self.aliases.clone())
        } else {
            None
        };
        let (returns, output_aliases, next_scope) =
            self.bind_projection_body(clause.kind, &clause.body)?;
        let star_projection = matches!(
            clause.body,
            ReturnBody::All(_) | ReturnBody::AllAndItems { .. }
        );
        let order_by_contains_aggregate = clause
            .order_by
            .iter()
            .any(|item| expr_contains_aggregate(&item.expr));
        if star_projection
            && (projection_body_contains_aggregate(&clause.body) || order_by_contains_aggregate)
        {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "* projections cannot be mixed with aggregate calls".to_string(),
                clause.span.clone(),
            ));
        }
        if clause.kind == GqlProjectionKind::With {
            self.aliases = next_scope;
        }

        let mut return_aliases = BTreeSet::new();
        if let GqlReturnPlan::Items(items) = &returns {
            for item in items {
                if let Some(alias) = item.explicit_alias.as_ref() {
                    return_aliases.insert(alias.clone());
                }
            }
        }
        for item in &clause.order_by {
            self.validate_projection_expr(&item.expr, &return_aliases)?;
        }
        if let Some(skip) = clause.skip.as_ref() {
            self.validate_expr(skip, &return_aliases)?;
        }
        if let Some(limit) = clause.limit.as_ref() {
            self.validate_expr(limit, &return_aliases)?;
        }
        if let Some(where_clause) = clause.where_clause.as_ref() {
            self.validate_predicate_expr(where_clause, &BTreeSet::new())?;
        }

        if let Some(previous_aliases) = previous_aliases {
            self.aliases = previous_aliases;
        }

        Ok(GqlBoundProjectionClause {
            kind: clause.kind,
            distinct: clause.distinct,
            distinct_span: clause.distinct_span.clone(),
            returns,
            output_aliases,
            where_clause: clause.where_clause.clone(),
            order_by: clause.order_by.clone(),
            skip: clause.skip.clone(),
            limit: clause.limit.clone(),
            span: clause.span.clone(),
        })
    }

    fn bind_projection_body(
        &mut self,
        kind: GqlProjectionKind,
        body: &ReturnBody,
    ) -> Result<(GqlReturnPlan, Vec<GqlProjectionAlias>, GqlAliasTable), EngineError> {
        let item_bindings = if let Some(items) = return_body_items(body) {
            self.bind_projection_items(kind, items)?
        } else {
            Vec::new()
        };
        let returns = self.return_plan_from_projection_body(body, &item_bindings);
        if kind == GqlProjectionKind::Return {
            let output_aliases =
                self.return_projection_output_aliases_from_bound(body, &item_bindings);
            return Ok((returns, output_aliases, GqlAliasTable::default()));
        }

        let mut next_scope = GqlAliasTable::default();
        let mut output_aliases = Vec::new();
        let mut seen = BTreeSet::new();

        if matches!(body, ReturnBody::All(_) | ReturnBody::AllAndItems { .. }) {
            for alias in &self.aliases.user_order {
                let Some(binding) = self.aliases.get(alias).cloned() else {
                    continue;
                };
                if !binding.user_visible {
                    continue;
                }
                insert_projection_alias(
                    &mut next_scope,
                    &mut output_aliases,
                    &mut seen,
                    binding.name.clone(),
                    binding.kind,
                    binding.span.clone(),
                )?;
            }
        }

        for item in item_bindings {
            let output = item.output_alias;
            insert_projection_alias(
                &mut next_scope,
                &mut output_aliases,
                &mut seen,
                output.name,
                output.kind,
                output.span,
            )?;
        }

        Ok((returns, output_aliases, next_scope))
    }

    fn return_projection_output_aliases_from_bound(
        &self,
        body: &ReturnBody,
        items: &[BoundProjectionItem],
    ) -> Vec<GqlProjectionAlias> {
        let mut aliases = self.star_projection_aliases(body);
        aliases.extend(items.iter().map(|item| item.output_alias.clone()));
        aliases
    }

    fn return_plan_from_projection_body(
        &self,
        body: &ReturnBody,
        items: &[BoundProjectionItem],
    ) -> GqlReturnPlan {
        match body {
            ReturnBody::All(span) => GqlReturnPlan::Star {
                span: span.clone(),
                expanded_aliases: self.aliases.user_order.clone(),
            },
            ReturnBody::AllAndItems { star_span, .. } => {
                let mut bound = self.star_return_item_bindings(star_span);
                bound.extend(items.iter().map(|item| item.return_binding.clone()));
                GqlReturnPlan::Items(bound)
            }
            ReturnBody::Items(_) => GqlReturnPlan::Items(
                items
                    .iter()
                    .map(|item| item.return_binding.clone())
                    .collect(),
            ),
        }
    }

    fn star_projection_aliases(&self, body: &ReturnBody) -> Vec<GqlProjectionAlias> {
        if !matches!(body, ReturnBody::All(_) | ReturnBody::AllAndItems { .. }) {
            return Vec::new();
        }
        self.aliases
            .user_order
            .iter()
            .filter_map(|alias| {
                let binding = self.aliases.get(alias)?;
                binding.user_visible.then(|| GqlProjectionAlias {
                    name: binding.name.clone(),
                    kind: binding.kind,
                    span: binding.span.clone(),
                })
            })
            .collect()
    }

    fn bind_projection_items(
        &mut self,
        kind: GqlProjectionKind,
        items: &[ReturnItem],
    ) -> Result<Vec<BoundProjectionItem>, EngineError> {
        let mut bound = Vec::with_capacity(items.len());
        for item in items {
            bound.push(self.bind_projection_item(kind, item)?);
        }
        Ok(bound)
    }

    fn bind_projection_item(
        &mut self,
        kind: GqlProjectionKind,
        item: &ReturnItem,
    ) -> Result<BoundProjectionItem, EngineError> {
        self.validate_projection_expr(&item.expr, &BTreeSet::new())?;
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
        let return_binding = GqlReturnItemBinding {
            expr: item.expr.clone(),
            explicit_alias,
            output_name,
            span: item.span.clone(),
        };
        let output_alias = self.projection_item_output(kind, item)?;
        Ok(BoundProjectionItem {
            return_binding,
            output_alias,
        })
    }

    fn projection_item_output(
        &self,
        kind: GqlProjectionKind,
        item: &ReturnItem,
    ) -> Result<GqlProjectionAlias, EngineError> {
        let direct_binding = variable_name(&item.expr)
            .and_then(|name| self.aliases.get(name))
            .cloned();
        let Some(explicit_alias) = item.alias.as_ref() else {
            if let Some(binding) = direct_binding {
                return Ok(GqlProjectionAlias {
                    name: binding.name,
                    kind: binding.kind,
                    span: item.span.clone(),
                });
            }
            if kind == GqlProjectionKind::With {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "non-variable WITH projections require an explicit AS alias".to_string(),
                    item.span.clone(),
                ));
            }
            return Ok(GqlProjectionAlias {
                name: expression_output_name(&item.expr),
                kind: GqlAliasKind::Scalar,
                span: item.span.clone(),
            });
        };

        Ok(GqlProjectionAlias {
            name: explicit_alias.name.clone(),
            kind: direct_binding
                .map(|binding| binding.kind)
                .unwrap_or(GqlAliasKind::Scalar),
            span: explicit_alias.span.clone(),
        })
    }

    fn bind_return_body(&mut self, body: &ReturnBody) -> Result<GqlReturnPlan, EngineError> {
        match body {
            ReturnBody::All(span) => Ok(GqlReturnPlan::Star {
                span: span.clone(),
                expanded_aliases: self.aliases.user_order.clone(),
            }),
            ReturnBody::AllAndItems { star_span, items } => {
                let mut bound = self.star_return_item_bindings(star_span);
                bound.extend(self.bind_return_items(items)?);
                Ok(GqlReturnPlan::Items(bound))
            }
            ReturnBody::Items(items) => self.bind_return_items(items).map(GqlReturnPlan::Items),
        }
    }

    fn star_return_item_bindings(&self, span: &SourceSpan) -> Vec<GqlReturnItemBinding> {
        self.aliases
            .user_order
            .iter()
            .filter_map(|alias| {
                let binding = self.aliases.get(alias)?;
                binding.user_visible.then(|| GqlReturnItemBinding {
                    expr: Expr {
                        kind: ExprKind::Variable(alias.clone()),
                        span: span.clone(),
                    },
                    explicit_alias: Some(alias.clone()),
                    output_name: alias.clone(),
                    span: span.clone(),
                })
            })
            .collect()
    }

    fn bind_return_items(
        &mut self,
        items: &[ReturnItem],
    ) -> Result<Vec<GqlReturnItemBinding>, EngineError> {
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
        Ok(bound)
    }

    fn validate_expr(
        &mut self,
        expr: &Expr,
        return_aliases: &BTreeSet<String>,
    ) -> Result<(), EngineError> {
        self.validate_expr_aggregate_context(expr, return_aliases, false, false, false)
    }

    fn validate_predicate_expr(
        &mut self,
        expr: &Expr,
        return_aliases: &BTreeSet<String>,
    ) -> Result<(), EngineError> {
        self.validate_expr_aggregate_context(expr, return_aliases, false, false, true)
    }

    fn validate_projection_expr(
        &mut self,
        expr: &Expr,
        return_aliases: &BTreeSet<String>,
    ) -> Result<(), EngineError> {
        self.validate_expr_aggregate_context(expr, return_aliases, true, false, false)
    }

    fn validate_expr_aggregate_context(
        &mut self,
        expr: &Expr,
        return_aliases: &BTreeSet<String>,
        allow_aggregate: bool,
        inside_aggregate: bool,
        allow_subquery: bool,
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
                self.validate_expr_aggregate_context(
                    object,
                    return_aliases,
                    allow_aggregate,
                    inside_aggregate,
                    allow_subquery,
                )?;
                if let ExprKind::Variable(alias) = &object.kind {
                    if self
                        .aliases
                        .get(alias)
                        .is_some_and(|binding| binding.kind == GqlAliasKind::Path)
                    {
                        return Err(path_property_access_error(&property.name, &property.span));
                    }
                }
                Ok(())
            }
            ExprKind::Unary { expr, .. } => self.validate_expr_aggregate_context(
                expr,
                return_aliases,
                allow_aggregate,
                inside_aggregate,
                allow_subquery,
            ),
            ExprKind::Binary { left, right, .. } => {
                self.validate_expr_aggregate_context(
                    left,
                    return_aliases,
                    allow_aggregate,
                    inside_aggregate,
                    allow_subquery,
                )?;
                self.validate_expr_aggregate_context(
                    right,
                    return_aliases,
                    allow_aggregate,
                    inside_aggregate,
                    allow_subquery,
                )
            }
            ExprKind::IsNull { expr, .. } => self.validate_expr_aggregate_context(
                expr,
                return_aliases,
                allow_aggregate,
                inside_aggregate,
                allow_subquery,
            ),
            ExprKind::FunctionCall { name, args } => self.validate_function_call(
                name,
                args,
                return_aliases,
                allow_aggregate,
                inside_aggregate,
                allow_subquery,
            ),
            ExprKind::AggregateCall { arg, name_span, .. } => {
                if !allow_aggregate {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        "aggregate calls are only valid in WITH/RETURN projections and projection ORDER BY".to_string(),
                        name_span.clone(),
                    ));
                }
                if inside_aggregate {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        "nested aggregate calls are not supported".to_string(),
                        name_span.clone(),
                    ));
                }
                if let Some(arg) = arg.as_ref() {
                    self.validate_expr_aggregate_context(arg, return_aliases, true, true, false)?;
                }
                Ok(())
            }
            ExprKind::ExistsSubquery(pipeline) => {
                if !allow_subquery {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        "EXISTS subqueries are supported only in predicate positions".to_string(),
                        expr.span.clone(),
                    ));
                }
                let outer_aliases = self.aliases.clone();
                let (_, _, _, parameters, parameter_spans) =
                    bind_subquery_pipeline_parts(pipeline, &outer_aliases, self.params)?;
                self.parameters.extend(parameters);
                self.parameter_spans.extend(parameter_spans);
                Ok(())
            }
            ExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand.as_ref() {
                    self.validate_expr_aggregate_context(
                        operand,
                        return_aliases,
                        allow_aggregate,
                        inside_aggregate,
                        allow_subquery,
                    )?;
                }
                for branch in branches {
                    self.validate_expr_aggregate_context(
                        &branch.when,
                        return_aliases,
                        allow_aggregate,
                        inside_aggregate,
                        allow_subquery,
                    )?;
                    self.validate_expr_aggregate_context(
                        &branch.then,
                        return_aliases,
                        allow_aggregate,
                        inside_aggregate,
                        allow_subquery,
                    )?;
                }
                if let Some(else_expr) = else_expr.as_ref() {
                    self.validate_expr_aggregate_context(
                        else_expr,
                        return_aliases,
                        allow_aggregate,
                        inside_aggregate,
                        allow_subquery,
                    )?;
                }
                Ok(())
            }
            ExprKind::List(items) => {
                for item in items {
                    self.validate_expr_aggregate_context(
                        item,
                        return_aliases,
                        allow_aggregate,
                        inside_aggregate,
                        allow_subquery,
                    )?;
                }
                Ok(())
            }
            ExprKind::Map(map) => {
                for entry in &map.entries {
                    self.validate_expr_aggregate_context(
                        &entry.value,
                        return_aliases,
                        allow_aggregate,
                        inside_aggregate,
                        allow_subquery,
                    )?;
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
        return_aliases: &BTreeSet<String>,
        allow_aggregate: bool,
        inside_aggregate: bool,
        allow_subquery: bool,
    ) -> Result<(), EngineError> {
        let function = name.name.to_ascii_lowercase();
        if is_scalar_function(&function) {
            validate_scalar_function_arity(&function, name, args.len())?;
            for arg in args {
                self.validate_expr_aggregate_context(
                    arg,
                    return_aliases,
                    allow_aggregate,
                    inside_aggregate,
                    allow_subquery,
                )?;
            }
            return Ok(());
        }
        if !is_graph_function(&function) {
            return Err(EngineError::GqlUnsupported {
                feature: "function".to_string(),
                message: format!("function '{}' is not supported in Phase 31", name.name),
                span: name.span.clone(),
            });
        }
        if args.len() != 1 {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("function '{}' expects exactly one argument", name.name),
                name.span.clone(),
            ));
        }
        if let Some((_, endpoint_arg)) = edge_endpoint_id_call(name, args) {
            // Projection aliases (RETURN ... AS q) are not function targets: validate the
            // inner variable against pattern aliases only, like the generic path below.
            self.validate_expr_aggregate_context(
                endpoint_arg,
                &BTreeSet::new(),
                allow_aggregate,
                inside_aggregate,
                allow_subquery,
            )?;
            let alias = variable_name(endpoint_arg).expect("endpoint call shape checked");
            let binding = self.aliases.get(alias).expect("alias validated above");
            if binding.kind != GqlAliasKind::Edge {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "startNode()/endNode() inside id() expects an edge alias".to_string(),
                    endpoint_arg.span.clone(),
                ));
            }
            return Ok(());
        }
        self.validate_expr_aggregate_context(
            &args[0],
            &BTreeSet::new(),
            allow_aggregate,
            inside_aggregate,
            allow_subquery,
        )?;
        let Some(alias) = variable_name(&args[0]) else {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("function '{}' expects a bound alias argument", name.name),
                args[0].span.clone(),
            ));
        };
        let binding = self.aliases.get(alias).expect("alias validated above");
        if let Some(metadata) = GqlMetadataFunction::from_lower(&function) {
            let valid = match binding.kind {
                GqlAliasKind::Node => metadata.valid_for_node(),
                GqlAliasKind::Edge => metadata.valid_for_edge(),
                GqlAliasKind::Path | GqlAliasKind::Scalar => false,
            };
            if valid {
                return Ok(());
            }
            let expected = match metadata {
                GqlMetadataFunction::ElementKey => "a node alias",
                GqlMetadataFunction::ValidFrom | GqlMetadataFunction::ValidTo => "an edge alias",
                _ => "a node or edge alias",
            };
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("{}() expects {expected}", metadata.canonical_name()),
                args[0].span.clone(),
            ));
        }
        if let Some(endpoint) = GqlEndpointFunction::from_lower(&function) {
            return match binding.kind {
                GqlAliasKind::Path => Ok(()),
                GqlAliasKind::Edge => Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!(
                        "{}() on an edge is only supported inside id(); use id({}({})) or the bound pattern alias",
                        endpoint.canonical_name(),
                        endpoint.canonical_name(),
                        alias
                    ),
                    args[0].span.clone(),
                )),
                GqlAliasKind::Node | GqlAliasKind::Scalar => Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!("{}() expects a path alias", endpoint.canonical_name()),
                    args[0].span.clone(),
                )),
            };
        }
        match (function.as_str(), binding.kind) {
            ("labels", GqlAliasKind::Node)
            | ("type", GqlAliasKind::Edge)
            | ("length", GqlAliasKind::Path)
            | ("nodes", GqlAliasKind::Path)
            | ("relationships", GqlAliasKind::Path)
            | ("nodeids", GqlAliasKind::Path)
            | ("edgeids", GqlAliasKind::Path) => Ok(()),
            ("labels", GqlAliasKind::Edge | GqlAliasKind::Path | GqlAliasKind::Scalar) => {
                Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "labels() expects a node alias".to_string(),
                    args[0].span.clone(),
                ))
            }
            ("type", GqlAliasKind::Node | GqlAliasKind::Path | GqlAliasKind::Scalar) => {
                Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "type() expects an edge alias".to_string(),
                    args[0].span.clone(),
                ))
            }
            (
                "length" | "nodes" | "relationships" | "nodeids" | "edgeids",
                GqlAliasKind::Node | GqlAliasKind::Edge | GqlAliasKind::Scalar,
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

pub(crate) fn bind_subquery_pipeline_for_outer_aliases(
    pipeline: &GqlReadPipeline,
    outer_aliases: &GqlAliasTable,
    params: &GqlParams,
) -> Result<(GqlBoundReadPipeline, Vec<String>, Vec<GqlProjectionAlias>), EngineError> {
    let (pipeline, imports, outputs, _, _) =
        bind_subquery_pipeline_parts(pipeline, outer_aliases, params)?;
    Ok((pipeline, imports, outputs))
}

type BoundSubqueryPipelineParts = (
    GqlBoundReadPipeline,
    Vec<String>,
    Vec<GqlProjectionAlias>,
    BTreeSet<String>,
    BTreeMap<String, SourceSpan>,
);

fn bind_subquery_pipeline_parts(
    pipeline: &GqlReadPipeline,
    outer_aliases: &GqlAliasTable,
    params: &GqlParams,
) -> Result<BoundSubqueryPipelineParts, EngineError> {
    let import_aliases = collect_subquery_import_aliases(pipeline, outer_aliases);
    let mut binder = SemanticBinder {
        aliases: outer_aliases.clone(),
        anonymous_node_counter: 0,
        parameters: BTreeSet::new(),
        parameter_spans: BTreeMap::new(),
        params,
    };
    let bound = binder.bind_read_pipeline(pipeline)?;
    let outputs = terminal_output_aliases_for_read_pipeline(&bound)?;
    Ok((
        bound,
        import_aliases,
        outputs,
        binder.parameters,
        binder.parameter_spans,
    ))
}

fn terminal_output_aliases_for_read_pipeline(
    pipeline: &GqlBoundReadPipeline,
) -> Result<Vec<GqlProjectionAlias>, EngineError> {
    let mut outputs = terminal_output_aliases(&pipeline.clauses)?;
    for branch in &pipeline.union_branches {
        let branch_outputs = terminal_output_aliases(&branch.clauses)?;
        if branch_outputs.len() != outputs.len() {
            return Err(EngineError::InvalidOperation(
                "GQL UNION branch output metadata length mismatch".to_string(),
            ));
        }
        for (output, branch_output) in outputs.iter_mut().zip(branch_outputs.iter()) {
            if output.name != branch_output.name {
                return Err(EngineError::InvalidOperation(
                    "GQL UNION branch output metadata name mismatch".to_string(),
                ));
            }
            if output.kind != branch_output.kind {
                output.kind = GqlAliasKind::Scalar;
            }
        }
    }
    Ok(outputs)
}

fn terminal_output_aliases(
    clauses: &[GqlBoundPipelineClause],
) -> Result<Vec<GqlProjectionAlias>, EngineError> {
    clauses
        .iter()
        .rev()
        .find_map(|clause| match clause {
            GqlBoundPipelineClause::Projection(projection)
                if projection.kind == GqlProjectionKind::Return =>
            {
                Some(projection.output_aliases.clone())
            }
            _ => None,
        })
        .ok_or_else(|| {
            EngineError::InvalidOperation("GQL read pipeline must end in RETURN".to_string())
        })
}

fn collect_subquery_import_aliases(
    pipeline: &GqlReadPipeline,
    outer_aliases: &GqlAliasTable,
) -> Vec<String> {
    let mut seen = BTreeSet::new();
    collect_pipeline_outer_alias_references(pipeline, outer_aliases, &mut seen);
    let mut ordered = Vec::new();
    for alias in &outer_aliases.user_order {
        if seen.remove(alias) {
            ordered.push(alias.clone());
        }
    }
    ordered.extend(seen);
    ordered
}

fn collect_pipeline_outer_alias_references(
    pipeline: &GqlReadPipeline,
    outer_aliases: &GqlAliasTable,
    seen: &mut BTreeSet<String>,
) {
    for clause in &pipeline.clauses {
        collect_pipeline_clause_outer_alias_references(clause, outer_aliases, seen);
    }
    for branch in &pipeline.union_branches {
        for clause in &branch.clauses {
            collect_pipeline_clause_outer_alias_references(clause, outer_aliases, seen);
        }
    }
}

fn collect_pipeline_clause_outer_alias_references(
    clause: &GqlPipelineClause,
    outer_aliases: &GqlAliasTable,
    seen: &mut BTreeSet<String>,
) {
    match clause {
        GqlPipelineClause::Match(clauses) => {
            for clause in clauses {
                for pattern in &clause.patterns {
                    collect_pattern_outer_alias_references(pattern, outer_aliases, seen);
                }
                if let Some(where_clause) = clause.where_clause.as_ref() {
                    collect_expr_outer_alias_references(where_clause, outer_aliases, seen);
                }
            }
        }
        GqlPipelineClause::ShortestPath(shortest) => {
            collect_pattern_outer_alias_references(&shortest.pattern, outer_aliases, seen);
        }
        GqlPipelineClause::Call(call) => {
            collect_pipeline_outer_alias_references(&call.pipeline, outer_aliases, seen);
        }
        GqlPipelineClause::Projection(projection) => {
            match &projection.body {
                ReturnBody::All(_) => {}
                ReturnBody::AllAndItems { items, .. } | ReturnBody::Items(items) => {
                    for item in items {
                        collect_expr_outer_alias_references(&item.expr, outer_aliases, seen);
                    }
                }
            }
            if let Some(where_clause) = projection.where_clause.as_ref() {
                collect_expr_outer_alias_references(where_clause, outer_aliases, seen);
            }
            for item in &projection.order_by {
                collect_expr_outer_alias_references(&item.expr, outer_aliases, seen);
            }
            if let Some(skip) = projection.skip.as_ref() {
                collect_expr_outer_alias_references(skip, outer_aliases, seen);
            }
            if let Some(limit) = projection.limit.as_ref() {
                collect_expr_outer_alias_references(limit, outer_aliases, seen);
            }
        }
    }
}

fn collect_pattern_outer_alias_references(
    pattern: &Pattern,
    outer_aliases: &GqlAliasTable,
    seen: &mut BTreeSet<String>,
) {
    if let Some(alias) = pattern.path_variable.as_ref() {
        collect_outer_alias_name(&alias.name, outer_aliases, seen);
    }
    collect_node_pattern_outer_alias_references(&pattern.start, outer_aliases, seen);
    for chain in &pattern.chains {
        if let Some(alias) = chain.relationship.variable.as_ref() {
            collect_outer_alias_name(&alias.name, outer_aliases, seen);
        }
        if let Some(properties) = chain.relationship.properties.as_ref() {
            collect_map_outer_alias_references(properties, outer_aliases, seen);
        }
        collect_node_pattern_outer_alias_references(&chain.node, outer_aliases, seen);
    }
}

fn collect_node_pattern_outer_alias_references(
    pattern: &NodePattern,
    outer_aliases: &GqlAliasTable,
    seen: &mut BTreeSet<String>,
) {
    if let Some(alias) = pattern.variable.as_ref() {
        collect_outer_alias_name(&alias.name, outer_aliases, seen);
    }
    if let Some(properties) = pattern.properties.as_ref() {
        collect_map_outer_alias_references(properties, outer_aliases, seen);
    }
}

fn collect_map_outer_alias_references(
    map: &MapLiteral,
    outer_aliases: &GqlAliasTable,
    seen: &mut BTreeSet<String>,
) {
    for entry in &map.entries {
        collect_expr_outer_alias_references(&entry.value, outer_aliases, seen);
    }
}

fn collect_expr_outer_alias_references(
    expr: &Expr,
    outer_aliases: &GqlAliasTable,
    seen: &mut BTreeSet<String>,
) {
    match &expr.kind {
        ExprKind::Variable(name) => collect_outer_alias_name(name, outer_aliases, seen),
        ExprKind::PropertyAccess { object, .. } => {
            collect_expr_outer_alias_references(object, outer_aliases, seen)
        }
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            collect_expr_outer_alias_references(expr, outer_aliases, seen)
        }
        ExprKind::Binary { left, right, .. } => {
            collect_expr_outer_alias_references(left, outer_aliases, seen);
            collect_expr_outer_alias_references(right, outer_aliases, seen);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            for arg in args {
                collect_expr_outer_alias_references(arg, outer_aliases, seen);
            }
        }
        ExprKind::AggregateCall { arg, .. } => {
            if let Some(arg) = arg.as_ref() {
                collect_expr_outer_alias_references(arg, outer_aliases, seen);
            }
        }
        ExprKind::ExistsSubquery(pipeline) => {
            collect_pipeline_outer_alias_references(pipeline, outer_aliases, seen);
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            if let Some(operand) = operand.as_ref() {
                collect_expr_outer_alias_references(operand, outer_aliases, seen);
            }
            for branch in branches {
                collect_expr_outer_alias_references(&branch.when, outer_aliases, seen);
                collect_expr_outer_alias_references(&branch.then, outer_aliases, seen);
            }
            if let Some(else_expr) = else_expr.as_ref() {
                collect_expr_outer_alias_references(else_expr, outer_aliases, seen);
            }
        }
        ExprKind::Map(map) => collect_map_outer_alias_references(map, outer_aliases, seen),
        ExprKind::Literal(_) | ExprKind::Parameter(_) => {}
    }
}

fn collect_outer_alias_name(
    name: &str,
    outer_aliases: &GqlAliasTable,
    seen: &mut BTreeSet<String>,
) {
    if outer_aliases.contains(name) {
        seen.insert(name.to_string());
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
            MutationClause::Merge(merge) => self
                .bind_merge_clause(merge)
                .map(GqlBoundMutationClause::Merge),
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

    fn bind_merge_clause(
        &mut self,
        merge: &MergeClause,
    ) -> Result<GqlBoundMergeClause, EngineError> {
        let pattern = self.bind_merge_pattern(&merge.pattern)?;
        let on_create = merge
            .on_create
            .as_ref()
            .map(|set| self.bind_set_clause_with_source_mode(set, true))
            .transpose()?
            .unwrap_or_else(|| GqlBoundSetClause {
                items: Vec::new(),
                span: merge.span.clone(),
            });
        let on_match = merge
            .on_match
            .as_ref()
            .map(|set| self.bind_set_clause_with_source_mode(set, true))
            .transpose()?
            .unwrap_or_else(|| GqlBoundSetClause {
                items: Vec::new(),
                span: merge.span.clone(),
            });
        Ok(GqlBoundMergeClause {
            pattern,
            on_create,
            on_match,
            span: merge.span.clone(),
        })
    }

    fn bind_merge_pattern(
        &mut self,
        pattern: &Pattern,
    ) -> Result<GqlBoundMergePattern, EngineError> {
        if let Some(path_variable) = pattern.path_variable.as_ref() {
            return Err(EngineError::GqlUnsupported {
                feature: "MERGE path assignment".to_string(),
                message: "MERGE path assignment is not supported".to_string(),
                span: path_variable.span.clone(),
            });
        }
        match pattern.chains.as_slice() {
            [] => self
                .bind_merge_node_pattern(&pattern.start)
                .map(GqlBoundMergePattern::Node),
            [chain] => self
                .bind_merge_relationship_pattern(&pattern.start, chain)
                .map(GqlBoundMergePattern::Relationship),
            _ => Err(EngineError::GqlUnsupported {
                feature: "general pattern MERGE".to_string(),
                message:
                    "MERGE supports only keyed node patterns and single-hop relationship patterns"
                        .to_string(),
                span: pattern.span.clone(),
            }),
        }
    }

    fn bind_merge_node_pattern(
        &mut self,
        pattern: &NodePattern,
    ) -> Result<GqlBoundMergeNode, EngineError> {
        let variable = pattern.variable.as_ref().ok_or_else(|| {
            gql_semantic_error(
                GqlSemanticErrorCode::UnknownVariable,
                "MERGE node pattern requires an alias".to_string(),
                pattern.span.clone(),
            )
        })?;
        if self.aliases.contains_key(&variable.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DuplicateAlias,
                format!("MERGE node alias '{}' is already bound", variable.name),
                variable.span.clone(),
            ));
        }
        if pattern.labels.is_empty() {
            return Err(EngineError::GqlUnsupported {
                feature: "unlabeled node MERGE".to_string(),
                message: "MERGE node patterns require exactly one static label".to_string(),
                span: pattern.span.clone(),
            });
        }
        if pattern.labels.len() != 1 {
            return Err(EngineError::GqlUnsupported {
                feature: "multi-label node MERGE".to_string(),
                message: "MERGE node patterns require exactly one static label".to_string(),
                span: pattern.span.clone(),
            });
        }
        let label = pattern.labels[0].clone();
        validate_label_token_name(&label.name).map_err(|err| match err {
            EngineError::InvalidOperation(message) => gql_semantic_error(
                GqlSemanticErrorCode::DynamicLabelNotSupported,
                message,
                label.span.clone(),
            ),
            other => other,
        })?;
        let properties =
            pattern
                .properties
                .as_ref()
                .ok_or_else(|| EngineError::GqlUnsupported {
                    feature: "unkeyed node MERGE".to_string(),
                    message:
                        "MERGE node patterns require exactly one identity entry named elementKey"
                            .to_string(),
                    span: pattern.span.clone(),
                })?;
        if properties.entries.len() != 1 {
            return Err(EngineError::GqlUnsupported {
                feature: "node MERGE property-map identity".to_string(),
                message: "MERGE node identity supports only {elementKey: expr}".to_string(),
                span: properties.span.clone(),
            });
        }
        let entry = &properties.entries[0];
        if GqlElementMapMetadataKey::from_key(&entry.key.name)
            != Some(GqlElementMapMetadataKey::ElementKey)
        {
            return Err(EngineError::GqlUnsupported {
                feature: "node MERGE non-key identity property".to_string(),
                message: "MERGE node identity entry must be named elementKey".to_string(),
                span: entry.key.span.clone(),
            });
        }
        self.validate_expr(&entry.value, &BTreeSet::new(), false)?;
        self.reject_statically_element_property_value(&entry.value)?;
        self.insert_merged_alias(variable, GqlAliasKind::Node)?;
        Ok(GqlBoundMergeNode {
            alias: variable.name.clone(),
            label,
            key: entry.value.clone(),
            span: pattern.span.clone(),
        })
    }

    fn bind_merge_relationship_pattern(
        &mut self,
        start: &NodePattern,
        chain: &PatternChain,
    ) -> Result<GqlBoundMergeRelationship, EngineError> {
        let rel = &chain.relationship;
        if rel.direction == RelationshipDirection::Undirected {
            return Err(EngineError::GqlUnsupported {
                feature: "undirected relationship MERGE".to_string(),
                message: "MERGE relationship patterns must be directed".to_string(),
                span: rel.span.clone(),
            });
        }
        if rel.quantifier.is_some() {
            return Err(EngineError::GqlUnsupported {
                feature: "variable-length MERGE".to_string(),
                message: "variable-length relationship patterns are not supported in MERGE"
                    .to_string(),
                span: rel.span.clone(),
            });
        }
        if rel.properties.is_some() {
            return Err(EngineError::GqlUnsupported {
                feature: "relationship MERGE properties".to_string(),
                message: "MERGE relationship patterns do not support identity properties; use ON CREATE SET".to_string(),
                span: rel.span.clone(),
            });
        }
        if rel.rel_types.len() != 1 {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DynamicRelationshipTypeNotSupported,
                "MERGE relationship patterns require exactly one static relationship label"
                    .to_string(),
                rel.span.clone(),
            ));
        }
        let rel_type = rel.rel_types[0].clone();
        validate_label_token_name(&rel_type.name).map_err(|err| match err {
            EngineError::InvalidOperation(message) => gql_semantic_error(
                GqlSemanticErrorCode::DynamicRelationshipTypeNotSupported,
                message,
                rel_type.span.clone(),
            ),
            other => other,
        })?;
        let alias = rel.variable.as_ref().ok_or_else(|| {
            gql_semantic_error(
                GqlSemanticErrorCode::UnknownVariable,
                "MERGE relationship pattern requires an alias".to_string(),
                rel.span.clone(),
            )
        })?;
        if self.aliases.contains_key(&alias.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::DuplicateAlias,
                format!("MERGE relationship alias '{}' is already bound", alias.name),
                alias.span.clone(),
            ));
        }
        let start_alias = self.require_merge_endpoint_alias(start)?;
        let end_alias = self.require_merge_endpoint_alias(&chain.node)?;
        let (from_alias, to_alias) = match rel.direction {
            RelationshipDirection::LeftToRight => (start_alias, end_alias),
            RelationshipDirection::RightToLeft => (end_alias, start_alias),
            RelationshipDirection::Undirected => unreachable!("rejected above"),
        };
        self.insert_merged_alias(alias, GqlAliasKind::Edge)?;
        self.record_incident_edge(&from_alias, &to_alias, &alias.name);
        Ok(GqlBoundMergeRelationship {
            alias: alias.name.clone(),
            from_alias,
            to_alias,
            rel_type,
            span: rel.span.clone(),
        })
    }

    fn require_merge_endpoint_alias(&self, pattern: &NodePattern) -> Result<String, EngineError> {
        if !pattern.labels.is_empty() || pattern.properties.is_some() {
            return Err(EngineError::GqlUnsupported {
                feature: "relationship MERGE endpoint pattern".to_string(),
                message: "MERGE relationship endpoints must be bare bound node aliases".to_string(),
                span: pattern.span.clone(),
            });
        }
        let variable = pattern
            .variable
            .as_ref()
            .ok_or_else(|| EngineError::GqlUnsupported {
                feature: "relationship MERGE endpoint pattern".to_string(),
                message: "MERGE relationship endpoints must be bound node aliases".to_string(),
                span: pattern.span.clone(),
            })?;
        let binding = self.aliases.get(&variable.name).ok_or_else(|| {
            gql_semantic_error(
                GqlSemanticErrorCode::UnknownVariable,
                format!(
                    "unknown MERGE relationship endpoint alias '{}'",
                    variable.name
                ),
                variable.span.clone(),
            )
        })?;
        if binding.kind != GqlAliasKind::Node {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!(
                    "MERGE relationship endpoint '{}' is bound as {:?}, not a node",
                    variable.name, binding.kind
                ),
                variable.span.clone(),
            ));
        }
        if self.deleted_aliases.contains(&variable.name) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!(
                    "MERGE relationship endpoint '{}' was deleted earlier in this statement",
                    variable.name
                ),
                variable.span.clone(),
            ));
        }
        Ok(variable.name.clone())
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
        self.bind_set_clause_with_source_mode(set, false)
    }

    fn bind_set_clause_with_source_mode(
        &mut self,
        set: &SetClause,
        allow_created_sources: bool,
    ) -> Result<GqlBoundSetClause, EngineError> {
        let items = set
            .items
            .iter()
            .map(|item| self.bind_set_item_with_source_mode(item, allow_created_sources))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GqlBoundSetClause {
            items,
            span: set.span.clone(),
        })
    }

    fn bind_set_item(&mut self, item: &SetItem) -> Result<GqlBoundSetItem, EngineError> {
        self.bind_set_item_with_source_mode(item, false)
    }

    fn bind_set_item_with_source_mode(
        &mut self,
        item: &SetItem,
        allow_created_sources: bool,
    ) -> Result<GqlBoundSetItem, EngineError> {
        match item {
            SetItem::Property {
                alias,
                property,
                value,
                span,
            } => {
                let binding = self.require_target_alias(alias)?;
                if !matches!(binding.kind, GqlAliasKind::Node | GqlAliasKind::Edge) {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "SET property target '{}' must be a node or edge alias",
                            alias.name
                        ),
                        alias.span.clone(),
                    ));
                }
                self.validate_expr(value, &BTreeSet::new(), allow_created_sources)?;
                if allow_created_sources {
                    self.reject_commit_dependent_created_source_value(value)?;
                }
                self.reject_statically_element_property_value(value)?;
                Ok(GqlBoundSetItem::Property {
                    alias: alias.name.clone(),
                    target_kind: binding.kind,
                    property: property.clone(),
                    value: value.clone(),
                    span: span.clone(),
                })
            }
            SetItem::Metadata {
                function,
                alias,
                value,
                span,
            } => {
                let binding = self.require_target_alias(alias)?;
                let field = GqlMetadataFunction::from_lower(&function.name.to_ascii_lowercase())
                    .ok_or_else(|| {
                        gql_semantic_error(
                            GqlSemanticErrorCode::InvalidPropertyAccess,
                            format!("unknown metadata function '{}' in SET", function.name),
                            function.span.clone(),
                        )
                    })?;
                let (valid_for_kind, writable) = match binding.kind {
                    GqlAliasKind::Node => (field.valid_for_node(), field.writable_for_node()),
                    GqlAliasKind::Edge => (field.valid_for_edge(), field.writable_for_edge()),
                    GqlAliasKind::Path | GqlAliasKind::Scalar => {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::InvalidPropertyAccess,
                            format!(
                                "SET metadata target '{}' must be a node or edge alias",
                                alias.name
                            ),
                            alias.span.clone(),
                        ));
                    }
                };
                if !valid_for_kind {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "{}() is not valid for {} alias '{}'",
                            field.canonical_name(),
                            kind_name(binding.kind),
                            alias.name
                        ),
                        function.span.clone(),
                    ));
                }
                if !writable {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "SET target '{}({})' is read-only metadata",
                            field.canonical_name(),
                            alias.name
                        ),
                        function.span.clone(),
                    ));
                }
                self.validate_expr(value, &BTreeSet::new(), allow_created_sources)?;
                if allow_created_sources {
                    self.reject_commit_dependent_created_source_value(value)?;
                }
                self.reject_statically_element_property_value(value)?;
                Ok(GqlBoundSetItem::Metadata {
                    alias: alias.name.clone(),
                    target_kind: binding.kind,
                    field,
                    value: value.clone(),
                    span: span.clone(),
                })
            }
            SetItem::MapMerge { alias, value, span } => {
                let binding = self.require_target_alias(alias)?;
                self.validate_expr(value, &BTreeSet::new(), allow_created_sources)?;
                if allow_created_sources {
                    self.reject_commit_dependent_created_source_value(value)?;
                }
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
                if !matches!(binding.kind, GqlAliasKind::Node | GqlAliasKind::Edge) {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "REMOVE property target '{}' must be a node or edge alias",
                            alias.name
                        ),
                        alias.span.clone(),
                    ));
                }
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
                (_, GqlAliasKind::Scalar) => {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidReturnExpression,
                        "scalar aliases cannot be deleted".to_string(),
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
            ReturnBody::AllAndItems { star_span, items } => {
                let mut bound = self
                    .user_order
                    .iter()
                    .map(|alias| GqlReturnItemBinding {
                        expr: Expr {
                            kind: ExprKind::Variable(alias.clone()),
                            span: star_span.clone(),
                        },
                        explicit_alias: Some(alias.clone()),
                        output_name: alias.clone(),
                        span: star_span.clone(),
                    })
                    .collect::<Vec<_>>();
                let mut item_bound = self.bind_mutation_return_items(items)?;
                bound.append(&mut item_bound);
                Ok(GqlReturnPlan::Items(bound))
            }
            ReturnBody::Items(items) => self
                .bind_mutation_return_items(items)
                .map(GqlReturnPlan::Items),
        }
    }

    fn bind_mutation_return_items(
        &mut self,
        items: &[ReturnItem],
    ) -> Result<Vec<GqlReturnItemBinding>, EngineError> {
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
        Ok(bound)
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
                "CREATE node patterns require a property map containing elementKey".to_string(),
                pattern.span.clone(),
            ));
        };
        let mut has_element_key = false;
        for entry in &properties.entries {
            if let Some(metadata) = GqlElementMapMetadataKey::from_key(&entry.key.name) {
                if !metadata.valid_for_node() {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "element map metadata '{}' is valid only for relationships",
                            metadata.canonical_name()
                        ),
                        entry.key.span.clone(),
                    ));
                }
                if metadata == GqlElementMapMetadataKey::ElementKey {
                    has_element_key = true;
                }
            }
            self.validate_expr(&entry.value, &BTreeSet::new(), false)?;
            self.reject_statically_element_property_value(&entry.value)?;
        }
        if !has_element_key {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "CREATE node property map must contain elementKey".to_string(),
                properties.span.clone(),
            ));
        }
        Ok(())
    }

    fn validate_create_edge_map(&mut self, properties: &MapLiteral) -> Result<(), EngineError> {
        for entry in &properties.entries {
            if let Some(metadata) = GqlElementMapMetadataKey::from_key(&entry.key.name) {
                if !metadata.valid_for_edge() {
                    return Err(gql_semantic_error(
                        GqlSemanticErrorCode::InvalidPropertyAccess,
                        format!(
                            "element map metadata '{}' is valid only for nodes",
                            metadata.canonical_name()
                        ),
                        entry.key.span.clone(),
                    ));
                }
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
        if matches!(binding.kind, GqlAliasKind::Path | GqlAliasKind::Scalar) {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidPropertyAccess,
                format!(
                    "{} aliases cannot be mutation targets",
                    kind_name(binding.kind)
                ),
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
                    if matches!(
                        binding.origin,
                        GqlAliasOrigin::Created | GqlAliasOrigin::Merged
                    ) && !allow_created_sources
                    {
                        return Err(gql_semantic_error(
                            GqlSemanticErrorCode::InvalidReturnExpression,
                            format!(
                                "created or merged alias '{}' cannot be used as a mutation expression source before commit",
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
                    {
                        return Err(path_property_access_error(&property.name, &property.span));
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
                self.validate_function_call(name, args, return_aliases, allow_created_sources)
            }
            ExprKind::AggregateCall { name_span, .. } => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "aggregate calls are not supported in mutation expressions or mutation RETURN"
                    .to_string(),
                name_span.clone(),
            )),
            ExprKind::ExistsSubquery(_) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "EXISTS subqueries are not supported in mutation expressions or mutation RETURN"
                    .to_string(),
                expr.span.clone(),
            )),
            ExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand.as_ref() {
                    self.validate_expr(operand, return_aliases, allow_created_sources)?;
                }
                for branch in branches {
                    self.validate_expr(&branch.when, return_aliases, allow_created_sources)?;
                    self.validate_expr(&branch.then, return_aliases, allow_created_sources)?;
                }
                if let Some(else_expr) = else_expr.as_ref() {
                    self.validate_expr(else_expr, return_aliases, allow_created_sources)?;
                }
                Ok(())
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
        return_aliases: &BTreeSet<String>,
        allow_created_sources: bool,
    ) -> Result<(), EngineError> {
        let function = name.name.to_ascii_lowercase();
        if is_scalar_function(&function) {
            validate_scalar_function_arity(&function, name, args.len())?;
            for arg in args {
                self.validate_expr(arg, return_aliases, allow_created_sources)?;
            }
            return Ok(());
        }
        if !is_graph_function(&function) {
            return Err(EngineError::GqlUnsupported {
                feature: "function".to_string(),
                message: format!("function '{}' is not supported", name.name),
                span: name.span.clone(),
            });
        }
        if args.len() != 1 {
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("function '{}' expects exactly one argument", name.name),
                name.span.clone(),
            ));
        }
        if let Some((_, endpoint_arg)) = edge_endpoint_id_call(name, args) {
            self.validate_expr(endpoint_arg, &BTreeSet::new(), allow_created_sources)?;
            let alias = variable_name(endpoint_arg).expect("endpoint call shape checked");
            let binding = self.aliases.get(alias).expect("alias validated above");
            if binding.kind != GqlAliasKind::Edge {
                return Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "startNode()/endNode() inside id() expects an edge alias".to_string(),
                    endpoint_arg.span.clone(),
                ));
            }
            return Ok(());
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
        if let Some(metadata) = GqlMetadataFunction::from_lower(&function) {
            let valid = match binding.kind {
                GqlAliasKind::Node => metadata.valid_for_node(),
                GqlAliasKind::Edge => metadata.valid_for_edge(),
                GqlAliasKind::Path | GqlAliasKind::Scalar => false,
            };
            if valid {
                return Ok(());
            }
            let expected = match metadata {
                GqlMetadataFunction::ElementKey => "a node alias",
                GqlMetadataFunction::ValidFrom | GqlMetadataFunction::ValidTo => "an edge alias",
                _ => "a node or edge alias",
            };
            return Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                format!("{}() expects {expected}", metadata.canonical_name()),
                args[0].span.clone(),
            ));
        }
        if let Some(endpoint) = GqlEndpointFunction::from_lower(&function) {
            return match binding.kind {
                GqlAliasKind::Path => Ok(()),
                GqlAliasKind::Edge => Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!(
                        "{}() on an edge is only supported inside id(); use id({}({})) or the bound pattern alias",
                        endpoint.canonical_name(),
                        endpoint.canonical_name(),
                        alias
                    ),
                    args[0].span.clone(),
                )),
                GqlAliasKind::Node | GqlAliasKind::Scalar => Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    format!("{}() expects a path alias", endpoint.canonical_name()),
                    args[0].span.clone(),
                )),
            };
        }
        match (function.as_str(), binding.kind) {
            ("labels", GqlAliasKind::Node)
            | ("type", GqlAliasKind::Edge)
            | ("length", GqlAliasKind::Path)
            | ("nodes", GqlAliasKind::Path)
            | ("relationships", GqlAliasKind::Path)
            | ("nodeids", GqlAliasKind::Path)
            | ("edgeids", GqlAliasKind::Path) => Ok(()),
            ("labels", GqlAliasKind::Edge | GqlAliasKind::Path | GqlAliasKind::Scalar) => {
                Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "labels() expects a node alias".to_string(),
                    args[0].span.clone(),
                ))
            }
            ("type", GqlAliasKind::Node | GqlAliasKind::Path | GqlAliasKind::Scalar) => {
                Err(gql_semantic_error(
                    GqlSemanticErrorCode::InvalidReturnExpression,
                    "type() expects an edge alias".to_string(),
                    args[0].span.clone(),
                ))
            }
            (
                "length" | "nodes" | "relationships" | "nodeids" | "edgeids",
                GqlAliasKind::Node | GqlAliasKind::Edge | GqlAliasKind::Scalar,
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
                    if binding.kind == GqlAliasKind::Scalar {
                        return Ok(());
                    }
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
                    "startnode" | "endnode" | "nodes" | "relationships"
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
                if is_scalar_function(&function) {
                    if let ExprKind::FunctionCall { args, .. } = &expr.kind {
                        for arg in args {
                            self.reject_statically_element_property_value(arg)?;
                        }
                    }
                }
                Ok(())
            }
            ExprKind::AggregateCall { name_span, .. } => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "aggregate functions cannot be used as property values".to_string(),
                name_span.clone(),
            )),
            ExprKind::ExistsSubquery(_) => Err(gql_semantic_error(
                GqlSemanticErrorCode::InvalidReturnExpression,
                "EXISTS subqueries cannot be used as property values".to_string(),
                expr.span.clone(),
            )),
            ExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand.as_ref() {
                    self.reject_statically_element_property_value(operand)?;
                }
                for branch in branches {
                    self.reject_statically_element_property_value(&branch.when)?;
                    self.reject_statically_element_property_value(&branch.then)?;
                }
                if let Some(else_expr) = else_expr.as_ref() {
                    self.reject_statically_element_property_value(else_expr)?;
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
            ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
                self.reject_statically_element_property_value(expr)
            }
            ExprKind::Binary { left, right, .. } => {
                self.reject_statically_element_property_value(left)?;
                self.reject_statically_element_property_value(right)
            }
            ExprKind::PropertyAccess { .. } | ExprKind::Literal(_) | ExprKind::Parameter(_) => {
                Ok(())
            }
        }
    }

    fn reject_commit_dependent_created_source_value(&self, expr: &Expr) -> Result<(), EngineError> {
        match &expr.kind {
            ExprKind::FunctionCall { name, args } => {
                let commit_dependent = matches!(
                    GqlMetadataFunction::from_lower(&name.name.to_ascii_lowercase()),
                    Some(
                        GqlMetadataFunction::Id
                            | GqlMetadataFunction::CreatedAt
                            | GqlMetadataFunction::UpdatedAt
                    )
                );
                if commit_dependent {
                    let target = edge_endpoint_id_call(name, args)
                        .map(|(_, endpoint_arg)| endpoint_arg)
                        .or_else(|| args.first())
                        .and_then(variable_name);
                    if let Some(alias) = target {
                        if self.aliases.get(alias).is_some_and(|binding| {
                            matches!(
                                binding.origin,
                                GqlAliasOrigin::Created | GqlAliasOrigin::Merged
                            ) && matches!(binding.kind, GqlAliasKind::Node | GqlAliasKind::Edge)
                        }) {
                            return Err(gql_semantic_error(
                                GqlSemanticErrorCode::InvalidReturnExpression,
                                format!(
                                    "MERGE action expression cannot read commit-assigned {}() from alias '{}' before commit",
                                    name.name, alias
                                ),
                                name.span.clone(),
                            ));
                        }
                    }
                }
                for arg in args {
                    self.reject_commit_dependent_created_source_value(arg)?;
                }
                Ok(())
            }
            ExprKind::PropertyAccess { object, .. } => {
                self.reject_commit_dependent_created_source_value(object)
            }
            ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
                self.reject_commit_dependent_created_source_value(expr)
            }
            ExprKind::Binary { left, right, .. } => {
                self.reject_commit_dependent_created_source_value(left)?;
                self.reject_commit_dependent_created_source_value(right)
            }
            ExprKind::Case {
                operand,
                branches,
                else_expr,
            } => {
                if let Some(operand) = operand.as_ref() {
                    self.reject_commit_dependent_created_source_value(operand)?;
                }
                for branch in branches {
                    self.reject_commit_dependent_created_source_value(&branch.when)?;
                    self.reject_commit_dependent_created_source_value(&branch.then)?;
                }
                if let Some(else_expr) = else_expr.as_ref() {
                    self.reject_commit_dependent_created_source_value(else_expr)?;
                }
                Ok(())
            }
            ExprKind::List(items) => {
                for item in items {
                    self.reject_commit_dependent_created_source_value(item)?;
                }
                Ok(())
            }
            ExprKind::Map(map) => {
                for entry in &map.entries {
                    self.reject_commit_dependent_created_source_value(&entry.value)?;
                }
                Ok(())
            }
            ExprKind::AggregateCall { .. }
            | ExprKind::ExistsSubquery(_)
            | ExprKind::Literal(_)
            | ExprKind::Parameter(_)
            | ExprKind::Variable(_) => Ok(()),
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

    fn insert_merged_alias(
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
                format!("merged alias '{}' is already bound", ident.name),
                ident.span.clone(),
            ));
        }
        self.aliases.insert(
            ident.name.clone(),
            GqlMutationAliasBinding {
                name: ident.name.clone(),
                kind,
                origin: GqlAliasOrigin::Merged,
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
    name == DIRECT_NODE_ALIAS
        || name == DIRECT_EDGE_ALIAS
        || name.starts_with("__gql_")
        || name.starts_with("__og_")
}

fn is_graph_function(function: &str) -> bool {
    GqlMetadataFunction::from_lower(function).is_some()
        || GqlEndpointFunction::from_lower(function).is_some()
        || matches!(
            function,
            "labels" | "type" | "length" | "nodes" | "relationships" | "nodeids" | "edgeids"
        )
}

fn is_scalar_function(function: &str) -> bool {
    matches!(
        function,
        "coalesce"
            | "tostring"
            | "tointeger"
            | "tofloat"
            | "abs"
            | "floor"
            | "ceil"
            | "round"
            | "lower"
            | "upper"
            | "trim"
            | "substring"
            | "size"
            | "head"
            | "last"
    )
}

fn validate_scalar_function_arity(
    function: &str,
    name: &Ident,
    arg_count: usize,
) -> Result<(), EngineError> {
    let valid = match function {
        "coalesce" => arg_count >= 1,
        "substring" => matches!(arg_count, 2 | 3),
        "tostring" | "tointeger" | "tofloat" | "abs" | "floor" | "ceil" | "round" | "lower"
        | "upper" | "trim" | "size" | "head" | "last" => arg_count == 1,
        _ => false,
    };
    if valid {
        return Ok(());
    }
    let expected = match function {
        "coalesce" => "at least one argument",
        "substring" => "two or three arguments",
        _ => "exactly one argument",
    };
    Err(gql_semantic_error(
        GqlSemanticErrorCode::InvalidReturnExpression,
        format!("function '{}' expects {expected}", name.name),
        name.span.clone(),
    ))
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

fn path_property_access_error(property: &str, span: &SourceSpan) -> EngineError {
    gql_semantic_error(
        GqlSemanticErrorCode::InvalidPropertyAccess,
        format!(
            "paths do not have properties; use length(p), nodeIds(p), or edgeIds(p) instead of '.{property}'"
        ),
        span.clone(),
    )
}

/// Recognizes the `id(startNode(r))` / `id(endNode(r))` shape: an `id` call whose single
/// argument is an endpoint function over a single bound variable. Returns the endpoint
/// function and the inner variable expression; the caller checks the alias kind.
pub(crate) fn edge_endpoint_id_call<'a>(
    name: &Ident,
    args: &'a [Expr],
) -> Option<(GqlEndpointFunction, &'a Expr)> {
    if !name.name.eq_ignore_ascii_case("id") || args.len() != 1 {
        return None;
    }
    let ExprKind::FunctionCall {
        name: inner,
        args: inner_args,
    } = &args[0].kind
    else {
        return None;
    };
    let endpoint = GqlEndpointFunction::from_lower(&inner.name.to_ascii_lowercase())?;
    if inner_args.len() != 1 {
        return None;
    }
    variable_name(&inner_args[0])?;
    Some((endpoint, &inner_args[0]))
}

pub(crate) fn variable_name(expr: &Expr) -> Option<&str> {
    match &expr.kind {
        ExprKind::Variable(name) => Some(name.as_str()),
        _ => None,
    }
}

fn mutation_statement_has_read_prefix(statement: &GqlMutationStatement) -> bool {
    statement.read_prefix_pipeline.is_some() || !statement.read_prefix.is_empty()
}

fn synthetic_read_prefix_query(statement: &GqlMutationStatement) -> GqlQuery {
    let span = &statement.span;
    let return_projection = GqlProjectionClause {
        kind: GqlProjectionKind::Return,
        distinct: false,
        distinct_span: None,
        body: ReturnBody::All(span.clone()),
        where_clause: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
        span: span.clone(),
    };
    let pipeline = if let Some(prefix) = statement.read_prefix_pipeline.as_ref() {
        let mut pipeline = prefix.clone();
        pipeline
            .clauses
            .push(GqlPipelineClause::Projection(return_projection.clone()));
        pipeline.span = span.clone();
        pipeline
    } else {
        GqlReadPipeline {
            clauses: vec![
                GqlPipelineClause::Match(statement.read_prefix.clone()),
                GqlPipelineClause::Projection(return_projection.clone()),
            ],
            union_branches: Vec::new(),
            span: span.clone(),
        }
    };
    GqlQuery {
        match_clauses: statement.read_prefix.clone(),
        return_clause: ReturnClause {
            body: ReturnBody::All(span.clone()),
            distinct: false,
            distinct_span: None,
            span: span.clone(),
        },
        order_by: Vec::new(),
        skip: None,
        limit: None,
        pipeline,
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
    for alias in &plan.aliases.user_order {
        if aliases.contains_key(alias) {
            continue;
        }
        let Some(binding) = plan.aliases.get(alias) else {
            continue;
        };
        aliases.insert(
            alias.clone(),
            GqlMutationAliasBinding {
                name: alias.clone(),
                kind: binding.kind,
                origin: GqlAliasOrigin::ReadPrefix,
                nullable: false,
                span: binding.span.clone(),
            },
        );
        user_order.push(alias.clone());
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

fn kind_name(kind: GqlAliasKind) -> &'static str {
    match kind {
        GqlAliasKind::Node => "node",
        GqlAliasKind::Edge => "edge",
        GqlAliasKind::Path => "path",
        GqlAliasKind::Scalar => "scalar",
    }
}

fn return_body_items(body: &ReturnBody) -> Option<&[ReturnItem]> {
    match body {
        ReturnBody::All(_) => None,
        ReturnBody::AllAndItems { items, .. } | ReturnBody::Items(items) => Some(items),
    }
}

fn projection_body_contains_aggregate(body: &ReturnBody) -> bool {
    return_body_items(body)
        .map(|items| items.iter().any(|item| expr_contains_aggregate(&item.expr)))
        .unwrap_or(false)
}

fn gql_read_pipeline_contains_aggregate(pipeline: &GqlReadPipeline) -> bool {
    pipeline
        .clauses
        .iter()
        .any(gql_pipeline_clause_contains_aggregate)
        || pipeline.union_branches.iter().any(|branch| {
            branch
                .clauses
                .iter()
                .any(gql_pipeline_clause_contains_aggregate)
        })
}

fn gql_pipeline_clause_contains_aggregate(clause: &GqlPipelineClause) -> bool {
    match clause {
        GqlPipelineClause::Match(clauses) => clauses.iter().any(|clause| {
            clause
                .where_clause
                .as_ref()
                .is_some_and(expr_contains_aggregate)
                || clause.patterns.iter().any(gql_pattern_contains_aggregate)
        }),
        GqlPipelineClause::ShortestPath(_) => false,
        GqlPipelineClause::Call(call) => gql_read_pipeline_contains_aggregate(&call.pipeline),
        GqlPipelineClause::Projection(projection) => {
            projection_body_contains_aggregate(&projection.body)
                || projection
                    .where_clause
                    .as_ref()
                    .is_some_and(expr_contains_aggregate)
                || projection
                    .order_by
                    .iter()
                    .any(|item| expr_contains_aggregate(&item.expr))
                || projection
                    .skip
                    .as_ref()
                    .is_some_and(expr_contains_aggregate)
                || projection
                    .limit
                    .as_ref()
                    .is_some_and(expr_contains_aggregate)
        }
    }
}

fn gql_pattern_contains_aggregate(pattern: &Pattern) -> bool {
    pattern
        .start
        .properties
        .as_ref()
        .is_some_and(gql_map_contains_aggregate)
        || pattern.chains.iter().any(|chain| {
            chain
                .relationship
                .properties
                .as_ref()
                .is_some_and(gql_map_contains_aggregate)
                || chain
                    .node
                    .properties
                    .as_ref()
                    .is_some_and(gql_map_contains_aggregate)
        })
}

fn gql_map_contains_aggregate(map: &MapLiteral) -> bool {
    map.entries
        .iter()
        .any(|entry| expr_contains_aggregate(&entry.value))
}

fn expr_contains_aggregate(expr: &Expr) -> bool {
    match &expr.kind {
        ExprKind::AggregateCall { .. } => true,
        ExprKind::ExistsSubquery(pipeline) => gql_read_pipeline_contains_aggregate(pipeline),
        ExprKind::PropertyAccess { object, .. } => expr_contains_aggregate(object),
        ExprKind::Unary { expr, .. } | ExprKind::IsNull { expr, .. } => {
            expr_contains_aggregate(expr)
        }
        ExprKind::Binary { left, right, .. } => {
            expr_contains_aggregate(left) || expr_contains_aggregate(right)
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::List(args) => {
            args.iter().any(expr_contains_aggregate)
        }
        ExprKind::Case {
            operand,
            branches,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| expr_contains_aggregate(expr))
                || branches.iter().any(|branch| {
                    expr_contains_aggregate(&branch.when) || expr_contains_aggregate(&branch.then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| expr_contains_aggregate(expr))
        }
        ExprKind::Map(map) => map
            .entries
            .iter()
            .any(|entry| expr_contains_aggregate(&entry.value)),
        ExprKind::Literal(_) | ExprKind::Parameter(_) | ExprKind::Variable(_) => false,
    }
}

fn insert_projection_alias(
    aliases: &mut GqlAliasTable,
    output_aliases: &mut Vec<GqlProjectionAlias>,
    seen: &mut BTreeSet<String>,
    name: String,
    kind: GqlAliasKind,
    span: SourceSpan,
) -> Result<(), EngineError> {
    if is_reserved_user_alias(&name) {
        return Err(gql_semantic_error(
            GqlSemanticErrorCode::DuplicateAlias,
            format!("'{name}' is reserved for internal GQL projection"),
            span,
        ));
    }
    if !seen.insert(name.clone()) {
        return Err(gql_semantic_error(
            GqlSemanticErrorCode::DuplicateAlias,
            format!("duplicate projection alias '{name}'"),
            span,
        ));
    }
    aliases.by_name.insert(
        name.clone(),
        GqlAliasBinding {
            name: name.clone(),
            kind,
            span: span.clone(),
            user_visible: true,
        },
    );
    aliases.user_order.push(name.clone());
    output_aliases.push(GqlProjectionAlias { name, kind, span });
    Ok(())
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
        ExprKind::AggregateCall {
            function,
            distinct,
            arg,
            ..
        } => {
            let name = match function {
                AggregateFunction::Count => "count",
                AggregateFunction::Sum => "sum",
                AggregateFunction::Avg => "avg",
                AggregateFunction::Min => "min",
                AggregateFunction::Max => "max",
                AggregateFunction::Collect => "collect",
            };
            let arg = arg
                .as_ref()
                .map(|expr| expression_output_name(expr))
                .unwrap_or_else(|| "*".to_string());
            let distinct = if *distinct { "DISTINCT " } else { "" };
            format!("{name}({distinct}{arg})")
        }
        ExprKind::Parameter(name) => format!("${name}"),
        ExprKind::Literal(Literal::Null) => "null".to_string(),
        ExprKind::Literal(Literal::Bool(value)) => value.to_string(),
        ExprKind::Literal(Literal::Int(value)) => value.to_string(),
        ExprKind::Literal(Literal::Float(value)) => value.to_string(),
        ExprKind::Literal(Literal::String(value)) => value.clone(),
        ExprKind::List(_) => "list".to_string(),
        ExprKind::Map(_) => "map".to_string(),
        ExprKind::Unary { .. }
        | ExprKind::Binary { .. }
        | ExprKind::IsNull { .. }
        | ExprKind::Case { .. }
        | ExprKind::ExistsSubquery(_) => "expr".to_string(),
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

    fn expect_semantic_code(err: EngineError, code: GqlSemanticErrorCode) {
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
    fn with_preserves_and_renames_graph_aliases() {
        let preserved = bind("MATCH (n) WITH n RETURN n").unwrap();
        assert_eq!(preserved.aliases.user_order, vec!["n"]);
        assert_eq!(preserved.aliases.get("n").unwrap().kind, GqlAliasKind::Node);

        let renamed = bind("MATCH (n) WITH n AS x RETURN x").unwrap();
        assert_eq!(renamed.aliases.user_order, vec!["x"]);
        assert_eq!(renamed.aliases.get("x").unwrap().kind, GqlAliasKind::Node);
        assert!(!renamed.aliases.contains("n"));

        let dropped = bind("MATCH (n) WITH n AS x RETURN n")
            .expect_err("dropped aliases should be hidden after WITH");
        assert!(matches!(
            dropped,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::UnknownVariable,
                ..
            }
        ));
    }

    #[test]
    fn with_creates_scalar_aliases_usable_in_post_projection_expressions() {
        let plan = bind(
            "MATCH (n) WITH n.name AS name WHERE name STARTS WITH 'a' RETURN name ORDER BY name",
        )
        .unwrap();
        assert_eq!(plan.aliases.user_order, vec!["name"]);
        assert_eq!(plan.aliases.get("name").unwrap().kind, GqlAliasKind::Scalar);
        let GqlReturnPlan::Items(items) = plan.returns else {
            panic!("expected explicit RETURN");
        };
        assert_eq!(items[0].output_name, "name");
    }

    #[test]
    fn with_rejects_scalar_aliases_as_pattern_variables() {
        let err = bind("MATCH (n) WITH n.name AS name MATCH (name) RETURN name")
            .expect_err("scalar alias cannot seed a node pattern");
        assert!(matches!(
            err,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::DuplicateAlias,
                ..
            }
        ));
    }

    #[test]
    fn later_match_after_with_binds_against_projected_scope() {
        let plan = bind("MATCH (n) WITH n MATCH (n)-[:R]->(m) RETURN m").unwrap();
        assert_eq!(plan.aliases.user_order, vec!["n", "m"]);
        assert_eq!(plan.aliases.get("n").unwrap().kind, GqlAliasKind::Node);
        assert_eq!(plan.aliases.get("m").unwrap().kind, GqlAliasKind::Node);

        let optional = bind("MATCH (n) WITH n OPTIONAL MATCH (n)-[:R]->(m) RETURN m").unwrap();
        let GqlBoundPipelineClause::Match(later_match) = &optional.pipeline.clauses[2] else {
            panic!("expected later MATCH after WITH");
        };
        assert_eq!(later_match.len(), 1);
        assert!(later_match[0].optional);
    }

    #[test]
    fn shortest_path_requires_prebound_node_endpoints_and_binds_path_alias() {
        let plan = bind(
            "MATCH (a) WITH a MATCH (b) WITH a, b \
             MATCH p = shortestPath((a)-[:R*1..3]->(b)) RETURN p",
        )
        .unwrap();
        assert_eq!(plan.aliases.get("p").unwrap().kind, GqlAliasKind::Path);
        let GqlBoundPipelineClause::ShortestPath(shortest) = &plan.pipeline.clauses[4] else {
            panic!("expected shortest-path stage");
        };
        assert_eq!(shortest.output_path_alias, "p");
        assert_eq!(shortest.from_alias, "a");
        assert_eq!(shortest.to_alias, "b");
        assert_eq!(shortest.min_hops, 1);
        assert_eq!(shortest.max_hops, 3);
    }

    #[test]
    fn shortest_path_rejects_broad_or_unbound_endpoints() {
        let err = bind("MATCH p = shortestPath((a)-[:R*1..3]->(b)) RETURN p")
            .expect_err("unbound endpoint aliases should fail");
        expect_semantic_code(err, GqlSemanticErrorCode::UnknownVariable);

        let err = bind(
            "MATCH (a) WITH a \
             MATCH p = shortestPath((a)-[:R*1..3]->(:Target {elementKey: 'b'})) RETURN p",
        )
        .expect_err("inline endpoint lookup should be rejected until specified");
        assert!(matches!(
            err,
            EngineError::GqlUnsupported { feature, .. }
                if feature == "shortest-path endpoint lookup"
        ));
    }

    #[test]
    fn with_star_preserves_visible_aliases_and_rejects_collisions() {
        let plan = bind("MATCH (a)-[r:R]->(b) WITH * RETURN *").unwrap();
        assert_eq!(plan.aliases.user_order, vec!["a", "r", "b"]);
        let GqlReturnPlan::Star {
            expanded_aliases, ..
        } = plan.returns
        else {
            panic!("expected RETURN *");
        };
        assert_eq!(expanded_aliases, vec!["a", "r", "b"]);

        let err = bind("MATCH (n) WITH *, n.name AS n RETURN n")
            .expect_err("WITH star collisions should be rejected");
        assert!(matches!(
            err,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::DuplicateAlias,
                ..
            }
        ));
    }

    #[test]
    fn reserved_internal_aliases_are_rejected() {
        for source in [
            "MATCH (n) RETURN n AS __og_union_order",
            "MATCH (n) WITH n AS __og_union_order RETURN __og_union_order",
        ] {
            let err = bind(source).expect_err("reserved internal alias should fail");
            assert!(
                matches!(
                    err,
                    EngineError::GqlSemantic {
                        code: GqlSemanticErrorCode::DuplicateAlias,
                        ..
                    }
                ),
                "unexpected error for {source}: {err:?}"
            );
        }
    }

    #[test]
    fn with_distinct_span_survives_semantic_binding() {
        let plan = bind("MATCH (n) WITH DISTINCT n RETURN n").unwrap();
        let GqlBoundPipelineClause::Projection(with) = &plan.pipeline.clauses[1] else {
            panic!("expected WITH projection");
        };
        assert!(with.distinct);
        assert!(with
            .distinct_span
            .as_ref()
            .is_some_and(|span| span.length > 0));
    }

    #[test]
    fn union_branches_bind_isolated_scopes_and_matching_columns() {
        let plan =
            bind("MATCH (n) RETURN n.name AS name UNION ALL MATCH (m) RETURN m.name AS name")
                .unwrap();
        assert_eq!(plan.pipeline.union_branches.len(), 1);
        assert_eq!(
            plan.pipeline.union_branches[0].modifier,
            GqlUnionModifier::All
        );
        assert_eq!(plan.aliases.user_order, vec!["n"]);
        assert!(!plan.aliases.contains("m"));
        let GqlReturnPlan::Items(items) = &plan.pipeline.union_branches[0].returns else {
            panic!("expected branch RETURN items");
        };
        assert_eq!(items[0].output_name, "name");
    }

    #[test]
    fn union_branch_column_mismatches_are_semantic_errors() {
        let count = bind("MATCH (n) RETURN n AS x UNION MATCH (m) RETURN m AS x, id(m) AS id")
            .expect_err("column count mismatch should fail");
        assert!(matches!(
            count,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidReturnExpression,
                ..
            }
        ));

        let names = bind("MATCH (n) RETURN n AS x UNION MATCH (m) RETURN m AS y")
            .expect_err("column name mismatch should fail");
        assert!(matches!(
            names,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidReturnExpression,
                ..
            }
        ));
    }

    #[test]
    fn aggregate_calls_are_valid_only_in_projection_contexts() {
        bind("MATCH (n) RETURN count(*) + 1 AS total ORDER BY count(*) DESC").unwrap();
        bind("MATCH (n) WITH count(*) AS c WHERE c > 1 RETURN c").unwrap();

        let plan =
            bind("MATCH (n) RETURN count(DISTINCT n.kind), collect(DISTINCT n.kind)").unwrap();
        let GqlReturnPlan::Items(items) = plan.returns else {
            panic!("expected aggregate return items");
        };
        assert_eq!(items[0].output_name, "count(DISTINCT n.kind)");
        assert_eq!(items[1].output_name, "collect(DISTINCT n.kind)");

        for source in [
            "MATCH (n) WHERE count(*) > 1 RETURN n",
            "MATCH (n) WITH n WHERE count(*) > 1 RETURN n",
            "MATCH (n {score: count(*)}) RETURN n",
            "MATCH (n) RETURN count(count(*))",
            "MATCH (n) WITH *, count(*) AS c RETURN c",
            "MATCH (n) RETURN * ORDER BY count(*)",
            "MATCH (n) WITH * ORDER BY count(*) RETURN n",
        ] {
            let err = bind(source).expect_err("aggregate placement should be rejected");
            assert!(
                matches!(
                    err,
                    EngineError::GqlSemantic {
                        code: GqlSemanticErrorCode::InvalidReturnExpression,
                        ..
                    }
                ),
                "unexpected error for {source}: {err:?}"
            );
        }
    }

    #[test]
    fn mutation_rejects_aggregate_expressions() {
        for source in [
            "CREATE (n:Person {elementKey: 'a'}) RETURN count(*)",
            "CREATE (n:Person {elementKey: 'a', score: count(*)})",
            "MATCH (n:Person {elementKey: 'a'}) SET n.score = count(*)",
        ] {
            let err = bind_mut(source).expect_err("mutation aggregate should be rejected");
            expect_mut_semantic_code(err, GqlSemanticErrorCode::InvalidReturnExpression);
        }
    }

    #[test]
    fn graph_function_kind_validation_survives_with_scope_transitions() {
        bind("MATCH p = (a)-[:KNOWS]->(b) WITH p AS x RETURN length(x)").unwrap();
        let err = bind("MATCH (n) WITH n.name AS name RETURN labels(name)")
            .expect_err("labels() should reject scalar aliases");
        assert!(matches!(
            err,
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidReturnExpression,
                ..
            }
        ));
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
            "MATCH (a:Person {elementKey: 'a'}) OPTIONAL MATCH p = (a)-[r:KNOWS*1..1]->(b) CREATE (c:Person {elementKey: 'c'})-[e:LINK]->(a) RETURN a, p, b, c, e",
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
    fn mutation_binds_keyed_node_merge_and_relationship_merge() {
        let node = bind_mut(
            "MERGE (n:Person {elementKey: 'ada'}) ON CREATE SET n.status = 'new' ON MATCH SET n.status = 'seen' RETURN n",
        )
        .unwrap();
        let n = node.aliases.get("n").unwrap();
        assert_eq!(n.kind, GqlAliasKind::Node);
        assert_eq!(n.origin, GqlAliasOrigin::Merged);
        let [GqlBoundMutationClause::Merge(merge)] = node.clauses.as_slice() else {
            panic!("expected node MERGE clause");
        };
        assert_eq!(merge.on_create.items.len(), 1);
        assert_eq!(merge.on_match.items.len(), 1);
        assert!(matches!(
            &merge.pattern,
            GqlBoundMergePattern::Node(node) if node.alias == "n" && node.label.name == "Person"
        ));
        bind_mut(
            "MERGE (n:Person {elementKey: 'ada'}) ON MATCH SET n.count = coalesce(n.count, 0) + 1",
        )
        .unwrap();
        // Property dot reads of the merged alias are plain user properties now and are
        // no longer commit-dependent.
        bind_mut(
            "MERGE (n:Person {elementKey: 'ada'}) ON MATCH SET n.source_created = n.created_at",
        )
        .unwrap();
        for source in [
            "MERGE (n:Person {elementKey: 'ada'}) ON CREATE SET n.source_id = id(n)",
            "MERGE (n:Person {elementKey: 'ada'}) ON MATCH SET n.source_created = createdAt(n)",
            "MERGE (n:Person {elementKey: 'ada'}) ON MATCH SET n.touched = updatedAt(n)",
        ] {
            let err = bind_mut(source)
                .expect_err("MERGE actions should reject commit-dependent metadata functions");
            match err {
                EngineError::GqlSemantic {
                    code: GqlSemanticErrorCode::InvalidReturnExpression,
                    message,
                    ..
                } => assert!(
                    message.contains("commit-assigned"),
                    "source: {source}, message: {message}"
                ),
                other => panic!("expected commit-dependent error for {source}, got {other:?}"),
            }
        }

        let relationship = bind_mut(
            "MATCH (a:Person) MATCH (b:Person) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.status = 'new' RETURN r",
        )
        .unwrap();
        let r = relationship.aliases.get("r").unwrap();
        assert_eq!(r.kind, GqlAliasKind::Edge);
        assert_eq!(r.origin, GqlAliasOrigin::Merged);
        let [GqlBoundMutationClause::Merge(merge)] = relationship.clauses.as_slice() else {
            panic!("expected relationship MERGE clause");
        };
        assert!(matches!(
            &merge.pattern,
            GqlBoundMergePattern::Relationship(rel)
                if rel.alias == "r" && rel.from_alias == "a" && rel.to_alias == "b"
                    && rel.rel_type.name == "KNOWS"
        ));
        // Edge dot access is a plain user property read now.
        bind_mut(
            "MATCH (a:Person) MATCH (b:Person) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET r.source_from = r.from",
        )
        .unwrap();
        for source in [
            "MATCH (a:Person) MATCH (b:Person) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET r.source_id = id(r)",
            "MATCH (a:Person) MATCH (b:Person) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET r.source_from = id(startNode(r))",
        ] {
            let err = bind_mut(source)
                .expect_err("MERGE relationship actions should reject commit-assigned edge metadata");
            assert!(matches!(
                err,
                EngineError::GqlSemantic {
                    code: GqlSemanticErrorCode::InvalidReturnExpression,
                    ..
                }
            ));
        }
    }

    #[test]
    fn order_by_endpoint_id_over_projection_alias_errors_not_panics() {
        // Regression: id(startNode(q)) where q is a RETURN projection alias used to
        // panic on the pattern-alias lookup instead of returning a semantic error.
        for source in [
            "MATCH (a)-[r:KNOWS]->(b) RETURN r AS q ORDER BY id(startNode(q))",
            "MATCH (a)-[r:KNOWS]->(b) RETURN r AS q ORDER BY id(endNode(q))",
        ] {
            let err = bind(source).expect_err("projection alias is not a function target");
            expect_semantic_code(err, GqlSemanticErrorCode::UnknownVariable);
        }
    }

    #[test]
    fn mutation_rejects_unsupported_merge_shapes() {
        for (source, expected_feature) in [
            ("MERGE (n {elementKey: 'a'})", "unlabeled node MERGE"),
            (
                "MERGE (n:Person:Employee {elementKey: 'a'})",
                "multi-label node MERGE",
            ),
            ("MERGE (n:Person)", "unkeyed node MERGE"),
            (
                "MERGE (n:Person {id: 'a'})",
                "node MERGE non-key identity property",
            ),
            (
                "MERGE (n:Person {key: 'a'})",
                "node MERGE non-key identity property",
            ),
            (
                "MERGE (n:Person {elementKey: 'a', name: 'Ada'})",
                "node MERGE property-map identity",
            ),
            (
                "MATCH (a:Person) MATCH (b:Person) MERGE (a)-[r:KNOWS {since: 2026}]->(b)",
                "relationship MERGE properties",
            ),
            (
                "MATCH (a:Person) MATCH (b:Person) MERGE (a:Person)-[r:KNOWS]->(b)",
                "relationship MERGE endpoint pattern",
            ),
        ] {
            let err = bind_mut(source).expect_err("unsupported MERGE shape should fail");
            assert!(
                matches!(err, EngineError::GqlUnsupported { ref feature, .. } if feature == expected_feature),
                "expected unsupported {expected_feature} for {source}, got {err:?}"
            );
        }

        let unbound = bind_mut("MERGE (a)-[r:KNOWS]->(b)")
            .expect_err("unbound relationship endpoints should fail");
        expect_mut_semantic_code(unbound, GqlSemanticErrorCode::UnknownVariable);
    }

    #[test]
    fn mutation_rejects_create_alias_collisions_and_invalid_create_shapes() {
        let duplicate = bind_mut("CREATE (n:Person {elementKey: 'a'}), (n:Person {elementKey: 'b'})")
            .expect_err("duplicate created alias should fail");
        expect_mut_semantic_code(duplicate, GqlSemanticErrorCode::DuplicateAlias);

        let bound_endpoint =
            bind_mut("MATCH (n:Person {elementKey: 'a'}) CREATE (n:Other {elementKey: 'b'})")
                .expect_err("bound endpoint with labels should fail");
        expect_mut_semantic_code(
            bound_endpoint,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let bound_endpoint_with_props =
            bind_mut("MATCH (n:Person {elementKey: 'a'}) CREATE (n {elementKey: 'b'})")
                .expect_err("bound endpoint with properties should fail");
        expect_mut_semantic_code(
            bound_endpoint_with_props,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let standalone_existing = bind_mut("MATCH (n:Person {elementKey: 'a'}) CREATE (n)")
            .expect_err("standalone existing CREATE endpoint should fail");
        expect_mut_semantic_code(
            standalone_existing,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let cross_pattern_created =
            bind_mut("CREATE (a:Person {elementKey: 'a'}), (a)-[:R]->(b:Person {elementKey: 'b'})")
                .expect_err("created alias reuse across CREATE patterns should fail");
        expect_mut_semantic_code(cross_pattern_created, GqlSemanticErrorCode::DuplicateAlias);

        let no_label = bind_mut("CREATE (n {elementKey: 'a'})")
            .expect_err("new node without label should fail");
        expect_mut_semantic_code(no_label, GqlSemanticErrorCode::InvalidReturnExpression);

        let no_key = bind_mut("CREATE (n:Person {name: 'Ada'})")
            .expect_err("node map without elementKey should fail");
        match no_key {
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidReturnExpression,
                message,
                ..
            } => assert!(
                message.contains("must contain elementKey"),
                "message: {message}"
            ),
            other => panic!("expected missing elementKey error, got {other:?}"),
        }

        let no_map = bind_mut("CREATE (n:Person)")
            .expect_err("node pattern without a property map should fail");
        match no_map {
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidReturnExpression,
                message,
                ..
            } => assert!(
                message.contains("require a property map containing elementKey"),
                "message: {message}"
            ),
            other => panic!("expected missing property map error, got {other:?}"),
        }

        let path_create = bind_mut("CREATE p = (n:Person {elementKey: 'a'})")
            .expect_err("CREATE path assignment should fail");
        assert!(matches!(
            path_create,
            EngineError::GqlUnsupported { feature, .. } if feature == "CREATE path assignment"
        ));

        // Property-name reservations are lifted: formerly reserved names are plain
        // user properties in CREATE maps.
        bind_mut("CREATE (n:Person {elementKey: 'a', id: 1, key: 'b', updated_at: 2})").unwrap();
        bind_mut(
            "CREATE (a:Person {elementKey: 'a'})-[r:R {from: 1, valid_from: 2}]->(b:Person {elementKey: 'b'})",
        )
        .unwrap();

        // Kind-invalid metadata map keys still error.
        let edge_meta_on_node = bind_mut("CREATE (n:Person {elementKey: 'a', validFrom: 1})")
            .expect_err("edge-only metadata key on node map should fail");
        expect_mut_semantic_code(edge_meta_on_node, GqlSemanticErrorCode::InvalidPropertyAccess);

        let node_meta_on_edge = bind_mut(
            "CREATE (a:Person {elementKey: 'a'})-[r:R {elementKey: 'x'}]->(b:Person {elementKey: 'b'})",
        )
        .expect_err("node-only metadata key on edge map should fail");
        expect_mut_semantic_code(node_meta_on_edge, GqlSemanticErrorCode::InvalidPropertyAccess);
    }

    #[test]
    fn mutation_validates_relationship_create_shape() {
        for source in [
            "CREATE (a:Person {elementKey: 'a'})-[r]-(b:Person {elementKey: 'b'})",
            "CREATE (a:Person {elementKey: 'a'})-[r*1..1]->(b:Person {elementKey: 'b'})",
            "CREATE (a:Person {elementKey: 'a'})-[r:A|B]->(b:Person {elementKey: 'b'})",
            "CREATE (a:Person {elementKey: 'a'})-[r]->(b:Person {elementKey: 'b'})",
        ] {
            assert!(
                bind_mut(source).is_err(),
                "invalid relationship CREATE should fail: {source}"
            );
        }
        let ok =
            bind_mut("CREATE (a:Person {elementKey: 'a'})-[r:KNOWS]->(b:Person {elementKey: 'b'})")
                .unwrap();
        assert_eq!(ok.aliases.get("r").unwrap().kind, GqlAliasKind::Edge);

        let duplicate_edge = bind_mut(
            "CREATE (a:Person {elementKey: 'a'})-[r:R]->(b:Person {elementKey: 'b'})-[r:S]->(c:Person {elementKey: 'c'})",
        )
        .expect_err("duplicate relationship CREATE alias should fail");
        expect_mut_semantic_code(duplicate_edge, GqlSemanticErrorCode::DuplicateAlias);
    }

    #[test]
    fn mutation_rejects_created_alias_rhs_sources_but_allows_targets_and_return() {
        let ok = bind_mut("CREATE (n:Person {elementKey: 'a'}) SET n.name = 'Ada' RETURN n").unwrap();
        assert!(ok.aliases.contains_key("n"));

        let rhs = bind_mut("CREATE (n:Person {elementKey: 'a'}) SET n.name = n.key")
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
        bind_mut("MATCH (n:Person {elementKey: 'a'}) DETACH DELETE n").unwrap();

        let set_deleted = bind_mut("MATCH (a)-[r:KNOWS]->(b) DELETE r SET r.weight = 1")
            .expect_err("SET after DELETE of same alias should fail");
        expect_mut_semantic_code(set_deleted, GqlSemanticErrorCode::InvalidReturnExpression);

        let remove_deleted =
            bind_mut("MATCH (n:Person {elementKey: 'a'}) DETACH DELETE n REMOVE n.name")
            .expect_err("REMOVE after DETACH DELETE of same alias should fail");
        expect_mut_semantic_code(
            remove_deleted,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let detach_then_set_incident =
            bind_mut("MATCH (a)-[r:KNOWS]->(b) DETACH DELETE a SET weight(r) = 1")
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
            "MATCH (a)-[r:KNOWS]->(b) DETACH DELETE a CREATE (a)-[:NEXT]->(c:Person {elementKey: 'c'})",
        )
        .expect_err("CREATE endpoint deleted earlier should fail");
        expect_mut_semantic_code(
            detach_then_create_from_deleted,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let created_incident_deleted = bind_mut(
            "CREATE (a:Person {elementKey: 'a'})-[r:R]->(b:Person {elementKey: 'b'}) DETACH DELETE a SET r.weight = 1",
        )
        .expect_err("SET created edge after DETACH DELETE of endpoint should fail");
        expect_mut_semantic_code(
            created_incident_deleted,
            GqlSemanticErrorCode::InvalidReturnExpression,
        );

        let delete_node = bind_mut("MATCH (n:Person {elementKey: 'a'}) DELETE n")
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
    fn mutation_set_remove_lifted_reservations_and_metadata_lvalues() {
        // Property-name reservations are lifted: every dot SET/REMOVE target is a plain
        // user property, regardless of name.
        for source in [
            "MATCH (n:Person {elementKey: 'a'}) SET n.id = 1",
            "MATCH (n:Person {elementKey: 'a'}) SET n.key = 'b'",
            "MATCH (n:Person {elementKey: 'a'}) SET n.dense_vector = []",
            "MATCH (n:Person {elementKey: 'a'}) SET n.weight = 1.0",
            "MATCH (n:Person {elementKey: 'a'}) SET n.updated_at = 5",
            "MATCH (a)-[r:KNOWS]->(b) SET r.from = 1",
            "MATCH (a)-[r:KNOWS]->(b) SET r.type = 'X'",
            "MATCH (a)-[r:KNOWS]->(b) SET r.valid_from = 1",
            "MATCH (n:Person {elementKey: 'a'}) REMOVE n.id",
            "MATCH (a)-[r:KNOWS]->(b) REMOVE r.weight",
            "MATCH (a)-[r:KNOWS]->(b) REMOVE r.valid_from",
        ] {
            bind_mut(source).unwrap_or_else(|err| {
                panic!("lifted property reservation should bind: {source}: {err:?}")
            });
        }

        // Writable metadata uses function l-values.
        bind_mut("MATCH (n:Person {elementKey: 'a'}) SET weight(n) = 1.0").unwrap();
        bind_mut("MATCH (a)-[r:KNOWS]->(b) SET weight(r) = 1.0").unwrap();
        bind_mut("MATCH (a)-[r:KNOWS]->(b) SET validFrom(r) = 1").unwrap();
        bind_mut("MATCH (a)-[r:KNOWS]->(b) SET validTo(r) = 2").unwrap();

        // Read-only metadata functions are rejected as SET targets.
        for source in [
            "MATCH (n:Person {elementKey: 'a'}) SET id(n) = 1",
            "MATCH (n:Person {elementKey: 'a'}) SET elementKey(n) = 'b'",
            "MATCH (n:Person {elementKey: 'a'}) SET createdAt(n) = 1",
            "MATCH (n:Person {elementKey: 'a'}) SET updatedAt(n) = 1",
            "MATCH (a)-[r:KNOWS]->(b) SET id(r) = 1",
            "MATCH (a)-[r:KNOWS]->(b) SET createdAt(r) = 1",
            "MATCH (a)-[r:KNOWS]->(b) SET updatedAt(r) = 1",
        ] {
            match bind_mut(source).expect_err("read-only metadata SET target should fail") {
                EngineError::GqlSemantic {
                    code: GqlSemanticErrorCode::InvalidPropertyAccess,
                    message,
                    ..
                } => assert!(
                    message.contains("read-only metadata"),
                    "source: {source}, message: {message}"
                ),
                other => panic!("expected read-only metadata error for {source}, got {other:?}"),
            }
        }

        // Kind-invalid metadata SET targets are rejected.
        for source in [
            "MATCH (n:Person {elementKey: 'a'}) SET validFrom(n) = 1",
            "MATCH (n:Person {elementKey: 'a'}) SET validTo(n) = 1",
            "MATCH (a)-[r:KNOWS]->(b) SET elementKey(r) = 'x'",
        ] {
            let err = bind_mut(source).expect_err("kind-invalid metadata SET target should fail");
            expect_mut_semantic_code(err, GqlSemanticErrorCode::InvalidPropertyAccess);
        }

        // Unknown functions as SET targets are rejected.
        match bind_mut("MATCH (n:Person {elementKey: 'a'}) SET foo(n) = 1")
            .expect_err("unknown metadata function SET target should fail")
        {
            EngineError::GqlSemantic {
                code: GqlSemanticErrorCode::InvalidPropertyAccess,
                message,
                ..
            } => assert!(
                message.contains("unknown metadata function"),
                "message: {message}"
            ),
            other => panic!("expected unknown metadata function error, got {other:?}"),
        }

        bind_mut(
            "MATCH (a:Person {elementKey: 'a'}) OPTIONAL MATCH (a)-[r:KNOWS]->(b) SET b.name = 'optional'",
        )
        .unwrap();
    }
}
