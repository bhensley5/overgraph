#![allow(dead_code)]

use crate::types::{GqlStatementKind, SourceSpan};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Ident {
    pub(crate) name: String,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlQuery {
    pub(crate) match_clauses: Vec<MatchClause>,
    pub(crate) return_clause: ReturnClause,
    pub(crate) order_by: Vec<OrderItem>,
    pub(crate) skip: Option<Expr>,
    pub(crate) limit: Option<Expr>,
    pub(crate) pipeline: GqlReadPipeline,
    pub(crate) span: SourceSpan,
}

impl GqlQuery {
    pub(crate) fn is_legacy_single_block(&self) -> bool {
        self.pipeline.union_branches.is_empty()
            && self.pipeline.is_legacy_single_block()
            && !self.return_clause.distinct
            && self.match_clauses
                == self
                    .pipeline
                    .leading_match_clauses()
                    .cloned()
                    .unwrap_or_default()
    }

    pub(crate) fn requires_deferred_pipeline_execution(&self) -> bool {
        !self.is_legacy_single_block()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlReadPipeline {
    pub(crate) clauses: Vec<GqlPipelineClause>,
    pub(crate) union_branches: Vec<GqlUnionBranch>,
    pub(crate) span: SourceSpan,
}

impl GqlReadPipeline {
    fn leading_match_clauses(&self) -> Option<&Vec<MatchClause>> {
        match self.clauses.first()? {
            GqlPipelineClause::Match(clauses) => Some(clauses),
            GqlPipelineClause::ShortestPath(_) => None,
            GqlPipelineClause::Call(_) => None,
            GqlPipelineClause::Projection(_) => None,
        }
    }

    fn is_legacy_single_block(&self) -> bool {
        matches!(
            self.clauses.as_slice(),
            [
                GqlPipelineClause::Match(_),
                GqlPipelineClause::Projection(GqlProjectionClause {
                    kind: GqlProjectionKind::Return,
                    distinct: false,
                    where_clause: None,
                    ..
                })
            ]
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlUnionBranch {
    pub(crate) modifier: GqlUnionModifier,
    pub(crate) clauses: Vec<GqlPipelineClause>,
    pub(crate) span: SourceSpan,
    pub(crate) union_span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlUnionModifier {
    Distinct,
    All,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlPipelineClause {
    Match(Vec<MatchClause>),
    ShortestPath(GqlShortestPathClause),
    Call(GqlCallSubquery),
    Projection(GqlProjectionClause),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlCallSubquery {
    pub(crate) pipeline: Box<GqlReadPipeline>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlShortestPathClause {
    pub(crate) optional: bool,
    pub(crate) output_path_alias: Ident,
    pub(crate) mode: GqlShortestPathMode,
    pub(crate) pattern: Pattern,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlShortestPathMode {
    One,
    All,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlProjectionClause {
    pub(crate) kind: GqlProjectionKind,
    pub(crate) distinct: bool,
    pub(crate) distinct_span: Option<SourceSpan>,
    pub(crate) body: ReturnBody,
    pub(crate) where_clause: Option<Expr>,
    pub(crate) order_by: Vec<OrderItem>,
    pub(crate) skip: Option<Expr>,
    pub(crate) limit: Option<Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlProjectionKind {
    With,
    Return,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlStatement {
    pub(crate) kind: GqlStatementKind,
    pub(crate) body: GqlStatementBody,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlStatementBody {
    Query(GqlQuery),
    Mutation(GqlMutationStatement),
    Schema(GqlSchemaStatement),
    Index(GqlIndexStatement),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlIndexStatement {
    Create(GqlCreatePropertyIndexStatement),
    Drop(GqlDropPropertyIndexStatement),
    Show(GqlShowPropertyIndexStatement),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlCreatePropertyIndexStatement {
    pub(crate) target: GqlPropertyIndexTarget,
    pub(crate) kind: GqlPropertyIndexKind,
    pub(crate) kind_span: SourceSpan,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlDropPropertyIndexStatement {
    pub(crate) target: GqlPropertyIndexTarget,
    pub(crate) kind: GqlPropertyIndexKind,
    pub(crate) kind_span: SourceSpan,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlShowPropertyIndexStatement {
    pub(crate) scope: GqlShowPropertyIndexScope,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlPropertyIndexTarget {
    Node {
        variable: Ident,
        label: GqlIndexName,
        fields: Vec<GqlPropertyIndexField>,
        field_list_span: SourceSpan,
        span: SourceSpan,
    },
    Edge {
        variable: Ident,
        label: GqlIndexName,
        fields: Vec<GqlPropertyIndexField>,
        field_list_span: SourceSpan,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlPropertyIndexField {
    Property {
        variable: Ident,
        key: GqlIndexName,
        span: SourceSpan,
    },
    Metadata {
        function: GqlPropertyIndexMetadataFunction,
        function_span: SourceSpan,
        variable: Ident,
        span: SourceSpan,
    },
    EndpointId {
        endpoint: GqlPropertyIndexEndpointFunction,
        function_span: SourceSpan,
        endpoint_span: SourceSpan,
        variable: Ident,
        span: SourceSpan,
    },
}

impl GqlPropertyIndexField {
    pub(crate) fn variable(&self) -> &Ident {
        match self {
            Self::Property { variable, .. }
            | Self::Metadata { variable, .. }
            | Self::EndpointId { variable, .. } => variable,
        }
    }
}

pub(crate) use super::metadata::{
    GqlEndpointFunction as GqlPropertyIndexEndpointFunction,
    GqlMetadataFunction as GqlPropertyIndexMetadataFunction,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlIndexName {
    pub(crate) name: String,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlPropertyIndexKind {
    Equality,
    Range,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlShowPropertyIndexScope {
    All,
    Node,
    Edge,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlSchemaStatement {
    AlterGraphType(GqlAlterGraphTypeStatement),
    DropCurrentGraphType { span: SourceSpan },
    CheckGraphType(GqlCheckGraphTypeStatement),
    Show(GqlShowSchemaStatement),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlAlterGraphTypeStatement {
    pub(crate) mode: GqlGraphTypeAlterMode,
    pub(crate) items: Vec<GqlSchemaItem>,
    pub(crate) drop_items: Vec<GqlSchemaDropItem>,
    pub(crate) options: Option<GqlSchemaLiteral>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlCheckGraphTypeStatement {
    pub(crate) mode: GqlGraphTypeCheckMode,
    pub(crate) items: Vec<GqlSchemaItem>,
    pub(crate) options: Option<GqlSchemaLiteral>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlShowSchemaStatement {
    pub(crate) kind: GqlShowSchemaKind,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlGraphTypeAlterMode {
    Add,
    Set,
    Drop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GqlGraphTypeCheckMode {
    Add,
    Set,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlSchemaItem {
    Node {
        label: GqlSchemaLabel,
        schema: GqlSchemaLiteral,
        span: SourceSpan,
    },
    Edge {
        label: GqlSchemaLabel,
        schema: GqlSchemaLiteral,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlSchemaDropItem {
    Node {
        label: GqlSchemaLabel,
        span: SourceSpan,
    },
    Edge {
        label: GqlSchemaLabel,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GqlSchemaLabel {
    pub(crate) name: String,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlSchemaLiteral {
    Map(MapLiteral),
    Parameter { name: String, span: SourceSpan },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum GqlShowSchemaKind {
    CurrentGraphType,
    NodeSchemas,
    EdgeSchemas,
    NodeSchema { label: GqlSchemaLabel },
    EdgeSchema { label: GqlSchemaLabel },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationStatement {
    pub(crate) read_prefix: Vec<MatchClause>,
    pub(crate) read_prefix_pipeline: Option<GqlReadPipeline>,
    pub(crate) mutation_clauses: Vec<MutationClause>,
    pub(crate) return_tail: Option<MutationReturnTail>,
    pub(crate) span: SourceSpan,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MutationClause {
    Create(CreateClause),
    Merge(MergeClause),
    Set(SetClause),
    Remove(RemoveClause),
    Delete(DeleteClause),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CreateClause {
    pub(crate) patterns: Vec<Pattern>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MergeClause {
    pub(crate) pattern: Pattern,
    pub(crate) on_create: Option<SetClause>,
    pub(crate) on_match: Option<SetClause>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SetClause {
    pub(crate) items: Vec<SetItem>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum SetItem {
    Property {
        alias: Ident,
        property: Ident,
        value: Expr,
        span: SourceSpan,
    },
    Metadata {
        function: Ident,
        alias: Ident,
        value: Expr,
        span: SourceSpan,
    },
    MapMerge {
        alias: Ident,
        value: Expr,
        span: SourceSpan,
    },
    NodeLabel {
        alias: Ident,
        label: Ident,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RemoveClause {
    pub(crate) items: Vec<RemoveItem>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RemoveItem {
    Property {
        alias: Ident,
        property: Ident,
        span: SourceSpan,
    },
    NodeLabel {
        alias: Ident,
        label: Ident,
        span: SourceSpan,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct DeleteClause {
    pub(crate) detach: bool,
    pub(crate) targets: Vec<Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MutationReturnTail {
    pub(crate) return_clause: ReturnClause,
    pub(crate) order_by: Vec<OrderItem>,
    pub(crate) skip: Option<Expr>,
    pub(crate) limit: Option<Expr>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MatchClause {
    pub(crate) optional: bool,
    pub(crate) patterns: Vec<Pattern>,
    pub(crate) where_clause: Option<Expr>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Pattern {
    pub(crate) path_variable: Option<Ident>,
    pub(crate) start: NodePattern,
    pub(crate) chains: Vec<PatternChain>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PatternChain {
    pub(crate) relationship: RelationshipPattern,
    pub(crate) node: NodePattern,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct NodePattern {
    pub(crate) variable: Option<Ident>,
    pub(crate) labels: Vec<Ident>,
    pub(crate) properties: Option<MapLiteral>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RelationshipPattern {
    pub(crate) variable: Option<Ident>,
    pub(crate) rel_types: Vec<Ident>,
    pub(crate) quantifier: Option<RelationshipQuantifier>,
    pub(crate) direction: RelationshipDirection,
    pub(crate) properties: Option<MapLiteral>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RelationshipQuantifier {
    pub(crate) min_hops: u8,
    pub(crate) max_hops: u8,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RelationshipDirection {
    LeftToRight,
    RightToLeft,
    Undirected,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReturnClause {
    pub(crate) body: ReturnBody,
    pub(crate) distinct: bool,
    pub(crate) distinct_span: Option<SourceSpan>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ReturnBody {
    All(SourceSpan),
    AllAndItems {
        star_span: SourceSpan,
        items: Vec<ReturnItem>,
    },
    Items(Vec<ReturnItem>),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReturnItem {
    pub(crate) expr: Expr,
    pub(crate) alias: Option<Ident>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct OrderItem {
    pub(crate) expr: Expr,
    pub(crate) direction: OrderDirection,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Expr {
    pub(crate) kind: ExprKind,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ExprKind {
    Literal(Literal),
    Parameter(String),
    Variable(String),
    PropertyAccess {
        object: Box<Expr>,
        property: Ident,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Binary {
        op: BinaryOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    FunctionCall {
        name: Ident,
        args: Vec<Expr>,
    },
    AggregateCall {
        function: AggregateFunction,
        distinct: bool,
        arg: Option<Box<Expr>>,
        name_span: SourceSpan,
    },
    ExistsSubquery(Box<GqlReadPipeline>),
    Case {
        operand: Option<Box<Expr>>,
        branches: Vec<CaseBranch>,
        else_expr: Option<Box<Expr>>,
    },
    List(Vec<Expr>),
    Map(MapLiteral),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CaseBranch {
    pub(crate) when: Expr,
    pub(crate) then: Expr,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Literal {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum UnaryOp {
    Not,
    Neg,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BinaryOp {
    Or,
    And,
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    StartsWith,
    EndsWith,
    Contains,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Collect,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MapLiteral {
    pub(crate) entries: Vec<MapEntry>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MapEntry {
    pub(crate) key: MapKey,
    pub(crate) value: Expr,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MapKey {
    pub(crate) name: String,
    pub(crate) span: SourceSpan,
}
