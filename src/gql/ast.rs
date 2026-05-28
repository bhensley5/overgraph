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
    pub(crate) span: SourceSpan,
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
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GqlMutationStatement {
    pub(crate) read_prefix: Vec<MatchClause>,
    pub(crate) mutation_clauses: Vec<MutationClause>,
    pub(crate) return_tail: Option<MutationReturnTail>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum MutationClause {
    Create(CreateClause),
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
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ReturnBody {
    All(SourceSpan),
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
    List(Vec<Expr>),
    Map(MapLiteral),
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
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BinaryOp {
    Or,
    And,
    Eq,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    In,
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
