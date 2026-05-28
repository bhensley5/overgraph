#![allow(dead_code)]

use crate::error::EngineError;
use crate::gql::ast::*;
use crate::gql::lexer::{lex, Keyword, Token, TokenKind};
use crate::types::{GqlStatementKind, SourceSpan};

#[derive(Clone, Debug)]
pub(crate) struct GqlParseOptions {
    pub(crate) max_query_bytes: usize,
    pub(crate) max_ast_depth: usize,
    pub(crate) max_literal_items: usize,
}

impl Default for GqlParseOptions {
    fn default() -> Self {
        Self {
            max_query_bytes: 1_048_576,
            max_ast_depth: 256,
            max_literal_items: 10_000,
        }
    }
}

pub(crate) fn parse_query(query: &str, options: &GqlParseOptions) -> Result<GqlQuery, EngineError> {
    if options.max_ast_depth == 0 {
        return Err(EngineError::GqlParse {
            message: "max_ast_depth must be at least 1".to_string(),
            span: span_at_offset(query, 0, query.chars().next().map_or(0, char::len_utf8)),
        });
    }
    if query.len() > options.max_query_bytes {
        return Err(EngineError::GqlParse {
            message: format!(
                "query exceeds max_query_bytes of {} bytes",
                options.max_query_bytes
            ),
            span: span_at_offset(
                query,
                options.max_query_bytes,
                query.len() - options.max_query_bytes,
            ),
        });
    }

    let tokens = lex(query)?;
    Parser::new(query, tokens, options).parse_query()
}

pub(crate) fn parse_statement(
    source: &str,
    options: &GqlParseOptions,
) -> Result<GqlStatement, EngineError> {
    if options.max_ast_depth == 0 {
        return Err(EngineError::GqlParse {
            message: "max_ast_depth must be at least 1".to_string(),
            span: span_at_offset(source, 0, source.chars().next().map_or(0, char::len_utf8)),
        });
    }
    if source.len() > options.max_query_bytes {
        return Err(EngineError::GqlParse {
            message: format!(
                "query exceeds max_query_bytes of {} bytes",
                options.max_query_bytes
            ),
            span: span_at_offset(
                source,
                options.max_query_bytes,
                source.len() - options.max_query_bytes,
            ),
        });
    }

    let tokens = lex(source)?;
    Parser::new(source, tokens, options).parse_statement()
}

struct Parser<'a> {
    query: &'a str,
    tokens: Vec<Token>,
    pos: usize,
    options: &'a GqlParseOptions,
}

struct ParsedExpr {
    expr: Expr,
    ast_depth: usize,
}

struct ParsedMapLiteral {
    literal: MapLiteral,
    ast_depth: usize,
}

type RelationshipDetail = (
    Option<Ident>,
    Vec<Ident>,
    Option<RelationshipQuantifier>,
    Option<MapLiteral>,
);

impl<'a> Parser<'a> {
    fn new(query: &'a str, tokens: Vec<Token>, options: &'a GqlParseOptions) -> Self {
        Self {
            query,
            tokens,
            pos: 0,
            options,
        }
    }

    fn parse_statement(&mut self) -> Result<GqlStatement, EngineError> {
        if self.at_mutation_clause_start() {
            return self.parse_mutation_statement(Vec::new());
        }

        if self.at_match_clause_start() {
            let mut match_clauses = Vec::new();
            match_clauses.push(self.parse_match_clause()?);
            while self.at_match_clause_start() {
                match_clauses.push(self.parse_match_clause()?);
            }
            if self.at_mutation_clause_start() {
                return self.parse_mutation_statement(match_clauses);
            }
            let query = self.parse_query_tail(match_clauses)?;
            return Ok(GqlStatement {
                kind: GqlStatementKind::Query,
                span: query.span.clone(),
                body: GqlStatementBody::Query(query),
            });
        }

        self.reject_unsupported_clause()?;
        let query = self.parse_query()?;
        Ok(GqlStatement {
            kind: GqlStatementKind::Query,
            span: query.span.clone(),
            body: GqlStatementBody::Query(query),
        })
    }

    fn parse_query(&mut self) -> Result<GqlQuery, EngineError> {
        self.reject_unsupported_clause()?;
        let mut match_clauses = Vec::new();
        match_clauses.push(self.parse_match_clause()?);
        while self.at_match_clause_start() {
            match_clauses.push(self.parse_match_clause()?);
        }

        self.parse_query_tail(match_clauses)
    }

    fn parse_query_tail(
        &mut self,
        match_clauses: Vec<MatchClause>,
    ) -> Result<GqlQuery, EngineError> {
        self.reject_unsupported_clause()?;
        let return_clause = self.parse_return_clause()?;
        let order_by = if self.at_keyword(Keyword::Order) {
            self.parse_order_by()?
        } else {
            Vec::new()
        };
        let mut skip = None;
        if self.at_keyword(Keyword::Skip) || self.at_keyword(Keyword::Offset) {
            skip = Some(self.parse_skip_or_offset()?);
        }
        if self.at_keyword(Keyword::Skip) || self.at_keyword(Keyword::Offset) {
            return Err(self.parse_error_current("SKIP and OFFSET cannot both be specified"));
        }
        let limit = if self.consume_keyword(Keyword::Limit).is_some() {
            Some(self.parse_expression(0)?.expr)
        } else {
            None
        };

        if let Some(semicolon) = self.consume_if(|kind| matches!(kind, TokenKind::Semicolon)) {
            if !self.at_eof() {
                return Err(EngineError::GqlParse {
                    message: "multiple statements are not supported".to_string(),
                    span: semicolon.span,
                });
            }
        }

        self.reject_unsupported_clause()?;
        if !self.at_eof() {
            return Err(self.parse_error_current("unexpected token after query"));
        }

        let span = self.span_between(
            &match_clauses
                .first()
                .map(|clause| clause.span.clone())
                .unwrap_or_else(|| return_clause.span.clone()),
            &self.previous_non_eof_span(),
        );
        Ok(GqlQuery {
            match_clauses,
            return_clause,
            order_by,
            skip,
            limit,
            span,
        })
    }

    fn parse_mutation_statement(
        &mut self,
        read_prefix: Vec<MatchClause>,
    ) -> Result<GqlStatement, EngineError> {
        for clause in &read_prefix {
            if clause.patterns.len() != 1 {
                return Err(EngineError::GqlUnsupported {
                    feature: "comma-separated mutation read-prefix pattern lists".to_string(),
                    message: "mutation read-prefix MATCH clauses support exactly one pattern; use repeated MATCH clauses instead".to_string(),
                    span: clause.span.clone(),
                });
            }
        }

        let start_span = read_prefix
            .first()
            .map(|clause| clause.span.clone())
            .unwrap_or_else(|| self.current().span.clone());
        let mut mutation_clauses = Vec::new();
        while self.at_mutation_clause_start() {
            mutation_clauses.push(self.parse_mutation_clause()?);
            if self.at_match_clause_start() {
                return Err(EngineError::GqlUnsupported {
                    feature: "read-after-write matching".to_string(),
                    message: "MATCH and OPTIONAL MATCH clauses must appear before mutation clauses"
                        .to_string(),
                    span: self.current().span.clone(),
                });
            }
        }
        if mutation_clauses.is_empty() {
            return Err(self.parse_error_current("expected mutation clause"));
        }

        let return_tail = if self.at_keyword(Keyword::Return) {
            Some(self.parse_mutation_return_tail()?)
        } else {
            if self.at_keyword(Keyword::Order)
                || self.at_keyword(Keyword::Skip)
                || self.at_keyword(Keyword::Offset)
                || self.at_keyword(Keyword::Limit)
            {
                return Err(EngineError::GqlUnsupported {
                    feature: "mutation row operations without RETURN".to_string(),
                    message: "mutation ORDER BY, SKIP/OFFSET, and LIMIT require a RETURN tail"
                        .to_string(),
                    span: self.current().span.clone(),
                });
            }
            None
        };

        if let Some(semicolon) = self.consume_if(|kind| matches!(kind, TokenKind::Semicolon)) {
            if !self.at_eof() {
                return Err(EngineError::GqlParse {
                    message: "multiple statements are not supported".to_string(),
                    span: semicolon.span,
                });
            }
        }

        self.reject_unsupported_clause()?;
        if self.at_match_clause_start() {
            return Err(EngineError::GqlUnsupported {
                feature: "read-after-write matching".to_string(),
                message: "MATCH and OPTIONAL MATCH clauses must appear before mutation clauses"
                    .to_string(),
                span: self.current().span.clone(),
            });
        }
        if !self.at_eof() {
            return Err(self.parse_error_current("unexpected token after mutation statement"));
        }

        let span = self.span_between(&start_span, &self.previous_non_eof_span());
        let mutation = GqlMutationStatement {
            read_prefix,
            mutation_clauses,
            return_tail,
            span,
        };
        Ok(GqlStatement {
            kind: GqlStatementKind::Mutation,
            span: mutation.span.clone(),
            body: GqlStatementBody::Mutation(mutation),
        })
    }

    fn parse_mutation_clause(&mut self) -> Result<MutationClause, EngineError> {
        if self.at_keyword(Keyword::Create) {
            Ok(MutationClause::Create(self.parse_create_clause()?))
        } else if self.at_keyword(Keyword::Set) {
            Ok(MutationClause::Set(self.parse_set_clause()?))
        } else if self.at_keyword(Keyword::Remove) {
            Ok(MutationClause::Remove(self.parse_remove_clause()?))
        } else if self.at_keyword(Keyword::Delete) || self.at_keyword(Keyword::Detach) {
            Ok(MutationClause::Delete(self.parse_delete_clause()?))
        } else {
            Err(self.parse_error_current("expected mutation clause"))
        }
    }

    fn parse_create_clause(&mut self) -> Result<CreateClause, EngineError> {
        if self.create_clause_is_schema_ddl() {
            return Err(self
                .unsupported_current("schema/DDL", "schema and DDL statements are not supported"));
        }
        let start = self.expect_keyword(Keyword::Create, "expected CREATE clause")?;
        let mut patterns = Vec::new();
        patterns.push(self.parse_pattern()?);
        while self
            .consume_if(|kind| matches!(kind, TokenKind::Comma))
            .is_some()
        {
            patterns.push(self.parse_pattern()?);
        }
        let end = patterns
            .last()
            .map(|pattern| pattern.span.clone())
            .unwrap_or_else(|| start.span.clone());
        Ok(CreateClause {
            patterns,
            span: self.span_between(&start.span, &end),
        })
    }

    fn parse_set_clause(&mut self) -> Result<SetClause, EngineError> {
        let start = self.expect_keyword(Keyword::Set, "expected SET clause")?;
        let mut items = Vec::new();
        items.push(self.parse_set_item()?);
        while self
            .consume_if(|kind| matches!(kind, TokenKind::Comma))
            .is_some()
        {
            items.push(self.parse_set_item()?);
        }
        let end = set_item_span(items.last().expect("at least one SET item")).clone();
        Ok(SetClause {
            items,
            span: self.span_between(&start.span, &end),
        })
    }

    fn parse_set_item(&mut self) -> Result<SetItem, EngineError> {
        let alias = self.parse_ident("expected alias in SET item")?;
        if self
            .consume_if(|kind| matches!(kind, TokenKind::PlusEquals))
            .is_some()
        {
            let value = self.parse_expression(0)?.expr;
            let span = self.span_between(&alias.span, &value.span);
            return Ok(SetItem::MapMerge { alias, value, span });
        }
        if self
            .consume_if(|kind| matches!(kind, TokenKind::Colon))
            .is_some()
        {
            if self.at_kind(|kind| matches!(kind, TokenKind::Dollar)) {
                return Err(self.unsupported_current(
                    "dynamic labels",
                    "dynamic node labels are not supported",
                ));
            }
            let label = self.parse_ident("expected node label after ':'")?;
            let span = self.span_between(&alias.span, &label.span);
            return Ok(SetItem::NodeLabel { alias, label, span });
        }
        self.expect_kind(
            |kind| matches!(kind, TokenKind::Dot),
            "expected '.', ':', or '+=' in SET item",
        )?;
        let property = self.parse_property_ident("expected property name after '.'")?;
        self.expect_kind(
            |kind| matches!(kind, TokenKind::Equals),
            "expected '=' in SET property item",
        )?;
        let value = self.parse_expression(0)?.expr;
        let span = self.span_between(&alias.span, &value.span);
        Ok(SetItem::Property {
            alias,
            property,
            value,
            span,
        })
    }

    fn parse_remove_clause(&mut self) -> Result<RemoveClause, EngineError> {
        let start = self.expect_keyword(Keyword::Remove, "expected REMOVE clause")?;
        let mut items = Vec::new();
        items.push(self.parse_remove_item()?);
        while self
            .consume_if(|kind| matches!(kind, TokenKind::Comma))
            .is_some()
        {
            items.push(self.parse_remove_item()?);
        }
        let end = remove_item_span(items.last().expect("at least one REMOVE item")).clone();
        Ok(RemoveClause {
            items,
            span: self.span_between(&start.span, &end),
        })
    }

    fn parse_remove_item(&mut self) -> Result<RemoveItem, EngineError> {
        let alias = self.parse_ident("expected alias in REMOVE item")?;
        if self
            .consume_if(|kind| matches!(kind, TokenKind::Colon))
            .is_some()
        {
            if self.at_kind(|kind| matches!(kind, TokenKind::Dollar)) {
                return Err(self.unsupported_current(
                    "dynamic labels",
                    "dynamic node labels are not supported",
                ));
            }
            let label = self.parse_ident("expected node label after ':'")?;
            let span = self.span_between(&alias.span, &label.span);
            return Ok(RemoveItem::NodeLabel { alias, label, span });
        }
        self.expect_kind(
            |kind| matches!(kind, TokenKind::Dot),
            "expected '.' or ':' in REMOVE item",
        )?;
        let property = self.parse_property_ident("expected property name after '.'")?;
        let span = self.span_between(&alias.span, &property.span);
        Ok(RemoveItem::Property {
            alias,
            property,
            span,
        })
    }

    fn parse_delete_clause(&mut self) -> Result<DeleteClause, EngineError> {
        let (detach, start) = if let Some(detach) = self.consume_keyword(Keyword::Detach) {
            self.expect_keyword(Keyword::Delete, "expected DELETE after DETACH")?;
            (true, detach.span)
        } else {
            let delete = self.expect_keyword(Keyword::Delete, "expected DELETE clause")?;
            (false, delete.span)
        };
        let mut targets = Vec::new();
        targets.push(self.parse_expression(0)?.expr);
        while self
            .consume_if(|kind| matches!(kind, TokenKind::Comma))
            .is_some()
        {
            targets.push(self.parse_expression(0)?.expr);
        }
        let end = targets
            .last()
            .map(|target| target.span.clone())
            .unwrap_or_else(|| start.clone());
        Ok(DeleteClause {
            detach,
            targets,
            span: self.span_between(&start, &end),
        })
    }

    fn parse_mutation_return_tail(&mut self) -> Result<MutationReturnTail, EngineError> {
        let return_clause = self.parse_return_clause()?;
        let order_by = if self.at_keyword(Keyword::Order) {
            self.parse_order_by()?
        } else {
            Vec::new()
        };
        let mut skip = None;
        if self.at_keyword(Keyword::Skip) || self.at_keyword(Keyword::Offset) {
            skip = Some(self.parse_skip_or_offset()?);
        }
        if self.at_keyword(Keyword::Skip) || self.at_keyword(Keyword::Offset) {
            return Err(self.parse_error_current("SKIP and OFFSET cannot both be specified"));
        }
        let limit = if self.consume_keyword(Keyword::Limit).is_some() {
            Some(self.parse_expression(0)?.expr)
        } else {
            None
        };
        Ok(MutationReturnTail {
            return_clause,
            order_by,
            skip,
            limit,
        })
    }

    fn parse_match_clause(&mut self) -> Result<MatchClause, EngineError> {
        let optional = self.consume_keyword(Keyword::Optional);
        let start = self.expect_keyword(Keyword::Match, "expected MATCH clause")?;
        let clause_start = optional
            .as_ref()
            .map(|token| token.span.clone())
            .unwrap_or_else(|| start.span.clone());
        let mut patterns = Vec::new();
        patterns.push(self.parse_pattern()?);
        while self
            .consume_if(|kind| matches!(kind, TokenKind::Comma))
            .is_some()
        {
            patterns.push(self.parse_pattern()?);
        }
        let where_clause = if self.consume_keyword(Keyword::Where).is_some() {
            Some(self.parse_expression(0)?.expr)
        } else {
            None
        };
        let end = patterns
            .last()
            .map(|pattern| pattern.span.clone())
            .unwrap_or_else(|| start.span.clone());
        let end = where_clause
            .as_ref()
            .map(|expr| expr.span.clone())
            .unwrap_or(end);
        Ok(MatchClause {
            optional: optional.is_some(),
            patterns,
            where_clause,
            span: self.span_between(&clause_start, &end),
        })
    }

    fn parse_pattern(&mut self) -> Result<Pattern, EngineError> {
        self.reject_shortest_path_syntax_here()?;
        let start = self.current().span.clone();
        let path_variable =
            if self.current_is_ident() && self.next_is(|kind| matches!(kind, TokenKind::Equals)) {
                let ident = self.parse_ident("expected path variable")?;
                self.expect_kind(
                    |kind| matches!(kind, TokenKind::Equals),
                    "expected '=' after path variable",
                )?;
                self.reject_shortest_path_syntax_here()?;
                Some(ident)
            } else {
                None
            };
        let start_node = self.parse_node_pattern()?;
        if self.at_kind(|kind| matches!(kind, TokenKind::LBrace)) {
            return Err(self.unsupported_current(
                "Graph Pattern v2",
                "pattern quantifiers are not supported in Phase 31",
            ));
        }
        let mut chains = Vec::new();
        while self.at_pattern_chain_start() {
            let relationship = self.parse_relationship_pattern()?;
            if self.at_kind(|kind| matches!(kind, TokenKind::LBrace)) {
                return Err(self.unsupported_current(
                    "variable-length relationship syntax",
                    "relationship quantifiers are not supported in Phase 31",
                ));
            }
            let node = self.parse_node_pattern()?;
            if self.at_kind(|kind| matches!(kind, TokenKind::LBrace)) {
                return Err(self.unsupported_current(
                    "Graph Pattern v2",
                    "path quantifiers are not supported in Phase 31",
                ));
            }
            let span = self.span_between(&relationship.span, &node.span);
            chains.push(PatternChain {
                relationship,
                node,
                span,
            });
        }
        let end = chains
            .last()
            .map(|chain| chain.span.clone())
            .unwrap_or_else(|| start_node.span.clone());
        Ok(Pattern {
            path_variable,
            start: start_node,
            chains,
            span: self.span_between(&start, &end),
        })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, EngineError> {
        let start = self.expect_kind(
            |kind| matches!(kind, TokenKind::LParen),
            "expected '(' to start node pattern",
        )?;
        let variable = if self.current_is_ident() {
            Some(self.parse_ident("expected node variable")?)
        } else {
            None
        };

        let mut labels = Vec::new();
        while self
            .consume_if(|kind| matches!(kind, TokenKind::Colon))
            .is_some()
        {
            if self.at_kind(|kind| matches!(kind, TokenKind::Dollar)) {
                return Err(self.unsupported_current(
                    "dynamic labels",
                    "dynamic node labels are not supported in Phase 31",
                ));
            }
            labels.push(self.parse_ident("expected node label after ':'")?);
        }

        let properties = if self.at_kind(|kind| matches!(kind, TokenKind::LBrace)) {
            Some(self.parse_map_literal(0)?.literal)
        } else {
            None
        };
        if self.at_keyword(Keyword::Where) {
            return Err(self.unsupported_current(
                "Graph Pattern v2",
                "pattern-local predicates are not supported in Phase 31",
            ));
        }
        let end = self.expect_kind(
            |kind| matches!(kind, TokenKind::RParen),
            "expected ')' to close node pattern",
        )?;
        Ok(NodePattern {
            variable,
            labels,
            properties,
            span: self.span_between(&start.span, &end.span),
        })
    }

    fn parse_relationship_pattern(&mut self) -> Result<RelationshipPattern, EngineError> {
        let start = self.current().span.clone();
        let (direction, variable, rel_types, quantifier, properties, end_span) =
            if let Some(left) = self.consume_if(|kind| matches!(kind, TokenKind::LeftArrow)) {
                let (variable, rel_types, quantifier, properties) =
                    if self.at_kind(|kind| matches!(kind, TokenKind::LBracket)) {
                        self.parse_relationship_detail()?
                    } else {
                        (None, Vec::new(), None, None)
                    };
                let end = self.expect_kind(
                    |kind| matches!(kind, TokenKind::Dash),
                    "expected '-' after reverse relationship pattern",
                )?;
                (
                    RelationshipDirection::RightToLeft,
                    variable,
                    rel_types,
                    quantifier,
                    properties,
                    self.span_between(&left.span, &end.span),
                )
            } else {
                self.expect_kind(
                    |kind| matches!(kind, TokenKind::Dash),
                    "expected relationship pattern",
                )?;
                if self.at_kind(|kind| matches!(kind, TokenKind::Star)) {
                    return Err(self.unsupported_current(
                        "variable-length relationship syntax",
                        "variable-length relationship patterns are not supported in Phase 31",
                    ));
                }
                let (variable, rel_types, quantifier, properties) =
                    if self.at_kind(|kind| matches!(kind, TokenKind::LBracket)) {
                        self.parse_relationship_detail()?
                    } else {
                        (None, Vec::new(), None, None)
                    };
                if let Some(end) = self.consume_if(|kind| matches!(kind, TokenKind::RightArrow)) {
                    (
                        RelationshipDirection::LeftToRight,
                        variable,
                        rel_types,
                        quantifier,
                        properties,
                        end.span,
                    )
                } else if let Some(end) = self.consume_if(|kind| matches!(kind, TokenKind::Dash)) {
                    (
                        RelationshipDirection::Undirected,
                        variable,
                        rel_types,
                        quantifier,
                        properties,
                        end.span,
                    )
                } else if self.at_kind(|kind| matches!(kind, TokenKind::Star)) {
                    return Err(self.unsupported_current(
                        "variable-length relationship syntax",
                        "variable-length relationship patterns are not supported in Phase 31",
                    ));
                } else {
                    return Err(
                        self.parse_error_current("expected '-' or '->' after relationship pattern")
                    );
                }
            };

        Ok(RelationshipPattern {
            variable,
            rel_types,
            quantifier,
            direction,
            properties,
            span: self.span_between(&start, &end_span),
        })
    }

    fn parse_relationship_detail(&mut self) -> Result<RelationshipDetail, EngineError> {
        self.expect_kind(
            |kind| matches!(kind, TokenKind::LBracket),
            "expected '[' to start relationship pattern",
        )?;
        let variable = if self.current_is_ident() {
            Some(self.parse_ident("expected relationship variable")?)
        } else {
            None
        };

        let mut rel_types = Vec::new();
        if self
            .consume_if(|kind| matches!(kind, TokenKind::Colon))
            .is_some()
        {
            if self.at_kind(|kind| matches!(kind, TokenKind::Dollar)) {
                return Err(self.unsupported_current(
                    "dynamic relationship types",
                    "dynamic relationship labels are not supported in Phase 31",
                ));
            }
            rel_types.push(self.parse_ident("expected relationship label after ':'")?);
            while self
                .consume_if(|kind| matches!(kind, TokenKind::Pipe))
                .is_some()
            {
                if self.at_dynamic_relationship_label() {
                    return Err(self.unsupported_current(
                        "dynamic relationship types",
                        "dynamic relationship labels are not supported in Phase 31",
                    ));
                }
                rel_types.push(self.parse_ident("expected relationship label after '|'")?);
            }
        }
        let quantifier = if self.at_kind(|kind| matches!(kind, TokenKind::Star)) {
            Some(self.parse_relationship_quantifier()?)
        } else {
            None
        };

        let properties = if self.at_kind(|kind| matches!(kind, TokenKind::LBrace)) {
            Some(self.parse_map_literal(0)?.literal)
        } else {
            None
        };
        if self.at_keyword(Keyword::Where) {
            return Err(self.unsupported_current(
                "Graph Pattern v2",
                "pattern-local predicates are not supported in Phase 31",
            ));
        }
        self.expect_kind(
            |kind| matches!(kind, TokenKind::RBracket),
            "expected ']' to close relationship pattern",
        )?;
        Ok((variable, rel_types, quantifier, properties))
    }

    fn parse_relationship_quantifier(&mut self) -> Result<RelationshipQuantifier, EngineError> {
        let star = self.expect_kind(
            |kind| matches!(kind, TokenKind::Star),
            "expected '*' to start relationship quantifier",
        )?;
        if self.at_kind(|kind| matches!(kind, TokenKind::RBracket)) {
            return Err(EngineError::GqlUnsupported {
                feature: "unbounded VLP".to_string(),
                message:
                    "unbounded variable-length relationship patterns require a finite upper bound"
                        .to_string(),
                span: star.span,
            });
        }

        let min_hops = if self.at_two_dots() {
            0
        } else {
            self.parse_hop_bound("expected finite lower hop bound after '*'")?
        };

        let max_hops = if self.consume_two_dots() {
            if self.at_kind(|kind| matches!(kind, TokenKind::RBracket)) {
                return Err(EngineError::GqlUnsupported {
                    feature: "unbounded VLP".to_string(),
                    message:
                        "variable-length relationship patterns must include a finite upper bound"
                            .to_string(),
                    span: self.span_between(&star.span, &self.current().span),
                });
            }
            self.parse_hop_bound("expected finite upper hop bound after '..'")?
        } else {
            min_hops
        };
        if min_hops > max_hops {
            return Err(EngineError::GqlParse {
                message: format!(
                    "relationship quantifier lower bound {min_hops} exceeds upper bound {max_hops}"
                ),
                span: self.span_between(&star.span, &self.previous_non_eof_span()),
            });
        }
        Ok(RelationshipQuantifier {
            min_hops,
            max_hops,
            span: self.span_between(&star.span, &self.previous_non_eof_span()),
        })
    }

    fn parse_hop_bound(&mut self, message: &str) -> Result<u8, EngineError> {
        let token = self.current().clone();
        let TokenKind::Int(value) = token.kind else {
            return Err(self.parse_error_current(message));
        };
        if value < 0 || value > u8::MAX as i64 {
            return Err(EngineError::GqlParse {
                message: "relationship hop bound must fit in u8".to_string(),
                span: token.span,
            });
        }
        self.advance();
        Ok(value as u8)
    }

    fn parse_return_clause(&mut self) -> Result<ReturnClause, EngineError> {
        let start = self.expect_keyword(Keyword::Return, "expected RETURN clause")?;
        if self.at_keyword(Keyword::Distinct) {
            return Err(
                self.unsupported_current("DISTINCT", "DISTINCT is not supported in Phase 31")
            );
        }
        if let Some(star) = self.consume_if(|kind| matches!(kind, TokenKind::Star)) {
            return Ok(ReturnClause {
                body: ReturnBody::All(star.span.clone()),
                span: self.span_between(&start.span, &star.span),
            });
        }

        let mut items = Vec::new();
        loop {
            let expr = self.parse_expression(0)?.expr;
            let alias = if self.consume_keyword(Keyword::As).is_some() {
                Some(self.parse_ident("expected alias after AS")?)
            } else {
                None
            };
            let end = alias
                .as_ref()
                .map(|alias| alias.span.clone())
                .unwrap_or_else(|| expr.span.clone());
            let span = self.span_between(&expr.span, &end);
            items.push(ReturnItem { expr, alias, span });
            if self
                .consume_if(|kind| matches!(kind, TokenKind::Comma))
                .is_none()
            {
                break;
            }
        }
        let end = items
            .last()
            .map(|item| item.span.clone())
            .unwrap_or_else(|| start.span.clone());
        Ok(ReturnClause {
            body: ReturnBody::Items(items),
            span: self.span_between(&start.span, &end),
        })
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderItem>, EngineError> {
        self.expect_keyword(Keyword::Order, "expected ORDER")?;
        self.expect_keyword(Keyword::By, "expected BY after ORDER")?;
        let mut items = Vec::new();
        loop {
            let expr = self.parse_expression(0)?.expr;
            let (direction, end) = if let Some(desc) = self.consume_keyword(Keyword::Desc) {
                (OrderDirection::Desc, desc.span)
            } else if let Some(asc) = self.consume_keyword(Keyword::Asc) {
                (OrderDirection::Asc, asc.span)
            } else {
                (OrderDirection::Asc, expr.span.clone())
            };
            let span = self.span_between(&expr.span, &end);
            items.push(OrderItem {
                expr,
                direction,
                span,
            });
            if self
                .consume_if(|kind| matches!(kind, TokenKind::Comma))
                .is_none()
            {
                break;
            }
        }
        Ok(items)
    }

    fn parse_skip_or_offset(&mut self) -> Result<Expr, EngineError> {
        if self.consume_keyword(Keyword::Skip).is_none() {
            self.expect_keyword(Keyword::Offset, "expected SKIP or OFFSET")?;
        }
        Ok(self.parse_expression(0)?.expr)
    }

    fn leaf_expr(expr: Expr) -> ParsedExpr {
        ParsedExpr { expr, ast_depth: 1 }
    }

    fn binary_expr(
        &self,
        op: BinaryOp,
        left: ParsedExpr,
        right: ParsedExpr,
    ) -> Result<ParsedExpr, EngineError> {
        let span = self.span_between(&left.expr.span, &right.expr.span);
        let ast_depth = left.ast_depth.max(right.ast_depth) + 1;
        self.check_depth(ast_depth, &span)?;
        Ok(ParsedExpr {
            expr: Expr {
                kind: ExprKind::Binary {
                    op,
                    left: Box::new(left.expr),
                    right: Box::new(right.expr),
                },
                span,
            },
            ast_depth,
        })
    }

    fn parse_expression(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        self.parse_or(depth)
    }

    fn parse_or(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let mut expr = self.parse_and(depth)?;
        while self.consume_keyword(Keyword::Or).is_some() {
            let right = self.parse_and(depth)?;
            expr = self.binary_expr(BinaryOp::Or, expr, right)?;
        }
        Ok(expr)
    }

    fn parse_and(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let mut expr = self.parse_comparison(depth)?;
        while self.consume_keyword(Keyword::And).is_some() {
            let right = self.parse_comparison(depth)?;
            expr = self.binary_expr(BinaryOp::And, expr, right)?;
        }
        Ok(expr)
    }

    fn parse_comparison(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let mut expr = self.parse_not(depth)?;
        loop {
            let op = if self
                .consume_if(|kind| matches!(kind, TokenKind::Equals))
                .is_some()
            {
                Some(BinaryOp::Eq)
            } else if self
                .consume_if(|kind| matches!(kind, TokenKind::Neq))
                .is_some()
            {
                Some(BinaryOp::Neq)
            } else if self
                .consume_if(|kind| matches!(kind, TokenKind::Lt))
                .is_some()
            {
                Some(BinaryOp::Lt)
            } else if self
                .consume_if(|kind| matches!(kind, TokenKind::Le))
                .is_some()
            {
                Some(BinaryOp::Le)
            } else if self
                .consume_if(|kind| matches!(kind, TokenKind::Gt))
                .is_some()
            {
                Some(BinaryOp::Gt)
            } else if self
                .consume_if(|kind| matches!(kind, TokenKind::Ge))
                .is_some()
            {
                Some(BinaryOp::Ge)
            } else if self.consume_keyword(Keyword::In).is_some() {
                Some(BinaryOp::In)
            } else {
                None
            };
            if let Some(op) = op {
                let right = self.parse_not(depth)?;
                expr = self.binary_expr(op, expr, right)?;
                continue;
            }

            if self.consume_keyword(Keyword::Is).is_some() {
                let negated = self.consume_keyword(Keyword::Not).is_some();
                let null = self.expect_keyword(Keyword::Null, "expected NULL after IS")?;
                let span = self.span_between(&expr.expr.span, &null.span);
                let ast_depth = expr.ast_depth + 1;
                self.check_depth(ast_depth, &span)?;
                expr = ParsedExpr {
                    expr: Expr {
                        kind: ExprKind::IsNull {
                            expr: Box::new(expr.expr),
                            negated,
                        },
                        span,
                    },
                    ast_depth,
                };
                continue;
            }
            break;
        }
        Ok(expr)
    }

    fn parse_not(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        if let Some(not) = self.consume_keyword(Keyword::Not) {
            self.check_depth(depth + 1, &not.span)?;
            let expr = self.parse_not(depth + 1)?;
            let span = self.span_between(&not.span, &expr.expr.span);
            let ast_depth = expr.ast_depth + 1;
            self.check_depth(ast_depth, &span)?;
            Ok(ParsedExpr {
                expr: Expr {
                    kind: ExprKind::Unary {
                        op: UnaryOp::Not,
                        expr: Box::new(expr.expr),
                    },
                    span,
                },
                ast_depth,
            })
        } else {
            self.parse_postfix(depth)
        }
    }

    fn parse_postfix(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let mut expr = self.parse_primary(depth)?;
        while self
            .consume_if(|kind| matches!(kind, TokenKind::Dot))
            .is_some()
        {
            let property = self.parse_property_ident("expected property name after '.'")?;
            let span = self.span_between(&expr.expr.span, &property.span);
            let ast_depth = expr.ast_depth + 1;
            self.check_depth(ast_depth, &span)?;
            expr = ParsedExpr {
                expr: Expr {
                    kind: ExprKind::PropertyAccess {
                        object: Box::new(expr.expr),
                        property,
                    },
                    span,
                },
                ast_depth,
            };
        }
        Ok(expr)
    }

    fn parse_primary(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        self.reject_expression_unsupported()?;
        let token = self.current().clone();
        match token.kind {
            TokenKind::Int(value) => {
                self.advance();
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::Int(value)),
                    span: token.span,
                }))
            }
            TokenKind::Float(value) => {
                self.advance();
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::Float(value)),
                    span: token.span,
                }))
            }
            TokenKind::String(value) => {
                self.advance();
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::String(value)),
                    span: token.span,
                }))
            }
            TokenKind::Keyword(Keyword::Null) => {
                self.advance();
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::Null),
                    span: token.span,
                }))
            }
            TokenKind::Keyword(Keyword::True) => {
                self.advance();
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::Bool(true)),
                    span: token.span,
                }))
            }
            TokenKind::Keyword(Keyword::False) => {
                self.advance();
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::Bool(false)),
                    span: token.span,
                }))
            }
            TokenKind::Dollar => self.parse_parameter(),
            TokenKind::Ident(name) => {
                if self.next_is(|kind| matches!(kind, TokenKind::LParen)) {
                    self.parse_function_call(name, token.span, depth)
                } else {
                    self.advance();
                    Ok(Self::leaf_expr(Expr {
                        kind: ExprKind::Variable(name),
                        span: token.span,
                    }))
                }
            }
            TokenKind::Keyword(Keyword::Exists)
                if self.next_is(|kind| matches!(kind, TokenKind::LBrace)) =>
            {
                Err(self
                    .unsupported_current("subqueries", "subqueries are not supported in Phase 31"))
            }
            TokenKind::Keyword(Keyword::Exists)
                if self.next_is(|kind| matches!(kind, TokenKind::LParen)) =>
            {
                Err(self.unsupported_current(
                    "EXISTS",
                    "EXISTS predicate syntax is not supported in Phase 31",
                ))
            }
            TokenKind::LParen => {
                self.check_depth(depth + 1, &token.span)?;
                self.advance();
                let mut expr = self.parse_expression(depth + 1)?;
                let end = self.expect_kind(
                    |kind| matches!(kind, TokenKind::RParen),
                    "expected ')' to close expression",
                )?;
                expr.expr.span = self.span_between(&token.span, &end.span);
                Ok(expr)
            }
            TokenKind::LBracket => self.parse_list_literal(depth),
            TokenKind::LBrace => self.parse_map_expr(depth),
            TokenKind::Dash if self.next_is(|kind| matches!(kind, TokenKind::Int(_))) => {
                let start = self.advance();
                let next = self.advance();
                let TokenKind::Int(value) = next.kind else {
                    unreachable!("checked above")
                };
                let Some(value) = value.checked_neg() else {
                    return Err(EngineError::GqlParse {
                        message: "integer literal is out of range".to_string(),
                        span: self.span_between(&start.span, &next.span),
                    });
                };
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::Int(value)),
                    span: self.span_between(&start.span, &next.span),
                }))
            }
            TokenKind::Dash if self.next_is(|kind| matches!(kind, TokenKind::Float(_))) => {
                let start = self.advance();
                let next = self.advance();
                let TokenKind::Float(value) = next.kind else {
                    unreachable!("checked above")
                };
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::Literal(Literal::Float(-value)),
                    span: self.span_between(&start.span, &next.span),
                }))
            }
            _ => Err(self.parse_error_current("expected expression")),
        }
    }

    fn parse_function_call(
        &mut self,
        name: String,
        name_span: SourceSpan,
        depth: usize,
    ) -> Result<ParsedExpr, EngineError> {
        let lower = name.to_ascii_lowercase();
        if is_shortest_path_function(&name) {
            return Err(EngineError::GqlUnsupported {
                feature: "shortest-path syntax".to_string(),
                message: "shortest-path functions are not supported in Phase 31".to_string(),
                span: name_span,
            });
        }
        if is_aggregation_function(&lower) {
            return Err(EngineError::GqlUnsupported {
                feature: "aggregation".to_string(),
                message: "aggregation functions are not supported in Phase 31".to_string(),
                span: name_span,
            });
        }
        if !is_supported_function(&lower) {
            return Err(EngineError::GqlUnsupported {
                feature: format!("function {}", name),
                message: format!(
                    "function '{}' is not supported in the current GQL subset",
                    name
                ),
                span: name_span,
            });
        }

        let ident = Ident {
            name,
            span: name_span.clone(),
        };
        self.check_depth(depth + 1, &name_span)?;
        self.advance();
        self.expect_kind(
            |kind| matches!(kind, TokenKind::LParen),
            "expected '(' after function name",
        )?;
        let mut args = Vec::new();
        let mut max_arg_depth = 0usize;
        if !self.at_kind(|kind| matches!(kind, TokenKind::RParen)) {
            loop {
                let arg = self.parse_expression(depth + 1)?;
                max_arg_depth = max_arg_depth.max(arg.ast_depth);
                args.push(arg.expr);
                if self
                    .consume_if(|kind| matches!(kind, TokenKind::Comma))
                    .is_none()
                {
                    break;
                }
            }
        }
        let end = self.expect_kind(
            |kind| matches!(kind, TokenKind::RParen),
            "expected ')' after function arguments",
        )?;
        let span = self.span_between(&name_span, &end.span);
        let ast_depth = max_arg_depth + 1;
        self.check_depth(ast_depth, &span)?;
        Ok(ParsedExpr {
            expr: Expr {
                kind: ExprKind::FunctionCall { name: ident, args },
                span,
            },
            ast_depth,
        })
    }

    fn parse_parameter(&mut self) -> Result<ParsedExpr, EngineError> {
        let start = self.expect_kind(
            |kind| matches!(kind, TokenKind::Dollar),
            "expected parameter",
        )?;
        let name = self.parse_parameter_ident("expected parameter name after '$'")?;
        Ok(Self::leaf_expr(Expr {
            kind: ExprKind::Parameter(name.name),
            span: self.span_between(&start.span, &name.span),
        }))
    }

    fn parse_list_literal(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let start = self.expect_kind(
            |kind| matches!(kind, TokenKind::LBracket),
            "expected '[' to start list literal",
        )?;
        self.check_depth(depth + 1, &start.span)?;
        let mut items = Vec::new();
        let mut max_item_depth = 0usize;
        if !self.at_kind(|kind| matches!(kind, TokenKind::RBracket)) {
            loop {
                if items.len() >= self.options.max_literal_items {
                    return Err(EngineError::GqlParse {
                        message: format!(
                            "list literal exceeds max_literal_items of {}",
                            self.options.max_literal_items
                        ),
                        span: self.current().span.clone(),
                    });
                }
                let item = self.parse_expression(depth + 1)?;
                max_item_depth = max_item_depth.max(item.ast_depth);
                self.check_depth(max_item_depth + 1, &start.span)?;
                items.push(item.expr);
                if self
                    .consume_if(|kind| matches!(kind, TokenKind::Comma))
                    .is_none()
                {
                    break;
                }
            }
        }
        let end = self.expect_kind(
            |kind| matches!(kind, TokenKind::RBracket),
            "expected ']' to close list literal",
        )?;
        let span = self.span_between(&start.span, &end.span);
        let ast_depth = max_item_depth + 1;
        self.check_depth(ast_depth, &span)?;
        Ok(ParsedExpr {
            expr: Expr {
                kind: ExprKind::List(items),
                span,
            },
            ast_depth,
        })
    }

    fn parse_map_expr(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let literal = self.parse_map_literal(depth)?;
        let span = literal.literal.span.clone();
        Ok(ParsedExpr {
            expr: Expr {
                kind: ExprKind::Map(literal.literal),
                span,
            },
            ast_depth: literal.ast_depth,
        })
    }

    fn parse_map_literal(&mut self, depth: usize) -> Result<ParsedMapLiteral, EngineError> {
        let start = self.expect_kind(
            |kind| matches!(kind, TokenKind::LBrace),
            "expected '{' to start map literal",
        )?;
        self.check_depth(depth + 1, &start.span)?;
        let mut entries = Vec::new();
        let mut max_value_depth = 0usize;
        if !self.at_kind(|kind| matches!(kind, TokenKind::RBrace)) {
            loop {
                if entries.len() >= self.options.max_literal_items {
                    return Err(EngineError::GqlParse {
                        message: format!(
                            "map literal exceeds max_literal_items of {}",
                            self.options.max_literal_items
                        ),
                        span: self.current().span.clone(),
                    });
                }
                let key = self.parse_map_key()?;
                self.expect_kind(
                    |kind| matches!(kind, TokenKind::Colon),
                    "expected ':' after map key",
                )?;
                let value = self.parse_expression(depth + 1)?;
                max_value_depth = max_value_depth.max(value.ast_depth);
                self.check_depth(max_value_depth + 1, &start.span)?;
                let span = self.span_between(&key.span, &value.expr.span);
                entries.push(MapEntry {
                    key,
                    value: value.expr,
                    span,
                });
                if self
                    .consume_if(|kind| matches!(kind, TokenKind::Comma))
                    .is_none()
                {
                    break;
                }
            }
        }
        let end = self.expect_kind(
            |kind| matches!(kind, TokenKind::RBrace),
            "expected '}' to close map literal",
        )?;
        let span = self.span_between(&start.span, &end.span);
        let ast_depth = max_value_depth + 1;
        self.check_depth(ast_depth, &span)?;
        Ok(ParsedMapLiteral {
            literal: MapLiteral { entries, span },
            ast_depth,
        })
    }

    fn parse_map_key(&mut self) -> Result<MapKey, EngineError> {
        let token = self.current().clone();
        match token.kind {
            TokenKind::Ident(name) => {
                self.advance();
                Ok(MapKey {
                    name,
                    span: token.span,
                })
            }
            TokenKind::Keyword(_) => {
                self.advance();
                Ok(MapKey {
                    name: self.source_for_span(&token.span).to_string(),
                    span: token.span,
                })
            }
            TokenKind::String(name) => {
                self.advance();
                Ok(MapKey {
                    name,
                    span: token.span,
                })
            }
            _ => Err(self.parse_error_current("expected map key")),
        }
    }

    fn parse_ident(&mut self, message: &str) -> Result<Ident, EngineError> {
        let token = self.current().clone();
        if let TokenKind::Ident(name) = token.kind {
            self.advance();
            Ok(Ident {
                name,
                span: token.span,
            })
        } else {
            Err(self.parse_error_current(message))
        }
    }

    fn parse_property_ident(&mut self, message: &str) -> Result<Ident, EngineError> {
        let token = self.current().clone();
        match token.kind {
            TokenKind::Ident(name) => {
                self.advance();
                Ok(Ident {
                    name,
                    span: token.span,
                })
            }
            TokenKind::Keyword(_) => {
                self.advance();
                Ok(Ident {
                    name: self.source_for_span(&token.span).to_string(),
                    span: token.span,
                })
            }
            _ => Err(self.parse_error_current(message)),
        }
    }

    fn parse_parameter_ident(&mut self, message: &str) -> Result<Ident, EngineError> {
        let token = self.current().clone();
        match token.kind {
            TokenKind::Ident(name) => {
                self.advance();
                Ok(Ident {
                    name,
                    span: token.span,
                })
            }
            TokenKind::Keyword(_) => {
                self.advance();
                Ok(Ident {
                    name: self.source_for_span(&token.span).to_string(),
                    span: token.span,
                })
            }
            _ => Err(self.parse_error_current(message)),
        }
    }

    fn reject_unsupported_clause(&self) -> Result<(), EngineError> {
        if self.token_word_eq(self.pos, "graph")
            || self.token_word_eq(self.pos, "use")
            || (self.token_word_eq(self.pos, "from") && self.token_word_eq(self.pos + 1, "graph"))
        {
            return Err(self.unsupported_current(
                "graph catalog/session selection syntax",
                "graph catalog and session selection syntax is not supported in Phase 31",
            ));
        }

        if let TokenKind::Keyword(keyword) = self.current().kind {
            match keyword {
                Keyword::With => {
                    return Err(
                        self.unsupported_current("WITH", "WITH is not supported in Phase 31")
                    );
                }
                Keyword::Union => {
                    return Err(
                        self.unsupported_current("UNION", "UNION is not supported in Phase 31")
                    );
                }
                Keyword::Call => {
                    let (feature, message) =
                        if self.next_is(|kind| matches!(kind, TokenKind::LBrace)) {
                            ("subqueries", "subqueries are not supported in Phase 31")
                        } else {
                            ("CALL", "CALL/procedure syntax is not supported in Phase 31")
                        };
                    return Err(self.unsupported_current(feature, message));
                }
                Keyword::Create if self.create_clause_is_schema_ddl() => {
                    return Err(self.unsupported_current(
                        "schema/DDL",
                        "schema and DDL statements are not supported in Phase 31",
                    ));
                }
                Keyword::Drop | Keyword::Alter | Keyword::Show => {
                    return Err(self.unsupported_current(
                        "schema/DDL",
                        "schema and catalog statements are not supported in Phase 31",
                    ));
                }
                Keyword::Create
                | Keyword::Merge
                | Keyword::Set
                | Keyword::Delete
                | Keyword::Detach
                | Keyword::Remove
                | Keyword::Foreach
                | Keyword::Load => {
                    return Err(self.unsupported_current(
                        "write clauses",
                        "write clauses are not supported in read-only GQL queries",
                    ));
                }
                Keyword::Unwind => {
                    return Err(
                        self.unsupported_current("UNWIND", "UNWIND is not supported in Phase 31")
                    );
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn reject_expression_unsupported(&self) -> Result<(), EngineError> {
        self.reject_unsupported_clause()?;
        if self.at_keyword(Keyword::Distinct) {
            return Err(
                self.unsupported_current("DISTINCT", "DISTINCT is not supported in Phase 31")
            );
        }
        if self.at_keyword(Keyword::Exists)
            && self.next_is(|kind| matches!(kind, TokenKind::LBrace))
        {
            return Err(
                self.unsupported_current("subqueries", "subqueries are not supported in Phase 31")
            );
        }
        Ok(())
    }

    fn check_depth(&self, depth: usize, span: &SourceSpan) -> Result<(), EngineError> {
        if depth > self.options.max_ast_depth {
            Err(EngineError::GqlParse {
                message: format!(
                    "AST depth exceeds max_ast_depth of {}",
                    self.options.max_ast_depth
                ),
                span: span.clone(),
            })
        } else {
            Ok(())
        }
    }

    fn unsupported_current(&self, feature: &str, message: &str) -> EngineError {
        EngineError::GqlUnsupported {
            feature: feature.to_string(),
            message: message.to_string(),
            span: self.current().span.clone(),
        }
    }

    fn parse_error_current(&self, message: &str) -> EngineError {
        EngineError::GqlParse {
            message: message.to_string(),
            span: self.current().span.clone(),
        }
    }

    fn expect_keyword(&mut self, keyword: Keyword, message: &str) -> Result<Token, EngineError> {
        if self.at_keyword(keyword) {
            Ok(self.advance())
        } else {
            Err(self.parse_error_current(message))
        }
    }

    fn expect_kind(
        &mut self,
        predicate: impl Fn(&TokenKind) -> bool,
        message: &str,
    ) -> Result<Token, EngineError> {
        if predicate(&self.current().kind) {
            Ok(self.advance())
        } else {
            Err(self.parse_error_current(message))
        }
    }

    fn consume_keyword(&mut self, keyword: Keyword) -> Option<Token> {
        self.at_keyword(keyword).then(|| self.advance())
    }

    fn consume_if(&mut self, predicate: impl Fn(&TokenKind) -> bool) -> Option<Token> {
        predicate(&self.current().kind).then(|| self.advance())
    }

    fn advance(&mut self) -> Token {
        let token = self.current().clone();
        if !matches!(token.kind, TokenKind::Eof) {
            self.pos += 1;
        }
        token
    }

    fn current(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn previous_non_eof_span(&self) -> SourceSpan {
        self.tokens
            .iter()
            .take(self.pos + 1)
            .rev()
            .find(|token| !matches!(token.kind, TokenKind::Eof))
            .map(|token| token.span.clone())
            .unwrap_or_else(|| self.current().span.clone())
    }

    fn at_eof(&self) -> bool {
        matches!(self.current().kind, TokenKind::Eof)
    }

    fn at_keyword(&self, keyword: Keyword) -> bool {
        matches!(self.current().kind, TokenKind::Keyword(current) if current == keyword)
    }

    fn at_match_clause_start(&self) -> bool {
        self.at_keyword(Keyword::Match)
            || (self.at_keyword(Keyword::Optional) && self.next_keyword_is(Keyword::Match))
    }

    fn at_mutation_clause_start(&self) -> bool {
        match self.current().kind {
            TokenKind::Keyword(Keyword::Create) => !self.create_clause_is_schema_ddl(),
            TokenKind::Keyword(
                Keyword::Set | Keyword::Remove | Keyword::Delete | Keyword::Detach,
            ) => true,
            _ => false,
        }
    }

    fn next_keyword_is(&self, keyword: Keyword) -> bool {
        matches!(
            self.tokens.get(self.pos + 1).map(|token| &token.kind),
            Some(TokenKind::Keyword(current)) if *current == keyword
        )
    }

    fn create_clause_is_schema_ddl(&self) -> bool {
        for lookahead in 1..=5 {
            let Some(token) = self.tokens.get(self.pos + lookahead) else {
                break;
            };
            match &token.kind {
                TokenKind::LParen | TokenKind::LBrace | TokenKind::Semicolon | TokenKind::Eof => {
                    break;
                }
                TokenKind::Keyword(Keyword::Index | Keyword::Constraint | Keyword::Graph) => {
                    return true;
                }
                TokenKind::Ident(name)
                    if matches!(
                        name.to_ascii_uppercase().as_str(),
                        "INDEX" | "CONSTRAINT" | "GRAPH" | "DATABASE"
                    ) =>
                {
                    return true;
                }
                _ => {}
            }
        }
        false
    }

    fn at_kind(&self, predicate: impl Fn(&TokenKind) -> bool) -> bool {
        predicate(&self.current().kind)
    }

    fn at_two_dots(&self) -> bool {
        matches!(self.current().kind, TokenKind::Dot)
            && self.next_is(|kind| matches!(kind, TokenKind::Dot))
    }

    fn consume_two_dots(&mut self) -> bool {
        if self.at_two_dots() {
            self.advance();
            self.advance();
            true
        } else {
            false
        }
    }

    fn next_is(&self, predicate: impl Fn(&TokenKind) -> bool) -> bool {
        self.tokens
            .get(self.pos + 1)
            .is_some_and(|token| predicate(&token.kind))
    }

    fn current_is_ident(&self) -> bool {
        matches!(self.current().kind, TokenKind::Ident(_))
    }

    fn current_ident_name(&self) -> Option<&str> {
        match &self.current().kind {
            TokenKind::Ident(name) => Some(name.as_str()),
            _ => None,
        }
    }

    fn token_word_eq(&self, index: usize, expected: &str) -> bool {
        let Some(token) = self.tokens.get(index) else {
            return false;
        };
        match &token.kind {
            TokenKind::Ident(name) => name.eq_ignore_ascii_case(expected),
            TokenKind::Keyword(_) => self
                .source_for_span(&token.span)
                .eq_ignore_ascii_case(expected),
            _ => false,
        }
    }

    fn at_gql_shortest_path_syntax(&self) -> bool {
        self.token_word_eq(self.pos, "shortest")
            || ((self.token_word_eq(self.pos, "any") || self.token_word_eq(self.pos, "all"))
                && self.token_word_eq(self.pos + 1, "shortest"))
    }

    fn reject_shortest_path_syntax_here(&self) -> Result<(), EngineError> {
        if self.at_gql_shortest_path_syntax()
            || (self
                .current_ident_name()
                .is_some_and(is_shortest_path_function)
                && self.next_is(|kind| matches!(kind, TokenKind::LParen)))
        {
            return Err(self.unsupported_current(
                "shortest-path syntax",
                "shortest-path pattern syntax is not supported in Phase 31",
            ));
        }
        Ok(())
    }

    fn at_pattern_chain_start(&self) -> bool {
        self.at_kind(|kind| matches!(kind, TokenKind::Dash | TokenKind::LeftArrow))
    }

    fn at_dynamic_relationship_label(&self) -> bool {
        self.at_kind(|kind| matches!(kind, TokenKind::Dollar))
            || (self.at_kind(|kind| matches!(kind, TokenKind::Colon))
                && self.next_is(|kind| matches!(kind, TokenKind::Dollar)))
    }

    fn source_for_span(&self, span: &SourceSpan) -> &str {
        &self.query[span.offset..span.end_offset()]
    }

    fn span_between(&self, start: &SourceSpan, end: &SourceSpan) -> SourceSpan {
        SourceSpan::new(
            start.offset,
            end.end_offset().saturating_sub(start.offset),
            start.line,
            start.column,
        )
    }
}

fn set_item_span(item: &SetItem) -> &SourceSpan {
    match item {
        SetItem::Property { span, .. }
        | SetItem::MapMerge { span, .. }
        | SetItem::NodeLabel { span, .. } => span,
    }
}

fn remove_item_span(item: &RemoveItem) -> &SourceSpan {
    match item {
        RemoveItem::Property { span, .. } | RemoveItem::NodeLabel { span, .. } => span,
    }
}

fn span_at_offset(query: &str, offset: usize, length: usize) -> SourceSpan {
    let mut line = 1u32;
    let mut column = 1u32;
    for (idx, ch) in query.char_indices() {
        if idx >= offset {
            break;
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }
    SourceSpan::new(offset, length, line, column)
}

fn is_aggregation_function(lower: &str) -> bool {
    matches!(
        lower,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "collect"
            | "stdev"
            | "percentilecont"
            | "percentiledisc"
    )
}

fn is_supported_function(lower: &str) -> bool {
    matches!(
        lower,
        "id" | "labels"
            | "type"
            | "length"
            | "start_node"
            | "end_node"
            | "nodes"
            | "relationships"
            | "node_ids"
            | "edge_ids"
    )
}

fn is_shortest_path_function(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    matches!(
        lower.as_str(),
        "shortestpath" | "allshortestpaths" | "anyshortestpath" | "shortest_path"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(query: &str) -> GqlQuery {
        parse_query(query, &GqlParseOptions::default()).unwrap_or_else(|err| {
            panic!("expected query to parse, got {err:?}");
        })
    }

    fn parse_with_options(query: &str, options: GqlParseOptions) -> Result<GqlQuery, EngineError> {
        parse_query(query, &options)
    }

    fn parse_err(query: &str) -> EngineError {
        parse_query(query, &GqlParseOptions::default()).expect_err("query should fail")
    }

    fn parse_statement_ok(source: &str) -> GqlStatement {
        parse_statement(source, &GqlParseOptions::default()).unwrap_or_else(|err| {
            panic!("expected statement to parse, got {err:?}");
        })
    }

    fn parse_statement_err(source: &str) -> EngineError {
        parse_statement(source, &GqlParseOptions::default()).expect_err("statement should fail")
    }

    fn expect_unsupported(query: &str, expected_feature: &str) -> SourceSpan {
        match parse_err(query) {
            EngineError::GqlUnsupported { feature, span, .. } => {
                assert_eq!(feature, expected_feature, "query: {query}");
                assert!(span.length > 0, "unsupported span should be non-empty");
                span
            }
            err => panic!("expected unsupported error for {query}, got {err:?}"),
        }
    }

    fn expect_parse_error(query: &str) -> SourceSpan {
        match parse_err(query) {
            EngineError::GqlParse { span, .. } => span,
            err => panic!("expected parse error for {query}, got {err:?}"),
        }
    }

    fn property_access_name(expr: &Expr) -> &str {
        match &expr.kind {
            ExprKind::PropertyAccess { property, .. } => &property.name,
            other => panic!("expected property access, got {other:?}"),
        }
    }

    #[test]
    fn parses_supported_fixed_node_patterns() {
        let query = parse_ok(r#"MATCH (n:Person {key: $key, status: "active"}) RETURN n"#);
        let pattern = &query.match_clauses[0].patterns[0];
        assert_eq!(pattern.start.variable.as_ref().unwrap().name, "n");
        assert_eq!(pattern.start.labels[0].name, "Person");
        let props = pattern.start.properties.as_ref().unwrap();
        assert_eq!(props.entries.len(), 2);
        assert_eq!(props.entries[0].key.name, "key");
        assert!(matches!(
            props.entries[0].value.kind,
            ExprKind::Parameter(ref name) if name == "key"
        ));
        assert_eq!(props.entries[1].key.name, "status");
        assert!(matches!(
            props.entries[1].value.kind,
            ExprKind::Literal(Literal::String(ref value)) if value == "active"
        ));
    }

    #[test]
    fn parses_supported_relationship_directions() {
        let query =
            parse_ok("MATCH (a)-[r:KNOWS]->(b), (c)<-[s:LIKES]-(d), (e)--(f) RETURN a, r, s");
        let first = &query.match_clauses[0].patterns[0].chains[0].relationship;
        assert_eq!(first.direction, RelationshipDirection::LeftToRight);
        assert_eq!(first.variable.as_ref().unwrap().name, "r");
        assert_eq!(first.rel_types[0].name, "KNOWS");

        let second = &query.match_clauses[0].patterns[1].chains[0].relationship;
        assert_eq!(second.direction, RelationshipDirection::RightToLeft);
        assert_eq!(second.variable.as_ref().unwrap().name, "s");
        assert_eq!(second.rel_types[0].name, "LIKES");

        let third = &query.match_clauses[0].patterns[2].chains[0].relationship;
        assert_eq!(third.direction, RelationshipDirection::Undirected);
        assert!(third.variable.is_none());
        assert!(third.rel_types.is_empty());
    }

    #[test]
    fn parses_ordered_optional_match_clauses() {
        let query = parse_ok(
            "MATCH (a) WHERE a.id = $id OPTIONAL MATCH (a)-[:KNOWS]->(b) WHERE b.active = true OPTIONAL MATCH (b)-[:LIKES]->(c) RETURN a, b, c",
        );
        assert_eq!(query.match_clauses.len(), 3);
        assert!(!query.match_clauses[0].optional);
        assert!(query.match_clauses[1].optional);
        assert!(query.match_clauses[2].optional);
        assert!(query.match_clauses[0].where_clause.is_some());
        assert!(query.match_clauses[1].where_clause.is_some());
        assert!(query.match_clauses[2].where_clause.is_none());
    }

    #[test]
    fn parses_path_assignment_and_bounded_relationship_quantifiers() {
        for (source, min_hops, max_hops) in [
            ("MATCH p = (a)-[:KNOWS*0..0]->(b) RETURN p", 0, 0),
            ("MATCH p = (a)-[:KNOWS*0..1]->(b) RETURN p", 0, 1),
            ("MATCH p = (a)-[:KNOWS*1..1]->(b) RETURN p", 1, 1),
            ("MATCH p = (a)-[:KNOWS*1..3]->(b) RETURN p", 1, 3),
            ("MATCH p = (a)-[:KNOWS*2]->(b) RETURN p", 2, 2),
        ] {
            let query = parse_ok(source);
            let pattern = &query.match_clauses[0].patterns[0];
            assert_eq!(pattern.path_variable.as_ref().unwrap().name, "p");
            let relationship = &pattern.chains[0].relationship;
            assert_eq!(relationship.rel_types[0].name, "KNOWS");
            let quantifier = relationship.quantifier.as_ref().unwrap();
            assert_eq!(quantifier.min_hops, min_hops);
            assert_eq!(quantifier.max_hops, max_hops);
            assert!(quantifier.span.length > 0);
        }
    }

    #[test]
    fn parses_supported_path_functions() {
        let query = parse_ok(
            "MATCH p = (a)-[:KNOWS*1..3]->(b) RETURN length(p), start_node(p), end_node(p), nodes(p), relationships(p), node_ids(p), edge_ids(p)",
        );
        let ReturnBody::Items(items) = &query.return_clause.body else {
            panic!("expected explicit return items");
        };
        let names = items
            .iter()
            .map(|item| match &item.expr.kind {
                ExprKind::FunctionCall { name, .. } => name.name.as_str(),
                other => panic!("expected function call, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            vec![
                "length",
                "start_node",
                "end_node",
                "nodes",
                "relationships",
                "node_ids",
                "edge_ids"
            ]
        );
    }

    #[test]
    fn parses_property_maps_in_node_and_relationship_patterns() {
        let query = parse_ok(
            r#"MATCH (a:User {id: $id})-[r:BOUGHT {sku: "sku-1", qty: 2}]->(b:Item) RETURN r"#,
        );
        let pattern = &query.match_clauses[0].patterns[0];
        assert_eq!(
            pattern
                .start
                .properties
                .as_ref()
                .unwrap()
                .entries
                .first()
                .unwrap()
                .key
                .name,
            "id"
        );
        let rel_props = pattern.chains[0].relationship.properties.as_ref().unwrap();
        assert_eq!(rel_props.entries.len(), 2);
        assert_eq!(rel_props.entries[0].key.name, "sku");
        assert_eq!(rel_props.entries[1].key.name, "qty");
    }

    #[test]
    fn parses_where_precedence_and_parentheses() {
        let query = parse_ok(
            "MATCH (n) WHERE NOT n.deleted = true AND (n.age >= 18 OR n.name = \"Ada\") RETURN n",
        );
        let where_expr = query.match_clauses[0].where_clause.as_ref().unwrap();
        let ExprKind::Binary {
            op: BinaryOp::And,
            left,
            right,
        } = &where_expr.kind
        else {
            panic!("expected top-level AND, got {:?}", where_expr.kind);
        };

        let ExprKind::Binary {
            op: BinaryOp::Eq,
            left: not_expr,
            ..
        } = &left.kind
        else {
            panic!("expected comparison on left side, got {:?}", left.kind);
        };
        assert!(matches!(
            not_expr.kind,
            ExprKind::Unary {
                op: UnaryOp::Not,
                ..
            }
        ));

        assert!(matches!(
            right.kind,
            ExprKind::Binary {
                op: BinaryOp::Or,
                ..
            }
        ));
    }

    #[test]
    fn parses_in_and_null_predicates() {
        let query = parse_ok(
            r#"MATCH (n) WHERE n.status IN ["active", "pending"] AND n.deleted IS NULL OR n.name IS NOT NULL RETURN n"#,
        );
        let where_expr = query.match_clauses[0].where_clause.as_ref().unwrap();
        let ExprKind::Binary {
            op: BinaryOp::Or,
            left,
            right,
        } = &where_expr.kind
        else {
            panic!("expected OR, got {:?}", where_expr.kind);
        };
        assert!(matches!(
            left.kind,
            ExprKind::Binary {
                op: BinaryOp::And,
                ..
            }
        ));
        assert!(matches!(right.kind, ExprKind::IsNull { negated: true, .. }));
    }

    #[test]
    fn parses_return_items_aliases_duplicates_and_star() {
        let query = parse_ok("MATCH (n) RETURN n.name AS x, id(n) AS x, labels(n) AS labels");
        let ReturnBody::Items(items) = &query.return_clause.body else {
            panic!("expected return items");
        };
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].alias.as_ref().unwrap().name, "x");
        assert_eq!(items[1].alias.as_ref().unwrap().name, "x");
        assert!(matches!(items[1].expr.kind, ExprKind::FunctionCall { .. }));

        let star = parse_ok("MATCH (n)-[r:KNOWS]->(m) RETURN *");
        assert!(matches!(star.return_clause.body, ReturnBody::All(_)));
    }

    #[test]
    fn parses_order_skip_offset_and_limit() {
        let query =
            parse_ok("MATCH (n) RETURN n ORDER BY n.name DESC, id(n) ASC SKIP 5 LIMIT $limit");
        assert_eq!(query.order_by.len(), 2);
        assert_eq!(query.order_by[0].direction, OrderDirection::Desc);
        assert_eq!(query.order_by[1].direction, OrderDirection::Asc);
        assert!(matches!(
            query.skip.as_ref().unwrap().kind,
            ExprKind::Literal(Literal::Int(5))
        ));
        assert!(matches!(
            query.limit.as_ref().unwrap().kind,
            ExprKind::Parameter(ref name) if name == "limit"
        ));

        let offset = parse_ok("MATCH (n) RETURN n OFFSET 10 LIMIT 20");
        assert!(matches!(
            offset.skip.as_ref().unwrap().kind,
            ExprKind::Literal(Literal::Int(10))
        ));
        assert!(matches!(
            offset.limit.as_ref().unwrap().kind,
            ExprKind::Literal(Literal::Int(20))
        ));
    }

    #[test]
    fn preserves_parameter_spans() {
        let source = "MATCH (n {name: $name}) RETURN n.name";
        let query = parse_ok(source);
        let props = query.match_clauses[0].patterns[0]
            .start
            .properties
            .as_ref()
            .unwrap();
        let param = &props.entries[0].value;
        assert!(matches!(param.kind, ExprKind::Parameter(ref name) if name == "name"));
        let expected = source.find("$name").unwrap();
        assert_eq!(param.span.offset, expected);
        assert_eq!(param.span.length, "$name".len());
    }

    #[test]
    fn parses_string_escapes_and_utf8_byte_spans() {
        let source = "MATCH (n {name: \"é\", note: 'a\\'b\\n'}) RETURN n.name";
        let query = parse_ok(source);
        let props = query.match_clauses[0].patterns[0]
            .start
            .properties
            .as_ref()
            .unwrap();
        let name_value = &props.entries[0].value;
        assert!(matches!(
            name_value.kind,
            ExprKind::Literal(Literal::String(ref value)) if value == "é"
        ));
        assert_eq!(name_value.span.offset, source.find("\"é\"").unwrap());
        assert_eq!(name_value.span.length, "\"é\"".len());

        let note_value = &props.entries[1].value;
        assert!(matches!(
            note_value.kind,
            ExprKind::Literal(Literal::String(ref value)) if value == "a'b\n"
        ));
    }

    #[test]
    fn reports_line_and_column_across_multiline_queries() {
        let source = "MATCH (n)\nWHERE n.name = $name\nRETURN n";
        let query = parse_ok(source);
        let where_expr = query.match_clauses[0].where_clause.as_ref().unwrap();
        let ExprKind::Binary { right, .. } = &where_expr.kind else {
            panic!("expected comparison");
        };
        assert!(matches!(right.kind, ExprKind::Parameter(ref name) if name == "name"));
        assert_eq!(right.span.line, 2);
        assert_eq!(right.span.column, 16);
    }

    #[test]
    fn enforces_query_byte_cap_before_lexing() {
        let source = "MATCH (n) RETURN n";
        let err = parse_with_options(
            source,
            GqlParseOptions {
                max_query_bytes: 5,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("byte cap should fail");
        let EngineError::GqlParse { span, message } = err else {
            panic!("expected parse cap error");
        };
        assert!(message.contains("max_query_bytes"));
        assert_eq!(span.offset, 5);
        assert_eq!(span.line, 1);
        assert_eq!(span.column, 6);
    }

    #[test]
    fn enforces_ast_depth_cap_on_nested_expressions() {
        let source = "MATCH (n) RETURN [[[[[$x]]]]]";
        let err = parse_with_options(
            source,
            GqlParseOptions {
                max_ast_depth: 3,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("depth cap should fail");
        let EngineError::GqlParse { span, message } = err else {
            panic!("expected parse depth error");
        };
        assert!(message.contains("max_ast_depth"));
        assert_eq!(span.offset, source.find("[[[[[$x").unwrap() + 3);

        let function_err = parse_with_options(
            "MATCH (n) RETURN id(id(id(n)))",
            GqlParseOptions {
                max_ast_depth: 2,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("function nesting should count toward depth cap");
        assert!(matches!(function_err, EngineError::GqlParse { .. }));

        let or_chain_err = parse_with_options(
            "MATCH (n) WHERE a OR b OR c RETURN n",
            GqlParseOptions {
                max_ast_depth: 2,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("binary chains should count toward depth cap");
        assert!(matches!(or_chain_err, EngineError::GqlParse { .. }));

        let and_chain_err = parse_with_options(
            "MATCH (n) WHERE a AND b AND c RETURN n",
            GqlParseOptions {
                max_ast_depth: 2,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("AND chains should count toward depth cap");
        assert!(matches!(and_chain_err, EngineError::GqlParse { .. }));

        let property_chain_err = parse_with_options(
            "MATCH (n) RETURN n.a.b.c",
            GqlParseOptions {
                max_ast_depth: 3,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("property-access chains should count toward depth cap");
        assert!(matches!(property_chain_err, EngineError::GqlParse { .. }));

        let zero_depth_err = parse_with_options(
            "MATCH (n) RETURN n",
            GqlParseOptions {
                max_ast_depth: 0,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("zero depth cap should fail before parsing");
        assert!(matches!(zero_depth_err, EngineError::GqlParse { .. }));
    }

    #[test]
    fn enforces_literal_item_caps_for_large_lists_and_maps() {
        let list_err = parse_with_options(
            "MATCH (n) RETURN [1, 2, 3]",
            GqlParseOptions {
                max_literal_items: 2,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("list cap should fail");
        assert!(matches!(list_err, EngineError::GqlParse { .. }));

        let map_err = parse_with_options(
            "MATCH (n) RETURN {a: 1, b: 2, c: 3}",
            GqlParseOptions {
                max_literal_items: 2,
                ..GqlParseOptions::default()
            },
        )
        .expect_err("map cap should fail");
        assert!(matches!(map_err, EngineError::GqlParse { .. }));
    }

    #[test]
    fn rejects_unsupported_features_with_spans() {
        let cases = [
            ("CREATE (n) RETURN n", "write clauses"),
            (
                "CREATE INDEX node_status FOR (n:User) ON (n.status)",
                "schema/DDL",
            ),
            (
                "CREATE TEXT INDEX node_status FOR (n:User) ON (n.status)",
                "schema/DDL",
            ),
            ("CREATE DATABASE overgraph", "schema/DDL"),
            (
                "GRAPH overgraph MATCH (n) RETURN n",
                "graph catalog/session selection syntax",
            ),
            (
                "FROM GRAPH overgraph MATCH (n) RETURN n",
                "graph catalog/session selection syntax",
            ),
            (
                "USE overgraph MATCH (n) RETURN n",
                "graph catalog/session selection syntax",
            ),
            ("MATCH (n)-[*]->(m) RETURN n", "unbounded VLP"),
            ("MATCH (n) RETURN count(n)", "aggregation"),
            ("MATCH (n) RETURN DISTINCT n", "DISTINCT"),
            ("MATCH (n) WITH n RETURN n", "WITH"),
            ("MATCH (n) RETURN n UNION MATCH (m) RETURN m", "UNION"),
            ("CALL db.labels()", "CALL"),
            ("CALL { MATCH (n) RETURN n } RETURN n", "subqueries"),
            ("MATCH (n:$(label)) RETURN n", "dynamic labels"),
            (
                "MATCH (n)-[r:$(rel_label)]->(m) RETURN r",
                "dynamic relationship types",
            ),
            (
                "MATCH (n)-[r:A|$(rel_label)]->(m) RETURN r",
                "dynamic relationship types",
            ),
            (
                "MATCH (n)-[r:A|:$(rel_label)]->(m) RETURN r",
                "dynamic relationship types",
            ),
            (
                "MATCH (n)-[:T]->{1,3}(m) RETURN n",
                "variable-length relationship syntax",
            ),
            ("MATCH (n)--(m){1,3} RETURN n", "Graph Pattern v2"),
            (
                "MATCH shortestPath((a)--(b)) RETURN *",
                "shortest-path syntax",
            ),
            (
                "MATCH p = shortestPath((a)--(b)) RETURN p",
                "shortest-path syntax",
            ),
            ("MATCH SHORTEST (a)--(b) RETURN *", "shortest-path syntax"),
            (
                "MATCH ANY SHORTEST (a)--(b) RETURN *",
                "shortest-path syntax",
            ),
            (
                "MATCH ALL SHORTEST (a)--(b) RETURN *",
                "shortest-path syntax",
            ),
            ("MATCH (n) RETURN exists(n.name)", "EXISTS"),
        ];

        for (query, feature) in cases {
            expect_unsupported(query, feature);
        }
    }

    #[test]
    fn reports_reviewed_unsupported_feature_spans() {
        let cases = [
            (
                "CREATE TEXT INDEX node_status FOR (n:User) ON (n.status)",
                "schema/DDL",
                "CREATE",
                "CREATE".len(),
            ),
            (
                "MATCH (n)-[r:A|$(rel_label)]->(m) RETURN r",
                "dynamic relationship types",
                "$(",
                1,
            ),
            (
                "MATCH (n)-[r:A|:$(rel_label)]->(m) RETURN r",
                "dynamic relationship types",
                ":$(",
                1,
            ),
            (
                "MATCH (n)-[:T]->{1,3}(m) RETURN n",
                "variable-length relationship syntax",
                "{1,3}",
                1,
            ),
            (
                "MATCH (n)--(m){1,3} RETURN n",
                "Graph Pattern v2",
                "{1,3}",
                1,
            ),
            (
                "MATCH (n) RETURN exists(n.name)",
                "EXISTS",
                "exists",
                "exists".len(),
            ),
            (
                "MATCH ANY SHORTEST (a)--(b) RETURN *",
                "shortest-path syntax",
                "ANY",
                "ANY".len(),
            ),
            (
                "GRAPH overgraph MATCH (n) RETURN n",
                "graph catalog/session selection syntax",
                "GRAPH",
                "GRAPH".len(),
            ),
            (
                "FROM GRAPH overgraph MATCH (n) RETURN n",
                "graph catalog/session selection syntax",
                "FROM",
                "FROM".len(),
            ),
        ];

        for (query, feature, needle, expected_length) in cases {
            let span = expect_unsupported(query, feature);
            assert_eq!(span.offset, query.find(needle).unwrap(), "query: {query}");
            assert_eq!(span.length, expected_length, "query: {query}");
        }
    }

    #[test]
    fn rejects_multistatement_queries() {
        let span = expect_parse_error("MATCH (n) RETURN n; MATCH (m) RETURN m");
        assert_eq!(span.offset, "MATCH (n) RETURN n".len());
        assert_eq!(span.length, 1);
    }

    #[test]
    fn rejects_pattern_v2_syntax_without_partial_acceptance() {
        let span = expect_unsupported(
            "MATCH (a)-[r WHERE r.since > 2020]->(b) RETURN r",
            "Graph Pattern v2",
        );
        assert_eq!(
            span.offset,
            "MATCH (a)-[r ".len(),
            "unsupported span should point at pattern-local WHERE"
        );
    }

    #[test]
    fn invalid_syntax_reports_tight_practical_span() {
        let source = "MATCH (n RETURN n";
        let span = expect_parse_error(source);
        assert_eq!(span.offset, source.find("RETURN").unwrap());
        assert_eq!(span.length, "RETURN".len());
    }

    #[test]
    fn parses_parameters_property_access_literals_lists_maps_and_functions() {
        let query = parse_ok(
            r#"MATCH (n) WHERE id(n) IN $ids RETURN n.name AS name, labels(n) AS labels, type(r) AS rel, {a: [null, true, false, 1, 2.5]} AS payload"#,
        );
        let where_expr = query.match_clauses[0].where_clause.as_ref().unwrap();
        assert!(matches!(
            where_expr.kind,
            ExprKind::Binary {
                op: BinaryOp::In,
                ..
            }
        ));
        let ReturnBody::Items(items) = &query.return_clause.body else {
            panic!("expected return items");
        };
        assert_eq!(property_access_name(&items[0].expr), "name");
        assert_eq!(items[0].alias.as_ref().unwrap().name, "name");
        assert!(matches!(items[1].expr.kind, ExprKind::FunctionCall { .. }));
        assert!(matches!(items[2].expr.kind, ExprKind::FunctionCall { .. }));
        assert!(matches!(items[3].expr.kind, ExprKind::Map(_)));
    }

    #[test]
    fn parse_statement_classifies_reads_as_query() {
        let statement = parse_statement_ok("MATCH (n:Person) RETURN n ORDER BY n.name LIMIT 10");
        assert_eq!(statement.kind, GqlStatementKind::Query);
        let GqlStatementBody::Query(query) = statement.body else {
            panic!("expected query statement");
        };
        assert_eq!(query.match_clauses.len(), 1);
        assert_eq!(query.order_by.len(), 1);
        assert!(query.limit.is_some());
    }

    #[test]
    fn parse_statement_accepts_basic_mutation_skeletons() {
        let cases = [
            "CREATE (n:Person {key: $key}) RETURN n",
            "MATCH (n:Person) SET n.name = $name RETURN n",
            "MATCH (n:Person) SET n += $map RETURN n",
            "MATCH (n:Person) REMOVE n.name RETURN n",
            "MATCH (n:Person) REMOVE n:Old RETURN n",
            "MATCH ()-[r:LIKES]->() DELETE r",
            "MATCH (n:Person) DETACH DELETE n",
        ];
        for source in cases {
            let statement = parse_statement_ok(source);
            assert_eq!(
                statement.kind,
                GqlStatementKind::Mutation,
                "source: {source}"
            );
            let GqlStatementBody::Mutation(mutation) = statement.body else {
                panic!("expected mutation statement for {source}");
            };
            assert!(
                !mutation.mutation_clauses.is_empty(),
                "expected mutation clauses for {source}"
            );
        }
    }

    #[test]
    fn parse_statement_parses_set_map_merge_item() {
        let statement = parse_statement_ok("MATCH (n:Person) SET n += $map RETURN n");
        let GqlStatementBody::Mutation(mutation) = statement.body else {
            panic!("expected mutation statement");
        };
        let MutationClause::Set(set) = &mutation.mutation_clauses[0] else {
            panic!("expected SET clause");
        };
        assert!(matches!(
            &set.items[0],
            SetItem::MapMerge { alias, value, .. }
                if alias.name == "n" && matches!(value.kind, ExprKind::Parameter(ref name) if name == "map")
        ));
    }

    #[test]
    fn parse_statement_rejects_mutation_row_ops_without_return() {
        for source in [
            "MATCH (n) SET n.name = 'Ada' ORDER BY n.name",
            "MATCH (n) SET n.name = 'Ada' SKIP 1",
            "MATCH (n) SET n.name = 'Ada' LIMIT 1",
        ] {
            match parse_statement_err(source) {
                EngineError::GqlUnsupported { feature, span, .. } => {
                    assert_eq!(feature, "mutation row operations without RETURN");
                    assert!(span.length > 0);
                }
                err => panic!("expected unsupported mutation row op for {source}, got {err:?}"),
            }
        }
    }

    #[test]
    fn parse_statement_rejects_read_after_write_matching() {
        match parse_statement_err("MATCH (n) CREATE (m:Person {key: 'm'}) MATCH (m) RETURN m") {
            EngineError::GqlUnsupported { feature, span, .. } => {
                assert_eq!(feature, "read-after-write matching");
                assert!(span.length > 0);
            }
            err => panic!("expected read-after-write unsupported error, got {err:?}"),
        }
    }

    #[test]
    fn parse_statement_rejects_comma_separated_mutation_read_prefix_patterns() {
        match parse_statement_err("MATCH (a:Person), (b:Person) SET a.name = b.name RETURN a") {
            EngineError::GqlUnsupported { feature, span, .. } => {
                assert_eq!(
                    feature,
                    "comma-separated mutation read-prefix pattern lists"
                );
                assert!(span.length > 0);
            }
            err => panic!("expected comma-separated read-prefix unsupported error, got {err:?}"),
        }
    }

    #[test]
    fn parse_statement_rejects_mutation_multistatement_scripts() {
        match parse_statement_err("CREATE (n:Person {key: 'n'}); CREATE (m:Person {key: 'm'})") {
            EngineError::GqlParse { message, span } => {
                assert_eq!(message, "multiple statements are not supported");
                assert!(span.length > 0);
            }
            err => panic!("expected parse error for mutation multistatement, got {err:?}"),
        }
    }

    #[test]
    fn parse_statement_rejects_unsupported_mutation_clauses() {
        for (source, expected_feature) in [
            ("WITH 1 AS n CREATE (m:Person {key: 'm'})", "WITH"),
            ("MATCH (n) MERGE (m:Person {key: 'm'})", "write clauses"),
            ("UNWIND [1] AS n CREATE (m:Person {key: 'm'})", "UNWIND"),
            ("CALL db.labels()", "CALL"),
            (
                "CREATE TEXT INDEX node_status FOR (n:User) ON (n.status)",
                "schema/DDL",
            ),
        ] {
            match parse_statement_err(source) {
                EngineError::GqlUnsupported { feature, span, .. } => {
                    assert_eq!(feature, expected_feature, "source: {source}");
                    assert!(span.length > 0);
                }
                err => panic!("expected unsupported error for {source}, got {err:?}"),
            }
        }
    }

    #[test]
    fn parse_statement_preserves_read_arithmetic_rejection() {
        let source = "MATCH (n) RETURN 1 + 2";
        let read_err = parse_err(source);
        let statement_err = parse_statement_err(source);
        match (read_err, statement_err) {
            (
                EngineError::GqlParse {
                    message: read_message,
                    span: read_span,
                },
                EngineError::GqlParse {
                    message: statement_message,
                    span: statement_span,
                },
            ) => {
                assert_eq!(read_message, statement_message);
                assert_eq!(read_span, statement_span);
            }
            other => panic!("expected matching parse errors, got {other:?}"),
        }
    }
}
