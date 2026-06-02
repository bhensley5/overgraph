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

struct ParsedReadBranch {
    clauses: Vec<GqlPipelineClause>,
    return_clause: ReturnClause,
    order_by: Vec<OrderItem>,
    skip: Option<Expr>,
    limit: Option<Expr>,
    span: SourceSpan,
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
            return self.parse_mutation_statement(Vec::new(), None);
        }

        if self.at_mutation_read_prefix_start() && self.has_top_level_mutation_before_return() {
            let (read_prefix, read_prefix_pipeline) = self.parse_mutation_read_prefix_pipeline()?;
            return self.parse_mutation_statement(read_prefix, Some(read_prefix_pipeline));
        }

        if self.at_match_clause_start() {
            let mut match_clauses = Vec::new();
            while self.at_regular_match_clause_start() {
                match_clauses.push(self.parse_match_clause()?);
            }
            if self.at_mutation_clause_start() {
                return self.parse_mutation_statement(match_clauses, None);
            }
            let query = self.parse_query_tail(match_clauses)?;
            return Ok(GqlStatement {
                kind: GqlStatementKind::Query,
                span: query.span.clone(),
                body: GqlStatementBody::Query(query),
            });
        }

        if self.at_call_subquery_start() {
            let query = self.parse_query_tail(Vec::new())?;
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
        if !self.at_match_clause_start() {
            return Err(self.parse_error_current("expected MATCH clause"));
        }
        while self.at_regular_match_clause_start() {
            match_clauses.push(self.parse_match_clause()?);
        }

        self.parse_query_tail(match_clauses)
    }

    fn parse_query_tail(
        &mut self,
        match_clauses: Vec<MatchClause>,
    ) -> Result<GqlQuery, EngineError> {
        let first_branch = self.parse_read_branch_tail(match_clauses.clone())?;
        let mut union_branches = Vec::new();
        while self.at_keyword(Keyword::Union) {
            let union = self.advance();
            let modifier = if self.token_word_eq(self.pos, "all") {
                self.advance();
                GqlUnionModifier::All
            } else {
                GqlUnionModifier::Distinct
            };
            self.reject_unsupported_clause()?;
            if !self.at_match_clause_start() {
                return Err(self.parse_error_current("expected MATCH after UNION"));
            }
            let mut branch_matches = Vec::new();
            while self.at_regular_match_clause_start() {
                branch_matches.push(self.parse_match_clause()?);
            }
            let branch = self.parse_read_branch_tail(branch_matches)?;
            union_branches.push(GqlUnionBranch {
                modifier,
                clauses: branch.clauses,
                span: branch.span,
                union_span: union.span,
            });
        }

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
            &first_branch
                .clauses
                .first()
                .map(gql_pipeline_clause_span)
                .unwrap_or_else(|| first_branch.return_clause.span.clone()),
            &self.previous_non_eof_span(),
        );
        let pipeline = GqlReadPipeline {
            clauses: first_branch.clauses,
            union_branches,
            span: span.clone(),
        };
        Ok(GqlQuery {
            match_clauses,
            return_clause: first_branch.return_clause,
            order_by: first_branch.order_by,
            skip: first_branch.skip,
            limit: first_branch.limit,
            pipeline,
            span,
        })
    }

    fn parse_read_branch_tail(
        &mut self,
        match_clauses: Vec<MatchClause>,
    ) -> Result<ParsedReadBranch, EngineError> {
        let mut clauses = Vec::new();
        if !match_clauses.is_empty() {
            clauses.push(GqlPipelineClause::Match(match_clauses.clone()));
        }
        self.parse_read_stage_sequence(&mut clauses)?;
        while self.at_keyword(Keyword::With) {
            clauses.push(GqlPipelineClause::Projection(
                self.parse_projection_clause(GqlProjectionKind::With)?,
            ));
            self.parse_read_stage_sequence(&mut clauses)?;
        }

        self.reject_unsupported_clause()?;
        let return_projection = self.parse_projection_clause(GqlProjectionKind::Return)?;
        let return_clause = ReturnClause {
            body: return_projection.body.clone(),
            distinct: return_projection.distinct,
            distinct_span: return_projection.distinct_span.clone(),
            span: return_projection.span.clone(),
        };
        let order_by = return_projection.order_by.clone();
        let skip = return_projection.skip.clone();
        let limit = return_projection.limit.clone();
        clauses.push(GqlPipelineClause::Projection(return_projection));

        let span = self.span_between(
            &clauses
                .first()
                .map(gql_pipeline_clause_span)
                .unwrap_or_else(|| return_clause.span.clone()),
            &self.previous_non_eof_span(),
        );
        Ok(ParsedReadBranch {
            clauses,
            return_clause,
            order_by,
            skip,
            limit,
            span,
        })
    }

    fn parse_read_stage_sequence(
        &mut self,
        clauses: &mut Vec<GqlPipelineClause>,
    ) -> Result<(), EngineError> {
        loop {
            if self.at_call_subquery_start() {
                clauses.push(GqlPipelineClause::Call(self.parse_call_subquery()?));
                continue;
            }
            if self.at_shortest_path_match_clause_start() {
                clauses.push(GqlPipelineClause::ShortestPath(
                    self.parse_shortest_path_clause()?,
                ));
                continue;
            }
            if self.at_regular_match_clause_start() {
                let mut matches = Vec::new();
                while self.at_regular_match_clause_start() {
                    matches.push(self.parse_match_clause()?);
                }
                if !matches.is_empty() {
                    clauses.push(GqlPipelineClause::Match(matches));
                }
                continue;
            }
            break;
        }
        Ok(())
    }

    fn parse_call_subquery(&mut self) -> Result<GqlCallSubquery, EngineError> {
        let start = self.expect_keyword(Keyword::Call, "expected CALL")?;
        self.expect_kind(
            |kind| matches!(kind, TokenKind::LBrace),
            "expected '{' after CALL",
        )?;
        let pipeline = self.parse_nested_read_pipeline()?;
        let end = self.expect_kind(
            |kind| matches!(kind, TokenKind::RBrace),
            "expected '}' to close CALL subquery",
        )?;
        Ok(GqlCallSubquery {
            pipeline: Box::new(pipeline),
            span: self.span_between(&start.span, &end.span),
        })
    }

    fn parse_nested_read_pipeline(&mut self) -> Result<GqlReadPipeline, EngineError> {
        let first_branch = self.parse_read_branch_tail(Vec::new())?;
        let mut union_branches = Vec::new();
        while self.at_keyword(Keyword::Union) {
            let union = self.advance();
            let modifier = if self.token_word_eq(self.pos, "all") {
                self.advance();
                GqlUnionModifier::All
            } else {
                GqlUnionModifier::Distinct
            };
            self.reject_unsupported_clause()?;
            let branch = self.parse_read_branch_tail(Vec::new())?;
            union_branches.push(GqlUnionBranch {
                modifier,
                clauses: branch.clauses,
                span: branch.span,
                union_span: union.span,
            });
        }
        let span = self.span_between(
            &first_branch
                .clauses
                .first()
                .map(gql_pipeline_clause_span)
                .unwrap_or_else(|| first_branch.return_clause.span.clone()),
            &self.previous_non_eof_span(),
        );
        Ok(GqlReadPipeline {
            clauses: first_branch.clauses,
            union_branches,
            span,
        })
    }

    fn parse_mutation_statement(
        &mut self,
        read_prefix: Vec<MatchClause>,
        read_prefix_pipeline: Option<GqlReadPipeline>,
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
            if self.at_read_after_write_clause_start() {
                return Err(EngineError::GqlUnsupported {
                    feature: "read-after-write clauses".to_string(),
                    message: "MATCH, WITH, CALL, UNION, and subquery read stages must appear before mutation clauses"
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
        if self.at_read_after_write_clause_start() {
            return Err(EngineError::GqlUnsupported {
                feature: "read-after-write clauses".to_string(),
                message: "MATCH, WITH, CALL, UNION, and subquery read stages must appear before mutation clauses"
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
            read_prefix_pipeline,
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
        } else if self.at_keyword(Keyword::Merge) {
            Ok(MutationClause::Merge(self.parse_merge_clause()?))
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

    fn parse_mutation_read_prefix_pipeline(
        &mut self,
    ) -> Result<(Vec<MatchClause>, GqlReadPipeline), EngineError> {
        let start = self.current().span.clone();
        let mut clauses = Vec::new();
        self.parse_read_stage_sequence(&mut clauses)?;
        while self.at_keyword(Keyword::With) {
            clauses.push(GqlPipelineClause::Projection(
                self.parse_projection_clause(GqlProjectionKind::With)?,
            ));
            self.parse_read_stage_sequence(&mut clauses)?;
        }
        if clauses.is_empty() {
            return Err(self.parse_error_current("expected read stage before mutation clause"));
        }
        if !self.at_mutation_clause_start() {
            return Err(self.parse_error_current("expected mutation clause after read prefix"));
        }
        let legacy_read_prefix = legacy_match_only_read_prefix(&clauses).unwrap_or_default();
        let span = self.span_between(&start, &self.previous_non_eof_span());
        Ok((
            legacy_read_prefix,
            GqlReadPipeline {
                clauses,
                union_branches: Vec::new(),
                span,
            },
        ))
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

    fn parse_merge_clause(&mut self) -> Result<MergeClause, EngineError> {
        let start = self.expect_keyword(Keyword::Merge, "expected MERGE clause")?;
        let pattern = self.parse_pattern()?;
        let mut on_create = None;
        let mut on_match = None;
        let mut end = pattern.span.clone();
        while self.at_keyword(Keyword::On) {
            let on = self.advance();
            if self.at_keyword(Keyword::Create) {
                if on_create.is_some() {
                    return Err(EngineError::GqlParse {
                        message: "MERGE supports at most one ON CREATE SET action".to_string(),
                        span: on.span,
                    });
                }
                self.expect_keyword(Keyword::Create, "expected CREATE after ON")?;
                let set = self.parse_set_clause()?;
                end = set.span.clone();
                on_create = Some(set);
            } else if self.at_keyword(Keyword::Match) {
                if on_match.is_some() {
                    return Err(EngineError::GqlParse {
                        message: "MERGE supports at most one ON MATCH SET action".to_string(),
                        span: on.span,
                    });
                }
                self.expect_keyword(Keyword::Match, "expected MATCH after ON")?;
                let set = self.parse_set_clause()?;
                end = set.span.clone();
                on_match = Some(set);
            } else {
                return Err(self.parse_error_current("expected CREATE or MATCH after ON"));
            }
        }
        Ok(MergeClause {
            pattern,
            on_create,
            on_match,
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

    fn parse_shortest_path_clause(&mut self) -> Result<GqlShortestPathClause, EngineError> {
        let optional = self.consume_keyword(Keyword::Optional);
        let start = self.expect_keyword(Keyword::Match, "expected MATCH clause")?;
        let clause_start = optional
            .as_ref()
            .map(|token| token.span.clone())
            .unwrap_or_else(|| start.span.clone());
        let output_path_alias =
            if self.current_is_ident() && self.next_is(|kind| matches!(kind, TokenKind::Equals)) {
                let ident = self.parse_ident("expected shortest-path alias")?;
                self.expect_kind(
                    |kind| matches!(kind, TokenKind::Equals),
                    "expected '=' after shortest-path alias",
                )?;
                ident
            } else {
                return Err(EngineError::GqlParse {
                    message: "shortest-path MATCH requires a path alias before '='".to_string(),
                    span: self.current().span.clone(),
                });
            };

        let function = self.parse_ident("expected shortest-path function")?;
        let mode = if function.name.eq_ignore_ascii_case("shortestPath") {
            GqlShortestPathMode::One
        } else if function.name.eq_ignore_ascii_case("allShortestPaths") {
            GqlShortestPathMode::All
        } else if is_shortest_path_function(&function.name) {
            return Err(EngineError::GqlUnsupported {
                feature: "shortest-path syntax".to_string(),
                message: format!(
                    "shortest-path function '{}' is not supported in the current GQL subset",
                    function.name
                ),
                span: function.span,
            });
        } else {
            return Err(EngineError::GqlParse {
                message: "expected shortestPath or allShortestPaths".to_string(),
                span: function.span,
            });
        };
        self.expect_kind(
            |kind| matches!(kind, TokenKind::LParen),
            "expected '(' after shortest-path function",
        )?;
        let pattern = self.parse_pattern()?;
        let close = self.expect_kind(
            |kind| matches!(kind, TokenKind::RParen),
            "expected ')' after shortest-path pattern",
        )?;
        validate_shortest_path_pattern(self, &pattern)?;
        Ok(GqlShortestPathClause {
            optional: optional.is_some(),
            output_path_alias,
            mode,
            pattern,
            span: self.span_between(&clause_start, &close.span),
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
        let distinct_span = self
            .consume_keyword(Keyword::Distinct)
            .map(|token| token.span);
        let distinct = distinct_span.is_some();
        let body = self.parse_projection_body(false)?;
        let end = projection_body_end(&body).clone();
        Ok(ReturnClause {
            body,
            distinct,
            distinct_span,
            span: self.span_between(&start.span, &end),
        })
    }

    fn parse_projection_clause(
        &mut self,
        kind: GqlProjectionKind,
    ) -> Result<GqlProjectionClause, EngineError> {
        let start = match kind {
            GqlProjectionKind::With => {
                self.expect_keyword(Keyword::With, "expected WITH clause")?
            }
            GqlProjectionKind::Return => {
                self.expect_keyword(Keyword::Return, "expected RETURN clause")?
            }
        };
        let distinct_span = self
            .consume_keyword(Keyword::Distinct)
            .map(|token| token.span);
        let distinct = distinct_span.is_some();
        let body = self.parse_projection_body(kind == GqlProjectionKind::With)?;
        let mut end = projection_body_end(&body).clone();
        let order_by = if self.at_keyword(Keyword::Order) {
            let order_by = self.parse_order_by()?;
            if let Some(last) = order_by.last() {
                end = last.span.clone();
            }
            order_by
        } else {
            Vec::new()
        };
        let mut skip = None;
        if self.at_keyword(Keyword::Skip) || self.at_keyword(Keyword::Offset) {
            let expr = self.parse_skip_or_offset()?;
            end = expr.span.clone();
            skip = Some(expr);
        }
        if self.at_keyword(Keyword::Skip) || self.at_keyword(Keyword::Offset) {
            return Err(self.parse_error_current("SKIP and OFFSET cannot both be specified"));
        }
        let limit = if self.consume_keyword(Keyword::Limit).is_some() {
            let expr = self.parse_expression(0)?.expr;
            end = expr.span.clone();
            Some(expr)
        } else {
            None
        };
        let where_clause =
            if kind == GqlProjectionKind::With && self.consume_keyword(Keyword::Where).is_some() {
                let expr = self.parse_expression(0)?.expr;
                end = expr.span.clone();
                Some(expr)
            } else {
                None
            };

        Ok(GqlProjectionClause {
            kind,
            distinct,
            distinct_span,
            body,
            where_clause,
            order_by,
            skip,
            limit,
            span: self.span_between(&start.span, &end),
        })
    }

    fn parse_projection_body(&mut self, allow_mixed_star: bool) -> Result<ReturnBody, EngineError> {
        if let Some(star) = self.consume_if(|kind| matches!(kind, TokenKind::Star)) {
            if self
                .consume_if(|kind| matches!(kind, TokenKind::Comma))
                .is_some()
            {
                if !allow_mixed_star {
                    return Err(EngineError::GqlUnsupported {
                        feature: "RETURN * with additional projection items".to_string(),
                        message:
                            "RETURN * with additional projection items is deferred until a later phase"
                                .to_string(),
                        span: star.span,
                    });
                }
                return Ok(ReturnBody::AllAndItems {
                    star_span: star.span,
                    items: self.parse_projection_items()?,
                });
            }
            return Ok(ReturnBody::All(star.span));
        }
        Ok(ReturnBody::Items(self.parse_projection_items()?))
    }

    fn parse_projection_items(&mut self) -> Result<Vec<ReturnItem>, EngineError> {
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
        Ok(items)
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
        let mut expr = self.parse_additive(depth)?;
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
            } else if self.consume_keyword(Keyword::Starts).is_some() {
                self.expect_keyword(Keyword::With, "expected WITH after STARTS")?;
                Some(BinaryOp::StartsWith)
            } else if self.consume_keyword(Keyword::Ends).is_some() {
                self.expect_keyword(Keyword::With, "expected WITH after ENDS")?;
                Some(BinaryOp::EndsWith)
            } else if self.consume_keyword(Keyword::Contains).is_some() {
                Some(BinaryOp::Contains)
            } else {
                None
            };
            if let Some(op) = op {
                let right = self.parse_additive(depth)?;
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

    fn parse_additive(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let mut expr = self.parse_multiplicative(depth)?;
        loop {
            let op = if self
                .consume_if(|kind| matches!(kind, TokenKind::Plus))
                .is_some()
            {
                Some(BinaryOp::Add)
            } else if self
                .consume_if(|kind| matches!(kind, TokenKind::Dash))
                .is_some()
            {
                Some(BinaryOp::Sub)
            } else {
                None
            };
            let Some(op) = op else { break };
            let right = self.parse_multiplicative(depth)?;
            expr = self.binary_expr(op, expr, right)?;
        }
        Ok(expr)
    }

    fn parse_multiplicative(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let mut expr = self.parse_unary(depth)?;
        loop {
            let op = if self
                .consume_if(|kind| matches!(kind, TokenKind::Star))
                .is_some()
            {
                Some(BinaryOp::Mul)
            } else if self
                .consume_if(|kind| matches!(kind, TokenKind::Slash))
                .is_some()
            {
                Some(BinaryOp::Div)
            } else {
                None
            };
            let Some(op) = op else { break };
            let right = self.parse_unary(depth)?;
            expr = self.binary_expr(op, expr, right)?;
        }
        Ok(expr)
    }

    fn parse_unary(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        if let Some(not) = self.consume_keyword(Keyword::Not) {
            self.check_depth(depth + 1, &not.span)?;
            let expr = self.parse_unary(depth + 1)?;
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
        } else if let Some(dash) = self.consume_if(|kind| matches!(kind, TokenKind::Dash)) {
            self.check_depth(depth + 1, &dash.span)?;
            let expr = self.parse_unary(depth + 1)?;
            let span = self.span_between(&dash.span, &expr.expr.span);
            let ast_depth = expr.ast_depth + 1;
            self.check_depth(ast_depth, &span)?;
            Ok(ParsedExpr {
                expr: Expr {
                    kind: ExprKind::Unary {
                        op: UnaryOp::Neg,
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
                let start = self.advance();
                self.expect_kind(
                    |kind| matches!(kind, TokenKind::LBrace),
                    "expected '{' after EXISTS",
                )?;
                let pipeline = self.parse_nested_read_pipeline()?;
                let end = self.expect_kind(
                    |kind| matches!(kind, TokenKind::RBrace),
                    "expected '}' to close EXISTS subquery",
                )?;
                Ok(Self::leaf_expr(Expr {
                    kind: ExprKind::ExistsSubquery(Box::new(pipeline)),
                    span: self.span_between(&start.span, &end.span),
                }))
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
            TokenKind::Keyword(Keyword::Case) => self.parse_case_expr(depth),
            _ => Err(self.parse_error_current("expected expression")),
        }
    }

    fn parse_case_expr(&mut self, depth: usize) -> Result<ParsedExpr, EngineError> {
        let start = self.expect_keyword(Keyword::Case, "expected CASE")?;
        self.check_depth(depth + 1, &start.span)?;

        let mut max_depth = 0usize;
        let operand = if self.at_keyword(Keyword::When) {
            None
        } else {
            let operand = self.parse_expression(depth + 1)?;
            max_depth = max_depth.max(operand.ast_depth);
            Some(Box::new(operand.expr))
        };

        let mut branches = Vec::new();
        while self.consume_keyword(Keyword::When).is_some() {
            let when = self.parse_expression(depth + 1)?;
            max_depth = max_depth.max(when.ast_depth);
            self.expect_keyword(Keyword::Then, "expected THEN after CASE WHEN expression")?;
            let then = self.parse_expression(depth + 1)?;
            max_depth = max_depth.max(then.ast_depth);
            branches.push(CaseBranch {
                when: when.expr,
                then: then.expr,
            });
        }

        if branches.is_empty() {
            return Err(EngineError::GqlParse {
                message: "CASE requires at least one WHEN branch".to_string(),
                span: start.span,
            });
        }

        let else_expr = if self.consume_keyword(Keyword::Else).is_some() {
            let expr = self.parse_expression(depth + 1)?;
            max_depth = max_depth.max(expr.ast_depth);
            Some(Box::new(expr.expr))
        } else {
            None
        };
        let end = self.expect_keyword(Keyword::End, "expected END to close CASE expression")?;
        let span = self.span_between(&start.span, &end.span);
        let ast_depth = max_depth + 1;
        self.check_depth(ast_depth, &span)?;
        Ok(ParsedExpr {
            expr: Expr {
                kind: ExprKind::Case {
                    operand,
                    branches,
                    else_expr,
                },
                span,
            },
            ast_depth,
        })
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
                message: "shortest-path functions are only supported in MATCH path clauses"
                    .to_string(),
                span: name_span,
            });
        }
        if let Some(function) = aggregate_function_from_name(&lower) {
            return self.parse_aggregate_call(function, name, name_span, depth);
        }
        if is_aggregation_function(&lower) {
            return Err(EngineError::GqlUnsupported {
                feature: "aggregation".to_string(),
                message: format!("aggregation function '{}' is not supported", name),
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

    fn parse_aggregate_call(
        &mut self,
        function: AggregateFunction,
        name: String,
        name_span: SourceSpan,
        depth: usize,
    ) -> Result<ParsedExpr, EngineError> {
        self.check_depth(depth + 1, &name_span)?;
        self.advance();
        self.expect_kind(
            |kind| matches!(kind, TokenKind::LParen),
            "expected '(' after aggregate function name",
        )?;
        let distinct_span = self
            .consume_keyword(Keyword::Distinct)
            .map(|token| token.span);
        let distinct = distinct_span.is_some();
        let (arg, max_arg_depth, end) =
            if let Some(star) = self.consume_if(|kind| matches!(kind, TokenKind::Star)) {
                if function != AggregateFunction::Count {
                    return Err(EngineError::GqlParse {
                        message: format!("aggregate function '{}' does not accept '*'", name),
                        span: star.span,
                    });
                }
                if distinct {
                    return Err(EngineError::GqlParse {
                        message: "count(DISTINCT *) is not supported".to_string(),
                        span: distinct_span.unwrap_or(star.span.clone()),
                    });
                }
                let end = self.expect_kind(
                    |kind| matches!(kind, TokenKind::RParen),
                    "expected ')' after aggregate arguments",
                )?;
                (None, 0, end)
            } else {
                if self.at_kind(|kind| matches!(kind, TokenKind::RParen)) {
                    return Err(EngineError::GqlParse {
                        message: format!("aggregate function '{}' expects an argument", name),
                        span: name_span.clone(),
                    });
                }
                let parsed = self.parse_expression(depth + 1)?;
                let end = self.expect_kind(
                    |kind| matches!(kind, TokenKind::RParen),
                    "expected ')' after aggregate arguments",
                )?;
                (Some(Box::new(parsed.expr)), parsed.ast_depth, end)
            };
        let span = self.span_between(&name_span, &end.span);
        let ast_depth = max_arg_depth + 1;
        self.check_depth(ast_depth, &span)?;
        Ok(ParsedExpr {
            expr: Expr {
                kind: ExprKind::AggregateCall {
                    function,
                    distinct,
                    arg,
                    name_span,
                },
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
                Keyword::Call => {
                    if self.next_is(|kind| matches!(kind, TokenKind::LBrace)) {
                        return Ok(());
                    }
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
            return Ok(());
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

    fn at_regular_match_clause_start(&self) -> bool {
        self.at_match_clause_start() && !self.at_shortest_path_match_clause_start()
    }

    fn at_call_subquery_start(&self) -> bool {
        self.at_keyword(Keyword::Call) && self.next_is(|kind| matches!(kind, TokenKind::LBrace))
    }

    fn at_shortest_path_match_clause_start(&self) -> bool {
        let mut index = self.pos;
        if self.token_word_eq(index, "optional") {
            index += 1;
        }
        if !self.token_word_eq(index, "match") {
            return false;
        }
        index += 1;

        if self
            .tokens
            .get(index)
            .is_some_and(|token| matches!(token.kind, TokenKind::Ident(_)))
            && self
                .tokens
                .get(index + 1)
                .is_some_and(|token| matches!(token.kind, TokenKind::Equals))
        {
            return self
                .tokens
                .get(index + 2)
                .and_then(|token| match &token.kind {
                    TokenKind::Ident(name) => Some(name.as_str()),
                    _ => None,
                })
                .is_some_and(is_shortest_path_function);
        }

        self.tokens
            .get(index)
            .and_then(|token| match &token.kind {
                TokenKind::Ident(name) => Some(name.as_str()),
                _ => None,
            })
            .is_some_and(is_shortest_path_function)
    }

    fn at_mutation_clause_start(&self) -> bool {
        match self.current().kind {
            TokenKind::Keyword(Keyword::Create) => !self.create_clause_is_schema_ddl(),
            TokenKind::Keyword(Keyword::Merge) => true,
            TokenKind::Keyword(
                Keyword::Set | Keyword::Remove | Keyword::Delete | Keyword::Detach,
            ) => true,
            _ => false,
        }
    }

    fn at_mutation_read_prefix_start(&self) -> bool {
        self.at_match_clause_start()
            || self.at_call_subquery_start()
            || self.at_keyword(Keyword::With)
    }

    fn at_read_after_write_clause_start(&self) -> bool {
        self.at_match_clause_start()
            || self.at_call_subquery_start()
            || self.at_keyword(Keyword::With)
            || self.at_keyword(Keyword::Union)
    }

    fn has_top_level_mutation_before_return(&self) -> bool {
        let mut depth = 0usize;
        for token in self.tokens.iter().skip(self.pos) {
            match &token.kind {
                TokenKind::Eof | TokenKind::Semicolon => return false,
                TokenKind::LParen | TokenKind::LBracket | TokenKind::LBrace => {
                    depth = depth.saturating_add(1);
                }
                TokenKind::RParen | TokenKind::RBracket | TokenKind::RBrace => {
                    depth = depth.saturating_sub(1);
                }
                TokenKind::Keyword(keyword) if depth == 0 => match keyword {
                    Keyword::Return => return false,
                    Keyword::Create
                    | Keyword::Merge
                    | Keyword::Set
                    | Keyword::Remove
                    | Keyword::Delete
                    | Keyword::Detach => return true,
                    _ => {}
                },
                _ => {}
            }
        }
        false
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

fn gql_pipeline_clause_span(clause: &GqlPipelineClause) -> SourceSpan {
    match clause {
        GqlPipelineClause::Match(clauses) => clauses
            .first()
            .map(|clause| clause.span.clone())
            .unwrap_or_else(|| SourceSpan::new(0, 0, 1, 1)),
        GqlPipelineClause::ShortestPath(shortest) => shortest.span.clone(),
        GqlPipelineClause::Call(call) => call.span.clone(),
        GqlPipelineClause::Projection(projection) => projection.span.clone(),
    }
}

fn legacy_match_only_read_prefix(clauses: &[GqlPipelineClause]) -> Option<Vec<MatchClause>> {
    let mut matches = Vec::new();
    for clause in clauses {
        match clause {
            GqlPipelineClause::Match(clauses) => matches.extend(clauses.iter().cloned()),
            GqlPipelineClause::ShortestPath(_)
            | GqlPipelineClause::Call(_)
            | GqlPipelineClause::Projection(_) => return None,
        }
    }
    Some(matches)
}

fn validate_shortest_path_pattern(
    parser: &Parser<'_>,
    pattern: &Pattern,
) -> Result<(), EngineError> {
    if let Some(path_variable) = pattern.path_variable.as_ref() {
        return Err(EngineError::GqlUnsupported {
            feature: "shortest-path syntax".to_string(),
            message:
                "shortest-path MATCH uses the alias before '='; nested path aliases are not supported"
                    .to_string(),
            span: path_variable.span.clone(),
        });
    }
    if pattern.chains.len() != 1 {
        return Err(EngineError::GqlUnsupported {
            feature: "shortest-path syntax".to_string(),
            message: "shortest-path MATCH supports exactly one relationship pattern".to_string(),
            span: pattern.span.clone(),
        });
    }
    let relationship = &pattern.chains[0].relationship;
    if let Some(variable) = relationship.variable.as_ref() {
        return Err(EngineError::GqlUnsupported {
            feature: "shortest-path relationship alias".to_string(),
            message: "relationship aliases are not supported inside shortest-path MATCH"
                .to_string(),
            span: variable.span.clone(),
        });
    }
    if relationship.properties.is_some() {
        return Err(EngineError::GqlUnsupported {
            feature: "weighted GQL shortest path syntax".to_string(),
            message:
                "relationship properties and weighted shortest-path syntax are not supported in GQL"
                    .to_string(),
            span: relationship.span.clone(),
        });
    }
    let Some(quantifier) = relationship.quantifier.as_ref() else {
        return Err(EngineError::GqlParse {
            message: "shortest-path relationship patterns require '*min..max' hop bounds"
                .to_string(),
            span: relationship.span.clone(),
        });
    };
    let quantifier_source = parser.source_for_span(&quantifier.span);
    if !quantifier_source.contains("..")
        || quantifier_source
            .strip_prefix('*')
            .is_some_and(|rest| rest.starts_with(".."))
    {
        return Err(EngineError::GqlParse {
            message: "shortest-path relationship patterns require both min and max hop bounds"
                .to_string(),
            span: quantifier.span.clone(),
        });
    }
    Ok(())
}

fn projection_body_end(body: &ReturnBody) -> &SourceSpan {
    match body {
        ReturnBody::All(span) => span,
        ReturnBody::AllAndItems { items, .. } | ReturnBody::Items(items) => {
            &items
                .last()
                .expect("projection items must be non-empty")
                .span
        }
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

fn aggregate_function_from_name(lower: &str) -> Option<AggregateFunction> {
    Some(match lower {
        "count" => AggregateFunction::Count,
        "sum" => AggregateFunction::Sum,
        "avg" => AggregateFunction::Avg,
        "min" => AggregateFunction::Min,
        "max" => AggregateFunction::Max,
        "collect" => AggregateFunction::Collect,
        _ => return None,
    })
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
            | "coalesce"
            | "to_string"
            | "to_integer"
            | "to_float"
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
    fn parses_supported_shortest_path_clauses() {
        let one = parse_ok("MATCH p = shortestPath((a)-[:TYPE*1..5]->(b)) RETURN p");
        assert!(matches!(
            &one.pipeline.clauses[0],
            GqlPipelineClause::ShortestPath(GqlShortestPathClause {
                mode: GqlShortestPathMode::One,
                output_path_alias,
                ..
            }) if output_path_alias.name == "p"
        ));

        let all = parse_ok("MATCH p = allShortestPaths((a)-[:TYPE*1..5]-(b)) RETURN p");
        assert!(matches!(
            &all.pipeline.clauses[0],
            GqlPipelineClause::ShortestPath(GqlShortestPathClause {
                mode: GqlShortestPathMode::All,
                pattern,
                ..
            }) if pattern.chains[0].relationship.direction == RelationshipDirection::Undirected
        ));
    }

    #[test]
    fn rejects_invalid_shortest_path_clause_shapes() {
        let missing_alias = parse_err("MATCH shortestPath((a)-[:TYPE*1..5]->(b)) RETURN *");
        assert!(matches!(missing_alias, EngineError::GqlParse { .. }));

        let exact_bound = parse_err("MATCH p = shortestPath((a)-[:TYPE*1]->(b)) RETURN p");
        assert!(matches!(exact_bound, EngineError::GqlParse { .. }));

        let missing_min = parse_err("MATCH p = shortestPath((a)-[:TYPE*..5]->(b)) RETURN p");
        assert!(matches!(missing_min, EngineError::GqlParse { .. }));

        let weighted = parse_err("MATCH p = shortestPath((a)-[:TYPE*1..5 {w: 1}]->(b)) RETURN p");
        assert!(matches!(
            weighted,
            EngineError::GqlUnsupported { feature, .. } if feature == "weighted GQL shortest path syntax"
        ));
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
    fn parses_arithmetic_precedence_and_unary_minus() {
        let query = parse_ok("MATCH (n) RETURN 1 + 2 * 3, (1 + 2) * 3, -n.age, -(1 + 2)");
        let ReturnBody::Items(items) = &query.return_clause.body else {
            panic!("expected return items");
        };
        assert!(matches!(
            items[0].expr.kind,
            ExprKind::Binary {
                op: BinaryOp::Add,
                ..
            }
        ));
        let ExprKind::Binary {
            op: BinaryOp::Add,
            right,
            ..
        } = &items[0].expr.kind
        else {
            panic!("expected addition");
        };
        assert!(matches!(
            right.kind,
            ExprKind::Binary {
                op: BinaryOp::Mul,
                ..
            }
        ));
        assert!(matches!(
            items[1].expr.kind,
            ExprKind::Binary {
                op: BinaryOp::Mul,
                ..
            }
        ));
        assert!(matches!(
            items[2].expr.kind,
            ExprKind::Unary {
                op: UnaryOp::Neg,
                ..
            }
        ));
        assert!(matches!(
            items[3].expr.kind,
            ExprKind::Unary {
                op: UnaryOp::Neg,
                ..
            }
        ));
    }

    #[test]
    fn parses_string_predicates_case_and_scalar_functions() {
        let query = parse_ok(
            "MATCH (n) WHERE lower(n.name) STARTS WITH 'a' AND n.name ENDS WITH 'z' AND n.name CONTAINS 'd' RETURN CASE WHEN n.age > 1 THEN upper(n.name) ELSE trim(' x ') END AS generic, CASE n.status WHEN 'a' THEN to_string(1) END AS simple, substring(n.name, 1, 2) AS sub",
        );
        let where_expr = query.match_clauses[0].where_clause.as_ref().unwrap();
        assert!(format!("{:?}", where_expr.kind).contains("StartsWith"));
        assert!(format!("{:?}", where_expr.kind).contains("EndsWith"));
        assert!(format!("{:?}", where_expr.kind).contains("Contains"));
        let ReturnBody::Items(items) = &query.return_clause.body else {
            panic!("expected return items");
        };
        assert!(matches!(
            items[0].expr.kind,
            ExprKind::Case { operand: None, .. }
        ));
        assert!(matches!(
            items[1].expr.kind,
            ExprKind::Case {
                operand: Some(_),
                ..
            }
        ));
        assert!(matches!(items[2].expr.kind, ExprKind::FunctionCall { .. }));
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
    fn parses_with_pipeline_projection_stages() {
        let query = parse_ok("MATCH (n) WITH n RETURN n");
        assert_eq!(query.pipeline.clauses.len(), 3);
        assert!(matches!(
            query.pipeline.clauses[0],
            GqlPipelineClause::Match(_)
        ));
        let GqlPipelineClause::Projection(with) = &query.pipeline.clauses[1] else {
            panic!("expected WITH projection");
        };
        assert_eq!(with.kind, GqlProjectionKind::With);
        assert!(!with.distinct);
        assert_eq!(with.span.offset, "MATCH (n) ".len());
        let ReturnBody::Items(items) = &with.body else {
            panic!("expected WITH item body");
        };
        assert_eq!(items.len(), 1);
        assert!(matches!(items[0].expr.kind, ExprKind::Variable(ref name) if name == "n"));
        let GqlPipelineClause::Projection(ret) = &query.pipeline.clauses[2] else {
            panic!("expected RETURN projection");
        };
        assert_eq!(ret.kind, GqlProjectionKind::Return);
    }

    #[test]
    fn parses_repeated_with_star_and_distinct() {
        let repeated = parse_ok("MATCH (n) WITH n WITH n AS x RETURN x");
        assert_eq!(
            repeated
                .pipeline
                .clauses
                .iter()
                .filter(|clause| matches!(
                    clause,
                    GqlPipelineClause::Projection(GqlProjectionClause {
                        kind: GqlProjectionKind::With,
                        ..
                    })
                ))
                .count(),
            2
        );

        let star = parse_ok("MATCH (n) WITH * RETURN n");
        let GqlPipelineClause::Projection(with_star) = &star.pipeline.clauses[1] else {
            panic!("expected WITH projection");
        };
        assert!(matches!(with_star.body, ReturnBody::All(_)));

        let distinct_star = parse_ok("MATCH (n) WITH DISTINCT * RETURN n");
        let GqlPipelineClause::Projection(with_distinct_star) = &distinct_star.pipeline.clauses[1]
        else {
            panic!("expected WITH projection");
        };
        assert!(with_distinct_star.distinct);
        assert!(matches!(with_distinct_star.body, ReturnBody::All(_)));

        let mixed_star = parse_ok("MATCH (n) WITH *, n.name AS name RETURN name");
        let GqlPipelineClause::Projection(with_mixed_star) = &mixed_star.pipeline.clauses[1] else {
            panic!("expected WITH projection");
        };
        assert!(matches!(
            with_mixed_star.body,
            ReturnBody::AllAndItems { .. }
        ));
    }

    #[test]
    fn rejects_return_mixed_star_projections_for_reads_and_mutations() {
        for source in [
            "MATCH (n) RETURN *, n",
            "CREATE (n:Person {key: 'n'}) RETURN *, n",
        ] {
            match parse_statement_err(source) {
                EngineError::GqlUnsupported { feature, span, .. } => {
                    assert_eq!(feature, "RETURN * with additional projection items");
                    assert!(span.length > 0, "source: {source}");
                }
                err => panic!("expected mixed-star unsupported error for {source}, got {err:?}"),
            }
        }
    }

    #[test]
    fn parses_with_where_and_string_predicates() {
        let source = "MATCH (n) WITH n.name AS name WHERE name STARTS WITH 'a' AND name ENDS WITH 'z' RETURN name";
        let query = parse_ok(source);
        let GqlPipelineClause::Projection(with) = &query.pipeline.clauses[1] else {
            panic!("expected WITH projection");
        };
        let where_clause = with.where_clause.as_ref().expect("WITH WHERE");
        assert!(format!("{:?}", where_clause.kind).contains("StartsWith"));
        assert!(format!("{:?}", where_clause.kind).contains("EndsWith"));
        assert_eq!(
            where_clause.span.offset,
            source.find("name STARTS").unwrap()
        );
    }

    #[test]
    fn parses_with_projection_local_row_ops() {
        let source = "MATCH (n) WITH n ORDER BY n.name SKIP 1 LIMIT 2 WHERE n.active RETURN n";
        let query = parse_ok(source);
        let GqlPipelineClause::Projection(with) = &query.pipeline.clauses[1] else {
            panic!("expected WITH projection");
        };
        assert_eq!(with.order_by.len(), 1);
        assert!(with.where_clause.is_some());
        assert!(matches!(
            with.skip.as_ref().unwrap().kind,
            ExprKind::Literal(Literal::Int(1))
        ));
        assert!(matches!(
            with.limit.as_ref().unwrap().kind,
            ExprKind::Literal(Literal::Int(2))
        ));
        assert_eq!(with.order_by[0].span.offset, source.find("n.name").unwrap());
        assert_eq!(
            with.skip.as_ref().unwrap().span.offset,
            source.find("1").unwrap()
        );
        assert_eq!(
            with.limit.as_ref().unwrap().span.offset,
            source.find("2").unwrap()
        );
        assert_eq!(
            with.where_clause.as_ref().unwrap().span.offset,
            source.find("n.active").unwrap()
        );
    }

    #[test]
    fn rejects_with_where_before_projection_local_row_ops() {
        let err = parse_statement_err("MATCH (n) WITH n WHERE n.active ORDER BY n.name RETURN n");
        assert!(matches!(err, EngineError::GqlParse { .. }));
    }

    #[test]
    fn parses_later_match_and_optional_match_after_with() {
        let query = parse_ok("MATCH (n) WITH n MATCH (n)-[:R]->(m) RETURN m");
        assert_eq!(query.pipeline.clauses.len(), 4);
        let GqlPipelineClause::Match(later_match) = &query.pipeline.clauses[2] else {
            panic!("expected later MATCH");
        };
        assert!(!later_match[0].optional);
        assert_eq!(
            later_match[0].patterns[0]
                .start
                .variable
                .as_ref()
                .unwrap()
                .name,
            "n"
        );

        let optional = parse_ok("MATCH (n) WITH n OPTIONAL MATCH (n)-[:R]->(m) RETURN m");
        let GqlPipelineClause::Match(later_optional) = &optional.pipeline.clauses[2] else {
            panic!("expected later OPTIONAL MATCH");
        };
        assert!(later_optional[0].optional);
    }

    #[test]
    fn parses_return_distinct_projection_shape() {
        let query = parse_ok("MATCH (n) RETURN DISTINCT n");
        assert!(query.return_clause.distinct);
        let GqlPipelineClause::Projection(ret) = query.pipeline.clauses.last().unwrap() else {
            panic!("expected RETURN projection");
        };
        assert!(ret.distinct);
    }

    #[test]
    fn parses_union_and_union_all_branches() {
        let distinct = parse_ok("MATCH (n) RETURN n AS x UNION MATCH (m) RETURN m AS x");
        assert_eq!(distinct.pipeline.union_branches.len(), 1);
        assert_eq!(
            distinct.pipeline.union_branches[0].modifier,
            GqlUnionModifier::Distinct
        );
        assert_eq!(distinct.pipeline.union_branches[0].clauses.len(), 2);

        let all = parse_ok(
            "MATCH (n) WITH n RETURN n.name AS name UNION ALL MATCH (m) RETURN m.name AS name",
        );
        assert_eq!(all.pipeline.union_branches.len(), 1);
        assert_eq!(
            all.pipeline.union_branches[0].modifier,
            GqlUnionModifier::All
        );
        assert!(matches!(
            all.pipeline.union_branches[0].clauses[0],
            GqlPipelineClause::Match(_)
        ));
    }

    #[test]
    fn rejects_union_branch_without_terminal_return() {
        let err = parse_err("MATCH (n) RETURN n UNION MATCH (m) WITH m");
        match err {
            EngineError::GqlParse { message, span } => {
                assert!(message.contains("expected RETURN"), "{message}");
                assert_eq!(
                    span.offset,
                    "MATCH (n) RETURN n UNION MATCH (m) WITH m".len()
                );
            }
            other => panic!("expected parse error, got {other:?}"),
        }
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
            ("MATCH (n) RETURN stdev(n)", "aggregation"),
            ("CALL db.labels()", "CALL"),
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
    fn parses_aggregate_calls_in_projection_and_order_expressions() {
        let query = parse_ok(
            "MATCH (n) RETURN count(*) + 1 AS total, collect(DISTINCT n.kind) AS kinds ORDER BY count(*) DESC",
        );
        let ReturnBody::Items(items) = &query.return_clause.body else {
            panic!("expected return items");
        };
        let ExprKind::Binary { left, .. } = &items[0].expr.kind else {
            panic!("expected scalar expression containing aggregate");
        };
        assert!(matches!(
            &left.kind,
            ExprKind::AggregateCall {
                function: AggregateFunction::Count,
                distinct: false,
                arg: None,
                ..
            }
        ));
        assert!(matches!(
            &items[1].expr.kind,
            ExprKind::AggregateCall {
                function: AggregateFunction::Collect,
                distinct: true,
                ..
            }
        ));
        assert!(matches!(
            &query.order_by[0].expr.kind,
            ExprKind::AggregateCall {
                function: AggregateFunction::Count,
                arg: None,
                ..
            }
        ));
    }

    #[test]
    fn parses_count_star_and_rejects_invalid_aggregate_star_forms() {
        let query = parse_ok("MATCH (n) RETURN count(*)");
        let ReturnBody::Items(items) = &query.return_clause.body else {
            panic!("expected return items");
        };
        assert!(matches!(
            &items[0].expr.kind,
            ExprKind::AggregateCall {
                function: AggregateFunction::Count,
                arg: None,
                ..
            }
        ));

        for source in [
            "MATCH (n) RETURN count(DISTINCT *)",
            "MATCH (n) RETURN sum(*)",
            "MATCH (n) RETURN collect()",
        ] {
            assert!(
                matches!(parse_err(source), EngineError::GqlParse { .. }),
                "expected parse error for {source}"
            );
        }
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
    fn parse_statement_accepts_read_only_exists_and_call_subqueries() {
        let exists = parse_statement_ok(
            "MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->(m) RETURN m } RETURN n",
        );
        let GqlStatementBody::Query(query) = exists.body else {
            panic!("expected query statement");
        };
        let GqlPipelineClause::Match(match_groups) = &query.pipeline.clauses[0] else {
            panic!("expected leading MATCH");
        };
        assert!(matches!(
            match_groups[0].where_clause.as_ref().map(|expr| &expr.kind),
            Some(ExprKind::ExistsSubquery(_))
        ));

        let call = parse_statement_ok("CALL { MATCH (n) RETURN n } RETURN n");
        let GqlStatementBody::Query(query) = call.body else {
            panic!("expected query statement");
        };
        assert!(matches!(
            query.pipeline.clauses.first(),
            Some(GqlPipelineClause::Call(_))
        ));
    }

    #[test]
    fn parse_statement_accepts_basic_mutation_skeletons() {
        let cases = [
            "CREATE (n:Person {key: $key}) RETURN n",
            "MERGE (n:Person {key: $key}) RETURN n",
            "MATCH (a:Person) MATCH (b:Person) MERGE (a)-[r:KNOWS]->(b) RETURN r",
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
    fn parse_statement_parses_merge_actions() {
        let statement = parse_statement_ok(
            "MERGE (n:Person {key: $key}) ON CREATE SET n.created = true ON MATCH SET n.seen = $seen RETURN n",
        );
        let GqlStatementBody::Mutation(mutation) = statement.body else {
            panic!("expected mutation statement");
        };
        let [MutationClause::Merge(merge)] = mutation.mutation_clauses.as_slice() else {
            panic!("expected one MERGE clause");
        };
        assert_eq!(merge.pattern.start.variable.as_ref().unwrap().name, "n");
        assert_eq!(merge.pattern.start.labels[0].name, "Person");
        assert_eq!(
            merge.pattern.start.properties.as_ref().unwrap().entries[0]
                .key
                .name,
            "key"
        );
        assert_eq!(merge.on_create.as_ref().unwrap().items.len(), 1);
        assert_eq!(merge.on_match.as_ref().unwrap().items.len(), 1);
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
                assert_eq!(feature, "read-after-write clauses");
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
    fn parse_statement_accepts_read_arithmetic() {
        let source = "MATCH (n) RETURN 1 + 2";
        let read = parse_ok(source);
        let statement = parse_statement_ok(source);
        let GqlStatementBody::Query(statement_query) = statement.body else {
            panic!("expected query statement");
        };
        assert_eq!(read, statement_query);
    }
}
