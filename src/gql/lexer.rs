#![allow(dead_code)]

use crate::error::EngineError;
use crate::types::SourceSpan;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Keyword {
    Alter,
    And,
    As,
    Asc,
    By,
    Call,
    Constraint,
    Create,
    Delete,
    Desc,
    Detach,
    Distinct,
    Drop,
    Exists,
    False,
    Foreach,
    Graph,
    In,
    Index,
    Is,
    Limit,
    Load,
    Match,
    Merge,
    Not,
    Null,
    Offset,
    Optional,
    Or,
    Order,
    Remove,
    Return,
    Set,
    Show,
    Skip,
    True,
    Union,
    Unwind,
    Use,
    Where,
    With,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum TokenKind {
    Ident(String),
    Keyword(Keyword),
    Int(i64),
    Float(f64),
    String(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Colon,
    Comma,
    Dot,
    Dollar,
    Semicolon,
    Equals,
    Neq,
    Lt,
    Le,
    Gt,
    Ge,
    Dash,
    RightArrow,
    LeftArrow,
    Star,
    Pipe,
    Plus,
    PlusEquals,
    Slash,
    Eof,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Token {
    pub(crate) kind: TokenKind,
    pub(crate) span: SourceSpan,
}

pub(crate) fn lex(query: &str) -> Result<Vec<Token>, EngineError> {
    Lexer::new(query).lex()
}

struct Lexer<'a> {
    query: &'a str,
    offset: usize,
    line: u32,
    column: u32,
    tokens: Vec<Token>,
}

impl<'a> Lexer<'a> {
    fn new(query: &'a str) -> Self {
        Self {
            query,
            offset: 0,
            line: 1,
            column: 1,
            tokens: Vec::new(),
        }
    }

    fn lex(mut self) -> Result<Vec<Token>, EngineError> {
        while let Some(ch) = self.peek_char() {
            match ch {
                ch if ch.is_whitespace() => {
                    self.advance_char();
                }
                '/' if self.peek_next_char() == Some('/') => self.skip_line_comment(),
                '/' if self.peek_next_char() == Some('*') => self.skip_block_comment()?,
                '(' => self.single(TokenKind::LParen),
                ')' => self.single(TokenKind::RParen),
                '[' => self.single(TokenKind::LBracket),
                ']' => self.single(TokenKind::RBracket),
                '{' => self.single(TokenKind::LBrace),
                '}' => self.single(TokenKind::RBrace),
                ':' => self.single(TokenKind::Colon),
                ',' => self.single(TokenKind::Comma),
                '.' => self.single(TokenKind::Dot),
                '$' => self.single(TokenKind::Dollar),
                ';' => self.single(TokenKind::Semicolon),
                '*' => self.single(TokenKind::Star),
                '|' => self.single(TokenKind::Pipe),
                '+' => self.lex_plus(),
                '/' => self.single(TokenKind::Slash),
                '-' => self.lex_dash(),
                '<' => self.lex_less(),
                '>' => self.lex_greater(),
                '=' => self.single(TokenKind::Equals),
                '!' => self.lex_bang()?,
                '\'' | '"' => self.lex_string()?,
                ch if ch.is_ascii_digit() => self.lex_number()?,
                ch if is_ident_start(ch) => self.lex_identifier(),
                _ => {
                    let span = self.current_span(ch.len_utf8());
                    return Err(EngineError::GqlParse {
                        message: format!("unexpected character '{}'", ch),
                        span,
                    });
                }
            }
        }

        self.tokens.push(Token {
            kind: TokenKind::Eof,
            span: self.current_span(0),
        });
        Ok(self.tokens)
    }

    fn single(&mut self, kind: TokenKind) {
        let start = self.mark();
        self.advance_char();
        self.push(kind, start);
    }

    fn lex_dash(&mut self) {
        let start = self.mark();
        self.advance_char();
        if self.peek_char() == Some('>') {
            self.advance_char();
            self.push(TokenKind::RightArrow, start);
        } else {
            self.push(TokenKind::Dash, start);
        }
    }

    fn lex_plus(&mut self) {
        let start = self.mark();
        self.advance_char();
        if self.peek_char() == Some('=') {
            self.advance_char();
            self.push(TokenKind::PlusEquals, start);
        } else {
            self.push(TokenKind::Plus, start);
        }
    }

    fn lex_less(&mut self) {
        let start = self.mark();
        self.advance_char();
        match self.peek_char() {
            Some('-') => {
                self.advance_char();
                self.push(TokenKind::LeftArrow, start);
            }
            Some('=') => {
                self.advance_char();
                self.push(TokenKind::Le, start);
            }
            Some('>') => {
                self.advance_char();
                self.push(TokenKind::Neq, start);
            }
            _ => self.push(TokenKind::Lt, start),
        }
    }

    fn lex_greater(&mut self) {
        let start = self.mark();
        self.advance_char();
        if self.peek_char() == Some('=') {
            self.advance_char();
            self.push(TokenKind::Ge, start);
        } else {
            self.push(TokenKind::Gt, start);
        }
    }

    fn lex_bang(&mut self) -> Result<(), EngineError> {
        let start = self.mark();
        self.advance_char();
        if self.peek_char() == Some('=') {
            self.advance_char();
            self.push(TokenKind::Neq, start);
            Ok(())
        } else {
            Err(EngineError::GqlParse {
                message: "expected '=' after '!'".to_string(),
                span: SourceSpan::new(
                    start.offset,
                    self.offset - start.offset,
                    start.line,
                    start.column,
                ),
            })
        }
    }

    fn lex_number(&mut self) -> Result<(), EngineError> {
        let start = self.mark();
        while self.peek_char().is_some_and(|ch| ch.is_ascii_digit()) {
            self.advance_char();
        }

        let mut is_float = false;
        if self.peek_char() == Some('.')
            && self.peek_next_char().is_some_and(|ch| ch.is_ascii_digit())
        {
            is_float = true;
            self.advance_char();
            while self.peek_char().is_some_and(|ch| ch.is_ascii_digit()) {
                self.advance_char();
            }
        }

        if matches!(self.peek_char(), Some('e' | 'E')) {
            is_float = true;
            self.advance_char();
            if matches!(self.peek_char(), Some('+' | '-')) {
                self.advance_char();
            }
            let exp_start = self.offset;
            while self.peek_char().is_some_and(|ch| ch.is_ascii_digit()) {
                self.advance_char();
            }
            if self.offset == exp_start {
                return Err(EngineError::GqlParse {
                    message: "expected exponent digits".to_string(),
                    span: SourceSpan::new(
                        start.offset,
                        self.offset - start.offset,
                        start.line,
                        start.column,
                    ),
                });
            }
        }

        let raw = &self.query[start.offset..self.offset];
        let kind = if is_float {
            TokenKind::Float(raw.parse::<f64>().map_err(|_| EngineError::GqlParse {
                message: format!("invalid float literal '{}'", raw),
                span: SourceSpan::new(
                    start.offset,
                    self.offset - start.offset,
                    start.line,
                    start.column,
                ),
            })?)
        } else {
            TokenKind::Int(raw.parse::<i64>().map_err(|_| EngineError::GqlParse {
                message: format!("invalid integer literal '{}'", raw),
                span: SourceSpan::new(
                    start.offset,
                    self.offset - start.offset,
                    start.line,
                    start.column,
                ),
            })?)
        };
        self.push(kind, start);
        Ok(())
    }

    fn lex_identifier(&mut self) {
        let start = self.mark();
        while self.peek_char().is_some_and(is_ident_continue) {
            self.advance_char();
        }
        let raw = &self.query[start.offset..self.offset];
        let kind = keyword(raw)
            .map(TokenKind::Keyword)
            .unwrap_or_else(|| TokenKind::Ident(raw.to_string()));
        self.push(kind, start);
    }

    fn lex_string(&mut self) -> Result<(), EngineError> {
        let quote = self.peek_char().expect("string starts with quote");
        let start = self.mark();
        self.advance_char();
        let mut value = String::new();

        loop {
            let Some(ch) = self.peek_char() else {
                return Err(EngineError::GqlParse {
                    message: "unterminated string literal".to_string(),
                    span: SourceSpan::new(
                        start.offset,
                        self.offset - start.offset,
                        start.line,
                        start.column,
                    ),
                });
            };
            if ch == quote {
                self.advance_char();
                self.push(TokenKind::String(value), start);
                return Ok(());
            }
            if ch == '\\' {
                self.advance_char();
                let Some(escaped) = self.peek_char() else {
                    return Err(EngineError::GqlParse {
                        message: "unterminated string escape".to_string(),
                        span: SourceSpan::new(
                            start.offset,
                            self.offset - start.offset,
                            start.line,
                            start.column,
                        ),
                    });
                };
                match escaped {
                    '\'' => {
                        value.push('\'');
                        self.advance_char();
                    }
                    '"' => {
                        value.push('"');
                        self.advance_char();
                    }
                    '\\' => {
                        value.push('\\');
                        self.advance_char();
                    }
                    'n' => {
                        value.push('\n');
                        self.advance_char();
                    }
                    'r' => {
                        value.push('\r');
                        self.advance_char();
                    }
                    't' => {
                        value.push('\t');
                        self.advance_char();
                    }
                    'b' => {
                        value.push('\u{0008}');
                        self.advance_char();
                    }
                    'f' => {
                        value.push('\u{000C}');
                        self.advance_char();
                    }
                    'u' => {
                        self.advance_char();
                        let escape_span = self.current_span(0);
                        let mut code = 0u32;
                        for _ in 0..4 {
                            let Some(hex) = self.peek_char() else {
                                return Err(EngineError::GqlParse {
                                    message: "incomplete unicode escape".to_string(),
                                    span: escape_span,
                                });
                            };
                            let Some(value) = hex.to_digit(16) else {
                                return Err(EngineError::GqlParse {
                                    message: "invalid unicode escape".to_string(),
                                    span: self.current_span(hex.len_utf8()),
                                });
                            };
                            code = (code << 4) | value;
                            self.advance_char();
                        }
                        let Some(decoded) = char::from_u32(code) else {
                            return Err(EngineError::GqlParse {
                                message: "invalid unicode scalar value".to_string(),
                                span: escape_span,
                            });
                        };
                        value.push(decoded);
                    }
                    _ => {
                        return Err(EngineError::GqlParse {
                            message: format!("unsupported string escape '\\{}'", escaped),
                            span: self.current_span(escaped.len_utf8()),
                        });
                    }
                }
            } else {
                value.push(ch);
                self.advance_char();
            }
        }
    }

    fn skip_line_comment(&mut self) {
        while let Some(ch) = self.peek_char() {
            self.advance_char();
            if ch == '\n' {
                break;
            }
        }
    }

    fn skip_block_comment(&mut self) -> Result<(), EngineError> {
        let start = self.mark();
        self.advance_char();
        self.advance_char();
        while let Some(ch) = self.peek_char() {
            self.advance_char();
            if ch == '*' && self.peek_char() == Some('/') {
                self.advance_char();
                return Ok(());
            }
        }
        Err(EngineError::GqlParse {
            message: "unterminated block comment".to_string(),
            span: SourceSpan::new(
                start.offset,
                self.offset - start.offset,
                start.line,
                start.column,
            ),
        })
    }

    fn push(&mut self, kind: TokenKind, start: Mark) {
        self.tokens.push(Token {
            kind,
            span: SourceSpan::new(
                start.offset,
                self.offset - start.offset,
                start.line,
                start.column,
            ),
        });
    }

    fn mark(&self) -> Mark {
        Mark {
            offset: self.offset,
            line: self.line,
            column: self.column,
        }
    }

    fn current_span(&self, length: usize) -> SourceSpan {
        SourceSpan::new(self.offset, length, self.line, self.column)
    }

    fn peek_char(&self) -> Option<char> {
        self.query[self.offset..].chars().next()
    }

    fn peek_next_char(&self) -> Option<char> {
        let mut chars = self.query[self.offset..].chars();
        chars.next()?;
        chars.next()
    }

    fn advance_char(&mut self) -> Option<char> {
        let ch = self.peek_char()?;
        self.offset += ch.len_utf8();
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        Some(ch)
    }
}

#[derive(Clone, Copy)]
struct Mark {
    offset: usize,
    line: u32,
    column: u32,
}

fn is_ident_start(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphabetic()
}

fn is_ident_continue(ch: char) -> bool {
    ch == '_' || ch.is_ascii_alphanumeric()
}

fn keyword(raw: &str) -> Option<Keyword> {
    Some(if raw.eq_ignore_ascii_case("ALTER") {
        Keyword::Alter
    } else if raw.eq_ignore_ascii_case("AND") {
        Keyword::And
    } else if raw.eq_ignore_ascii_case("AS") {
        Keyword::As
    } else if raw.eq_ignore_ascii_case("ASC") {
        Keyword::Asc
    } else if raw.eq_ignore_ascii_case("BY") {
        Keyword::By
    } else if raw.eq_ignore_ascii_case("CALL") {
        Keyword::Call
    } else if raw.eq_ignore_ascii_case("CONSTRAINT") {
        Keyword::Constraint
    } else if raw.eq_ignore_ascii_case("CREATE") {
        Keyword::Create
    } else if raw.eq_ignore_ascii_case("DELETE") {
        Keyword::Delete
    } else if raw.eq_ignore_ascii_case("DESC") {
        Keyword::Desc
    } else if raw.eq_ignore_ascii_case("DETACH") {
        Keyword::Detach
    } else if raw.eq_ignore_ascii_case("DISTINCT") {
        Keyword::Distinct
    } else if raw.eq_ignore_ascii_case("DROP") {
        Keyword::Drop
    } else if raw.eq_ignore_ascii_case("EXISTS") {
        Keyword::Exists
    } else if raw.eq_ignore_ascii_case("FALSE") {
        Keyword::False
    } else if raw.eq_ignore_ascii_case("FOREACH") {
        Keyword::Foreach
    } else if raw.eq_ignore_ascii_case("GRAPH") {
        Keyword::Graph
    } else if raw.eq_ignore_ascii_case("IN") {
        Keyword::In
    } else if raw.eq_ignore_ascii_case("INDEX") {
        Keyword::Index
    } else if raw.eq_ignore_ascii_case("IS") {
        Keyword::Is
    } else if raw.eq_ignore_ascii_case("LIMIT") {
        Keyword::Limit
    } else if raw.eq_ignore_ascii_case("LOAD") {
        Keyword::Load
    } else if raw.eq_ignore_ascii_case("MATCH") {
        Keyword::Match
    } else if raw.eq_ignore_ascii_case("MERGE") {
        Keyword::Merge
    } else if raw.eq_ignore_ascii_case("NOT") {
        Keyword::Not
    } else if raw.eq_ignore_ascii_case("NULL") {
        Keyword::Null
    } else if raw.eq_ignore_ascii_case("OFFSET") {
        Keyword::Offset
    } else if raw.eq_ignore_ascii_case("OPTIONAL") {
        Keyword::Optional
    } else if raw.eq_ignore_ascii_case("OR") {
        Keyword::Or
    } else if raw.eq_ignore_ascii_case("ORDER") {
        Keyword::Order
    } else if raw.eq_ignore_ascii_case("REMOVE") {
        Keyword::Remove
    } else if raw.eq_ignore_ascii_case("RETURN") {
        Keyword::Return
    } else if raw.eq_ignore_ascii_case("SET") {
        Keyword::Set
    } else if raw.eq_ignore_ascii_case("SHOW") {
        Keyword::Show
    } else if raw.eq_ignore_ascii_case("SKIP") {
        Keyword::Skip
    } else if raw.eq_ignore_ascii_case("TRUE") {
        Keyword::True
    } else if raw.eq_ignore_ascii_case("UNION") {
        Keyword::Union
    } else if raw.eq_ignore_ascii_case("UNWIND") {
        Keyword::Unwind
    } else if raw.eq_ignore_ascii_case("USE") {
        Keyword::Use
    } else if raw.eq_ignore_ascii_case("WHERE") {
        Keyword::Where
    } else if raw.eq_ignore_ascii_case("WITH") {
        Keyword::With
    } else {
        return None;
    })
}
