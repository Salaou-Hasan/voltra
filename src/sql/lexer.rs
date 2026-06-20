// ============================================================================
// NeonDB SQL Lexer
//
// Tokenises a SQL string into a flat Vec<Token> for the parser to consume.
// Handles:
//   - Keywords (SELECT, FROM, WHERE, JOIN, ON, GROUP BY, HAVING, ORDER BY,
//               LIMIT, OFFSET, DISTINCT, AS, AND, OR, NOT, IN, IS, NULL,
//               LIKE, BETWEEN, EXISTS, INNER, LEFT, RIGHT, FULL, OUTER,
//               CROSS, UNION, ALL, ASC, DESC, COUNT, SUM, AVG, MIN, MAX,
//               INSERT, UPDATE, DELETE, SET, INTO, VALUES, CASE, WHEN,
//               THEN, ELSE, END, TRUE, FALSE)
//   - Identifiers and quoted identifiers (`foo`, "foo")
//   - String literals ('hello')
//   - Numeric literals (integer and float)
//   - Operators: =, !=, <>, <, <=, >, >=, +, -, *, /, %
//   - Punctuation: ( ) , . ; *
//   - Comments: -- line comment (stripped)
// ============================================================================

use crate::error::{NeonDBError, Result};

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // ── Literals ──────────────────────────────────────────────────────────────
    Integer(i64),
    Float(f64),
    StringLit(String),
    BoolLit(bool),
    Null,

    // ── Identifiers / keywords ────────────────────────────────────────────────
    Ident(String),

    // ── Keywords ──────────────────────────────────────────────────────────────
    Select,
    From,
    Where,
    Join,
    Inner,
    Left,
    Right,
    Full,
    Outer,
    Cross,
    On,
    As,
    And,
    Or,
    Not,
    In,
    Is,
    Like,
    Between,
    Exists,
    Union,
    All,
    Distinct,
    Group,
    By,
    Having,
    Order,
    Limit,
    Offset,
    Asc,
    Desc,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Case,
    When,
    Then,
    Else,
    End,
    Coalesce,
    Nullif,
    Cast,
    SubStr,
    Length,
    Upper,
    Lower,
    Trim,
    Replace,
    Round,
    Floor,
    Ceil,
    Abs,
    Now,
    Concat,

    // ── Operators ─────────────────────────────────────────────────────────────
    Eq,      // =
    Ne,      // != / <>
    Lt,      // <
    Le,      // <=
    Gt,      // >
    Ge,      // >=
    Plus,    // +
    Minus,   // -
    Star,    // *
    Slash,   // /
    Percent, // %
    Concat2, // ||

    // ── Punctuation ───────────────────────────────────────────────────────────
    LParen, // (
    RParen, // )
    Comma,  // ,
    Dot,    // .
    Semi,   // ;

    // ── Sentinel ──────────────────────────────────────────────────────────────
    Eof,
}

/// Convert a keyword string (already lowercased) to a keyword Token if
/// recognised; otherwise return Token::Ident.
fn keyword_or_ident(s: &str) -> Token {
    match s {
        "select"   => Token::Select,
        "from"     => Token::From,
        "where"    => Token::Where,
        "join"     => Token::Join,
        "inner"    => Token::Inner,
        "left"     => Token::Left,
        "right"    => Token::Right,
        "full"     => Token::Full,
        "outer"    => Token::Outer,
        "cross"    => Token::Cross,
        "on"       => Token::On,
        "as"       => Token::As,
        "and"      => Token::And,
        "or"       => Token::Or,
        "not"      => Token::Not,
        "in"       => Token::In,
        "is"       => Token::Is,
        "like"     => Token::Like,
        "between"  => Token::Between,
        "exists"   => Token::Exists,
        "union"    => Token::Union,
        "all"      => Token::All,
        "distinct" => Token::Distinct,
        "group"    => Token::Group,
        "by"       => Token::By,
        "having"   => Token::Having,
        "order"    => Token::Order,
        "limit"    => Token::Limit,
        "offset"   => Token::Offset,
        "asc"      => Token::Asc,
        "desc"     => Token::Desc,
        "count"    => Token::Count,
        "sum"      => Token::Sum,
        "avg"      => Token::Avg,
        "min"      => Token::Min,
        "max"      => Token::Max,
        "insert"   => Token::Insert,
        "into"     => Token::Into,
        "values"   => Token::Values,
        "update"   => Token::Update,
        "set"      => Token::Set,
        "delete"   => Token::Delete,
        "case"     => Token::Case,
        "when"     => Token::When,
        "then"     => Token::Then,
        "else"     => Token::Else,
        "end"      => Token::End,
        "true"     => Token::BoolLit(true),
        "false"    => Token::BoolLit(false),
        "null"     => Token::Null,
        "coalesce" => Token::Coalesce,
        "nullif"   => Token::Nullif,
        "cast"     => Token::Cast,
        "substr" | "substring" => Token::SubStr,
        "length"   => Token::Length,
        "upper"    => Token::Upper,
        "lower"    => Token::Lower,
        "trim"     => Token::Trim,
        "replace"  => Token::Replace,
        "round"    => Token::Round,
        "floor"    => Token::Floor,
        "ceil" | "ceiling" => Token::Ceil,
        "abs"      => Token::Abs,
        "now"      => Token::Now,
        "concat"   => Token::Concat,
        other      => Token::Ident(other.to_string()),
    }
}

pub struct Lexer<'a> {
    input: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Lexer { input: input.as_bytes(), pos: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.pos).copied()
    }

    fn peek2(&self) -> Option<u8> {
        self.input.get(self.pos + 1).copied()
    }

    fn advance(&mut self) -> Option<u8> {
        let ch = self.input.get(self.pos).copied();
        if ch.is_some() { self.pos += 1; }
        ch
    }

    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_ascii_whitespace() { self.pos += 1; } else { break; }
        }
    }

    fn skip_line_comment(&mut self) {
        while let Some(ch) = self.advance() {
            if ch == b'\n' { break; }
        }
    }

    fn read_string(&mut self, quote: u8) -> Result<Token> {
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err(NeonDBError::invalid_argument("Unterminated string literal")),
                Some(ch) if ch == quote => {
                    // Doubled quote = escape
                    if self.peek() == Some(quote) {
                        self.pos += 1;
                        s.push(quote as char);
                    } else {
                        break;
                    }
                }
                Some(b'\\') => {
                    match self.advance() {
                        Some(b'n')  => s.push('\n'),
                        Some(b't')  => s.push('\t'),
                        Some(b'r')  => s.push('\r'),
                        Some(b'\\') => s.push('\\'),
                        Some(c)     => { s.push('\\'); s.push(c as char); }
                        None => return Err(NeonDBError::invalid_argument("Unterminated escape")),
                    }
                }
                Some(ch) => s.push(ch as char),
            }
        }
        Ok(Token::StringLit(s))
    }

    fn read_quoted_ident(&mut self) -> Result<Token> {
        let mut s = String::new();
        loop {
            match self.advance() {
                None => return Err(NeonDBError::invalid_argument("Unterminated quoted identifier")),
                Some(b'`') | Some(b'"') => break,
                Some(ch) => s.push(ch as char),
            }
        }
        Ok(Token::Ident(s))
    }

    fn read_number(&mut self, first: u8) -> Token {
        let mut s = String::new();
        s.push(first as char);
        let mut is_float = false;

        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                s.push(ch as char);
                self.pos += 1;
            } else if ch == b'.' && !is_float && self.peek2().is_some_and(|c| c.is_ascii_digit()) {
                is_float = true;
                s.push('.');
                self.pos += 1;
            } else if (ch == b'e' || ch == b'E') && !s.contains('e') && !s.contains('E') {
                s.push(ch as char);
                self.pos += 1;
                if let Some(sign) = self.peek() {
                    if sign == b'+' || sign == b'-' {
                        s.push(sign as char);
                        self.pos += 1;
                    }
                }
                is_float = true;
            } else {
                break;
            }
        }

        if is_float {
            Token::Float(s.parse().unwrap_or(0.0))
        } else {
            Token::Integer(s.parse().unwrap_or(0))
        }
    }

    fn read_ident(&mut self, first: u8) -> Token {
        let mut s = String::new();
        s.push(first.to_ascii_lowercase() as char);
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == b'_' {
                s.push(ch.to_ascii_lowercase() as char);
                self.pos += 1;
            } else {
                break;
            }
        }
        keyword_or_ident(&s)
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace();
            let ch = match self.peek() {
                None => { tokens.push(Token::Eof); break; }
                Some(c) => c,
            };
            self.pos += 1;

            let tok = match ch {
                b'\'' => self.read_string(b'\'')?,
                b'"'  => self.read_quoted_ident()?,
                b'`'  => self.read_quoted_ident()?,
                b'('  => Token::LParen,
                b')'  => Token::RParen,
                b','  => Token::Comma,
                b'.'  => Token::Dot,
                b';'  => Token::Semi,
                b'+'  => Token::Plus,
                b'-'  => {
                    if self.peek() == Some(b'-') {
                        self.pos += 1;
                        self.skip_line_comment();
                        continue;
                    }
                    Token::Minus
                }
                b'*'  => Token::Star,
                b'/'  => Token::Slash,
                b'%'  => Token::Percent,
                b'='  => Token::Eq,
                b'!'  => {
                    if self.peek() == Some(b'=') { self.pos += 1; Token::Ne }
                    else { return Err(NeonDBError::invalid_argument("Unexpected char '!'".to_string())) }
                }
                b'<'  => {
                    if self.peek() == Some(b'=') { self.pos += 1; Token::Le }
                    else if self.peek() == Some(b'>') { self.pos += 1; Token::Ne }
                    else { Token::Lt }
                }
                b'>'  => {
                    if self.peek() == Some(b'=') { self.pos += 1; Token::Ge }
                    else { Token::Gt }
                }
                b'|'  => {
                    if self.peek() == Some(b'|') { self.pos += 1; Token::Concat2 }
                    else { return Err(NeonDBError::invalid_argument("Expected '||'")); }
                }
                c if c.is_ascii_digit() => self.read_number(c),
                c if c.is_ascii_alphabetic() || c == b'_' => self.read_ident(c),
                other => return Err(NeonDBError::invalid_argument(
                    format!("Unexpected character: '{}'", other as char)
                )),
            };
            tokens.push(tok);
        }
        Ok(tokens)
    }
}

pub fn tokenize(sql: &str) -> Result<Vec<Token>> {
    Lexer::new(sql).tokenize()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(sql: &str) -> Vec<Token> {
        tokenize(sql).unwrap()
    }

    #[test]
    fn basic_select() {
        let t = toks("SELECT * FROM players");
        assert_eq!(t[0], Token::Select);
        assert_eq!(t[1], Token::Star);
        assert_eq!(t[2], Token::From);
        assert_eq!(t[3], Token::Ident("players".into()));
    }

    #[test]
    fn string_literal() {
        let t = toks("SELECT 'hello world'");
        assert_eq!(t[1], Token::StringLit("hello world".into()));
    }

    #[test]
    fn operators() {
        let t = toks(">= <= != <>");
        assert_eq!(t[0], Token::Ge);
        assert_eq!(t[1], Token::Le);
        assert_eq!(t[2], Token::Ne);
        assert_eq!(t[3], Token::Ne);
    }

    #[test]
    fn numbers() {
        let t = toks("42 3.14 1e5");
        assert_eq!(t[0], Token::Integer(42));
        assert_eq!(t[1], Token::Float(3.14));
        assert_eq!(t[2], Token::Float(1e5));
    }

    #[test]
    fn keywords_case_insensitive() {
        let t = toks("SELECT FROM WHERE GROUP BY ORDER");
        assert_eq!(t[0], Token::Select);
        assert_eq!(t[2], Token::Where);
        assert_eq!(t[3], Token::Group);
    }

    #[test]
    fn line_comment_stripped() {
        let t = toks("SELECT * -- this is a comment\nFROM t");
        assert_eq!(t[0], Token::Select);
        assert_eq!(t[1], Token::Star);
        assert_eq!(t[2], Token::From);
    }
}
