// ============================================================================
// VQL — Lexer
//
// Tokenizes VQL source into a stream of tokens.
// Extends the DSL lexer with SQL/VQL keywords for queries and subscriptions.
// ============================================================================

use super::error::VqlError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // ── Keywords ──────────────────────────────────────────────────────────
    // SQL-like
    Select, From, Where, And, Or, Not, In, Is, Null, As,
    Insert, Into, Values, Update, Set, Delete,
    Join, Inner, Left, Right, Outer, Full, Cross, On,
    Group, By, Having, Order, Limit, Offset,
    Distinct, Union, All, Exists, Between, Like, ILike,
    Case, When, Then, Else, End, Cast,
    Return, Returning,

    // Transactions
    Begin, Commit, Rollback,

    // VQL-specific
    Subscribe,  // reactive push
    Leaderboard, // game primitive
    Upsert,     // atomic upsert
    Conflict,   // ON CONFLICT
    Do,         // ON CONFLICT DO UPDATE
    Asc, Desc,  // sort direction
    Nulls,      // NULLS FIRST/LAST
    First, Last,

    // TTL (Redis-inspired)
    Ttl,

    // Aggregate functions
    Count, Sum, Avg, Min, Max,

    // Scalar functions
    Upper, Lower, Length, Trim, Ltrim, Rtrim, Replace, Substr,
    Round, Floor, Ceil, Abs, Coalesce, Nullif,
    Now, Concat, Random,

    // ── Type literals ─────────────────────────────────────────────────────
    TStr, TInt, TFloat, TBool,

    // ── Punctuation ───────────────────────────────────────────────────────
    LBrace, RBrace, LParen, RParen, LBracket, RBracket,
    Comma, Colon, Dot, Semi, Star, Question,

    // ── Operators ─────────────────────────────────────────────────────────
    Eq,           // =
    EqEq, BangEq, // == !=
    Lt, Gt, LtEq, GtEq,
    Plus, Minus, Slash, Percent,
    AmpAmp, PipePipe, Bang,     // && || !
    Pipe,                         // | (concat alternative)

    // ── Literals ──────────────────────────────────────────────────────────
    Integer(i64),
    Float(f64),
    StringLit(String),
    BoolLit(bool),

    // ── Identifier ────────────────────────────────────────────────────────
    Ident(String),

    // ── End of file ───────────────────────────────────────────────────────
    Eof,
}

#[derive(Debug, Clone)]
pub struct Spanned {
    pub token: Token,
    pub line:  usize,
}

pub fn tokenize(src: &str) -> Result<Vec<Spanned>, VqlError> {
    let mut tokens = Vec::new();
    let mut chars  = src.chars().peekable();
    let mut line   = 1usize;

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\r' => { chars.next(); }
            '\n' => { line += 1; chars.next(); }

            // Line comment
            '/' if chars.clone().nth(1) == Some('/') => {
                while chars.peek().map(|&c| c != '\n').unwrap_or(false) {
                    chars.next();
                }
            }

            // String literal
            '\'' => {
                chars.next();
                let mut s = String::new();
                loop {
                    match chars.next() {
                        Some('\'')  => break,
                        Some('\\') => match chars.next() {
                            Some('n')  => s.push('\n'),
                            Some('t')  => s.push('\t'),
                            Some('\\') => s.push('\\'),
                            Some('\'') => s.push('\''),
                            Some(c)    => s.push(c),
                            None => return Err(VqlError::new(line, "unterminated string escape")),
                        },
                        Some('\n') => return Err(VqlError::new(line, "unterminated string literal")),
                        Some(c)    => s.push(c),
                        None => return Err(VqlError::new(line, "unterminated string literal")),
                    }
                }
                tokens.push(Spanned { token: Token::StringLit(s), line });
            }

            // Number literal
            c if c.is_ascii_digit() => {
                let mut num = String::new();
                while chars.peek().map(|&c: &char| c.is_ascii_digit()).unwrap_or(false) {
                    num.push(chars.next().unwrap());
                }
                if chars.peek() == Some(&'.') && chars.clone().nth(1).map(|c: char| c.is_ascii_digit()).unwrap_or(false) {
                    num.push(chars.next().unwrap());
                    while chars.peek().map(|&c: &char| c.is_ascii_digit()).unwrap_or(false) {
                        num.push(chars.next().unwrap());
                    }
                    let f: f64 = num.parse().map_err(|_| VqlError::new(line, format!("invalid float: {}", num)))?;
                    tokens.push(Spanned { token: Token::Float(f), line });
                } else {
                    let i: i64 = num.parse().map_err(|_| VqlError::new(line, format!("invalid integer: {}", num)))?;
                    tokens.push(Spanned { token: Token::Integer(i), line });
                }
            }

            // Identifier or keyword
            c if c.is_alphabetic() || c == '_' => {
                let mut ident = String::new();
                while chars.peek().map(|&c: &char| c.is_alphanumeric() || c == '_').unwrap_or(false) {
                    ident.push(chars.next().unwrap());
                }
                let lower = ident.to_lowercase();
                let tok = match lower.as_str() {
                    // SQL keywords
                    "select"    => Token::Select,
                    "from"      => Token::From,
                    "where"     => Token::Where,
                    "and"       => Token::And,
                    "or"        => Token::Or,
                    "not"       => Token::Not,
                    "in"        => Token::In,
                    "is"        => Token::Is,
                    "null"      => Token::Null,
                    "as"        => Token::As,
                    "insert"    => Token::Insert,
                    "into"      => Token::Into,
                    "values"    => Token::Values,
                    "update"    => Token::Update,
                    "set"       => Token::Set,
                    "delete"    => Token::Delete,
                    "join"      => Token::Join,
                    "inner"     => Token::Inner,
                    "left"      => Token::Left,
                    "right"     => Token::Right,
                    "outer"     => Token::Outer,
                    "full"      => Token::Full,
                    "cross"     => Token::Cross,
                    "on"        => Token::On,
                    "group"     => Token::Group,
                    "by"        => Token::By,
                    "having"    => Token::Having,
                    "order"     => Token::Order,
                    "limit"     => Token::Limit,
                    "offset"    => Token::Offset,
                    "distinct"  => Token::Distinct,
                    "union"     => Token::Union,
                    "all"       => Token::All,
                    "exists"    => Token::Exists,
                    "between"   => Token::Between,
                    "like"      => Token::Like,
                    "ilike"     => Token::ILike,
                    "case"      => Token::Case,
                    "when"      => Token::When,
                    "then"      => Token::Then,
                    "else"      => Token::Else,
                    "end"       => Token::End,
                    "cast"      => Token::Cast,
                    "returning" => Token::Returning,
                    "asc"       => Token::Asc,
                    "desc"      => Token::Desc,
                    "nulls"     => Token::Nulls,
                    "first"     => Token::First,
                    "last"      => Token::Last,

                    // Transactions
                    "begin"     => Token::Begin,
                    "commit"    => Token::Commit,
                    "rollback"  => Token::Rollback,

                    // VQL-specific
                    "subscribe" => Token::Subscribe,
                    "leaderboard" => Token::Leaderboard,
                    "upsert"    => Token::Upsert,
                    "conflict"  => Token::Conflict,
                    "do"        => Token::Do,
                    "ttl"       => Token::Ttl,

                    // Aggregates
                    "count"     => Token::Count,
                    "sum"       => Token::Sum,
                    "avg"       => Token::Avg,
                    "min"       => Token::Min,
                    "max"       => Token::Max,

                    // Scalar functions
                    "upper"     => Token::Upper,
                    "lower"     => Token::Lower,
                    "length"    => Token::Length,
                    "trim"      => Token::Trim,
                    "ltrim"     => Token::Ltrim,
                    "rtrim"     => Token::Rtrim,
                    "replace"   => Token::Replace,
                    "substr"    => Token::Substr,
                    "round"     => Token::Round,
                    "floor"     => Token::Floor,
                    "ceil"      => Token::Ceil,
                    "abs"       => Token::Abs,
                    "coalesce"  => Token::Coalesce,
                    "nullif"    => Token::Nullif,
                    "now"       => Token::Now,
                    "concat"    => Token::Concat,
                    "random"    => Token::Random,

                    // Types
                    "str"       => Token::TStr,
                    "int"       => Token::TInt,
                    "float"     => Token::TFloat,
                    "bool"      => Token::TBool,

                    // Booleans
                    "true"      => Token::BoolLit(true),
                    "false"     => Token::BoolLit(false),

                    _           => Token::Ident(ident),
                };
                tokens.push(Spanned { token: tok, line });
            }

            // Two-character operators
            '=' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::EqEq, line });
            }
            '!' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::BangEq, line });
            }
            '<' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::LtEq, line });
            }
            '>' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::GtEq, line });
            }
            '&' if chars.clone().nth(1) == Some('&') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::AmpAmp, line });
            }
            '|' if chars.clone().nth(1) == Some('|') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::PipePipe, line });
            }

            // Single-character tokens
            '{' => { chars.next(); tokens.push(Spanned { token: Token::LBrace,   line }); }
            '}' => { chars.next(); tokens.push(Spanned { token: Token::RBrace,   line }); }
            '(' => { chars.next(); tokens.push(Spanned { token: Token::LParen,   line }); }
            ')' => { chars.next(); tokens.push(Spanned { token: Token::RParen,   line }); }
            '[' => { chars.next(); tokens.push(Spanned { token: Token::LBracket, line }); }
            ']' => { chars.next(); tokens.push(Spanned { token: Token::RBracket, line }); }
            ',' => { chars.next(); tokens.push(Spanned { token: Token::Comma,    line }); }
            ':' => { chars.next(); tokens.push(Spanned { token: Token::Colon,    line }); }
            '.' => { chars.next(); tokens.push(Spanned { token: Token::Dot,      line }); }
            ';' => { chars.next(); tokens.push(Spanned { token: Token::Semi,     line }); }
            '=' => { chars.next(); tokens.push(Spanned { token: Token::Eq,       line }); }
            '<' => { chars.next(); tokens.push(Spanned { token: Token::Lt,       line }); }
            '>' => { chars.next(); tokens.push(Spanned { token: Token::Gt,       line }); }
            '!' => { chars.next(); tokens.push(Spanned { token: Token::Bang,     line }); }
            '+' => { chars.next(); tokens.push(Spanned { token: Token::Plus,     line }); }
            '-' => { chars.next(); tokens.push(Spanned { token: Token::Minus,    line }); }
            '*' => { chars.next(); tokens.push(Spanned { token: Token::Star,     line }); }
            '/' => { chars.next(); tokens.push(Spanned { token: Token::Slash,    line }); }
            '%' => { chars.next(); tokens.push(Spanned { token: Token::Percent,  line }); }
            '|' => { chars.next(); tokens.push(Spanned { token: Token::Pipe,     line }); }

            other => {
                return Err(VqlError::new(line, format!("unexpected character: {:?}", other)));
            }
        }
    }

    tokens.push(Spanned { token: Token::Eof, line });
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(src: &str) -> Vec<Token> {
        tokenize(src).unwrap().into_iter().map(|s| s.token).collect()
    }

    #[test]
    fn lex_select_star() {
        let t = toks("SELECT * FROM players");
        assert!(t.contains(&Token::Select));
        assert!(t.contains(&Token::Star));
        assert!(t.contains(&Token::From));
        assert!(t.contains(&Token::Ident("players".into())));
    }

    #[test]
    fn lex_where_clause() {
        let t = toks("WHERE hp > 0 AND alive = true");
        assert!(t.contains(&Token::Where));
        assert!(t.contains(&Token::And));
        assert!(t.contains(&Token::Gt));
        assert!(t.contains(&Token::BoolLit(true)));
    }

    #[test]
    fn lex_subscribe() {
        let t = toks("SUBSCRIBE players WHERE zone = 'z1' LIMIT 50");
        assert!(t.contains(&Token::Subscribe));
        assert!(t.contains(&Token::Limit));
        assert!(t.contains(&Token::StringLit("z1".into())));
    }

    #[test]
    fn lex_leaderboard() {
        let t = toks("LEADERBOARD scores BY score DESC LIMIT 10");
        assert!(t.contains(&Token::Leaderboard));
        assert!(t.contains(&Token::Desc));
        assert!(t.contains(&Token::Integer(10)));
    }

    #[test]
    fn lex_upsert() {
        let t = toks("UPSERT players['p1'] SET hp = 100");
        assert!(t.contains(&Token::Upsert));
        assert!(t.contains(&Token::Set));
        assert!(t.contains(&Token::StringLit("p1".into())));
        assert!(t.contains(&Token::Integer(100)));
    }

    #[test]
    fn lex_ttl() {
        let t = toks("INSERT players ['p1'] VALUES ({}) TTL 3600");
        assert!(t.contains(&Token::Ttl));
        assert!(t.contains(&Token::Integer(3600)));
    }

    #[test]
    fn lex_transaction() {
        let t = toks("BEGIN; COMMIT; ROLLBACK");
        assert!(t.contains(&Token::Begin));
        assert!(t.contains(&Token::Commit));
        assert!(t.contains(&Token::Rollback));
    }

    #[test]
    fn lex_aggregates() {
        let t = toks("COUNT(*) SUM(score) AVG(hp) MIN(x) MAX(y)");
        assert!(t.contains(&Token::Count));
        assert!(t.contains(&Token::Sum));
        assert!(t.contains(&Token::Avg));
        assert!(t.contains(&Token::Min));
        assert!(t.contains(&Token::Max));
    }

    #[test]
    fn lex_join_types() {
        let t = toks("JOIN LEFT JOIN RIGHT JOIN FULL JOIN CROSS JOIN INNER JOIN");
        assert!(t.contains(&Token::Join));
        assert!(t.contains(&Token::Left));
        assert!(t.contains(&Token::Right));
        assert!(t.contains(&Token::Full));
        assert!(t.contains(&Token::Cross));
        assert!(t.contains(&Token::Inner));
    }

    #[test]
    fn lex_returning() {
        let t = toks("UPDATE t SET x = 1 RETURNING *");
        assert!(t.contains(&Token::Returning));
    }

    #[test]
    fn lex_between_like() {
        let t = toks("BETWEEN 1 AND 10 LIKE 'a%' ILIKE 'B%'");
        assert!(t.contains(&Token::Between));
        assert!(t.contains(&Token::Like));
        assert!(t.contains(&Token::ILike));
    }

    #[test]
    fn lex_case_expression() {
        let t = toks("CASE WHEN x > 0 THEN 'pos' ELSE 'neg' END");
        assert!(t.contains(&Token::Case));
        assert!(t.contains(&Token::When));
        assert!(t.contains(&Token::Then));
        assert!(t.contains(&Token::End));
    }

    #[test]
    fn lex_string_single_quotes() {
        let t = toks("'hello world'");
        assert!(t.contains(&Token::StringLit("hello world".into())));
    }

    #[test]
    fn lex_line_comment() {
        let t = toks("SELECT // comment\n*");
        assert!(t.contains(&Token::Select));
        assert!(t.contains(&Token::Star));
        assert!(!t.iter().any(|tok| matches!(tok, Token::Ident(s) if s == "comment")));
    }

    #[test]
    fn lex_all_operators() {
        let t = toks("= == != < > <= >= + - * / % && || ! |");
        assert!(t.contains(&Token::Eq));
        assert!(t.contains(&Token::EqEq));
        assert!(t.contains(&Token::BangEq));
        assert!(t.contains(&Token::Lt));
        assert!(t.contains(&Token::Gt));
        assert!(t.contains(&Token::LtEq));
        assert!(t.contains(&Token::GtEq));
        assert!(t.contains(&Token::Plus));
        assert!(t.contains(&Token::Minus));
        assert!(t.contains(&Token::Star));
        assert!(t.contains(&Token::Slash));
        assert!(t.contains(&Token::Percent));
        assert!(t.contains(&Token::AmpAmp));
        assert!(t.contains(&Token::PipePipe));
        assert!(t.contains(&Token::Bang));
        assert!(t.contains(&Token::Pipe));
    }
}
