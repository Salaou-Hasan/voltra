// ============================================================================
// .vol DSL — Lexer
// ============================================================================

use super::error::NeonError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Table, Reducer, Let, If, Else, Delete, Return, Error,
    For, In, While, Break, Continue,
    // Built-in type names
    TStr, TInt, TFloat, TBool,
    // Punctuation
    LBrace, RBrace, LParen, RParen, LBracket, RBracket,
    Comma, Colon, Dot,
    // Operators
    Eq,                                     // =
    EqEq, BangEq,                           // == !=
    Lt, Gt, LtEq, GtEq,                    // < > <= >=
    LtLt, GtGt,                             // << >>
    AmpAmp, PipePipe, Bang,                 // && || !
    Amp, Pipe, Caret,                       // & | ^  (bitwise)
    Plus, Minus, Star, Slash, Percent,      // + - * / %
    PlusEq, MinusEq, StarEq, SlashEq,      // += -= *= /=
    // Literals
    IntLit(i64),
    FloatLit(f64),
    StrLit(String),
    BoolLit(bool),
    // Identifier (anything else)
    Ident(String),
    // End of file
    Eof,
}

#[derive(Debug, Clone)]
pub struct Spanned {
    pub token: Token,
    pub line:  usize,
}

pub fn tokenize(src: &str) -> Result<Vec<Spanned>, NeonError> {
    let mut tokens = Vec::new();
    let mut chars  = src.chars().peekable();
    let mut line   = 1usize;

    while let Some(&ch) = chars.peek() {
        match ch {
            // Whitespace (not newlines — newlines don't matter in this grammar)
            ' ' | '\t' | '\r' => { chars.next(); }
            '\n' => { line += 1; chars.next(); }

            // Line comment
            '/' if chars.clone().nth(1) == Some('/') => {
                while chars.peek().map(|&c| c != '\n').unwrap_or(false) {
                    chars.next();
                }
            }

            // String literal
            '"' => {
                chars.next();
                let mut s = String::new();
                loop {
                    match chars.next() {
                        Some('"')  => break,
                        Some('\\') => match chars.next() {
                            Some('n')  => s.push('\n'),
                            Some('t')  => s.push('\t'),
                            Some('\\') => s.push('\\'),
                            Some('"')  => s.push('"'),
                            Some(c)    => s.push(c),
                            None => return Err(NeonError { line, message: "unterminated string escape".into() }),
                        },
                        Some('\n') => return Err(NeonError { line, message: "unterminated string literal".into() }),
                        Some(c)    => s.push(c),
                        None => return Err(NeonError { line, message: "unterminated string literal".into() }),
                    }
                }
                tokens.push(Spanned { token: Token::StrLit(s), line });
            }

            // Number literal
            c if c.is_ascii_digit() || (c == '-' && chars.clone().nth(1).map(|d: char| d.is_ascii_digit()).unwrap_or(false)) => {
                let mut num = String::new();
                if c == '-' { num.push('-'); chars.next(); }
                while chars.peek().map(|&c: &char| c.is_ascii_digit()).unwrap_or(false) {
                    num.push(chars.next().unwrap());
                }
                if chars.peek() == Some(&'.') && chars.clone().nth(1).map(|c: char| c.is_ascii_digit()).unwrap_or(false) {
                    num.push(chars.next().unwrap()); // '.'
                    while chars.peek().map(|&c: &char| c.is_ascii_digit()).unwrap_or(false) {
                        num.push(chars.next().unwrap());
                    }
                    let f: f64 = num.parse().map_err(|_| NeonError { line, message: format!("invalid float: {}", num) })?;
                    tokens.push(Spanned { token: Token::FloatLit(f), line });
                } else {
                    let i: i64 = num.parse().map_err(|_| NeonError { line, message: format!("invalid integer: {}", num) })?;
                    tokens.push(Spanned { token: Token::IntLit(i), line });
                }
            }

            // Identifier or keyword
            c if c.is_alphabetic() || c == '_' => {
                let mut ident = String::new();
                while chars.peek().map(|&c: &char| c.is_alphanumeric() || c == '_').unwrap_or(false) {
                    ident.push(chars.next().unwrap());
                }
                let tok = match ident.as_str() {
                    "table"   => Token::Table,
                    "reducer" => Token::Reducer,
                    "let"     => Token::Let,
                    "if"      => Token::If,
                    "else"    => Token::Else,
                    "delete"  => Token::Delete,
                    "return"  => Token::Return,
                    "error"   => Token::Error,
                    "for"      => Token::For,
                    "in"       => Token::In,
                    "while"    => Token::While,
                    "break"    => Token::Break,
                    "continue" => Token::Continue,
                    "str"     => Token::TStr,
                    "int"     => Token::TInt,
                    "float"   => Token::TFloat,
                    "bool"    => Token::TBool,
                    "true"    => Token::BoolLit(true),
                    "false"   => Token::BoolLit(false),
                    _         => Token::Ident(ident),
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
            '<' if chars.clone().nth(1) == Some('<') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::LtLt, line });
            }
            '<' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::LtEq, line });
            }
            '>' if chars.clone().nth(1) == Some('>') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::GtGt, line });
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
            '+' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::PlusEq, line });
            }
            '-' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::MinusEq, line });
            }
            '*' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::StarEq, line });
            }
            '/' if chars.clone().nth(1) == Some('=') => {
                chars.next(); chars.next();
                tokens.push(Spanned { token: Token::SlashEq, line });
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
            '=' => { chars.next(); tokens.push(Spanned { token: Token::Eq,       line }); }
            '<' => { chars.next(); tokens.push(Spanned { token: Token::Lt,       line }); }
            '>' => { chars.next(); tokens.push(Spanned { token: Token::Gt,       line }); }
            '!' => { chars.next(); tokens.push(Spanned { token: Token::Bang,     line }); }
            '+' => { chars.next(); tokens.push(Spanned { token: Token::Plus,     line }); }
            '-' => { chars.next(); tokens.push(Spanned { token: Token::Minus,    line }); }
            '*' => { chars.next(); tokens.push(Spanned { token: Token::Star,     line }); }
            '/' => { chars.next(); tokens.push(Spanned { token: Token::Slash,    line }); }
            '%' => { chars.next(); tokens.push(Spanned { token: Token::Percent,  line }); }
            '&' => { chars.next(); tokens.push(Spanned { token: Token::Amp,      line }); }
            '|' => { chars.next(); tokens.push(Spanned { token: Token::Pipe,     line }); }
            '^' => { chars.next(); tokens.push(Spanned { token: Token::Caret,    line }); }

            other => {
                return Err(NeonError { line, message: format!("unexpected character: {:?}", other) });
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
    fn lex_keywords() {
        let t = toks("table reducer let if else delete return error");
        assert!(t.contains(&Token::Table));
        assert!(t.contains(&Token::Reducer));
        assert!(t.contains(&Token::Let));
        assert!(t.contains(&Token::If));
        assert!(t.contains(&Token::Else));
        assert!(t.contains(&Token::Delete));
        assert!(t.contains(&Token::Return));
        assert!(t.contains(&Token::Error));
    }

    #[test]
    fn lex_types() {
        let t = toks("str int float bool");
        assert!(t.contains(&Token::TStr));
        assert!(t.contains(&Token::TInt));
        assert!(t.contains(&Token::TFloat));
        assert!(t.contains(&Token::TBool));
    }

    #[test]
    fn lex_literals() {
        let t = toks("42 3.14 \"hello\" true false");
        assert!(t.contains(&Token::IntLit(42)));
        assert!(t.contains(&Token::FloatLit(3.14)));
        assert!(t.contains(&Token::StrLit("hello".into())));
        assert!(t.contains(&Token::BoolLit(true)));
        assert!(t.contains(&Token::BoolLit(false)));
    }

    #[test]
    fn lex_operators() {
        let t = toks("== != <= >= && ||");
        assert!(t.contains(&Token::EqEq));
        assert!(t.contains(&Token::BangEq));
        assert!(t.contains(&Token::LtEq));
        assert!(t.contains(&Token::GtEq));
        assert!(t.contains(&Token::AmpAmp));
        assert!(t.contains(&Token::PipePipe));
    }

    #[test]
    fn lex_line_comment() {
        let t = toks("let // this is a comment\nif");
        assert!(t.contains(&Token::Let));
        assert!(t.contains(&Token::If));
        // Comment content should NOT appear
        assert!(!t.iter().any(|tok| matches!(tok, Token::Ident(s) if s == "this")));
    }

    #[test]
    fn lex_negative_int() {
        let t = toks("-5");
        assert!(t.contains(&Token::IntLit(-5)));
    }

    #[test]
    fn lex_string_escape() {
        let t = toks(r#""hello\nworld""#);
        assert!(t.contains(&Token::StrLit("hello\nworld".into())));
    }
}
