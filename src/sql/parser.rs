// ============================================================================
// Voltra SQL Parser
//
// Recursive-descent parser.  Produces AST nodes from a token stream.
//
// Grammar (simplified Pratt precedence, lowest → highest):
//   expr     := or_expr
//   or_expr  := and_expr  (OR and_expr)*
//   and_expr := not_expr  (AND not_expr)*
//   not_expr := NOT not_expr | cmp_expr
//   cmp_expr := add_expr (op add_expr)?  | add_expr IS [NOT] NULL
//             | add_expr [NOT] BETWEEN … AND …
//             | add_expr [NOT] LIKE …
//             | add_expr [NOT] IN (list | subquery)
//   add_expr := mul_expr ((+|-|||) mul_expr)*
//   mul_expr := unary   ((*|/|%) unary)*
//   unary    := (-|+) unary | primary
//   primary  := literal | ident | fn_call | aggregate | subquery | CASE | (expr)
// ============================================================================

use super::ast::*;
use super::lexer::Token;
use crate::error::{VoltraError, Result};

pub struct Parser {
    tokens: Vec<Token>,
    pos:    usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, pos: 0 }
    }

    // ── Token navigation ──────────────────────────────────────────────────────

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek2(&self) -> &Token {
        self.tokens.get(self.pos + 1).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        if self.pos < self.tokens.len() { self.pos += 1; }
        tok
    }

    fn eat(&mut self, expected: &Token) -> Result<()> {
        if self.peek() == expected {
            self.pos += 1;
            Ok(())
        } else {
            Err(VoltraError::invalid_argument(format!(
                "Expected {:?} but got {:?}", expected, self.peek()
            )))
        }
    }

    /// Accept an identifier or any keyword that is commonly used as an
    /// identifier in practice (e.g. column named "count", "sum", "value").
    fn expect_ident(&mut self) -> Result<String> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            // Allow aggregate keywords used as bare column/alias names
            Token::Count  => Ok("count".into()),
            Token::Sum    => Ok("sum".into()),
            Token::Avg    => Ok("avg".into()),
            Token::Min    => Ok("min".into()),
            Token::Max    => Ok("max".into()),
            // Other contextually-safe keywords
            Token::Now    => Ok("now".into()),
            Token::Length => Ok("length".into()),
            Token::Upper  => Ok("upper".into()),
            Token::Lower  => Ok("lower".into()),
            other => Err(VoltraError::invalid_argument(format!(
                "Expected identifier, got {:?}", other
            ))),
        }
    }

    fn eat_keyword_by(&mut self) -> Result<()> {
        self.eat(&Token::By)
    }

    // ── Top-level entry points ────────────────────────────────────────────────

    pub fn parse_statement(&mut self) -> Result<Statement> {
        match self.peek().clone() {
            Token::Select => Ok(Statement::Select(self.parse_select()?)),
            Token::Insert => Ok(Statement::Insert(self.parse_insert()?)),
            Token::Update => Ok(Statement::Update(self.parse_update()?)),
            Token::Delete => Ok(Statement::Delete(self.parse_delete()?)),
            other => Err(VoltraError::invalid_argument(format!(
                "Expected SELECT/INSERT/UPDATE/DELETE, got {:?}", other
            ))),
        }
    }

    // ── SELECT ────────────────────────────────────────────────────────────────

    fn parse_select(&mut self) -> Result<SelectStmt> {
        self.eat(&Token::Select)?;

        let distinct = if self.peek() == &Token::Distinct {
            self.pos += 1;
            true
        } else {
            false
        };

        let columns = self.parse_projection_list()?;

        // FROM clause (optional — `SELECT 1+1` is valid)
        let mut from = Vec::new();
        let mut joins = Vec::new();
        if self.peek() == &Token::From {
            self.pos += 1;
            from = self.parse_table_refs()?;
            joins = self.parse_joins()?;
        }

        let where_ = if self.peek() == &Token::Where {
            self.pos += 1;
            Some(self.parse_expr()?)
        } else {
            None
        };

        let group_by = if self.peek() == &Token::Group {
            self.pos += 1;
            self.eat_keyword_by()?;
            self.parse_expr_list()?
        } else {
            vec![]
        };

        let having = if self.peek() == &Token::Having {
            self.pos += 1;
            Some(self.parse_expr()?)
        } else {
            None
        };

        let order_by = if self.peek() == &Token::Order {
            self.pos += 1;
            self.eat_keyword_by()?;
            self.parse_order_by_list()?
        } else {
            vec![]
        };

        let limit = if self.peek() == &Token::Limit {
            self.pos += 1;
            Some(self.parse_usize("LIMIT")?)
        } else {
            None
        };

        let offset = if self.peek() == &Token::Offset {
            self.pos += 1;
            Some(self.parse_usize("OFFSET")?)
        } else {
            None
        };

        // UNION / UNION ALL
        let union = if self.peek() == &Token::Union {
            self.pos += 1;
            let all = self.peek() == &Token::All;
            if all { self.pos += 1; }
            let rhs = self.parse_select()?;
            Some((all, Box::new(rhs)))
        } else {
            None
        };

        Ok(SelectStmt { distinct, columns, from, joins, where_, group_by, having, order_by, limit, offset, union })
    }

    fn parse_usize(&mut self, ctx: &str) -> Result<usize> {
        match self.advance() {
            Token::Integer(n) if n >= 0 => Ok(n as usize),
            other => Err(VoltraError::invalid_argument(format!(
                "{} requires a non-negative integer, got {:?}", ctx, other
            ))),
        }
    }

    // ── Projection list: SELECT col1, col2 AS alias, * ────────────────────────

    fn parse_projection_list(&mut self) -> Result<Vec<Expr>> {
        let mut cols = vec![self.parse_projection_item()?];
        while self.peek() == &Token::Comma {
            self.pos += 1;
            cols.push(self.parse_projection_item()?);
        }
        Ok(cols)
    }

    fn parse_projection_item(&mut self) -> Result<Expr> {
        // `*`  →  Wildcard
        if self.peek() == &Token::Star {
            self.pos += 1;
            return Ok(Expr::Wildcard { table: None });
        }
        // `table.*`  →  qualified wildcard
        if matches!(self.peek(), Token::Ident(_)) && self.peek2() == &Token::Dot {
            if let Token::Ident(tbl) = self.peek().clone() {
                let after_dot = self.tokens.get(self.pos + 2).unwrap_or(&Token::Eof);
                if after_dot == &Token::Star {
                    self.pos += 3;
                    return Ok(Expr::Wildcard { table: Some(tbl) });
                }
            }
        }
        let expr = self.parse_expr()?;
        // Wrap in alias if followed by AS or a bare identifier
        let alias = if self.peek() == &Token::As {
            self.pos += 1;
            Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            // bare alias (no AS keyword)
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(match alias {
            Some(a) => Expr::Alias { expr: Box::new(expr), alias: a },
            None    => expr,
        })
    }

    // ── FROM clause ───────────────────────────────────────────────────────────

    fn parse_table_refs(&mut self) -> Result<Vec<TableRef>> {
        let mut refs = vec![self.parse_table_ref()?];
        while self.peek() == &Token::Comma {
            self.pos += 1;
            refs.push(self.parse_table_ref()?);
        }
        Ok(refs)
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        if self.peek() == &Token::LParen {
            // Subquery in FROM: `(SELECT …) AS alias`
            self.pos += 1;
            let query = self.parse_select()?;
            self.eat(&Token::RParen)?;
            let alias = if self.peek() == &Token::As {
                self.pos += 1;
                self.expect_ident()?
            } else {
                self.expect_ident()?
            };
            return Ok(TableRef::Subquery { query: Box::new(query), alias });
        }
        let name = self.expect_ident()?;
        let alias = if self.peek() == &Token::As {
            self.pos += 1;
            Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(TableRef::Named { name, alias })
    }

    fn parse_joins(&mut self) -> Result<Vec<Join>> {
        let mut joins = Vec::new();
        loop {
            let kind = match self.peek() {
                Token::Join         => { self.pos += 1; JoinKind::Inner }
                Token::Inner        => {
                    self.pos += 1;
                    self.eat(&Token::Join)?;
                    JoinKind::Inner
                }
                Token::Left         => {
                    self.pos += 1;
                    if self.peek() == &Token::Outer { self.pos += 1; }
                    self.eat(&Token::Join)?;
                    JoinKind::Left
                }
                Token::Right        => {
                    self.pos += 1;
                    if self.peek() == &Token::Outer { self.pos += 1; }
                    self.eat(&Token::Join)?;
                    JoinKind::Right
                }
                Token::Full         => {
                    self.pos += 1;
                    if self.peek() == &Token::Outer { self.pos += 1; }
                    self.eat(&Token::Join)?;
                    JoinKind::Full
                }
                Token::Cross        => {
                    self.pos += 1;
                    self.eat(&Token::Join)?;
                    JoinKind::Cross
                }
                _ => break,
            };
            let table = self.parse_table_ref()?;
            let on = if self.peek() == &Token::On {
                self.pos += 1;
                Some(self.parse_expr()?)
            } else {
                None
            };
            joins.push(Join { kind, table, on });
        }
        Ok(joins)
    }

    // ── ORDER BY ─────────────────────────────────────────────────────────────

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = vec![self.parse_order_by_item()?];
        while self.peek() == &Token::Comma {
            self.pos += 1;
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem> {
        let expr = self.parse_expr()?;
        let asc = match self.peek() {
            Token::Asc  => { self.pos += 1; true }
            Token::Desc => { self.pos += 1; false }
            _           => true,
        };
        // NULLS FIRST / NULLS LAST
        let nulls_first = if matches!(self.peek(), Token::Ident(s) if s == "nulls") {
            self.pos += 1;
            match self.peek().clone() {
                Token::Ident(s) if s == "first" => { self.pos += 1; Some(true) }
                Token::Ident(s) if s == "last"  => { self.pos += 1; Some(false) }
                _ => None,
            }
        } else {
            None
        };
        Ok(OrderByItem { expr, asc, nulls_first })
    }

    // ── Expression parser (Pratt / recursive-descent) ─────────────────────────

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or()
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = vec![self.parse_expr()?];
        while self.peek() == &Token::Comma {
            self.pos += 1;
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    fn parse_or(&mut self) -> Result<Expr> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.pos += 1;
            let right = self.parse_and()?;
            left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Or, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr> {
        let mut left = self.parse_not()?;
        while self.peek() == &Token::And {
            self.pos += 1;
            let right = self.parse_not()?;
            left = Expr::BinaryOp { left: Box::new(left), op: BinOp::And, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr> {
        if self.peek() == &Token::Not {
            self.pos += 1;
            let expr = self.parse_not()?;
            return Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(expr) });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr> {
        let left = self.parse_add()?;

        // IS [NOT] NULL
        if self.peek() == &Token::Is {
            self.pos += 1;
            let negated = self.peek() == &Token::Not;
            if negated { self.pos += 1; }
            self.eat(&Token::Null)?;
            return Ok(Expr::IsNull { expr: Box::new(left), negated });
        }

        // [NOT] BETWEEN … AND …
        let negated_between = if self.peek() == &Token::Not && self.peek2() == &Token::Between {
            self.pos += 1;
            true
        } else {
            false
        };
        if self.peek() == &Token::Between {
            self.pos += 1;
            let low = self.parse_add()?;
            self.eat(&Token::And)?;
            let high = self.parse_add()?;
            return Ok(Expr::Between {
                expr: Box::new(left), low: Box::new(low), high: Box::new(high),
                negated: negated_between,
            });
        }

        // [NOT] LIKE …
        let negated_like = if self.peek() == &Token::Not && self.peek2() == &Token::Like {
            self.pos += 1;
            true
        } else {
            false
        };
        if self.peek() == &Token::Like {
            self.pos += 1;
            let pattern = self.parse_add()?;
            return Ok(Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), negated: negated_like });
        }

        // [NOT] IN (list | subquery)
        let negated_in = if self.peek() == &Token::Not && self.peek2() == &Token::In {
            self.pos += 1;
            true
        } else {
            false
        };
        if self.peek() == &Token::In {
            self.pos += 1;
            self.eat(&Token::LParen)?;
            if self.peek() == &Token::Select {
                let query = self.parse_select()?;
                self.eat(&Token::RParen)?;
                return Ok(Expr::InSubquery { expr: Box::new(left), query: Box::new(query), negated: negated_in });
            }
            let list = self.parse_expr_list()?;
            self.eat(&Token::RParen)?;
            return Ok(Expr::InList { expr: Box::new(left), list, negated: negated_in });
        }

        // Standard comparison operators
        let op = match self.peek() {
            Token::Eq    => BinOp::Eq,
            Token::Ne    => BinOp::Ne,
            Token::Lt    => BinOp::Lt,
            Token::Le    => BinOp::Le,
            Token::Gt    => BinOp::Gt,
            Token::Ge    => BinOp::Ge,
            _            => return Ok(left),
        };
        self.pos += 1;
        let right = self.parse_add()?;
        Ok(Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) })
    }

    fn parse_add(&mut self) -> Result<Expr> {
        let mut left = self.parse_mul()?;
        loop {
            let op = match self.peek() {
                Token::Plus    => BinOp::Add,
                Token::Minus   => BinOp::Sub,
                Token::Concat2 => BinOp::Concat,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_mul()?;
            left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star    => BinOp::Mul,
                Token::Slash   => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_unary()?;
            left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        match self.peek() {
            Token::Minus => { self.pos += 1; let e = self.parse_unary()?; Ok(Expr::UnaryOp { op: UnaryOp::Neg, expr: Box::new(e) }) }
            Token::Plus  => { self.pos += 1; let e = self.parse_unary()?; Ok(Expr::UnaryOp { op: UnaryOp::Pos, expr: Box::new(e) }) }
            Token::Not   => { self.pos += 1; let e = self.parse_unary()?; Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(e) }) }
            _ => self.parse_primary(),
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.peek().clone() {
            // ── Literals ──────────────────────────────────────────────────────
            Token::Integer(n)      => { self.pos += 1; Ok(Expr::Literal(serde_json::json!(n))) }
            Token::Float(f)        => { self.pos += 1; Ok(Expr::Literal(serde_json::json!(f))) }
            Token::StringLit(s)    => { self.pos += 1; Ok(Expr::Literal(serde_json::Value::String(s))) }
            Token::BoolLit(b)      => { self.pos += 1; Ok(Expr::Literal(serde_json::json!(b))) }
            Token::Null            => { self.pos += 1; Ok(Expr::Literal(serde_json::Value::Null)) }

            // ── Parenthesised expression or subquery ──────────────────────────
            Token::LParen => {
                self.pos += 1;
                if self.peek() == &Token::Select {
                    let query = self.parse_select()?;
                    self.eat(&Token::RParen)?;
                    return Ok(Expr::Subquery(Box::new(query)));
                }
                let expr = self.parse_expr()?;
                self.eat(&Token::RParen)?;
                Ok(expr)
            }

            // ── EXISTS (subquery) ─────────────────────────────────────────────
            Token::Exists => {
                self.pos += 1;
                self.eat(&Token::LParen)?;
                let query = self.parse_select()?;
                self.eat(&Token::RParen)?;
                Ok(Expr::Exists { query: Box::new(query), negated: false })
            }

            // ── CASE expression ───────────────────────────────────────────────
            Token::Case => self.parse_case(),

            // ── Aggregate functions ───────────────────────────────────────────
            Token::Count => self.parse_aggregate(AggFunc::Count),
            Token::Sum   => self.parse_aggregate(AggFunc::Sum),
            Token::Avg   => self.parse_aggregate(AggFunc::Avg),
            Token::Min   => self.parse_aggregate(AggFunc::Min),
            Token::Max   => self.parse_aggregate(AggFunc::Max),

            // ── Scalar functions ──────────────────────────────────────────────
            Token::Upper    => self.parse_scalar_fn("upper"),
            Token::Lower    => self.parse_scalar_fn("lower"),
            Token::Length   => self.parse_scalar_fn("length"),
            Token::Trim     => self.parse_scalar_fn("trim"),
            Token::Replace  => self.parse_scalar_fn("replace"),
            Token::Round    => self.parse_scalar_fn("round"),
            Token::Floor    => self.parse_scalar_fn("floor"),
            Token::Ceil     => self.parse_scalar_fn("ceil"),
            Token::Abs      => self.parse_scalar_fn("abs"),
            Token::Coalesce => self.parse_scalar_fn("coalesce"),
            Token::Nullif   => self.parse_scalar_fn("nullif"),
            Token::SubStr   => self.parse_scalar_fn("substr"),
            Token::Now      => self.parse_scalar_fn("now"),
            Token::Concat   => self.parse_scalar_fn("concat"),
            Token::Cast     => self.parse_cast(),

            // ── Identifier / qualified column ─────────────────────────────────
            Token::Ident(name) => {
                self.pos += 1;
                // Could be: `name`, `table.name`, or `fn_name(...)`
                if self.peek() == &Token::Dot {
                    self.pos += 1;
                    if self.peek() == &Token::Star {
                        self.pos += 1;
                        return Ok(Expr::Wildcard { table: Some(name) });
                    }
                    let col = self.expect_ident()?;
                    return Ok(Expr::Column { table: Some(name), name: col });
                }
                if self.peek() == &Token::LParen {
                    // Unknown function call
                    self.pos += 1;
                    let args = if self.peek() == &Token::RParen {
                        vec![]
                    } else {
                        self.parse_expr_list()?
                    };
                    self.eat(&Token::RParen)?;
                    return Ok(Expr::Function { name, args });
                }
                Ok(Expr::Column { table: None, name })
            }

            other => Err(VoltraError::invalid_argument(format!(
                "Unexpected token in expression: {:?}", other
            ))),
        }
    }

    fn parse_case(&mut self) -> Result<Expr> {
        self.eat(&Token::Case)?;

        // Simple CASE: `CASE expr WHEN val THEN result … END`
        // Searched CASE: `CASE WHEN cond THEN result … END`
        let operand = if self.peek() != &Token::When {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut branches = Vec::new();
        while self.peek() == &Token::When {
            self.pos += 1;
            let cond = self.parse_expr()?;
            self.eat(&Token::Then)?;
            let result = self.parse_expr()?;
            branches.push((cond, result));
        }

        let else_ = if self.peek() == &Token::Else {
            self.pos += 1;
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.eat(&Token::End)?;
        Ok(Expr::Case { operand, branches, else_ })
    }

    fn parse_aggregate(&mut self, func: AggFunc) -> Result<Expr> {
        self.pos += 1; // consume func name token
        self.eat(&Token::LParen)?;
        let distinct = self.peek() == &Token::Distinct;
        if distinct { self.pos += 1; }

        let arg = if self.peek() == &Token::Star && matches!(func, AggFunc::Count) {
            self.pos += 1;
            None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        self.eat(&Token::RParen)?;
        Ok(Expr::Aggregate { func, distinct, arg })
    }

    fn parse_scalar_fn(&mut self, name: &str) -> Result<Expr> {
        self.pos += 1;
        self.eat(&Token::LParen)?;
        let args = if self.peek() == &Token::RParen {
            vec![]
        } else {
            self.parse_expr_list()?
        };
        self.eat(&Token::RParen)?;
        Ok(Expr::Function { name: name.to_string(), args })
    }

    fn parse_cast(&mut self) -> Result<Expr> {
        self.pos += 1; // consume CAST
        self.eat(&Token::LParen)?;
        let expr = self.parse_expr()?;
        // AS type_name
        self.eat(&Token::As)?;
        let type_name = self.expect_ident()?;
        self.eat(&Token::RParen)?;
        Ok(Expr::Function {
            name: format!("cast::{}", type_name),
            args: vec![expr],
        })
    }

    // ── INSERT ────────────────────────────────────────────────────────────────

    fn parse_insert(&mut self) -> Result<InsertStmt> {
        self.eat(&Token::Insert)?;
        self.eat(&Token::Into)?;
        let table = self.expect_ident()?;

        // Column list
        let columns = if self.peek() == &Token::LParen {
            self.pos += 1;
            let mut cols = vec![self.expect_ident()?];
            while self.peek() == &Token::Comma {
                self.pos += 1;
                cols.push(self.expect_ident()?);
            }
            self.eat(&Token::RParen)?;
            cols
        } else {
            vec![]
        };

        self.eat(&Token::Values)?;

        let mut all_values = Vec::new();
        loop {
            self.eat(&Token::LParen)?;
            let row = self.parse_expr_list()?;
            self.eat(&Token::RParen)?;
            all_values.push(row);
            if self.peek() == &Token::Comma { self.pos += 1; } else { break; }
        }

        Ok(InsertStmt { table, columns, values: all_values })
    }

    // ── UPDATE ────────────────────────────────────────────────────────────────

    fn parse_update(&mut self) -> Result<UpdateStmt> {
        self.eat(&Token::Update)?;
        let table = self.expect_ident()?;
        let alias = if self.peek() == &Token::As {
            self.pos += 1;
            Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else {
            None
        };
        self.eat(&Token::Set)?;
        let mut sets = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.eat(&Token::Eq)?;
            let val = self.parse_expr()?;
            sets.push((col, val));
            if self.peek() == &Token::Comma { self.pos += 1; } else { break; }
        }
        let where_ = if self.peek() == &Token::Where {
            self.pos += 1;
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(UpdateStmt { table, alias, sets, where_ })
    }

    // ── DELETE ────────────────────────────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<DeleteStmt> {
        self.eat(&Token::Delete)?;
        self.eat(&Token::From)?;
        let table = self.expect_ident()?;
        let where_ = if self.peek() == &Token::Where {
            self.pos += 1;
            Some(self.parse_expr()?)
        } else {
            None
        };
        Ok(DeleteStmt { table, where_ })
    }
}

// ── Public entry points ────────────────────────────────────────────────────────

pub fn parse(sql: &str) -> Result<Statement> {
    let tokens = super::lexer::tokenize(sql)?;
    let mut parser = Parser::new(tokens);
    let stmt = parser.parse_statement()?;
    // Allow trailing semicolons
    if parser.peek() == &Token::Semi { parser.pos += 1; }
    Ok(stmt)
}

pub fn parse_select(sql: &str) -> Result<SelectStmt> {
    match parse(sql)? {
        Statement::Select(s) => Ok(s),
        other => Err(VoltraError::invalid_argument(format!(
            "Expected SELECT statement, got {:?}", std::mem::discriminant(&other)
        ))),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(sql: &str) -> SelectStmt {
        parse_select(sql).unwrap()
    }

    #[test]
    fn select_star() {
        let s = sel("SELECT * FROM players");
        assert_eq!(s.from[0], TableRef::Named { name: "players".into(), alias: None });
        assert_eq!(s.columns[0], Expr::Wildcard { table: None });
    }

    #[test]
    fn select_with_where() {
        let s = sel("SELECT id, score FROM players WHERE level > 5");
        assert!(s.where_.is_some());
        assert_eq!(s.columns.len(), 2);
    }

    #[test]
    fn select_with_alias() {
        let s = sel("SELECT score AS pts FROM players");
        match &s.columns[0] {
            Expr::Alias { alias, .. } => assert_eq!(alias, "pts"),
            other => panic!("expected Alias, got {:?}", other),
        }
    }

    #[test]
    fn select_aggregate_count_star() {
        let s = sel("SELECT COUNT(*) FROM players");
        match &s.columns[0] {
            Expr::Aggregate { func: AggFunc::Count, arg: None, .. } => {}
            other => panic!("expected COUNT(*), got {:?}", other),
        }
    }

    #[test]
    fn select_group_by_having() {
        let s = sel("SELECT zone, COUNT(*) FROM players GROUP BY zone HAVING COUNT(*) > 2");
        assert_eq!(s.group_by.len(), 1);
        assert!(s.having.is_some());
    }

    #[test]
    fn select_order_by_limit_offset() {
        let s = sel("SELECT * FROM scores ORDER BY score DESC LIMIT 10 OFFSET 20");
        assert_eq!(s.order_by.len(), 1);
        assert!(!s.order_by[0].asc);
        assert_eq!(s.limit, Some(10));
        assert_eq!(s.offset, Some(20));
    }

    #[test]
    fn select_join() {
        let s = sel("SELECT p.id, i.name FROM players p JOIN items i ON p.item_id = i.id");
        assert_eq!(s.joins.len(), 1);
        assert_eq!(s.joins[0].kind, JoinKind::Inner);
    }

    #[test]
    fn select_left_join() {
        let s = sel("SELECT * FROM players p LEFT JOIN inventory i ON p.id = i.player_id");
        assert_eq!(s.joins[0].kind, JoinKind::Left);
    }

    #[test]
    fn select_in_list() {
        let s = sel("SELECT * FROM players WHERE status IN ('active', 'vip')");
        match s.where_.unwrap() {
            Expr::InList { list, negated, .. } => {
                assert!(!negated);
                assert_eq!(list.len(), 2);
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn select_between() {
        let s = sel("SELECT * FROM scores WHERE value BETWEEN 10 AND 100");
        assert!(matches!(s.where_.unwrap(), Expr::Between { .. }));
    }

    #[test]
    fn select_like() {
        let s = sel("SELECT * FROM players WHERE name LIKE 'al%'");
        assert!(matches!(s.where_.unwrap(), Expr::Like { .. }));
    }

    #[test]
    fn select_distinct() {
        let s = sel("SELECT DISTINCT zone FROM players");
        assert!(s.distinct);
    }

    #[test]
    fn select_case() {
        let s = sel("SELECT CASE WHEN score > 100 THEN 'high' ELSE 'low' END FROM players");
        assert!(matches!(&s.columns[0], Expr::Case { .. }));
    }

    #[test]
    fn select_subquery_in_where() {
        let s = sel("SELECT * FROM players WHERE id IN (SELECT id FROM vip_list)");
        match s.where_.unwrap() {
            Expr::InSubquery { .. } => {}
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn select_union() {
        let s = sel("SELECT id FROM players UNION ALL SELECT id FROM bots");
        assert!(s.union.is_some());
        let (all, _) = s.union.unwrap();
        assert!(all);
    }

    #[test]
    fn insert_statement() {
        match parse("INSERT INTO players (id, score) VALUES ('p1', 100), ('p2', 200)").unwrap() {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "players");
                assert_eq!(ins.columns, vec!["id", "score"]);
                assert_eq!(ins.values.len(), 2);
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn update_statement() {
        match parse("UPDATE players SET score = 999 WHERE id = 'p1'").unwrap() {
            Statement::Update(upd) => {
                assert_eq!(upd.table, "players");
                assert_eq!(upd.sets[0].0, "score");
                assert!(upd.where_.is_some());
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn delete_statement() {
        match parse("DELETE FROM players WHERE id = 'p1'").unwrap() {
            Statement::Delete(del) => {
                assert_eq!(del.table, "players");
                assert!(del.where_.is_some());
            }
            other => panic!("{:?}", other),
        }
    }

    #[test]
    fn complex_select() {
        let sql = r#"
            SELECT p.id, p.name, SUM(s.score) AS total_score
            FROM players p
            LEFT JOIN scores s ON p.id = s.player_id
            WHERE p.active = true AND p.level > 5
            GROUP BY p.id, p.name
            HAVING SUM(s.score) > 100
            ORDER BY total_score DESC
            LIMIT 20 OFFSET 0
        "#;
        let s = sel(sql);
        assert_eq!(s.columns.len(), 3);
        assert_eq!(s.joins.len(), 1);
        assert!(s.where_.is_some());
        assert_eq!(s.group_by.len(), 2);
        assert!(s.having.is_some());
        assert_eq!(s.limit, Some(20));
        assert_eq!(s.offset, Some(0));
    }
}
