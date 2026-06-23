// ============================================================================
// VQL — Recursive Descent Parser
//
// Parses VQL token streams into AST nodes.
// Grammar (simplified Pratt precedence for expressions):
//   statement := select | insert | update | delete | subscribe | leaderboard
//              | upsert | begin | commit | rollback
//   expr      := or_expr
//   or_expr   := and_expr (OR and_expr)*
//   and_expr  := not_expr (AND not_expr)*
//   not_expr  := NOT not_expr | cmp_expr
//   cmp_expr  := add_expr [op add_expr | IS [NOT] NULL | BETWEEN ... | LIKE ... | IN ...]
//   add_expr  := mul_expr ((+|-|||) mul_expr)*
//   mul_expr  := unary ((*|/|%) unary)*
//   unary     := (-|+) unary | primary
//   primary   := literal | ident | fn_call | aggregate | subquery | CASE | (expr) | row_access
// ============================================================================

use super::ast::*;
use super::error::VqlError;
use super::lexer::{Token, Spanned};

pub struct Parser {
    tokens: Vec<Spanned>,
    pos:    usize,
}

impl Parser {
    pub fn new(tokens: Vec<Spanned>) -> Self {
        Parser { tokens, pos: 0 }
    }

    // ── Token navigation ──────────────────────────────────────────────────

    fn peek(&self) -> &Token {
        &self.tokens.get(self.pos).map(|s| &s.token).unwrap_or(&Token::Eof)
    }

    fn peek2(&self) -> &Token {
        &self.tokens.get(self.pos + 1).map(|s| &s.token).unwrap_or(&Token::Eof)
    }

    fn line(&self) -> usize {
        self.tokens.get(self.pos).map(|s| s.line).unwrap_or(0)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Spanned { token: Token::Eof, line: 0 }).token;
        if self.pos < self.tokens.len() { self.pos += 1; }
        tok
    }

    fn eat(&mut self, expected: &Token) -> Result<(), VqlError> {
        if self.peek() == expected {
            self.pos += 1;
            Ok(())
        } else {
            Err(VqlError::new(self.line(), format!(
                "expected {:?}, found {:?}", expected, self.peek()
            )))
        }
    }

    fn expect_ident(&mut self) -> Result<String, VqlError> {
        self.keyword_as_ident().ok_or_else(|| {
            VqlError::new(self.line(), format!("expected identifier, found {:?}", self.peek()))
        })
    }

    /// Accept any token as an identifier — keywords can be used as table/column names.
    fn keyword_as_ident(&mut self) -> Option<String> {
        match self.advance() {
            Token::Ident(s) => Some(s),
            // All VQL keywords can be used as identifiers (standard SQL behavior)
            Token::Select => Some("select".into()),
            Token::From => Some("from".into()),
            Token::Where => Some("where".into()),
            Token::And => Some("and".into()),
            Token::Or => Some("or".into()),
            Token::Not => Some("not".into()),
            Token::In => Some("in".into()),
            Token::Is => Some("is".into()),
            Token::Null => Some("null".into()),
            Token::As => Some("as".into()),
            Token::Insert => Some("insert".into()),
            Token::Into => Some("into".into()),
            Token::Values => Some("values".into()),
            Token::Update => Some("update".into()),
            Token::Set => Some("set".into()),
            Token::Delete => Some("delete".into()),
            Token::Join => Some("join".into()),
            Token::Inner => Some("inner".into()),
            Token::Left => Some("left".into()),
            Token::Right => Some("right".into()),
            Token::Outer => Some("outer".into()),
            Token::Full => Some("full".into()),
            Token::Cross => Some("cross".into()),
            Token::On => Some("on".into()),
            Token::Group => Some("group".into()),
            Token::By => Some("by".into()),
            Token::Having => Some("having".into()),
            Token::Order => Some("order".into()),
            Token::Limit => Some("limit".into()),
            Token::Offset => Some("offset".into()),
            Token::Distinct => Some("distinct".into()),
            Token::Union => Some("union".into()),
            Token::All => Some("all".into()),
            Token::Exists => Some("exists".into()),
            Token::Between => Some("between".into()),
            Token::Like => Some("like".into()),
            Token::ILike => Some("ilike".into()),
            Token::Case => Some("case".into()),
            Token::When => Some("when".into()),
            Token::Then => Some("then".into()),
            Token::Else => Some("else".into()),
            Token::End => Some("end".into()),
            Token::Cast => Some("cast".into()),
            Token::Return => Some("return".into()),
            Token::Returning => Some("returning".into()),
            Token::Begin => Some("begin".into()),
            Token::Commit => Some("commit".into()),
            Token::Rollback => Some("rollback".into()),
            Token::Subscribe => Some("subscribe".into()),
            Token::Leaderboard => Some("leaderboard".into()),
            Token::Upsert => Some("upsert".into()),
            Token::Conflict => Some("conflict".into()),
            Token::Do => Some("do".into()),
            Token::Asc => Some("asc".into()),
            Token::Desc => Some("desc".into()),
            Token::Nulls => Some("nulls".into()),
            Token::First => Some("first".into()),
            Token::Last => Some("last".into()),
            Token::Ttl => Some("ttl".into()),
            Token::Count => Some("count".into()),
            Token::Sum => Some("sum".into()),
            Token::Avg => Some("avg".into()),
            Token::Min => Some("min".into()),
            Token::Max => Some("max".into()),
            Token::Upper => Some("upper".into()),
            Token::Lower => Some("lower".into()),
            Token::Length => Some("length".into()),
            Token::Trim => Some("trim".into()),
            Token::Ltrim => Some("ltrim".into()),
            Token::Rtrim => Some("rtrim".into()),
            Token::Replace => Some("replace".into()),
            Token::Substr => Some("substr".into()),
            Token::Round => Some("round".into()),
            Token::Floor => Some("floor".into()),
            Token::Ceil => Some("ceil".into()),
            Token::Abs => Some("abs".into()),
            Token::Coalesce => Some("coalesce".into()),
            Token::Nullif => Some("nullif".into()),
            Token::Now => Some("now".into()),
            Token::Concat => Some("concat".into()),
            Token::Random => Some("random".into()),
            Token::TStr => Some("str".into()),
            Token::TInt => Some("int".into()),
            Token::TFloat => Some("float".into()),
            Token::TBool => Some("bool".into()),
            _ => None,
        }
    }

    fn at_end(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    // ── Top-level ─────────────────────────────────────────────────────────

    pub fn parse_program(&mut self) -> Result<Program, VqlError> {
        let mut statements = Vec::new();
        while !self.at_end() {
            // Skip trailing semicolons
            if self.peek() == &Token::Semi {
                self.advance();
                continue;
            }
            statements.push(self.parse_statement()?);
            // Optional trailing semicolon
            if self.peek() == &Token::Semi {
                self.advance();
            }
        }
        Ok(Program { statements })
    }

    fn parse_statement(&mut self) -> Result<Statement, VqlError> {
        match self.peek().clone() {
            Token::Select      => Ok(Statement::Select(self.parse_select()?)),
            Token::Insert      => Ok(Statement::Insert(self.parse_insert()?)),
            Token::Update      => Ok(Statement::Update(self.parse_update()?)),
            Token::Delete      => Ok(Statement::Delete(self.parse_delete()?)),
            Token::Subscribe   => Ok(Statement::Subscribe(self.parse_subscribe()?)),
            Token::Leaderboard => Ok(Statement::Leaderboard(self.parse_leaderboard()?)),
            Token::Upsert      => Ok(Statement::Upsert(self.parse_upsert()?)),
            Token::Begin       => { self.advance(); Ok(Statement::Begin { line: self.line() }) }
            Token::Commit      => { self.advance(); Ok(Statement::Commit { line: self.line() }) }
            Token::Rollback    => { self.advance(); Ok(Statement::Rollback { line: self.line() }) }
            other => Err(VqlError::new(self.line(), format!(
                "unexpected statement start: {:?} — expected SELECT, INSERT, UPDATE, DELETE, SUBSCRIBE, LEADERBOARD, UPSERT, BEGIN, COMMIT, or ROLLBACK", other
            ))),
        }
    }

    // ── SELECT ────────────────────────────────────────────────────────────

    fn parse_select(&mut self) -> Result<SelectStmt, VqlError> {
        self.eat(&Token::Select)?;

        let distinct = if self.peek() == &Token::Distinct {
            self.advance(); true
        } else { false };

        let columns = self.parse_projection_list()?;

        let mut from = Vec::new();
        let mut joins = Vec::new();
        if self.peek() == &Token::From {
            self.advance();
            from = self.parse_table_refs()?;
            joins = self.parse_joins()?;
        }

        let where_ = if self.peek() == &Token::Where {
            self.advance(); Some(self.parse_expr()?)
        } else { None };

        let group_by = if self.peek() == &Token::Group {
            self.advance(); self.eat(&Token::By)?; self.parse_expr_list()?
        } else { vec![] };

        let having = if self.peek() == &Token::Having {
            self.advance(); Some(self.parse_expr()?)
        } else { None };

        let order_by = if self.peek() == &Token::Order {
            self.advance(); self.eat(&Token::By)?; self.parse_order_by_list()?
        } else { vec![] };

        let limit = if self.peek() == &Token::Limit {
            self.advance(); Some(self.parse_usize("LIMIT")?)
        } else { None };

        let offset = if self.peek() == &Token::Offset {
            self.advance(); Some(self.parse_usize("OFFSET")?)
        } else { None };

        let union = if self.peek() == &Token::Union {
            self.advance();
            let all = self.peek() == &Token::All;
            if all { self.advance(); }
            let rhs = self.parse_select()?;
            Some((all, Box::new(rhs)))
        } else { None };

        Ok(SelectStmt { distinct, columns, from, joins, where_, group_by, having, order_by, limit, offset, union })
    }

    fn parse_usize(&mut self, ctx: &str) -> Result<usize, VqlError> {
        match self.advance() {
            Token::Integer(n) if n >= 0 => Ok(n as usize),
            other => Err(VqlError::new(self.line(), format!("{} requires a non-negative integer, got {:?}", ctx, other))),
        }
    }

    // ── Projection list ───────────────────────────────────────────────────

    fn parse_projection_list(&mut self) -> Result<Vec<Expr>, VqlError> {
        let mut cols = vec![self.parse_projection_item()?];
        while self.peek() == &Token::Comma {
            self.advance();
            cols.push(self.parse_projection_item()?);
        }
        Ok(cols)
    }

    fn parse_projection_item(&mut self) -> Result<Expr, VqlError> {
        if self.peek() == &Token::Star {
            self.advance();
            return Ok(Expr::Wildcard { table: None });
        }
        if matches!(self.peek(), Token::Ident(_)) && self.peek2() == &Token::Dot {
            if let Token::Ident(tbl) = self.peek().clone() {
                let after_dot = self.tokens.get(self.pos + 2).map(|s| &s.token);
                if after_dot == Some(&Token::Star) {
                    self.pos += 3;
                    return Ok(Expr::Wildcard { table: Some(tbl) });
                }
            }
        }
        let expr = self.parse_expr()?;
        let alias = if self.peek() == &Token::As {
            self.advance(); Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else { None };
        Ok(match alias {
            Some(a) => Expr::Alias { expr: Box::new(expr), alias: a },
            None    => expr,
        })
    }

    // ── FROM / JOIN ───────────────────────────────────────────────────────

    fn parse_table_refs(&mut self) -> Result<Vec<TableRef>, VqlError> {
        let mut refs = vec![self.parse_table_ref()?];
        while self.peek() == &Token::Comma {
            self.advance();
            refs.push(self.parse_table_ref()?);
        }
        Ok(refs)
    }

    fn parse_table_ref(&mut self) -> Result<TableRef, VqlError> {
        if self.peek() == &Token::LParen {
            self.advance();
            let query = self.parse_select()?;
            self.eat(&Token::RParen)?;
            let alias = if self.peek() == &Token::As {
                self.advance(); self.expect_ident()?
            } else { self.expect_ident()? };
            return Ok(TableRef::Subquery { query: Box::new(query), alias });
        }
        let name = self.expect_ident()?;
        let alias = if self.peek() == &Token::As {
            self.advance(); Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) {
            Some(self.expect_ident()?)
        } else { None };
        Ok(TableRef::Named { name, alias })
    }

    fn parse_joins(&mut self) -> Result<Vec<Join>, VqlError> {
        let mut joins = Vec::new();
        loop {
            let kind = match self.peek() {
                Token::Join  => { self.advance(); JoinKind::Inner }
                Token::Inner => { self.advance(); self.eat(&Token::Join)?; JoinKind::Inner }
                Token::Left  => {
                    self.advance();
                    if self.peek() == &Token::Outer { self.advance(); }
                    self.eat(&Token::Join)?; JoinKind::Left
                }
                Token::Right => {
                    self.advance();
                    if self.peek() == &Token::Outer { self.advance(); }
                    self.eat(&Token::Join)?; JoinKind::Right
                }
                Token::Full  => {
                    self.advance();
                    if self.peek() == &Token::Outer { self.advance(); }
                    self.eat(&Token::Join)?; JoinKind::Full
                }
                Token::Cross => { self.advance(); self.eat(&Token::Join)?; JoinKind::Cross }
                _ => break,
            };
            let table = self.parse_table_ref()?;
            let on = if self.peek() == &Token::On {
                self.advance(); Some(self.parse_expr()?)
            } else { None };
            joins.push(Join { kind, table, on });
        }
        Ok(joins)
    }

    // ── ORDER BY ──────────────────────────────────────────────────────────

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>, VqlError> {
        let mut items = vec![self.parse_order_by_item()?];
        while self.peek() == &Token::Comma {
            self.advance();
            items.push(self.parse_order_by_item()?);
        }
        Ok(items)
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, VqlError> {
        let expr = self.parse_expr()?;
        let asc = match self.peek() {
            Token::Asc  => { self.advance(); true }
            Token::Desc => { self.advance(); false }
            _           => true,
        };
        let nulls_first = if self.peek() == &Token::Nulls {
            self.advance();
            match self.peek().clone() {
                Token::First => { self.advance(); Some(true) }
                Token::Last  => { self.advance(); Some(false) }
                _ => None,
            }
        } else { None };
        Ok(OrderByItem { expr, asc, nulls_first })
    }

    // ── INSERT ────────────────────────────────────────────────────────────

    fn parse_insert(&mut self) -> Result<InsertStmt, VqlError> {
        self.eat(&Token::Insert)?;
        self.eat(&Token::Into)?;
        let table = self.expect_ident()?;

        // Column list (optional)
        let columns = if self.peek() == &Token::LParen {
            self.advance();
            let mut cols = vec![self.expect_ident()?];
            while self.peek() == &Token::Comma {
                self.advance();
                cols.push(self.expect_ident()?);
            }
            self.eat(&Token::RParen)?;
            cols
        } else { vec![] };

        self.eat(&Token::Values)?;
        let mut all_values = Vec::new();
        loop {
            self.eat(&Token::LParen)?;
            let row = self.parse_expr_list()?;
            self.eat(&Token::RParen)?;
            all_values.push(row);
            if self.peek() == &Token::Comma { self.advance(); } else { break; }
        }

        // ON CONFLICT (optional)
        let upsert = if self.peek() == &Token::On {
            self.advance();
            self.eat(&Token::Conflict)?;
            self.eat(&Token::LParen)?;
            let mut conflict_cols = vec![self.expect_ident()?];
            while self.peek() == &Token::Comma {
                self.advance();
                conflict_cols.push(self.expect_ident()?);
            }
            self.eat(&Token::RParen)?;
            self.eat(&Token::Do)?;
            self.eat(&Token::Update)?;
            self.eat(&Token::Set)?;
            let mut do_update = Vec::new();
            loop {
                let col = self.expect_ident()?;
                self.eat(&Token::Eq)?;
                let val = self.parse_expr()?;
                do_update.push((col, val));
                if self.peek() == &Token::Comma { self.advance(); } else { break; }
            }
            Some(UpsertClause { conflict_columns: conflict_cols, do_update })
        } else { None };

        // TTL (optional)
        let ttl = if self.peek() == &Token::Ttl {
            self.advance();
            let seconds = self.parse_expr()?;
            Some(TtlClause { seconds })
        } else { None };

        // RETURNING (optional)
        let returning = if self.peek() == &Token::Returning {
            self.advance();
            Some(ReturningClause { columns: self.parse_projection_list()? })
        } else { None };

        Ok(InsertStmt { table, columns, values: all_values, upsert, ttl, returning })
    }

    // ── UPDATE ────────────────────────────────────────────────────────────

    fn parse_update(&mut self) -> Result<UpdateStmt, VqlError> {
        self.eat(&Token::Update)?;
        let table = self.expect_ident()?;
        let alias = if self.peek() == &Token::As {
            self.advance(); Some(self.expect_ident()?)
        } else if matches!(self.peek(), Token::Ident(_)) && self.peek2() == &Token::Set {
            Some(self.expect_ident()?)
        } else { None };

        self.eat(&Token::Set)?;
        let mut sets = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.eat(&Token::Eq)?;
            let val = self.parse_expr()?;
            sets.push((col, val));
            if self.peek() == &Token::Comma { self.advance(); } else { break; }
        }

        let where_ = if self.peek() == &Token::Where {
            self.advance(); Some(self.parse_expr()?)
        } else { None };

        let returning = if self.peek() == &Token::Returning {
            self.advance();
            Some(ReturningClause { columns: self.parse_projection_list()? })
        } else { None };

        Ok(UpdateStmt { table, alias, sets, where_, returning })
    }

    // ── DELETE ────────────────────────────────────────────────────────────

    fn parse_delete(&mut self) -> Result<DeleteStmt, VqlError> {
        self.eat(&Token::Delete)?;
        self.eat(&Token::From)?;
        let table = self.expect_ident()?;

        let where_ = if self.peek() == &Token::Where {
            self.advance(); Some(self.parse_expr()?)
        } else { None };

        let returning = if self.peek() == &Token::Returning {
            self.advance();
            Some(ReturningClause { columns: self.parse_projection_list()? })
        } else { None };

        Ok(DeleteStmt { table, where_, returning })
    }

    // ── SUBSCRIBE (reactive push) ─────────────────────────────────────────

    fn parse_subscribe(&mut self) -> Result<SubscribeStmt, VqlError> {
        self.eat(&Token::Subscribe)?;
        let table = self.expect_ident()?;
        let alias = if self.peek() == &Token::As {
            self.advance(); Some(self.expect_ident()?)
        } else { None };

        let where_ = if self.peek() == &Token::Where {
            self.advance(); Some(self.parse_expr()?)
        } else { None };

        let order_by = if self.peek() == &Token::Order {
            self.advance(); self.eat(&Token::By)?; self.parse_order_by_list()?
        } else { vec![] };

        let limit = if self.peek() == &Token::Limit {
            self.advance(); Some(self.parse_usize("LIMIT")?)
        } else { None };

        Ok(SubscribeStmt { table, alias, where_, order_by, limit })
    }

    // ── LEADERBOARD (game primitive) ──────────────────────────────────────

    fn parse_leaderboard(&mut self) -> Result<LeaderboardStmt, VqlError> {
        self.eat(&Token::Leaderboard)?;
        let table = self.expect_ident()?;
        self.eat(&Token::By)?;
        let by = self.expect_ident()?;

        let asc = match self.peek() {
            Token::Asc  => { self.advance(); true }
            Token::Desc => { self.advance(); false }
            _           => false, // DESC is default for leaderboards
        };

        let limit = if self.peek() == &Token::Limit {
            self.advance(); Some(self.parse_usize("LIMIT")?)
        } else { None };

        let where_ = if self.peek() == &Token::Where {
            self.advance(); Some(self.parse_expr()?)
        } else { None };

        Ok(LeaderboardStmt { table, by, asc, limit, where_ })
    }

    // ── UPSERT (game primitive) ──────────────────────────────────────────

    fn parse_upsert(&mut self) -> Result<UpsertStmt, VqlError> {
        self.eat(&Token::Upsert)?;
        let table = self.expect_ident()?;
        self.eat(&Token::LBracket)?;
        let key = self.parse_expr()?;
        self.eat(&Token::RBracket)?;

        self.eat(&Token::Set)?;
        let mut sets = Vec::new();
        loop {
            let col = self.expect_ident()?;
            self.eat(&Token::Eq)?;
            let val = self.parse_expr()?;
            sets.push((col, val));
            if self.peek() == &Token::Comma { self.advance(); } else { break; }
        }

        let ttl = if self.peek() == &Token::Ttl {
            self.advance();
            let seconds = self.parse_expr()?;
            Some(TtlClause { seconds })
        } else { None };

        Ok(UpsertStmt { table, key: Box::new(key), sets, ttl })
    }

    // ── Argument list ─────────────────────────────────────────────────────

    fn parse_arg_list(&mut self) -> Result<Vec<Expr>, VqlError> {
        let mut args = Vec::new();
        while self.peek() != &Token::RParen && !self.at_end() {
            args.push(self.parse_expr()?);
            if self.peek() == &Token::Comma { self.advance(); }
        }
        Ok(args)
    }

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>, VqlError> {
        let mut exprs = vec![self.parse_expr()?];
        while self.peek() == &Token::Comma {
            self.advance();
            exprs.push(self.parse_expr()?);
        }
        Ok(exprs)
    }

    // ── Expression parser (Pratt) ────────────────────────────────────────

    fn parse_expr(&mut self) -> Result<Expr, VqlError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, VqlError> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinaryOp { left: Box::new(left), op: BinOp::Or, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, VqlError> {
        let mut left = self.parse_not()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_not()?;
            left = Expr::BinaryOp { left: Box::new(left), op: BinOp::And, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_not(&mut self) -> Result<Expr, VqlError> {
        if self.peek() == &Token::Not {
            self.advance();
            let expr = self.parse_not()?;
            return Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(expr) });
        }
        self.parse_comparison()
    }

    fn parse_comparison(&mut self) -> Result<Expr, VqlError> {
        let left = self.parse_add()?;

        // IS [NOT] NULL
        if self.peek() == &Token::Is {
            self.advance();
            let negated = self.peek() == &Token::Not;
            if negated { self.advance(); }
            self.eat(&Token::Null)?;
            return Ok(Expr::IsNull { expr: Box::new(left), negated });
        }

        // [NOT] BETWEEN ... AND ...
        let negated_between = self.peek() == &Token::Not && self.peek2() == &Token::Between;
        if negated_between { self.advance(); }
        if self.peek() == &Token::Between {
            self.advance();
            let low = self.parse_add()?;
            self.eat(&Token::And)?;
            let high = self.parse_add()?;
            return Ok(Expr::Between { expr: Box::new(left), low: Box::new(low), high: Box::new(high), negated: negated_between });
        }

        // [NOT] LIKE / ILIKE
        let negated_like = self.peek() == &Token::Not && matches!(self.peek2(), Token::Like | Token::ILike);
        if negated_like { self.advance(); }
        if self.peek() == &Token::Like {
            self.advance();
            let pattern = self.parse_add()?;
            return Ok(Expr::Like { expr: Box::new(left), pattern: Box::new(pattern), negated: negated_like });
        }
        if self.peek() == &Token::ILike {
            self.advance();
            let pattern = self.parse_add()?;
            return Ok(Expr::ILike { expr: Box::new(left), pattern: Box::new(pattern), negated: negated_like });
        }

        // [NOT] IN (list | subquery)
        let negated_in = self.peek() == &Token::Not && self.peek2() == &Token::In;
        if negated_in { self.advance(); }
        if self.peek() == &Token::In {
            self.advance();
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
            Token::BangEq => BinOp::Ne,
            Token::Lt    => BinOp::Lt,
            Token::LtEq  => BinOp::Le,
            Token::Gt    => BinOp::Gt,
            Token::GtEq  => BinOp::Ge,
            _            => return Ok(left),
        };
        self.advance();
        let right = self.parse_add()?;
        Ok(Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) })
    }

    fn parse_add(&mut self) -> Result<Expr, VqlError> {
        let mut left = self.parse_mul()?;
        loop {
            let op = match self.peek() {
                Token::Plus     => BinOp::Add,
                Token::Minus    => BinOp::Sub,
                Token::PipePipe => BinOp::Concat,
                Token::Pipe     => BinOp::Concat,
                _ => break,
            };
            self.advance();
            let right = self.parse_mul()?;
            left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_mul(&mut self) -> Result<Expr, VqlError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star    => BinOp::Mul,
                Token::Slash   => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, VqlError> {
        match self.peek() {
            Token::Minus => {
                self.advance();
                let e = self.parse_unary()?;
                Ok(Expr::UnaryOp { op: UnaryOp::Neg, expr: Box::new(e) })
            }
            Token::Plus => {
                self.advance();
                let e = self.parse_unary()?;
                Ok(Expr::UnaryOp { op: UnaryOp::Pos, expr: Box::new(e) })
            }
            Token::Bang => {
                self.advance();
                let e = self.parse_unary()?;
                Ok(Expr::UnaryOp { op: UnaryOp::Not, expr: Box::new(e) })
            }
            _ => self.parse_postfix(),
        }
    }

    fn parse_postfix(&mut self) -> Result<Expr, VqlError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.peek() == &Token::Dot {
                self.advance();
                let field = self.expect_ident()?;
                expr = Expr::FieldAccess { object: Box::new(expr), field };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr, VqlError> {
        match self.peek().clone() {
            // ── Literals ──────────────────────────────────────────────────
            Token::Integer(n)   => { self.advance(); Ok(Expr::Literal(serde_json::json!(n))) }
            Token::Float(f)     => { self.advance(); Ok(Expr::Literal(serde_json::json!(f))) }
            Token::StringLit(s) => { self.advance(); Ok(Expr::Literal(serde_json::Value::String(s))) }
            Token::BoolLit(b)   => { self.advance(); Ok(Expr::Literal(serde_json::json!(b))) }
            Token::Null         => { self.advance(); Ok(Expr::Literal(serde_json::Value::Null)) }

            // ── Parenthesised expression or subquery ──────────────────────
            Token::LParen => {
                self.advance();
                if self.peek() == &Token::Select {
                    let query = self.parse_select()?;
                    self.eat(&Token::RParen)?;
                    return Ok(Expr::Subquery(Box::new(query)));
                }
                let expr = self.parse_expr()?;
                self.eat(&Token::RParen)?;
                Ok(expr)
            }

            // ── EXISTS (subquery) ────────────────────────────────────────
            Token::Exists => {
                self.advance();
                self.eat(&Token::LParen)?;
                let query = self.parse_select()?;
                self.eat(&Token::RParen)?;
                Ok(Expr::Exists { query: Box::new(query), negated: false })
            }

            // ── CASE ─────────────────────────────────────────────────────
            Token::Case => self.parse_case(),

            // ── Aggregates ───────────────────────────────────────────────
            Token::Count => self.parse_aggregate(AggFunc::Count),
            Token::Sum   => self.parse_aggregate(AggFunc::Sum),
            Token::Avg   => self.parse_aggregate(AggFunc::Avg),
            Token::Min   => self.parse_aggregate(AggFunc::Min),
            Token::Max   => self.parse_aggregate(AggFunc::Max),

            // ── Scalar functions ──────────────────────────────────────────
            Token::Upper    => self.parse_scalar_fn("upper"),
            Token::Lower    => self.parse_scalar_fn("lower"),
            Token::Length   => self.parse_scalar_fn("length"),
            Token::Trim     => self.parse_scalar_fn("trim"),
            Token::Ltrim    => self.parse_scalar_fn("ltrim"),
            Token::Rtrim    => self.parse_scalar_fn("rtrim"),
            Token::Replace  => self.parse_scalar_fn("replace"),
            Token::Substr   => self.parse_scalar_fn("substr"),
            Token::Round    => self.parse_scalar_fn("round"),
            Token::Floor    => self.parse_scalar_fn("floor"),
            Token::Ceil     => self.parse_scalar_fn("ceil"),
            Token::Abs      => self.parse_scalar_fn("abs"),
            Token::Coalesce => self.parse_scalar_fn("coalesce"),
            Token::Nullif   => self.parse_scalar_fn("nullif"),
            Token::Now      => self.parse_scalar_fn("now"),
            Token::Concat   => self.parse_scalar_fn("concat"),
            Token::Random   => self.parse_scalar_fn("random"),

            // ── CAST ─────────────────────────────────────────────────────
            Token::Cast => self.parse_cast(),

            // ── Identifier / qualified column ────────────────────────────
            Token::Ident(name) => {
                self.advance();
                // table.column
                if self.peek() == &Token::Dot {
                    self.advance();
                    if self.peek() == &Token::Star {
                        self.advance();
                        return Ok(Expr::Wildcard { table: Some(name) });
                    }
                    let col = self.expect_ident()?;
                    return Ok(Expr::Column { table: Some(name), name: col });
                }
                // function call
                if self.peek() == &Token::LParen {
                    self.advance();
                    let args = if self.peek() == &Token::RParen { vec![] } else { self.parse_arg_list()? };
                    self.eat(&Token::RParen)?;
                    return Ok(Expr::Function { name, args });
                }
                // table[key] — row access
                if self.peek() == &Token::LBracket {
                    self.advance();
                    let key = self.parse_expr()?;
                    self.eat(&Token::RBracket)?;
                    return Ok(Expr::RowAccess { table: name, key: Box::new(key) });
                }
                Ok(Expr::Column { table: None, name })
            }

            other => Err(VqlError::new(self.line(), format!("unexpected token in expression: {:?}", other))),
        }
    }

    fn parse_case(&mut self) -> Result<Expr, VqlError> {
        self.eat(&Token::Case)?;
        let operand = if self.peek() != &Token::When {
            Some(Box::new(self.parse_expr()?))
        } else { None };

        let mut branches = Vec::new();
        while self.peek() == &Token::When {
            self.advance();
            let cond = self.parse_expr()?;
            self.eat(&Token::Then)?;
            let result = self.parse_expr()?;
            branches.push((cond, result));
        }

        let else_ = if self.peek() == &Token::Else {
            self.advance(); Some(Box::new(self.parse_expr()?))
        } else { None };

        self.eat(&Token::End)?;
        Ok(Expr::Case { operand, branches, else_ })
    }

    fn parse_aggregate(&mut self, func: AggFunc) -> Result<Expr, VqlError> {
        self.advance();
        self.eat(&Token::LParen)?;
        let distinct = self.peek() == &Token::Distinct;
        if distinct { self.advance(); }
        let arg = if self.peek() == &Token::Star && matches!(func, AggFunc::Count) {
            self.advance(); None
        } else {
            Some(Box::new(self.parse_expr()?))
        };
        self.eat(&Token::RParen)?;
        Ok(Expr::Aggregate { func, distinct, arg })
    }

    fn parse_scalar_fn(&mut self, name: &str) -> Result<Expr, VqlError> {
        self.advance();
        self.eat(&Token::LParen)?;
        let args = if self.peek() == &Token::RParen { vec![] } else { self.parse_arg_list()? };
        self.eat(&Token::RParen)?;
        Ok(Expr::Function { name: name.to_string(), args })
    }

    fn parse_cast(&mut self) -> Result<Expr, VqlError> {
        self.advance();
        self.eat(&Token::LParen)?;
        let expr = self.parse_expr()?;
        self.eat(&Token::As)?;
        let type_name = self.expect_ident()?;
        self.eat(&Token::RParen)?;
        Ok(Expr::Function { name: format!("cast::{}", type_name), args: vec![expr] })
    }
}

// ── Public entry points ────────────────────────────────────────────────────────

pub fn parse(tokens: Vec<Spanned>) -> Result<Program, VqlError> {
    let mut p = Parser::new(tokens);
    p.parse_program()
}

pub fn parse_statement(tokens: Vec<Spanned>) -> Result<Statement, VqlError> {
    let mut p = Parser::new(tokens);
    let stmt = p.parse_statement()?;
    if !p.at_end() && p.peek() == &Token::Semi { p.advance(); }
    Ok(stmt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vql::lexer::tokenize;

    fn parse_src(src: &str) -> Program {
        let tokens = tokenize(src).expect("lex failed");
        parse(tokens).expect("parse failed")
    }

    fn parse_one(src: &str) -> Statement {
        let prog = parse_src(src);
        assert_eq!(prog.statements.len(), 1, "expected 1 statement, got {}", prog.statements.len());
        prog.statements.into_iter().next().unwrap()
    }

    // ── SELECT tests ──────────────────────────────────────────────────────

    #[test]
    fn select_star() {
        let s = parse_one("SELECT * FROM players");
        assert!(matches!(s, Statement::Select(SelectStmt { columns, .. }) if columns.len() == 1));
    }

    #[test]
    fn select_with_where() {
        let s = parse_one("SELECT id, score FROM players WHERE level > 5");
        match s {
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 2);
                assert!(sel.where_.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_with_alias() {
        let s = parse_one("SELECT score AS pts FROM players");
        match s {
            Statement::Select(sel) => {
                assert!(matches!(&sel.columns[0], Expr::Alias { alias, .. } if alias == "pts"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_join() {
        let s = parse_one("SELECT p.id, i.name FROM players p JOIN items i ON p.item_id = i.id");
        match s {
            Statement::Select(sel) => {
                assert_eq!(sel.joins.len(), 1);
                assert!(matches!(sel.joins[0].kind, JoinKind::Inner));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_left_join() {
        let s = parse_one("SELECT * FROM players p LEFT JOIN guilds g ON p.guild_id = g.id");
        match s {
            Statement::Select(sel) => assert!(matches!(sel.joins[0].kind, JoinKind::Left)),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_group_by_having() {
        let s = parse_one("SELECT zone, COUNT(*) FROM players GROUP BY zone HAVING COUNT(*) > 2");
        match s {
            Statement::Select(sel) => {
                assert_eq!(sel.group_by.len(), 1);
                assert!(sel.having.is_some());
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_order_by_limit() {
        let s = parse_one("SELECT * FROM scores ORDER BY score DESC LIMIT 10 OFFSET 20");
        match s {
            Statement::Select(sel) => {
                assert_eq!(sel.order_by.len(), 1);
                assert!(!sel.order_by[0].asc);
                assert_eq!(sel.limit, Some(10));
                assert_eq!(sel.offset, Some(20));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_in_list() {
        let s = parse_one("SELECT * FROM players WHERE status IN ('active', 'vip')");
        match s {
            Statement::Select(sel) => {
                match sel.where_.unwrap() {
                    Expr::InList { list, negated, .. } => {
                        assert!(!negated);
                        assert_eq!(list.len(), 2);
                    }
                    other => panic!("{:?}", other),
                }
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_between() {
        let s = parse_one("SELECT * FROM scores WHERE value BETWEEN 10 AND 100");
        match s {
            Statement::Select(sel) => assert!(matches!(sel.where_.unwrap(), Expr::Between { .. })),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_like() {
        let s = parse_one("SELECT * FROM players WHERE name LIKE 'al%'");
        match s {
            Statement::Select(sel) => assert!(matches!(sel.where_.unwrap(), Expr::Like { .. })),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_distinct() {
        let s = parse_one("SELECT DISTINCT zone FROM players");
        match s {
            Statement::Select(sel) => assert!(sel.distinct),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_case() {
        let s = parse_one("SELECT CASE WHEN score > 100 THEN 'high' ELSE 'low' END FROM players");
        match s {
            Statement::Select(sel) => assert!(matches!(&sel.columns[0], Expr::Case { .. })),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_subquery() {
        let s = parse_one("SELECT * FROM players WHERE id IN (SELECT id FROM vip_list)");
        match s {
            Statement::Select(sel) => assert!(matches!(sel.where_.unwrap(), Expr::InSubquery { .. })),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_union() {
        let s = parse_one("SELECT id FROM players UNION ALL SELECT id FROM bots");
        match s {
            Statement::Select(sel) => {
                assert!(sel.union.is_some());
                let (all, _) = sel.union.unwrap();
                assert!(all);
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn select_exists() {
        let s = parse_one("SELECT * FROM players WHERE EXISTS (SELECT 1 FROM guilds WHERE guilds.owner = players.id)");
        match s {
            Statement::Select(sel) => assert!(matches!(sel.where_.unwrap(), Expr::Exists { negated: false, .. })),
            _ => panic!("expected Select"),
        }
    }

    // ── INSERT tests ──────────────────────────────────────────────────────

    #[test]
    fn insert_values() {
        let s = parse_one("INSERT INTO players (id, score) VALUES ('p1', 100), ('p2', 200)");
        match s {
            Statement::Insert(ins) => {
                assert_eq!(ins.table, "players");
                assert_eq!(ins.columns, vec!["id", "score"]);
                assert_eq!(ins.values.len(), 2);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn insert_with_ttl() {
        let s = parse_one("INSERT INTO sessions (id, data) VALUES ('s1', 'abc') TTL 3600");
        match s {
            Statement::Insert(ins) => {
                assert!(ins.ttl.is_some());
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn insert_with_on_conflict() {
        let s = parse_one("INSERT INTO players (id, score) VALUES ('p1', 100) ON CONFLICT (id) DO UPDATE SET score = 100");
        match s {
            Statement::Insert(ins) => {
                assert!(ins.upsert.is_some());
                let up = ins.upsert.unwrap();
                assert_eq!(up.conflict_columns, vec!["id"]);
                assert_eq!(up.do_update.len(), 1);
            }
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn insert_returning() {
        let s = parse_one("INSERT INTO t (x) VALUES (1) RETURNING *");
        match s {
            Statement::Insert(ins) => assert!(ins.returning.is_some()),
            _ => panic!("expected Insert"),
        }
    }

    // ── UPDATE tests ──────────────────────────────────────────────────────

    #[test]
    fn update_where() {
        let s = parse_one("UPDATE players SET score = 999 WHERE id = 'p1'");
        match s {
            Statement::Update(upd) => {
                assert_eq!(upd.table, "players");
                assert_eq!(upd.sets[0].0, "score");
                assert!(upd.where_.is_some());
            }
            _ => panic!("expected Update"),
        }
    }

    #[test]
    fn update_returning() {
        let s = parse_one("UPDATE t SET x = 1 RETURNING *");
        match s {
            Statement::Update(upd) => assert!(upd.returning.is_some()),
            _ => panic!("expected Update"),
        }
    }

    // ── DELETE tests ──────────────────────────────────────────────────────

    #[test]
    fn delete_where() {
        let s = parse_one("DELETE FROM players WHERE id = 'p1'");
        match s {
            Statement::Delete(del) => {
                assert_eq!(del.table, "players");
                assert!(del.where_.is_some());
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn delete_returning() {
        let s = parse_one("DELETE FROM t WHERE x = 1 RETURNING *");
        match s {
            Statement::Delete(del) => assert!(del.returning.is_some()),
            _ => panic!("expected Delete"),
        }
    }

    // ── SUBSCRIBE tests ───────────────────────────────────────────────────

    #[test]
    fn subscribe_simple() {
        let s = parse_one("SUBSCRIBE players");
        match s {
            Statement::Subscribe(sub) => {
                assert_eq!(sub.table, "players");
                assert!(sub.where_.is_none());
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn subscribe_with_where() {
        let s = parse_one("SUBSCRIBE players WHERE zone = 'z1' AND status IN ('alive', 'idle')");
        match s {
            Statement::Subscribe(sub) => {
                assert!(sub.where_.is_some());
            }
            _ => panic!("expected Subscribe"),
        }
    }

    #[test]
    fn subscribe_with_order_limit() {
        let s = parse_one("SUBSCRIBE players WHERE zone = 'z1' ORDER BY score DESC LIMIT 50");
        match s {
            Statement::Subscribe(sub) => {
                assert_eq!(sub.order_by.len(), 1);
                assert!(!sub.order_by[0].asc);
                assert_eq!(sub.limit, Some(50));
            }
            _ => panic!("expected Subscribe"),
        }
    }

    // ── LEADERBOARD tests ─────────────────────────────────────────────────

    #[test]
    fn leaderboard_simple() {
        let s = parse_one("LEADERBOARD scores BY score DESC LIMIT 10");
        match s {
            Statement::Leaderboard(lb) => {
                assert_eq!(lb.table, "scores");
                assert_eq!(lb.by, "score");
                assert!(!lb.asc);
                assert_eq!(lb.limit, Some(10));
            }
            _ => panic!("expected Leaderboard"),
        }
    }

    #[test]
    fn leaderboard_asc() {
        let s = parse_one("LEADERBOARD times BY duration ASC");
        match s {
            Statement::Leaderboard(lb) => assert!(lb.asc),
            _ => panic!("expected Leaderboard"),
        }
    }

    // ── UPSERT tests ──────────────────────────────────────────────────────

    #[test]
    fn upsert_simple() {
        let s = parse_one("UPSERT players['p1'] SET hp = 100, alive = true");
        match s {
            Statement::Upsert(up) => {
                assert_eq!(up.table, "players");
                assert_eq!(up.sets.len(), 2);
            }
            _ => panic!("expected Upsert"),
        }
    }

    #[test]
    fn upsert_with_ttl() {
        let s = parse_one("UPSERT items['i1'] SET count = 5 TTL 60");
        match s {
            Statement::Upsert(up) => {
                assert!(up.ttl.is_some());
            }
            _ => panic!("expected Upsert"),
        }
    }

    // ── Transaction tests ─────────────────────────────────────────────────

    #[test]
    fn begin_commit_rollback() {
        let prog = parse_src("BEGIN; COMMIT; ROLLBACK");
        assert_eq!(prog.statements.len(), 3);
        assert!(matches!(prog.statements[0], Statement::Begin { .. }));
        assert!(matches!(prog.statements[1], Statement::Commit { .. }));
        assert!(matches!(prog.statements[2], Statement::Rollback { .. }));
    }

    // ── Complex queries ───────────────────────────────────────────────────

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
        let s = parse_one(sql);
        match s {
            Statement::Select(sel) => {
                assert_eq!(sel.columns.len(), 3);
                assert_eq!(sel.joins.len(), 1);
                assert!(sel.where_.is_some());
                assert_eq!(sel.group_by.len(), 2);
                assert!(sel.having.is_some());
                assert_eq!(sel.limit, Some(20));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn row_access_in_expr() {
        let s = parse_one("SELECT players['p1'].name FROM players");
        match s {
            Statement::Select(sel) => {
                assert!(matches!(&sel.columns[0], Expr::FieldAccess { field, .. } if field == "name"));
            }
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn function_call_in_where() {
        let s = parse_one("SELECT * FROM players WHERE LENGTH(name) > 5");
        match s {
            Statement::Select(sel) => assert!(sel.where_.is_some()),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn is_null_syntax() {
        let s = parse_one("SELECT * FROM players WHERE deleted_at IS NULL");
        match s {
            Statement::Select(sel) => assert!(matches!(sel.where_.unwrap(), Expr::IsNull { negated: false, .. })),
            _ => panic!("expected Select"),
        }
    }

    #[test]
    fn is_not_null_syntax() {
        let s = parse_one("SELECT * FROM players WHERE deleted_at IS NOT NULL");
        match s {
            Statement::Select(sel) => assert!(matches!(sel.where_.unwrap(), Expr::IsNull { negated: true, .. })),
            _ => panic!("expected Select"),
        }
    }
}
