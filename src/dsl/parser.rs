// ============================================================================
// .neon DSL — Recursive descent parser
// ============================================================================

use super::ast::*;
use super::error::NeonError;
use super::lexer::{Token, Spanned};

struct Parser {
    tokens: Vec<Spanned>,
    pos:    usize,
}

impl Parser {
    fn new(tokens: Vec<Spanned>) -> Self {
        Parser { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos].token
    }

    fn line(&self) -> usize {
        self.tokens[self.pos].line
    }

    fn advance(&mut self) -> &Token {
        let t = &self.tokens[self.pos].token;
        if self.pos + 1 < self.tokens.len() { self.pos += 1; }
        t
    }

    fn eat(&mut self, expected: &Token) -> Result<(), NeonError> {
        if self.peek() == expected {
            self.advance();
            Ok(())
        } else {
            Err(NeonError::new(self.line(), format!(
                "expected {:?}, found {:?}", expected, self.peek()
            )))
        }
    }

    fn eat_ident(&mut self) -> Result<String, NeonError> {
        match self.peek().clone() {
            Token::Ident(s) => { self.advance(); Ok(s) }
            other => Err(NeonError::new(self.line(), format!("expected identifier, found {:?}", other))),
        }
    }

    fn eat_type(&mut self) -> Result<Type, NeonError> {
        let line = self.line();
        let ty = match self.peek() {
            Token::TStr   => Type::Str,
            Token::TInt   => Type::Int,
            Token::TFloat => Type::Float,
            Token::TBool  => Type::Bool,
            other => return Err(NeonError::new(line, format!("expected type (str/int/float/bool), found {:?}", other))),
        };
        self.advance();
        Ok(ty)
    }

    fn at_end(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    // ── Top-level ─────────────────────────────────────────────────────────────

    fn parse_program(&mut self) -> Result<Program, NeonError> {
        let mut tables   = Vec::new();
        let mut reducers = Vec::new();

        while !self.at_end() {
            match self.peek() {
                Token::Table   => tables.push(self.parse_table()?),
                Token::Reducer => reducers.push(self.parse_reducer()?),
                other => return Err(NeonError::new(self.line(), format!(
                    "expected 'table' or 'reducer', found {:?}", other
                ))),
            }
        }

        Ok(Program { tables, reducers })
    }

    // ── Table declaration ─────────────────────────────────────────────────────

    fn parse_table(&mut self) -> Result<TableDecl, NeonError> {
        let line = self.line();
        self.eat(&Token::Table)?;
        let name = self.eat_ident()?;
        self.eat(&Token::LBrace)?;

        let mut fields = Vec::new();
        while self.peek() != &Token::RBrace && !self.at_end() {
            fields.push(self.parse_field_def()?);
            if self.peek() == &Token::Comma { self.advance(); }
        }

        self.eat(&Token::RBrace)?;
        Ok(TableDecl { name, fields, line })
    }

    fn parse_field_def(&mut self) -> Result<FieldDef, NeonError> {
        let line = self.line();
        let name = self.eat_ident()?;
        self.eat(&Token::Colon)?;
        let ty = self.eat_type()?;

        let default = if self.peek() == &Token::Eq {
            self.advance();
            Some(self.parse_literal()?)
        } else {
            None
        };

        Ok(FieldDef { name, ty, default, line })
    }

    fn parse_literal(&mut self) -> Result<Literal, NeonError> {
        let line = self.line();
        match self.peek().clone() {
            Token::IntLit(n)  => { self.advance(); Ok(Literal::Int(n)) }
            Token::FloatLit(f)=> { self.advance(); Ok(Literal::Float(f)) }
            Token::StrLit(s)  => { self.advance(); Ok(Literal::Str(s)) }
            Token::BoolLit(b) => { self.advance(); Ok(Literal::Bool(b)) }
            other => Err(NeonError::new(line, format!("expected literal, found {:?}", other))),
        }
    }

    // ── Reducer declaration ───────────────────────────────────────────────────

    fn parse_reducer(&mut self) -> Result<ReducerDecl, NeonError> {
        let line = self.line();
        self.eat(&Token::Reducer)?;
        let name = self.eat_ident()?;
        self.eat(&Token::LParen)?;

        let mut params = Vec::new();
        while self.peek() != &Token::RParen && !self.at_end() {
            let pname = self.eat_ident()?;
            self.eat(&Token::Colon)?;
            let ty = self.eat_type()?;
            params.push(Param { name: pname, ty });
            if self.peek() == &Token::Comma { self.advance(); }
        }

        self.eat(&Token::RParen)?;
        let body = self.parse_block()?;
        Ok(ReducerDecl { name, params, body, line })
    }

    // ── Block: { stmt* } ─────────────────────────────────────────────────────

    fn parse_block(&mut self) -> Result<Vec<Stmt>, NeonError> {
        self.eat(&Token::LBrace)?;
        let mut stmts = Vec::new();
        while self.peek() != &Token::RBrace && !self.at_end() {
            stmts.push(self.parse_stmt()?);
        }
        self.eat(&Token::RBrace)?;
        Ok(stmts)
    }

    // ── Statement ─────────────────────────────────────────────────────────────

    fn parse_stmt(&mut self) -> Result<Stmt, NeonError> {
        let line = self.line();
        match self.peek().clone() {
            Token::Let      => self.parse_let(line),
            Token::Delete   => self.parse_delete(line),
            Token::If       => self.parse_if(line),
            Token::Return   => self.parse_return(line),
            Token::Error    => self.parse_error_stmt(line),
            Token::For      => self.parse_for_stmt(line),
            Token::While    => self.parse_while_stmt(line),
            Token::Break    => { self.advance(); Ok(Stmt::Break { line }) }
            Token::Continue => { self.advance(); Ok(Stmt::Continue { line }) }
            Token::Ident(name) => self.parse_ident_stmt(name, line),
            other => Err(NeonError::new(line, format!("unexpected statement start: {:?}", other))),
        }
    }

    fn parse_let(&mut self, line: usize) -> Result<Stmt, NeonError> {
        self.eat(&Token::Let)?;
        let name = self.eat_ident()?;
        self.eat(&Token::Eq)?;

        // Lookahead: `ident[expr]` → LetRow
        if let Token::Ident(table) = self.peek().clone() {
            let saved_pos = self.pos;
            self.advance();
            if self.peek() == &Token::LBracket {
                self.advance();
                let key = self.parse_expr()?;
                self.eat(&Token::RBracket)?;

                let else_body = if self.peek() == &Token::Else {
                    self.advance();
                    Some(self.parse_block()?)
                } else {
                    None
                };

                return Ok(Stmt::LetRow { name, table, key: Box::new(key), else_body, line });
            }
            self.pos = saved_pos;
        }

        let value = self.parse_expr()?;
        Ok(Stmt::Let { name, value: Box::new(value), line })
    }

    fn parse_delete(&mut self, line: usize) -> Result<Stmt, NeonError> {
        self.eat(&Token::Delete)?;
        let table = self.eat_ident()?;
        self.eat(&Token::LBracket)?;
        let key = self.parse_expr()?;
        self.eat(&Token::RBracket)?;
        Ok(Stmt::DeleteRow { table, key: Box::new(key), line })
    }

    fn parse_if(&mut self, line: usize) -> Result<Stmt, NeonError> {
        self.eat(&Token::If)?;
        let condition = self.parse_expr()?;
        let then_body = self.parse_block()?;
        let else_body = if self.peek() == &Token::Else {
            self.advance();
            // `else if` sugar: wrap the nested if in a block
            if self.peek() == &Token::If {
                let nested_line = self.line();
                let nested = self.parse_if(nested_line)?;
                Some(vec![nested])
            } else {
                Some(self.parse_block()?)
            }
        } else {
            None
        };
        Ok(Stmt::If { condition: Box::new(condition), then_body, else_body, line })
    }

    fn parse_return(&mut self, line: usize) -> Result<Stmt, NeonError> {
        self.eat(&Token::Return)?;
        let value = self.parse_expr()?;
        Ok(Stmt::Return { value: Box::new(value), line })
    }

    fn parse_error_stmt(&mut self, line: usize) -> Result<Stmt, NeonError> {
        self.eat(&Token::Error)?;
        self.eat(&Token::LParen)?;
        let message = match self.peek().clone() {
            Token::StrLit(s) => { self.advance(); s }
            other => return Err(NeonError::new(line, format!("error() expects a string literal, found {:?}", other))),
        };
        self.eat(&Token::RParen)?;
        Ok(Stmt::Error { message, line })
    }

    /// `for key_var, val_var in table { ... }`  →  ForRow
    /// `for item_var in expr { ... }`           →  ForArray
    fn parse_for_stmt(&mut self, line: usize) -> Result<Stmt, NeonError> {
        self.eat(&Token::For)?;
        let first_var = self.eat_ident()?;

        if self.peek() == &Token::Comma {
            // ForRow: for key, val in table { }
            self.advance(); // consume `,`
            let val_var = self.eat_ident()?;
            self.eat(&Token::In)?;
            let table = self.eat_ident()?;
            let body = self.parse_block()?;
            Ok(Stmt::ForRow { key_var: first_var, val_var, table, body, line })
        } else {
            // ForArray: for item in expr { }
            self.eat(&Token::In)?;
            let array = self.parse_expr()?;
            let body = self.parse_block()?;
            Ok(Stmt::ForArray { item_var: first_var, array: Box::new(array), body, line })
        }
    }

    /// `while expr { ... }`
    fn parse_while_stmt(&mut self, line: usize) -> Result<Stmt, NeonError> {
        self.eat(&Token::While)?;
        let condition = self.parse_expr()?;
        let body = self.parse_block()?;
        Ok(Stmt::While { condition: Box::new(condition), body, line })
    }

    /// Statement starting with an identifier:
    ///   `ident(args)`                 → CallStmt
    ///   `ident = expr`                → Assign (plain variable reassignment)
    ///   `ident[key] = expr`           → AssignRow
    ///   `ident[key].field = expr`     → AssignField
    ///   `ident[key].field += expr`    → AssignFieldOp
    fn parse_ident_stmt(&mut self, name: String, line: usize) -> Result<Stmt, NeonError> {
        self.advance(); // consume the ident

        // Function-call statement: set_counter("kills", v)
        if self.peek() == &Token::LParen {
            self.advance(); // consume `(`
            let args = self.parse_arg_list()?;
            self.eat(&Token::RParen)?;
            return Ok(Stmt::CallStmt { name, args, line });
        }

        // Plain variable reassignment: `name = expr`
        if self.peek() == &Token::Eq {
            self.advance(); // consume `=`
            let value = self.parse_expr()?;
            return Ok(Stmt::Assign { name, value: Box::new(value), line });
        }

        // Array-index statements
        self.eat(&Token::LBracket)?;
        let key = self.parse_expr()?;
        self.eat(&Token::RBracket)?;

        if self.peek() == &Token::Dot {
            self.advance(); // consume `.`
            let field = self.eat_ident()?;

            // Compound assignment: +=, -=, *=, /=
            let compound_op = match self.peek() {
                Token::PlusEq  => Some(BinOp::Add),
                Token::MinusEq => Some(BinOp::Sub),
                Token::StarEq  => Some(BinOp::Mul),
                Token::SlashEq => Some(BinOp::Div),
                _ => None,
            };
            if let Some(op) = compound_op {
                self.advance(); // consume the compound-assignment token
                let value = self.parse_expr()?;
                return Ok(Stmt::AssignFieldOp { table: name, key: Box::new(key), field, op, value: Box::new(value), line });
            }

            // Plain assignment
            self.eat(&Token::Eq)?;
            let value = self.parse_expr()?;
            Ok(Stmt::AssignField { table: name, key: Box::new(key), field, value: Box::new(value), line })
        } else {
            self.eat(&Token::Eq)?;
            let value = self.parse_expr()?;
            Ok(Stmt::AssignRow { table: name, key: Box::new(key), value: Box::new(value), line })
        }
    }

    // ── Argument list (shared by FnCall expr and CallStmt) ────────────────────

    fn parse_arg_list(&mut self) -> Result<Vec<Expr>, NeonError> {
        let mut args = Vec::new();
        while self.peek() != &Token::RParen && !self.at_end() {
            args.push(self.parse_expr()?);
            if self.peek() == &Token::Comma { self.advance(); }
        }
        Ok(args)
    }

    // ── Expressions — Pratt-style precedence chain ────────────────────────────
    //
    // Precedence (low → high):
    //   or → and → bitor → bitxor → bitand → equality → comparison
    //   → shift → additive → multiplicative → unary → postfix → primary

    fn parse_expr(&mut self) -> Result<Expr, NeonError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::PipePipe {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinOp { left: Box::new(left), op: BinOp::Or, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_bitor()?;
        while self.peek() == &Token::AmpAmp {
            self.advance();
            let right = self.parse_bitor()?;
            left = Expr::BinOp { left: Box::new(left), op: BinOp::And, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_bitor(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_bitxor()?;
        while self.peek() == &Token::Pipe {
            self.advance();
            let right = self.parse_bitxor()?;
            left = Expr::BinOp { left: Box::new(left), op: BinOp::BitOr, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_bitxor(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_bitand()?;
        while self.peek() == &Token::Caret {
            self.advance();
            let right = self.parse_bitand()?;
            left = Expr::BinOp { left: Box::new(left), op: BinOp::BitXor, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_bitand(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_equality()?;
        while self.peek() == &Token::Amp {
            self.advance();
            let right = self.parse_equality()?;
            left = Expr::BinOp { left: Box::new(left), op: BinOp::BitAnd, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_equality(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_comparison()?;
        loop {
            let op = match self.peek() {
                Token::EqEq   => BinOp::Eq,
                Token::BangEq => BinOp::Ne,
                _ => break,
            };
            self.advance();
            let right = self.parse_comparison()?;
            left = Expr::BinOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_shift()?;
        loop {
            let op = match self.peek() {
                Token::Lt   => BinOp::Lt,
                Token::Gt   => BinOp::Gt,
                Token::LtEq => BinOp::Le,
                Token::GtEq => BinOp::Ge,
                _ => break,
            };
            self.advance();
            let right = self.parse_shift()?;
            left = Expr::BinOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_shift(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_additive()?;
        loop {
            let op = match self.peek() {
                Token::LtLt => BinOp::Shl,
                Token::GtGt => BinOp::Shr,
                _ => break,
            };
            self.advance();
            let right = self.parse_additive()?;
            left = Expr::BinOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, NeonError> {
        let mut left = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Token::Plus  => BinOp::Add,
                Token::Minus => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplicative()?;
            left = Expr::BinOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, NeonError> {
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
            left = Expr::BinOp { left: Box::new(left), op, right: Box::new(right) };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, NeonError> {
        if self.peek() == &Token::Bang {
            self.advance();
            let inner = self.parse_unary()?;
            return Ok(Expr::Not(Box::new(inner)));
        }
        self.parse_postfix()
    }

    fn parse_postfix(&mut self) -> Result<Expr, NeonError> {
        let mut expr = self.parse_primary()?;
        loop {
            if self.peek() == &Token::Dot {
                self.advance();
                let field = self.eat_ident()?;
                expr = Expr::FieldAccess { object: Box::new(expr), field };
            } else {
                break;
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr, NeonError> {
        let line = self.line();
        match self.peek().clone() {
            Token::IntLit(n)  => { self.advance(); Ok(Expr::Lit(Literal::Int(n))) }
            Token::FloatLit(f)=> { self.advance(); Ok(Expr::Lit(Literal::Float(f))) }
            Token::StrLit(s)  => { self.advance(); Ok(Expr::Lit(Literal::Str(s))) }
            Token::BoolLit(b) => { self.advance(); Ok(Expr::Lit(Literal::Bool(b))) }

            Token::LParen => {
                self.advance();
                let inner = self.parse_expr()?;
                self.eat(&Token::RParen)?;
                Ok(inner)
            }

            // Array literal: [expr, expr, ...]
            Token::LBracket => {
                self.advance();
                let mut elems = Vec::new();
                while self.peek() != &Token::RBracket && !self.at_end() {
                    elems.push(self.parse_expr()?);
                    if self.peek() == &Token::Comma { self.advance(); }
                }
                self.eat(&Token::RBracket)?;
                Ok(Expr::ArrayLit(elems))
            }

            // Row literal: { field: expr, ... }
            Token::LBrace => {
                self.advance();
                let mut fields = Vec::new();
                while self.peek() != &Token::RBrace && !self.at_end() {
                    let field_name = self.eat_ident()?;
                    self.eat(&Token::Colon)?;
                    let val = self.parse_expr()?;
                    fields.push((field_name, val));
                    if self.peek() == &Token::Comma { self.advance(); }
                }
                self.eat(&Token::RBrace)?;
                Ok(Expr::RowLiteral { fields })
            }

            // Ident — variable, built-in function call, or row read.
            Token::Ident(name) => {
                self.advance();

                // Special globals
                if name == "caller_id" || name == "caller_role" {
                    return Ok(Expr::Var(name));
                }

                // Function call expression: fn_name(args...)
                if self.peek() == &Token::LParen {
                    self.advance(); // consume `(`
                    let args = self.parse_arg_list()?;
                    self.eat(&Token::RParen)?;
                    return Ok(Expr::FnCall { name, args });
                }

                // Row read: table[key]
                if self.peek() == &Token::LBracket {
                    self.advance();
                    let key = self.parse_expr()?;
                    self.eat(&Token::RBracket)?;
                    return Ok(Expr::RowRead { table: name, key: Box::new(key) });
                }

                Ok(Expr::Var(name))
            }

            // Type keywords used as cast functions: int(x), float(x), str(x), bool(x)
            Token::TInt   if self.tokens.get(self.pos + 1).map(|s| &s.token) == Some(&Token::LParen) => {
                self.advance(); self.advance();
                let args = self.parse_arg_list()?;
                self.eat(&Token::RParen)?;
                Ok(Expr::FnCall { name: "int".to_owned(), args })
            }
            Token::TFloat if self.tokens.get(self.pos + 1).map(|s| &s.token) == Some(&Token::LParen) => {
                self.advance(); self.advance();
                let args = self.parse_arg_list()?;
                self.eat(&Token::RParen)?;
                Ok(Expr::FnCall { name: "float".to_owned(), args })
            }
            Token::TStr   if self.tokens.get(self.pos + 1).map(|s| &s.token) == Some(&Token::LParen) => {
                self.advance(); self.advance();
                let args = self.parse_arg_list()?;
                self.eat(&Token::RParen)?;
                Ok(Expr::FnCall { name: "str".to_owned(), args })
            }
            Token::TBool  if self.tokens.get(self.pos + 1).map(|s| &s.token) == Some(&Token::LParen) => {
                self.advance(); self.advance();
                let args = self.parse_arg_list()?;
                self.eat(&Token::RParen)?;
                Ok(Expr::FnCall { name: "bool".to_owned(), args })
            }

            other => Err(NeonError::new(line, format!("unexpected expression token: {:?}", other))),
        }
    }
}

pub fn parse(tokens: Vec<Spanned>) -> Result<Program, NeonError> {
    let mut p = Parser::new(tokens);
    p.parse_program()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::lexer::tokenize;

    fn parse_src(src: &str) -> Program {
        let tokens = tokenize(src).expect("lex failed");
        parse(tokens).expect("parse failed")
    }

    #[test]
    fn parse_empty_table() {
        let p = parse_src("table players {}");
        assert_eq!(p.tables.len(), 1);
        assert_eq!(p.tables[0].name, "players");
        assert!(p.tables[0].fields.is_empty());
    }

    #[test]
    fn parse_table_with_fields() {
        let p = parse_src("table players { hp: int = 100, alive: bool = true }");
        assert_eq!(p.tables[0].fields.len(), 2);
        assert_eq!(p.tables[0].fields[0].name, "hp");
        assert_eq!(p.tables[0].fields[0].ty, Type::Int);
        assert!(matches!(p.tables[0].fields[0].default, Some(Literal::Int(100))));
    }

    #[test]
    fn parse_empty_reducer() {
        let p = parse_src("reducer reset() {}");
        assert_eq!(p.reducers.len(), 1);
        assert_eq!(p.reducers[0].name, "reset");
        assert!(p.reducers[0].params.is_empty());
        assert!(p.reducers[0].body.is_empty());
    }

    #[test]
    fn parse_reducer_with_params() {
        let p = parse_src("reducer spawn(player_id: str, x: float) {}");
        assert_eq!(p.reducers[0].params.len(), 2);
        assert_eq!(p.reducers[0].params[0].name, "player_id");
        assert_eq!(p.reducers[0].params[0].ty, Type::Str);
        assert_eq!(p.reducers[0].params[1].ty, Type::Float);
    }

    #[test]
    fn parse_let_row_with_else() {
        let src = r#"
            reducer move(id: str, x: float) {
                let p = players[id] else { error("not found") }
            }
        "#;
        let p = parse_src(src);
        let stmt = &p.reducers[0].body[0];
        assert!(matches!(stmt, Stmt::LetRow { else_body: Some(_), .. }));
    }

    #[test]
    fn parse_assign_row() {
        let src = r#"reducer spawn(id: str) { players[id] = { hp: 100, alive: true } }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0], Stmt::AssignRow { table, .. } if table == "players"));
    }

    #[test]
    fn parse_assign_field() {
        let src = r#"reducer mv(id: str, x: float) { players[id].x = x }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0], Stmt::AssignField { field, .. } if field == "x"));
    }

    #[test]
    fn parse_assign_field_compound() {
        let src = r#"reducer heal(id: str, amt: int) { players[id].hp += amt }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0], Stmt::AssignFieldOp { op: BinOp::Add, field, .. } if field == "hp"));
    }

    #[test]
    fn parse_delete() {
        let src = r#"reducer despawn(id: str) { delete players[id] }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0], Stmt::DeleteRow { table, .. } if table == "players"));
    }

    #[test]
    fn parse_if_else() {
        let src = r#"reducer check(hp: int) { if hp <= 0 { error("dead") } else { error("alive") } }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0], Stmt::If { else_body: Some(_), .. }));
    }

    #[test]
    fn parse_else_if() {
        let src = r#"reducer tier(rank: int) {
            if rank == 1 { return { tier: "gold" } }
            else if rank == 2 { return { tier: "silver" } }
            else { return { tier: "bronze" } }
        }"#;
        let p = parse_src(src);
        if let Stmt::If { else_body: Some(else_stmts), .. } = &p.reducers[0].body[0] {
            assert!(matches!(&else_stmts[0], Stmt::If { .. }));
        } else {
            panic!("expected else-if chain");
        }
    }

    #[test]
    fn parse_return() {
        let src = r#"reducer get(id: str) { return { ok: true } }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0], Stmt::Return { .. }));
    }

    #[test]
    fn parse_binop_precedence() {
        let src = r#"reducer x() { let r = 2 + 3 * 4 }"#;
        let p = parse_src(src);
        if let Stmt::Let { value, .. } = &p.reducers[0].body[0] {
            if let Expr::BinOp { op: BinOp::Add, right, .. } = value.as_ref() {
                assert!(matches!(right.as_ref(), Expr::BinOp { op: BinOp::Mul, .. }));
            } else {
                panic!("expected Add at top level");
            }
        }
    }

    #[test]
    fn parse_for_row_loop() {
        let src = r#"reducer cleanup() { for id, p in players { delete players[id] } }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0],
            Stmt::ForRow { key_var, val_var, table, .. }
            if key_var == "id" && val_var == "p" && table == "players"
        ));
    }

    #[test]
    fn parse_for_array_loop() {
        let src = r#"reducer x(id: str) { let p = players[id] else { error("x") }
            for item in p.inventory { let n = item } }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[1], Stmt::ForArray { item_var, .. } if item_var == "item"));
    }

    #[test]
    fn parse_while_loop() {
        let src = r#"reducer x(n: int) { while n > 0 { let n = n - 1 } }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0], Stmt::While { .. }));
    }

    #[test]
    fn parse_break_continue() {
        let src = r#"reducer x() { while true { break } }"#;
        let p = parse_src(src);
        if let Stmt::While { body, .. } = &p.reducers[0].body[0] {
            assert!(matches!(&body[0], Stmt::Break { .. }));
        }
    }

    #[test]
    fn parse_builtin_fn_call_expr() {
        let src = r#"reducer x() { let t = timestamp() }"#;
        let p = parse_src(src);
        if let Stmt::Let { value, .. } = &p.reducers[0].body[0] {
            assert!(matches!(value.as_ref(), Expr::FnCall { name, .. } if name == "timestamp"));
        } else {
            panic!("expected Let");
        }
    }

    #[test]
    fn parse_call_stmt() {
        let src = r#"reducer x() { set_counter("kills", 1) }"#;
        let p = parse_src(src);
        assert!(matches!(&p.reducers[0].body[0],
            Stmt::CallStmt { name, .. } if name == "set_counter"
        ));
    }

    #[test]
    fn parse_fn_call_with_args() {
        let src = r#"reducer x(a: int, b: int) { let m = min(a, b) }"#;
        let p = parse_src(src);
        if let Stmt::Let { value, .. } = &p.reducers[0].body[0] {
            if let Expr::FnCall { name, args } = value.as_ref() {
                assert_eq!(name, "min");
                assert_eq!(args.len(), 2);
            } else { panic!("expected FnCall"); }
        }
    }

    #[test]
    fn parse_array_literal() {
        let src = r#"reducer x() { let arr = [1, 2, 3] }"#;
        let p = parse_src(src);
        if let Stmt::Let { value, .. } = &p.reducers[0].body[0] {
            assert!(matches!(value.as_ref(), Expr::ArrayLit(v) if v.len() == 3));
        }
    }

    #[test]
    fn parse_modulo_op() {
        let src = r#"reducer x(n: int) { let r = n % 2 }"#;
        let p = parse_src(src);
        if let Stmt::Let { value, .. } = &p.reducers[0].body[0] {
            assert!(matches!(value.as_ref(), Expr::BinOp { op: BinOp::Mod, .. }));
        }
    }

    #[test]
    fn parse_bitwise_ops() {
        let src = r#"reducer x(a: int, b: int) { let r = a & b }"#;
        let p = parse_src(src);
        if let Stmt::Let { value, .. } = &p.reducers[0].body[0] {
            assert!(matches!(value.as_ref(), Expr::BinOp { op: BinOp::BitAnd, .. }));
        }
    }

    #[test]
    fn parse_shift_ops() {
        let src = r#"reducer x(a: int) { let r = a << 2 }"#;
        let p = parse_src(src);
        if let Stmt::Let { value, .. } = &p.reducers[0].body[0] {
            assert!(matches!(value.as_ref(), Expr::BinOp { op: BinOp::Shl, .. }));
        }
    }
}
