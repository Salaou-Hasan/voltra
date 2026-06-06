// ============================================================================
// NeonDB SQL AST
//
// Abstract syntax tree produced by the parser and consumed by the executor.
// Supports full SELECT (projections, aliases, JOINs, WHERE, GROUP BY, HAVING,
// ORDER BY, LIMIT, OFFSET, DISTINCT, subqueries), plus INSERT, UPDATE, DELETE.
// ============================================================================

use serde_json::Value;

// ── Expressions ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Literal value
    Literal(Value),

    /// Column reference, optionally qualified: `table.column`
    Column { table: Option<String>, name: String },

    /// Wildcard `*` or `table.*`
    Wildcard { table: Option<String> },

    /// Binary operation: `left op right`
    BinaryOp {
        left:  Box<Expr>,
        op:    BinOp,
        right: Box<Expr>,
    },

    /// Unary operation: `NOT expr`, `- expr`, `+ expr`
    UnaryOp { op: UnaryOp, expr: Box<Expr> },

    /// `expr IS NULL` / `expr IS NOT NULL`
    IsNull { expr: Box<Expr>, negated: bool },

    /// `expr IN (v1, v2, …)` / `expr NOT IN (…)`
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },

    /// `expr IN (subquery)` / `expr NOT IN (subquery)`
    InSubquery { expr: Box<Expr>, query: Box<SelectStmt>, negated: bool },

    /// `expr BETWEEN low AND high` / `expr NOT BETWEEN …`
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },

    /// `expr LIKE pattern` / `expr NOT LIKE pattern`
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool },

    /// Aggregate function: COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x)
    Aggregate { func: AggFunc, distinct: bool, arg: Option<Box<Expr>> },

    /// Scalar function call: UPPER(x), LOWER(x), LENGTH(x), …
    Function { name: String, args: Vec<Expr> },

    /// `CASE WHEN cond THEN val … ELSE default END`
    Case {
        operand:  Option<Box<Expr>>,
        branches: Vec<(Expr, Expr)>,
        else_:    Option<Box<Expr>>,
    },

    /// Scalar subquery `(SELECT …)`
    Subquery(Box<SelectStmt>),

    /// `EXISTS (SELECT …)`
    Exists { query: Box<SelectStmt>, negated: bool },

    /// Expression alias: `expr AS alias`
    Alias { expr: Box<Expr>, alias: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    Eq, Ne, Lt, Le, Gt, Ge,
    And, Or,
    Add, Sub, Mul, Div, Mod,
    Concat, // ||
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
    Pos,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count, // COUNT(*) when arg is None, COUNT(expr) when arg is Some
    Sum,
    Avg,
    Min,
    Max,
}

// ── FROM clause ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    /// A simple table name, optionally aliased
    Named { name: String, alias: Option<String> },
    /// A derived table (subquery)
    Subquery { query: Box<SelectStmt>, alias: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub kind:  JoinKind,
    pub table: TableRef,
    /// ON condition (None for CROSS JOIN)
    pub on:    Option<Expr>,
}

// ── ORDER BY ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr:  Expr,
    pub asc:   bool,
    /// NULLS FIRST / NULLS LAST (default: NULLS LAST for ASC, NULLS FIRST for DESC)
    pub nulls_first: Option<bool>,
}

// ── SELECT statement ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct:   bool,
    /// Projection list — each element is an Expr (may carry Expr::Alias)
    pub columns:    Vec<Expr>,
    /// FROM clause (may be empty for valueless SELECTs like `SELECT 1+1`)
    pub from:       Vec<TableRef>,
    /// JOIN clauses
    pub joins:      Vec<Join>,
    /// WHERE clause
    pub where_:     Option<Expr>,
    /// GROUP BY expressions
    pub group_by:   Vec<Expr>,
    /// HAVING clause (applied after grouping)
    pub having:     Option<Expr>,
    /// ORDER BY
    pub order_by:   Vec<OrderByItem>,
    /// LIMIT N
    pub limit:      Option<usize>,
    /// OFFSET N
    pub offset:     Option<usize>,
    /// UNION / UNION ALL with another query
    pub union:      Option<(bool /*all*/, Box<SelectStmt>)>,
}

// ── INSERT statement ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStmt {
    pub table:   String,
    pub columns: Vec<String>,
    pub values:  Vec<Vec<Expr>>,
}

// ── UPDATE statement ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStmt {
    pub table:   String,
    pub alias:   Option<String>,
    pub sets:    Vec<(String, Expr)>,
    pub where_:  Option<Expr>,
}

// ── DELETE statement ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStmt {
    pub table:  String,
    pub where_: Option<Expr>,
}

// ── Top-level statement ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
}
