// ============================================================================
// VQL — Voltra Query Language — Abstract Syntax Tree
//
// VQL is a declarative query language that unifies:
//   - SQL-like queries (SELECT, INSERT, UPDATE, DELETE)
//   - Reactive subscriptions (SUBSCRIBE)
//   - Game primitives (LEADERBOARD, UPSERT)
//   - Transactions (BEGIN, COMMIT, ROLLBACK)
//
// Borrowed from PostgreSQL: JOINs, GROUP BY, HAVING, subqueries, CASE
// Borrowed from Redis: sorted sets (LEADERBOARD), TTL, atomic counters
// Novel to VQL: SUBSCRIBE (reactive push), UPSERT, game-first syntax
// ============================================================================

use serde_json::Value;

// ── Expressions ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Literal value
    Literal(Value),

    /// Column reference: `table.column` or `column`
    Column { table: Option<String>, name: String },

    /// Wildcard: `*` or `table.*`
    Wildcard { table: Option<String> },

    /// Binary operation: `left op right`
    BinaryOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },

    /// Unary operation: `NOT expr`, `- expr`
    UnaryOp { op: UnaryOp, expr: Box<Expr> },

    /// `expr IS [NOT] NULL`
    IsNull { expr: Box<Expr>, negated: bool },

    /// `expr IN (v1, v2, ...)`
    InList { expr: Box<Expr>, list: Vec<Expr>, negated: bool },

    /// `expr IN (SELECT ...)`
    InSubquery { expr: Box<Expr>, query: Box<SelectStmt>, negated: bool },

    /// `expr BETWEEN low AND high`
    Between { expr: Box<Expr>, low: Box<Expr>, high: Box<Expr>, negated: bool },

    /// `expr LIKE pattern`
    Like { expr: Box<Expr>, pattern: Box<Expr>, negated: bool },

    /// `expr ILIKE pattern` (case-insensitive like)
    ILike { expr: Box<Expr>, pattern: Box<Expr>, negated: bool },

    /// Aggregate: COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x)
    Aggregate { func: AggFunc, distinct: bool, arg: Option<Box<Expr>> },

    /// Scalar function: UPPER(x), LENGTH(x), NOW(), ...
    Function { name: String, args: Vec<Expr> },

    /// `CASE WHEN cond THEN val ... ELSE default END`
    Case { operand: Option<Box<Expr>>, branches: Vec<(Expr, Expr)>, else_: Option<Box<Expr>> },

    /// `(SELECT ...)` scalar subquery
    Subquery(Box<SelectStmt>),

    /// `EXISTS (SELECT ...)`
    Exists { query: Box<SelectStmt>, negated: bool },

    /// `expr AS alias`
    Alias { expr: Box<Expr>, alias: String },

    /// Row access: `table[key]`
    RowAccess { table: String, key: Box<Expr> },

    /// Field access: `expr.field`
    FieldAccess { object: Box<Expr>, field: String },

    /// Row literal: `{ field: value, ... }`
    RowLiteral { fields: Vec<(String, Expr)> },
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
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

// ── FROM / JOIN ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum TableRef {
    Named { name: String, alias: Option<String> },
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
    pub on:    Option<Expr>,
}

// ── ORDER BY ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub expr:         Expr,
    pub asc:          bool,
    pub nulls_first:  Option<bool>,
}

// ── TTL (Redis-inspired) ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct TtlClause {
    pub seconds: Expr,
}

// ── RETURNING ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct ReturningClause {
    pub columns: Vec<Expr>,
}

// ── UPSERT (ON CONFLICT) ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct UpsertClause {
    pub conflict_columns: Vec<String>,
    pub do_update:        Vec<(String, Expr)>,
}

// ── Statements ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Statement {
    /// `SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... ORDER BY ... LIMIT ...`
    Select(SelectStmt),

    /// `INSERT INTO table [(cols)] VALUES (...) [ON CONFLICT ...] [TTL n] [RETURNING ...]`
    Insert(InsertStmt),

    /// `UPDATE table SET col = expr, ... WHERE ... [RETURNING ...]`
    Update(UpdateStmt),

    /// `DELETE FROM table WHERE ... [RETURNING ...]`
    Delete(DeleteStmt),

    /// `SUBSCRIBE table [WHERE ...] [ORDER BY ...] [LIMIT n]` — reactive push
    Subscribe(SubscribeStmt),

    /// `LEADERBOARD table BY field [ASC|DESC] [LIMIT n]` — game primitive
    Leaderboard(LeaderboardStmt),

    /// `UPSERT table[key] SET field = expr, ...` — atomic upsert
    Upsert(UpsertStmt),

    /// `BEGIN` — start transaction
    Begin { line: usize },

    /// `COMMIT` — commit transaction
    Commit { line: usize },

    /// `ROLLBACK` — rollback transaction
    Rollback { line: usize },
}

// ── SELECT ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub distinct:   bool,
    pub columns:    Vec<Expr>,
    pub from:       Vec<TableRef>,
    pub joins:      Vec<Join>,
    pub where_:     Option<Expr>,
    pub group_by:   Vec<Expr>,
    pub having:     Option<Expr>,
    pub order_by:   Vec<OrderByItem>,
    pub limit:      Option<usize>,
    pub offset:     Option<usize>,
    pub union:      Option<(bool, Box<SelectStmt>)>,
}

// ── INSERT ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table:    String,
    pub columns:  Vec<String>,
    pub values:   Vec<Vec<Expr>>,
    pub upsert:   Option<UpsertClause>,
    pub ttl:      Option<TtlClause>,
    pub returning: Option<ReturningClause>,
}

// ── UPDATE ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table:     String,
    pub alias:     Option<String>,
    pub sets:      Vec<(String, Expr)>,
    pub where_:    Option<Expr>,
    pub returning: Option<ReturningClause>,
}

// ── DELETE ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table:     String,
    pub where_:    Option<Expr>,
    pub returning: Option<ReturningClause>,
}

// ── SUBSCRIBE (reactive push) ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SubscribeStmt {
    pub table:    String,
    pub alias:    Option<String>,
    pub where_:   Option<Expr>,
    pub order_by: Vec<OrderByItem>,
    pub limit:    Option<usize>,
}

// ── LEADERBOARD (game primitive) ──────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LeaderboardStmt {
    pub table:  String,
    pub by:     String,
    pub asc:    bool,
    pub limit:  Option<usize>,
    pub where_: Option<Expr>,
}

// ── UPSERT (game primitive) ──────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UpsertStmt {
    pub table: String,
    pub key:   Box<Expr>,
    pub sets:  Vec<(String, Expr)>,
    pub ttl:   Option<TtlClause>,
}

// ── Program ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Program {
    pub statements: Vec<Statement>,
}
