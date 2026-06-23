// ============================================================================
// .vol DSL — Abstract Syntax Tree
// ============================================================================

/// A complete .vol source file.
#[derive(Debug, Clone)]
pub struct Program {
    pub tables:   Vec<TableDecl>,
    pub reducers: Vec<ReducerDecl>,
}

/// `table players { hp: int = 100, ... }`
#[derive(Debug, Clone)]
pub struct TableDecl {
    pub name:   String,
    pub fields: Vec<FieldDef>,
    pub line:   usize,
}

/// One field inside a table block.
#[derive(Debug, Clone)]
pub struct FieldDef {
    pub name:    String,
    pub ty:      Type,
    pub default: Option<Literal>,
    pub line:    usize,
}

/// `reducer spawn(player_id: str, x: float, y: float) { ... }`
#[derive(Debug, Clone)]
pub struct ReducerDecl {
    pub name:   String,
    pub params: Vec<Param>,
    pub body:   Vec<Stmt>,
    pub line:   usize,
}

/// One parameter of a reducer.
#[derive(Debug, Clone)]
pub struct Param {
    pub name: String,
    pub ty:   Type,
}

/// Supported value types in the DSL.
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Str,
    Int,
    Float,
    Bool,
}

impl Type {
    pub fn to_rust(&self) -> &'static str {
        match self {
            Type::Str   => "String",
            Type::Int   => "i64",
            Type::Float => "f64",
            Type::Bool  => "bool",
        }
    }

    /// The default zero-value for this type, as a Rust literal.
    pub fn zero_rust(&self) -> &'static str {
        match self {
            Type::Str   => "String::new()",
            Type::Int   => "0i64",
            Type::Float => "0.0f64",
            Type::Bool  => "false",
        }
    }

    /// The serde_json extractor method for this type.
    pub fn json_getter(&self) -> &'static str {
        match self {
            Type::Str   => "as_str",
            Type::Int   => "as_i64",
            Type::Float => "as_f64",
            Type::Bool  => "as_bool",
        }
    }
}

/// Compile-time literal values.
#[derive(Debug, Clone)]
pub enum Literal {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
}

// ── Statements ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Stmt {
    /// `let name = table[key] else { body }`
    LetRow {
        name:      String,
        table:     String,
        key:       Box<Expr>,
        else_body: Option<Vec<Stmt>>,
        line:      usize,
    },
    /// `let name = expr`
    Let {
        name:  String,
        value: Box<Expr>,
        line:  usize,
    },
    /// `table[key] = { field: expr, ... }`
    AssignRow {
        table: String,
        key:   Box<Expr>,
        value: Box<Expr>,
        line:  usize,
    },
    /// `table[key].field = expr`
    AssignField {
        table: String,
        key:   Box<Expr>,
        field: String,
        value: Box<Expr>,
        line:  usize,
    },
    /// `delete table[key]`
    DeleteRow {
        table: String,
        key:   Box<Expr>,
        line:  usize,
    },
    /// `if expr { ... } else if expr { ... } else { ... }`
    If {
        condition: Box<Expr>,
        then_body: Vec<Stmt>,
        else_body: Option<Vec<Stmt>>,
        line:      usize,
    },
    /// `return expr`
    Return {
        value: Box<Expr>,
        line:  usize,
    },
    /// `error("message")`
    Error {
        message: String,
        line:    usize,
    },
    /// `for key, row in table { ... }`
    ForRow {
        key_var: String,
        val_var: String,
        table:   String,
        body:    Vec<Stmt>,
        line:    usize,
    },
    /// `for item in expr { ... }`  — iterates over a JSON array
    ForArray {
        item_var: String,
        array:    Box<Expr>,
        body:     Vec<Stmt>,
        line:     usize,
    },
    /// `while expr { ... }`
    While {
        condition: Box<Expr>,
        body:      Vec<Stmt>,
        line:      usize,
    },
    /// `break`
    Break { line: usize },
    /// `continue`
    Continue { line: usize },
    /// `table[key].field += expr`  (op = Add | Sub | Mul | Div | Mod)
    AssignFieldOp {
        table: String,
        key:   Box<Expr>,
        field: String,
        op:    BinOp,
        value: Box<Expr>,
        line:  usize,
    },
    /// `name = expr` — reassign an existing local variable
    Assign {
        name:  String,
        value: Box<Expr>,
        line:  usize,
    },
    /// `set_counter("name", val)` and other void built-in calls
    CallStmt {
        name: String,
        args: Vec<Expr>,
        line: usize,
    },
}

// ── Expressions ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expr {
    Lit(Literal),
    /// A variable name.
    Var(String),
    /// `table[key]` — inline row read (in expression context).
    RowRead { table: String, key: Box<Expr> },
    /// `obj.field` — field access on a bound row variable.
    FieldAccess { object: Box<Expr>, field: String },
    /// `{ field: expr, ... }` — row literal / object.
    RowLiteral { fields: Vec<(String, Expr)> },
    /// `[expr, expr, ...]` — array literal.
    ArrayLit(Vec<Expr>),
    /// `left op right`
    BinOp { left: Box<Expr>, op: BinOp, right: Box<Expr> },
    /// `!expr`
    Not(Box<Expr>),
    /// `fn_name(arg, ...)` — built-in function call
    FnCall {
        name: String,
        args: Vec<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinOp {
    Add, Sub, Mul, Div, Mod,
    Eq, Ne, Lt, Gt, Le, Ge,
    And, Or,
    BitAnd, BitOr, BitXor, Shl, Shr,
}

impl BinOp {
    pub fn to_rust(&self) -> &'static str {
        match self {
            BinOp::Add    => "+",   BinOp::Sub => "-",
            BinOp::Mul    => "*",   BinOp::Div => "/",   BinOp::Mod => "%",
            BinOp::Eq     => "==",  BinOp::Ne  => "!=",
            BinOp::Lt     => "<",   BinOp::Gt  => ">",
            BinOp::Le     => "<=",  BinOp::Ge  => ">=",
            BinOp::And    => "&&",  BinOp::Or  => "||",
            BinOp::BitAnd => "&",   BinOp::BitOr => "|", BinOp::BitXor => "^",
            BinOp::Shl    => "<<",  BinOp::Shr => ">>",
        }
    }
}
