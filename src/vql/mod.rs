// ============================================================================
// VQL — Voltra Query Language — Public API
//
// A declarative query language that unifies SQL, reactive subscriptions,
// and game-specific primitives into a single syntax.
//
// Borrowed from PostgreSQL: JOINs, GROUP BY, HAVING, subqueries, CASE, RETURNING
// Borrowed from Redis: sorted sets (LEADERBOARD), TTL, atomic counters
// Novel to VQL: SUBSCRIBE (reactive push), UPSERT, game-first syntax
// ============================================================================

pub mod ast;
pub mod error;
pub mod executor;
pub mod lexer;
pub mod parser;

use error::VqlError;

/// Parse a complete VQL source string into a program AST.
pub fn parse(source: &str) -> Result<ast::Program, Vec<VqlError>> {
    let tokens = lexer::tokenize(source).map_err(|e| vec![e])?;
    parser::parse(tokens).map_err(|e| vec![e])
}

/// Parse a single VQL statement.
pub fn parse_statement(source: &str) -> Result<ast::Statement, VqlError> {
    let tokens = lexer::tokenize(source)?;
    parser::parse_statement(tokens)
}
