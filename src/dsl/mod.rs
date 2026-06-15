// ============================================================================
// .neon DSL — public API
// ============================================================================

pub mod ast;
pub mod codegen;
pub mod error;
pub mod lexer;
pub mod parser;

use error::NeonError;

/// Compile a complete `.neon` source file to a `reducers.rs` string.
///
/// Returns the generated Rust source on success, or a list of errors
/// (with line numbers) on failure.
pub fn compile(source: &str, _filename: &str) -> Result<String, Vec<NeonError>> {
    let tokens = lexer::tokenize(source).map_err(|e| vec![e])?;
    let program = parser::parse(tokens).map_err(|e| vec![e])?;
    codegen::generate(&program).map_err(|e| vec![e])
}
