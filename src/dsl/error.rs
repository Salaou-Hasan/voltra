// ============================================================================
// .vol DSL — Error type
// ============================================================================

use std::fmt;

#[derive(Debug, Clone)]
pub struct NeonError {
    pub line:    usize,
    pub message: String,
}

impl NeonError {
    pub fn new(line: usize, message: impl Into<String>) -> Self {
        NeonError { line, message: message.into() }
    }
}

impl fmt::Display for NeonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

/// Format a list of errors for display to the user.
pub fn format_errors(filename: &str, errors: &[NeonError]) -> String {
    errors.iter()
        .map(|e| format!("{}:{}: error: {}", filename, e.line, e.message))
        .collect::<Vec<_>>()
        .join("\n")
}
