// ============================================================================
// VQL — Error type
// ============================================================================

use std::fmt;

#[derive(Debug, Clone)]
pub struct VqlError {
    pub line:    usize,
    pub message: String,
}

impl VqlError {
    pub fn new(line: usize, message: impl Into<String>) -> Self {
        VqlError { line, message: message.into() }
    }
}

impl fmt::Display for VqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

impl std::error::Error for VqlError {}
