use crate::error::Result;
use crate::reducer::context::ReducerContext;

/// Trait for any reducer backend implementation.
pub trait ReducerBackend: Send + Sync {
    fn execute(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>>;
}
