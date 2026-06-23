use crate::error::Result;
use crate::reducer::backend::ReducerBackend;
use crate::reducer::context::ReducerContext;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct IncrementArgs {
    name: String,
    delta: i32,
}

pub struct NativeReducerBackend {
    reducer: fn(&mut ReducerContext, &[u8]) -> Result<Vec<u8>>,
}

impl NativeReducerBackend {
    pub fn new(reducer: fn(&mut ReducerContext, &[u8]) -> Result<Vec<u8>>) -> Self {
        NativeReducerBackend { reducer }
    }

    /// Stage the increment and return provisional result bytes.
    ///
    /// The actual commit (locked re-read-and-add) happens in the worker loop
    /// via ctx.commit().  The provisional new_value equals current+delta as
    /// seen from this context's pending_deltas, which is correct for a
    /// single-context call.  For concurrent calls, the committed value may
    /// differ from the provisional but the WAL and subscription fan-out always
    /// use the committed delta returned by apply_delta_batch.
    pub fn increment_reducer(ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        let parsed: IncrementArgs = rmp_serde::from_slice(args)?;
        let (result, _) =
            crate::reducer::context::increment_reducer(ctx, parsed.name, parsed.delta)?;
        Ok(rmp_serde::to_vec(&result)?)
    }
}

impl ReducerBackend for NativeReducerBackend {
    fn execute(&self, ctx: &mut ReducerContext, args: &[u8]) -> Result<Vec<u8>> {
        (self.reducer)(ctx, args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reducer::context::IncrementResult;
    use crate::table::TableStore;
    use std::sync::Arc;

    #[test]
    fn test_native_increment_backend() {
        let tables = Arc::new(TableStore::new());
        let mut ctx = ReducerContext::new(tables.clone(), 1234);
        let args = rmp_serde::to_vec(&IncrementArgs {
            name: "hello".to_string(),
            delta: 7,
        })
        .unwrap();

        let backend = NativeReducerBackend::new(NativeReducerBackend::increment_reducer);
        let bytes = backend.execute(&mut ctx, &args).unwrap();
        let result: IncrementResult = rmp_serde::from_slice(&bytes).unwrap();

        assert_eq!(result.new_value, 7);
        // commit() in the worker loop — the delta is applied and we get 1 committed delta.
        assert_eq!(ctx.commit().unwrap().len(), 1);
        assert_eq!(tables.get_counter("hello").unwrap().unwrap().value, 7);
    }
}
