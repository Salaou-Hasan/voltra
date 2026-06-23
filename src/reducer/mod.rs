pub mod backend;
pub mod context;
pub mod native;
pub mod registry;
pub mod v8;
pub mod wasm;

pub use context::{increment_reducer, IncrementResult, ReducerContext};
pub use registry::{ReducerRegistry, ReducerRuntime};

// ─────────────────────────────────────────────────────────────────────────────
// Process-wide reducer sandbox limits.
//
// Mirrors the `table::set_max_blob_size` pattern (Session 39).  Limits are
// configured once at startup via `Config::apply_global_limits` and read by
// every reducer backend before instantiation/execution.  Using atomics avoids
// threading another parameter through `ReducerRegistry::new`.
// ─────────────────────────────────────────────────────────────────────────────

use std::sync::atomic::{AtomicUsize, Ordering};

/// Default maximum linear memory a single WASM reducer may grow to (64 MiB).
const DEFAULT_REDUCER_MAX_MEMORY_BYTES: usize = 64 * 1024 * 1024;
/// Default maximum size of args bytes IN and result bytes OUT (1 MiB).
const DEFAULT_REDUCER_MAX_IO_BYTES: usize = 1024 * 1024;

static REDUCER_MAX_MEMORY_BYTES: AtomicUsize =
    AtomicUsize::new(DEFAULT_REDUCER_MAX_MEMORY_BYTES);
static REDUCER_MAX_IO_BYTES: AtomicUsize =
    AtomicUsize::new(DEFAULT_REDUCER_MAX_IO_BYTES);

/// Set the process-wide WASM linear-memory cap (in bytes) for reducer
/// instances.  Typically called once from `main` via
/// `Config::apply_global_limits`.
pub fn set_max_memory_bytes(bytes: usize) {
    REDUCER_MAX_MEMORY_BYTES.store(bytes.max(64 * 1024), Ordering::Relaxed);
}

/// Set the process-wide cap on reducer args/result byte size (applies to
/// every backend).
pub fn set_max_io_bytes(bytes: usize) {
    REDUCER_MAX_IO_BYTES.store(bytes.max(4 * 1024), Ordering::Relaxed);
}

/// Current WASM linear-memory cap, in bytes.
pub fn max_memory_bytes() -> usize {
    REDUCER_MAX_MEMORY_BYTES.load(Ordering::Relaxed)
}

/// Current args/result byte cap, in bytes.
pub fn max_io_bytes() -> usize {
    REDUCER_MAX_IO_BYTES.load(Ordering::Relaxed)
}

/// Test-only mutex used to serialize the handful of unit tests that mutate
/// the process-wide reducer sandbox atomics (`set_max_memory_bytes`,
/// `set_max_io_bytes`).  Cargo runs tests in parallel within a single test
/// binary; without this lock, two tests racing on the atomics produce
/// flaky failures.  Real production code never touches this.
#[cfg(test)]
pub(crate) static SANDBOX_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
