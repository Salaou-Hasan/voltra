// ============================================================================
// Voltra binary application modules
//
// This module tree holds the `voltra` binary's command implementations,
// scaffolding, build pipeline, admin HTTP server, and server bootstrap.
// It lives under `src/app/` (not at the crate root) so binary module names
// cannot clash with library module names declared in `src/lib.rs`.
//
// `src/main.rs` declares `mod app;` and dispatches into these submodules.
// ============================================================================

use std::path::Path;
use std::sync::Arc;

use voltra::{error::Result, table::TableStore, wal::WalReader};

pub mod bench;
pub mod bootstrap;
pub mod build;
pub mod cli;
pub mod scaffold;
pub mod templates;

pub(crate) fn current_timestamp_nanos() -> u64 {
    voltra::now_nanos()
}

pub(crate) fn recover_from_wal(
    wal_path: &Path,
    tables: &Arc<TableStore>,
    min_seq: u64,
) -> Result<(usize, u64)> {
    let mut reader = WalReader::open(wal_path)?;
    let entries = reader.read_all_entries()?;
    let mut replayed = 0usize;
    let mut max_seq = min_seq;
    for entry in &entries {
        max_seq = max_seq.max(entry.header.sequence_number);
        if entry.header.sequence_number <= min_seq {
            continue;
        }
        if !entry.verify_checksum() {
            log::warn!(
                "WAL entry {} bad checksum, skipping",
                entry.header.sequence_number
            );
            continue;
        }
        for delta in &entry.payload.deltas {
            tables.apply_delta(delta)?;
        }
        replayed += 1;
    }
    Ok((replayed, max_seq))
}
