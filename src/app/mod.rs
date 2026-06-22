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

pub mod admin;
pub mod bench;
pub mod bootstrap;
pub mod build;
pub mod cli;
pub mod scaffold;
pub mod templates;

pub(crate) fn current_timestamp_nanos() -> u64 {
    voltra::now_nanos()
}

/// Best-effort memory usage query (WorkingSetSize on Windows, /proc/self/statm on Linux).
/// Returns 0 if the platform does not support the query or if parsing fails.
pub(crate) fn get_memory_usage_bytes() -> u64 {
    #[cfg(target_os = "windows")]
    {
        // Use GetProcessMemoryInfo via psapi — no child process, no wmic (deprecated Win11).
        use std::mem;
        #[allow(non_camel_case_types)]
        type HANDLE = *mut std::ffi::c_void;
        #[allow(non_camel_case_types)]
        type DWORD = u32;
        #[allow(non_camel_case_types)]
        type SIZE_T = usize;
        #[repr(C)]
        struct PROCESS_MEMORY_COUNTERS {
            cb: DWORD,
            page_fault_count: DWORD,
            peak_working_set_size: SIZE_T,
            working_set_size: SIZE_T,
            quota_peak_paged_pool_usage: SIZE_T,
            quota_paged_pool_usage: SIZE_T,
            quota_peak_non_paged_pool_usage: SIZE_T,
            quota_non_paged_pool_usage: SIZE_T,
            pagefile_usage: SIZE_T,
            peak_pagefile_usage: SIZE_T,
        }
        #[link(name = "kernel32")]
        extern "system" {
            fn GetCurrentProcess() -> HANDLE;
        }
        #[link(name = "psapi")]
        extern "system" {
            fn GetProcessMemoryInfo(
                process: HANDLE,
                ppsmemcounters: *mut PROCESS_MEMORY_COUNTERS,
                cb: DWORD,
            ) -> i32;
        }
        unsafe {
            let mut pmc: PROCESS_MEMORY_COUNTERS = mem::zeroed();
            pmc.cb = mem::size_of::<PROCESS_MEMORY_COUNTERS>() as DWORD;
            if GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb) != 0 {
                return pmc.working_set_size as u64;
            }
        }
        0
    }
    #[cfg(target_os = "linux")]
    {
        if let Ok(data) = std::fs::read_to_string("/proc/self/statm") {
            // statm fields are in pages; second field is resident set size
            if let Some(rss_pages) = data.split_whitespace().nth(1) {
                if let Ok(pages) = rss_pages.parse::<u64>() {
                    return pages * 4096; // Assume 4KB page size
                }
            }
        }
        0
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        0
    }
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
