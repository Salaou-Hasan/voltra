// src/stat_sync.rs â€” Post-match cross-region stat write-back
//
// When a player on region A plays a match that runs on region B (lobby-
// authoritative routing), the match result needs to be written back to
// region A's tables (player stats, XP, rank, etc.) after the match ends.
//
// This module provides a fire-and-forget queue:
//   1. Game code calls StatSyncQueue::enqueue(job) at match end.
//   2. A background worker batches jobs by target region and POSTs them to
//      POST {metrics_url}/cluster/stat-sync on the home region.
//   3. The receiving node writes the rows directly into its TableStore.
//
// Retries: 3 attempts with exponential back-off (200ms / 600ms / 1.8s).
// Failed jobs after all retries are logged and dropped â€” the alternative
// (blocking the match-end flow) is worse for player experience.
//
// Config:
//   VOLTRA_STAT_SYNC_WORKERS â€” parallel flush workers (default 4)
//   VOLTRA_STAT_SYNC_FLUSH_MS â€” how often to flush the queue (default 500)

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::cluster::regions::RegionRegistry;
use crate::table::TableStore;

/// A single stat write-back job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatSyncJob {
    /// The player's home region â€” where this write should land.
    pub home_region: String,
    /// Table to write to (e.g. "players", "stats").
    pub table: String,
    /// Row key (e.g. player_id).
    pub key: String,
    /// Row data to merge. Uses last-write-wins semantics.
    pub data: serde_json::Value,
}

/// Payload sent to POST /cluster/stat-sync.
#[derive(Serialize, Deserialize)]
pub struct StatSyncBatch {
    pub jobs: Vec<StatSyncJob>,
}

/// Response from POST /cluster/stat-sync.
#[derive(Deserialize)]
pub struct StatSyncResponse {
    pub written: usize,
    pub errors:  usize,
}

pub struct StatSyncQueue {
    tx:      flume::Sender<StatSyncJob>,
}

impl StatSyncQueue {
    /// Create the queue and spawn the background flush worker.
    pub fn new(
        tables:    Arc<TableStore>,
        regions:   Arc<RegionRegistry>,
        flush_ms:  u64,
        shutdown: watch::Receiver<()>,
    ) -> Arc<Self> {
        let (tx, rx) = flume::unbounded::<StatSyncJob>();
        let q = Arc::new(StatSyncQueue { tx });

        tokio::spawn(flush_loop(rx, tables, regions, flush_ms, shutdown));

        q
    }

    /// Enqueue a stat write-back job.  Non-blocking â€” drops if queue is full
    /// (unbounded, so only drops on channel close at shutdown).
    pub fn enqueue(&self, job: StatSyncJob) {
        let _ = self.tx.send(job);
    }

    /// Enqueue multiple jobs at once.
    pub fn enqueue_batch(&self, jobs: impl IntoIterator<Item = StatSyncJob>) {
        for job in jobs {
            self.enqueue(job);
        }
    }
}

async fn flush_loop(
    rx:       flume::Receiver<StatSyncJob>,
    tables:   Arc<TableStore>,
    regions:  Arc<RegionRegistry>,
    flush_ms: u64,
    mut shutdown: watch::Receiver<()>,
) {
    let interval = if flush_ms == 0 { 500 } else { flush_ms };
    let mut ticker = tokio::time::interval(Duration::from_millis(interval));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let jobs: Vec<StatSyncJob> = rx.drain().collect();
                if jobs.is_empty() { continue; }

                // Partition by home region.
                let mut by_region: std::collections::HashMap<String, Vec<StatSyncJob>> =
                    std::collections::HashMap::new();
                for job in jobs {
                    by_region.entry(job.home_region.clone()).or_default().push(job);
                }

                for (region_id, batch) in by_region {
                    if region_id == regions.my_region {
                        // Local write â€” no HTTP needed.
                        apply_local(&tables, &batch);
                    } else if let Some(region) = regions.get(&region_id) {
                        let url = format!("{}/cluster/stat-sync", region.metrics_url);
                        tokio::spawn(send_batch(url, batch));
                    } else {
                        log::warn!("[stat-sync] Unknown home region '{}', dropping {} jobs", region_id, batch.len());
                    }
                }
            }
            _ = shutdown.changed() => {
                // Drain remaining jobs before exit.
                let remaining: Vec<StatSyncJob> = rx.drain().collect();
                if !remaining.is_empty() {
                    apply_local(&tables, &remaining);
                }
                break;
            }
        }
    }
}

fn apply_local(tables: &TableStore, jobs: &[StatSyncJob]) {
    for job in jobs {
        if let Err(e) = tables.set_row(job.table.clone(), job.key.clone(), job.data.clone()) {
            log::warn!("[stat-sync] Local write failed for {}/{}: {}", job.table, job.key, e);
        }
    }
}

async fn send_batch(url: String, jobs: Vec<StatSyncJob>) {
    let payload = StatSyncBatch { jobs };
    let body = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => { log::warn!("[stat-sync] Serialize error: {}", e); return; }
    };

    const MAX_ATTEMPTS: u32 = 3;
    let delays = [200u64, 600, 1800];

    for attempt in 0..MAX_ATTEMPTS {
        let body_clone = body.clone();
        let url_clone  = url.clone();
        let result = tokio::task::spawn_blocking(move || {
            reqwest::blocking::Client::new()
                .post(&url_clone)
                .header("Content-Type", "application/json")
                .body(body_clone)
                .timeout(Duration::from_secs(5))
                .send()
                .and_then(|r| r.json::<StatSyncResponse>())
        }).await;

        match result {
            Ok(Ok(resp)) => {
                if resp.errors > 0 {
                    log::warn!("[stat-sync] Remote wrote {} ok, {} errors", resp.written, resp.errors);
                }
                return;
            }
            Ok(Err(e)) => {
                log::warn!("[stat-sync] Attempt {}/{}: {}", attempt + 1, MAX_ATTEMPTS, e);
            }
            Err(e) => {
                log::warn!("[stat-sync] Task panic attempt {}/{}: {}", attempt + 1, MAX_ATTEMPTS, e);
            }
        }

        if attempt + 1 < MAX_ATTEMPTS {
            tokio::time::sleep(Duration::from_millis(delays[attempt as usize])).await;
        }
    }
    log::warn!("[stat-sync] Dropping batch to {} after {} failed attempts", url, MAX_ATTEMPTS);
}

/// HTTP handler for POST /cluster/stat-sync.
/// Applies incoming jobs to the local TableStore.
pub fn handle_stat_sync(tables: &TableStore, body: &[u8]) -> serde_json::Value {
    let batch: StatSyncBatch = match serde_json::from_slice(body) {
        Ok(b) => b,
        Err(e) => return serde_json::json!({ "error": e.to_string(), "written": 0, "errors": 0 }),
    };

    let mut written = 0usize;
    let mut errors  = 0usize;

    for job in &batch.jobs {
        match tables.set_row(job.table.clone(), job.key.clone(), job.data.clone()) {
            Ok(_) => written += 1,
            Err(e) => {
                log::warn!("[stat-sync] Write {}/{} failed: {}", job.table, job.key, e);
                errors += 1;
            }
        }
    }

    serde_json::json!({ "written": written, "errors": errors })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::TableStore;

    #[test]
    fn apply_local_writes_rows() {
        let tables = TableStore::new();
        let jobs = vec![
            StatSyncJob {
                home_region: "europe".to_string(),
                table: "players".to_string(),
                key: "alice".to_string(),
                data: serde_json::json!({ "xp": 500 }),
            },
        ];
        apply_local(&tables, &jobs);
        let row = tables.get_row("players", "alice").unwrap().unwrap();
        assert_eq!(row["xp"], 500);
    }

    #[test]
    fn handle_stat_sync_applies_batch() {
        let tables = TableStore::new();
        let batch = StatSyncBatch {
            jobs: vec![
                StatSyncJob {
                    home_region: "europe".to_string(),
                    table: "stats".to_string(),
                    key: "bob".to_string(),
                    data: serde_json::json!({ "kills": 10 }),
                },
            ],
        };
        let body = serde_json::to_vec(&batch).unwrap();
        let result = handle_stat_sync(&tables, &body);
        assert_eq!(result["written"], 1);
        assert_eq!(result["errors"], 0);
    }

    #[test]
    fn handle_stat_sync_bad_json() {
        let tables = TableStore::new();
        let result = handle_stat_sync(&tables, b"not json");
        assert!(result["error"].as_str().unwrap().len() > 0);
    }
}
