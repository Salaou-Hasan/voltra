use std::sync::atomic::{AtomicU64, Ordering};

// Tier thresholds (MsgPack bytes before compression)
const TIER_NONE_CEILING: usize = 128;
const TIER_BASIC_CEILING: usize = 2048;

// zstd compression levels
const ZSTD_BASIC: i32 = 1;
const ZSTD_HEAVY: i32 = 6;

// Queue pressure threshold: above this fraction, downgrade Heavy → Basic
const PRESSURE_DOWNGRADE: f32 = 0.5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionTier {
    None,
    Basic,
    Heavy,
}

pub struct HybridCompressor {
    queue_capacity: u64,
    queue_depth: AtomicU64,
    bytes_saved: AtomicU64,
    batches_compressed: AtomicU64,
    batches_skipped: AtomicU64,
}

impl HybridCompressor {
    pub fn new(queue_capacity: u64) -> Self {
        Self {
            queue_capacity,
            queue_depth: AtomicU64::new(0),
            bytes_saved: AtomicU64::new(0),
            batches_compressed: AtomicU64::new(0),
            batches_skipped: AtomicU64::new(0),
        }
    }

    pub fn update_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    fn queue_pressure(&self) -> f32 {
        if self.queue_capacity == 0 {
            return 0.0;
        }
        self.queue_depth.load(Ordering::Relaxed) as f32 / self.queue_capacity as f32
    }

    pub fn select_tier(&self, payload_bytes: usize) -> CompressionTier {
        if payload_bytes < TIER_NONE_CEILING {
            return CompressionTier::None;
        }

        let pressure = self.queue_pressure();

        if payload_bytes < TIER_BASIC_CEILING || pressure > PRESSURE_DOWNGRADE {
            CompressionTier::Basic
        } else {
            CompressionTier::Heavy
        }
    }

    pub fn compress(&self, raw: &[u8]) -> (Vec<u8>, bool) {
        let tier = self.select_tier(raw.len());

        match tier {
            CompressionTier::None => {
                self.batches_skipped.fetch_add(1, Ordering::Relaxed);
                (raw.to_vec(), false)
            }
            CompressionTier::Basic => {
                match zstd::encode_all(raw, ZSTD_BASIC) {
                    Ok(compressed) => {
                        if compressed.len() < raw.len() {
                            let saved = (raw.len() - compressed.len()) as u64;
                            self.bytes_saved.fetch_add(saved, Ordering::Relaxed);
                            self.batches_compressed.fetch_add(1, Ordering::Relaxed);
                            (compressed, true)
                        } else {
                            self.batches_skipped.fetch_add(1, Ordering::Relaxed);
                            (raw.to_vec(), false)
                        }
                    }
                    Err(_) => {
                        self.batches_skipped.fetch_add(1, Ordering::Relaxed);
                        (raw.to_vec(), false)
                    }
                }
            }
            CompressionTier::Heavy => {
                match zstd::encode_all(raw, ZSTD_HEAVY) {
                    Ok(compressed) => {
                        if compressed.len() < raw.len() {
                            let saved = (raw.len() - compressed.len()) as u64;
                            self.bytes_saved.fetch_add(saved, Ordering::Relaxed);
                            self.batches_compressed.fetch_add(1, Ordering::Relaxed);
                            (compressed, true)
                        } else {
                            self.batches_skipped.fetch_add(1, Ordering::Relaxed);
                            (raw.to_vec(), false)
                        }
                    }
                    Err(_) => {
                        self.batches_skipped.fetch_add(1, Ordering::Relaxed);
                        (raw.to_vec(), false)
                    }
                }
            }
        }
    }

    pub fn compress_subscription_batch(
        &self,
        diffs: &[crate::network::message::SubscriptionDiff],
    ) -> Option<(Vec<u8>, bool)> {
        let raw = rmp_serde::to_vec(diffs).ok()?;
        let (payload, compressed) = self.compress(&raw);
        Some((payload, compressed))
    }

    pub fn bytes_saved_total(&self) -> u64 {
        self.bytes_saved.load(Ordering::Relaxed)
    }

    pub fn batches_compressed_total(&self) -> u64 {
        self.batches_compressed.load(Ordering::Relaxed)
    }

    pub fn batches_skipped_total(&self) -> u64 {
        self.batches_skipped.load(Ordering::Relaxed)
    }

    pub fn compression_ratio_estimate(&self) -> f64 {
        let compressed = self.batches_compressed.load(Ordering::Relaxed);
        let total = compressed + self.batches_skipped.load(Ordering::Relaxed);
        if total == 0 { return 0.0; }
        compressed as f64 / total as f64
    }
}

pub fn decompress(payload: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    zstd::decode_all(payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_compressor() -> HybridCompressor {
        HybridCompressor::new(1000)
    }

    #[test]
    fn tier_none_for_small_payloads() {
        let c = make_compressor();
        assert_eq!(c.select_tier(0), CompressionTier::None);
        assert_eq!(c.select_tier(50), CompressionTier::None);
        assert_eq!(c.select_tier(127), CompressionTier::None);
    }

    #[test]
    fn tier_basic_for_medium_payloads() {
        let c = make_compressor();
        assert_eq!(c.select_tier(128), CompressionTier::Basic);
        assert_eq!(c.select_tier(500), CompressionTier::Basic);
        assert_eq!(c.select_tier(2047), CompressionTier::Basic);
    }

    #[test]
    fn tier_heavy_for_large_payloads() {
        let c = make_compressor();
        assert_eq!(c.select_tier(2048), CompressionTier::Heavy);
        assert_eq!(c.select_tier(10_000), CompressionTier::Heavy);
    }

    #[test]
    fn pressure_downgrades_heavy_to_basic() {
        let c = make_compressor();
        c.update_queue_depth(600); // 60% pressure > 50% threshold
        assert_eq!(c.select_tier(5000), CompressionTier::Basic);
    }

    #[test]
    fn small_payload_not_compressed() {
        let c = make_compressor();
        let data = vec![1u8; 50];
        let (out, compressed) = c.compress(&data);
        assert!(!compressed);
        assert_eq!(out, data);
    }

    #[test]
    fn large_repetitive_payload_compressed() {
        let c = make_compressor();
        let data = vec![42u8; 4096]; // highly compressible
        let (out, compressed) = c.compress(&data);
        assert!(compressed);
        assert!(out.len() < data.len());
    }

    #[test]
    fn compressed_data_roundtrips() {
        let c = make_compressor();
        let data = vec![42u8; 4096];
        let (compressed_bytes, was_compressed) = c.compress(&data);
        assert!(was_compressed);
        let decompressed = decompress(&compressed_bytes).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn stats_track_correctly() {
        let c = make_compressor();

        // Small payload → skip
        c.compress(&vec![1u8; 50]);
        assert_eq!(c.batches_skipped_total(), 1);
        assert_eq!(c.batches_compressed_total(), 0);

        // Large payload → compress
        c.compress(&vec![42u8; 4096]);
        assert_eq!(c.batches_compressed_total(), 1);
        assert!(c.bytes_saved_total() > 0);
    }

    #[test]
    fn zero_queue_capacity_never_panics() {
        let c = HybridCompressor::new(0);
        assert_eq!(c.queue_pressure(), 0.0);
        assert_eq!(c.select_tier(5000), CompressionTier::Heavy);
    }

    #[test]
    fn compress_subscription_batch_roundtrips() {
        use crate::network::message::SubscriptionDiff;

        let c = make_compressor();
        let diffs: Vec<SubscriptionDiff> = (0..50)
            .map(|i| SubscriptionDiff {
                subscription_id: format!("sub_{}", i),
                table_name: "l0_players".to_string(),
                row_key: format!("p{}", i),
                operation: "patch".to_string(),
                row_data: Some(serde_json::json!({"x": i * 10})),
            })
            .collect();

        let (payload, compressed) = c.compress_subscription_batch(&diffs).unwrap();
        let raw = if compressed {
            decompress(&payload).unwrap()
        } else {
            payload
        };
        let decoded: Vec<SubscriptionDiff> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(decoded.len(), 50);
        assert_eq!(decoded[0].operation, "patch");
    }
}
