use crate::table::RowDelta;
use serde::{Deserialize, Serialize};

/// WAL entry header
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct WalHeader {
    pub version: u32,
    pub entry_type: u8, // 1=ReducerCall, 2=Snapshot
    pub timestamp: u64, // Unix nanos
    pub sequence_number: u64,
    pub checksum: u32, // CRC32 of payload
}

/// A reducer call entry in the WAL
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReducerCallEntry {
    pub reducer_id: String, // "increment"
    pub args: Vec<u8>,      // Serialized args (MessagePack)
    pub deltas: Vec<RowDelta>,
}

/// Complete WAL entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalEntry {
    pub header: WalHeader,
    pub payload: ReducerCallEntry,
}

impl WalEntry {
    pub fn new(
        timestamp: u64,
        sequence_number: u64,
        reducer_id: String,
        args: Vec<u8>,
        deltas: Vec<RowDelta>,
    ) -> Self {
        let payload = ReducerCallEntry {
            reducer_id,
            args,
            deltas,
        };

        let checksum = Self::compute_checksum(&payload);

        let header = WalHeader {
            version: 1,
            entry_type: 1, // ReducerCall
            timestamp,
            sequence_number,
            checksum,
        };

        WalEntry { header, payload }
    }

    fn compute_checksum(payload: &ReducerCallEntry) -> u32 {
        // Simple CRC32 of serialized payload
        if let Ok(encoded) = rmp_serde::to_vec(payload) {
            crc32fast::hash(&encoded)
        } else {
            0
        }
    }

    pub fn verify_checksum(&self) -> bool {
        let expected = Self::compute_checksum(&self.payload);
        expected == self.header.checksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_entry_creation() {
        let entry = WalEntry::new(1000, 1, "increment".to_string(), vec![1, 2, 3], vec![]);

        assert_eq!(entry.header.version, 1);
        assert_eq!(entry.header.entry_type, 1);
        assert_eq!(entry.header.timestamp, 1000);
        assert_eq!(entry.header.sequence_number, 1);
    }

    #[test]
    fn test_checksum_verification() {
        let entry = WalEntry::new(1000, 1, "increment".to_string(), vec![1, 2, 3], vec![]);

        assert!(entry.verify_checksum());
    }
}
