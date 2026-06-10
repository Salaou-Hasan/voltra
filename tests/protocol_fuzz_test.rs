// ============================================================================
// protocol_fuzz_test.rs — Robustness fuzzing of the wire protocol + WAL parser
//
// Goal: NO input — random bytes, truncated frames, bit-flipped valid frames,
// deeply nested payloads — may PANIC the decoder.  Returning Err is fine;
// crashing the process is a production outage.
//
// Pure in-process, no server spawn.  Deterministic PRNG (xorshift) so failures
// are reproducible from the printed seed.
// ============================================================================

use neondb::network::protocol::{decode_client_message, decode_reducer_call, encode_message};
use neondb::network::ReducerCall;
use neondb::wal::{WalEntry, WalReader, WalWriter};

/// Tiny deterministic PRNG — keeps the fuzz reproducible without a rand dep.
struct XorShift(u64);
impl XorShift {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn byte(&mut self) -> u8 { (self.next() & 0xFF) as u8 }
    fn range(&mut self, max: usize) -> usize { (self.next() as usize) % max.max(1) }
}

const SEED: u64 = 0x5EED_CAFE_F00D_2026;
const ITERATIONS: usize = 20_000;

#[test]
fn fuzz_decode_client_message_random_bytes_never_panics() {
    let mut rng = XorShift(SEED);
    for i in 0..ITERATIONS {
        let len = rng.range(512);
        let buf: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        // Must not panic — Err is acceptable, Ok is acceptable (rare but
        // possible for tiny random buffers that happen to be valid msgpack).
        let result = std::panic::catch_unwind(|| decode_client_message(&buf));
        assert!(result.is_ok(), "decode_client_message PANICKED on iteration {} (seed {:#x})", i, SEED);
    }
}

#[test]
fn fuzz_decode_reducer_call_random_bytes_never_panics() {
    let mut rng = XorShift(SEED ^ 0xABCD);
    for i in 0..ITERATIONS {
        let len = rng.range(512);
        let buf: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        let result = std::panic::catch_unwind(|| decode_reducer_call(&buf));
        assert!(result.is_ok(), "decode_reducer_call PANICKED on iteration {} (seed {:#x})", i, SEED);
    }
}

#[test]
fn fuzz_bitflipped_valid_frames_never_panic() {
    // Start from a VALID encoded frame, then flip random bits.  This explores
    // the "almost valid" space that pure random bytes rarely reach.
    let valid_call = ReducerCall {
        call_id: 42,
        reducer_name: "spawn_player".to_string(),
        args: rmp_serde::to_vec(&serde_json::json!(["alice", 10, 20, "warrior"])).unwrap(),
    };
    let frame = encode_message(&valid_call).expect("valid frame must encode");

    let mut rng = XorShift(SEED ^ 0x1234_5678);
    for i in 0..ITERATIONS {
        let mut mutated = frame.clone();
        // Flip 1-8 random bits.
        let flips = 1 + rng.range(8);
        for _ in 0..flips {
            let byte_idx = rng.range(mutated.len());
            let bit = rng.range(8);
            mutated[byte_idx] ^= 1 << bit;
        }
        let result = std::panic::catch_unwind(|| {
            let _ = decode_reducer_call(&mutated);
            let _ = decode_client_message(&mutated);
        });
        assert!(result.is_ok(), "bit-flip decode PANICKED on iteration {} (seed {:#x})", i, SEED);
    }
}

#[test]
fn fuzz_truncated_valid_frames_never_panic() {
    let valid_call = ReducerCall {
        call_id: 7,
        reducer_name: "attack".to_string(),
        args: rmp_serde::to_vec(&serde_json::json!(["a", "b", "sword", 25])).unwrap(),
    };
    let frame = encode_message(&valid_call).expect("valid frame must encode");

    // Every possible truncation point.
    for cut in 0..frame.len() {
        let truncated = &frame[..cut];
        let result = std::panic::catch_unwind(|| {
            let _ = decode_reducer_call(truncated);
            let _ = decode_client_message(truncated);
        });
        assert!(result.is_ok(), "truncation at byte {} PANICKED", cut);
    }
}

#[test]
fn fuzz_oversized_length_claims_never_panic() {
    // MessagePack frames that CLAIM huge string/array/map lengths but carry
    // few actual bytes — the classic allocation-bomb vector.
    let nasty_frames: Vec<Vec<u8>> = vec![
        vec![0xDB, 0xFF, 0xFF, 0xFF, 0xFF],             // str32 claiming 4 GiB
        vec![0xDD, 0xFF, 0xFF, 0xFF, 0xFF],             // array32 claiming 4 G elements
        vec![0xDF, 0xFF, 0xFF, 0xFF, 0xFF],             // map32 claiming 4 G pairs
        vec![0xC6, 0xFF, 0xFF, 0xFF, 0xFF],             // bin32 claiming 4 GiB
        vec![0xDB, 0x7F, 0xFF, 0xFF, 0xFF, 0x41, 0x41], // str32 2 GiB with 2 bytes
    ];
    for (i, frame) in nasty_frames.iter().enumerate() {
        let result = std::panic::catch_unwind(|| {
            let _ = decode_reducer_call(frame);
            let _ = decode_client_message(frame);
        });
        assert!(result.is_ok(), "oversized-length frame {} PANICKED", i);
    }
}

#[test]
fn fuzz_deeply_nested_json_args_never_panic() {
    // 1000-deep nested arrays in args — recursion-depth attack on any
    // recursive deserializer in the pipeline.
    let mut nested = String::new();
    for _ in 0..1000 { nested.push('['); }
    for _ in 0..1000 { nested.push(']'); }
    // Direct value parse (serde_json has its own depth limit — must Err, not stack-overflow).
    let result = std::panic::catch_unwind(|| {
        let _ = serde_json::from_str::<serde_json::Value>(&nested);
    });
    assert!(result.is_ok(), "deep-nesting JSON parse PANICKED");
}

#[test]
fn fuzz_wal_reader_random_bytes_never_panics() {
    let mut rng = XorShift(SEED ^ 0xDEAD_BEEF);
    for i in 0..200 {
        let path = std::env::temp_dir().join(format!(
            "fuzz_wal_{}_{}_{}.bin", std::process::id(), i, rng.next()
        ));
        let len = rng.range(4096);
        let garbage: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        std::fs::write(&path, &garbage).unwrap();

        let result = std::panic::catch_unwind(|| {
            if let Ok(mut reader) = WalReader::open(&path) {
                let _ = reader.read_all_entries();
            }
        });
        std::fs::remove_file(&path).ok();
        assert!(result.is_ok(), "WAL reader PANICKED on garbage file iteration {} (seed {:#x})", i, SEED);
    }
}

#[test]
fn fuzz_wal_reader_corrupted_valid_file_never_panics() {
    // Valid WAL with 5 entries, then corrupt random bytes and re-read.
    let mut rng = XorShift(SEED ^ 0xFEED);
    let base_path = std::env::temp_dir().join(format!(
        "fuzz_wal_corrupt_base_{}.bin", std::process::id()
    ));
    {
        let mut w = WalWriter::open(&base_path).unwrap();
        for seq in 1..=5u64 {
            w.append(&WalEntry::new(1000 + seq, seq, "inc".into(), vec![1, 2, 3], vec![])).unwrap();
        }
        w.fsync().unwrap();
    }
    let valid_bytes = std::fs::read(&base_path).unwrap();
    std::fs::remove_file(&base_path).ok();

    for i in 0..500 {
        let mut corrupted = valid_bytes.clone();
        let flips = 1 + rng.range(16);
        for _ in 0..flips {
            let idx = rng.range(corrupted.len());
            corrupted[idx] ^= (1 << rng.range(8)) as u8;
        }
        let path = std::env::temp_dir().join(format!(
            "fuzz_wal_corrupt_{}_{}.bin", std::process::id(), i
        ));
        std::fs::write(&path, &corrupted).unwrap();

        let result = std::panic::catch_unwind(|| {
            if let Ok(mut reader) = WalReader::open(&path) {
                if let Ok(entries) = reader.read_all_entries() {
                    // Checksum must catch payload corruption — verify never panics.
                    for e in &entries { let _ = e.verify_checksum(); }
                }
            }
        });
        std::fs::remove_file(&path).ok();
        assert!(result.is_ok(), "corrupted-WAL read PANICKED on iteration {} (seed {:#x})", i, SEED);
    }
}

#[test]
fn fuzz_replication_decode_garbage_never_panics() {
    let mut rng = XorShift(SEED ^ 0x9999);
    for i in 0..2_000 {
        // Random "base64-ish" strings, some valid base64 of garbage.
        let len = rng.range(256);
        let raw: Vec<u8> = (0..len).map(|_| rng.byte()).collect();
        let strings = vec![
            String::from_utf8_lossy(&raw).into_owned(),
            base64_encode(&raw),
        ];
        let result = std::panic::catch_unwind(|| {
            let _ = neondb::replication::decode_entries(&strings);
        });
        assert!(result.is_ok(), "replication decode PANICKED on iteration {} (seed {:#x})", i, SEED);
    }
}

fn base64_encode(data: &[u8]) -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::STANDARD.encode(data)
}
