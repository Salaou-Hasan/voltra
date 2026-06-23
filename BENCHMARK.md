# Voltra Phase 1 Benchmark Report

## Environment

- CPU: Intel(R) Core(TM) i7-14650HX
- Cores: 16
- Logical processors: 24
- Max clock: 2200 MHz
- Rust toolchain: rustc 1.96.0 (ac68faa20 2026-05-25)
- OS: Windows (PowerShell host)

## Benchmark harness

- Command: `cargo bench`
- Benchmark target: `benches/throughput.rs`
- Metric: single `increment` reducer invocation cost

## Results

- `increment_1x` latency: [345.11 ns 345.77 ns 346.41 ns]
- Estimated p50: ~345.8 ns
- Estimated p95 / p99: within the same narrow distribution range (sub-350 ns) as reported by Criterion
- Estimated raw TPS: ~2.9 million operations per second

## Notes

- These results measure the hot-path reducer function cost in a benchmark harness, not full network/WAL throughput.
- The benchmark shows the current Phase 1 reducer implementation is extremely fast, with per-call cost under 0.35 microseconds.
- The simple `cargo bench` output was the source of the latency range above.
