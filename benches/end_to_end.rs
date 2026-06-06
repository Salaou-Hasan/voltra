use futures::{SinkExt, StreamExt};
use hdrhistogram::Histogram;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_tungstenite::tungstenite::Message;

async fn client_workload(
    client_id: usize,
    num_calls: usize,
    url: String,
    latencies: Arc<Mutex<Histogram<u64>>>,
) -> usize {
    match tokio_tungstenite::connect_async(&url).await {
        Ok((mut ws_stream, _)) => {
            let mut success_count = 0;

            for i in 0..num_calls {
                // Create a simple JSON message that the server will parse
                let call_id = client_id as u64 * 100000 + i as u64;
                let msg = serde_json::json!({
                    "call_id": call_id,
                    "reducer_name": "increment",
                    "args": [1, 2, 3]
                });

                let start = Instant::now();
                if ws_stream.send(Message::Text(msg.to_string())).await.is_err() {
                    break;
                }

                if let Ok(Some(msg)) = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    ws_stream.next(),
                )
                .await
                {
                    let elapsed = start.elapsed();
                    match msg {
                        Ok(Message::Text(_)) | Ok(Message::Binary(_)) => {
                            let latency_us = elapsed.as_micros() as u64;
                            if let Ok(mut hist) = latencies.lock() {
                                let _ = hist.record(latency_us);
                            }
                            success_count += 1;
                        }
                        _ => {}
                    }
                }
            }

            success_count
        }
        Err(e) => {
            eprintln!("Client {} failed to connect: {}", client_id, e);
            0
        }
    }
}

#[tokio::main]
async fn main() {
    println!("=== NEONDB END-TO-END BENCHMARK ===\n");

    let ws_url = "ws://127.0.0.1:3000";

    println!("Attempting to connect to existing server on {}", ws_url);

    // Try to connect; if server isn't running, instructions will tell user
    match tokio_tungstenite::connect_async(ws_url).await {
        Ok(_) => {
            println!("✓ Server is ready\n");
        }
        Err(e) => {
            eprintln!("Error: Cannot connect to server at {}", ws_url);
            eprintln!("Please start the NeonDB server first:");
            eprintln!("  cargo run --release --bin neondb start");
            eprintln!("\nCurrent error: {}", e);
            return;
        }
    }

    let num_clients = 10;
    let calls_per_client = 5000;

    println!("Spawning {} clients, {} calls each...", num_clients, calls_per_client);
    println!("Total expected calls: {}\n", num_clients * calls_per_client);

    let latencies = Arc::new(Mutex::new(
        Histogram::<u64>::new(3).expect("Failed to create histogram"),
    ));

    let start = Instant::now();

    let mut tasks = vec![];
    for client_id in 0..num_clients {
        let url = ws_url.to_string();
        let latencies = latencies.clone();
        let task = tokio::spawn(async move {
            client_workload(client_id, calls_per_client, url, latencies).await
        });
        tasks.push(task);
    }

    let mut total_success = 0;
    for task in tasks {
        if let Ok(success) = task.await {
            total_success += success;
        }
    }

    let elapsed = start.elapsed();
    let tps = (total_success as f64) / elapsed.as_secs_f64();

    println!("=== RESULTS (No Subscriptions) ===");
    println!("Completed calls: {}/{}", total_success, num_clients * calls_per_client);
    println!("Elapsed time: {:.2}s", elapsed.as_secs_f64());
    println!("Throughput: {:.0} TPS", tps);

    if let Ok(hist) = latencies.lock() {
        println!("Latency (microseconds):");
        println!("  p50:  {:.2} μs ({:.3} ms)", hist.value_at_percentile(50.0), hist.value_at_percentile(50.0) as f64 / 1000.0);
        println!("  p95:  {:.2} μs ({:.3} ms)", hist.value_at_percentile(95.0), hist.value_at_percentile(95.0) as f64 / 1000.0);
        println!("  p99:  {:.2} μs ({:.3} ms)", hist.value_at_percentile(99.0), hist.value_at_percentile(99.0) as f64 / 1000.0);
        println!("  p99.9: {:.2} μs ({:.3} ms)", hist.value_at_percentile(99.9), hist.value_at_percentile(99.9) as f64 / 1000.0);
        println!("  max:  {:.2} μs ({:.3} ms)", hist.max(), hist.max() as f64 / 1000.0);
    }

    println!("\n✓ Benchmark complete");
}
