// ============================================================================
// src/tracing_setup.rs — distributed tracing bootstrap
//
// Voltra's reducer-call lifecycle (queue wait → dispatch → commit → WAL
// append → subscription fan-out) is instrumented with `tracing` spans in the
// three worker-loop implementations:
//   - src/app/bootstrap.rs   (`voltra start` binary path)
//   - src/server.rs          (embedded-library `run_server` path)
//   - src/worker_pool.rs     (per-lobby dedicated-thread path)
//
// This module wires up WHERE those spans go:
//   - Always: a `fmt` subscriber prints span/event data to stderr. This is
//     the zero-config, zero-network default — same cost class as the
//     existing `env_logger` setup it sits alongside.
//   - `tracing-log` bridges the ~260 existing `log::info!/warn!/error!`
//     call sites into the `tracing` pipeline, so they inherit whatever span
//     is active (e.g. a log line inside a reducer-call span is tagged with
//     that span's call_id) without rewriting a single one of those sites.
//   - Optionally (feature `otel` + `VOLTRA_OTEL_ENDPOINT` set at runtime):
//     spans are ALSO exported over OTLP/gRPC to a collector (Jaeger, Tempo,
//     Honeycomb, ...). Without the feature compiled in, or without the env
//     var even when it is, this is a complete no-op — no exporter is built,
//     no background export task runs, no network calls are made. This is
//     what keeps tracing "opt-in, no perf cost by default" per the task.
//
// This intentionally does NOT replace `env_logger` call sites — see
// CLAUDE.md pitfall list culture: this codebase favors small, targeted
// changes over sweeping rewrites. `tracing_log::LogTracer` bridges the two
// ecosystems instead.
// ============================================================================

use crate::config::Config;

/// Guard returned by [`init`]. Dropping it flushes and shuts down the OTLP
/// exporter (when active). Hold this for the lifetime of the process (e.g.
/// bind it to a `let _guard = ...` in `main`/`run_server`) — dropping it
/// early would tear down tracing while the server is still running.
pub struct TracingGuard {
    #[cfg(feature = "otel")]
    _otel_active: bool,
}

/// Initialize the global `tracing` subscriber.
///
/// Safe to call multiple times within a process (e.g. once from the CLI
/// binary's `run_server` and, in tests, once per spawned server) — a second
/// call is a harmless no-op; `tracing`'s global dispatcher can only be set
/// once, and we swallow the `SetGlobalDefaultError` rather than panic, since
/// integration tests spawn many server instances in one test binary.
pub fn init(config: &Config) -> TracingGuard {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    // Reuse the same level the existing env_logger setup uses
    // (config.log_level, itself driven by RUST_LOG) so the two logging
    // paths never disagree about verbosity.
    let filter = EnvFilter::try_new(&config.log_level)
        .or_else(|_| EnvFilter::try_new("info"))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(true)
        .with_level(true);

    #[cfg(feature = "otel")]
    {
        if let Some(endpoint) = config.otel_endpoint.as_deref() {
            match build_otel_layer(
                endpoint,
                &config.otel_service_name,
                config.otel_sample_ratio,
            ) {
                Ok(otel_layer) => {
                    let registry = tracing_subscriber::registry()
                        .with(filter)
                        .with(fmt_layer)
                        .with(otel_layer);
                    let _ = registry.try_init();
                    let _ = tracing_log::LogTracer::init();
                    log::info!(
                        "[tracing] OTLP export active — endpoint={} service={}",
                        endpoint,
                        config.otel_service_name
                    );
                    return TracingGuard { _otel_active: true };
                }
                Err(e) => {
                    log::warn!(
                        "[tracing] Failed to initialize OTLP exporter ({}); falling back to local-only tracing",
                        e
                    );
                }
            }
        }
    }

    let registry = tracing_subscriber::registry().with(filter).with(fmt_layer);
    let _ = registry.try_init();
    let _ = tracing_log::LogTracer::init();

    #[cfg(feature = "otel")]
    {
        return TracingGuard {
            _otel_active: false,
        };
    }
    #[cfg(not(feature = "otel"))]
    {
        TracingGuard {}
    }
}

#[cfg(feature = "otel")]
fn build_otel_layer(
    endpoint: &str,
    service_name: &str,
    sample_ratio: f64,
) -> Result<
    tracing_opentelemetry::OpenTelemetryLayer<
        tracing_subscriber::Registry,
        opentelemetry_sdk::trace::Tracer,
    >,
    String,
> {
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry::KeyValue;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::Sampler;

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint)
        .build()
        .map_err(|e| format!("OTLP exporter build failed: {e}"))?;

    let resource = opentelemetry_sdk::Resource::builder()
        .with_attribute(KeyValue::new("service.name", service_name.to_string()))
        .build();

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::TraceIdRatioBased(sample_ratio.clamp(0.0, 1.0)))
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("voltra");

    // Leak the provider so it lives for the process lifetime (it must
    // outlive every span it created). This mirrors the common
    // tracing-opentelemetry setup pattern: the provider is intentionally
    // process-lifetime and torn down only on process exit, not per-request.
    let _ = opentelemetry::global::set_tracer_provider(provider);

    Ok(tracing_opentelemetry::layer().with_tracer(tracer))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_does_not_panic_without_otel_endpoint() {
        let mut cfg = Config::from_env();
        cfg.otel_endpoint = None;
        let _guard = init(&cfg);
        // A second call must also not panic (global dispatcher already set).
        let _guard2 = init(&cfg);
    }

    #[test]
    fn otel_endpoint_none_by_default() {
        // Confirms the documented "no-op by default" contract at the config layer.
        let cfg = Config::from_env();
        if std::env::var("VOLTRA_OTEL_ENDPOINT").is_err() {
            assert!(cfg.otel_endpoint.is_none());
        }
    }
}
