//! `oxn-load` — load, soak and chaos harness for oxn.
//!
//! ```text
//! scripts/test-redis.sh start                       # dedicated Redis on :6390
//! cargo run -p oxn-load --release -- run --scenario smoke
//! cargo run -p oxn-load --release -- run --scenario latency
//! cargo run -p oxn-load --release -- run --scenario kill-worker
//! cargo run -p oxn-load --release -- run --scenario throughput --jobs 200000
//! cargo run -p oxn-load --release -- run --chaos "5s:script-flush,10s:client-kill=0.5"
//! ```
//!
//! Every run prints a live per-second table, then verifies each job against
//! its plan and writes `target/oxn-reports/load/<scenario>-<ts>.{md,json}`.
//! The exit code is non-zero when anything is wrong, so a run doubles as a
//! CI gate.

mod chaos;
mod config;
mod ledger;
mod report;
mod run;
mod stats;
mod workload;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "oxn-load", about = "Load, soak and chaos harness for oxn")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a scenario and write a report.
    Run(Box<config::RunArgs>),
    /// Internal: a worker child process (used by `--processes true`).
    #[command(hide = true)]
    Worker {
        #[arg(long)]
        redis: String,
        #[arg(long)]
        queue: String,
        #[arg(long)]
        index: u32,
        #[arg(long)]
        concurrency: usize,
        #[arg(long)]
        lock_ms: u64,
        #[arg(long)]
        drain_delay_ms: Option<u64>,
        #[arg(long, default_value_t = 200_000)]
        max_stream_len: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if let Ok(f) = std::env::var("OXN_LOG") {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::new(f))
            .with_writer(std::io::stderr)
            .init();
    }
    match Cli::parse().cmd {
        Cmd::Worker {
            redis,
            queue,
            index,
            concurrency,
            lock_ms,
            drain_delay_ms,
            max_stream_len,
        } => {
            workload::MAX_STREAM_LEN.store(max_stream_len, std::sync::atomic::Ordering::Relaxed);
            workload::child_worker_main(redis, queue, index, concurrency, lock_ms, drain_delay_ms)
                .await
        }
        Cmd::Run(args) => {
            let cfg = config::Config::resolve(&args)?;
            let outcome = run::run(cfg).await?;
            let path = report::write(&outcome)?;
            println!();
            println!(
                "{} {} — {} jobs, {:.0} ok/s, pickup p50 {:.1}ms p99 {:.1}ms",
                if outcome.passed { "PASS" } else { "FAIL" },
                outcome.config.slug(),
                outcome.added,
                outcome.throughput_ok_per_s,
                outcome.pickup.p50_ms,
                outcome.pickup.p99_ms
            );
            for f in &outcome.failures {
                println!("  - {f}");
            }
            println!("report: {}", path.display());
            if !outcome.passed {
                std::process::exit(1);
            }
            Ok(())
        }
    }
}
