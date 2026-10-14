//! Graceful shutdown: on Ctrl-C (or after `STOP_AFTER_MS`, default 1500),
//! the worker stops taking jobs, lets running handlers finish for up to
//! `shutdown_timeout`, and releases the rest back to the queue with their
//! attempt refunded. No job is failed by the shutdown.
//!
//! ```text
//! cargo run --example graceful_shutdown
//! REDIS_URL=redis://127.0.0.1:6390 cargo run --example graceful_shutdown
//! ```
//!
//! Short jobs finish during the grace period. The long one notices the
//! shutdown through its `JobContext` and checkpoints. The one that ignores
//! it is aborted at `shutdown_timeout` and released, so the next worker
//! runs it from the start.

use std::time::Duration;

use futures::StreamExt;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Task {
    kind: Kind,
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
enum Kind {
    /// Finishes within the grace period.
    Short,
    /// Long, but checks for shutdown and saves its place.
    Cooperative,
    /// Long and ignores shutdown: aborted and released.
    Stubborn,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let stop_after = std::env::var("STOP_AFTER_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1500);

    let queue: Queue<Task> = Queue::builder("shutdown-demo").redis(url).build().await?;
    queue.obliterate(true).await?;

    // Watch what happens to the jobs.
    let mut events = queue.events().stream().await?;
    let watcher = tokio::spawn(async move {
        while let Some(Ok(ev)) = events.next().await {
            match ev {
                Event::Completed { id, .. } => println!("  event: {id} completed"),
                Event::Waiting {
                    id,
                    prev: Some(prev),
                } => println!("  event: {id} back to waiting (from {prev})"),
                Event::Failed { id, reason, .. } => println!("  event: {id} FAILED: {reason}"),
                _ => {}
            }
        }
    });

    for kind in [Kind::Short, Kind::Short, Kind::Cooperative, Kind::Stubborn] {
        queue
            .add(Task { kind }, JobOptions::new().attempts(1))
            .await?;
    }

    let worker = Worker::builder_with_ctx(
        queue.clone(),
        move |job: Job<Task>, ctx: JobContext| async move {
            println!("start {} ({:?})", job.id, job.data.kind);
            match job.data.kind {
                Kind::Short => tokio::time::sleep(Duration::from_millis(stop_after + 500)).await,
                Kind::Cooperative => {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                        _ = ctx.cancelled() => {
                            ctx.log("shutdown: saved progress, will resume later").await?;
                            println!("{} checkpointed", job.id);
                            // Postpone: back to `delayed`, attempt refunded.
                            return Err(Error::Delayed { delay_ms: 1_000 });
                        }
                    }
                }
                Kind::Stubborn => tokio::time::sleep(Duration::from_secs(60)).await,
            }
            println!("done  {}", job.id);
            Ok::<_, Error>(())
        },
    )
    .concurrency(4)
    .shutdown_timeout(Duration::from_secs(2))
    .build();

    let token = worker.cancellation_token();
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = tokio::time::sleep(Duration::from_millis(stop_after)) => {}
        }
        println!("-- shutting down (grace period 2s) --");
        token.cancel();
    });

    worker.run().await?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    watcher.abort();

    let c = queue.counts().await?;
    println!(
        "after shutdown: completed={} delayed={} waiting={} failed={}",
        c.completed, c.delayed, c.waiting, c.failed
    );
    assert_eq!(c.failed, 0, "shutdown must not fail jobs");
    queue.obliterate(true).await?;
    Ok(())
}
