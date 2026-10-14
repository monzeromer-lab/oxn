//! A queue-wide rate limit: at most 5 jobs start per second, however many
//! workers (or processes) run, as long as each one sets the same limit.
//!
//! ```text
//! cargo run --example rate_limit
//! REDIS_URL=redis://127.0.0.1:6390 cargo run --example rate_limit
//! ```

use std::sync::Arc;
use std::time::{Duration, Instant};

use oxn::prelude::*;

const JOBS: usize = 20;
const PER_SECOND: u32 = 5;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

    let queue: Queue<u32> = Queue::builder("rate-limit-demo").redis(url).build().await?;
    queue.obliterate(true).await?;
    for n in 0..JOBS as u32 {
        queue.add(n, JobOptions::new()).await?;
    }

    let t0 = Instant::now();
    let starts = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let (done_tx, mut done_rx) = tokio::sync::mpsc::unbounded_channel::<()>();

    // Two workers with plenty of concurrency: the limit, not the worker
    // count, sets the pace.
    let mut tokens = Vec::new();
    let mut handles = Vec::new();
    for w in 0..2 {
        let starts = starts.clone();
        let done_tx = done_tx.clone();
        let worker = Worker::builder(queue.clone(), move |job: Job<u32>| {
            let starts = starts.clone();
            let done_tx = done_tx.clone();
            async move {
                let at = t0.elapsed();
                println!(
                    "{:>6.2}s  worker {w} runs job {}",
                    at.as_secs_f64(),
                    job.data
                );
                starts.lock().push(at);
                let _ = done_tx.send(());
                Ok::<_, Error>(())
            }
        })
        .concurrency(10)
        .rate_limit(PER_SECOND, Duration::from_secs(1))
        .build();
        tokens.push(worker.cancellation_token());
        handles.push(tokio::spawn(worker.run()));
    }

    for _ in 0..JOBS {
        tokio::time::timeout(Duration::from_secs(30), done_rx.recv()).await?;
    }
    for t in &tokens {
        t.cancel();
    }
    for h in handles {
        h.await??;
    }

    // The window opens at the first start and lasts a second; the next
    // one opens when it expires. 20 jobs at 5 per window need 4 windows,
    // so the last job cannot start before 3s.
    let starts = starts.lock().clone();
    let span = *starts.iter().max().unwrap_or(&Duration::ZERO)
        - *starts.iter().min().unwrap_or(&Duration::ZERO);
    let windows = (JOBS as u32).div_ceil(PER_SECOND);
    println!(
        "{JOBS} jobs started over {:.2}s ({windows} windows of {PER_SECOND})",
        span.as_secs_f64()
    );
    assert!(span >= Duration::from_millis(u64::from(windows - 1) * 1000 - 50));
    queue.obliterate(true).await?;
    Ok(())
}
