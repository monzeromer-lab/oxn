//! Demonstrates delayed and prioritized submission.

use std::time::Duration;

use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Task(String);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let q: Queue<Task> = Queue::builder("demo")
        .redis("redis://127.0.0.1:6379")
        .build()
        .await?;

    // Fires immediately.
    q.add(Task("now".into()), JobOptions::new().priority(1)).await?;
    // Delayed by 2s.
    q.add(
        Task("later".into()),
        JobOptions::new().delay(Duration::from_secs(2)),
    )
    .await?;

    let w = Worker::builder(q.clone(), |job: Job<Task>| async move {
        println!("processed {:?}", job.data);
        Ok::<_, Error>(())
    })
    .concurrency(2)
    .build();

    let tok = w.cancellation_token();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        tok.cancel();
    });

    w.run().await?;
    Ok(())
}
