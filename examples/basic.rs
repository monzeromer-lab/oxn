//! Basic producer + consumer on the same process.
//!
//! Run with `cargo run --example basic` against a local Redis.

use std::time::Duration;

use oxn::options::Removal;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct Email {
    to: String,
    subject: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let queue: Queue<Email> = Queue::builder("emails")
        .redis("redis://127.0.0.1:6379")
        // Drop jobs from Redis as soon as they finish, so the queue
        // stays tidy. Swap for `Removal::KeepLast(100)` to retain a
        // bounded history.
        .remove_on_complete(Removal::Remove)
        .build()
        .await?;

    queue
        .add(
            Email {
                to: "alice@example.com".into(),
                subject: "hello".into(),
            },
            JobOptions::new().attempts(3),
        )
        .await?;

    let worker = Worker::builder(queue.clone(), |job: Job<Email>| async move {
        println!("→ processing {:?}", job.data);
        tokio::time::sleep(Duration::from_millis(200)).await;
        Ok::<_, Error>(())
    })
    .concurrency(4)
    .build();

    let token = worker.cancellation_token();
    // Stop after 2 seconds for the example.
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        token.cancel();
    });

    worker.run().await?;
    Ok(())
}
