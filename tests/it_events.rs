//! Redis Streams event consumer.

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T;

#[tokio::test(flavor = "multi_thread")]
async fn subscriber_receives_added_and_completed_events() {
    let (q, _be) = common::fresh_queue::<T>("events-basic").await;

    let seen = Arc::new(Mutex::new(Vec::<String>::new()));
    let seen_c = seen.clone();

    let events = q.events();
    let reader = tokio::spawn(async move {
        let mut stream = events.stream().await.unwrap();
        while let Some(Ok(ev)) = stream.next().await {
            let tag = match ev {
                Event::Added { .. } => "added",
                Event::Waiting { .. } => "waiting",
                Event::Active { .. } => "active",
                Event::Completed { .. } => "completed",
                _ => continue,
            };
            let mut s = seen_c.lock().await;
            s.push(tag.to_string());
            if s.iter().any(|t| t == "completed") {
                return;
            }
        }
    });

    // Give the stream a moment to attach (XREAD BLOCK sits on `$`).
    tokio::time::sleep(Duration::from_millis(150)).await;

    q.add(T, JobOptions::new()).await.unwrap();

    let worker = Worker::builder(q.clone(), |_job: Job<T>| async move {
        Ok::<_, Error>(())
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(100))
    .build();

    let cancel = worker.cancellation_token();
    let wh = tokio::spawn(worker.run());

    let ok = tokio::time::timeout(Duration::from_secs(5), reader).await;
    assert!(ok.is_ok(), "event reader timed out");

    let s = seen.lock().await.clone();
    assert!(s.contains(&"added".to_string()), "no added: {s:?}");
    assert!(s.contains(&"completed".to_string()), "no completed: {s:?}");

    cancel.cancel();
    wh.await.unwrap().unwrap();
    common::teardown(&q).await;
}
