//! B8 — the event stream loses nothing.
//!
//! In 0.2 `QueueEvents::stream()` started reading at `$` lazily on the
//! first poll, so everything emitted between `stream()` and that poll was
//! lost (the 0.2 probe saw 0 of 40 events). Each `XREAD` kept only its first
//! entry, every subscriber held a pool slot blocked in `XREAD`, and errors
//! were retried in a tight loop.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common;
use futures::StreamExt;
use oxn::backend::Backend;
use oxn::events::{EventRecord, StreamStart};
use oxn::prelude::*;
use oxn_testkit::{ms, secs};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
struct T(u32);

/// Collect `n` events (or fail after `max`).
async fn take(stream: &mut oxn::events::EventStream, n: usize, max: Duration) -> Vec<Event> {
    let mut out = Vec::with_capacity(n);
    let deadline = Instant::now() + max;
    while out.len() < n {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, stream.next()).await {
            Ok(Some(Ok(e))) => out.push(e),
            Ok(Some(Err(e))) => panic!("stream error: {e}"),
            Ok(None) => panic!("stream ended after {} events", out.len()),
            Err(_) => panic!("only {} of {n} events within {max:?}", out.len()),
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn events_between_subscribe_and_first_poll_are_delivered() {
    let (q, _b) = common::fresh_queue::<T>("b8-first-poll").await;
    let mut stream = q.events().stream().await.unwrap();
    // No sleep, no poll yet: in 0.2 everything below was lost.
    for i in 0..20 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }
    let evs = take(&mut stream, 40, secs(5)).await;
    let added = evs
        .iter()
        .filter(|e| matches!(e, Event::Added { .. }))
        .count();
    let waiting = evs
        .iter()
        .filter(|e| matches!(e, Event::Waiting { .. }))
        .count();
    assert_eq!((added, waiting), (20, 20));
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_burst_is_delivered_completely_and_in_order() {
    let (q, _b) = common::fresh_queue::<T>("b8-burst").await;
    let mut stream = q.events().stream().await.unwrap();
    let n = 1_500u32;
    q.add_bulk((0..n).map(|i| (T(i), JobOptions::new())).collect())
        .await
        .unwrap();
    let t = Instant::now();
    let evs = take(&mut stream, 2 * n as usize, secs(10)).await;
    let added: Vec<u64> = evs
        .iter()
        .filter_map(|e| match e {
            Event::Added { id, .. } => id.as_str().parse().ok(),
            _ => None,
        })
        .collect();
    assert_eq!(
        added,
        (1..=n as u64).collect::<Vec<_>>(),
        "every add, in order"
    );
    // One entry per round trip would take seconds; batches take ms.
    assert!(t.elapsed() < secs(3), "3000 events took {:?}", t.elapsed());
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn subscribers_do_not_hold_pool_slots() {
    let backend = Arc::new(
        oxn::backend::redis::RedisBackend::connect(common::test_config().pool_size(1))
            .await
            .unwrap(),
    );
    let name = common::unique_queue_name("b8-pool");
    let q: Queue<T> = common::queue(backend.clone(), &name).await;
    let mut readers = Vec::new();
    for _ in 0..3 {
        let mut s = q.events().stream().await.unwrap();
        readers.push(tokio::spawn(
            async move { while s.next().await.is_some() {} },
        ));
    }
    tokio::time::sleep(ms(200)).await; // all three now blocked in XREAD
    let t = Instant::now();
    tokio::time::timeout(secs(3), q.add(T(1), JobOptions::new()))
        .await
        .expect("the only pool slot is held by a subscriber")
        .unwrap();
    assert!(
        t.elapsed() < ms(300),
        "add waited {:?} behind subscribers",
        t.elapsed()
    );
    for r in readers {
        r.abort();
    }
    let _ = backend.list_queues().await;
    common::teardown(&q).await;
}

async fn admin() -> redis::aio::MultiplexedConnection {
    redis::Client::open(common::redis_url())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap()
}

/// `CLIENT LIST` ids of this queue's subscriber connections.
async fn subscriber_ids(q: &str) -> Vec<String> {
    let list: String = redis::cmd("CLIENT")
        .arg("LIST")
        .query_async(&mut admin().await)
        .await
        .unwrap();
    let name = format!("name=oxn-events:{q} ");
    list.lines()
        .filter(|l| l.contains(&name))
        .filter_map(|l| {
            l.split_whitespace()
                .find_map(|f| f.strip_prefix("id="))
                .map(str::to_string)
        })
        .collect()
}

async fn take_records(
    s: &mut oxn::events::RecordStream,
    n: usize,
    max: Duration,
) -> Vec<EventRecord> {
    let mut out = Vec::new();
    let deadline = Instant::now() + max;
    while out.len() < n {
        let left = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(left, s.next()).await {
            Ok(Some(Ok(r))) => out.push(r),
            Ok(Some(Err(_))) => continue, // transport errors are tested separately
            Ok(None) => panic!("ended"),
            Err(_) => panic!("only {} of {n} records within {max:?}", out.len()),
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn records_resume_exactly_after_a_saved_id() {
    let (q, _b) = common::fresh_queue::<T>("b8-resume").await;
    for i in 0..10 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }
    // Beginning replays history: 10 × (waiting, added).
    let mut all = q.events().records(StreamStart::Beginning).await.unwrap();
    let first = take_records(&mut all, 20, secs(5)).await;
    let ids: Vec<&str> = first.iter().map(|r| r.id.as_str()).collect();
    let mut sorted = ids.clone();
    sorted.sort_by_key(|id| {
        let (ms, seq) = id.split_once('-').unwrap();
        (ms.parse::<u64>().unwrap(), seq.parse::<u64>().unwrap())
    });
    assert_eq!(ids, sorted, "ids strictly increase");
    assert!(first.iter().all(|r| r.at_ms > 0));

    // Resume after the 7th record: exactly records 8..20 come back.
    let saved = first[6].id.clone();
    let mut resumed = q.events().records(StreamStart::After(saved)).await.unwrap();
    let rest = take_records(&mut resumed, 13, secs(5)).await;
    assert_eq!(rest, first[7..].to_vec());
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_connection_reconnects_without_losing_events() {
    let (q, _b) = common::fresh_queue::<T>("b8-reconnect").await;
    let mut s = q.events().records(StreamStart::Now).await.unwrap();
    q.add(T(0), JobOptions::new()).await.unwrap();
    take_records(&mut s, 2, secs(5)).await; // connected and reading

    let ids = subscriber_ids(q.name()).await;
    assert_eq!(ids.len(), 1, "one named subscriber connection");
    let _: () = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("ID")
        .arg(&ids[0])
        .query_async(&mut admin().await)
        .await
        .unwrap();
    // Emitted while the subscriber is disconnected.
    for i in 1..=5 {
        q.add(T(i), JobOptions::new()).await.unwrap();
    }
    let mut errors = 0;
    let mut got = Vec::new();
    let deadline = Instant::now() + secs(10);
    while got.len() < 10 && Instant::now() < deadline {
        match tokio::time::timeout(secs(10), s.next())
            .await
            .unwrap()
            .unwrap()
        {
            Ok(r) => got.push(r),
            Err(_) => errors += 1,
        }
    }
    assert_eq!(errors, 1, "the drop surfaces once, as an error item");
    let added: Vec<String> = got
        .iter()
        .filter_map(|r| match &r.event {
            Event::Added { id, .. } => Some(id.to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        added,
        ["2", "3", "4", "5", "6"],
        "nothing lost or repeated across the reconnect"
    );
    assert_eq!(subscriber_ids(q.name()).await.len(), 1, "reconnected");
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_stream_closes_its_connection() {
    let (q, _b) = common::fresh_queue::<T>("b8-drop").await;
    let mut s = q.events().stream().await.unwrap();
    q.add(T(0), JobOptions::new()).await.unwrap();
    take(&mut s, 2, secs(5)).await;
    assert_eq!(subscriber_ids(q.name()).await.len(), 1);
    drop(s);
    let deadline = Instant::now() + secs(3);
    while !subscriber_ids(q.name()).await.is_empty() {
        assert!(Instant::now() < deadline, "subscriber connection leaked");
        tokio::time::sleep(ms(50)).await;
    }
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn delayed_and_released_events_carry_their_fields() {
    let (q, b) = common::fresh_queue::<T>("b8-fields").await;
    let mut s = q.events().stream().await.unwrap();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    q.add(T(1), JobOptions::new().delay(secs(60)))
        .await
        .unwrap();
    let evs = take(&mut s, 2, secs(5)).await;
    let due = evs
        .iter()
        .find_map(|e| match e {
            Event::Delayed { due_at_ms, .. } => Some(*due_at_ms),
            _ => None,
        })
        .expect("delayed event");
    assert!(
        due >= before + 60_000 && due < before + 62_000,
        "absolute due time, got {due}"
    );

    // A released job re-enters as waiting with prev=active.
    let id = q.add(T(2), JobOptions::new()).await.unwrap();
    take(&mut s, 2, secs(5)).await;
    let none = tokio_util::sync::CancellationToken::new();
    b.fetch_next(q.name(), "w", secs(30), ms(10), &none)
        .await
        .unwrap()
        .unwrap();
    b.release(q.name(), &id, "w").await.unwrap();
    let evs = take(&mut s, 2, secs(5)).await; // active, waiting
    assert!(
        evs.iter().any(
            |e| matches!(e, Event::Waiting { id: i, prev: Some(p) } if *i == id && p == "active")
        ),
        "{evs:?}"
    );
    common::teardown(&q).await;
}
