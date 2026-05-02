//! Bulk dashboard ops: `clean(state)` and `promote_all`.

mod common;

use std::time::Duration;

use oxn::backend::Backend;
use oxn::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct T;

#[tokio::test(flavor = "multi_thread")]
async fn clean_completed_drops_jobs_and_their_hashes() {
    let (q, backend) = common::fresh_queue::<T>("clean-completed").await;

    // Seed three jobs and run them through to completion.
    for _ in 0..3u32 {
        q.add(T, JobOptions::new()).await.unwrap();
    }
    let worker =
        Worker::builder(q.clone(), |_: Job<T>| async { Ok::<_, Error>(()) })
            .concurrency(1)
            .drain_delay(Duration::from_millis(50))
            .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());

    common::wait_for(Duration::from_secs(8), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.completed == 3).unwrap_or(false) }
    })
    .await;

    cancel.cancel();
    h.await.unwrap().unwrap();

    let pre_jobs = q
        .list(JobState::Completed, oxn::backend::JobRange::first(10))
        .await
        .unwrap();
    assert_eq!(pre_jobs.len(), 3);

    let removed = backend
        .clean(q.name(), JobState::Completed, 0)
        .await
        .unwrap();
    assert_eq!(removed, 3);

    assert_eq!(q.counts().await.unwrap().completed, 0);
    // Job hashes should be gone too.
    for j in pre_jobs {
        assert!(
            backend.get(q.name(), &j.id).await.unwrap().is_none(),
            "job hash {} survived clean",
            j.id
        );
    }

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_with_limit_only_removes_oldest_n() {
    let (q, backend) = common::fresh_queue::<T>("clean-limit").await;

    for _ in 0..5u32 {
        q.add(T, JobOptions::new()).await.unwrap();
    }
    let worker =
        Worker::builder(q.clone(), |_: Job<T>| async { Ok::<_, Error>(()) })
            .concurrency(1)
            .drain_delay(Duration::from_millis(50))
            .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());

    common::wait_for(Duration::from_secs(8), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.completed == 5).unwrap_or(false) }
    })
    .await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let removed = backend
        .clean(q.name(), JobState::Completed, 2)
        .await
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(q.counts().await.unwrap().completed, 3);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_rejects_list_backed_states() {
    let (_q, backend) = common::fresh_queue::<T>("clean-bad-state").await;
    let err = backend
        .clean("anything", JobState::Active, 0)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Config(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_all_moves_every_delayed_job_to_wait() {
    let (q, backend) = common::fresh_queue::<T>("promote-all").await;

    for _ in 0..4u32 {
        q.add(T, JobOptions::new().delay(Duration::from_secs(60)))
            .await
            .unwrap();
    }
    assert_eq!(q.counts().await.unwrap().delayed, 4);

    let promoted = backend.promote_all(q.name()).await.unwrap();
    assert_eq!(promoted, 4);

    let c = q.counts().await.unwrap();
    assert_eq!(c.delayed, 0);
    assert_eq!(c.waiting, 4);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_all_honors_paused_queue() {
    let (q, backend) = common::fresh_queue::<T>("promote-all-paused").await;

    q.add(T, JobOptions::new().delay(Duration::from_secs(60)))
        .await
        .unwrap();
    q.add(T, JobOptions::new().delay(Duration::from_secs(60)))
        .await
        .unwrap();
    q.pause().await.unwrap();

    let promoted = backend.promote_all(q.name()).await.unwrap();
    assert_eq!(promoted, 2);

    // Promoted jobs should land in `paused`, not `wait`, since the queue
    // is currently paused.
    let c = q.counts().await.unwrap();
    assert_eq!(c.delayed, 0);
    assert_eq!(c.waiting, 0);
    assert_eq!(c.paused, 2);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_all_returns_zero_when_no_delayed_jobs() {
    let (q, backend) = common::fresh_queue::<T>("promote-all-empty").await;
    let promoted = backend.promote_all(q.name()).await.unwrap();
    assert_eq!(promoted, 0);
    common::teardown(&q).await;
}

// ── extra coverage ────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn clean_works_on_failed_state_too() {
    let (q, backend) = common::fresh_queue::<T>("clean-failed").await;

    // 3 jobs that always fail with attempts=1 → all land in `failed`.
    for _ in 0..3u32 {
        q.add(T, JobOptions::new().attempts(1)).await.unwrap();
    }
    let worker = Worker::builder(q.clone(), |_: Job<T>| async {
        Err::<(), _>(Error::handler(std::io::Error::other("boom")))
    })
    .concurrency(1)
    .drain_delay(Duration::from_millis(50))
    .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());

    common::wait_for(Duration::from_secs(8), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.failed == 3).unwrap_or(false) }
    })
    .await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    let removed = backend.clean(q.name(), JobState::Failed, 0).await.unwrap();
    assert_eq!(removed, 3);
    assert_eq!(q.counts().await.unwrap().failed, 0);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_works_on_delayed_state() {
    let (q, backend) = common::fresh_queue::<T>("clean-delayed").await;
    for _ in 0..3u32 {
        q.add(T, JobOptions::new().delay(Duration::from_secs(60)))
            .await
            .unwrap();
    }
    assert_eq!(q.counts().await.unwrap().delayed, 3);

    let removed = backend.clean(q.name(), JobState::Delayed, 0).await.unwrap();
    assert_eq!(removed, 3);
    assert_eq!(q.counts().await.unwrap().delayed, 0);

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_returns_zero_when_state_is_empty() {
    let (q, backend) = common::fresh_queue::<T>("clean-empty").await;
    let removed = backend.clean(q.name(), JobState::Completed, 0).await.unwrap();
    assert_eq!(removed, 0);
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_rejects_waiting_paused_active() {
    let (_q, backend) = common::fresh_queue::<T>("clean-bad-states").await;
    for s in [JobState::Waiting, JobState::Paused, JobState::Active] {
        let err = backend.clean("anything", s, 0).await.unwrap_err();
        assert!(
            matches!(err, Error::Config(_)),
            "expected Config error for state {s}, got {err:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_emits_cleaned_event_on_event_stream() {
    use futures::StreamExt;

    let (q, backend) = common::fresh_queue::<T>("clean-emits").await;

    // Get one job into `completed` first.
    q.add(T, JobOptions::new()).await.unwrap();
    let worker = Worker::builder(q.clone(), |_: Job<T>| async { Ok::<_, Error>(()) })
        .concurrency(1)
        .drain_delay(Duration::from_millis(50))
        .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());
    common::wait_for(Duration::from_secs(5), || {
        let q = q.clone();
        async move { q.counts().await.map(|c| c.completed == 1).unwrap_or(false) }
    })
    .await;
    cancel.cancel();
    h.await.unwrap().unwrap();

    // Subscribe BEFORE issuing the clean so we don't miss the event.
    let events = q.events();
    let reader = tokio::spawn(async move {
        let mut stream = events.stream().await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
                Ok(Some(Ok(Event::Cleaned { count }))) => return Some(count),
                Ok(Some(Ok(_))) => continue,
                _ => break,
            }
        }
        None
    });
    tokio::time::sleep(Duration::from_millis(150)).await; // let stream attach

    let removed = backend.clean(q.name(), JobState::Completed, 0).await.unwrap();
    assert_eq!(removed, 1);

    let count = reader.await.unwrap();
    assert_eq!(count, Some(1), "Event::Cleaned with count=1 should have arrived");

    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_all_wakes_up_a_blocked_worker() {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let (q, backend) = common::fresh_queue::<T>("promote-wakes-worker").await;

    // Push a couple of far-future delayed jobs.
    for _ in 0..2u32 {
        q.add(T, JobOptions::new().delay(Duration::from_secs(86400)))
            .await
            .unwrap();
    }

    let processed = Arc::new(AtomicU32::new(0));
    let p = processed.clone();
    let worker = Worker::builder(q.clone(), move |_: Job<T>| {
        let p = p.clone();
        async move {
            p.fetch_add(1, Ordering::SeqCst);
            Ok::<_, Error>(())
        }
    })
    .concurrency(2)
    .drain_delay(Duration::from_secs(2)) // worker spends most time blocked
    .build();
    let cancel = worker.cancellation_token();
    let h = tokio::spawn(worker.run());

    // Worker is now blocking on BZPOPMIN(marker). Promote should bump the
    // marker zset and wake it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    backend.promote_all(q.name()).await.unwrap();

    let ok = common::wait_for(Duration::from_secs(5), || {
        let p = processed.clone();
        async move { p.load(Ordering::SeqCst) == 2 }
    })
    .await;
    assert!(
        ok,
        "worker did not pick up promoted jobs: processed={}, counts={:?}",
        processed.load(Ordering::SeqCst),
        q.counts().await.unwrap()
    );

    cancel.cancel();
    h.await.unwrap().unwrap();
    common::teardown(&q).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn clean_one_queue_does_not_touch_another() {
    let (a, backend) = common::fresh_queue::<T>("clean-iso-a").await;
    // Build a sibling queue on the SAME backend so we can check isolation.
    let b_name = common::unique_queue_name("clean-iso-b");
    let b: oxn::Queue<T> = common::queue(backend.clone(), &b_name).await;

    // Seed both queues with 2 completed jobs each.
    for q in [&a, &b] {
        for _ in 0..2u32 {
            q.add(T, JobOptions::new()).await.unwrap();
        }
        let worker =
            Worker::builder(q.clone(), |_: Job<T>| async { Ok::<_, Error>(()) })
                .concurrency(1)
                .drain_delay(Duration::from_millis(50))
                .build();
        let c = worker.cancellation_token();
        let h = tokio::spawn(worker.run());
        common::wait_for(Duration::from_secs(5), || {
            let q = q.clone();
            async move { q.counts().await.map(|c| c.completed == 2).unwrap_or(false) }
        })
        .await;
        c.cancel();
        h.await.unwrap().unwrap();
    }
    assert_eq!(a.counts().await.unwrap().completed, 2);
    assert_eq!(b.counts().await.unwrap().completed, 2);

    backend.clean(a.name(), JobState::Completed, 0).await.unwrap();

    assert_eq!(a.counts().await.unwrap().completed, 0);
    assert_eq!(
        b.counts().await.unwrap().completed,
        2,
        "cleaning queue A must not touch queue B"
    );

    common::teardown(&a).await;
    common::teardown(&b).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn promote_all_appends_alongside_existing_waiting_jobs() {
    let (q, backend) = common::fresh_queue::<T>("promote-mixed").await;

    // 2 delayed + 3 already-waiting.
    q.add(T, JobOptions::new().delay(Duration::from_secs(60)))
        .await
        .unwrap();
    q.add(T, JobOptions::new().delay(Duration::from_secs(60)))
        .await
        .unwrap();
    for _ in 0..3u32 {
        q.add(T, JobOptions::new()).await.unwrap();
    }
    assert_eq!(q.counts().await.unwrap().waiting, 3);
    assert_eq!(q.counts().await.unwrap().delayed, 2);

    let promoted = backend.promote_all(q.name()).await.unwrap();
    assert_eq!(promoted, 2);

    let c = q.counts().await.unwrap();
    assert_eq!(c.delayed, 0);
    assert_eq!(c.waiting, 5, "promoted jobs should be added, not replace");

    common::teardown(&q).await;
}
