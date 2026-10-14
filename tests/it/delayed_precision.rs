//! B3b — delayed jobs start on time even on an idle Redis.
//!
//! A worker waiting for a delayed job blocks in `BZPOPMIN` with a timeout
//! of exactly the time until it is due. Redis fires blocking timeouts on
//! its own clock tick (`hz`, 10 by default: every 100ms), so on a quiet
//! server the job started up to ~100ms late — and scheduler ticks with it.
//! The worker now also keeps a local timer for the due time and wakes the
//! queue itself when it fires.

use std::sync::Arc;
use std::time::Duration;

use crate::common;
use oxn::prelude::*;
use oxn_testkit::{Recorder, ms, secs};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Due(i64);

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

// Ignored in the parallel suite: it needs an otherwise idle Redis. With 160
// tests hammering the server every command queues (and the bug itself hides:
// traffic makes Redis check timeouts sooner). CI runs it alone:
// `cargo test --test it delayed_precision -- --ignored`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing: needs an otherwise idle Redis; run alone"]
async fn delayed_jobs_start_within_a_few_ms_of_their_due_time() {
    let (q, _b) = common::fresh_queue::<Due>("b3b-precise").await;
    let mut rec = Recorder::new("b3b_delayed_precision", q.name()).await;
    let late = Arc::new(Mutex::new(Vec::new()));
    let l = late.clone();
    let w = Worker::builder(q.clone(), move |j: Job<Due>| {
        let l = l.clone();
        async move {
            l.lock().push(now_ms() - j.data.0);
            Ok::<_, Error>(())
        }
    })
    .build(); // default drain_delay
    let cancel = w.cancellation_token();
    let h = tokio::spawn(w.run());
    tokio::time::sleep(ms(100)).await; // let it block
    // One at a time, at offsets that don't line up with Redis's 100ms tick.
    let mut control = Vec::new();
    for delay in [137u64, 211, 263, 318, 389, 142, 277, 351] {
        let due = now_ms() + delay as i64;
        // Control: a plain local timer for the same instant. On a busy
        // machine every timer runs late; only lateness beyond the
        // control's is oxn's.
        let ctl = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis((due - now_ms()).max(0) as u64)).await;
            now_ms() - due
        });
        rec.step(
            &format!("add job due in {delay}ms"),
            q.add(
                Due(due),
                JobOptions::new().delay(Duration::from_millis(delay)),
            ),
        )
        .await
        .unwrap();
        // Wait locally: polling Redis here would keep it awake and hide
        // the bug (any traffic makes it check timeouts sooner).
        let n = late.lock().len();
        let t = std::time::Instant::now();
        while late.lock().len() == n {
            assert!(t.elapsed() < secs(2), "job due in {delay}ms never ran");
            tokio::time::sleep(ms(2)).await;
        }
        control.push(ctl.await.unwrap());
    }
    cancel.cancel();
    h.await.unwrap().unwrap();
    rec.observe("all ran").await;
    let late = late.lock().clone();
    let excess: Vec<i64> = late.iter().zip(&control).map(|(l, c)| l - c).collect();
    println!("started ms after due: {late:?}; a local timer: {control:?}; beyond it: {excess:?}");
    assert!(late.iter().all(|&l| l >= -1), "ran early: {late:?}");
    // The bug's signature is Redis's 100ms tick: a median around 40-50ms
    // and peaks near 90ms beyond a local timer. Fixed, it is a few ms.
    let mut sorted = excess.clone();
    sorted.sort_unstable();
    let (median, worst) = (sorted[sorted.len() / 2], sorted[sorted.len() - 1]);
    assert!(
        median <= 12,
        "median {median}ms beyond a local timer: {excess:?} (raw {late:?})"
    );
    assert!(
        worst <= 60,
        "up to {worst}ms beyond a local timer: {excess:?} (raw {late:?})"
    );
    rec.assert_clean();
    common::teardown(&q).await;
}
