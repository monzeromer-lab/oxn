//! Diagnostic harness for managed-Redis performance.
//!
//! Run: `cargo run --release --example diag_managed_redis --features "tls"`
//! with `REDIS_URL=rediss://...` set.
//!
//! Reports: connect time, first add, 9 sequential adds, 16 parallel adds,
//! a fresh connect followed by exactly one add (cold-start scenario from
//! the bug report), and the per-call breakdown of `add` (script call vs.
//! pool acquire).

use std::sync::Arc;
use std::time::{Duration, Instant};

use oxn::backend::redis::{RedisBackend, RedisConfig};
use oxn::backend::Backend;
use oxn::{JobOptions, Queue, QueueOptions};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Dummy {
    payload: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| {
        "redis://127.0.0.1:6379".into()
    });
    let pool_size = std::env::var("POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(16usize);

    println!("== oxn diagnostic ==");
    println!("URL pool_size={pool_size}");
    println!();

    // ── connect() — includes TCP handshake + TLS + warmup
    let t = Instant::now();
    let backend = Arc::new(
        RedisBackend::connect(
            RedisConfig::new(url.clone())
                .pool_size(pool_size)
                .prefix("oxn-diag"),
        )
        .await?,
    );
    let connect_elapsed = t.elapsed();
    println!("connect() (TLS handshakes + pool-wide preload): {connect_elapsed:?}");

    let q: Queue<Dummy> = Queue::from_backend(
        "diag-q",
        backend.clone() as Arc<dyn Backend>,
        QueueOptions::default().prefix("oxn-diag"),
    );

    // Force any lingering pool slots open so we measure script cost only.
    let probe_t = Instant::now();
    let probes = (0..pool_size).map(|_| q.counts());
    let _ = futures::future::join_all(probes).await;
    println!("post-connect counts() x{pool_size}: {:?}", probe_t.elapsed());
    println!();

    // ── 10 sequential adds — first should be hot if warmup worked
    println!("-- sequential adds (each on a fresh pool slot) --");
    for i in 0..10 {
        let t = Instant::now();
        let id = q
            .add(
                Dummy {
                    payload: format!("seq-{i}"),
                },
                JobOptions::new().delay(Duration::from_secs(86400)),
            )
            .await?;
        let elapsed = t.elapsed();
        println!("  add #{i:02}: {elapsed:?}");
        let _ = q.remove(&id).await;
    }
    println!();

    // ── 16 parallel adds — exposes any cold pool slot at once
    println!("-- 16 parallel adds (cold pool exposure) --");
    let t = Instant::now();
    let futs = (0..pool_size).map(|i| {
        let q = q.clone();
        async move {
            let t = Instant::now();
            let id = q
                .add(
                    Dummy {
                        payload: format!("par-{i}"),
                    },
                    JobOptions::new().delay(Duration::from_secs(86400)),
                )
                .await?;
            let dt = t.elapsed();
            let _ = q.remove(&id).await;
            Ok::<_, oxn::Error>((i, dt))
        }
    });
    let results = futures::future::join_all(futs).await;
    let parallel_wall = t.elapsed();
    let mut times: Vec<_> = results
        .into_iter()
        .filter_map(|r| r.ok())
        .collect();
    times.sort_by_key(|(_, d)| *d);
    let min = times.first().map(|(_, d)| *d).unwrap_or_default();
    let max = times.last().map(|(_, d)| *d).unwrap_or_default();
    let median = times
        .get(times.len() / 2)
        .map(|(_, d)| *d)
        .unwrap_or_default();
    println!(
        "  wall: {parallel_wall:?}  min: {min:?}  median: {median:?}  max: {max:?}"
    );
    println!();

    // ── cold-start scenario: brand new backend, time the *very first* add()
    println!("-- cold-start repro (fresh connect → exactly one add) --");
    drop(q);
    drop(backend);

    let t = Instant::now();
    let backend2 = Arc::new(
        RedisBackend::connect(
            RedisConfig::new(url.clone())
                .pool_size(pool_size)
                .prefix("oxn-diag"),
        )
        .await?,
    );
    println!("  fresh connect():            {:?}", t.elapsed());

    let q2: Queue<Dummy> = Queue::from_backend(
        "diag-q",
        backend2.clone() as Arc<dyn Backend>,
        QueueOptions::default().prefix("oxn-diag"),
    );

    let t = Instant::now();
    let id = q2
        .add(
            Dummy {
                payload: "very first".into(),
            },
            JobOptions::new().delay(Duration::from_secs(86400)),
        )
        .await?;
    println!("  very first add():           {:?}", t.elapsed());
    let _ = q2.remove(&id).await;

    let t = Instant::now();
    let id = q2
        .add(
            Dummy {
                payload: "second".into(),
            },
            JobOptions::new().delay(Duration::from_secs(86400)),
        )
        .await?;
    println!("  second add():               {:?}", t.elapsed());
    let _ = q2.remove(&id).await;
    println!();

    // ── isolated SCRIPT LOAD timing — establishes a baseline
    println!("-- raw SCRIPT LOAD timing (no oxn) --");
    let mut conn = redis::Client::open(url.as_str())?
        .get_multiplexed_tokio_connection()
        .await?;
    let body = include_str!("../src/backend/redis/lua/add_job.lua");
    let t = Instant::now();
    let _: String = redis::cmd("SCRIPT")
        .arg("LOAD")
        .arg(body)
        .query_async(&mut conn)
        .await?;
    println!("  one SCRIPT LOAD:            {:?}", t.elapsed());

    let t = Instant::now();
    for _ in 0..10 {
        let _: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(body)
            .query_async(&mut conn)
            .await?;
    }
    println!("  10 sequential SCRIPT LOAD:  {:?}", t.elapsed());

    let t = Instant::now();
    let _: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await?;
    println!("  one PING:                   {:?}", t.elapsed());

    let _ = backend2.obliterate("diag-q", true).await;
    Ok(())
}
