//! Fault injection: what can go wrong, on a schedule.
//!
//! Network faults are injected by [`Proxy`], a tiny TCP forwarder that sits
//! between oxn and Redis when the schedule contains `latency`, `blackhole`
//! or `drop-conns`. Process faults (`kill-worker`) need `--processes true`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

/// One injectable fault.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "kebab-case")]
pub enum ChaosAction {
    /// SIGKILL one random worker process (respawned after 1s).
    KillWorker,
    /// Cancel every worker gracefully and start fresh ones.
    RestartWorkers,
    /// `CLIENT KILL` this fraction of the server's other clients.
    ClientKill { fraction: f64 },
    /// `SCRIPT FLUSH` — every cached Lua script disappears.
    ScriptFlush,
    /// Run `--redis-restart-cmd`.
    RedisRestart,
    /// Add `delay_ms` to every chunk through the proxy for `for_ms`.
    Latency { delay_ms: u64, for_ms: u64 },
    /// Close every proxied connection right now.
    DropConns,
    /// Stop forwarding bytes (connections stay open) for `for_ms`.
    Blackhole { for_ms: u64 },
}

impl ChaosAction {
    /// Parse `kill-worker`, `client-kill=0.5`, `latency=200ms/5s`, …
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        let (name, arg) = match s.split_once('=') {
            Some((n, a)) => (n, Some(a)),
            None => (s, None),
        };
        let dur = |x: &str| crate::config::parse_duration(x).map_err(anyhow::Error::msg);
        Ok(match (name, arg) {
            ("kill-worker", None) => Self::KillWorker,
            ("restart-workers", None) => Self::RestartWorkers,
            ("client-kill", a) => Self::ClientKill {
                fraction: a.map(str::parse).transpose()?.unwrap_or(0.5),
            },
            ("script-flush", None) => Self::ScriptFlush,
            ("redis-restart", None) => Self::RedisRestart,
            ("latency", Some(a)) => {
                let (d, f) = a.split_once('/').unwrap_or((a, "5s"));
                Self::Latency {
                    delay_ms: dur(d)?.as_millis() as u64,
                    for_ms: dur(f)?.as_millis() as u64,
                }
            }
            ("drop-conns", None) => Self::DropConns,
            ("blackhole", a) => Self::Blackhole {
                for_ms: dur(a.unwrap_or("2s"))?.as_millis() as u64,
            },
            _ => anyhow::bail!("unknown chaos action `{s}`"),
        })
    }

    /// Whether this action needs traffic to flow through the proxy.
    pub fn needs_proxy(&self) -> bool {
        matches!(
            self,
            Self::Latency { .. } | Self::DropConns | Self::Blackhole { .. }
        )
    }

    /// Whether this action disrupts the Redis server for everyone.
    pub fn is_server_wide(&self) -> bool {
        matches!(
            self,
            Self::ClientKill { .. } | Self::ScriptFlush | Self::RedisRestart
        )
    }
}

/// TCP forwarder with injectable latency, blackholes and connection drops.
#[derive(Debug, Clone)]
pub struct Proxy {
    /// `host:port` oxn should connect to.
    pub listen_addr: String,
    latency_ms: Arc<AtomicU64>,
    blackhole_until_ms: Arc<AtomicU64>,
    generation: Arc<Mutex<CancellationToken>>,
    /// Connections accepted so far.
    pub accepted: Arc<AtomicU64>,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Proxy {
    /// Bind on an ephemeral local port and forward to `upstream` (`host:port`).
    pub async fn start(upstream: String) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let listen_addr = listener.local_addr()?.to_string();
        let p = Proxy {
            listen_addr,
            latency_ms: Arc::new(AtomicU64::new(0)),
            blackhole_until_ms: Arc::new(AtomicU64::new(0)),
            generation: Arc::new(Mutex::new(CancellationToken::new())),
            accepted: Arc::new(AtomicU64::new(0)),
        };
        let me = p.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    break;
                };
                me.accepted.fetch_add(1, Ordering::Relaxed);
                let upstream = upstream.clone();
                let me = me.clone();
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(&upstream).await else {
                        return;
                    };
                    let _ = client.set_nodelay(true);
                    let _ = server.set_nodelay(true);
                    let gen_token = me.generation.lock().clone();
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    let a = me.pump(cr, sw);
                    let b = me.pump(sr, cw);
                    tokio::select! {
                        _ = gen_token.cancelled() => {}
                        _ = a => {}
                        _ = b => {}
                    }
                });
            }
        });
        Ok(p)
    }

    async fn pump(
        &self,
        mut from: tokio::net::tcp::OwnedReadHalf,
        mut to: tokio::net::tcp::OwnedWriteHalf,
    ) {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = match from.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            loop {
                let until = self.blackhole_until_ms.load(Ordering::Relaxed);
                let now = now_ms();
                if until <= now {
                    break;
                }
                tokio::time::sleep(Duration::from_millis((until - now).min(50))).await;
            }
            let lat = self.latency_ms.load(Ordering::Relaxed);
            if lat > 0 {
                tokio::time::sleep(Duration::from_millis(lat)).await;
            }
            if to.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
    }

    /// Add `delay` to each forwarded chunk for `for_`, then restore.
    pub fn latency(&self, delay: Duration, for_: Duration) {
        self.latency_ms
            .store(delay.as_millis() as u64, Ordering::Relaxed);
        let l = self.latency_ms.clone();
        tokio::spawn(async move {
            tokio::time::sleep(for_).await;
            l.store(0, Ordering::Relaxed);
        });
    }

    /// Freeze forwarding for `for_`.
    pub fn blackhole(&self, for_: Duration) {
        self.blackhole_until_ms
            .store(now_ms() + for_.as_millis() as u64, Ordering::Relaxed);
    }

    /// Close every live proxied connection; new ones still work.
    pub fn drop_all(&self) {
        let mut g = self.generation.lock();
        g.cancel();
        *g = CancellationToken::new();
    }
}

/// `redis://user:pw@host:port/db` → (`host:port`, a closure that rebuilds
/// the URL for another `host:port`).
pub fn split_redis_url(url: &str) -> anyhow::Result<(String, impl Fn(&str) -> String + use<>)> {
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow::anyhow!("bad redis url `{url}`"))?;
    if scheme != "redis" {
        anyhow::bail!("the chaos proxy only supports plain redis:// (got {scheme}://)");
    }
    let (auth, hostpart) = match rest.rsplit_once('@') {
        Some((a, h)) => (format!("{a}@"), h.to_string()),
        None => (String::new(), rest.to_string()),
    };
    let (hostport, path) = match hostpart.split_once('/') {
        Some((h, p)) => (h.to_string(), format!("/{p}")),
        None => (hostpart.clone(), String::new()),
    };
    let hostport = if hostport.contains(':') {
        hostport
    } else {
        format!("{hostport}:6379")
    };
    Ok((hostport, move |hp: &str| {
        format!("redis://{auth}{hp}{path}")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_actions() {
        assert_eq!(
            ChaosAction::parse("kill-worker").unwrap(),
            ChaosAction::KillWorker
        );
        assert_eq!(
            ChaosAction::parse("latency=200ms/3s").unwrap(),
            ChaosAction::Latency {
                delay_ms: 200,
                for_ms: 3000
            }
        );
        assert_eq!(
            ChaosAction::parse("client-kill=0.25").unwrap(),
            ChaosAction::ClientKill { fraction: 0.25 }
        );
        assert_eq!(
            ChaosAction::parse("blackhole").unwrap(),
            ChaosAction::Blackhole { for_ms: 2000 }
        );
        assert!(ChaosAction::parse("explode").is_err());
    }

    #[test]
    fn url_rewrite() {
        let (hp, re) = split_redis_url("redis://:pw@10.0.0.1:6390/3").unwrap();
        assert_eq!(hp, "10.0.0.1:6390");
        assert_eq!(re("127.0.0.1:1"), "redis://:pw@127.0.0.1:1/3");
        let (hp, _) = split_redis_url("redis://localhost").unwrap();
        assert_eq!(hp, "localhost:6379");
        assert!(split_redis_url("rediss://h:1").is_err());
    }

    #[tokio::test]
    async fn proxy_forwards_delays_and_drops() {
        // Echo server stands in for Redis.
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = echo.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut b = [0u8; 1024];
                    while let Ok(n) = s.read(&mut b).await {
                        if n == 0 || s.write_all(&b[..n]).await.is_err() {
                            break;
                        }
                    }
                });
            }
        });
        let p = Proxy::start(echo_addr).await.unwrap();
        let mut c = TcpStream::connect(&p.listen_addr).await.unwrap();
        let mut b = [0u8; 4];
        c.write_all(b"ping").await.unwrap();
        c.read_exact(&mut b).await.unwrap();
        assert_eq!(&b, b"ping");

        p.latency(Duration::from_millis(100), Duration::from_secs(5));
        let t = std::time::Instant::now();
        c.write_all(b"pong").await.unwrap();
        c.read_exact(&mut b).await.unwrap();
        assert!(
            t.elapsed() >= Duration::from_millis(200),
            "latency applied both ways"
        );

        p.drop_all();
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = c.write_all(b"xxxx").await;
        let r = c.read(&mut b).await;
        assert!(
            matches!(r, Ok(0) | Err(_)),
            "connection closed after drop_all"
        );
    }
}
