//! A TCP proxy in front of Redis that drops a connection at a chosen
//! command — the deterministic way to reproduce "the connection died
//! mid-call".
//!
//! Arm it with a byte pattern (e.g. a script's SHA, which appears in its
//! `EVALSHA`) and a [`Drop`] mode. The next client→server chunk containing
//! the pattern triggers it, once:
//!
//! - [`Drop::BeforeForward`]: the command never reaches Redis.
//! - [`Drop::AfterForward`]: Redis runs it, the client never sees the
//!   reply — the ambiguous case, where a retry must not apply it twice.
//!
//! Either way the connection is closed, so the client sees an I/O error.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Where the armed command's connection is cut.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Drop {
    /// Before the command reaches Redis.
    BeforeForward,
    /// After Redis received (and ran) it, before the reply reaches the
    /// client.
    AfterForward,
}

#[derive(Default)]
struct State {
    armed: Mutex<Vec<(Vec<u8>, Drop)>>,
    fired: AtomicU64,
}

/// The running proxy. Dropping it does not stop the listener task; the
/// process ending does.
#[derive(Clone)]
pub struct FaultProxy {
    /// `127.0.0.1:{port}` to connect to instead of Redis.
    pub addr: String,
    state: Arc<State>,
}

impl std::fmt::Debug for FaultProxy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultProxy")
            .field("addr", &self.addr)
            .finish()
    }
}

impl FaultProxy {
    /// Start proxying to `upstream` (`host:port`).
    pub async fn start(upstream: &str) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?.to_string();
        let state = Arc::new(State::default());
        let (st, up) = (state.clone(), upstream.to_string());
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let (st, up) = (st.clone(), up.clone());
                tokio::spawn(async move {
                    if let Ok(server) = TcpStream::connect(&up).await {
                        pipe(client, server, st).await;
                    }
                });
            }
        });
        Ok(Self { addr, state })
    }

    /// A `redis://` URL through the proxy.
    pub fn url(&self) -> String {
        format!("redis://{}", self.addr)
    }

    /// Cut the connection of the next command containing `pattern`.
    pub fn arm(&self, pattern: impl AsRef<[u8]>, mode: Drop) {
        self.state
            .armed
            .lock()
            .push((pattern.as_ref().to_vec(), mode));
    }

    /// How many armed faults have fired.
    pub fn fired(&self) -> u64 {
        self.state.fired.load(Ordering::SeqCst)
    }
}

async fn pipe(client: TcpStream, server: TcpStream, st: Arc<State>) {
    let (mut cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    let down = tokio::spawn(async move {
        let mut buf = vec![0u8; 64 * 1024];
        while let Ok(n) = sr.read(&mut buf).await {
            if n == 0 || cw.write_all(&buf[..n]).await.is_err() {
                break;
            }
        }
    });
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = match cr.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let chunk = &buf[..n];
        let hit = {
            let mut armed = st.armed.lock();
            let pos = armed
                .iter()
                .position(|(p, _)| chunk.windows(p.len()).any(|w| w == p.as_slice()));
            pos.map(|i| armed.remove(i).1)
        };
        match hit {
            Some(Drop::BeforeForward) => {
                st.fired.fetch_add(1, Ordering::SeqCst);
                break;
            }
            Some(Drop::AfterForward) => {
                st.fired.fetch_add(1, Ordering::SeqCst);
                // Stop relaying replies first, so this one can't slip
                // through; then let Redis run the command.
                down.abort();
                let _ = sw.write_all(chunk).await;
                tokio::time::sleep(Duration::from_millis(30)).await;
                break;
            }
            None => {
                if sw.write_all(chunk).await.is_err() {
                    break;
                }
            }
        }
    }
    down.abort();
}
