//! Redis `MONITOR` tap scoped to one queue.
//!
//! `MONITOR` streams every command the server executes, including the
//! `redis.call(...)`s made **inside** Lua scripts (shown with a `[0 lua]`
//! source). Filtering that firehose down to lines mentioning
//! `{prefix}:{queue}:` gives a step-by-step record of exactly which keys
//! each oxn script read and wrote — the closest thing to single-stepping a
//! Lua state transition.
//!
//! Opt-in (it slows Redis down): `Recorder::with_monitor()` or
//! `OXN_MONITOR=1`. Plain `redis://` URLs only.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

/// Cap per tap so a noisy test can't exhaust memory.
const MAX_LINES: usize = 50_000;

/// One command observed by `MONITOR`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Command {
    /// Server timestamp, epoch-µs.
    pub at_us: u64,
    /// `true` when issued from inside a Lua script.
    pub from_lua: bool,
    /// Rendered command, with `EVALSHA <sha>` replaced by the script name
    /// and long arguments truncated.
    pub text: String,
}

/// Background `MONITOR` reader.
#[derive(Debug)]
pub struct MonitorTap {
    lines: Arc<Mutex<Vec<Command>>>,
    task: JoinHandle<()>,
}

impl Drop for MonitorTap {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl MonitorTap {
    /// Connect to `url`, issue `MONITOR`, and keep only lines containing
    /// `needle` (normally `"{prefix}:{queue}:"`).
    pub async fn start(url: &str, needle: String) -> std::io::Result<Self> {
        let target = parse_url(url)?;
        let mut stream = TcpStream::connect((target.host.as_str(), target.port)).await?;
        if let Some(pw) = &target.password {
            let cmd = match &target.user {
                Some(u) => resp(&["AUTH", u, pw]),
                None => resp(&["AUTH", pw]),
            };
            stream.write_all(&cmd).await?;
        }
        if target.db != 0 {
            stream
                .write_all(&resp(&["SELECT", &target.db.to_string()]))
                .await?;
        }
        stream.write_all(&resp(&["MONITOR"])).await?;
        let mut reader = BufReader::new(stream);

        // Consume the +OK replies for AUTH / SELECT / MONITOR.
        let expected_oks = 1 + target.password.is_some() as usize + (target.db != 0) as usize;
        let mut line = String::new();
        for _ in 0..expected_oks {
            line.clear();
            reader.read_line(&mut line).await?;
            if !line.starts_with("+OK") {
                return Err(std::io::Error::other(format!(
                    "MONITOR handshake failed: {}",
                    line.trim()
                )));
            }
        }

        let names: HashMap<String, &'static str> = oxn::backend::redis::script_hashes()
            .into_iter()
            .map(|(n, sha)| (sha, n))
            .collect();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let sink = lines.clone();
        let task = tokio::spawn(async move {
            let mut buf = String::new();
            loop {
                buf.clear();
                match reader.read_line(&mut buf).await {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
                if !buf.contains(&needle) {
                    continue;
                }
                if let Some(cmd) = parse_line(buf.trim_end(), &names) {
                    let mut l = sink.lock();
                    if l.len() < MAX_LINES {
                        l.push(cmd);
                    }
                }
            }
        });
        Ok(Self { lines, task })
    }

    /// Take every command captured since the last drain.
    pub fn drain(&self) -> Vec<Command> {
        std::mem::take(&mut *self.lines.lock())
    }
}

struct Target {
    host: String,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    db: i64,
}

fn parse_url(url: &str) -> std::io::Result<Target> {
    let rest = url
        .strip_prefix("redis://")
        .ok_or_else(|| std::io::Error::other("MONITOR tap supports plain redis:// URLs only"))?;
    let (auth, hostpart) = match rest.rsplit_once('@') {
        Some((a, h)) => (Some(a), h),
        None => (None, rest),
    };
    let (hostport, db) = match hostpart.split_once('/') {
        Some((hp, d)) => (hp, d.parse().unwrap_or(0)),
        None => (hostpart, 0),
    };
    let (host, port) = match hostport.rsplit_once(':') {
        Some((h, p)) => (h.to_string(), p.parse().unwrap_or(6379)),
        None => (hostport.to_string(), 6379),
    };
    let (user, password) = match auth {
        Some(a) => match a.split_once(':') {
            Some((u, p)) => ((!u.is_empty()).then(|| u.to_string()), Some(p.to_string())),
            None => (None, Some(a.to_string())),
        },
        None => (None, None),
    };
    Ok(Target {
        host,
        port,
        user,
        password,
        db,
    })
}

fn resp(parts: &[&str]) -> Vec<u8> {
    let mut out = format!("*{}\r\n", parts.len()).into_bytes();
    for p in parts {
        out.extend_from_slice(format!("${}\r\n{p}\r\n", p.len()).as_bytes());
    }
    out
}

/// `+1700000000.123456 [0 127.0.0.1:5555] "EVALSHA" "ab12…" "2" "k1" …`
fn parse_line(line: &str, names: &HashMap<String, &'static str>) -> Option<Command> {
    let line = line.strip_prefix('+').unwrap_or(line);
    let (ts, rest) = line.split_once(' ')?;
    let at_us = (ts.parse::<f64>().ok()? * 1_000_000.0) as u64;
    let close = rest.find(']')?;
    let source = &rest[..=close];
    let from_lua = source.contains("lua");
    let args = split_quoted(rest[close + 1..].trim());
    let mut rendered: Vec<String> = Vec::with_capacity(args.len());
    let mut i = 0;
    while i < args.len() {
        let a = &args[i];
        if i == 0 && a.eq_ignore_ascii_case("EVALSHA") && args.len() > 1 {
            let name = names.get(&args[1]).copied().unwrap_or("unknown-script");
            rendered.push(format!("EVALSHA<{name}>"));
            i += 2;
            continue;
        }
        if i == 0 && a.eq_ignore_ascii_case("EVAL") && args.len() > 1 {
            rendered.push("EVAL<inline>".into());
            i += 2;
            continue;
        }
        rendered.push(truncate(a, 96));
        i += 1;
    }
    Some(Command {
        at_us,
        from_lua,
        text: rendered.join(" "),
    })
}

fn split_quoted(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        match (c, in_q) {
            ('"', false) => in_q = true,
            ('"', true) => {
                out.push(std::mem::take(&mut cur));
                in_q = false;
            }
            ('\\', true) => {
                if let Some(n) = chars.next() {
                    cur.push(n);
                }
            }
            (c, true) => cur.push(c),
            _ => {}
        }
    }
    out
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let head: String = s.chars().take(max).collect();
    format!("{head}…(+{}b)", s.len() - head.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_url_variants() {
        let t = parse_url("redis://127.0.0.1:6380/2").unwrap();
        assert_eq!((t.host.as_str(), t.port, t.db), ("127.0.0.1", 6380, 2));
        let t = parse_url("redis://:secret@h").unwrap();
        assert_eq!(t.password.as_deref(), Some("secret"));
        assert!(t.user.is_none());
        let t = parse_url("redis://alice:pw@h:1").unwrap();
        assert_eq!(t.user.as_deref(), Some("alice"));
        assert!(parse_url("rediss://h").is_err());
    }

    #[test]
    fn renders_evalsha_with_script_name() {
        let mut names = HashMap::new();
        names.insert("abc".to_string(), "move_to_active");
        let c = parse_line(
            r#"+1700000000.5 [0 127.0.0.1:1] "EVALSHA" "abc" "1" "oxn:q:wait""#,
            &names,
        )
        .unwrap();
        assert_eq!(c.text, "EVALSHA<move_to_active> 1 oxn:q:wait");
        assert!(!c.from_lua);
        assert_eq!(c.at_us, 1_700_000_000_500_000);
        let c = parse_line(r#"+1.0 [0 lua] "LPUSH" "oxn:q:wait" "7""#, &names).unwrap();
        assert!(c.from_lua);
        assert_eq!(c.text, "LPUSH oxn:q:wait 7");
    }

    #[test]
    fn truncates_long_args() {
        let long = "x".repeat(200);
        let t = truncate(&long, 10);
        assert!(t.starts_with("xxxxxxxxxx…"));
    }
}
