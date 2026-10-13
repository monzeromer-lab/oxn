//! Point-in-time dump of everything a queue owns in Redis.
//!
//! A [`Snapshot`] is read with a dedicated connection (never oxn's pool),
//! so taking one can't disturb the code under test beyond the extra reads.
//! Every key under `{prefix}:{queue}:` is classified; anything that isn't
//! a known queue-level key, a job hash, a job auxiliary key or a dedup
//! pointer lands in [`Snapshot::unknown_keys`] for the invariant checker.

use std::collections::{BTreeMap, BTreeSet};

use redis::aio::MultiplexedConnection;
use serde::Serialize;

/// List-backed containers, in display order.
pub const LIST_CONTAINERS: &[&str] = &["wait", "paused", "active"];
/// Zset-backed containers that hold jobs, in display order.
pub const ZSET_CONTAINERS: &[&str] = &[
    "delayed",
    "prioritized",
    "waiting-children",
    "completed",
    "failed",
];
/// Every container a job can live in.
pub fn job_containers() -> impl Iterator<Item = &'static str> {
    LIST_CONTAINERS.iter().chain(ZSET_CONTAINERS).copied()
}

/// Queue-level keys that are not job containers.
pub const QUEUE_KEYS: &[&str] = &[
    "stalled",
    "stalled-check",
    "meta",
    "events",
    "marker",
    "id",
    "pc",
    "limiter",
];

/// Suffixes of per-job auxiliary keys (`{prefix}:{queue}:{id}:{suffix}`).
pub const AUX_SUFFIXES: &[&str] = &[
    "lock",
    "logs",
    "dependencies",
    "processed",
    "unsuccessful",
    "failed",
];

/// One job hash plus the keys hanging off it.
#[derive(Debug, Clone, Default, Serialize)]
pub struct JobSnap {
    /// Raw hash fields.
    pub fields: BTreeMap<String, String>,
    /// `(token, pttl_ms)` of the `:lock` key, when present.
    pub lock: Option<(String, i64)>,
    /// Auxiliary key suffixes present for this job (`logs`, `lock`, …).
    pub aux: BTreeSet<String>,
}

impl JobSnap {
    /// Parsed `attemptsMade`, 0 when absent.
    pub fn attempts_made(&self) -> u32 {
        self.num("attemptsMade") as u32
    }
    /// Parsed `stalledCounter`, 0 when absent.
    pub fn stalled_counter(&self) -> u32 {
        self.num("stalledCounter") as u32
    }
    /// `opts.attempts` from the stored options JSON, 1 when absent.
    pub fn attempts_allowed(&self) -> u32 {
        self.fields
            .get("opts")
            .and_then(|o| serde_json::from_str::<serde_json::Value>(o).ok())
            .and_then(|v| v.get("attempts").and_then(|a| a.as_u64()))
            .unwrap_or(1) as u32
    }
    fn num(&self, k: &str) -> i64 {
        self.fields.get(k).and_then(|v| v.parse().ok()).unwrap_or(0)
    }
}

/// Everything under one queue at one instant.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Snapshot {
    /// Key prefix.
    pub prefix: String,
    /// Queue name.
    pub queue: String,
    /// Wall-clock epoch-ms when the snapshot was taken.
    pub at_ms: i64,
    /// `wait` / `paused` / `active` contents, left (head) to right.
    pub lists: BTreeMap<String, Vec<String>>,
    /// Job zsets plus `marker`, ascending by score.
    pub zsets: BTreeMap<String, Vec<(String, f64)>>,
    /// Members of the `stalled` set.
    pub stalled: BTreeSet<String>,
    /// The `meta` hash.
    pub meta: BTreeMap<String, String>,
    /// Job hashes by id.
    pub jobs: BTreeMap<String, JobSnap>,
    /// Aux keys whose job hash does not exist: `(id, suffix)`.
    pub orphan_aux: Vec<(String, String)>,
    /// Dedup pointers: dedup id → job id.
    pub dedup: BTreeMap<String, String>,
    /// Keys under the queue that fit no known pattern: `(key, type)`.
    pub unknown_keys: Vec<(String, String)>,
    /// `XLEN` of the events stream.
    pub stream_len: u64,
    /// Total keys under the queue.
    pub key_count: usize,
}

/// Reads every key matching `ARGV[1]` (a glob) **atomically** — Redis
/// runs the script to completion without interleaving other clients, so
/// the snapshot can never observe a job half-way through a transition.
///
/// Returns `{ key, type, payload }` triples. Uses `KEYS`, which is O(N) in
/// the keyspace: fine for tests, keep it off production databases.
const SNAPSHOT_LUA: &str = r#"
local out = {}
for _, k in ipairs(redis.call('KEYS', ARGV[1])) do
  local t = redis.call('TYPE', k)['ok']
  local p
  if t == 'list' then p = redis.call('LRANGE', k, 0, -1)
  elseif t == 'zset' then p = redis.call('ZRANGE', k, 0, -1, 'WITHSCORES')
  elseif t == 'set' then p = redis.call('SMEMBERS', k)
  elseif t == 'hash' then p = redis.call('HGETALL', k)
  elseif t == 'string' then p = { redis.call('GET', k), tostring(redis.call('PTTL', k)) }
  elseif t == 'stream' then p = { tostring(redis.call('XLEN', k)) }
  else p = {} end
  table.insert(out, { k, t, p })
end
return out
"#;

impl Snapshot {
    /// Read the full state of `queue` in one atomic server-side step.
    pub async fn take(
        conn: &mut MultiplexedConnection,
        prefix: &str,
        queue: &str,
    ) -> redis::RedisResult<Self> {
        let base = format!("{prefix}:{queue}:");
        let raw: Vec<(String, String, Vec<String>)> = redis::Script::new(SNAPSHOT_LUA)
            .arg(format!("{}*", glob_escape(&base)))
            .invoke_async(conn)
            .await?;
        let mut snap = Snapshot {
            prefix: prefix.to_string(),
            queue: queue.to_string(),
            at_ms: now_ms(),
            key_count: raw.len(),
            ..Default::default()
        };
        for name in LIST_CONTAINERS {
            snap.lists.insert((*name).to_string(), Vec::new());
        }
        for name in ZSET_CONTAINERS.iter().chain(["marker"].iter()) {
            snap.zsets.insert((*name).to_string(), Vec::new());
        }

        let mut aux = Vec::new();
        for (key, ty, payload) in raw {
            let suffix = key[base.len()..].to_string();
            let is_list_container = LIST_CONTAINERS.contains(&suffix.as_str());
            let is_zset_container =
                ZSET_CONTAINERS.contains(&suffix.as_str()) || suffix == "marker";
            match (suffix.as_str(), ty.as_str()) {
                (_, "list") if is_list_container => {
                    snap.lists.insert(suffix, payload);
                }
                (_, "zset") if is_zset_container => {
                    snap.zsets.insert(suffix, pairs_scored(payload));
                }
                ("stalled", "set") => snap.stalled = payload.into_iter().collect(),
                ("meta", "hash") => snap.meta = pairs(payload),
                ("events", "stream") => {
                    snap.stream_len = payload.first().and_then(|n| n.parse().ok()).unwrap_or(0);
                }
                ("stalled-check" | "id" | "pc" | "limiter", "string") => {}
                (s, "string") if s.starts_with("de:") => {
                    if let Some(job) = payload.into_iter().next() {
                        snap.dedup.insert(s[3..].to_string(), job);
                    }
                }
                (s, _)
                    if s.rsplit_once(':')
                        .is_some_and(|(_, x)| AUX_SUFFIXES.contains(&x)) =>
                {
                    let (id, sfx) = s.rsplit_once(':').unwrap_or_default();
                    aux.push((id.to_string(), sfx.to_string(), payload));
                }
                (s, "hash") if !job_containers().any(|c| c == s) && !QUEUE_KEYS.contains(&s) => {
                    snap.jobs.insert(
                        suffix.clone(),
                        JobSnap {
                            fields: pairs(payload),
                            ..Default::default()
                        },
                    );
                }
                _ => snap.unknown_keys.push((key.clone(), ty)),
            }
        }
        for (id, sfx, payload) in aux {
            match snap.jobs.get_mut(&id) {
                Some(job) => {
                    if sfx == "lock" {
                        let mut it = payload.into_iter();
                        if let (Some(token), Some(pttl)) = (it.next(), it.next()) {
                            job.lock = Some((token, pttl.parse().unwrap_or(-1)));
                        }
                    }
                    job.aux.insert(sfx);
                }
                None => snap.orphan_aux.push((id, sfx)),
            }
        }
        Ok(snap)
    }

    /// Containers (from [`job_containers`]) that contain `id`, with
    /// repetition if a list holds it more than once.
    pub fn containers_of(&self, id: &str) -> Vec<&'static str> {
        let mut out = Vec::new();
        for name in LIST_CONTAINERS {
            for x in self.lists.get(*name).into_iter().flatten() {
                if x == id {
                    out.push(*name);
                }
            }
        }
        for name in ZSET_CONTAINERS {
            if self
                .zsets
                .get(*name)
                .is_some_and(|z| z.iter().any(|(m, _)| m == id))
            {
                out.push(*name);
            }
        }
        out
    }

    /// Number of entries in a container (list or zset) by name.
    pub fn len(&self, container: &str) -> usize {
        self.lists
            .get(container)
            .map(Vec::len)
            .or_else(|| self.zsets.get(container).map(Vec::len))
            .unwrap_or(0)
    }

    /// Ids held in any job container.
    pub fn container_ids(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for name in LIST_CONTAINERS {
            out.extend(self.lists.get(*name).into_iter().flatten().cloned());
        }
        for name in ZSET_CONTAINERS {
            out.extend(
                self.zsets
                    .get(*name)
                    .into_iter()
                    .flatten()
                    .map(|(m, _)| m.clone()),
            );
        }
        out
    }

    /// `wait=3 active=1 completed=2 · jobs=6 keys=14`, omitting empties.
    pub fn summary(&self) -> String {
        let mut parts: Vec<String> = job_containers()
            .filter_map(|c| {
                let n = self.len(c);
                (n > 0).then(|| format!("{c}={n}"))
            })
            .collect();
        if parts.is_empty() {
            parts.push("(all containers empty)".into());
        }
        let marker = self.len("marker");
        let paused = if self.meta.get("paused").map(String::as_str) == Some("1") {
            " PAUSED"
        } else {
            ""
        };
        format!(
            "{}{paused} · jobs={} keys={} marker={marker} stream={}",
            parts.join(" "),
            self.jobs.len(),
            self.key_count,
            self.stream_len
        )
    }

    /// Where a job currently is, for per-job history: `completed`,
    /// `wait+completed` (a violation), or `(no container)`.
    pub fn location(&self, id: &str) -> String {
        let c = self.containers_of(id);
        match (c.is_empty(), self.jobs.contains_key(id)) {
            (true, true) => "(hash exists, in no container)".into(),
            (true, false) => "(gone)".into(),
            (false, _) => c.join("+"),
        }
    }
}

fn glob_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if matches!(c, '*' | '?' | '[' | ']' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn pairs(v: Vec<String>) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut it = v.into_iter();
    while let (Some(k), Some(val)) = (it.next(), it.next()) {
        out.insert(k, val);
    }
    out
}

fn pairs_scored(v: Vec<String>) -> Vec<(String, f64)> {
    let mut out = Vec::with_capacity(v.len() / 2);
    let mut it = v.into_iter();
    while let (Some(m), Some(score)) = (it.next(), it.next()) {
        out.push((m, score.parse().unwrap_or(f64::NAN)));
    }
    out
}
