//! Compiled Lua scripts.
//!
//! Each script is loaded once (content-addressed via `EVALSHA`) and
//! re-invoked on every call. Source is embedded with `include_str!` so the
//! binary ships with no on-disk Lua dependency.
//!
//! ## Why eager preload matters
//!
//! `redis::Script::invoke_async` is lazy: on the first call it tries
//! `EVALSHA`, gets `NOSCRIPT`, then runs `SCRIPT LOAD` and retries. Against
//! a managed Redis sitting behind a TLS-terminating proxy
//! (DigitalOcean Managed Redis, AWS ElastiCache Serverless, Upstash, …)
//! that one-time `SCRIPT LOAD` round-trip can stall for **seconds** —
//! enough to make the first `Queue::add()` of a fresh process visibly hang.
//!
//! [`Scripts::preload`] eagerly issues `SCRIPT LOAD` for every embedded
//! script. [`super::RedisBackend::connect`] calls it automatically so the
//! cost is paid during application boot, not on the first user-facing add.

use redis::Script;

use crate::error::Result;

/// Source-of-truth array of every Lua script body we ship.
///
/// Indexes line up with the field declarations in [`Scripts`]. Anything
/// added here must be added to `Scripts::new` in the same position.
const ALL_SCRIPTS: &[&str] = &[
    include_str!("lua/add_job.lua"),
    include_str!("lua/move_to_active.lua"),
    include_str!("lua/move_to_finished.lua"),
    include_str!("lua/move_to_delayed.lua"),
    include_str!("lua/extend_lock.lua"),
    include_str!("lua/scan_stalled.lua"),
    include_str!("lua/pause.lua"),
    include_str!("lua/promote.lua"),
    include_str!("lua/retry.lua"),
    include_str!("lua/remove.lua"),
];

pub(super) struct Scripts {
    pub add_job: Script,
    pub move_to_active: Script,
    pub move_to_finished: Script,
    pub move_to_delayed: Script,
    pub extend_lock: Script,
    pub scan_stalled: Script,
    pub pause: Script,
    pub promote: Script,
    pub retry: Script,
    pub remove: Script,
}

impl Scripts {
    pub fn new() -> Self {
        Self {
            add_job: Script::new(ALL_SCRIPTS[0]),
            move_to_active: Script::new(ALL_SCRIPTS[1]),
            move_to_finished: Script::new(ALL_SCRIPTS[2]),
            move_to_delayed: Script::new(ALL_SCRIPTS[3]),
            extend_lock: Script::new(ALL_SCRIPTS[4]),
            scan_stalled: Script::new(ALL_SCRIPTS[5]),
            pause: Script::new(ALL_SCRIPTS[6]),
            promote: Script::new(ALL_SCRIPTS[7]),
            retry: Script::new(ALL_SCRIPTS[8]),
            remove: Script::new(ALL_SCRIPTS[9]),
        }
    }

    /// Issue `SCRIPT LOAD` for every embedded script.
    ///
    /// Idempotent: calling on a Redis that already has the script cached
    /// is a no-op (the SHA matches and Redis just returns it). Safe to
    /// call repeatedly — e.g. after a `SCRIPT FLUSH`.
    pub async fn preload<C>(conn: &mut C) -> Result<()>
    where
        C: redis::aio::ConnectionLike + Send,
    {
        for body in ALL_SCRIPTS {
            let _: String = redis::cmd("SCRIPT")
                .arg("LOAD")
                .arg(*body)
                .query_async(conn)
                .await?;
        }
        Ok(())
    }
}
