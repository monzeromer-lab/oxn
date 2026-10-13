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

/// Every script body is `includes/common.lua` (shared helpers such as
/// `oxnWake`) followed by the script itself, assembled at compile time.
macro_rules! script {
    ($name:literal) => {
        (
            $name,
            concat!(
                include_str!("lua/includes/common.lua"),
                "\n",
                include_str!(concat!("lua/", $name, ".lua"))
            ),
        )
    };
}

/// Source-of-truth table of every Lua script we ship, as `(name, body)`.
///
/// Indexes line up with the field declarations in [`Scripts`]. Anything
/// added here must be added to `Scripts::new` in the same position. The
/// name is what tracing spans and test tooling report instead of the SHA.
pub(super) const ALL_SCRIPTS: &[(&str, &str)] = &[
    script!("add_job"),
    script!("move_to_active"),
    script!("move_to_finished"),
    script!("move_to_delayed"),
    script!("extend_lock"),
    script!("scan_stalled"),
    script!("pause"),
    script!("promote"),
    script!("retry"),
    script!("remove"),
    script!("clean"),
    script!("promote_all"),
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
    pub clean: Script,
    pub promote_all: Script,
}

impl Scripts {
    pub fn new() -> Self {
        let s = |i: usize| Script::new(ALL_SCRIPTS[i].1);
        Self {
            add_job: s(0),
            move_to_active: s(1),
            move_to_finished: s(2),
            move_to_delayed: s(3),
            extend_lock: s(4),
            scan_stalled: s(5),
            pause: s(6),
            promote: s(7),
            retry: s(8),
            remove: s(9),
            clean: s(10),
            promote_all: s(11),
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
        for (_, body) in ALL_SCRIPTS {
            let _: String = redis::cmd("SCRIPT")
                .arg("LOAD")
                .arg(*body)
                .query_async(conn)
                .await?;
        }
        Ok(())
    }
}

/// `(name, sha1)` for every embedded Lua script.
///
/// Lets tooling that watches Redis (`MONITOR`, slowlog, the test kit's
/// command tap) translate `EVALSHA <sha>` back into a readable script name.
pub fn script_hashes() -> Vec<(&'static str, String)> {
    ALL_SCRIPTS
        .iter()
        .map(|(name, body)| (*name, Script::new(body).get_hash().to_string()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_script_carries_the_common_prelude() {
        for (name, body) in ALL_SCRIPTS {
            assert!(
                body.starts_with("-- common.lua"),
                "{name} lacks the prelude"
            );
            assert!(
                body.contains(&format!("-- {name}.lua")) || body.contains(&format!("-- {name}")),
                "{name} body missing"
            );
        }
    }

    #[test]
    fn script_names_are_unique_and_hashes_stable() {
        let hashes = script_hashes();
        assert_eq!(hashes.len(), ALL_SCRIPTS.len());
        let mut names: Vec<_> = hashes.iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), ALL_SCRIPTS.len(), "duplicate script name");
        for (_, sha) in &hashes {
            assert_eq!(sha.len(), 40);
        }
    }
}
