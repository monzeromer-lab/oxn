//! Compiled Lua scripts.
//!
//! Each script is loaded once (content-addressed via `EVALSHA`) and
//! re-invoked on every call. Source is embedded with `include_str!` so the
//! binary ships with no on-disk Lua dependency.

use redis::Script;

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
            add_job: Script::new(include_str!("lua/add_job.lua")),
            move_to_active: Script::new(include_str!("lua/move_to_active.lua")),
            move_to_finished: Script::new(include_str!("lua/move_to_finished.lua")),
            move_to_delayed: Script::new(include_str!("lua/move_to_delayed.lua")),
            extend_lock: Script::new(include_str!("lua/extend_lock.lua")),
            scan_stalled: Script::new(include_str!("lua/scan_stalled.lua")),
            pause: Script::new(include_str!("lua/pause.lua")),
            promote: Script::new(include_str!("lua/promote.lua")),
            retry: Script::new(include_str!("lua/retry.lua")),
            remove: Script::new(include_str!("lua/remove.lua")),
        }
    }
}
