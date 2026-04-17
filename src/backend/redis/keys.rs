//! Redis key layout.
//!
//! All queue-scoped keys follow `{prefix}:{queue}:{suffix}`. Per-job keys are
//! `{prefix}:{queue}:{jobId}` with optional `:sub` suffixes. This mirrors
//! BullMQ's scheme so existing tooling can still inspect the DB, but the
//! builder is typed so nothing in Rust-land concatenates strings by hand.

/// Helper for computing queue- and job-scoped Redis keys.
///
/// Most users don't need to touch this directly — it's exposed for anyone
/// writing custom Lua scripts, a sibling Backend, or tooling that reads
/// the Redis layout. Each method returns an owned `String` so it can be
/// handed straight to `redis::cmd(...).arg(...)`.
///
/// # Examples
///
/// ```
/// use oxn::backend::redis::KeyBuilder;
///
/// let k = KeyBuilder::new("oxn", "emails");
/// assert_eq!(k.wait(),       "oxn:emails:wait");
/// assert_eq!(k.job_hash("42"), "oxn:emails:42");
/// assert_eq!(k.job_lock("42"), "oxn:emails:42:lock");
/// ```
#[derive(Debug, Clone)]
pub struct KeyBuilder {
    prefix: String,
    queue: String,
}

impl KeyBuilder {
    /// Build a new key helper for `{prefix}:{queue}`.
    pub fn new(prefix: &str, queue: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            queue: queue.to_string(),
        }
    }

    fn q(&self, suffix: &str) -> String {
        format!("{}:{}:{}", self.prefix, self.queue, suffix)
    }

    /// The prefix passed to [`Self::new`].
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// The queue name passed to [`Self::new`].
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// `{prefix}:{queue}:wait` — FIFO list of ready job ids.
    pub fn wait(&self) -> String {
        self.q("wait")
    }

    /// `{prefix}:{queue}:paused` — where `wait` is renamed to during pauses.
    pub fn paused(&self) -> String {
        self.q("paused")
    }

    /// `{prefix}:{queue}:active` — list of jobs currently being processed.
    pub fn active(&self) -> String {
        self.q("active")
    }

    /// `{prefix}:{queue}:delayed` — zset of future jobs, score = epoch-ms.
    pub fn delayed(&self) -> String {
        self.q("delayed")
    }

    /// `{prefix}:{queue}:prioritized` — zset of prioritized jobs.
    pub fn prioritized(&self) -> String {
        self.q("prioritized")
    }

    /// `{prefix}:{queue}:waiting-children` — parents blocked on children.
    pub fn waiting_children(&self) -> String {
        self.q("waiting-children")
    }

    /// `{prefix}:{queue}:completed` — zset of completed jobs.
    pub fn completed(&self) -> String {
        self.q("completed")
    }

    /// `{prefix}:{queue}:failed` — zset of permanently-failed jobs.
    pub fn failed(&self) -> String {
        self.q("failed")
    }

    /// `{prefix}:{queue}:stalled` — set of recently-stalled jobs.
    pub fn stalled(&self) -> String {
        self.q("stalled")
    }

    /// `{prefix}:{queue}:stalled-check` — rate-limit flag for the scanner.
    pub fn stalled_check(&self) -> String {
        self.q("stalled-check")
    }

    /// `{prefix}:{queue}:meta` — hash of queue-wide config.
    pub fn meta(&self) -> String {
        self.q("meta")
    }

    /// `{prefix}:{queue}:events` — Redis Stream of lifecycle events.
    pub fn events(&self) -> String {
        self.q("events")
    }

    /// `{prefix}:{queue}:marker` — wake-up key for `BZPOPMIN` blocking.
    pub fn marker(&self) -> String {
        self.q("marker")
    }

    /// `{prefix}:{queue}:id` — monotonically-increasing job id counter.
    pub fn id_counter(&self) -> String {
        self.q("id")
    }

    /// `{prefix}:{queue}:pc` — counter breaking priority ties.
    pub fn priority_counter(&self) -> String {
        self.q("pc")
    }

    /// `{prefix}:{queue}:limiter` — global token bucket.
    pub fn limiter(&self) -> String {
        self.q("limiter")
    }

    /// `{prefix}:{queue}:de:{id}` — deduplication pointer.
    pub fn dedup(&self, id: &str) -> String {
        self.q(&format!("de:{id}"))
    }

    /// `{prefix}:{queue}:{id}` — job record hash.
    pub fn job_hash(&self, id: &str) -> String {
        self.q(id)
    }

    /// `{prefix}:{queue}:` — base prefix used by Lua scripts to form job
    /// hashes from the id (`prefix .. id`).
    pub fn job_hash_prefix(&self) -> String {
        format!("{}:{}:", self.prefix, self.queue)
    }

    /// `{prefix}:{queue}:{id}:lock` — lock token string, TTL `lockDuration`.
    pub fn job_lock(&self, id: &str) -> String {
        self.q(&format!("{id}:lock"))
    }

    /// `{prefix}:{queue}:{id}:logs` — list of user-emitted log lines.
    pub fn job_logs(&self, id: &str) -> String {
        self.q(&format!("{id}:logs"))
    }

    /// `{prefix}:{queue}:{id}:dependencies` — set of pending child job keys.
    pub fn job_deps(&self, id: &str) -> String {
        self.q(&format!("{id}:dependencies"))
    }

    /// `{prefix}:{queue}:{id}:processed` — hash of child-key → returnvalue.
    pub fn job_processed(&self, id: &str) -> String {
        self.q(&format!("{id}:processed"))
    }

    /// SCAN pattern matching all keys for this queue (used by `obliterate`).
    pub fn scan_pattern(&self) -> String {
        format!("{}:{}:*", self.prefix, self.queue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_match_layout() {
        let k = KeyBuilder::new("oxn", "emails");
        assert_eq!(k.wait(), "oxn:emails:wait");
        assert_eq!(k.job_hash("42"), "oxn:emails:42");
        assert_eq!(k.job_lock("42"), "oxn:emails:42:lock");
        assert_eq!(k.dedup("abc"), "oxn:emails:de:abc");
        assert_eq!(k.scan_pattern(), "oxn:emails:*");
    }

    #[test]
    fn all_queue_level_keys() {
        let k = KeyBuilder::new("p", "q");
        assert_eq!(k.wait(), "p:q:wait");
        assert_eq!(k.paused(), "p:q:paused");
        assert_eq!(k.active(), "p:q:active");
        assert_eq!(k.delayed(), "p:q:delayed");
        assert_eq!(k.prioritized(), "p:q:prioritized");
        assert_eq!(k.waiting_children(), "p:q:waiting-children");
        assert_eq!(k.completed(), "p:q:completed");
        assert_eq!(k.failed(), "p:q:failed");
        assert_eq!(k.stalled(), "p:q:stalled");
        assert_eq!(k.stalled_check(), "p:q:stalled-check");
        assert_eq!(k.meta(), "p:q:meta");
        assert_eq!(k.events(), "p:q:events");
        assert_eq!(k.marker(), "p:q:marker");
        assert_eq!(k.id_counter(), "p:q:id");
        assert_eq!(k.priority_counter(), "p:q:pc");
        assert_eq!(k.limiter(), "p:q:limiter");
    }

    #[test]
    fn per_job_keys() {
        let k = KeyBuilder::new("p", "q");
        assert_eq!(k.job_hash("7"), "p:q:7");
        assert_eq!(k.job_hash_prefix(), "p:q:");
        assert_eq!(k.job_lock("7"), "p:q:7:lock");
        assert_eq!(k.job_logs("7"), "p:q:7:logs");
        assert_eq!(k.job_deps("7"), "p:q:7:dependencies");
        assert_eq!(k.job_processed("7"), "p:q:7:processed");
    }

    #[test]
    fn prefix_and_queue_are_accessible() {
        let k = KeyBuilder::new("my-prefix", "my-queue");
        assert_eq!(k.prefix(), "my-prefix");
        assert_eq!(k.queue(), "my-queue");
    }
}
