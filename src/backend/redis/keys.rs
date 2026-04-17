//! Redis key layout.
//!
//! All queue-scoped keys follow `{prefix}:{queue}:{suffix}`. Per-job keys are
//! `{prefix}:{queue}:{jobId}` with optional `:sub` suffixes. This mirrors
//! BullMQ's scheme so existing tooling can still inspect the DB, but the
//! builder is typed so nothing in Rust-land concatenates strings by hand.

/// Helper for computing queue- and job-scoped Redis keys.
#[derive(Debug, Clone)]
pub struct KeyBuilder {
    prefix: String,
    queue: String,
}

impl KeyBuilder {
    pub fn new(prefix: &str, queue: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            queue: queue.to_string(),
        }
    }

    fn q(&self, suffix: &str) -> String {
        format!("{}:{}:{}", self.prefix, self.queue, suffix)
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn queue(&self) -> &str {
        &self.queue
    }

    pub fn wait(&self) -> String {
        self.q("wait")
    }

    pub fn paused(&self) -> String {
        self.q("paused")
    }

    pub fn active(&self) -> String {
        self.q("active")
    }

    pub fn delayed(&self) -> String {
        self.q("delayed")
    }

    pub fn prioritized(&self) -> String {
        self.q("prioritized")
    }

    pub fn waiting_children(&self) -> String {
        self.q("waiting-children")
    }

    pub fn completed(&self) -> String {
        self.q("completed")
    }

    pub fn failed(&self) -> String {
        self.q("failed")
    }

    pub fn stalled(&self) -> String {
        self.q("stalled")
    }

    pub fn stalled_check(&self) -> String {
        self.q("stalled-check")
    }

    pub fn meta(&self) -> String {
        self.q("meta")
    }

    pub fn events(&self) -> String {
        self.q("events")
    }

    pub fn marker(&self) -> String {
        self.q("marker")
    }

    pub fn id_counter(&self) -> String {
        self.q("id")
    }

    pub fn priority_counter(&self) -> String {
        self.q("pc")
    }

    pub fn limiter(&self) -> String {
        self.q("limiter")
    }

    pub fn dedup(&self, id: &str) -> String {
        self.q(&format!("de:{id}"))
    }

    pub fn job_hash(&self, id: &str) -> String {
        self.q(id)
    }

    pub fn job_hash_prefix(&self) -> String {
        format!("{}:{}:", self.prefix, self.queue)
    }

    pub fn job_lock(&self, id: &str) -> String {
        self.q(&format!("{id}:lock"))
    }

    pub fn job_logs(&self, id: &str) -> String {
        self.q(&format!("{id}:logs"))
    }

    pub fn job_deps(&self, id: &str) -> String {
        self.q(&format!("{id}:dependencies"))
    }

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
