//! Input validation for names that become parts of Redis keys.
//!
//! Every queue's keys are `{prefix}:{queue}:{suffix}` and every job's are
//! `{prefix}:{queue}:{id}[:{aux}]`, so a `:` in a queue name, or a job id
//! that equals a queue-level suffix (`wait`, `meta`, …) or contains `:`
//! (`a:lock`), would silently alias another key.

use crate::error::{Error, Result};

/// Key suffixes owned by the queue itself; no job may be named after one.
pub(crate) const RESERVED_JOB_IDS: &[&str] = &[
    "wait",
    "paused",
    "active",
    "delayed",
    "prioritized",
    "waiting-children",
    "completed",
    "failed",
    "stalled",
    "stalled-check",
    "meta",
    "events",
    "marker",
    "id",
    "pc",
    "limiter",
    "schedulers",
];

/// Longest queue name / explicit job id accepted.
const MAX_LEN: usize = 256;

/// Check a queue name: non-empty, at most 256 bytes, no `:`, no whitespace
/// or control characters.
pub fn queue_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Config("queue name must not be empty".into()));
    }
    if name.len() > MAX_LEN {
        return Err(Error::Config(format!(
            "queue name longer than {MAX_LEN} bytes"
        )));
    }
    if let Some(c) = name
        .chars()
        .find(|c| *c == ':' || c.is_whitespace() || c.is_control())
    {
        return Err(Error::Config(format!(
            "queue name {name:?} contains {c:?}; `:`, whitespace and control characters are not allowed"
        )));
    }
    Ok(())
}

/// Check an explicit job id: non-empty, at most 256 bytes, no `:`, no
/// whitespace or control characters, not all digits (those are reserved
/// for auto-assigned ids), and not one of the queue's own key names.
pub fn job_id(id: &str) -> Result<()> {
    if id.is_empty() {
        return Err(Error::Config("explicit job id must not be empty".into()));
    }
    if id.len() > MAX_LEN {
        return Err(Error::Config(format!("job id longer than {MAX_LEN} bytes")));
    }
    if let Some(c) = id
        .chars()
        .find(|c| *c == ':' || c.is_whitespace() || c.is_control())
    {
        return Err(Error::Config(format!(
            "job id {id:?} contains {c:?}; `:`, whitespace and control characters are not allowed"
        )));
    }
    if id.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Error::Config(format!(
            "job id {id:?} is all digits; numeric ids are reserved for auto-assigned ids"
        )));
    }
    if RESERVED_JOB_IDS.contains(&id) {
        return Err(Error::Config(format!(
            "job id {id:?} is reserved (it names one of the queue's own keys)"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_names() {
        for ok in ["emails", "emails.v2", "tenant-7_reports", "ÜnicodeQ"] {
            assert!(queue_name(ok).is_ok(), "{ok}");
        }
        for bad in ["", "a:b", "has space", "tab\tx", "nl\n", &"x".repeat(257)] {
            assert!(queue_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn job_ids() {
        for ok in ["order-123", "a.b", "user_42", "12a", "waiting", "-1", "1.5"] {
            assert!(job_id(ok).is_ok(), "{ok}");
        }
        for bad in [
            "",
            "a:b",
            "x:lock",
            "123",
            "007",
            "wait",
            "meta",
            "waiting-children",
            "a b",
        ] {
            assert!(job_id(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn reserved_ids_cover_every_queue_level_key() {
        // Keep in sync with KeyBuilder's queue-level suffixes.
        #[cfg(feature = "redis-backend")]
        {
            let k = crate::backend::redis::KeyBuilder::new("p", "q");
            for key in [
                k.wait(),
                k.paused(),
                k.active(),
                k.delayed(),
                k.prioritized(),
                k.waiting_children(),
                k.completed(),
                k.failed(),
                k.stalled(),
                k.stalled_check(),
                k.meta(),
                k.events(),
                k.marker(),
                k.id_counter(),
                k.priority_counter(),
                k.limiter(),
                k.schedulers(),
            ] {
                let suffix = key.strip_prefix("p:q:").unwrap();
                assert!(RESERVED_JOB_IDS.contains(&suffix), "{suffix} not reserved");
            }
        }
    }
}
