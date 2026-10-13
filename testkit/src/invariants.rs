//! Structural invariants of a queue's Redis state.
//!
//! [`check`] inspects a [`Snapshot`] and returns every rule it breaks. The
//! rules encode what oxn promises — "a job never lives in two containers at
//! once", "nothing leaks", "a lock always belongs to the job's current
//! holder" — so a test can assert them after **every** step instead of only
//! checking the final answer.
//!
//! | Rule | Severity | Meaning |
//! | --- | --- | --- |
//! | `single-container` | error | id appears in more than one container (or twice in one list) |
//! | `dangling-entry` | error | container holds an id with no job hash |
//! | `orphan-hash` | error | job hash exists but is in no container |
//! | `lock-mismatch` | error | active job's lock token ≠ its `lockToken` field |
//! | `lock-expired` | transient | active job has no lock yet (stalled scan pending) |
//! | `orphan-lock` | error | job not in `active` still has a `:lock` key |
//! | `orphan-aux` | error | `:logs` / `:dependencies` / … without a job hash |
//! | `stale-lock-field` | error | non-active job still carries `lockToken` |
//! | `attempts-exceeded` | error | `attemptsMade > attempts + stalledCounter` |
//! | `paused-but-waiting` | error | queue paused yet `wait` is non-empty |
//! | `finished-without-timestamp` | error | completed/failed job lacks `finishedOn` |
//! | `dedup-dangling` | warning | dedup pointer to a job that no longer exists |
//! | `stalled-set-leak` | warning | `stalled` set holds ids that aren't active/waiting |
//! | `unknown-key` | error | key under the queue fits no documented pattern |

use std::fmt;

use serde::Serialize;

use crate::snapshot::Snapshot;

/// How bad a violation is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Severity {
    /// Expected to resolve on its own (e.g. a stall the scanner hasn't
    /// swept yet). Reported, never fails a test.
    Transient,
    /// Suspicious (usually a slow leak). Reported, fails only under
    /// [`crate::Recorder::strict`].
    Warning,
    /// The state is wrong. Fails the test.
    Error,
}

/// One broken rule.
#[derive(Debug, Clone, Serialize)]
pub struct Violation {
    /// Stable rule id (see the module table).
    pub rule: &'static str,
    /// Severity.
    pub severity: Severity,
    /// Job the violation is about, when there is one.
    pub job_id: Option<String>,
    /// Human explanation including the offending values.
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sev = match self.severity {
            Severity::Error => "ERROR",
            Severity::Warning => "warn",
            Severity::Transient => "transient",
        };
        match &self.job_id {
            Some(id) => write!(f, "[{sev}] {} (job {id}): {}", self.rule, self.detail),
            None => write!(f, "[{sev}] {}: {}", self.rule, self.detail),
        }
    }
}

fn v(rule: &'static str, severity: Severity, job: Option<&str>, detail: String) -> Violation {
    Violation {
        rule,
        severity,
        job_id: job.map(str::to_string),
        detail,
    }
}

/// Check every rule against `snap`.
pub fn check(snap: &Snapshot) -> Vec<Violation> {
    use Severity::*;
    let mut out = Vec::new();

    // Membership: each id in exactly one container, and has a hash.
    for id in snap.container_ids() {
        let c = snap.containers_of(&id);
        if c.len() > 1 {
            out.push(v(
                "single-container",
                Error,
                Some(&id),
                format!("present in {}", c.join(" + ")),
            ));
        }
        if !snap.jobs.contains_key(&id) {
            out.push(v(
                "dangling-entry",
                Error,
                Some(&id),
                format!("listed in {} but its job hash does not exist", c.join("+")),
            ));
        }
    }

    for (id, job) in &snap.jobs {
        let containers = snap.containers_of(id);
        let active = containers.contains(&"active");
        if containers.is_empty() {
            out.push(v(
                "orphan-hash",
                Error,
                Some(id),
                format!(
                    "hash exists (fields: {}) but the job is in no container",
                    job.fields.keys().cloned().collect::<Vec<_>>().join(",")
                ),
            ));
        }
        let field_token = job.fields.get("lockToken");
        match (&job.lock, active) {
            (Some((token, _)), true) => {
                if field_token != Some(token) {
                    out.push(v(
                        "lock-mismatch",
                        Error,
                        Some(id),
                        format!("lock key holds {token:?}, hash lockToken={field_token:?}"),
                    ));
                }
            }
            (None, true) => out.push(v(
                "lock-expired",
                Transient,
                Some(id),
                "active without a lock (expired, awaiting stalled scan)".into(),
            )),
            (Some((token, pttl)), false) => out.push(v(
                "orphan-lock",
                Error,
                Some(id),
                format!(
                    "not active (in {}) but lock {token:?} still exists (pttl {pttl}ms)",
                    snap.location(id)
                ),
            )),
            (None, false) => {}
        }
        if !active && field_token.is_some() && !containers.is_empty() {
            out.push(v(
                "stale-lock-field",
                Error,
                Some(id),
                format!(
                    "in {} but hash still has lockToken={:?}",
                    snap.location(id),
                    field_token.unwrap_or(&String::new())
                ),
            ));
        }
        let made = job.attempts_made();
        let allowed = job.attempts_allowed() + job.stalled_counter();
        if made > allowed {
            out.push(v(
                "attempts-exceeded",
                Error,
                Some(id),
                format!(
                    "attemptsMade={made} > attempts={} + stalledCounter={}",
                    job.attempts_allowed(),
                    job.stalled_counter()
                ),
            ));
        }
        if (containers.contains(&"completed") || containers.contains(&"failed"))
            && !job.fields.contains_key("finishedOn")
        {
            out.push(v(
                "finished-without-timestamp",
                Error,
                Some(id),
                format!("in {} without finishedOn", snap.location(id)),
            ));
        }
    }

    for (id, sfx) in &snap.orphan_aux {
        out.push(v(
            "orphan-aux",
            Error,
            Some(id),
            format!("`{id}:{sfx}` exists but the job hash is gone"),
        ));
    }

    if snap.meta.get("paused").map(String::as_str) == Some("1") && snap.len("wait") > 0 {
        out.push(v(
            "paused-but-waiting",
            Error,
            None,
            format!(
                "queue is paused but wait holds {:?}",
                snap.lists.get("wait").cloned().unwrap_or_default()
            ),
        ));
    }

    for (dedup_id, job) in &snap.dedup {
        if !snap.jobs.contains_key(job) {
            out.push(v(
                "dedup-dangling",
                Warning,
                Some(job),
                format!("dedup `{dedup_id}` points at a job that no longer exists"),
            ));
        }
    }

    let live: Vec<&String> = snap
        .stalled
        .iter()
        .filter(|id| {
            let c = snap.containers_of(id);
            !(c.contains(&"active") || c.contains(&"wait") || c.contains(&"paused"))
        })
        .collect();
    if !live.is_empty() {
        out.push(v(
            "stalled-set-leak",
            Warning,
            None,
            format!(
                "stalled set holds {} id(s) no longer pending: {live:?}",
                live.len()
            ),
        ));
    }

    for (key, ty) in &snap.unknown_keys {
        out.push(v(
            "unknown-key",
            Error,
            None,
            format!("`{key}` (type {ty}) matches no documented key pattern"),
        ));
    }

    out.sort_by(|a, b| b.severity.cmp(&a.severity).then(a.rule.cmp(b.rule)));
    out
}

/// `true` if any violation is at least `min` severe.
pub fn any_at_least(vs: &[Violation], min: Severity) -> bool {
    vs.iter().any(|v| v.severity >= min)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snapshot::JobSnap;

    fn job(fields: &[(&str, &str)]) -> JobSnap {
        JobSnap {
            fields: fields
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            ..Default::default()
        }
    }

    fn rules(s: &Snapshot) -> Vec<&'static str> {
        check(s).into_iter().map(|v| v.rule).collect()
    }

    #[test]
    fn healthy_waiting_job_is_clean() {
        let mut s = Snapshot::default();
        s.lists.insert("wait".into(), vec!["1".into()]);
        s.jobs.insert("1".into(), job(&[("attemptsMade", "0")]));
        assert!(check(&s).is_empty(), "{:?}", check(&s));
    }

    #[test]
    fn detects_double_membership_and_orphans() {
        let mut s = Snapshot::default();
        s.lists.insert("wait".into(), vec!["1".into(), "2".into()]);
        s.zsets.insert("completed".into(), vec![("1".into(), 1.0)]);
        s.jobs.insert("1".into(), job(&[("finishedOn", "1")]));
        s.jobs.insert("3".into(), job(&[]));
        let r = rules(&s);
        assert!(r.contains(&"single-container"));
        assert!(r.contains(&"dangling-entry")); // "2" has no hash
        assert!(r.contains(&"orphan-hash")); // "3" is nowhere
    }

    #[test]
    fn lock_rules() {
        let mut s = Snapshot::default();
        s.lists
            .insert("active".into(), vec!["1".into(), "2".into()]);
        let mut j1 = job(&[("lockToken", "a")]);
        j1.lock = Some(("b".into(), 1000));
        s.jobs.insert("1".into(), j1);
        s.jobs.insert("2".into(), job(&[("lockToken", "x")]));
        s.lists.insert("wait".into(), vec!["3".into()]);
        let mut j3 = job(&[("lockToken", "old")]);
        j3.lock = Some(("old".into(), 500));
        s.jobs.insert("3".into(), j3);
        let vs = check(&s);
        let find = |rule: &str, id: &str| {
            vs.iter()
                .any(|v| v.rule == rule && v.job_id.as_deref() == Some(id))
        };
        assert!(find("lock-mismatch", "1"));
        assert!(find("lock-expired", "2"));
        assert!(find("orphan-lock", "3"));
        assert!(find("stale-lock-field", "3"));
        let expired = vs.iter().find(|v| v.rule == "lock-expired").unwrap();
        assert_eq!(expired.severity, Severity::Transient);
    }

    #[test]
    fn attempts_and_pause_and_finished() {
        let mut s = Snapshot::default();
        s.meta.insert("paused".into(), "1".into());
        s.lists.insert("wait".into(), vec!["1".into()]);
        s.jobs.insert(
            "1".into(),
            job(&[
                ("attemptsMade", "4"),
                ("opts", r#"{"attempts":2}"#),
                ("stalledCounter", "1"),
            ]),
        );
        s.zsets.insert("failed".into(), vec![("2".into(), 1.0)]);
        s.jobs.insert("2".into(), job(&[]));
        let r = rules(&s);
        assert!(r.contains(&"attempts-exceeded"));
        assert!(r.contains(&"paused-but-waiting"));
        assert!(r.contains(&"finished-without-timestamp"));
    }

    #[test]
    fn errors_sort_first() {
        let mut s = Snapshot::default();
        s.lists.insert("active".into(), vec!["1".into()]);
        s.jobs.insert("1".into(), job(&[]));
        s.jobs.insert("2".into(), job(&[]));
        let vs = check(&s);
        assert_eq!(vs[0].severity, Severity::Error);
        assert!(any_at_least(&vs, Severity::Transient));
    }
}
