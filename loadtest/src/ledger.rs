//! Per-job ledger: what we planned for every job, and every attempt we saw.
//!
//! Each job's payload carries its own plan (how many transient failures,
//! whether it's fatal, its delay), so the expected end state is known in
//! advance. Workers — in-process or child processes — report every start
//! and finish here, and the stream observer adds server-side events
//! (stalled, failed, …). At the end [`Ledger::verify`] classifies every job
//! as correct or as a specific anomaly, with its full history attached.

use std::collections::BTreeMap;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

const SHARDS: usize = 64;

/// The payload every load job carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadJob {
    /// Ledger index.
    pub seq: u64,
    /// Epoch-µs when the producer called `add`.
    pub enq_us: u64,
    /// Delay requested at add time.
    pub delay_ms: u64,
    /// Transient failures before success.
    pub fail_attempts: u32,
    /// Fail with `Error::Unrecoverable` on the first attempt.
    pub fatal: bool,
    /// Simulated work.
    pub work_us: u64,
}

/// Terminal state the plan predicts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Expected {
    /// Succeeds on attempt `starts`.
    Completed { starts: u32 },
    /// Fails for good after `starts` attempts.
    Failed { starts: u32 },
}

impl LoadJob {
    /// What should happen to this job given `attempts`.
    pub fn expected(&self, attempts: u32) -> Expected {
        if self.fatal {
            Expected::Failed { starts: 1 }
        } else if self.fail_attempts < attempts {
            Expected::Completed {
                starts: self.fail_attempts + 1,
            }
        } else {
            Expected::Failed { starts: attempts }
        }
    }
}

/// One thing that happened to a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Mark {
    /// Producer's `add` returned.
    Added,
    /// Handler started (attempt number from `attemptsMade`).
    Start { attempt: u32, worker: u32 },
    /// Handler returned `Ok`.
    Ok { worker: u32 },
    /// Handler returned a transient error.
    Err { worker: u32 },
    /// Handler returned `Unrecoverable`.
    Fatal { worker: u32 },
    /// Server emitted `stalled`.
    Stalled,
    /// Server emitted `failed`.
    ServerFailed,
    /// Server emitted `completed`.
    ServerCompleted,
    /// Server emitted `delayed`.
    ServerDelayed,
    /// Server emitted `active`.
    ServerActive,
}

/// Ledger row for one job.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Entry {
    /// Payload as submitted (None until added).
    pub job: Option<LoadJob>,
    /// Backend-assigned id.
    pub job_id: Option<String>,
    /// Handler starts.
    pub starts: u32,
    /// `Ok` returns.
    pub oks: u32,
    /// Transient error returns.
    pub errs: u32,
    /// Fatal returns.
    pub fatals: u32,
    /// Server `stalled` events.
    pub stalls: u32,
    /// Server `failed` events.
    pub server_failed: u32,
    /// Server `completed` events.
    pub server_completed: u32,
    /// Epoch-µs of the first start.
    pub first_start_us: Option<u64>,
    /// Epoch-µs of the first `Ok`.
    pub ok_us: Option<u64>,
    /// Highest attempt number seen.
    pub max_attempt: u32,
    /// Most recent marks, epoch-µs.
    pub history: Vec<(u64, Mark)>,
}

impl Entry {
    fn mark(&mut self, cap: usize, at_us: u64, m: Mark) {
        if cap == 0 {
            return;
        }
        if self.history.len() >= cap {
            self.history.remove(1.min(self.history.len() - 1)); // keep the first (Added) for context
        }
        self.history.push((at_us, m));
    }

    /// Whether the job reached the terminal state its plan predicts
    /// (or any terminal state at all, for a planned success that failed).
    pub fn is_terminal(&self, attempts: u32) -> bool {
        let Some(job) = &self.job else { return false };
        if self.oks > 0 || self.fatals > 0 {
            return true;
        }
        match job.expected(attempts) {
            Expected::Completed { .. } => self.server_failed > 0,
            Expected::Failed { starts } => self.errs >= starts || self.server_failed > 0,
        }
    }
}

/// Shared ledger, sharded to keep lock contention off the hot path.
#[derive(Debug)]
pub struct Ledger {
    shards: Vec<Mutex<BTreeMap<u64, Entry>>>,
    by_id: Mutex<BTreeMap<String, u64>>,
    /// Per-job history entries kept (oldest dropped beyond this). Scaled
    /// down for million-job runs to bound memory.
    history_cap: usize,
}

impl Ledger {
    pub fn new(history_cap: usize) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(BTreeMap::new())).collect(),
            by_id: Mutex::new(BTreeMap::new()),
            history_cap,
        }
    }

    fn with<R>(&self, seq: u64, f: impl FnOnce(&mut Entry) -> R) -> R {
        let mut s = self.shards[(seq as usize) % SHARDS].lock();
        f(s.entry(seq).or_default())
    }

    pub fn added(&self, job: LoadJob, id: String, at_us: u64) {
        let cap = self.history_cap;
        let seq = job.seq;
        self.by_id.lock().insert(id.clone(), seq);
        self.with(seq, |e| {
            e.job = Some(job);
            e.job_id = Some(id);
            e.mark(cap, at_us, Mark::Added);
        });
    }

    pub fn start(&self, seq: u64, attempt: u32, worker: u32, at_us: u64) {
        let cap = self.history_cap;
        self.with(seq, |e| {
            e.starts += 1;
            e.max_attempt = e.max_attempt.max(attempt);
            e.first_start_us.get_or_insert(at_us);
            e.mark(cap, at_us, Mark::Start { attempt, worker });
        });
    }

    pub fn finish(&self, seq: u64, mark: Mark, at_us: u64) {
        let cap = self.history_cap;
        self.with(seq, |e| {
            match mark {
                Mark::Ok { .. } => {
                    e.oks += 1;
                    e.ok_us.get_or_insert(at_us);
                }
                Mark::Err { .. } => e.errs += 1,
                Mark::Fatal { .. } => e.fatals += 1,
                _ => {}
            }
            e.mark(cap, at_us, mark);
        });
    }

    /// Record a server-side stream event by job id.
    pub fn server_event(&self, job_id: &str, event: &str, at_us: u64) {
        let cap = self.history_cap;
        let Some(seq) = self.by_id.lock().get(job_id).copied() else {
            return;
        };
        let mark = match event {
            "stalled" => Mark::Stalled,
            "failed" => Mark::ServerFailed,
            "completed" => Mark::ServerCompleted,
            "delayed" => Mark::ServerDelayed,
            "active" => Mark::ServerActive,
            _ => return,
        };
        self.with(seq, |e| {
            match mark {
                Mark::Stalled => e.stalls += 1,
                Mark::ServerFailed => e.server_failed += 1,
                Mark::ServerCompleted => e.server_completed += 1,
                _ => {}
            }
            e.mark(cap, at_us, mark);
        });
    }

    /// Jobs not yet terminal.
    pub fn pending(&self, attempts: u32) -> usize {
        self.shards
            .iter()
            .map(|s| {
                s.lock()
                    .values()
                    .filter(|e| e.job.is_some() && !e.is_terminal(attempts))
                    .count()
            })
            .sum()
    }

    /// Classify every job. See [`Anomaly`].
    pub fn verify(&self, attempts: u32, allow_extra_starts: u32) -> Verdict {
        let mut v = Verdict::default();
        for shard in &self.shards {
            for (seq, e) in shard.lock().iter() {
                let Some(job) = &e.job else { continue };
                v.total += 1;
                let exp = job.expected(attempts);
                let mut push = |kind: Anomaly, why: String| {
                    let list = v.anomalies.entry(kind).or_default();
                    v.counts.entry(kind).and_modify(|c| *c += 1).or_insert(1);
                    if list.len() < 25 {
                        list.push(Example {
                            seq: *seq,
                            job_id: e.job_id.clone(),
                            why,
                            entry: e.clone(),
                        });
                    }
                };
                if e.oks > 0 {
                    v.completed_jobs += 1;
                } else if e.is_terminal(attempts) {
                    v.failed_jobs += 1;
                }
                if !e.is_terminal(attempts) {
                    push(
                        Anomaly::Lost,
                        format!(
                            "never reached a terminal state (starts={}, expected {exp:?})",
                            e.starts
                        ),
                    );
                    continue;
                }
                if e.oks > 1 {
                    // At-least-once: a job whose lock was lost may legitimately
                    // run to success twice. Only unexplained repeats count.
                    let extra = e.oks - 1;
                    if extra > e.stalls + allow_extra_starts {
                        push(
                            Anomaly::DuplicateCompletion,
                            format!(
                                "handler returned Ok {} times with only {} stall(s)",
                                e.oks, e.stalls
                            ),
                        );
                    } else {
                        v.explained_duplicates += extra as u64;
                    }
                }
                match exp {
                    Expected::Completed { starts } => {
                        if e.oks == 0 {
                            push(
                                Anomaly::WrongOutcome,
                                format!(
                                    "planned to succeed on attempt {starts} but failed after {} start(s)",
                                    e.starts
                                ),
                            );
                        }
                        let allowed = starts + e.stalls + allow_extra_starts;
                        if e.starts > allowed {
                            push(
                                Anomaly::ExtraStarts,
                                format!(
                                    "{} starts > planned {starts} + {} stall(s) + {allow_extra_starts} chaos allowance",
                                    e.starts, e.stalls
                                ),
                            );
                        } else if e.starts > starts {
                            v.explained_extra_starts += (e.starts - starts) as u64;
                        }
                    }
                    Expected::Failed { starts } => {
                        if e.oks > 0 {
                            push(
                                Anomaly::WrongOutcome,
                                format!("planned to fail after {starts} start(s) but completed"),
                            );
                        }
                        if job.fatal && e.starts > 1 + e.stalls + allow_extra_starts {
                            push(
                                Anomaly::FatalRetried,
                                format!("unrecoverable job started {} times", e.starts),
                            );
                        }
                        if !job.fatal && e.starts < starts && e.server_failed > 0 {
                            push(
                                Anomaly::PrematureFailure,
                                format!("failed for good after {} of {starts} attempts", e.starts),
                            );
                        }
                    }
                }
                if job.delay_ms > 0
                    && let Some(first) = e.first_start_us
                {
                    let due = job.enq_us + job.delay_ms * 1000;
                    // 5ms tolerance for clock granularity.
                    if first + 5_000 < due {
                        push(
                            Anomaly::RanEarly,
                            format!(
                                "started {:.1}ms before its delay elapsed",
                                (due - first) as f64 / 1000.0
                            ),
                        );
                    }
                }
                if e.oks == 1 && e.server_completed == 0 && e.server_failed == 0 {
                    v.missing_server_events += 1;
                }
            }
        }
        v
    }
}

/// Kinds of wrong behaviour the ledger can detect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Anomaly {
    /// Job never reached a terminal state.
    Lost,
    /// Handler succeeded more than once.
    DuplicateCompletion,
    /// Ended completed when it should have failed, or vice versa.
    WrongOutcome,
    /// More handler starts than planned, not explained by stalls.
    ExtraStarts,
    /// `Unrecoverable` job was retried.
    FatalRetried,
    /// Failed permanently before using its attempts.
    PrematureFailure,
    /// Delayed job ran before its delay elapsed.
    RanEarly,
}

/// One anomalous job with its full ledger row.
#[derive(Debug, Clone, Serialize)]
pub struct Example {
    pub seq: u64,
    pub job_id: Option<String>,
    pub why: String,
    pub entry: Entry,
}

/// Outcome of [`Ledger::verify`].
#[derive(Debug, Clone, Default, Serialize)]
pub struct Verdict {
    pub total: u64,
    pub counts: BTreeMap<Anomaly, u64>,
    pub anomalies: BTreeMap<Anomaly, Vec<Example>>,
    /// Extra starts fully explained by stall events or chaos.
    pub explained_extra_starts: u64,
    /// Repeat successes explained by stalls (at-least-once delivery).
    pub explained_duplicates: u64,
    /// Distinct jobs whose handler succeeded at least once.
    pub completed_jobs: u64,
    /// Distinct jobs that ended failed.
    pub failed_jobs: u64,
    /// Successful jobs for which the observer saw no server terminal event
    /// (events trimmed from the stream, or never emitted).
    pub missing_server_events: u64,
}

impl Verdict {
    #[cfg(test)]
    pub fn ok(&self) -> bool {
        self.counts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(seq: u64, fail: u32, fatal: bool) -> LoadJob {
        LoadJob {
            seq,
            enq_us: 0,
            delay_ms: 0,
            fail_attempts: fail,
            fatal,
            work_us: 0,
        }
    }

    #[test]
    fn expectations() {
        assert_eq!(
            job(0, 0, false).expected(3),
            Expected::Completed { starts: 1 }
        );
        assert_eq!(
            job(0, 2, false).expected(3),
            Expected::Completed { starts: 3 }
        );
        assert_eq!(job(0, 3, false).expected(3), Expected::Failed { starts: 3 });
        assert_eq!(job(0, 0, true).expected(3), Expected::Failed { starts: 1 });
    }

    #[test]
    fn clean_run_verifies() {
        let l = Ledger::new(24);
        l.added(job(1, 1, false), "1".into(), 0);
        l.start(1, 1, 0, 10);
        l.finish(1, Mark::Err { worker: 0 }, 11);
        l.start(1, 2, 0, 20);
        l.finish(1, Mark::Ok { worker: 0 }, 21);
        l.server_event("1", "completed", 22);
        assert_eq!(l.pending(3), 0);
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.total, 1);
    }

    #[test]
    fn detects_lost_duplicate_and_extra_starts() {
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0); // never started → lost
        l.added(job(2, 0, false), "2".into(), 0);
        for _ in 0..2 {
            l.start(2, 1, 0, 1);
            l.finish(2, Mark::Ok { worker: 0 }, 2);
        }
        l.added(job(3, 0, true), "3".into(), 0);
        l.start(3, 1, 0, 1);
        l.finish(3, Mark::Fatal { worker: 0 }, 2);
        l.start(3, 2, 0, 3);
        let v = l.verify(3, 0);
        assert_eq!(v.counts.get(&Anomaly::Lost), Some(&1));
        assert_eq!(v.counts.get(&Anomaly::DuplicateCompletion), Some(&1));
        assert_eq!(v.counts.get(&Anomaly::ExtraStarts), Some(&1));
        assert_eq!(v.counts.get(&Anomaly::FatalRetried), Some(&1));
        assert_eq!(v.anomalies[&Anomaly::Lost][0].seq, 1);
    }

    #[test]
    fn stalls_explain_extra_starts() {
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0);
        l.start(1, 1, 0, 1);
        l.server_event("1", "stalled", 2);
        l.start(1, 2, 1, 3);
        l.finish(1, Mark::Ok { worker: 1 }, 4);
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.explained_extra_starts, 1);
    }

    #[test]
    fn ran_early_detected() {
        let l = Ledger::new(24);
        let mut j = job(1, 0, false);
        j.delay_ms = 1000;
        j.enq_us = 1_000_000;
        l.added(j, "1".into(), 0);
        l.start(1, 1, 0, 1_100_000); // 900ms early
        l.finish(1, Mark::Ok { worker: 0 }, 1_100_001);
        let v = l.verify(3, 0);
        assert_eq!(v.counts.get(&Anomaly::RanEarly), Some(&1));
    }
}
