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
    /// If > 0, the first pickup returns `Error::Delayed` for this long — a
    /// postponement that must not cost an attempt.
    #[serde(default)]
    pub postpone_ms: u64,
    /// Planned transient failures panic instead of returning `Err`.
    #[serde(default)]
    pub panic: bool,
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
    Ok { worker: u32, attempt: u32 },
    /// Handler returned a transient error.
    Err { worker: u32, attempt: u32 },
    /// Handler returned `Unrecoverable`.
    Fatal { worker: u32, attempt: u32 },
    /// Handler returned `Error::Delayed` (planned postponement).
    Postponed { worker: u32, attempt: u32 },
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
    /// A shutting-down worker released the job back to the queue.
    Released,
    /// The job exceeded max_stalled and was failed by the stalled scanner.
    StallFailed,
}

/// Ledger row for one job.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Entry {
    /// Payload as submitted (None until added).
    pub job: Option<LoadJob>,
    /// Backend-assigned id.
    pub job_id: Option<String>,
    /// The `add` returned an error, so whether the job exists is unknown
    /// until (and unless) a worker starts it.
    pub ambiguous: bool,
    /// Handler starts.
    pub starts: u32,
    /// `Ok` returns.
    pub oks: u32,
    /// Transient error returns.
    pub errs: u32,
    /// Fatal returns.
    pub fatals: u32,
    /// Planned postponements (`Error::Delayed`) returned.
    pub postpones: u32,
    /// Server `stalled` events.
    pub stalls: u32,
    /// Releases by shutting-down workers.
    pub releases: u32,
    /// Failed by the stalled scanner (max_stalled exceeded).
    pub stall_failed: bool,
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
    /// Highest attempt number that ended in a transient error.
    pub max_err_attempt: u32,
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

    /// An ambiguous add no worker ever started: assumed not to exist.
    pub fn unlanded(&self) -> bool {
        self.ambiguous && self.starts == 0
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

    /// A submission whose `add` failed: it may or may not have landed.
    pub fn add_ambiguous(&self, job: LoadJob) {
        let cap = self.history_cap;
        let at = job.enq_us;
        self.with(job.seq, |e| {
            e.job = Some(job);
            e.ambiguous = true;
            e.mark(cap, at, Mark::Added);
        });
    }

    pub fn start(&self, seq: u64, attempt: u32, worker: u32, at_us: u64, job_id: u64) {
        let cap = self.history_cap;
        // An ambiguous add that did land: learn its id so the stream's
        // events for it are attributed from here on.
        let bind = self.with(seq, |e| {
            (e.ambiguous && e.job_id.is_none() && job_id != 0).then(|| {
                let id = job_id.to_string();
                e.job_id = Some(id.clone());
                id
            })
        });
        if let Some(id) = bind {
            self.by_id.lock().insert(id, seq);
        }
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
                Mark::Err { attempt, .. } => {
                    e.errs += 1;
                    e.max_err_attempt = e.max_err_attempt.max(attempt);
                }
                Mark::Fatal { .. } => e.fatals += 1,
                Mark::Postponed { .. } => e.postpones += 1,
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
            "released" => Mark::Released,
            "stall-failed" => Mark::StallFailed,
            _ => return,
        };
        self.with(seq, |e| {
            match mark {
                Mark::Stalled => e.stalls += 1,
                Mark::Released => e.releases += 1,
                Mark::StallFailed => {
                    e.stalls += 1;
                    e.server_failed += 1;
                    e.stall_failed = true;
                }
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
                    .filter(|e| e.job.is_some() && !e.is_terminal(attempts) && !e.unlanded())
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
                if e.unlanded() {
                    v.ambiguous_adds_unseen += 1;
                    continue;
                }
                if e.ambiguous {
                    v.ambiguous_adds_landed += 1;
                }
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
                if e.stall_failed {
                    // Ending failed after running out of stalls is the
                    // documented outcome, whatever the plan said — even if a
                    // handler had returned Ok: a worker killed between its
                    // handler and complete() loses that outcome, the lock
                    // lapses, and with no stalls left the job fails.
                    v.stall_failures += 1;
                    v.failed_jobs += 1;
                    if e.oks > 0 {
                        v.discarded_outcomes += 1;
                    }
                    continue;
                }
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
                // Attempts that started but never reported back were cut
                // short (SIGKILL, stall, shutdown). The handler fails by
                // attempt *number*, so an interruption shifts the plan: the
                // job legitimately needs more starts and may even succeed
                // where the plan alone predicted a failure. Without
                // interruptions the plan is exact; with them, check that oxn
                // followed its retry rules for the history actually seen.
                // Attempt numbers are assigned by the server at fetch time, so
                // a gap (attempt 3 seen, only 2 handler runs) is an attempt a
                // worker took and died with before its handler even started.
                let finished = e.oks + e.errs + e.fatals + e.postpones;
                let interrupted = e.starts.max(e.max_attempt).saturating_sub(finished);
                if interrupted == 0 {
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
                            // Duplicate successes are judged above.
                            let allowed = starts
                                + e.postpones
                                + (e.oks.saturating_sub(1))
                                + allow_extra_starts;
                            if e.starts > allowed {
                                push(
                                    Anomaly::ExtraStarts,
                                    format!(
                                        "{} starts > planned {starts}, none of them interrupted",
                                        e.starts
                                    ),
                                );
                            }
                        }
                        Expected::Failed { starts } => {
                            if e.oks > 0 {
                                push(
                                    Anomaly::WrongOutcome,
                                    format!(
                                        "planned to fail after {starts} start(s) but completed"
                                    ),
                                );
                            }
                            if !job.fatal && e.starts < starts && e.server_failed > 0 {
                                push(
                                    Anomaly::PrematureFailure,
                                    format!(
                                        "failed for good after {} of {starts} attempts",
                                        e.starts
                                    ),
                                );
                            }
                        }
                    }
                } else {
                    v.interrupted_attempts += interrupted as u64;
                    v.explained_extra_starts += interrupted as u64;
                    if interrupted > e.stalls + e.releases + allow_extra_starts {
                        push(
                            Anomaly::ExtraStarts,
                            format!(
                                "{interrupted} interrupted attempt(s) but only {} stall(s) and {} release(s) recorded",
                                e.stalls, e.releases
                            ),
                        );
                    }
                    if e.oks > 0 && e.max_err_attempt >= attempts {
                        push(
                            Anomaly::WrongOutcome,
                            format!(
                                "failed on attempt {} (limit {attempts}) yet was retried and completed",
                                e.max_err_attempt
                            ),
                        );
                    }
                    if e.oks == 0
                        && e.fatals == 0
                        && e.server_failed > 0
                        && e.max_err_attempt < attempts
                        && e.stalls == 0
                    {
                        push(
                            Anomaly::PrematureFailure,
                            format!(
                                "failed for good though its last failure was attempt {} of {attempts}",
                                e.max_err_attempt
                            ),
                        );
                    }
                }
                // A fatal job may only run again if a stall took it back.
                if job.fatal && e.starts > 1 + e.stalls + e.postpones + allow_extra_starts {
                    push(
                        Anomaly::FatalRetried,
                        format!("unrecoverable job started {} times", e.starts),
                    );
                }
                v.postponements += e.postpones as u64;
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
    /// Attempts that started but never finished (killed / stalled).
    pub interrupted_attempts: u64,
    /// Jobs failed for exceeding max_stalled.
    pub stall_failures: u64,
    /// Planned `Error::Delayed` postponements returned by handlers.
    pub postponements: u64,
    /// Handler successes never recorded because the job's lock lapsed
    /// first (worker killed / too slow); the job's fate went to the
    /// stalled scanner instead.
    pub discarded_outcomes: u64,
    /// Distinct jobs whose handler succeeded at least once.
    pub completed_jobs: u64,
    /// Distinct jobs that ended failed.
    pub failed_jobs: u64,
    /// Successful jobs for which the observer saw no server terminal event
    /// (events trimmed from the stream, or never emitted).
    pub missing_server_events: u64,
    /// `add` calls that errored yet created the job (a worker ran it).
    pub ambiguous_adds_landed: u64,
    /// `add` calls that errored and whose job never ran (assumed absent).
    pub ambiguous_adds_unseen: u64,
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
            postpone_ms: 0,
            panic: false,
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
    fn ambiguous_adds_count_only_if_they_ran() {
        let l = Ledger::new(24);
        // seq 1: the add errored but the job landed and completed (id 7).
        l.add_ambiguous(job(1, 0, false));
        // seq 2: the add errored and nothing ran; its retry is seq 3.
        l.add_ambiguous(job(2, 0, false));
        l.added(job(3, 0, false), "8".into(), 0);
        assert_eq!(
            l.pending(3),
            1,
            "an ambiguous add isn't waited for until it starts"
        );
        l.start(1, 1, 0, 10, 7);
        l.finish(
            1,
            Mark::Ok {
                worker: 0,
                attempt: 1,
            },
            11,
        );
        l.server_event("7", "completed", 12);
        l.start(3, 1, 0, 10, 8);
        l.finish(
            3,
            Mark::Ok {
                worker: 0,
                attempt: 1,
            },
            11,
        );
        l.server_event("8", "completed", 12);
        assert_eq!(l.pending(3), 0);
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!((v.ambiguous_adds_landed, v.ambiguous_adds_unseen), (1, 1));
        assert_eq!(v.completed_jobs, 2);
        assert_eq!(
            v.missing_server_events, 0,
            "the landed job's id was learned"
        );
    }

    #[test]
    fn clean_run_verifies() {
        let l = Ledger::new(24);
        l.added(job(1, 1, false), "1".into(), 0);
        l.start(1, 1, 0, 10, 0);
        l.finish(
            1,
            Mark::Err {
                worker: 0,
                attempt: 1,
            },
            11,
        );
        l.start(1, 2, 0, 20, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 0,
                attempt: 2,
            },
            21,
        );
        l.server_event("1", "completed", 22);
        assert_eq!(l.pending(3), 0);
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.total, 1);
    }

    #[test]
    fn detects_lost_duplicate_and_fatal_retried() {
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0); // never started → lost
        l.added(job(2, 0, false), "2".into(), 0);
        for _ in 0..2 {
            l.start(2, 1, 0, 1, 0);
            l.finish(
                2,
                Mark::Ok {
                    worker: 0,
                    attempt: 1,
                },
                2,
            );
        }
        l.added(job(3, 0, true), "3".into(), 0);
        l.start(3, 1, 0, 1, 0);
        l.finish(
            3,
            Mark::Fatal {
                worker: 0,
                attempt: 1,
            },
            2,
        );
        l.start(3, 2, 0, 3, 0);
        let v = l.verify(3, 0);
        assert_eq!(v.counts.get(&Anomaly::Lost), Some(&1));
        assert_eq!(v.counts.get(&Anomaly::DuplicateCompletion), Some(&1));
        assert_eq!(v.counts.get(&Anomaly::FatalRetried), Some(&1));
        assert_eq!(
            v.counts.get(&Anomaly::ExtraStarts),
            Some(&1),
            "job 3's restart is unexplained"
        );
        assert_eq!(v.anomalies[&Anomaly::Lost][0].seq, 1);
    }

    #[test]
    fn interrupted_attempt_without_stall_is_flagged() {
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0); // never reports back, no stall recorded
        l.start(1, 2, 0, 2, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 0,
                attempt: 2,
            },
            3,
        );
        let v = l.verify(3, 0);
        assert_eq!(v.counts.get(&Anomaly::ExtraStarts), Some(&1));
    }

    #[test]
    fn killed_final_attempt_may_legitimately_succeed() {
        // The kill-worker case: plan says fail 3× with 3 attempts, but
        // attempt 3 was killed (stall), so attempt 4 runs and succeeds.
        let l = Ledger::new(24);
        l.added(job(1, 3, false), "1".into(), 0);
        for a in 1..=2 {
            l.start(1, a, 0, a as u64, 0);
            l.finish(
                1,
                Mark::Err {
                    worker: 0,
                    attempt: a,
                },
                a as u64,
            );
        }
        l.start(1, 3, 0, 10, 0);
        l.server_event("1", "stalled", 11);
        l.start(1, 4, 1, 12, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 1,
                attempt: 4,
            },
            13,
        );
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.interrupted_attempts, 1);
    }

    #[test]
    fn attempt_taken_but_never_started_counts_as_interrupted() {
        // r3 of the B1 kill-worker run: attempt 2 was fetched by a worker
        // that died before its handler ran; attempt 3 fails at the limit.
        let l = Ledger::new(24);
        l.added(job(1, 5, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0);
        l.finish(
            1,
            Mark::Err {
                worker: 0,
                attempt: 1,
            },
            2,
        );
        l.server_event("1", "stalled", 3);
        l.start(1, 3, 1, 4, 0);
        l.finish(
            1,
            Mark::Err {
                worker: 1,
                attempt: 3,
            },
            5,
        );
        l.server_event("1", "failed", 6);
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.interrupted_attempts, 1);
    }

    #[test]
    fn releases_explain_interrupted_attempts() {
        // Shutdown aborted attempt 1 and released it (attempt refunded), so
        // the re-run is attempt 1 again.
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0);
        l.server_event("1", "released", 2);
        l.start(1, 1, 1, 3, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 1,
                attempt: 1,
            },
            4,
        );
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.interrupted_attempts, 1);
    }

    #[test]
    fn running_out_of_stalls_is_an_accepted_failure() {
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0); // killed
        l.server_event("1", "stall-failed", 2);
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.stall_failures, 1);
        assert_eq!(v.failed_jobs, 1);
    }

    #[test]
    fn ok_lost_to_a_killed_worker_then_stall_failed_is_accounted() {
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 0,
                attempt: 1,
            },
            2,
        ); // reported, then SIGKILL
        l.server_event("1", "stall-failed", 3);
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(
            (v.completed_jobs, v.failed_jobs, v.discarded_outcomes),
            (0, 1, 1)
        );
    }

    #[test]
    fn a_postponement_is_neither_an_attempt_nor_a_failure() {
        // fail twice, succeed on the 3rd of 3 attempts, after one postponement
        let l = Ledger::new(24);
        l.added(job(1, 2, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0);
        l.finish(
            1,
            Mark::Postponed {
                worker: 0,
                attempt: 1,
            },
            2,
        );
        for a in 1..=2 {
            l.start(1, a, 0, 10 + a as u64, 0);
            l.finish(
                1,
                Mark::Err {
                    worker: 0,
                    attempt: a,
                },
                11 + a as u64,
            );
        }
        l.start(1, 3, 0, 20, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 0,
                attempt: 3,
            },
            21,
        );
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
        assert_eq!(v.postponements, 1);

        // A fatal job postponed first starts twice; that is not a retry.
        let l = Ledger::new(24);
        l.added(job(2, 0, true), "2".into(), 0);
        l.start(2, 1, 0, 1, 0);
        l.finish(
            2,
            Mark::Postponed {
                worker: 0,
                attempt: 1,
            },
            2,
        );
        l.start(2, 1, 0, 3, 0);
        l.finish(
            2,
            Mark::Fatal {
                worker: 0,
                attempt: 1,
            },
            4,
        );
        let v = l.verify(3, 0);
        assert!(v.ok(), "{:?}", v.counts);
    }

    #[test]
    fn retry_past_the_limit_is_flagged_even_with_interruptions() {
        let l = Ledger::new(24);
        l.added(job(1, 5, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0); // killed
        l.server_event("1", "stalled", 2);
        for a in 2..=3 {
            l.start(1, a, 0, a as u64 * 10, 0);
            l.finish(
                1,
                Mark::Err {
                    worker: 0,
                    attempt: a,
                },
                a as u64 * 10 + 1,
            );
        }
        // attempt 3 failed at the limit of 3 — oxn must not run it again.
        l.start(1, 6, 0, 100, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 0,
                attempt: 6,
            },
            101,
        );
        let v = l.verify(3, 0);
        assert_eq!(v.counts.get(&Anomaly::WrongOutcome), Some(&1));
    }

    #[test]
    fn stalls_explain_extra_starts() {
        let l = Ledger::new(24);
        l.added(job(1, 0, false), "1".into(), 0);
        l.start(1, 1, 0, 1, 0);
        l.server_event("1", "stalled", 2);
        l.start(1, 2, 1, 3, 0);
        l.finish(
            1,
            Mark::Ok {
                worker: 1,
                attempt: 2,
            },
            4,
        );
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
        l.start(1, 1, 0, 1_100_000, 0); // 900ms early
        l.finish(
            1,
            Mark::Ok {
                worker: 0,
                attempt: 2,
            },
            1_100_001,
        );
        let v = l.verify(3, 0);
        assert_eq!(v.counts.get(&Anomaly::RanEarly), Some(&1));
    }
}
