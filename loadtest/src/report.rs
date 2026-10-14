//! Markdown + JSON rendering of a load run.

use std::fmt::Write as _;
use std::path::PathBuf;

use crate::run::Outcome;
use crate::stats::HistSummary;

pub fn write(o: &Outcome) -> anyhow::Result<PathBuf> {
    let dir = oxn_testkit::report::report_root().join("load");
    std::fs::create_dir_all(&dir)?;
    let stem = format!("{}-{}", o.config.slug(), o.started_at);
    let md = dir.join(format!("{stem}.md"));
    std::fs::write(&md, render(o))?;
    std::fs::write(
        dir.join(format!("{stem}.json")),
        serde_json::to_string_pretty(o)?,
    )?;
    Ok(md)
}

fn hist_row(name: &str, h: &HistSummary) -> String {
    format!(
        "| {name} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} |",
        h.count, h.p50_ms, h.p90_ms, h.p99_ms, h.p999_ms, h.max_ms, h.mean_ms
    )
}

pub fn render(o: &Outcome) -> String {
    let mut s = String::new();
    let c = &o.config;
    let verdict = if o.passed { "✅ PASS" } else { "❌ FAIL" };
    let _ = writeln!(s, "# oxn load run — `{}` {verdict}\n", c.slug());
    let _ = writeln!(
        s,
        "queue `{}` · {} jobs added ({} add errors) · load {:.1}s + drain {:.1}s · **{:.0} completions/s**\n",
        o.queue, o.added, o.add_errors, o.load_secs, o.drain_secs, o.throughput_ok_per_s
    );
    if let (Some(a), Some(b)) = (&o.loadavg_start, &o.loadavg_end) {
        let _ = writeln!(
            s,
            "host load average (1/5/15m): start `{a}` → end `{b}` — compare throughput only between quiet runs\n"
        );
    }
    if !o.failures.is_empty() {
        let _ = writeln!(s, "## Failures\n");
        for f in &o.failures {
            let _ = writeln!(s, "- {f}");
        }
        let _ = writeln!(s);
    }

    let _ = writeln!(s, "## Configuration\n");
    let _ = writeln!(s, "| knob | value |\n|---|---|");
    let cfg = serde_json::to_value(c).unwrap_or_default();
    if let Some(m) = cfg.as_object() {
        for (k, v) in m {
            let _ = writeln!(s, "| {k} | `{v}` |");
        }
    }

    let _ = writeln!(s, "\n## Latency (ms)\n");
    let _ = writeln!(
        s,
        "| metric | count | p50 | p90 | p99 | p99.9 | max | mean |\n|---|---|---|---|---|---|---|---|"
    );
    let _ = writeln!(
        s,
        "{}",
        hist_row("pickup (eligible → first start)", &o.pickup)
    );
    let _ = writeln!(s, "{}", hist_row("handler run", &o.run));
    let _ = writeln!(s, "{}", hist_row("end-to-end (eligible → ok)", &o.e2e));
    let _ = writeln!(s, "{}", hist_row("producer add()", &o.add_latency));

    let v = &o.verdict;
    let _ = writeln!(s, "\n## Ledger verification\n");
    let _ = writeln!(
        s,
        "{} jobs checked · {} completed · {} failed ({} by the stall limit) · {} planned postponement(s) · {} interrupted attempt(s) (killed/stalled, re-run) · {} handler success(es) discarded by a lapsed lock · {} duplicate success(es) explained by stalls · {} success(es) without a server terminal event\n",
        v.total,
        v.completed_jobs,
        v.failed_jobs,
        v.stall_failures,
        v.postponements,
        v.interrupted_attempts,
        v.discarded_outcomes,
        v.explained_duplicates,
        v.missing_server_events
    );
    if v.ambiguous_adds_landed + v.ambiguous_adds_unseen > 0 {
        let _ = writeln!(
            s,
            "Failed `add` calls: {} created their job anyway (it ran and is verified above), {} never ran (assumed not created).\n",
            v.ambiguous_adds_landed, v.ambiguous_adds_unseen
        );
    }
    if v.counts.is_empty() {
        let _ = writeln!(
            s,
            "No anomalies. Every job reached exactly the outcome its plan predicted.\n"
        );
    } else {
        for (kind, examples) in &v.anomalies {
            let _ = writeln!(s, "### {kind:?} — {} job(s)\n", v.counts[kind]);
            for ex in examples.iter().take(10) {
                let _ = writeln!(
                    s,
                    "- seq {} (job `{}`): {}",
                    ex.seq,
                    ex.job_id.as_deref().unwrap_or("?"),
                    ex.why
                );
                let first = ex.entry.history.first().map(|(t, _)| *t).unwrap_or(0);
                let hist: Vec<String> = ex
                    .entry
                    .history
                    .iter()
                    .map(|(t, m)| format!("+{:.1}ms {m:?}", (*t as f64 - first as f64) / 1000.0))
                    .collect();
                let _ = writeln!(s, "  - history: {}", hist.join(" → "));
            }
            let _ = writeln!(s);
        }
    }

    let _ = writeln!(s, "## Redis state after drain\n");
    let _ = writeln!(s, "containers: {:?}\n", o.final_sizes);
    for r in &o.retention_findings {
        let _ = writeln!(s, "- {r}");
    }
    let l = &o.leak;
    let _ = writeln!(
        s,
        "- keys under queue: {} · job hashes: {} · retained entries: {} · orphan hashes: {} · orphan aux: {}{}",
        l.total_queue_keys,
        l.job_hashes,
        l.retained_entries,
        l.orphan_hashes,
        l.orphan_aux_keys,
        l.note
            .as_deref()
            .map(|n| format!(" ({n})"))
            .unwrap_or_default()
    );
    let mb = |b: u64| b as f64 / 1_048_576.0;
    let _ = writeln!(
        s,
        "- used_memory: before {:.1}MB → after drain {:.1}MB{}",
        mb(o.health_before.used_memory),
        mb(o.health_after_drain.used_memory),
        o.health_after_cleanup
            .as_ref()
            .map(|h| format!(" → after cleanup {:.1}MB", mb(h.used_memory)))
            .unwrap_or_default()
    );

    let _ = writeln!(s, "\n## Stream events observed\n");
    let ev: Vec<String> = o
        .stream_events
        .iter()
        .map(|(k, n)| format!("{k}={n}"))
        .collect();
    let _ = writeln!(s, "{}\n", ev.join(" · "));

    if let Some(sub) = &o.oxn_subscriber_events {
        let _ = writeln!(s, "## oxn subscriber cross-check\n");
        let ev: Vec<String> = sub.iter().map(|(k, n)| format!("{k}={n}")).collect();
        let _ = writeln!(
            s,
            "{} · transport errors surfaced: {}\n",
            ev.join(" · "),
            o.oxn_subscriber_errors
        );
        if o.oxn_subscriber_mismatch.is_empty() {
            let _ = writeln!(s, "Matches the raw stream exactly.\n");
        } else {
            for m in &o.oxn_subscriber_mismatch {
                let _ = writeln!(s, "- ❌ {m}");
            }
            let _ = writeln!(s);
        }
    }
    if !o.violations.is_empty() {
        let _ = writeln!(s, "## Invariant violations\n");
        for (t, v) in o.violations.iter().take(50) {
            let _ = writeln!(s, "- t={t:.1}s {v}");
        }
        let _ = writeln!(s);
    }

    if !o.chaos_log.is_empty() {
        let _ = writeln!(s, "## Chaos\n");
        for (t, m) in &o.chaos_log {
            let _ = writeln!(s, "- t={t:.1}s {m}");
        }
        let _ = writeln!(s, "- worker processes killed: {}\n", o.kills);
    }
    if let (Some((max, per)), Some(peak), Some(mean)) = (
        o.config.rate_limit,
        o.rate_limit_peak,
        o.rate_limit_mean_per_window,
    ) {
        let _ = writeln!(
            s,
            "- rate limit {max}/{per}ms: peak {peak} starts in any {per}ms span (≤ {} allowed), mean {mean:.1} per window\n",
            2 * max
        );
    }

    if let Some(a) = &o.api {
        let _ = writeln!(s, "## HTTP API\n");
        let _ = writeln!(
            s,
            "- every job added through the API (separate process); {} add error(s)",
            a.add_errors
        );
        let h = &a.read_latency;
        let _ = writeln!(
            s,
            "- reads: {} requests, p50 {:.2}ms · p99 {:.2}ms · max {:.2}ms; {} error(s)",
            h.count, h.p50_ms, h.p99_ms, h.max_ms, a.read_errors
        );
        let _ = writeln!(s, "- SSE events received: {:?}\n", a.sse_events);
    }
    if let Some(p) = &o.plateau {
        let _ = writeln!(s, "## Plateau (leak check)\n");
        let _ = writeln!(
            s,
            "- keys: {:.0} (t={:.0}–{:.0}s) → {:.0} (t={:.0}–{:.0}s): {:+.1}%",
            p.early_keys,
            p.early_window_s.0,
            p.early_window_s.1,
            p.late_keys,
            p.late_window_s.0,
            p.late_window_s.1,
            p.key_growth() * 100.0
        );
        let _ = writeln!(
            s,
            "- used_memory: {:.1}MB → {:.1}MB: {:+.1}%\n",
            p.early_memory_mb,
            p.late_memory_mb,
            p.memory_growth() * 100.0
        );
    }
    if let Some(m) = &o.oxn_metrics {
        let _ = writeln!(s, "## oxn metrics\n");
        for (k, v) in m {
            let _ = writeln!(s, "- `{k}` = {v}");
        }
        let _ = writeln!(s);
    }
    let _ = writeln!(s, "## Timeline (per second)\n");
    let _ = writeln!(
        s,
        "| t | add/s | start/s | ok/s | err/s | wait | active | delayed | prio | completed | failed | stream | pickup p50 | pickup p99 | run p99 | add p99 | redis ops/s | mem MB | keys | clients | notes |"
    );
    let _ = writeln!(s, "|{}", "---|".repeat(21));
    let step = (o.rows.len() / 300).max(1);
    for r in o.rows.iter().step_by(step) {
        let _ = writeln!(
            s,
            "| {:.0} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {} | {:.1} | {} | {} | {} |",
            r.t_s,
            r.added_ps,
            r.started_ps,
            r.ok_ps,
            r.err_ps,
            r.wait,
            r.active,
            r.delayed,
            r.prioritized,
            r.completed,
            r.failed,
            r.stream_len,
            r.pickup_p50_ms,
            r.pickup_p99_ms,
            r.run_p99_ms,
            r.add_p99_ms,
            r.redis_ops,
            r.used_memory_mb,
            r.keys,
            r.clients,
            r.notes.join("; ").replace('|', "/")
        );
    }
    s
}
