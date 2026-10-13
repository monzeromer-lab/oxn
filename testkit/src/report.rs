//! Timeline report rendering (`timeline.md` + `timeline.json`).
//!
//! Layout of `timeline.md`:
//! 1. header — test, queue, verdict, violation counts;
//! 2. step table — one row per step with offset, duration, container
//!    summary, event/trace counts and the worst violation;
//! 3. step details — result, stream events, oxn traces, Redis commands and
//!    violations for each step, timestamps relative to the recorder start;
//! 4. per-job history — every event, trace and location change for each
//!    job, merged and sorted by time: the "what happened to job 7" view;
//! 5. final snapshot — raw JSON of the last Redis state.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use crate::invariants::Severity;
use crate::recorder::Recorder;

/// Root directory reports are written under: `OXN_REPORT_DIR`, else
/// `<workspace>/target/oxn-reports`.
pub fn report_root() -> PathBuf {
    if let Ok(d) = std::env::var("OXN_REPORT_DIR") {
        return PathBuf::from(d);
    }
    let start = std::env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_default());
    let mut dir: &Path = &start;
    loop {
        if dir.join("Cargo.lock").exists() {
            return dir.join("target").join("oxn-reports");
        }
        match dir.parent() {
            Some(p) => dir = p,
            None => return start.join("target").join("oxn-reports"),
        }
    }
}

/// Make `name` safe to use as a directory name.
pub fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

pub(crate) fn write(rec: &Recorder) -> PathBuf {
    let dir = report_root().join(sanitize(&rec.test));
    let _ = std::fs::create_dir_all(&dir);
    let md = dir.join("timeline.md");
    let json = dir.join("timeline.json");
    let _ = std::fs::write(&md, render_markdown(rec));
    let body = serde_json::json!({
        "test": rec.test,
        "queue": rec.queue,
        "prefix": rec.prefix,
        "started_ms": rec.started_ms,
        "steps": rec.steps,
        "final_snapshot": rec.last,
    });
    let _ = std::fs::write(
        &json,
        serde_json::to_string_pretty(&body).unwrap_or_default(),
    );
    md
}

fn rel_ms(rec: &Recorder, epoch_us: u64) -> f64 {
    epoch_us as f64 / 1000.0 - rec.started_ms as f64
}

fn sev_icon(s: Severity) -> &'static str {
    match s {
        Severity::Error => "❌",
        Severity::Warning => "⚠️",
        Severity::Transient => "⏳",
    }
}

fn render_markdown(rec: &Recorder) -> String {
    let mut out = String::new();
    let errors = rec.violations(Severity::Error).len();
    let warnings = rec.violations(Severity::Warning).len() - errors;
    let verdict = if errors > 0 { "FAIL" } else { "PASS" };
    let _ = writeln!(out, "# oxn timeline — `{}`\n", rec.test);
    let _ = writeln!(
        out,
        "queue `{}` · prefix `{}` · {} steps · **{verdict}** ({errors} error(s), {warnings} warning(s))\n",
        rec.queue,
        rec.prefix,
        rec.steps.len()
    );

    let _ = writeln!(out, "## Steps\n");
    let _ = writeln!(
        out,
        "| # | t+ms | step | took | state after | events | traces | worst violation |"
    );
    let _ = writeln!(out, "|---|---|---|---|---|---|---|---|");
    for s in &rec.steps {
        let worst = s
            .violations
            .iter()
            .max_by_key(|v| v.severity)
            .map(|v| format!("{} `{}`", sev_icon(v.severity), v.rule))
            .unwrap_or_else(|| "✅".into());
        let _ = writeln!(
            out,
            "| {} | {:.1} | {} | {:.1}ms | {} | {} | {} | {} |",
            s.index,
            s.at_ms,
            s.label.replace('|', "\\|"),
            s.took_ms,
            s.summary,
            s.events.len(),
            s.traces.len(),
            worst
        );
    }

    let _ = writeln!(out, "\n## Step details\n");
    for s in &rec.steps {
        let _ = writeln!(
            out,
            "### {}. {} — t+{:.1}ms, took {:.1}ms\n",
            s.index, s.label, s.at_ms, s.took_ms
        );
        if let Some(r) = &s.result {
            let _ = writeln!(out, "**Result:** `{r}`\n");
        }
        let _ = writeln!(out, "**State after:** {}\n", s.summary);
        if !s.violations.is_empty() {
            let _ = writeln!(out, "**Violations:**\n");
            for v in &s.violations {
                let _ = writeln!(out, "- {} {v}", sev_icon(v.severity));
            }
            let _ = writeln!(out);
        }
        if !s.events.is_empty() {
            let _ = writeln!(
                out,
                "<details><summary>{} stream event(s)</summary>\n",
                s.events.len()
            );
            for e in &s.events {
                let _ = writeln!(
                    out,
                    "- `+{:.1}ms` {}",
                    e.at_ms as f64 - rec.started_ms as f64,
                    e.render()
                );
            }
            let _ = writeln!(out, "\n</details>\n");
        }
        if !s.traces.is_empty() {
            let _ = writeln!(
                out,
                "<details><summary>{} oxn trace record(s)</summary>\n",
                s.traces.len()
            );
            for t in &s.traces {
                let job = t
                    .job_id
                    .as_deref()
                    .map(|j| format!(" job={j}"))
                    .unwrap_or_default();
                let _ = writeln!(
                    out,
                    "- `+{:.1}ms`{job} {}",
                    rel_ms(rec, t.at_us),
                    t.render()
                );
            }
            let _ = writeln!(out, "\n</details>\n");
        }
        if !s.commands.is_empty() {
            let _ = writeln!(
                out,
                "<details><summary>{} Redis command(s)</summary>\n",
                s.commands.len()
            );
            for c in &s.commands {
                let src = if c.from_lua { "  lua" } else { "" };
                let _ = writeln!(out, "- `+{:.1}ms`{src} `{}`", rel_ms(rec, c.at_us), c.text);
            }
            let _ = writeln!(out, "\n</details>\n");
        }
    }

    let _ = writeln!(out, "## Per-job history\n");
    for (job, lines) in per_job(rec) {
        let _ = writeln!(out, "### job `{job}`\n");
        for (_, l) in lines {
            let _ = writeln!(out, "- {l}");
        }
        let _ = writeln!(out, "- **final:** {}\n", rec.last.location(&job));
    }

    let _ = writeln!(
        out,
        "## Final snapshot\n\nSee `timeline.json` → `final_snapshot`. Summary: {}",
        rec.last.summary()
    );
    out
}

fn per_job(rec: &Recorder) -> BTreeMap<String, Vec<(f64, String)>> {
    let mut by: BTreeMap<String, Vec<(f64, String)>> = BTreeMap::new();
    let mut last_loc: BTreeMap<String, String> = BTreeMap::new();
    for s in &rec.steps {
        for e in &s.events {
            if let Some(j) = &e.job_id {
                let t = e.at_ms as f64 - rec.started_ms as f64;
                by.entry(j.clone())
                    .or_default()
                    .push((t, format!("`+{t:.1}ms` event **{}**", e.render())));
            }
        }
        for tr in &s.traces {
            if let Some(j) = &tr.job_id {
                let t = rel_ms(rec, tr.at_us);
                by.entry(j.clone())
                    .or_default()
                    .push((t, format!("`+{t:.1}ms` {}", tr.render())));
            }
        }
        for (j, loc) in &s.locations {
            if last_loc.get(j) != Some(loc) {
                last_loc.insert(j.clone(), loc.clone());
                let t = s.at_ms + s.took_ms;
                by.entry(j.clone()).or_default().push((
                    t,
                    format!(
                        "`+{t:.1}ms` after step {} ({}): in **{loc}**",
                        s.index, s.label
                    ),
                ));
            }
        }
        for v in &s.violations {
            if let Some(j) = &v.job_id {
                let t = s.at_ms + s.took_ms;
                by.entry(j.clone())
                    .or_default()
                    .push((t, format!("`+{t:.1}ms` {} {v}", sev_icon(v.severity))));
            }
        }
    }
    for lines in by.values_mut() {
        lines.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        lines.dedup_by(|a, b| a.1 == b.1);
    }
    by
}
