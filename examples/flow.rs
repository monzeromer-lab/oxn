//! A flow: a parent job that runs once all of its children have finished,
//! with their return values.
//!
//! ```text
//! cargo run --example flow --features flow
//! REDIS_URL=redis://127.0.0.1:6390 cargo run --example flow --features flow
//! ```
//!
//! Three `extract` children (in their own queue) each count the words of a
//! page; the `report` parent then adds the counts up. The parent sits in
//! `waiting-children` until the last child completes.

use std::time::Duration;

use oxn::prelude::*;
use oxn::{FlowNode, FlowProducer};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Report {
    title: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Page {
    text: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());

    let reports: Queue<Report> = Queue::builder("flow-demo-reports")
        .redis(url)
        .build()
        .await?;
    // Same backend (and prefix) for every queue of the flow.
    let pages: Queue<Page> = Queue::builder("flow-demo-pages")
        .backend(reports.backend())
        .build()
        .await?;

    let page = |text: &str| Page { text: text.into() };
    let tree = FlowNode::new(
        "flow-demo-reports",
        "report",
        &Report {
            title: "weekly".into(),
        },
        JobOptions::new(),
    )?
    .child(FlowNode::new(
        "flow-demo-pages",
        "extract",
        &page("the quick brown fox"),
        JobOptions::new(),
    )?)
    .child(FlowNode::new(
        "flow-demo-pages",
        "extract",
        &page("jumps over"),
        JobOptions::new(),
    )?)
    .child(
        FlowNode::new(
            "flow-demo-pages",
            "extract",
            &page("the lazy dog"),
            JobOptions::new().attempts(3),
        )?
        // If this child fails for good, fail the report too instead of
        // leaving it waiting for a manual retry.
        .fail_parent_on_failure(),
    );

    let flow = FlowProducer::new(reports.backend()).add(tree).await?;
    println!(
        "added {}/{} with children {:?}",
        flow.queue,
        flow.id,
        flow.children
            .iter()
            .map(|c| format!("{}/{}", c.queue, c.id))
            .collect::<Vec<_>>()
    );
    println!(
        "{} report(s) waiting for children",
        reports.counts().await?.waiting_children
    );

    // Children: count words.
    let page_worker = Worker::builder(pages.clone(), |job: Job<Page>| async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Ok::<_, Error>(job.data.text.split_whitespace().count())
    })
    .concurrency(3)
    .build();

    // Parent: add up what the children returned.
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<usize>(1);
    let report_worker =
        Worker::builder_with_ctx(reports.clone(), move |job: Job<Report>, ctx: JobContext| {
            let done_tx = done_tx.clone();
            async move {
                let counts: Vec<(JobRef, usize)> = ctx.children_values().await?;
                for (child, n) in &counts {
                    println!("  child {}/{} counted {n} words", child.queue, child.id);
                }
                let total: usize = counts.iter().map(|(_, n)| n).sum();
                println!("report {:?}: {total} words", job.data.title);
                let _ = done_tx.send(total).await;
                Ok::<_, Error>(total)
            }
        })
        .build();

    let stop_pages = page_worker.cancellation_token();
    let stop_reports = report_worker.cancellation_token();
    let workers = tokio::spawn(async move {
        let _ = tokio::join!(page_worker.run(), report_worker.run());
    });

    let total = tokio::time::timeout(Duration::from_secs(30), done_rx.recv()).await?;
    assert_eq!(total, Some(9));

    stop_pages.cancel();
    stop_reports.cancel();
    workers.await?;
    reports.obliterate(true).await?;
    pages.obliterate(true).await?;
    Ok(())
}
