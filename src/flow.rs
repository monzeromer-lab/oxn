//! Parent/child job flows.
//!
//! A [`FlowProducer`] adds a tree of jobs in **one atomic step**. A job
//! with children waits in its queue's `waiting-children` state until every
//! child has resolved, then becomes ready like any other job (its priority,
//! its own `delay` and the queue's pause state apply). Children may live in
//! other queues under the same backend prefix.
//!
//! ```no_run
//! # use oxn::prelude::*;
//! # use oxn::flow::{FlowNode, FlowProducer};
//! # async fn demo(queue: Queue<serde_json::Value>) -> oxn::Result<()> {
//! let flow = FlowProducer::new(queue.backend());
//! let report = FlowNode::new("reports", "monthly", &"2026-09", JobOptions::new())?
//!     .child(FlowNode::new("fetch", "sales", &"sales", JobOptions::new())?)
//!     .child(FlowNode::new("fetch", "costs", &"costs", JobOptions::new())?.fail_parent_on_failure());
//! let added = flow.add(report).await?;
//! println!("parent {} waits for {} children", added.id, added.children.len());
//! # Ok(()) }
//! ```
//!
//! # How children resolve
//!
//! - **Completed:** the child's return value is recorded with the parent
//!   (read it with [`JobContext::children_values`](crate::JobContext::children_values)
//!   or [`Queue::dependencies`](crate::Queue::dependencies)).
//! - **Failed for good** (attempts exhausted, unrecoverable, stall limit),
//!   by the child's [`OnFailure`]:
//!   - [`OnFailure::Wait`] (default): the parent keeps waiting; retrying the
//!     child and letting it complete releases the parent.
//!   - [`OnFailure::FailParent`]: the parent fails too (if it is still
//!     waiting for its children), and so on up the tree.
//!   - [`OnFailure::Ignore`]: the dependency is dropped and the failure is
//!     listed in [`Dependencies::failed`](crate::backend::Dependencies).
//! - **Deleted** (`remove`, `clean`, `drain`, a retention trim): the
//!   dependency is resolved; the parent no longer waits for it.
//!
//! Removing a parent removes its whole tree. A handler can also add
//! children to its own job at run time with [`FlowProducer::add_under`]
//! and then return [`Error::WaitingChildren`]
//! to run again once they resolve.
//!
//! Job options are resolved with the built-in defaults: a `FlowProducer`
//! does not see the queue defaults configured on a [`Queue`](crate::Queue).

use std::collections::BTreeSet;
use std::sync::Arc;

use serde::Serialize;
use serde_json::Value;

use crate::backend::{Backend, FlowJobInsert, JobInsert};
use crate::error::{Error, Result};
use crate::job::{JobId, JobRef};
use crate::options::JobOptions;

/// What a child's permanent failure does to its parent (see the
/// [module docs](self)).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OnFailure {
    /// The parent keeps waiting for this child (BullMQ's default).
    #[default]
    Wait,
    /// The parent fails too.
    FailParent,
    /// The dependency is dropped; the parent runs without this child.
    Ignore,
}

/// A node in a flow tree.
#[derive(Debug, Clone)]
pub struct FlowNode {
    /// Name of the queue this node is submitted to.
    pub queue: String,
    /// Logical name of the job.
    pub name: String,
    /// JSON payload.
    pub data: Value,
    /// Job-level options (priority, delay, attempts, …). Deduplication is
    /// not supported in flows.
    pub opts: JobOptions,
    /// Children this node waits for.
    pub children: Vec<FlowNode>,
    /// What this node's permanent failure does to its parent.
    pub on_failure: OnFailure,
}

impl FlowNode {
    /// A node with `data` JSON-encoded.
    pub fn new<D: Serialize>(
        queue: impl Into<String>,
        name: impl Into<String>,
        data: &D,
        opts: JobOptions,
    ) -> Result<Self> {
        Ok(Self {
            queue: queue.into(),
            name: name.into(),
            data: serde_json::to_value(data)?,
            opts,
            children: Vec::new(),
            on_failure: OnFailure::Wait,
        })
    }

    /// Append a child.
    #[must_use]
    pub fn child(mut self, node: FlowNode) -> Self {
        self.children.push(node);
        self
    }

    /// If this job fails for good, fail its parent too.
    #[must_use]
    pub fn fail_parent_on_failure(mut self) -> Self {
        self.on_failure = OnFailure::FailParent;
        self
    }

    /// If this job fails for good, let its parent run without it.
    #[must_use]
    pub fn ignore_dependency_on_failure(mut self) -> Self {
        self.on_failure = OnFailure::Ignore;
        self
    }
}

/// A job a flow added, with the jobs added below it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowJob {
    /// The job's id.
    pub id: JobId,
    /// The job's queue.
    pub queue: String,
    /// The job's name.
    pub name: String,
    /// Its children, in the order they were given.
    pub children: Vec<FlowJob>,
}

impl FlowJob {
    /// This job as a [`JobRef`].
    pub fn job_ref(&self) -> JobRef {
        JobRef::new(self.queue.clone(), self.id.clone())
    }
}

/// Submits flow trees.
#[derive(Debug, Clone)]
pub struct FlowProducer {
    backend: Arc<dyn Backend>,
}

impl FlowProducer {
    /// A producer on `backend` (e.g. [`Queue::backend`](crate::Queue::backend)).
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// Add a whole flow atomically: every job is written, or (on an error)
    /// none. An explicit id that already names a job fails the flow with
    /// [`ScriptError::JobExists`](crate::error::ScriptError::JobExists).
    pub async fn add(&self, node: FlowNode) -> Result<FlowJob> {
        self.submit(node, None).await
    }

    /// Add a flow whose root becomes a child of `parent`, which must be
    /// active (typically: called from its own handler, followed by
    /// returning [`Error::WaitingChildren`]) or already waiting for
    /// children.
    pub async fn add_under(&self, parent: &JobRef, node: FlowNode) -> Result<FlowJob> {
        self.submit(node, Some(parent)).await
    }

    async fn submit(&self, node: FlowNode, parent: Option<&JobRef>) -> Result<FlowJob> {
        let mut flat = Vec::new();
        let mut shape = Vec::new();
        flatten(node, None, &mut flat, &mut shape)?;
        let mut explicit = BTreeSet::new();
        for n in &flat {
            if let Some(id) = &n.insert.opts.id
                && !explicit.insert((n.queue.clone(), id.clone()))
            {
                return Err(Error::Config(format!(
                    "flow uses job id `{id}` twice in queue `{}`",
                    n.queue
                )));
            }
        }
        let ids = self.backend.add_flow(flat, parent).await?;
        if ids.len() != shape.len() {
            return Err(Error::Config(format!(
                "backend returned {} ids for {} flow nodes",
                ids.len(),
                shape.len()
            )));
        }
        Ok(rebuild(0, &shape, &ids))
    }
}

/// `(queue, name, child indexes)` per flattened node.
type Shape = Vec<(String, String, Vec<usize>)>;

/// Pre-order: a parent is always before its children.
fn flatten(
    node: FlowNode,
    parent: Option<usize>,
    out: &mut Vec<FlowJobInsert>,
    shape: &mut Shape,
) -> Result<()> {
    let FlowNode {
        queue,
        name,
        data,
        opts,
        children,
        on_failure,
    } = node;
    crate::validate::queue_name(&queue)?;
    let opts = opts.resolve();
    if let Some(id) = &opts.id {
        crate::validate::job_id(id)?;
    }
    if opts.deduplication.is_some() {
        return Err(Error::Config(format!(
            "flow node `{name}` in `{queue}`: deduplication is not supported in flows"
        )));
    }
    let index = out.len();
    if let Some(p) = parent {
        shape[p].2.push(index);
    }
    shape.push((queue.clone(), name.clone(), Vec::new()));
    out.push(FlowJobInsert {
        queue,
        insert: JobInsert {
            name: opts.name.clone().unwrap_or(name),
            data: serde_json::to_string(&data)?,
            opts,
            timestamp_ms: now_ms(),
        },
        parent,
        children: children.len(),
        fail_parent_on_failure: parent.is_some() && on_failure == OnFailure::FailParent,
        ignore_dependency_on_failure: parent.is_some() && on_failure == OnFailure::Ignore,
    });
    for child in children {
        flatten(child, Some(index), out, shape)?;
    }
    Ok(())
}

fn rebuild(i: usize, shape: &Shape, ids: &[JobId]) -> FlowJob {
    let (queue, name, kids) = &shape[i];
    FlowJob {
        id: ids[i].clone(),
        queue: queue.clone(),
        name: name.clone(),
        children: kids.iter().map(|&k| rebuild(k, shape, ids)).collect(),
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Task {
        step: u32,
    }

    fn node(q: &str, name: &str) -> FlowNode {
        FlowNode::new(q, name, &Task { step: 0 }, JobOptions::default()).unwrap()
    }

    #[test]
    fn tree_builds_with_depth() {
        let root = node("parent", "root")
            .child(node("child", "a"))
            .child(node("child", "b").child(node("grand", "c")));
        assert_eq!(root.queue, "parent");
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[1].children[0].queue, "grand");
    }

    #[test]
    fn data_encodes_to_json() {
        let n = FlowNode::new("q", "j", &Task { step: 9 }, JobOptions::default()).unwrap();
        assert_eq!(n.data["step"], 9);
    }

    #[test]
    fn flattening_is_preorder_with_parent_links() {
        let root = node("p", "root")
            .child(node("c", "a").fail_parent_on_failure())
            .child(node("c", "b").child(node("g", "x").ignore_dependency_on_failure()))
            .fail_parent_on_failure(); // meaningless on a root: ignored
        let (mut flat, mut shape) = (Vec::new(), Vec::new());
        flatten(root, None, &mut flat, &mut shape).unwrap();
        let names: Vec<_> = flat.iter().map(|n| n.insert.name.as_str()).collect();
        assert_eq!(names, ["root", "a", "b", "x"]);
        let parents: Vec<_> = flat.iter().map(|n| n.parent).collect();
        assert_eq!(parents, [None, Some(0), Some(0), Some(2)]);
        let kids: Vec<_> = flat.iter().map(|n| n.children).collect();
        assert_eq!(kids, [2, 0, 1, 0]);
        assert!(!flat[0].fail_parent_on_failure);
        assert!(flat[1].fail_parent_on_failure && !flat[1].ignore_dependency_on_failure);
        assert!(flat[3].ignore_dependency_on_failure);
        let ids: Vec<JobId> = (1..=4).map(|i| JobId::new(i.to_string())).collect();
        let tree = rebuild(0, &shape, &ids);
        assert_eq!(tree.children[1].children[0].id.as_str(), "4");
        assert_eq!(tree.children[1].children[0].queue, "g");
    }

    #[test]
    fn dedup_and_bad_names_are_rejected_before_anything_is_sent() {
        let (mut flat, mut shape) = (Vec::new(), Vec::new());
        let bad = node("p", "root").child(
            FlowNode::new(
                "c",
                "a",
                &1,
                JobOptions::new().deduplicate("d", std::time::Duration::from_secs(1)),
            )
            .unwrap(),
        );
        assert!(matches!(
            flatten(bad, None, &mut flat, &mut shape),
            Err(Error::Config(_))
        ));
        let (mut flat, mut shape) = (Vec::new(), Vec::new());
        assert!(flatten(node("bad:queue", "r"), None, &mut flat, &mut shape).is_err());
    }
}
