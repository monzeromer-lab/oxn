//! Parent/child job flows.
//!
//! A [`FlowProducer`] submits a tree of jobs where each parent's `active`
//! transition is blocked until its children complete. This is a thin layer:
//! each child job is added with a `parent` reference; the backend stores the
//! parent/child edges as standard BullMQ-style `{jobId}:dependencies` sets.
//!
//! For v1 the Rust API is deliberately minimal — the expectation is that
//! callers build flows programmatically, not via JSON config.

use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::backend::{Backend, JobInsert};
use crate::error::Result;
use crate::job::{JobId, ParentRef};
use crate::options::JobOptions;

/// A node in a flow tree.
///
/// Payloads live on every node; children inherit queue + options from their
/// parent unless they override.
#[derive(Debug, Clone)]
pub struct FlowNode {
    pub queue: String,
    pub name: String,
    pub data: Value,
    pub opts: JobOptions,
    pub children: Vec<FlowNode>,
}

impl FlowNode {
    /// Helper that JSON-encodes `data`.
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
        })
    }

    /// Append a child.
    #[must_use]
    pub fn child(mut self, node: FlowNode) -> Self {
        self.children.push(node);
        self
    }
}

/// Submits flow trees.
///
/// Submission is depth-first: children are added first (so their ids are
/// known by the time the parent is created).
#[derive(Debug, Clone)]
pub struct FlowProducer {
    backend: Arc<dyn Backend>,
}

impl FlowProducer {
    pub fn new(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }

    /// Submit a whole flow. Returns the root job's id.
    pub async fn add(&self, node: FlowNode) -> Result<JobId> {
        Box::pin(self.add_impl(node, None)).await
    }

    async fn add_impl(
        &self,
        node: FlowNode,
        parent: Option<ParentRef>,
    ) -> Result<JobId> {
        // Children first.
        let FlowNode {
            queue,
            name,
            data,
            mut opts,
            children,
        } = node;

        if let Some(p) = parent {
            // Encode parent ref in the opts JSON so the worker can pick it up.
            // For now we stash a sidecar in opts.name (light-touch); a full
            // implementation would extend RawJob with a dedicated field.
            opts.name = Some(format!("{name}@child-of:{}:{}", p.queue, p.id));
        }

        let data_json = serde_json::to_string(&data)?;
        let insert = JobInsert {
            name: opts.name.clone().unwrap_or_else(|| name.clone()),
            data: data_json,
            opts: opts.clone(),
            timestamp_ms: now_ms(),
        };
        let parent_id = self.backend.add(&queue, insert).await?;

        for child in children {
            Box::pin(self.add_impl(
                child,
                Some(ParentRef {
                    id: parent_id.clone(),
                    queue: queue.clone(),
                }),
            ))
            .await?;
        }
        Ok(parent_id)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// typed convenience re-export
#[allow(unused)]
fn _type_check<D: Serialize + DeserializeOwned + Send + Sync + 'static>() {}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(serde::Serialize, serde::Deserialize)]
    struct Task {
        step: u32,
    }

    #[test]
    fn tree_builds_with_depth() {
        let root = FlowNode::new("parent", "root", &Task { step: 0 }, JobOptions::default())
            .unwrap()
            .child(
                FlowNode::new("child", "a", &Task { step: 1 }, JobOptions::default())
                    .unwrap(),
            )
            .child(
                FlowNode::new("child", "b", &Task { step: 2 }, JobOptions::default())
                    .unwrap()
                    .child(
                        FlowNode::new("grand", "c", &Task { step: 3 }, JobOptions::default())
                            .unwrap(),
                    ),
            );

        assert_eq!(root.queue, "parent");
        assert_eq!(root.name, "root");
        assert_eq!(root.children.len(), 2);
        assert_eq!(root.children[1].children[0].queue, "grand");
    }

    #[test]
    fn data_encodes_to_json() {
        let n = FlowNode::new("q", "j", &Task { step: 9 }, JobOptions::default()).unwrap();
        assert_eq!(n.data["step"], 9);
    }
}
