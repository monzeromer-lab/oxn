//! oxn's integration tests, compiled into **one** test binary.
//!
//! Each file used to be its own `tests/it_*.rs` target, so every run linked
//! ~25 binaries against the whole dependency graph — several GB of linker
//! output per change on a slow disk. One crate, one link. Run a subset with
//! the usual filters, e.g. `cargo test --test it shutdown::` or
//! `cargo test --test it -- pickup_latency`.
//!
//! All tests share one process now; each isolates itself with a unique
//! queue name (see `common`), and the few that touch server-global state
//! (`SCRIPT FLUSH`, `CONFIG RESETSTAT`) only assert lower bounds or take
//! `script_preload`'s lock.

mod common;

mod add_and_complete;
mod bulk;
mod bulk_atomic;
mod clean_and_promote_all;
mod concurrency;
mod dashboard;
mod dashboard_actix;
mod dedup;
mod delayed;
mod drain_obliterate;
mod event_stream;
mod events;
mod explicit_ids;
mod flow;
mod handler_panics;
mod job_defaults;
mod obliterate_scope;
mod pause_resume;
mod pickup_latency;
mod priority;
mod retention_leaks;
mod retry;
mod scheduler;
mod script_errors;
mod script_preload;
mod shutdown;
mod stacktraces;
mod stalled;
mod stalled_lifecycle;
mod state_machine;
