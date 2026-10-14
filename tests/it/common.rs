//! Shared test harness. The implementation lives in the `oxn-testkit`
//! workspace crate; this module re-exports it so every integration test
//! keeps calling `common::fresh_queue`, `common::wait_for`, etc.

#![allow(dead_code, unused_imports)] // each integration test only uses a subset

pub use oxn_testkit::*;
