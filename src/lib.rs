//! ArcDB - A simple relational database engine written in Rust
//!
//! This library provides the core components for a SQL database:
//! - SQL parsing (lexer, parser, AST)
//! - Storage engine (pages, buffer pool, heap files, B+ tree)
//! - Query execution (planner, executor)
//! - System catalog
//! - TCP server

pub mod catalog;
pub mod error;
pub mod executor;
pub mod server;
pub mod sql;
pub mod storage;
pub mod transaction;

pub use error::{Error, Result};
