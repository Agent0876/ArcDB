//! Catalog module
//!
//! This module contains the system catalog, schema definitions, and data types.

pub mod catalog;
pub mod schema;
pub mod types;

pub use catalog::Catalog;
pub use schema::{Column, IndexDef, Schema, TableDef, TableStatistics};
pub use types::DataType;
