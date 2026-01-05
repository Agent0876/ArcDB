//! Schema definitions for ArcDB
//!
//! This module defines table schemas and column metadata.

use super::types::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Column definition in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// Column position (0-indexed)
    pub position: usize,
    /// Is this column nullable?
    pub nullable: bool,
    /// Default value expression (as string)
    pub default: Option<String>,
    /// Is this part of the primary key?
    pub primary_key: bool,
    /// Is this column unique?
    pub unique: bool,
}

impl Column {
    /// Create a new column with minimal required fields
    pub fn new(name: impl Into<String>, data_type: DataType, position: usize) -> Self {
        Self {
            name: name.into(),
            data_type,
            position,
            nullable: true,
            default: None,
            primary_key: false,
            unique: false,
        }
    }

    /// Set nullable flag
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set default value
    pub fn default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }

    /// Set primary key flag
    pub fn primary_key(mut self, pk: bool) -> Self {
        self.primary_key = pk;
        if pk {
            self.nullable = false;
        }
        self
    }

    /// Set unique flag
    pub fn unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }
}

/// Table schema - defines the structure of a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// Ordered list of columns
    columns: Vec<Column>,
    /// Column name to index mapping
    name_to_index: HashMap<String, usize>,
}

impl Schema {
    /// Create a new empty schema
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            name_to_index: HashMap::new(),
        }
    }

    /// Create a schema from a list of columns
    pub fn from_columns(columns: Vec<Column>) -> Self {
        let mut schema = Self::new();
        for col in columns {
            schema.add_column(col);
        }
        schema
    }

    /// Add a column to the schema
    pub fn add_column(&mut self, mut column: Column) {
        column.position = self.columns.len();
        self.name_to_index
            .insert(column.name.clone(), column.position);
        self.columns.push(column);
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.name_to_index.get(name).map(|&idx| &self.columns[idx])
    }

    /// Get column by index
    pub fn get_column_by_index(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }

    /// Get column index by name
    pub fn get_column_index(&self, name: &str) -> Option<usize> {
        self.name_to_index.get(name).copied()
    }

    /// Get all columns
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Get number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if column exists
    pub fn has_column(&self, name: &str) -> bool {
        self.name_to_index.contains_key(name)
    }

    /// Get primary key columns
    pub fn primary_key_columns(&self) -> Vec<&Column> {
        self.columns.iter().filter(|c| c.primary_key).collect()
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}
/// Table statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStatistics {
    /// Row count for the table
    pub row_count: usize,
}

/// Table definition - full table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDef {
    /// Table name
    pub name: String,
    /// Table schema
    pub schema: Schema,
    /// Table ID (for internal use)
    pub id: u32,
    /// Table statistics
    pub stats: Option<TableStatistics>,
}

impl TableDef {
    /// Create a new table definition
    pub fn new(name: impl Into<String>, schema: Schema, id: u32) -> Self {
        Self {
            name: name.into(),
            schema,
            id,
            stats: None,
        }
    }

    /// Get the table name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the table schema
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Get column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.schema.get_column(name)
    }
}

/// Index definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    /// Index name
    pub name: String,
    /// Table this index belongs to
    pub table_name: String,
    /// Columns included in the index
    pub columns: Vec<String>,
    /// Is this a unique index?
    pub unique: bool,
    /// Is this the primary key index?
    pub primary: bool,
    /// Index ID
    pub id: u32,
}

impl IndexDef {
    /// Create a new index definition
    pub fn new(
        name: impl Into<String>,
        table_name: impl Into<String>,
        columns: Vec<String>,
        id: u32,
    ) -> Self {
        Self {
            name: name.into(),
            table_name: table_name.into(),
            columns,
            unique: false,
            primary: false,
            id,
        }
    }

    /// Set unique flag
    pub fn unique(mut self, unique: bool) -> Self {
        self.unique = unique;
        self
    }

    /// Set primary flag
    pub fn primary(mut self, primary: bool) -> Self {
        self.primary = primary;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let mut schema = Schema::new();
        schema.add_column(Column::new("id", DataType::Integer, 0).primary_key(true));
        schema.add_column(Column::new("name", DataType::Varchar(100), 1).nullable(false));
        schema.add_column(Column::new("email", DataType::Varchar(255), 2));

        assert_eq!(schema.column_count(), 3);
        assert!(schema.has_column("id"));
        assert!(!schema.has_column("unknown"));

        let id_col = schema.get_column("id").unwrap();
        assert!(id_col.primary_key);
        assert!(!id_col.nullable);
    }

    #[test]
    fn test_table_def() {
        let mut schema = Schema::new();
        schema.add_column(Column::new("id", DataType::Integer, 0).primary_key(true));
        schema.add_column(Column::new("value", DataType::Text, 1));

        let table = TableDef::new("test_table", schema, 1);

        assert_eq!(table.name(), "test_table");
        assert_eq!(table.schema().column_count(), 2);
        assert!(table.get_column("id").is_some());
    }
}
