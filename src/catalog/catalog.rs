//! System Catalog for ArcDB
//!
//! This module manages metadata about tables, indexes, and other database objects.

use super::schema::{Column, IndexDef, Schema, TableDef, TableStatistics};
use super::types::DataType;
use crate::error::{Error, Result};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// System Catalog - manages all database metadata
#[derive(Debug)]
pub struct Catalog {
    /// Table definitions by name
    tables: RwLock<HashMap<String, Arc<TableDef>>>,
    /// Index definitions by name
    indexes: RwLock<HashMap<String, Arc<IndexDef>>>,
    /// Next table ID
    next_table_id: RwLock<u32>,
    /// Next index ID
    next_index_id: RwLock<u32>,
}

impl Catalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            indexes: RwLock::new(HashMap::new()),
            next_table_id: RwLock::new(1),
            next_index_id: RwLock::new(1),
        }
    }

    /// Create a new table
    pub fn create_table(&self, name: &str, schema: Schema) -> Result<Arc<TableDef>> {
        let mut tables = self.tables.write().unwrap();

        if tables.contains_key(name) {
            return Err(Error::TableAlreadyExists(name.to_string()));
        }

        let mut next_id = self.next_table_id.write().unwrap();
        let table_def = Arc::new(TableDef::new(name, schema, *next_id));
        *next_id += 1;

        tables.insert(name.to_string(), table_def.clone());
        Ok(table_def)
    }

    /// Get a table by name
    pub fn get_table(&self, name: &str) -> Result<Arc<TableDef>> {
        let tables = self.tables.read().unwrap();
        tables
            .get(name)
            .cloned()
            .ok_or_else(|| Error::TableNotFound(name.to_string()))
    }

    /// Check if a table exists
    pub fn table_exists(&self, name: &str) -> bool {
        let tables = self.tables.read().unwrap();
        tables.contains_key(name)
    }

    /// Drop a table
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let mut tables = self.tables.write().unwrap();

        if tables.remove(name).is_none() {
            return Err(Error::TableNotFound(name.to_string()));
        }

        // Also drop all indexes on this table
        let mut indexes = self.indexes.write().unwrap();
        indexes.retain(|_, idx| idx.table_name != name);

        Ok(())
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().unwrap();
        tables.keys().cloned().collect()
    }

    /// Update table statistics
    pub fn update_table_stats(&self, name: &str, stats: TableStatistics) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        let table = tables
            .get(name)
            .ok_or_else(|| Error::TableNotFound(name.to_string()))?;

        let mut new_table = (**table).clone();
        new_table.stats = Some(stats);
        tables.insert(name.to_string(), Arc::new(new_table));

        Ok(())
    }

    /// Create an index
    pub fn create_index(
        &self,
        name: &str,
        table_name: &str,
        columns: Vec<String>,
        unique: bool,
    ) -> Result<Arc<IndexDef>> {
        // Verify table exists
        let table = self.get_table(table_name)?;

        // Verify all columns exist
        for col_name in &columns {
            if table.get_column(col_name).is_none() {
                return Err(Error::ColumnNotFound(
                    col_name.clone(),
                    table_name.to_string(),
                ));
            }
        }

        let mut indexes = self.indexes.write().unwrap();

        if indexes.contains_key(name) {
            return Err(Error::IndexAlreadyExists(name.to_string()));
        }

        let mut next_id = self.next_index_id.write().unwrap();
        let index_def = Arc::new(IndexDef::new(name, table_name, columns, *next_id).unique(unique));
        *next_id += 1;

        indexes.insert(name.to_string(), index_def.clone());
        Ok(index_def)
    }

    /// Get an index by name
    pub fn get_index(&self, name: &str) -> Result<Arc<IndexDef>> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .get(name)
            .cloned()
            .ok_or_else(|| Error::IndexNotFound(name.to_string()))
    }

    /// Drop an index
    pub fn drop_index(&self, name: &str) -> Result<()> {
        let mut indexes = self.indexes.write().unwrap();

        if indexes.remove(name).is_none() {
            return Err(Error::IndexNotFound(name.to_string()));
        }

        Ok(())
    }

    /// Get all indexes for a table
    pub fn get_table_indexes(&self, table_name: &str) -> Vec<Arc<IndexDef>> {
        let indexes = self.indexes.read().unwrap();
        indexes
            .values()
            .filter(|idx| idx.table_name == table_name)
            .cloned()
            .collect()
    }

    /// List all index names
    pub fn list_indexes(&self) -> Vec<String> {
        let indexes = self.indexes.read().unwrap();
        indexes.keys().cloned().collect()
    }

    /// Get table schema info as a formatted string (for .schema command)
    pub fn get_table_info(&self, name: &str) -> Result<String> {
        let table = self.get_table(name)?;
        let mut info = format!("Table: {}\n", table.name());
        info.push_str("Columns:\n");

        for col in table.schema().columns() {
            let mut flags = Vec::new();
            if col.primary_key {
                flags.push("PRIMARY KEY");
            }
            if !col.nullable {
                flags.push("NOT NULL");
            }
            if col.unique {
                flags.push("UNIQUE");
            }

            let flags_str = if flags.is_empty() {
                String::new()
            } else {
                format!(" [{}]", flags.join(", "))
            };

            info.push_str(&format!("  {} {}{}\n", col.name, col.data_type, flags_str));
        }

        // Add index info
        let indexes = self.get_table_indexes(name);
        if !indexes.is_empty() {
            info.push_str("Indexes:\n");
            for idx in indexes {
                info.push_str(&format!(
                    "  {} ({}){}\n",
                    idx.name,
                    idx.columns.join(", "),
                    if idx.unique { " UNIQUE" } else { "" }
                ));
            }
        }

        Ok(info)
    }

    /// Save catalog to disk
    pub fn save_to_disk(&self, path: &str) -> Result<()> {
        let data = CatalogData {
            tables: self
                .tables
                .read()
                .unwrap()
                .values()
                .map(|t| (**t).clone())
                .collect(),
            indexes: self
                .indexes
                .read()
                .unwrap()
                .values()
                .map(|i| (**i).clone())
                .collect(),
            next_table_id: *self.next_table_id.read().unwrap(),
            next_index_id: *self.next_index_id.read().unwrap(),
        };

        let json =
            serde_json::to_string_pretty(&data).map_err(|e| Error::Internal(e.to_string()))?;
        std::fs::write(path, json).map_err(|e| Error::IoError(e))?;
        Ok(())
    }

    /// Load catalog from disk
    pub fn load_from_disk(path: &str) -> Result<Self> {
        let json = std::fs::read_to_string(path).map_err(|e| Error::IoError(e))?;
        let data: CatalogData =
            serde_json::from_str(&json).map_err(|e| Error::Internal(e.to_string()))?;

        let mut tables = HashMap::new();
        for table in data.tables {
            tables.insert(table.name.clone(), Arc::new(table));
        }

        let mut indexes = HashMap::new();
        for index in data.indexes {
            indexes.insert(index.name.clone(), Arc::new(index));
        }

        Ok(Self {
            tables: RwLock::new(tables),
            indexes: RwLock::new(indexes),
            next_table_id: RwLock::new(data.next_table_id),
            next_index_id: RwLock::new(data.next_index_id),
        })
    }
}

/// Serializable proxy for Catalog
#[derive(serde::Serialize, serde::Deserialize)]
struct CatalogData {
    tables: Vec<TableDef>,
    indexes: Vec<IndexDef>,
    next_table_id: u32,
    next_index_id: u32,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating tables with a fluent API
pub struct TableBuilder {
    name: String,
    columns: Vec<Column>,
}

impl TableBuilder {
    /// Start building a new table
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
        }
    }

    /// Add a column
    pub fn column(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let position = self.columns.len();
        self.columns.push(Column::new(name, data_type, position));
        self
    }

    /// Add a primary key column (INTEGER PRIMARY KEY)
    pub fn primary_key(mut self, name: impl Into<String>) -> Self {
        let position = self.columns.len();
        self.columns
            .push(Column::new(name, DataType::Integer, position).primary_key(true));
        self
    }

    /// Add a NOT NULL column
    pub fn column_not_null(mut self, name: impl Into<String>, data_type: DataType) -> Self {
        let position = self.columns.len();
        self.columns
            .push(Column::new(name, data_type, position).nullable(false));
        self
    }

    /// Build the table in the catalog
    pub fn build(self, catalog: &Catalog) -> Result<Arc<TableDef>> {
        let schema = Schema::from_columns(self.columns);
        catalog.create_table(&self.name, schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_table() {
        let catalog = Catalog::new();

        let mut schema = Schema::new();
        schema.add_column(Column::new("id", DataType::Integer, 0).primary_key(true));
        schema.add_column(Column::new("name", DataType::Varchar(100), 1));

        let table = catalog.create_table("users", schema).unwrap();

        assert_eq!(table.name(), "users");
        assert_eq!(table.schema().column_count(), 2);

        let retrieved = catalog.get_table("users").unwrap();
        assert_eq!(retrieved.name(), "users");
    }

    #[test]
    fn test_table_already_exists() {
        let catalog = Catalog::new();

        catalog.create_table("test", Schema::new()).unwrap();

        let result = catalog.create_table("test", Schema::new());
        assert!(matches!(result, Err(Error::TableAlreadyExists(_))));
    }

    #[test]
    fn test_drop_table() {
        let catalog = Catalog::new();

        catalog.create_table("test", Schema::new()).unwrap();
        assert!(catalog.table_exists("test"));

        catalog.drop_table("test").unwrap();
        assert!(!catalog.table_exists("test"));
    }

    #[test]
    fn test_table_builder() {
        let catalog = Catalog::new();

        let table = TableBuilder::new("posts")
            .primary_key("id")
            .column_not_null("title", DataType::Varchar(200))
            .column("content", DataType::Text)
            .build(&catalog)
            .unwrap();

        assert_eq!(table.name(), "posts");
        assert_eq!(table.schema().column_count(), 3);

        let id_col = table.get_column("id").unwrap();
        assert!(id_col.primary_key);
    }

    #[test]
    fn test_create_index() {
        let catalog = Catalog::new();

        let mut schema = Schema::new();
        schema.add_column(Column::new("id", DataType::Integer, 0));
        schema.add_column(Column::new("email", DataType::Varchar(255), 1));

        catalog.create_table("users", schema).unwrap();

        let index = catalog
            .create_index("idx_users_email", "users", vec!["email".to_string()], true)
            .unwrap();

        assert_eq!(index.name, "idx_users_email");
        assert!(index.unique);

        let indexes = catalog.get_table_indexes("users");
        assert_eq!(indexes.len(), 1);
    }
}
