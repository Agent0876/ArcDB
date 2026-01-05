//! Query Executor for ArcDB
//!
//! This module executes logical plans and returns results.

use serde::Serialize;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::{HeuristicOptimizer, LogicalPlan};
#[cfg(test)]
use crate::catalog::DataType;
use crate::catalog::{Catalog, Column, Schema, TableStatistics};
use crate::error::{Error, Result};
use crate::sql::ast::*;
use crate::storage::btree::IndexKey;
use crate::storage::wal::LogManager;
use crate::storage::{BufferPoolManager, DiskManager, SlotId, Table, Tuple, Value};
use crate::transaction::{LockMode, TransactionManager};

/// Query result
#[derive(Debug, Serialize)]
pub struct QueryResult {
    /// Column names
    pub columns: Vec<String>,
    /// Result rows
    pub rows: Vec<Tuple>,
    /// Number of affected rows (for INSERT/UPDATE/DELETE)
    pub affected_rows: usize,
    /// Message
    pub message: Option<String>,
}

impl QueryResult {
    /// Create a new empty result
    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            message: None,
        }
    }

    /// Create a result with a message
    pub fn with_message(message: impl Into<String>) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: 0,
            message: Some(message.into()),
        }
    }

    /// Create a result with affected rows count
    pub fn with_affected_rows(count: usize, message: impl Into<String>) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected_rows: count,
            message: Some(message.into()),
        }
    }
}

/// Execution Engine
pub struct ExecutionEngine {
    /// System catalog
    catalog: Arc<Catalog>,
    /// Table storage (table_name -> Table)
    tables: HashMap<String, Table>,
    /// Transaction Manager
    transaction_manager: Arc<TransactionManager>,
    /// Current Transaction ID
    current_trans_id: Option<u64>,
    /// Buffer Pool Manager
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
}

impl ExecutionEngine {
    /// Create a new execution engine
    pub fn new(catalog: Arc<Catalog>) -> Result<Self> {
        let log_manager = Arc::new(LogManager::new());
        let transaction_manager = Arc::new(TransactionManager::new(log_manager));

        let data_dir = std::path::PathBuf::from("data");
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir).map_err(Error::IoError)?;
        }
        let disk_manager = Arc::new(DiskManager::new(data_dir));
        let buffer_pool = Arc::new(Mutex::new(BufferPoolManager::new(1024, disk_manager)));

        let mut engine = Self {
            catalog,
            tables: HashMap::new(),
            transaction_manager,
            current_trans_id: None,
            buffer_pool,
        };

        // Automatic recovery on startup
        engine.recover()?;

        Ok(engine)
    }

    /// Execute a logical plan
    pub fn execute(&mut self, plan: LogicalPlan) -> Result<QueryResult> {
        // Optimize the plan
        let optimizer = HeuristicOptimizer::new(&self.tables);
        let plan = optimizer.optimize(plan);

        match plan {
            LogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
            } => self.execute_create_table(&table_name, columns, if_not_exists),
            LogicalPlan::DropTable {
                table_name,
                if_exists,
            } => self.execute_drop_table(&table_name, if_exists),
            LogicalPlan::CreateIndex {
                index_name,
                table_name,
                columns,
                unique,
                if_not_exists,
            } => {
                self.execute_create_index(&index_name, &table_name, columns, unique, if_not_exists)
            }
            LogicalPlan::Insert {
                table_name,
                columns,
                values,
            } => {
                self.ensure_table_loaded(&table_name)?;
                if let Some(trans_id) = self.current_trans_id {
                    if !self.transaction_manager.acquire_lock(
                        &table_name,
                        trans_id,
                        LockMode::Exclusive,
                    )? {
                        return Err(Error::Internal(format!(
                            "Could not acquire lock on table {}",
                            table_name
                        )));
                    }
                }
                self.execute_insert(&table_name, columns, values)
            }
            LogicalPlan::Update {
                table_name,
                assignments,
                predicate,
            } => {
                self.ensure_table_loaded(&table_name)?;
                if let Some(trans_id) = self.current_trans_id {
                    if !self.transaction_manager.acquire_lock(
                        &table_name,
                        trans_id,
                        LockMode::Exclusive,
                    )? {
                        return Err(Error::Internal(format!(
                            "Could not acquire lock on table {}",
                            table_name
                        )));
                    }
                }
                self.execute_update(&table_name, assignments, predicate)
            }
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => self.execute_join(*left, *right, join_type, condition),
            LogicalPlan::HashJoin {
                left,
                right,
                left_key,
                right_key,
            } => self.execute_hash_join(*left, *right, left_key, right_key),
            LogicalPlan::Delete {
                table_name,
                predicate,
            } => {
                self.ensure_table_loaded(&table_name)?;
                if let Some(trans_id) = self.current_trans_id {
                    if !self.transaction_manager.acquire_lock(
                        &table_name,
                        trans_id,
                        LockMode::Exclusive,
                    )? {
                        return Err(Error::Internal(format!(
                            "Could not acquire lock on table {}",
                            table_name
                        )));
                    }
                }
                self.execute_delete(&table_name, predicate)
            }
            LogicalPlan::IndexScan {
                table_name,
                index_name,
                columns: _columns, // Ignored
                op,
                value,
            } => {
                self.ensure_table_loaded(&table_name)?;
                self.execute_index_scan(&table_name, &index_name, &op, &value)
            }
            LogicalPlan::BeginTransaction => self.execute_begin(),
            LogicalPlan::Commit => self.execute_commit(),
            LogicalPlan::Rollback => self.execute_rollback(),
            LogicalPlan::Analyze { table_name } => self.execute_analyze(table_name),
            LogicalPlan::Project { input, expressions } => self.execute_select(*input, expressions),
            LogicalPlan::Scan { table_name, .. } => {
                if !table_name.is_empty() {
                    self.ensure_table_loaded(&table_name)?;
                    // Acquire Shared Lock for Scan
                    if let Some(trans_id) = self.current_trans_id {
                        if !self.transaction_manager.acquire_lock(
                            &table_name,
                            trans_id,
                            LockMode::Shared,
                        )? {
                            return Err(Error::Internal(format!(
                                "Could not acquire lock on table {}",
                                table_name
                            )));
                        }
                    }
                }
                self.execute_scan(&table_name)
            }
            _ => Err(Error::ExecutionError("Unsupported plan type".to_string())),
        }
    }

    pub fn recover(&mut self) -> Result<()> {
        let wal_path = "arcdb.wal";
        if !std::path::Path::new(wal_path).exists() {
            return Ok(());
        }

        println!("Recovery: Starting from WAL...");
        let log_manager = self.transaction_manager.log_manager();
        let records = log_manager.read_from_log(wal_path)?;

        // Pass 1: Analysis - Find committed and active transactions
        let mut committed = std::collections::HashSet::new();
        let mut active = std::collections::HashSet::new();

        for record in &records {
            match record.record_type {
                crate::storage::wal::LogRecordType::Begin => {
                    active.insert(record.trans_id);
                }
                crate::storage::wal::LogRecordType::Commit => {
                    committed.insert(record.trans_id);
                    active.remove(&record.trans_id);
                }
                crate::storage::wal::LogRecordType::Rollback
                | crate::storage::wal::LogRecordType::Abort => {
                    active.remove(&record.trans_id);
                }
                _ => {}
            }
        }

        // Pass 2: Redo committed transactions
        for record in &records {
            if committed.contains(&record.trans_id) {
                if let (Some(ref table_name), Some(slot_id)) = (&record.table_name, record.slot_id)
                {
                    self.ensure_table_loaded(table_name)?;
                    let table = self.tables.get_mut(table_name).unwrap();

                    // Idempotency check: check page LSN
                    if table.get_page_lsn(slot_id.page_id) < record.lsn {
                        match record.record_type {
                            crate::storage::wal::LogRecordType::Insert => {
                                if let Some(after) = &record.after_image {
                                    table.insert(after.clone()).ok();
                                }
                            }
                            crate::storage::wal::LogRecordType::Update => {
                                if let Some(after) = &record.after_image {
                                    table.update(slot_id, after.clone()).ok();
                                }
                            }
                            crate::storage::wal::LogRecordType::Delete => {
                                table.delete(slot_id).ok();
                            }
                            _ => {}
                        }
                        table.set_page_lsn(slot_id.page_id, record.lsn);
                    }
                }
            }
        }

        // Pass 3: Undo uncommitted transactions (active)
        // For simplicity in ArcDB, we assume NO-STEAL, so uncommitted
        // changes usually don't hit disk unless we explicitly flushed.
        // But for completeness, we can undo in reverse order.
        for record in records.iter().rev() {
            if active.contains(&record.trans_id) {
                if let (Some(ref table_name), Some(slot_id)) = (&record.table_name, record.slot_id)
                {
                    self.ensure_table_loaded(table_name)?;
                    let table = self.tables.get_mut(table_name).unwrap();

                    if table.get_page_lsn(slot_id.page_id) >= record.lsn {
                        match record.record_type {
                            crate::storage::wal::LogRecordType::Insert => {
                                table.delete(slot_id).ok();
                            }
                            crate::storage::wal::LogRecordType::Update => {
                                if let Some(before) = &record.before_image {
                                    table.update(slot_id, before.clone()).ok();
                                }
                            }
                            crate::storage::wal::LogRecordType::Delete => {
                                if let Some(before) = &record.before_image {
                                    table.insert(before.clone()).ok();
                                }
                            }
                            _ => {}
                        }
                        // Usually we would log the undo as a CLR (Compensation Log Record)
                        // but here we just update page LSN to prevent re-undoing if we crashed during undo.
                        table.set_page_lsn(slot_id.page_id, record.lsn);
                    }
                }
            }
        }

        println!(
            "Recovery: Finished. Committed: {}, Uncommitted (rolled back): {}",
            committed.len(),
            active.len()
        );
        Ok(())
    }

    /// Helper to ensure a table's storage is loaded in memory
    fn execute_analyze(&mut self, table_name: String) -> Result<QueryResult> {
        self.ensure_table_loaded(&table_name)?;
        let table = self.tables.get_mut(&table_name).unwrap();
        let row_count = table.tuple_count();

        let stats = TableStatistics { row_count };
        self.catalog.update_table_stats(&table_name, stats)?;

        Ok(QueryResult::with_message(format!(
            "Analyzed table {}, found {} rows",
            table_name, row_count
        )))
    }

    fn ensure_table_loaded(&mut self, table_name: &str) -> Result<()> {
        if self.tables.contains_key(table_name) {
            return Ok(());
        }

        // Check if it exists in catalog
        let table_def = self.catalog.get_table(table_name)?;

        // Try to open it from disk
        let path = format!("table_{}.data", table_def.id);
        let mut table = if std::path::Path::new(&path).exists() {
            Table::open(table_def.clone(), path, self.buffer_pool.clone())
                .map_err(|e| Error::Internal(e.to_string()))?
        } else {
            Table::new(table_def.clone(), self.buffer_pool.clone())
        };

        // Load indexes from catalog
        let indexes = self.catalog.get_table_indexes(table_name);
        for index_def in indexes {
            table.load_index(index_def)?;
        }

        self.tables.insert(table_name.to_string(), table);
        Ok(())
    }

    fn execute_begin(&mut self) -> Result<QueryResult> {
        if self.current_trans_id.is_some() {
            return Err(Error::Internal("Transaction already active".to_string()));
        }
        let trans_id = self.transaction_manager.begin()?;
        self.current_trans_id = Some(trans_id);
        Ok(QueryResult::with_message(format!(
            "Transaction {} started",
            trans_id
        )))
    }

    fn execute_commit(&mut self) -> Result<QueryResult> {
        let trans_id = self
            .current_trans_id
            .take()
            .ok_or_else(|| Error::Internal("No active transaction".to_string()))?;

        self.transaction_manager.commit(trans_id)?;

        // Save all indexes
        for table in self.tables.values() {
            table.save_indexes().ok();
        }

        Ok(QueryResult::with_message(format!(
            "Transaction {} committed",
            trans_id
        )))
    }

    fn execute_rollback(&mut self) -> Result<QueryResult> {
        let trans_id = self
            .current_trans_id
            .take()
            .ok_or_else(|| Error::Internal("No active transaction".to_string()))?;

        self.transaction_manager.rollback(trans_id)?;
        Ok(QueryResult::with_message(format!(
            "Transaction {} rolled back",
            trans_id
        )))
    }

    fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    ) -> Result<QueryResult> {
        // Check if table already exists
        if self.catalog.table_exists(table_name) {
            if if_not_exists {
                return Ok(QueryResult::with_message(format!(
                    "Table '{}' already exists",
                    table_name
                )));
            }
            return Err(Error::TableAlreadyExists(table_name.to_string()));
        }

        // Build schema
        let mut schema = Schema::new();
        for (i, col_def) in columns.into_iter().enumerate() {
            let mut column = Column::new(col_def.name.clone(), col_def.data_type, i);
            column = column.nullable(!col_def.not_null);
            column = column.primary_key(col_def.primary_key);
            column = column.unique(col_def.unique);
            schema.add_column(column);
        }

        // Create table in catalog
        let table_def = self.catalog.create_table(table_name, schema)?;

        // Create storage
        let table = Table::new(table_def, self.buffer_pool.clone());
        self.tables.insert(table_name.to_string(), table);

        Ok(QueryResult::with_message(format!(
            "Table '{}' created",
            table_name
        )))
    }

    fn execute_drop_table(&mut self, table_name: &str, if_exists: bool) -> Result<QueryResult> {
        if !self.catalog.table_exists(table_name) {
            if if_exists {
                return Ok(QueryResult::with_message(format!(
                    "Table '{}' does not exist",
                    table_name
                )));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }

        // Drop storage
        self.tables.remove(table_name);

        // Delete files from disk
        let table_id = self.catalog.get_table(table_name).map(|t| t.id).ok();
        if let Some(id) = table_id {
            std::fs::remove_file(format!("table_{}.data", id)).ok();

            // Delete all index files for this table
            if let Ok(entries) = std::fs::read_dir(".") {
                for entry in entries.flatten() {
                    if let Some(filename) = entry.file_name().to_str() {
                        if filename.starts_with(&format!("table_{}_", id))
                            && filename.ends_with(".index")
                        {
                            std::fs::remove_file(entry.path()).ok();
                        }
                    }
                }
            }
        }

        // Drop from catalog
        self.catalog.drop_table(table_name)?;

        self.catalog.save_to_disk("arcdb.meta").ok();

        Ok(QueryResult::with_message(format!(
            "Table '{}' dropped",
            table_name
        )))
    }

    fn execute_create_index(
        &mut self,
        index_name: &str,
        table_name: &str,
        columns: Vec<String>,
        unique: bool,
        _if_not_exists: bool,
    ) -> Result<QueryResult> {
        self.ensure_table_loaded(table_name)?;

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        // Create index in storage
        table.create_index(index_name.to_string(), columns.clone())?;

        // Register in catalog
        self.catalog
            .create_index(index_name, table_name, columns, unique)?;

        // Auto-save catalog
        self.catalog.save_to_disk("arcdb.meta").ok();

        Ok(QueryResult::with_message(format!(
            "Index '{}' created on '{}'",
            index_name, table_name
        )))
    }

    fn execute_insert(
        &mut self,
        table_name: &str,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    ) -> Result<QueryResult> {
        // First, get schema info without mutable borrow
        let schema = {
            let table = self
                .tables
                .get(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
            table.schema().clone()
        };

        // Evaluate all expressions first (before mutable borrow)
        let mut all_tuples = Vec::new();
        for row_values in values {
            let mut tuple_values = Vec::new();

            if let Some(ref cols) = columns {
                // Initialize all values to NULL
                for _ in 0..schema.column_count() {
                    tuple_values.push(Value::Null);
                }

                // Set the specified columns
                for (i, col_name) in cols.iter().enumerate() {
                    let col_idx = schema.get_column_index(col_name).ok_or_else(|| {
                        Error::ColumnNotFound(col_name.clone(), table_name.to_string())
                    })?;
                    if i < row_values.len() {
                        tuple_values[col_idx] = self.evaluate_expr(&row_values[i], &[], &[])?;
                    }
                }
            } else {
                for expr in &row_values {
                    tuple_values.push(self.evaluate_expr(expr, &[], &[])?);
                }
            }

            all_tuples.push(Tuple::new(tuple_values));
        }

        // Now borrow table mutably and insert all tuples
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        let mut inserted = 0;
        for tuple in all_tuples {
            let slot_id = table.insert(tuple.clone())?;
            inserted += 1;

            // Log insert if transaction active
            if let Some(trans_id) = self.current_trans_id {
                let lsn = self.transaction_manager.log_manager().append(
                    trans_id,
                    crate::storage::wal::LogRecordType::Insert,
                    Some(table_name.to_string()),
                    Some(slot_id),
                    None,
                    Some(tuple),
                )?;
                table.set_page_lsn(slot_id.page_id, lsn);
            }
        }

        Ok(QueryResult::with_affected_rows(
            inserted,
            format!("{} row(s) inserted", inserted),
        ))
    }

    fn execute_update(
        &mut self,
        table_name: &str,
        assignments: Vec<Assignment>,
        predicate: Option<Expr>,
    ) -> Result<QueryResult> {
        // Get schema and data without mutable borrow
        let (schema, column_names, tuples_to_check) = {
            let table = self
                .tables
                .get_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
            let schema = table.schema().clone();
            let column_names: Vec<String> = schema
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let tuples: Vec<(SlotId, Tuple)> = table.scan();
            (schema, column_names, tuples)
        };

        // Filter and prepare updates
        let mut updates = Vec::new();
        for (slot_id, tuple) in tuples_to_check {
            let matches = if let Some(ref pred) = predicate {
                let result = self.evaluate_expr(pred, tuple.values(), &column_names)?;
                result.as_bool().unwrap_or(false)
            } else {
                true
            };

            if matches {
                // Evaluate new values
                let mut new_tuple = tuple.clone();
                for assignment in &assignments {
                    let col_idx = schema.get_column_index(&assignment.column).ok_or_else(|| {
                        Error::ColumnNotFound(assignment.column.clone(), table_name.to_string())
                    })?;
                    let new_value =
                        self.evaluate_expr(&assignment.value, tuple.values(), &column_names)?;
                    new_tuple.set(col_idx, new_value);
                }
                updates.push((slot_id, new_tuple));
            }
        }

        let updated_count = updates.len();

        // Apply updates with mutable borrow
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        for (slot_id, new_tuple) in updates {
            let before_image = table.get_tuple(slot_id);
            table.update(slot_id, new_tuple.clone())?;

            // Log update if transaction active
            if let Some(trans_id) = self.current_trans_id {
                let lsn = self.transaction_manager.log_manager().append(
                    trans_id,
                    crate::storage::wal::LogRecordType::Update,
                    Some(table_name.to_string()),
                    Some(slot_id),
                    before_image,
                    Some(new_tuple),
                )?;
                table.set_page_lsn(slot_id.page_id, lsn);
            }
        }

        Ok(QueryResult::with_affected_rows(
            updated_count,
            format!("{} row(s) updated", updated_count),
        ))
    }

    fn execute_delete(&mut self, table_name: &str, predicate: Option<Expr>) -> Result<QueryResult> {
        // Get data without mutable borrow
        let (column_names, tuples_to_check) = {
            let table = self
                .tables
                .get_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
            let column_names: Vec<String> = table
                .schema()
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            let tuples = table.scan();
            (column_names, tuples)
        };

        // Determine which rows to delete
        let mut to_delete = Vec::new();
        for (slot_id, tuple) in tuples_to_check {
            let matches = if let Some(ref pred) = predicate {
                let result = self.evaluate_expr(pred, tuple.values(), &column_names)?;
                result.as_bool().unwrap_or(false)
            } else {
                true
            };

            if matches {
                to_delete.push(slot_id);
            }
        }

        let deleted_count = to_delete.len();

        // Delete with mutable borrow
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        for slot_id in to_delete {
            let before_image = table.get_tuple(slot_id);
            table.delete(slot_id)?;

            // Log delete if transaction active
            if let Some(trans_id) = self.current_trans_id {
                let lsn = self.transaction_manager.log_manager().append(
                    trans_id,
                    crate::storage::wal::LogRecordType::Delete,
                    Some(table_name.to_string()),
                    Some(slot_id),
                    before_image,
                    None,
                )?;
                table.set_page_lsn(slot_id.page_id, lsn);
            }
        }

        Ok(QueryResult::with_affected_rows(
            deleted_count,
            format!("{} row(s) deleted", deleted_count),
        ))
    }

    fn execute_select(
        &mut self,
        input: LogicalPlan,
        expressions: Vec<SelectItem>,
    ) -> Result<QueryResult> {
        // First, execute the input plan to get rows
        let (input_rows, input_columns) = self.execute_scan_plan(input)?;

        // Build result columns
        let mut result_columns = Vec::new();
        for item in &expressions {
            match item {
                SelectItem::Wildcard => {
                    result_columns.extend(input_columns.clone());
                }
                SelectItem::QualifiedWildcard(_table) => {
                    // TODO: Handle qualified wildcards properly
                    result_columns.extend(input_columns.clone());
                }
                SelectItem::Expr { alias, expr } => {
                    let col_name = alias.clone().unwrap_or_else(|| self.expr_to_string(expr));
                    result_columns.push(col_name);
                }
            }
        }

        // Project rows
        let mut result_rows = Vec::new();
        for row in &input_rows {
            let mut projected_values = Vec::new();

            for item in &expressions {
                match item {
                    SelectItem::Wildcard => {
                        projected_values.extend(row.values().iter().cloned());
                    }
                    SelectItem::QualifiedWildcard(_) => {
                        projected_values.extend(row.values().iter().cloned());
                    }
                    SelectItem::Expr { expr, .. } => {
                        let value = self.evaluate_expr(expr, row.values(), &input_columns)?;
                        projected_values.push(value);
                    }
                }
            }

            result_rows.push(Tuple::new(projected_values));
        }

        Ok(QueryResult {
            columns: result_columns,
            rows: result_rows,
            affected_rows: 0,
            message: None,
        })
    }

    fn execute_scan(&mut self, table_name: &str) -> Result<QueryResult> {
        if table_name.is_empty() {
            // SELECT without FROM
            return Ok(QueryResult::empty());
        }

        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        let columns: Vec<String> = table
            .schema()
            .column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let rows: Vec<Tuple> = table.scan().into_iter().map(|(_, t)| t).collect();

        Ok(QueryResult {
            columns,
            rows,
            affected_rows: 0,
            message: None,
        })
    }

    fn execute_index_scan(
        &mut self,
        table_name: &str,
        index_name: &str,
        op: &BinaryOperator,
        value: &Expr,
    ) -> Result<QueryResult> {
        // Collect results first to avoid borrow conflicts
        let mut rows = Vec::new();
        let columns;

        {
            let val = self.evaluate_expr(value, &[], &[])?;
            let key = IndexKey::new(val);

            let table = self
                .tables
                .get_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

            columns = table
                .schema()
                .column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();

            match op {
                BinaryOperator::Eq => {
                    let maybe_slot = table.get_index(index_name).and_then(|idx| idx.search(&key));
                    if let Some(slot_id) = maybe_slot {
                        if let Some(tuple) = table.get(slot_id) {
                            rows.push(tuple);
                        }
                    }
                }
                BinaryOperator::Gt
                | BinaryOperator::Gte
                | BinaryOperator::Lt
                | BinaryOperator::Lte => {
                    let (min, max) = match op {
                        BinaryOperator::Gt | BinaryOperator::Gte => (Some(&key), None),
                        BinaryOperator::Lt | BinaryOperator::Lte => (None, Some(&key)),
                        _ => unreachable!(),
                    };

                    if let Some(col_indices) = table.get_index_columns(index_name) {
                        let col_idx = col_indices[0];
                        if let Some(index) = table.get_index(index_name) {
                            for (_, slot_id) in index.range_scan(min, max) {
                                if let Some(tuple) = table.get(slot_id) {
                                    // Filter for exclusive if needed
                                    let matches = match op {
                                        BinaryOperator::Gt => tuple
                                            .get(col_idx)
                                            .map_or(false, |v| IndexKey::new(v.clone()) > key),
                                        BinaryOperator::Lt => tuple
                                            .get(col_idx)
                                            .map_or(false, |v| IndexKey::new(v.clone()) < key),
                                        _ => true, // Gte and Lte are handled by inclusive range_scan
                                    };

                                    if matches {
                                        rows.push(tuple);
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    return Err(Error::ExecutionError(format!(
                        "Operator {:?} not supported for index scan",
                        op
                    )));
                }
            }
        }

        Ok(QueryResult {
            columns,
            rows,
            affected_rows: 0,
            message: None,
        })
    }

    fn execute_join(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        condition: Option<Expr>,
    ) -> Result<QueryResult> {
        // Execute left child
        let left_result = self.execute(left)?;

        // Execute right child
        let right_result = self.execute(right)?;

        // Only support Inner Join for now
        if !matches!(join_type, JoinType::Inner) {
            return Err(Error::ExecutionError(
                "Only INNER JOIN is currently supported".to_string(),
            ));
        }

        let mut result_rows = Vec::new();
        let mut columns = left_result.columns.clone();
        columns.extend(right_result.columns.clone());

        // Nested Loop Join
        for l_row in &left_result.rows {
            for r_row in &right_result.rows {
                // Combine tuples
                let mut values = l_row.values().to_vec();
                values.extend(r_row.values().to_vec());
                let joined_tuple = Tuple::new(values);

                // Check condition
                let matches = if let Some(ref cond) = condition {
                    let result = self.evaluate_expr(cond, joined_tuple.values(), &columns)?;
                    result.as_bool().unwrap_or(false)
                } else {
                    true
                };

                if matches {
                    result_rows.push(joined_tuple);
                }
            }
        }

        Ok(QueryResult {
            columns,
            rows: result_rows,
            affected_rows: 0,
            message: None,
        })
    }

    fn execute_hash_join(
        &mut self,
        left: LogicalPlan,
        right: LogicalPlan,
        left_key: Expr,
        right_key: Expr,
    ) -> Result<QueryResult> {
        // 1. Execute build side (left)
        let left_result = self.execute(left)?;

        // Build hash table
        let mut hash_table: HashMap<Value, Vec<Tuple>> = HashMap::new();

        for row in &left_result.rows {
            let key = self.evaluate_expr(&left_key, row.values(), &left_result.columns)?;
            hash_table.entry(key).or_default().push(row.clone());
        }

        // 2. Execute probe side (right)
        let right_result = self.execute(right)?;

        let mut result_rows = Vec::new();

        // Probe hash table
        for r_row in &right_result.rows {
            let key = self.evaluate_expr(&right_key, r_row.values(), &right_result.columns)?;

            if let Some(matches) = hash_table.get(&key) {
                for l_row in matches {
                    // Combine tuples: left + right
                    let mut values = l_row.values().to_vec();
                    values.extend(r_row.values().to_vec());
                    result_rows.push(Tuple::new(values));
                }
            }
        }

        // Combine schemas
        let mut columns = left_result.columns;
        columns.extend(right_result.columns);

        Ok(QueryResult {
            columns,
            rows: result_rows,
            affected_rows: 0,
            message: None,
        })
    }

    fn execute_scan_plan(&mut self, plan: LogicalPlan) -> Result<(Vec<Tuple>, Vec<String>)> {
        match plan {
            LogicalPlan::Scan { table_name, .. } => {
                if table_name.is_empty() {
                    return Ok((vec![Tuple::empty()], vec![]));
                }

                let table = self
                    .tables
                    .get_mut(&table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                let columns: Vec<String> = table
                    .schema()
                    .column_names()
                    .iter()
                    .map(|s| s.to_string())
                    .collect();
                let rows: Vec<Tuple> = table.scan().into_iter().map(|(_, t)| t).collect();

                Ok((rows, columns))
            }
            LogicalPlan::Filter { input, predicate } => {
                let (rows, columns) = self.execute_scan_plan(*input)?;

                let filtered: Vec<Tuple> = rows
                    .into_iter()
                    .filter(|row| {
                        self.evaluate_expr(&predicate, row.values(), &columns)
                            .ok()
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false)
                    })
                    .collect();

                Ok((filtered, columns))
            }
            LogicalPlan::Sort { input, order_by } => {
                let (mut rows, columns) = self.execute_scan_plan(*input)?;

                rows.sort_by(|a, b| {
                    for item in &order_by {
                        let val_a = self.evaluate_expr(&item.expr, a.values(), &columns).ok();
                        let val_b = self.evaluate_expr(&item.expr, b.values(), &columns).ok();

                        let cmp = match (val_a, val_b) {
                            (Some(va), Some(vb)) => va.compare(&vb).unwrap_or(Ordering::Equal),
                            _ => Ordering::Equal,
                        };

                        if cmp != Ordering::Equal {
                            return if item.ascending { cmp } else { cmp.reverse() };
                        }
                    }
                    Ordering::Equal
                });

                Ok((rows, columns))
            }
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let (rows, columns) = self.execute_scan_plan(*input)?;

                let offset_val = offset
                    .and_then(|e| self.evaluate_const_expr(&e).ok())
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as usize;

                let limit_val = limit
                    .and_then(|e| self.evaluate_const_expr(&e).ok())
                    .and_then(|v| v.as_i64())
                    .map(|l| l as usize)
                    .unwrap_or(rows.len());

                let limited: Vec<Tuple> =
                    rows.into_iter().skip(offset_val).take(limit_val).collect();

                Ok((limited, columns))
            }
            _ => Err(Error::ExecutionError(
                "Unsupported plan in scan".to_string(),
            )),
        }
    }

    fn evaluate_expr(&self, expr: &Expr, row: &[Value], columns: &[String]) -> Result<Value> {
        match expr {
            Expr::Literal(lit) => Ok(self.literal_to_value(lit)),

            Expr::Column(col_ref) => {
                let col_name = &col_ref.column;
                let idx = columns
                    .iter()
                    .position(|c| c == col_name)
                    .ok_or_else(|| Error::ColumnNotFound(col_name.clone(), String::new()))?;

                row.get(idx).cloned().ok_or_else(|| {
                    Error::ExecutionError(format!("Column index {} out of bounds", idx))
                })
            }

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left, row, columns)?;
                let right_val = self.evaluate_expr(right, row, columns)?;

                self.evaluate_binary_op(&left_val, op, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_expr(expr, row, columns)?;
                self.evaluate_unary_op(op, &val)
            }

            Expr::IsNull(inner) => {
                let val = self.evaluate_expr(inner, row, columns)?;
                Ok(Value::Boolean(val.is_null()))
            }

            Expr::IsNotNull(inner) => {
                let val = self.evaluate_expr(inner, row, columns)?;
                Ok(Value::Boolean(!val.is_null()))
            }

            Expr::Nested(inner) => self.evaluate_expr(inner, row, columns),

            Expr::Function { name, args, .. } => self.evaluate_function(name, args, row, columns),

            _ => Err(Error::ExecutionError(format!(
                "Unsupported expression: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_const_expr(&self, expr: &Expr) -> Result<Value> {
        self.evaluate_expr(expr, &[], &[])
    }

    fn literal_to_value(&self, lit: &Literal) -> Value {
        match lit {
            Literal::Null => Value::Null,
            Literal::Boolean(b) => Value::Boolean(*b),
            Literal::Integer(i) => Value::Integer(*i as i32),
            Literal::Float(f) => Value::Float(*f),
            Literal::String(s) => Value::String(s.clone()),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        match op {
            BinaryOperator::Eq => {
                let cmp = left.compare(right);
                Ok(Value::Boolean(cmp == Some(Ordering::Equal)))
            }
            BinaryOperator::Neq => {
                let cmp = left.compare(right);
                Ok(Value::Boolean(cmp != Some(Ordering::Equal)))
            }
            BinaryOperator::Lt => {
                let cmp = left.compare(right);
                Ok(Value::Boolean(cmp == Some(Ordering::Less)))
            }
            BinaryOperator::Gt => {
                let cmp = left.compare(right);
                Ok(Value::Boolean(cmp == Some(Ordering::Greater)))
            }
            BinaryOperator::Lte => {
                let cmp = left.compare(right);
                Ok(Value::Boolean(matches!(
                    cmp,
                    Some(Ordering::Less) | Some(Ordering::Equal)
                )))
            }
            BinaryOperator::Gte => {
                let cmp = left.compare(right);
                Ok(Value::Boolean(matches!(
                    cmp,
                    Some(Ordering::Greater) | Some(Ordering::Equal)
                )))
            }
            BinaryOperator::And => {
                let left_bool = left.as_bool().unwrap_or(false);
                let right_bool = right.as_bool().unwrap_or(false);
                Ok(Value::Boolean(left_bool && right_bool))
            }
            BinaryOperator::Or => {
                let left_bool = left.as_bool().unwrap_or(false);
                let right_bool = right.as_bool().unwrap_or(false);
                Ok(Value::Boolean(left_bool || right_bool))
            }
            BinaryOperator::Add => left.add(right).ok_or_else(|| Error::TypeMismatch {
                from: left.type_name().to_string(),
                to: right.type_name().to_string(),
            }),
            BinaryOperator::Sub => left.sub(right).ok_or_else(|| Error::TypeMismatch {
                from: left.type_name().to_string(),
                to: right.type_name().to_string(),
            }),
            BinaryOperator::Mul => left.mul(right).ok_or_else(|| Error::TypeMismatch {
                from: left.type_name().to_string(),
                to: right.type_name().to_string(),
            }),
            BinaryOperator::Div => {
                if let Some(r) = right.as_f64() {
                    if r == 0.0 {
                        return Err(Error::DivisionByZero);
                    }
                }
                left.div(right).ok_or_else(|| Error::TypeMismatch {
                    from: left.type_name().to_string(),
                    to: right.type_name().to_string(),
                })
            }
            BinaryOperator::Concat => match (left, right) {
                (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
                _ => Ok(Value::String(format!("{}{}", left, right))),
            },
            _ => Err(Error::ExecutionError(format!(
                "Unsupported operator: {:?}",
                op
            ))),
        }
    }

    fn evaluate_unary_op(&self, op: &UnaryOperator, val: &Value) -> Result<Value> {
        match op {
            UnaryOperator::Not => {
                let bool_val = val.as_bool().unwrap_or(false);
                Ok(Value::Boolean(!bool_val))
            }
            UnaryOperator::Minus => match val {
                Value::Integer(i) => Ok(Value::Integer(-i)),
                Value::BigInt(i) => Ok(Value::BigInt(-i)),
                Value::Float(f) => Ok(Value::Float(-f)),
                _ => Err(Error::TypeMismatch {
                    from: val.type_name().to_string(),
                    to: "numeric".to_string(),
                }),
            },
            UnaryOperator::Plus => Ok(val.clone()),
        }
    }

    fn evaluate_function(
        &self,
        name: &str,
        args: &[Expr],
        row: &[Value],
        columns: &[String],
    ) -> Result<Value> {
        let name_upper = name.to_uppercase();

        // For now, just handle simple cases
        match name_upper.as_str() {
            "COUNT" => {
                // This is a simplified version; real aggregates need proper handling
                Ok(Value::Integer(1))
            }
            "UPPER" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_expr(arg, row, columns)?;
                    if let Value::String(s) = val {
                        Ok(Value::String(s.to_uppercase()))
                    } else {
                        Ok(val)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "LOWER" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_expr(arg, row, columns)?;
                    if let Value::String(s) = val {
                        Ok(Value::String(s.to_lowercase()))
                    } else {
                        Ok(val)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            "LENGTH" => {
                if let Some(arg) = args.first() {
                    let val = self.evaluate_expr(arg, row, columns)?;
                    if let Value::String(s) = val {
                        Ok(Value::Integer(s.len() as i32))
                    } else {
                        Ok(Value::Null)
                    }
                } else {
                    Ok(Value::Null)
                }
            }
            _ => Err(Error::ExecutionError(format!("Unknown function: {}", name))),
        }
    }

    fn expr_to_string(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column(col_ref) => col_ref.column.clone(),
            Expr::Literal(lit) => format!("{:?}", lit),
            Expr::Function { name, .. } => name.clone(),
            _ => "expr".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_engine() -> ExecutionEngine {
        let catalog = Arc::new(Catalog::new());
        ExecutionEngine::new(catalog).unwrap()
    }

    #[test]
    fn test_create_table() {
        let mut engine = create_test_engine();

        let plan = LogicalPlan::CreateTable {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    not_null: true,
                    default: None,
                    primary_key: true,
                    unique: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Varchar(100),
                    not_null: false,
                    default: None,
                    primary_key: false,
                    unique: false,
                },
            ],
            if_not_exists: false,
        };

        let result = engine.execute(plan).unwrap();
        assert!(result.message.is_some());
        assert!(result.message.unwrap().contains("created"));
    }

    #[test]
    fn test_insert_and_select() {
        let mut engine = create_test_engine();

        // Create table
        let create_plan = LogicalPlan::CreateTable {
            table_name: "test".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    not_null: true,
                    default: None,
                    primary_key: true,
                    unique: false,
                },
                ColumnDef {
                    name: "value".to_string(),
                    data_type: DataType::Varchar(50),
                    not_null: false,
                    default: None,
                    primary_key: false,
                    unique: false,
                },
            ],
            if_not_exists: false,
        };
        engine.execute(create_plan).unwrap();

        // Insert
        let insert_plan = LogicalPlan::Insert {
            table_name: "test".to_string(),
            columns: None,
            values: vec![vec![
                Expr::Literal(Literal::Integer(1)),
                Expr::Literal(Literal::String("hello".to_string())),
            ]],
        };
        let result = engine.execute(insert_plan).unwrap();
        assert_eq!(result.affected_rows, 1);

        // Select
        let scan_plan = LogicalPlan::Scan {
            table_name: "test".to_string(),
            projection: None,
        };
        let result = engine.execute(scan_plan).unwrap();
        assert_eq!(result.rows.len(), 1);
    }

    #[test]
    fn test_join_execution() {
        let catalog = Arc::new(Catalog::new());
        let mut engine = ExecutionEngine::new(catalog.clone()).unwrap();

        // Create users table
        engine
            .execute(LogicalPlan::CreateTable {
                table_name: "users".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        not_null: true,
                        default: None,
                        primary_key: true,
                        unique: false,
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Varchar(50),
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                ],
                if_not_exists: false,
            })
            .unwrap();

        // Create orders table
        engine
            .execute(LogicalPlan::CreateTable {
                table_name: "orders".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "oid".to_string(),
                        data_type: DataType::Integer,
                        not_null: true,
                        default: None,
                        primary_key: true,
                        unique: false,
                    },
                    ColumnDef {
                        name: "uid".to_string(),
                        data_type: DataType::Integer,
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                    ColumnDef {
                        name: "amount".to_string(),
                        data_type: DataType::Integer,
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                ],
                if_not_exists: false,
            })
            .unwrap();

        // Create users table

        // Insert users
        engine
            .execute_insert(
                "users",
                None,
                vec![
                    vec![
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::String("Alice".to_string())),
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(2)),
                        Expr::Literal(Literal::String("Bob".to_string())),
                    ],
                ],
            )
            .unwrap();

        // Insert orders
        engine
            .execute_insert(
                "orders",
                None,
                vec![
                    vec![
                        Expr::Literal(Literal::Integer(100)),
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::Integer(500)),
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(101)),
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::Integer(300)),
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(102)),
                        Expr::Literal(Literal::Integer(2)),
                        Expr::Literal(Literal::Integer(700)),
                    ],
                ],
            )
            .unwrap();

        // Plan JOIN: SELECT * FROM users JOIN orders ON users.id = orders.uid
        // Note: ColumnRef needs full path if not imported, but super::* should handle it if in ast
        let scan_users = LogicalPlan::Scan {
            table_name: "users".to_string(),
            projection: None,
        };
        let scan_orders = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            projection: None,
        };

        let join_plan = LogicalPlan::Join {
            left: Box::new(scan_users),
            right: Box::new(scan_orders),
            join_type: JoinType::Inner,
            condition: Some(Expr::BinaryOp {
                left: Box::new(Expr::Column(crate::sql::ast::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Column(crate::sql::ast::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "uid".to_string(),
                })),
            }),
        };

        let result = engine.execute(join_plan).unwrap();

        // Expected rows: (1, Alice, 100, 1, 500), (1, Alice, 101, 1, 300), (2, Bob, 102, 2, 700)
        assert_eq!(result.rows.len(), 3);

        let alice_rows = result
            .rows
            .iter()
            .filter(|r: &&Tuple| r.get(0) == Some(&Value::Integer(1)))
            .count();
        assert_eq!(alice_rows, 2);
    }

    #[test]
    fn test_index_execution() {
        let mut engine = create_test_engine();

        // 1. Create Table
        let plan = LogicalPlan::CreateTable {
            table_name: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    not_null: true,
                    default: None,
                    primary_key: true,
                    unique: false,
                },
                ColumnDef {
                    name: "email".to_string(),
                    data_type: DataType::Varchar(100),
                    not_null: true,
                    default: None,
                    primary_key: false,
                    unique: true,
                },
            ],
            if_not_exists: false,
        };
        engine.execute(plan).unwrap();

        // 2. Create Index
        let plan = LogicalPlan::CreateIndex {
            index_name: "email_idx".to_string(),
            table_name: "users".to_string(),
            columns: vec!["email".to_string()],
            unique: true,
            if_not_exists: false,
        };
        engine.execute(plan).unwrap();

        // 3. Insert Data
        engine
            .execute_insert(
                "users",
                Some(vec!["id".to_string(), "email".to_string()]),
                vec![
                    vec![
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::String("alice@example.com".to_string())),
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(2)),
                        Expr::Literal(Literal::String("bob@example.com".to_string())),
                    ],
                ],
            )
            .unwrap();

        // 4. Validate manual index creation worked (via scan)
        {
            let table = engine.tables.get("users").unwrap();
            let index = table.get_index("email_idx").unwrap();
            assert_eq!(index.len(), 2);
        }

        // 5. Execute Index Scan with ID logic
        let plan = LogicalPlan::IndexScan {
            table_name: "users".to_string(),
            index_name: "email_idx".to_string(),
            columns: vec!["email".to_string()],
            op: BinaryOperator::Eq,
            value: Expr::Literal(Literal::String("bob@example.com".to_string())),
        };

        let result = engine.execute(plan).unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0].get(1),
            Some(&Value::String("bob@example.com".to_string()))
        );
    }

    #[test]
    fn test_analyze_execution() {
        let catalog = Arc::new(Catalog::new());
        let mut engine = ExecutionEngine::new(catalog.clone()).unwrap();

        // Create table and insert data
        engine
            .execute(LogicalPlan::CreateTable {
                table_name: "test".to_string(),
                columns: vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        not_null: true,
                        default: None,
                        primary_key: true,
                        unique: false,
                    },
                    ColumnDef {
                        name: "val".to_string(),
                        data_type: DataType::Integer,
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                ],
                if_not_exists: false,
            })
            .unwrap();

        for i in 0..10 {
            engine
                .execute(LogicalPlan::Insert {
                    table_name: "test".to_string(),
                    columns: None,
                    values: vec![vec![
                        Expr::Literal(Literal::Integer(i as i64)),
                        Expr::Literal(Literal::Integer(i as i64 * 10)),
                    ]],
                })
                .unwrap();
        }

        // Run ANALYZE
        engine
            .execute(LogicalPlan::Analyze {
                table_name: "test".to_string(),
            })
            .unwrap();

        // Verify stats
        let table_def = catalog.get_table("test").unwrap();
        assert!(table_def.stats.is_some());
        assert_eq!(table_def.stats.as_ref().unwrap().row_count, 10);
    }

    #[test]
    fn test_hash_join_execution() {
        let catalog = Arc::new(Catalog::new());
        let mut engine = ExecutionEngine::new(catalog).unwrap();
        engine.execute(LogicalPlan::BeginTransaction).unwrap();

        // 1. Create tables
        engine
            .execute_create_table(
                "users",
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                    ColumnDef {
                        name: "name".to_string(),
                        data_type: DataType::Varchar(50),
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                ],
                false,
            )
            .unwrap();

        engine
            .execute_create_table(
                "orders",
                vec![
                    ColumnDef {
                        name: "id".to_string(),
                        data_type: DataType::Integer,
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                    ColumnDef {
                        name: "user_id".to_string(),
                        data_type: DataType::Integer,
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                    ColumnDef {
                        name: "item".to_string(),
                        data_type: DataType::Varchar(50),
                        not_null: false,
                        default: None,
                        primary_key: false,
                        unique: false,
                    },
                ],
                false,
            )
            .unwrap();

        // 2. Insert Data
        engine
            .execute_insert(
                "users",
                None,
                vec![
                    vec![
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::String("Alice".to_string())),
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(2)),
                        Expr::Literal(Literal::String("Bob".to_string())),
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(3)),
                        Expr::Literal(Literal::String("Charlie".to_string())),
                    ],
                ],
            )
            .unwrap();

        engine
            .execute_insert(
                "orders",
                None,
                vec![
                    // Alice's orders
                    vec![
                        Expr::Literal(Literal::Integer(101)),
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::String("Book".to_string())),
                    ],
                    vec![
                        Expr::Literal(Literal::Integer(102)),
                        Expr::Literal(Literal::Integer(1)),
                        Expr::Literal(Literal::String("Pen".to_string())),
                    ],
                    // Bob's order
                    vec![
                        Expr::Literal(Literal::Integer(103)),
                        Expr::Literal(Literal::Integer(2)),
                        Expr::Literal(Literal::String("Phone".to_string())),
                    ],
                ],
            )
            .unwrap();

        // 3. Execute Hash Join: users.id = orders.user_id
        let left = LogicalPlan::Scan {
            table_name: "users".to_string(),
            projection: None,
        };
        let right = LogicalPlan::Scan {
            table_name: "orders".to_string(),
            projection: None,
        };

        let plan = LogicalPlan::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            left_key: Expr::Column(ColumnRef {
                table: None,
                column: "id".to_string(),
            }),
            right_key: Expr::Column(ColumnRef {
                table: None,
                column: "user_id".to_string(),
            }),
        };

        let result = engine.execute(plan).unwrap();

        // Check result count (Alice: 2, Bob: 1)
        assert_eq!(result.rows.len(), 3);

        let mut alice_count = 0;
        let mut bob_count = 0;

        for row in result.rows {
            if let Some(val) = row.get(1) {
                // name column
                if let Value::String(s) = val {
                    if s == "Alice" {
                        alice_count += 1;
                    }
                    if s == "Bob" {
                        bob_count += 1;
                    }
                }
            }
        }
        assert_eq!(alice_count, 2);
        assert_eq!(bob_count, 1);
    }
}
