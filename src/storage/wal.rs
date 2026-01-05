//! Write-Ahead Log (WAL) Manager
//!
//! Handles durability by logging all changes before they are applied to data files.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::storage::{SlotId, Tuple};

/// Type of log record
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogRecordType {
    /// Transaction Begin
    Begin,
    /// Transaction Commit
    Commit,
    /// Transaction Rollback
    Rollback,
    /// Insert Tuple
    Insert,
    /// Update Tuple
    Update,
    /// Delete Tuple
    Delete,
    /// Aborted Transaction
    Abort,
}

/// A single log record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Log Sequence Number
    pub lsn: u64,
    /// Transaction ID
    pub trans_id: u64,
    /// Type of operation
    pub record_type: LogRecordType,
    /// Table Name (for data operations)
    pub table_name: Option<String>,
    /// Physical location of the row
    pub slot_id: Option<SlotId>,
    /// Before Image (for Undo/Rollback)
    pub before_image: Option<Tuple>,
    /// After Image (for Redo/Recovery)
    pub after_image: Option<Tuple>,
}

impl LogRecord {
    pub fn new(
        lsn: u64,
        trans_id: u64,
        record_type: LogRecordType,
        table_name: Option<String>,
        slot_id: Option<SlotId>,
        before_image: Option<Tuple>,
        after_image: Option<Tuple>,
    ) -> Self {
        Self {
            lsn,
            trans_id,
            record_type,
            table_name,
            slot_id,
            before_image,
            after_image,
        }
    }
}

/// Manages Write-Ahead Logs
#[derive(Debug)]
pub struct LogManager {
    /// Log Buffer (In-memory)
    buffer: Arc<Mutex<Vec<LogRecord>>>,
    /// Log File (On-disk)
    /// Simplification: We will just mock disk I/O or append to a file depending on complexity needs.
    /// For now, keeping it in memory is easier for the "mini" aspect, but we should pretend to flush.
    log_file: Option<Arc<Mutex<File>>>,
    /// Next LSN
    next_lsn: Arc<Mutex<u64>>,
}

impl LogManager {
    pub fn new() -> Self {
        Self {
            buffer: Arc::new(Mutex::new(Vec::new())),
            log_file: None, // Can be opened with open_log_file later
            next_lsn: Arc::new(Mutex::new(0)),
        }
    }

    /// Append a log record
    pub fn append(
        &self,
        trans_id: u64,
        record_type: LogRecordType,
        table_name: Option<String>,
        slot_id: Option<SlotId>,
        before_image: Option<Tuple>,
        after_image: Option<Tuple>,
    ) -> Result<u64> {
        let mut lsn_guard = self.next_lsn.lock().unwrap();
        let lsn = *lsn_guard;
        *lsn_guard += 1;

        let record = LogRecord::new(
            lsn,
            trans_id,
            record_type,
            table_name,
            slot_id,
            before_image,
            after_image,
        );

        let mut buffer = self.buffer.lock().unwrap();
        buffer.push(record);

        // In a real system, we might flush if buffer is full.
        // Here we just return LSN.
        Ok(lsn)
    }

    /// Flush logs to disk
    pub fn flush(&self) -> Result<()> {
        let mut buffer = self.buffer.lock().unwrap();
        if buffer.is_empty() {
            return Ok(());
        }

        if let Some(file_mutex) = &self.log_file {
            let mut file = file_mutex.lock().unwrap();
            for record in buffer.iter() {
                serde_json::to_writer(&mut *file, record)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                writeln!(file).map_err(|e| Error::IoError(e))?;
            }
            file.flush().map_err(|e| Error::IoError(e))?;
            buffer.clear();
        }
        Ok(())
    }

    /// Configure log file path
    pub fn set_log_file(&mut self, path: &str) -> Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|e| Error::IoError(e))?;
        self.log_file = Some(Arc::new(Mutex::new(file)));
        Ok(())
    }

    /// Read all logs from disk (for Recovery)
    pub fn read_from_log(&self, path: &str) -> Result<Vec<LogRecord>> {
        use std::io::BufRead;
        let file = File::open(path).map_err(|_| Error::FileNotFound(path.to_string()))?;
        let reader = std::io::BufReader::new(file);
        let mut records = Vec::new();

        for line in reader.lines() {
            let line = line.map_err(|e| Error::IoError(e))?;
            if line.trim().is_empty() {
                continue;
            }
            let record: LogRecord =
                serde_json::from_str(&line).map_err(|e| Error::Internal(e.to_string()))?;
            records.push(record);
        }

        Ok(records)
    }

    /// Get iterator over log records (for Recovery/Rollback)
    /// This is simplified; usually we read from disk reversely.
    pub fn iterator(&self) -> Vec<LogRecord> {
        self.buffer.lock().unwrap().clone()
    }
}
