//! Transaction Manager
//!
//! Handles transaction lifecycle (Begin, Commit, Rollback) and concurrency control.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, Result};
use crate::storage::wal::{LogManager, LogRecordType};

/// Transaction State
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Transaction Context
#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: u64,
    pub state: TransactionState,
}

/// Transaction Manager
pub struct TransactionManager {
    /// Log Manager
    log_manager: Arc<LogManager>,
    /// Active Transactions
    transactions: RwLock<HashMap<u64, Mutex<Transaction>>>,
    /// Next Transaction ID
    next_trans_id: Mutex<u64>,
    /// Lock Manager (Table -> LockMode -> Vec<TransID>)
    lock_manager: Arc<LockManager>,
}

/// Lock Mode
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

/// Lock Manager
pub struct LockManager {
    /// Locks: Table Name -> (Exclusive Lock Holder, Shared Lock Holders)
    locks: RwLock<HashMap<String, (Option<u64>, Vec<u64>)>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
        }
    }

    /// Acquire lock
    pub fn acquire(&self, table: &str, trans_id: u64, mode: LockMode) -> Result<bool> {
        let mut locks = self.locks.write().unwrap();
        let entry = locks.entry(table.to_string()).or_insert((None, Vec::new()));

        match mode {
            LockMode::Shared => {
                // Determine if we can grant shared lock
                if let Some(current_exclusive) = entry.0 {
                    if current_exclusive == trans_id {
                        // Already have exclusive, so we have shared implicitly
                        return Ok(true);
                    }
                    // Locked exclusively by someone else
                    return Ok(false);
                }
                // Grant shared lock
                if !entry.1.contains(&trans_id) {
                    entry.1.push(trans_id);
                }
                Ok(true)
            }
            LockMode::Exclusive => {
                // Determine if we can grant exclusive lock
                if let Some(current_exclusive) = entry.0 {
                    if current_exclusive == trans_id {
                        return Ok(true);
                    }
                    return Ok(false);
                }
                if !entry.1.is_empty() {
                    // If shared locks exist
                    if entry.1.len() == 1 && entry.1[0] == trans_id {
                        // Upgrade to exclusive
                        entry.1.clear();
                        entry.0 = Some(trans_id);
                        return Ok(true);
                    }
                    return Ok(false);
                }
                // Grant exclusive lock
                entry.0 = Some(trans_id);
                Ok(true)
            }
        }
    }

    /// Release locks for a transaction
    pub fn release_all(&self, trans_id: u64) {
        let mut locks = self.locks.write().unwrap();
        for (_, (exclusive, shared)) in locks.iter_mut() {
            if *exclusive == Some(trans_id) {
                *exclusive = None;
            }
            shared.retain(|&id| id != trans_id);
        }
    }
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new(log_manager: Arc<LogManager>) -> Self {
        Self {
            log_manager,
            transactions: RwLock::new(HashMap::new()),
            next_trans_id: Mutex::new(1),
            lock_manager: Arc::new(LockManager::new()),
        }
    }

    /// Get the log manager
    pub fn log_manager(&self) -> Arc<LogManager> {
        self.log_manager.clone()
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Result<u64> {
        let mut trans_id_guard = self.next_trans_id.lock().unwrap();
        let trans_id = *trans_id_guard;
        *trans_id_guard += 1;

        let transaction = Transaction {
            id: trans_id,
            state: TransactionState::Active,
        };

        self.transactions
            .write()
            .unwrap()
            .insert(trans_id, Mutex::new(transaction));

        // Log Begin
        self.log_manager
            .append(trans_id, LogRecordType::Begin, None, None, None, None)?;

        Ok(trans_id)
    }

    /// Commit a transaction
    pub fn commit(&self, trans_id: u64) -> Result<()> {
        let transactions = self.transactions.read().unwrap();
        let trans_mutex = transactions
            .get(&trans_id)
            .ok_or(Error::TransactionNotFound(trans_id))?;

        let mut trans = trans_mutex.lock().unwrap();
        if trans.state != TransactionState::Active {
            return Err(Error::Internal("Transaction not active".to_string()));
        }

        // Log Commit
        self.log_manager
            .append(trans_id, LogRecordType::Commit, None, None, None, None)?;

        // Flush WAL to disk
        self.log_manager.flush()?;

        trans.state = TransactionState::Committed;

        // Release locks
        self.lock_manager.release_all(trans_id);

        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback(&self, trans_id: u64) -> Result<()> {
        let transactions = self.transactions.read().unwrap();
        let trans_mutex = transactions
            .get(&trans_id)
            .ok_or(Error::TransactionNotFound(trans_id))?;

        let mut trans = trans_mutex.lock().unwrap();
        if trans.state != TransactionState::Active {
            return Err(Error::Internal("Transaction not active".to_string()));
        }

        // Undo operations
        // In a real system, we'd read the log backwards.
        // For now, since everything is in memory primarily, we'd assume memory state might be reverted?
        // Actually, without Undo implementation in Storage, we can't really rollback data changes yet.
        // We will just mark it as Aborted and log Rollback.
        // In future steps: Use LogManager iterator to find records for this trans_id and undo them.

        // Log Rollback
        self.log_manager
            .append(trans_id, LogRecordType::Rollback, None, None, None, None)?;

        trans.state = TransactionState::Aborted;

        // Release locks
        self.lock_manager.release_all(trans_id);

        Ok(())
    }

    /// Check if transaction is active
    pub fn is_active(&self, trans_id: u64) -> bool {
        if let Some(trans_mutex) = self.transactions.read().unwrap().get(&trans_id) {
            let trans = trans_mutex.lock().unwrap();
            trans.state == TransactionState::Active
        } else {
            false
        }
    }

    /// Acquire lock
    pub fn acquire_lock(&self, table: &str, trans_id: u64, mode: LockMode) -> Result<bool> {
        self.lock_manager.acquire(table, trans_id, mode)
    }
}
