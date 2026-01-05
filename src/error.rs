//! Error types for ArcDB
//!
//! This module defines all error types used throughout the database engine.

use thiserror::Error;

/// The main error type for ArcDB
#[derive(Error, Debug)]
pub enum Error {
    // ========== Lexer Errors ==========
    #[error("Lexer error: unexpected character '{0}' at position {1}")]
    UnexpectedCharacter(char, usize),

    #[error("Lexer error: unterminated string literal starting at position {0}")]
    UnterminatedString(usize),

    #[error("Lexer error: invalid number format at position {0}")]
    InvalidNumber(usize),

    // ========== Parser Errors ==========
    #[error("Parse error: unexpected token '{found}', expected {expected}")]
    UnexpectedToken { expected: String, found: String },

    #[error("Parse error: unexpected end of input, expected {0}")]
    UnexpectedEof(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    // ========== Catalog Errors ==========
    #[error("Catalog error: table '{0}' not found")]
    TableNotFound(String),

    #[error("Catalog error: table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("Catalog error: column '{0}' not found in table '{1}'")]
    ColumnNotFound(String, String),

    #[error("Catalog error: column '{0}' already exists in table '{1}'")]
    ColumnAlreadyExists(String, String),

    #[error("Catalog error: index '{0}' not found")]
    IndexNotFound(String),

    #[error("Catalog error: index '{0}' already exists")]
    IndexAlreadyExists(String),

    // ========== Type Errors ==========
    #[error("Type error: cannot convert {from} to {to}")]
    TypeMismatch { from: String, to: String },

    #[error("Type error: null value not allowed for column '{0}'")]
    NullNotAllowed(String),

    #[error("Type error: value too large for column '{0}'")]
    ValueTooLarge(String),

    // ========== Execution Errors ==========
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Execution error: {0}")]
    ExecutionError(String),

    #[error("Execution error: division by zero")]
    DivisionByZero,

    #[error("Execution error: constraint violation - {0}")]
    ConstraintViolation(String),

    #[error("Execution error: primary key violation for table '{0}'")]
    PrimaryKeyViolation(String),

    #[error("Execution error: foreign key violation - {0}")]
    ForeignKeyViolation(String),

    // ========== Storage Errors ==========
    #[error("Storage error: page {0} not found")]
    PageNotFound(u32),

    #[error("Storage error: page {0} is full")]
    PageFull(u32),

    #[error("Storage error: buffer pool is full")]
    BufferPoolFull,

    #[error("Storage error: corrupted page {0}")]
    CorruptedPage(u32),

    #[error("Storage error: file '{0}' not found")]
    FileNotFound(String),

    // ========== I/O Errors ==========
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    // ========== Transaction Errors ==========
    #[error("Transaction error: transaction {0} not found")]
    TransactionNotFound(u64),

    #[error("Transaction error: deadlock detected")]
    Deadlock,

    #[error("Transaction error: lock timeout")]
    LockTimeout,

    // ========== Internal Errors ==========
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Result type alias for ArcDB operations
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = Error::TableNotFound("users".to_string());
        assert_eq!(err.to_string(), "Catalog error: table 'users' not found");

        let err = Error::UnexpectedCharacter('@', 5);
        assert_eq!(
            err.to_string(),
            "Lexer error: unexpected character '@' at position 5"
        );
    }
}
