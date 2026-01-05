//! Data types for ArcDB
//!
//! This module defines the SQL data types supported by the database.

use serde::{Deserialize, Serialize};
use std::fmt;

/// SQL Data Types
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// Boolean type
    Boolean,
    /// Small integer (16-bit)
    SmallInt,
    /// Integer (32-bit)
    Integer,
    /// Big integer (64-bit)
    BigInt,
    /// Single-precision floating point
    Float,
    /// Double-precision floating point
    Double,
    /// Fixed-point decimal with precision and scale
    Decimal(u8, u8),
    /// Fixed-length character string
    Char(usize),
    /// Variable-length character string with max length
    Varchar(usize),
    /// Unlimited text
    Text,
    /// Date (year, month, day)
    Date,
    /// Time (hour, minute, second)
    Time,
    /// Timestamp (date + time)
    Timestamp,
    /// Binary data
    Blob,
}

impl DataType {
    /// Get the size in bytes for this type (for fixed-size types)
    pub fn size(&self) -> Option<usize> {
        match self {
            DataType::Boolean => Some(1),
            DataType::SmallInt => Some(2),
            DataType::Integer => Some(4),
            DataType::BigInt => Some(8),
            DataType::Float => Some(4),
            DataType::Double => Some(8),
            DataType::Decimal(_, _) => Some(16), // Fixed size for decimal
            DataType::Char(n) => Some(*n),
            DataType::Date => Some(4),       // Days since epoch
            DataType::Time => Some(8),       // Microseconds since midnight
            DataType::Timestamp => Some(12), // Date + Time
            // Variable-length types
            DataType::Varchar(_) => None,
            DataType::Text => None,
            DataType::Blob => None,
        }
    }

    /// Check if this type is numeric
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::SmallInt
                | DataType::Integer
                | DataType::BigInt
                | DataType::Float
                | DataType::Double
                | DataType::Decimal(_, _)
        )
    }

    /// Check if this type is a string type
    pub fn is_string(&self) -> bool {
        matches!(
            self,
            DataType::Char(_) | DataType::Varchar(_) | DataType::Text
        )
    }

    /// Check if this type is comparable with another type
    pub fn is_comparable_with(&self, other: &DataType) -> bool {
        match (self, other) {
            // Same types are always comparable
            (a, b) if a == b => true,
            // All numeric types are comparable
            (a, b) if a.is_numeric() && b.is_numeric() => true,
            // All string types are comparable
            (a, b) if a.is_string() && b.is_string() => true,
            // Date/time types
            (DataType::Date, DataType::Date) => true,
            (DataType::Time, DataType::Time) => true,
            (DataType::Timestamp, DataType::Timestamp) => true,
            (DataType::Date, DataType::Timestamp) => true,
            (DataType::Timestamp, DataType::Date) => true,
            _ => false,
        }
    }

    /// Get the default value for this type
    pub fn default_value(&self) -> &'static str {
        match self {
            DataType::Boolean => "FALSE",
            DataType::SmallInt | DataType::Integer | DataType::BigInt => "0",
            DataType::Float | DataType::Double | DataType::Decimal(_, _) => "0.0",
            DataType::Char(_) | DataType::Varchar(_) | DataType::Text => "''",
            DataType::Date => "CURRENT_DATE",
            DataType::Time => "CURRENT_TIME",
            DataType::Timestamp => "CURRENT_TIMESTAMP",
            DataType::Blob => "NULL",
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::SmallInt => write!(f, "SMALLINT"),
            DataType::Integer => write!(f, "INTEGER"),
            DataType::BigInt => write!(f, "BIGINT"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Double => write!(f, "DOUBLE"),
            DataType::Decimal(p, s) => write!(f, "DECIMAL({}, {})", p, s),
            DataType::Char(n) => write!(f, "CHAR({})", n),
            DataType::Varchar(n) => write!(f, "VARCHAR({})", n),
            DataType::Text => write!(f, "TEXT"),
            DataType::Date => write!(f, "DATE"),
            DataType::Time => write!(f, "TIME"),
            DataType::Timestamp => write!(f, "TIMESTAMP"),
            DataType::Blob => write!(f, "BLOB"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_size() {
        assert_eq!(DataType::Integer.size(), Some(4));
        assert_eq!(DataType::BigInt.size(), Some(8));
        assert_eq!(DataType::Varchar(100).size(), None);
    }

    #[test]
    fn test_type_comparison() {
        assert!(DataType::Integer.is_comparable_with(&DataType::BigInt));
        assert!(DataType::Varchar(50).is_comparable_with(&DataType::Text));
        assert!(!DataType::Integer.is_comparable_with(&DataType::Text));
    }
}
