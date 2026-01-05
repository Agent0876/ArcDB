//! Tuple and Value types for ArcDB
//!
//! This module defines how data values are represented in memory.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;

/// A value in the database
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// NULL value
    Null,
    /// Boolean value
    Boolean(bool),
    /// Integer value (32-bit)
    Integer(i32),
    /// Big integer value (64-bit)
    BigInt(i64),
    /// Float value (64-bit)
    Float(f64),
    /// String value
    String(String),
    /// Date value (days since epoch as i32)
    Date(i32),
    /// Timestamp value (milliseconds since epoch as i64)
    Timestamp(i64),
    /// Binary data
    Bytes(Vec<u8>),
}

// Implement PartialEq manually to support Float via bitwise comparison for HashJoin
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Bytes(a), Value::Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Boolean(v) => v.hash(state),
            Value::Integer(v) => v.hash(state),
            Value::BigInt(v) => v.hash(state),
            Value::Float(v) => v.to_bits().hash(state),
            Value::String(v) => v.hash(state),
            Value::Date(v) => v.hash(state),
            Value::Timestamp(v) => v.hash(state),
            Value::Bytes(v) => v.hash(state),
        }
    }
}

impl Value {
    /// Check if this value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to convert to boolean
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            Value::Integer(i) => Some(*i != 0),
            Value::BigInt(i) => Some(*i != 0),
            _ => None,
        }
    }

    /// Try to convert to i32
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Integer(i) => Some(*i),
            Value::BigInt(i) => (*i).try_into().ok(),
            Value::Float(f) => Some(*f as i32),
            _ => None,
        }
    }

    /// Try to convert to i64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i as i64),
            Value::BigInt(i) => Some(*i),
            Value::Float(f) => Some(*f as i64),
            _ => None,
        }
    }

    /// Try to convert to f64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Integer(i) => Some(*i as f64),
            Value::BigInt(i) => Some(*i as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    /// Try to convert to string
    pub fn as_string(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get the type name of this value
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "NULL",
            Value::Boolean(_) => "BOOLEAN",
            Value::Integer(_) => "INTEGER",
            Value::BigInt(_) => "BIGINT",
            Value::Float(_) => "FLOAT",
            Value::String(_) => "STRING",
            Value::Date(_) => "DATE",
            Value::Timestamp(_) => "TIMESTAMP",
            Value::Bytes(_) => "BYTES",
        }
    }

    /// Compare two values (for WHERE clauses, ORDER BY, etc.)
    pub fn compare(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less), // NULL is less than everything
            (_, Value::Null) => Some(Ordering::Greater),

            (Value::Boolean(a), Value::Boolean(b)) => Some(a.cmp(b)),

            (Value::Integer(a), Value::Integer(b)) => Some(a.cmp(b)),
            (Value::Integer(a), Value::BigInt(b)) => Some((*a as i64).cmp(b)),
            (Value::BigInt(a), Value::Integer(b)) => Some(a.cmp(&(*b as i64))),
            (Value::BigInt(a), Value::BigInt(b)) => Some(a.cmp(b)),

            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            (Value::BigInt(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::BigInt(b)) => a.partial_cmp(&(*b as f64)),

            (Value::String(a), Value::String(b)) => Some(a.cmp(b)),

            (Value::Date(a), Value::Date(b)) => Some(a.cmp(b)),
            (Value::Timestamp(a), Value::Timestamp(b)) => Some(a.cmp(b)),

            (Value::Bytes(a), Value::Bytes(b)) => Some(a.cmp(b)),

            _ => None, // Incompatible types
        }
    }

    /// Add two values
    pub fn add(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a + b)),
            (Value::Integer(a), Value::BigInt(b)) => Some(Value::BigInt(*a as i64 + b)),
            (Value::BigInt(a), Value::Integer(b)) => Some(Value::BigInt(a + *b as i64)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a + b)),
            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a + b)),
            (Value::Integer(a), Value::Float(b)) => Some(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Integer(b)) => Some(Value::Float(a + *b as f64)),
            (Value::String(a), Value::String(b)) => Some(Value::String(format!("{}{}", a, b))),
            _ => None,
        }
    }

    /// Subtract two values
    pub fn sub(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a - b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a - b)),
            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a - b)),
            _ => None,
        }
    }

    /// Multiply two values
    pub fn mul(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => Some(Value::Integer(a * b)),
            (Value::BigInt(a), Value::BigInt(b)) => Some(Value::BigInt(a * b)),
            (Value::Float(a), Value::Float(b)) => Some(Value::Float(a * b)),
            _ => None,
        }
    }

    /// Divide two values
    pub fn div(&self, other: &Value) -> Option<Value> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) if *b != 0 => Some(Value::Integer(a / b)),
            (Value::BigInt(a), Value::BigInt(b)) if *b != 0 => Some(Value::BigInt(a / b)),
            (Value::Float(a), Value::Float(b)) if *b != 0.0 => Some(Value::Float(a / b)),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Boolean(b) => write!(f, "{}", if *b { "TRUE" } else { "FALSE" }),
            Value::Integer(i) => write!(f, "{}", i),
            Value::BigInt(i) => write!(f, "{}", i),
            Value::Float(n) => write!(f, "{}", n),
            Value::String(s) => write!(f, "{}", s),
            Value::Date(d) => write!(f, "DATE({})", d),
            Value::Timestamp(t) => write!(f, "TIMESTAMP({})", t),
            Value::Bytes(b) => write!(f, "BYTES[{}]", b.len()),
        }
    }
}

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::BigInt(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Float(v)
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::String(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::String(v.to_string())
    }
}

impl From<Option<i32>> for Value {
    fn from(v: Option<i32>) -> Self {
        match v {
            Some(i) => Value::Integer(i),
            None => Value::Null,
        }
    }
}

/// A tuple (row) in the database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tuple {
    /// Values in this tuple
    values: Vec<Value>,
}

impl Tuple {
    /// Create a new tuple from values
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Create an empty tuple
    pub fn empty() -> Self {
        Self { values: Vec::new() }
    }

    /// Get a value by index
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Get a mutable value by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Value> {
        self.values.get_mut(index)
    }

    /// Set a value by index
    pub fn set(&mut self, index: usize, value: Value) {
        if index < self.values.len() {
            self.values[index] = value;
        }
    }

    /// Add a value to the tuple
    pub fn push(&mut self, value: Value) {
        self.values.push(value);
    }

    /// Get all values
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Get number of values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if tuple is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Consume the tuple and return the values
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Project specific columns
    pub fn project(&self, indices: &[usize]) -> Tuple {
        let values = indices
            .iter()
            .filter_map(|&i| self.values.get(i).cloned())
            .collect();
        Tuple::new(values)
    }

    /// Concatenate two tuples
    pub fn concat(&self, other: &Tuple) -> Tuple {
        let mut values = self.values.clone();
        values.extend(other.values.iter().cloned());
        Tuple::new(values)
    }

    /// Serialize tuple to binary format
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        // Number of values
        bytes.extend_from_slice(&(self.values.len() as u32).to_le_bytes());

        for value in &self.values {
            match value {
                Value::Null => bytes.push(0),
                Value::Boolean(b) => {
                    bytes.push(1);
                    bytes.push(if *b { 1 } else { 0 });
                }
                Value::Integer(i) => {
                    bytes.push(2);
                    bytes.extend_from_slice(&i.to_le_bytes());
                }
                Value::BigInt(i) => {
                    bytes.push(3);
                    bytes.extend_from_slice(&i.to_le_bytes());
                }
                Value::Float(f) => {
                    bytes.push(4);
                    bytes.extend_from_slice(&f.to_le_bytes());
                }
                Value::String(s) => {
                    bytes.push(5);
                    let s_bytes = s.as_bytes();
                    bytes.extend_from_slice(&(s_bytes.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(s_bytes);
                }
                Value::Date(d) => {
                    bytes.push(6);
                    bytes.extend_from_slice(&d.to_le_bytes());
                }
                Value::Timestamp(t) => {
                    bytes.push(7);
                    bytes.extend_from_slice(&t.to_le_bytes());
                }
                Value::Bytes(b) => {
                    bytes.push(8);
                    bytes.extend_from_slice(&(b.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(b);
                }
            }
        }
        bytes
    }

    /// Deserialize tuple from binary format
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 4 {
            return Err("Buffer too short for tuple header".to_string());
        }

        let mut offset = 0;
        let val_count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        offset += 4;

        let mut values = Vec::with_capacity(val_count);

        for _ in 0..val_count {
            if offset >= bytes.len() {
                return Err("Buffer overflow while reading tuple".to_string());
            }

            let type_tag = bytes[offset];
            offset += 1;

            match type_tag {
                0 => values.push(Value::Null),
                1 => {
                    values.push(Value::Boolean(bytes[offset] != 0));
                    offset += 1;
                }
                2 => {
                    values.push(Value::Integer(i32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ])));
                    offset += 4;
                }
                3 => {
                    values.push(Value::BigInt(i64::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                        bytes[offset + 4],
                        bytes[offset + 5],
                        bytes[offset + 6],
                        bytes[offset + 7],
                    ])));
                    offset += 8;
                }
                4 => {
                    values.push(Value::Float(f64::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                        bytes[offset + 4],
                        bytes[offset + 5],
                        bytes[offset + 6],
                        bytes[offset + 7],
                    ])));
                    offset += 8;
                }
                5 => {
                    let s_len = u32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]) as usize;
                    offset += 4;
                    let s = String::from_utf8(bytes[offset..offset + s_len].to_vec())
                        .map_err(|e| e.to_string())?;
                    values.push(Value::String(s));
                    offset += s_len;
                }
                6 => {
                    values.push(Value::Date(i32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ])));
                    offset += 4;
                }
                7 => {
                    values.push(Value::Timestamp(i64::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                        bytes[offset + 4],
                        bytes[offset + 5],
                        bytes[offset + 6],
                        bytes[offset + 7],
                    ])));
                    offset += 8;
                }
                8 => {
                    let b_len = u32::from_le_bytes([
                        bytes[offset],
                        bytes[offset + 1],
                        bytes[offset + 2],
                        bytes[offset + 3],
                    ]) as usize;
                    offset += 4;
                    values.push(Value::Bytes(bytes[offset..offset + b_len].to_vec()));
                    offset += b_len;
                }
                _ => return Err(format!("Unknown type tag: {}", type_tag)),
            }
        }

        Ok(Tuple::new(values))
    }
}

impl FromIterator<Value> for Tuple {
    fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
        Tuple::new(iter.into_iter().collect())
    }
}

impl IntoIterator for Tuple {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

impl<'a> IntoIterator for &'a Tuple {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_value_comparison() {
        assert_eq!(
            Value::Integer(5).compare(&Value::Integer(3)),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Value::String("abc".to_string()).compare(&Value::String("def".to_string())),
            Some(Ordering::Less)
        );
        assert_eq!(
            Value::Null.compare(&Value::Integer(1)),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn test_value_arithmetic() {
        assert_eq!(
            Value::Integer(5).add(&Value::Integer(3)),
            Some(Value::Integer(8))
        );
        assert_eq!(
            Value::Float(3.0).mul(&Value::Float(2.0)),
            Some(Value::Float(6.0))
        );
    }

    #[test]
    fn test_tuple_operations() {
        let tuple = Tuple::new(vec![
            Value::Integer(1),
            Value::String("hello".to_string()),
            Value::Boolean(true),
        ]);

        assert_eq!(tuple.len(), 3);
        assert_eq!(tuple.get(0), Some(&Value::Integer(1)));

        let projected = tuple.project(&[0, 2]);
        assert_eq!(projected.len(), 2);
        assert_eq!(projected.get(1), Some(&Value::Boolean(true)));
    }
}
