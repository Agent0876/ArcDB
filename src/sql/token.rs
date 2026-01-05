//! SQL Token definitions
//!
//! This module defines all tokens that can appear in SQL statements.

use std::fmt;

/// SQL Token types
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // ========== Keywords ==========
    // DDL Keywords
    Create,
    Drop,
    Alter,
    Table,
    Index,
    Database,
    Schema,
    View,

    // DML Keywords
    Select,
    Insert,
    Update,
    Delete,
    Into,
    Values,
    Set,
    From,
    Where,

    // Clauses
    And,
    Or,
    Not,
    As,
    On,
    Join,
    Inner,
    Left,
    Right,
    Outer,
    Full,
    Cross,
    Natural,
    Using,

    // Ordering & Grouping
    Order,
    By,
    Asc,
    Desc,
    Group,
    Having,
    Limit,
    Offset,

    // Constraints
    Primary,
    Foreign,
    Key,
    References,
    Unique,
    Check,
    Default,
    Constraint,
    Null,

    // Data Types
    Int,
    Integer,
    BigInt,
    SmallInt,
    Float,
    Double,
    Decimal,
    Numeric,
    Varchar,
    Char,
    Text,
    Boolean,
    Date,
    Time,
    Timestamp,

    // Boolean Literals
    True,
    False,

    // Aggregate Functions
    Count,
    Sum,
    Avg,
    Min,
    Max,

    // Other Keywords
    Distinct,
    All,
    Exists,
    In,
    Between,
    Like,
    Is,
    Case,
    When,
    Then,
    Else,
    End,
    If,
    Analyze,
    Begin,
    Commit,
    Rollback,
    Transaction,

    // ========== Literals ==========
    /// Integer literal
    IntegerLiteral(i64),
    /// Float literal
    FloatLiteral(f64),
    /// String literal (single-quoted)
    StringLiteral(String),
    /// Identifier (table name, column name, etc.)
    Identifier(String),

    // ========== Operators ==========
    /// =
    Eq,
    /// <> or !=
    Neq,
    /// <
    Lt,
    /// >
    Gt,
    /// <=
    Lte,
    /// >=
    Gte,
    /// +
    Plus,
    /// -
    Minus,
    /// *
    Asterisk,
    /// /
    Slash,
    /// %
    Percent,
    /// ||
    Concat,

    // ========== Delimiters ==========
    /// (
    LParen,
    /// )
    RParen,
    /// ,
    Comma,
    /// ;
    Semicolon,
    /// .
    Dot,
    /// :
    Colon,

    // ========== Special ==========
    /// End of input
    Eof,
}

impl Token {
    /// Check if this token is a keyword
    pub fn is_keyword(&self) -> bool {
        matches!(
            self,
            Token::Create
                | Token::Drop
                | Token::Alter
                | Token::Table
                | Token::Index
                | Token::Database
                | Token::Schema
                | Token::View
                | Token::Select
                | Token::Insert
                | Token::Update
                | Token::Delete
                | Token::Into
                | Token::Values
                | Token::Set
                | Token::From
                | Token::Where
                | Token::And
                | Token::Or
                | Token::Not
                | Token::As
                | Token::On
                | Token::Join
                | Token::Inner
                | Token::Left
                | Token::Right
                | Token::Outer
                | Token::Full
                | Token::Cross
                | Token::Natural
                | Token::Using
                | Token::Order
                | Token::By
                | Token::Asc
                | Token::Desc
                | Token::Group
                | Token::Having
                | Token::Limit
                | Token::Offset
                | Token::Primary
                | Token::Foreign
                | Token::Key
                | Token::References
                | Token::Unique
                | Token::Check
                | Token::Default
                | Token::Constraint
                | Token::Null
                | Token::True
                | Token::False
        )
    }

    /// Try to parse a keyword from a string
    pub fn from_keyword(s: &str) -> Option<Token> {
        match s.to_uppercase().as_str() {
            // DDL
            "CREATE" => Some(Token::Create),
            "DROP" => Some(Token::Drop),
            "ALTER" => Some(Token::Alter),
            "TABLE" => Some(Token::Table),
            "INDEX" => Some(Token::Index),
            "DATABASE" => Some(Token::Database),
            "SCHEMA" => Some(Token::Schema),
            "VIEW" => Some(Token::View),

            // DML
            "SELECT" => Some(Token::Select),
            "INSERT" => Some(Token::Insert),
            "UPDATE" => Some(Token::Update),
            "DELETE" => Some(Token::Delete),
            "INTO" => Some(Token::Into),
            "VALUES" => Some(Token::Values),
            "SET" => Some(Token::Set),
            "FROM" => Some(Token::From),
            "WHERE" => Some(Token::Where),

            // Clauses
            "AND" => Some(Token::And),
            "OR" => Some(Token::Or),
            "NOT" => Some(Token::Not),
            "AS" => Some(Token::As),
            "ON" => Some(Token::On),
            "JOIN" => Some(Token::Join),
            "INNER" => Some(Token::Inner),
            "LEFT" => Some(Token::Left),
            "RIGHT" => Some(Token::Right),
            "OUTER" => Some(Token::Outer),
            "FULL" => Some(Token::Full),
            "CROSS" => Some(Token::Cross),
            "NATURAL" => Some(Token::Natural),
            "USING" => Some(Token::Using),

            // Ordering & Grouping
            "ORDER" => Some(Token::Order),
            "BY" => Some(Token::By),
            "ASC" => Some(Token::Asc),
            "DESC" => Some(Token::Desc),
            "GROUP" => Some(Token::Group),
            "HAVING" => Some(Token::Having),
            "LIMIT" => Some(Token::Limit),
            "OFFSET" => Some(Token::Offset),

            // Constraints
            "PRIMARY" => Some(Token::Primary),
            "FOREIGN" => Some(Token::Foreign),
            "KEY" => Some(Token::Key),
            "REFERENCES" => Some(Token::References),
            "UNIQUE" => Some(Token::Unique),
            "CHECK" => Some(Token::Check),
            "DEFAULT" => Some(Token::Default),
            "CONSTRAINT" => Some(Token::Constraint),
            "NULL" => Some(Token::Null),

            // Data Types
            "INT" => Some(Token::Int),
            "INTEGER" => Some(Token::Integer),
            "BIGINT" => Some(Token::BigInt),
            "SMALLINT" => Some(Token::SmallInt),
            "FLOAT" => Some(Token::Float),
            "DOUBLE" => Some(Token::Double),
            "DECIMAL" => Some(Token::Decimal),
            "NUMERIC" => Some(Token::Numeric),
            "VARCHAR" => Some(Token::Varchar),
            "CHAR" => Some(Token::Char),
            "TEXT" => Some(Token::Text),
            "BOOLEAN" => Some(Token::Boolean),
            "DATE" => Some(Token::Date),
            "TIME" => Some(Token::Time),
            "TIMESTAMP" => Some(Token::Timestamp),

            // Boolean Literals
            "TRUE" => Some(Token::True),
            "FALSE" => Some(Token::False),

            // Aggregate Functions
            "COUNT" => Some(Token::Count),
            "SUM" => Some(Token::Sum),
            "AVG" => Some(Token::Avg),
            "MIN" => Some(Token::Min),
            "MAX" => Some(Token::Max),

            // Other Keywords
            "DISTINCT" => Some(Token::Distinct),
            "ALL" => Some(Token::All),
            "EXISTS" => Some(Token::Exists),
            "IN" => Some(Token::In),
            "BETWEEN" => Some(Token::Between),
            "LIKE" => Some(Token::Like),
            "IS" => Some(Token::Is),
            "CASE" => Some(Token::Case),
            "WHEN" => Some(Token::When),
            "THEN" => Some(Token::Then),
            "ELSE" => Some(Token::Else),
            "END" => Some(Token::End),
            "IF" => Some(Token::If),
            "ANALYZE" => Some(Token::Analyze),
            "BEGIN" => Some(Token::Begin),
            "COMMIT" => Some(Token::Commit),
            "ROLLBACK" => Some(Token::Rollback),
            "TRANSACTION" => Some(Token::Transaction),

            _ => None,
        }
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Token::Create => write!(f, "CREATE"),
            Token::Drop => write!(f, "DROP"),
            Token::Alter => write!(f, "ALTER"),
            Token::Table => write!(f, "TABLE"),
            Token::Index => write!(f, "INDEX"),
            Token::Database => write!(f, "DATABASE"),
            Token::Schema => write!(f, "SCHEMA"),
            Token::View => write!(f, "VIEW"),
            Token::Select => write!(f, "SELECT"),
            Token::Insert => write!(f, "INSERT"),
            Token::Update => write!(f, "UPDATE"),
            Token::Delete => write!(f, "DELETE"),
            Token::Into => write!(f, "INTO"),
            Token::Values => write!(f, "VALUES"),
            Token::Set => write!(f, "SET"),
            Token::From => write!(f, "FROM"),
            Token::Where => write!(f, "WHERE"),
            Token::And => write!(f, "AND"),
            Token::Or => write!(f, "OR"),
            Token::Not => write!(f, "NOT"),
            Token::As => write!(f, "AS"),
            Token::On => write!(f, "ON"),
            Token::Join => write!(f, "JOIN"),
            Token::Inner => write!(f, "INNER"),
            Token::Left => write!(f, "LEFT"),
            Token::Right => write!(f, "RIGHT"),
            Token::Outer => write!(f, "OUTER"),
            Token::Full => write!(f, "FULL"),
            Token::Cross => write!(f, "CROSS"),
            Token::Natural => write!(f, "NATURAL"),
            Token::Using => write!(f, "USING"),
            Token::Order => write!(f, "ORDER"),
            Token::By => write!(f, "BY"),
            Token::Asc => write!(f, "ASC"),
            Token::Desc => write!(f, "DESC"),
            Token::Group => write!(f, "GROUP"),
            Token::Having => write!(f, "HAVING"),
            Token::Limit => write!(f, "LIMIT"),
            Token::Offset => write!(f, "OFFSET"),
            Token::Primary => write!(f, "PRIMARY"),
            Token::Foreign => write!(f, "FOREIGN"),
            Token::Key => write!(f, "KEY"),
            Token::References => write!(f, "REFERENCES"),
            Token::Unique => write!(f, "UNIQUE"),
            Token::Check => write!(f, "CHECK"),
            Token::Default => write!(f, "DEFAULT"),
            Token::Constraint => write!(f, "CONSTRAINT"),
            Token::Null => write!(f, "NULL"),
            Token::Int => write!(f, "INT"),
            Token::Integer => write!(f, "INTEGER"),
            Token::BigInt => write!(f, "BIGINT"),
            Token::SmallInt => write!(f, "SMALLINT"),
            Token::Float => write!(f, "FLOAT"),
            Token::Double => write!(f, "DOUBLE"),
            Token::Decimal => write!(f, "DECIMAL"),
            Token::Numeric => write!(f, "NUMERIC"),
            Token::Varchar => write!(f, "VARCHAR"),
            Token::Char => write!(f, "CHAR"),
            Token::Text => write!(f, "TEXT"),
            Token::Boolean => write!(f, "BOOLEAN"),
            Token::Date => write!(f, "DATE"),
            Token::Time => write!(f, "TIME"),
            Token::Timestamp => write!(f, "TIMESTAMP"),
            Token::True => write!(f, "TRUE"),
            Token::False => write!(f, "FALSE"),
            Token::Count => write!(f, "COUNT"),
            Token::Sum => write!(f, "SUM"),
            Token::Avg => write!(f, "AVG"),
            Token::Min => write!(f, "MIN"),
            Token::Max => write!(f, "MAX"),
            Token::Distinct => write!(f, "DISTINCT"),
            Token::All => write!(f, "ALL"),
            Token::Exists => write!(f, "EXISTS"),
            Token::In => write!(f, "IN"),
            Token::Between => write!(f, "BETWEEN"),
            Token::Like => write!(f, "LIKE"),
            Token::Is => write!(f, "IS"),
            Token::Case => write!(f, "CASE"),
            Token::When => write!(f, "WHEN"),
            Token::Then => write!(f, "THEN"),
            Token::Else => write!(f, "ELSE"),
            Token::End => write!(f, "END"),
            Token::If => write!(f, "IF"),
            Token::Analyze => write!(f, "ANALYZE"),
            Token::Begin => write!(f, "BEGIN"),
            Token::Commit => write!(f, "COMMIT"),
            Token::Rollback => write!(f, "ROLLBACK"),
            Token::Transaction => write!(f, "TRANSACTION"),
            Token::IntegerLiteral(n) => write!(f, "{}", n),
            Token::FloatLiteral(n) => write!(f, "{}", n),
            Token::StringLiteral(s) => write!(f, "'{}'", s),
            Token::Identifier(s) => write!(f, "{}", s),
            Token::Eq => write!(f, "="),
            Token::Neq => write!(f, "<>"),
            Token::Lt => write!(f, "<"),
            Token::Gt => write!(f, ">"),
            Token::Lte => write!(f, "<="),
            Token::Gte => write!(f, ">="),
            Token::Plus => write!(f, "+"),
            Token::Minus => write!(f, "-"),
            Token::Asterisk => write!(f, "*"),
            Token::Slash => write!(f, "/"),
            Token::Percent => write!(f, "%"),
            Token::Concat => write!(f, "||"),
            Token::LParen => write!(f, "("),
            Token::RParen => write!(f, ")"),
            Token::Comma => write!(f, ","),
            Token::Semicolon => write!(f, ";"),
            Token::Dot => write!(f, "."),
            Token::Colon => write!(f, ":"),
            Token::Eof => write!(f, "EOF"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_parsing() {
        assert_eq!(Token::from_keyword("SELECT"), Some(Token::Select));
        assert_eq!(Token::from_keyword("select"), Some(Token::Select));
        assert_eq!(Token::from_keyword("SeLeCt"), Some(Token::Select));
        assert_eq!(Token::from_keyword("unknown"), None);
    }

    #[test]
    fn test_is_keyword() {
        assert!(Token::Select.is_keyword());
        assert!(Token::Create.is_keyword());
        assert!(!Token::Asterisk.is_keyword());
        assert!(!Token::IntegerLiteral(42).is_keyword());
    }
}
