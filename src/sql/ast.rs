//! SQL Abstract Syntax Tree (AST)
//!
//! This module defines the AST nodes for SQL statements.

use crate::catalog::DataType;

/// A SQL statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    /// SELECT statement
    Select(SelectStatement),
    /// INSERT statement
    Insert(InsertStatement),
    /// UPDATE statement
    Update(UpdateStatement),
    /// DELETE statement
    Delete(DeleteStatement),
    /// CREATE TABLE statement
    CreateTable(CreateTableStatement),
    /// DROP TABLE statement
    DropTable(DropTableStatement),
    /// CREATE INDEX statement
    CreateIndex(CreateIndexStatement),
    /// BEGIN TRANSACTION
    BeginTransaction,
    /// COMMIT
    Commit,
    /// ROLLBACK
    Rollback,
    /// ANALYZE table
    Analyze(String),
}

/// SELECT statement
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    /// DISTINCT flag
    pub distinct: bool,
    /// Select list (columns or expressions)
    pub columns: Vec<SelectItem>,
    /// FROM clause
    pub from: Option<FromClause>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
    /// GROUP BY clause
    pub group_by: Vec<Expr>,
    /// HAVING clause
    pub having: Option<Expr>,
    /// ORDER BY clause
    pub order_by: Vec<OrderByItem>,
    /// LIMIT clause
    pub limit: Option<Expr>,
    /// OFFSET clause
    pub offset: Option<Expr>,
}

impl Default for SelectStatement {
    fn default() -> Self {
        Self {
            distinct: false,
            columns: Vec::new(),
            from: None,
            where_clause: None,
            group_by: Vec::new(),
            having: None,
            order_by: Vec::new(),
            limit: None,
            offset: None,
        }
    }
}

/// A single item in the SELECT list
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// All columns (*)
    Wildcard,
    /// A table's all columns (table.*)
    QualifiedWildcard(String),
    /// An expression with optional alias
    Expr { expr: Expr, alias: Option<String> },
}

/// FROM clause
#[derive(Debug, Clone, PartialEq)]
pub struct FromClause {
    /// Base table
    pub table: TableRef,
    /// JOIN clauses
    pub joins: Vec<Join>,
}

/// Table reference
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    /// Table name
    pub name: String,
    /// Optional alias
    pub alias: Option<String>,
}

/// JOIN clause
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    /// Join type
    pub join_type: JoinType,
    /// Table to join
    pub table: TableRef,
    /// Join condition
    pub condition: Option<Expr>,
}

/// Type of JOIN
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

/// ORDER BY item
#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    /// Expression to order by
    pub expr: Expr,
    /// Ascending (true) or descending (false)
    pub ascending: bool,
}

/// INSERT statement
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    /// Target table name
    pub table_name: String,
    /// Column names (optional)
    pub columns: Option<Vec<String>>,
    /// Values to insert
    pub values: Vec<Vec<Expr>>,
}

/// UPDATE statement
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    /// Target table name
    pub table_name: String,
    /// SET clause (column = value pairs)
    pub assignments: Vec<Assignment>,
    /// WHERE clause
    pub where_clause: Option<Expr>,
}

/// Column assignment (for UPDATE)
#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    /// Column name
    pub column: String,
    /// New value
    pub value: Expr,
}

/// DELETE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    /// Target table name
    pub table_name: String,
    /// WHERE clause
    pub where_clause: Option<Expr>,
}

/// CREATE TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    /// Table name
    pub table_name: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Table constraints
    pub constraints: Vec<TableConstraint>,
    /// IF NOT EXISTS flag
    pub if_not_exists: bool,
}

/// Column definition
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: DataType,
    /// NOT NULL constraint
    pub not_null: bool,
    /// DEFAULT value
    pub default: Option<Expr>,
    /// PRIMARY KEY constraint
    pub primary_key: bool,
    /// UNIQUE constraint
    pub unique: bool,
}

/// Table-level constraint
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// PRIMARY KEY constraint
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    /// UNIQUE constraint
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    /// FOREIGN KEY constraint
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
    },
    /// CHECK constraint
    Check { name: Option<String>, expr: Expr },
}

/// DROP TABLE statement
#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    /// Table name
    pub table_name: String,
    /// IF EXISTS flag
    pub if_exists: bool,
}

/// CREATE INDEX statement
#[derive(Debug, Clone, PartialEq)]
pub struct CreateIndexStatement {
    /// Index name
    pub index_name: String,
    /// Table name
    pub table_name: String,
    /// Column names
    pub columns: Vec<String>,
    /// UNIQUE flag
    pub unique: bool,
    /// IF NOT EXISTS flag
    pub if_not_exists: bool,
}

/// SQL Expression
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// Column reference
    Column(ColumnRef),
    /// Literal value
    Literal(Literal),
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp { op: UnaryOperator, expr: Box<Expr> },
    /// Function call
    Function {
        name: String,
        args: Vec<Expr>,
        distinct: bool,
    },
    /// IS NULL
    IsNull(Box<Expr>),
    /// IS NOT NULL
    IsNotNull(Box<Expr>),
    /// BETWEEN
    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },
    /// IN
    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// LIKE
    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
    },
    /// CASE expression
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_clause: Option<Box<Expr>>,
    },
    /// Subquery
    Subquery(Box<SelectStatement>),
    /// EXISTS
    Exists(Box<SelectStatement>),
    /// Nested expression (in parentheses)
    Nested(Box<Expr>),
}

/// Column reference
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    /// Table name (optional)
    pub table: Option<String>,
    /// Column name
    pub column: String,
}

impl From<String> for ColumnRef {
    fn from(column: String) -> Self {
        Self {
            table: None,
            column,
        }
    }
}

/// Literal value
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    /// NULL
    Null,
    /// Boolean
    Boolean(bool),
    /// Integer
    Integer(i64),
    /// Float
    Float(f64),
    /// String
    String(String),
}

/// Binary operator
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    // Comparison
    Eq,
    Neq,
    Lt,
    Gt,
    Lte,
    Gte,
    // Logical
    And,
    Or,
    // Arithmetic
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // String
    Concat,
}

impl BinaryOperator {
    /// Get the precedence of this operator (higher = binds tighter)
    pub fn precedence(&self) -> u8 {
        match self {
            BinaryOperator::Or => 1,
            BinaryOperator::And => 2,
            BinaryOperator::Eq
            | BinaryOperator::Neq
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::Lte
            | BinaryOperator::Gte => 3,
            BinaryOperator::Add | BinaryOperator::Sub | BinaryOperator::Concat => 4,
            BinaryOperator::Mul | BinaryOperator::Div | BinaryOperator::Mod => 5,
        }
    }
}

/// Unary operator
#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    /// NOT
    Not,
    /// - (negation)
    Minus,
    /// + (plus sign)
    Plus,
}
