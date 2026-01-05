//! SQL Parser
//!
//! This module parses SQL tokens into an AST.

use super::ast::*;
use super::lexer::Lexer;
use super::token::Token;
use crate::catalog::DataType;
use crate::error::{Error, Result};

/// SQL Parser
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    /// Create a new parser from a SQL string
    pub fn new(sql: &str) -> Result<Self> {
        let mut lexer = Lexer::new(sql);
        let tokens = lexer.tokenize()?;

        Ok(Self {
            tokens,
            position: 0,
        })
    }

    /// Parse a single SQL statement
    pub fn parse(&mut self) -> Result<Statement> {
        let stmt = self.parse_statement()?;

        // Consume optional semicolon
        if self.check(&Token::Semicolon) {
            self.advance();
        }

        Ok(stmt)
    }

    /// Parse multiple SQL statements
    pub fn parse_all(&mut self) -> Result<Vec<Statement>> {
        let mut statements = Vec::new();

        while !self.is_at_end() {
            statements.push(self.parse()?);
        }

        Ok(statements)
    }

    /// Parse a single statement
    fn parse_statement(&mut self) -> Result<Statement> {
        match self.current() {
            Token::Select => self.parse_select().map(Statement::Select),
            Token::Insert => self.parse_insert().map(Statement::Insert),
            Token::Update => self.parse_update().map(Statement::Update),
            Token::Delete => self.parse_delete().map(Statement::Delete),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Begin => self.parse_begin(),
            Token::Commit => self.parse_commit(),
            Token::Rollback => self.parse_rollback(),
            Token::Analyze => self.parse_analyze(),
            _ => Err(Error::UnexpectedToken {
                expected:
                    "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, BEGIN, COMMIT, ROLLBACK, or ANALYZE"
                        .to_string(),
                found: format!("{}", self.current()),
            }),
        }
    }

    // ========== SELECT Statement ==========

    fn parse_select(&mut self) -> Result<SelectStatement> {
        self.expect(&Token::Select)?;

        let mut stmt = SelectStatement::default();

        // DISTINCT
        if self.check(&Token::Distinct) {
            self.advance();
            stmt.distinct = true;
        } else if self.check(&Token::All) {
            self.advance();
        }

        // Select list
        stmt.columns = self.parse_select_list()?;

        // FROM clause
        if self.check(&Token::From) {
            stmt.from = Some(self.parse_from_clause()?);
        }

        // WHERE clause
        if self.check(&Token::Where) {
            self.advance();
            stmt.where_clause = Some(self.parse_expr()?);
        }

        // GROUP BY clause
        if self.check(&Token::Group) {
            self.advance();
            self.expect(&Token::By)?;
            stmt.group_by = self.parse_expr_list()?;
        }

        // HAVING clause
        if self.check(&Token::Having) {
            self.advance();
            stmt.having = Some(self.parse_expr()?);
        }

        // ORDER BY clause
        if self.check(&Token::Order) {
            self.advance();
            self.expect(&Token::By)?;
            stmt.order_by = self.parse_order_by_list()?;
        }

        // LIMIT clause
        if self.check(&Token::Limit) {
            self.advance();
            stmt.limit = Some(self.parse_expr()?);
        }

        // OFFSET clause
        if self.check(&Token::Offset) {
            self.advance();
            stmt.offset = Some(self.parse_expr()?);
        }

        Ok(stmt)
    }

    fn parse_select_list(&mut self) -> Result<Vec<SelectItem>> {
        let mut items = Vec::new();

        loop {
            items.push(self.parse_select_item()?);

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance(); // consume comma
        }

        Ok(items)
    }

    fn parse_select_item(&mut self) -> Result<SelectItem> {
        // Check for *
        if self.check(&Token::Asterisk) {
            self.advance();
            return Ok(SelectItem::Wildcard);
        }

        // Check for table.*
        if let Token::Identifier(name) = self.current().clone() {
            if self.peek() == Some(&Token::Dot) {
                self.advance(); // consume identifier
                self.advance(); // consume dot
                if self.check(&Token::Asterisk) {
                    self.advance();
                    return Ok(SelectItem::QualifiedWildcard(name));
                } else {
                    // It's a qualified column reference, rewind and parse as expression
                    self.position -= 2;
                }
            }
        }

        // Parse as expression
        let expr = self.parse_expr()?;

        // Check for alias
        let alias = if self.check(&Token::As) {
            self.advance();
            Some(self.expect_identifier()?)
        } else if let Token::Identifier(_) = self.current() {
            // Alias without AS
            if !self.check(&Token::From) && !self.check(&Token::Comma) {
                Some(self.expect_identifier()?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(SelectItem::Expr { expr, alias })
    }

    fn parse_from_clause(&mut self) -> Result<FromClause> {
        self.expect(&Token::From)?;

        let table = self.parse_table_ref()?;
        let mut joins = Vec::new();

        // Parse JOINs
        while self.is_join_keyword() {
            joins.push(self.parse_join()?);
        }

        Ok(FromClause { table, joins })
    }

    fn parse_table_ref(&mut self) -> Result<TableRef> {
        let name = self.expect_identifier()?;

        let alias = if self.check(&Token::As) {
            self.advance();
            Some(self.expect_identifier()?)
        } else if let Token::Identifier(_) = self.current() {
            if !self.is_join_keyword()
                && !self.check(&Token::Where)
                && !self.check(&Token::Group)
                && !self.check(&Token::Order)
                && !self.check(&Token::Limit)
                && !self.check(&Token::On)
                && !self.check(&Token::Comma)
                && !self.check(&Token::RParen)
            {
                Some(self.expect_identifier()?)
            } else {
                None
            }
        } else {
            None
        };

        Ok(TableRef { name, alias })
    }

    fn is_join_keyword(&self) -> bool {
        matches!(
            self.current(),
            Token::Join | Token::Inner | Token::Left | Token::Right | Token::Full | Token::Cross
        )
    }

    fn parse_join(&mut self) -> Result<Join> {
        let join_type = self.parse_join_type()?;
        self.expect(&Token::Join)?;
        let table = self.parse_table_ref()?;

        let condition = if self.check(&Token::On) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(Join {
            join_type,
            table,
            condition,
        })
    }

    fn parse_join_type(&mut self) -> Result<JoinType> {
        if self.check(&Token::Inner) {
            self.advance();
            Ok(JoinType::Inner)
        } else if self.check(&Token::Left) {
            self.advance();
            if self.check(&Token::Outer) {
                self.advance();
            }
            Ok(JoinType::Left)
        } else if self.check(&Token::Right) {
            self.advance();
            if self.check(&Token::Outer) {
                self.advance();
            }
            Ok(JoinType::Right)
        } else if self.check(&Token::Full) {
            self.advance();
            if self.check(&Token::Outer) {
                self.advance();
            }
            Ok(JoinType::Full)
        } else if self.check(&Token::Cross) {
            self.advance();
            Ok(JoinType::Cross)
        } else {
            // Just JOIN means INNER JOIN
            Ok(JoinType::Inner)
        }
    }

    fn parse_order_by_list(&mut self) -> Result<Vec<OrderByItem>> {
        let mut items = Vec::new();

        loop {
            let expr = self.parse_expr()?;
            let ascending = if self.check(&Token::Desc) {
                self.advance();
                false
            } else {
                if self.check(&Token::Asc) {
                    self.advance();
                }
                true
            };

            items.push(OrderByItem { expr, ascending });

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(items)
    }

    // ========== INSERT Statement ==========

    fn parse_insert(&mut self) -> Result<InsertStatement> {
        self.expect(&Token::Insert)?;
        self.expect(&Token::Into)?;

        let table_name = self.expect_identifier()?;

        // Optional column list
        let columns = if self.check(&Token::LParen) {
            self.advance();
            let cols = self.parse_identifier_list()?;
            self.expect(&Token::RParen)?;
            Some(cols)
        } else {
            None
        };

        self.expect(&Token::Values)?;

        // Parse value rows
        let mut values = Vec::new();
        loop {
            self.expect(&Token::LParen)?;
            let row = self.parse_expr_list()?;
            self.expect(&Token::RParen)?;
            values.push(row);

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(InsertStatement {
            table_name,
            columns,
            values,
        })
    }

    // ========== UPDATE Statement ==========

    fn parse_update(&mut self) -> Result<UpdateStatement> {
        self.expect(&Token::Update)?;

        let table_name = self.expect_identifier()?;

        self.expect(&Token::Set)?;

        let mut assignments = Vec::new();
        loop {
            let column = self.expect_identifier()?;
            self.expect(&Token::Eq)?;
            let value = self.parse_expr()?;
            assignments.push(Assignment { column, value });

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        let where_clause = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(UpdateStatement {
            table_name,
            assignments,
            where_clause,
        })
    }

    // ========== DELETE Statement ==========

    fn parse_delete(&mut self) -> Result<DeleteStatement> {
        self.expect(&Token::Delete)?;
        self.expect(&Token::From)?;

        let table_name = self.expect_identifier()?;

        let where_clause = if self.check(&Token::Where) {
            self.advance();
            Some(self.parse_expr()?)
        } else {
            None
        };

        Ok(DeleteStatement {
            table_name,
            where_clause,
        })
    }

    // ========== CREATE Statement ==========

    fn parse_create(&mut self) -> Result<Statement> {
        self.expect(&Token::Create)?;

        match self.current() {
            Token::Table => self.parse_create_table().map(Statement::CreateTable),
            Token::Index | Token::Unique => self.parse_create_index().map(Statement::CreateIndex),
            _ => Err(Error::UnexpectedToken {
                expected: "TABLE or INDEX".to_string(),
                found: format!("{}", self.current()),
            }),
        }
    }

    fn parse_create_table(&mut self) -> Result<CreateTableStatement> {
        self.expect(&Token::Table)?;

        let if_not_exists = if self.check(&Token::If) {
            self.advance();
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        let table_name = self.expect_identifier()?;

        self.expect(&Token::LParen)?;

        let mut columns = Vec::new();
        let mut constraints = Vec::new();

        loop {
            // Check for table constraint
            if self.check(&Token::Primary)
                || self.check(&Token::Foreign)
                || self.check(&Token::Unique)
                || self.check(&Token::Check)
                || self.check(&Token::Constraint)
            {
                constraints.push(self.parse_table_constraint()?);
            } else {
                columns.push(self.parse_column_def()?);
            }

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        self.expect(&Token::RParen)?;

        Ok(CreateTableStatement {
            table_name,
            columns,
            constraints,
            if_not_exists,
        })
    }

    fn parse_create_index(&mut self) -> Result<CreateIndexStatement> {
        let unique = if self.check(&Token::Unique) {
            self.advance();
            true
        } else {
            false
        };

        self.expect(&Token::Index)?;

        let if_not_exists = if self.check(&Token::If) {
            self.advance();
            self.expect(&Token::Not)?;
            self.expect(&Token::Exists)?;
            true
        } else {
            false
        };

        let index_name = self.expect_identifier()?;
        self.expect(&Token::On)?;
        let table_name = self.expect_identifier()?;

        self.expect(&Token::LParen)?;
        let columns = self.parse_identifier_list()?;
        self.expect(&Token::RParen)?;

        Ok(CreateIndexStatement {
            index_name,
            table_name,
            columns,
            unique,
            if_not_exists,
        })
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef> {
        let name = self.expect_identifier()?;
        let data_type = self.parse_data_type()?;

        let mut not_null = false;
        let mut default = None;
        let mut primary_key = false;
        let mut unique = false;

        // Parse column constraints
        loop {
            if self.check(&Token::Not) {
                self.advance();
                self.expect(&Token::Null)?;
                not_null = true;
            } else if self.check(&Token::Null) {
                self.advance();
                // NULL is allowed (default)
            } else if self.check(&Token::Default) {
                self.advance();
                default = Some(self.parse_primary_expr()?);
            } else if self.check(&Token::Primary) {
                self.advance();
                self.expect(&Token::Key)?;
                primary_key = true;
                not_null = true;
            } else if self.check(&Token::Unique) {
                self.advance();
                unique = true;
            } else {
                break;
            }
        }

        Ok(ColumnDef {
            name,
            data_type,
            not_null,
            default,
            primary_key,
            unique,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let dt = match self.current() {
            Token::Int | Token::Integer => {
                self.advance();
                DataType::Integer
            }
            Token::BigInt => {
                self.advance();
                DataType::BigInt
            }
            Token::SmallInt => {
                self.advance();
                DataType::SmallInt
            }
            Token::Float => {
                self.advance();
                DataType::Float
            }
            Token::Double => {
                self.advance();
                DataType::Double
            }
            Token::Decimal | Token::Numeric => {
                self.advance();
                if self.check(&Token::LParen) {
                    self.advance();
                    let precision = self.expect_integer()? as u8;
                    let scale = if self.check(&Token::Comma) {
                        self.advance();
                        self.expect_integer()? as u8
                    } else {
                        0
                    };
                    self.expect(&Token::RParen)?;
                    DataType::Decimal(precision, scale)
                } else {
                    DataType::Decimal(10, 0)
                }
            }
            Token::Varchar => {
                self.advance();
                self.expect(&Token::LParen)?;
                let len = self.expect_integer()? as usize;
                self.expect(&Token::RParen)?;
                DataType::Varchar(len)
            }
            Token::Char => {
                self.advance();
                if self.check(&Token::LParen) {
                    self.advance();
                    let len = self.expect_integer()? as usize;
                    self.expect(&Token::RParen)?;
                    DataType::Char(len)
                } else {
                    DataType::Char(1)
                }
            }
            Token::Text => {
                self.advance();
                DataType::Text
            }
            Token::Boolean => {
                self.advance();
                DataType::Boolean
            }
            Token::Date => {
                self.advance();
                DataType::Date
            }
            Token::Time => {
                self.advance();
                DataType::Time
            }
            Token::Timestamp => {
                self.advance();
                DataType::Timestamp
            }
            _ => {
                return Err(Error::UnexpectedToken {
                    expected: "data type".to_string(),
                    found: format!("{}", self.current()),
                });
            }
        };

        Ok(dt)
    }

    fn parse_table_constraint(&mut self) -> Result<TableConstraint> {
        // Optional constraint name
        let name = if self.check(&Token::Constraint) {
            self.advance();
            Some(self.expect_identifier()?)
        } else {
            None
        };

        if self.check(&Token::Primary) {
            self.advance();
            self.expect(&Token::Key)?;
            self.expect(&Token::LParen)?;
            let columns = self.parse_identifier_list()?;
            self.expect(&Token::RParen)?;
            Ok(TableConstraint::PrimaryKey { name, columns })
        } else if self.check(&Token::Unique) {
            self.advance();
            self.expect(&Token::LParen)?;
            let columns = self.parse_identifier_list()?;
            self.expect(&Token::RParen)?;
            Ok(TableConstraint::Unique { name, columns })
        } else if self.check(&Token::Foreign) {
            self.advance();
            self.expect(&Token::Key)?;
            self.expect(&Token::LParen)?;
            let columns = self.parse_identifier_list()?;
            self.expect(&Token::RParen)?;
            self.expect(&Token::References)?;
            let ref_table = self.expect_identifier()?;
            self.expect(&Token::LParen)?;
            let ref_columns = self.parse_identifier_list()?;
            self.expect(&Token::RParen)?;
            Ok(TableConstraint::ForeignKey {
                name,
                columns,
                ref_table,
                ref_columns,
            })
        } else if self.check(&Token::Check) {
            self.advance();
            self.expect(&Token::LParen)?;
            let expr = self.parse_expr()?;
            self.expect(&Token::RParen)?;
            Ok(TableConstraint::Check { name, expr })
        } else {
            Err(Error::UnexpectedToken {
                expected: "PRIMARY, UNIQUE, FOREIGN, or CHECK".to_string(),
                found: format!("{}", self.current()),
            })
        }
    }

    // ========== DROP Statement ==========

    fn parse_drop(&mut self) -> Result<Statement> {
        self.expect(&Token::Drop)?;

        match self.current() {
            Token::Table => {
                self.advance();

                let if_exists = if self.check(&Token::If) {
                    self.advance();
                    self.expect(&Token::Exists)?;
                    true
                } else {
                    false
                };

                let table_name = self.expect_identifier()?;

                Ok(Statement::DropTable(DropTableStatement {
                    table_name,
                    if_exists,
                }))
            }
            _ => Err(Error::UnexpectedToken {
                expected: "TABLE".to_string(),
                found: format!("{}", self.current()),
            }),
        }
    }

    // ========== Expression Parsing ==========

    fn parse_expr(&mut self) -> Result<Expr> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_and_expr()?;

        while self.check(&Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_not_expr()?;

        while self.check(&Token::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr> {
        if self.check(&Token::Not) {
            self.advance();
            let expr = self.parse_not_expr()?;
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison_expr()
        }
    }

    fn parse_comparison_expr(&mut self) -> Result<Expr> {
        let left = self.parse_additive_expr()?;

        // IS NULL / IS NOT NULL
        if self.check(&Token::Is) {
            self.advance();
            if self.check(&Token::Not) {
                self.advance();
                self.expect(&Token::Null)?;
                return Ok(Expr::IsNotNull(Box::new(left)));
            } else {
                self.expect(&Token::Null)?;
                return Ok(Expr::IsNull(Box::new(left)));
            }
        }

        // BETWEEN
        if self.check(&Token::Between) {
            self.advance();
            let low = self.parse_additive_expr()?;
            self.expect(&Token::And)?;
            let high = self.parse_additive_expr()?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: false,
            });
        }

        // NOT BETWEEN, NOT IN, NOT LIKE
        if self.check(&Token::Not) {
            self.advance();
            if self.check(&Token::Between) {
                self.advance();
                let low = self.parse_additive_expr()?;
                self.expect(&Token::And)?;
                let high = self.parse_additive_expr()?;
                return Ok(Expr::Between {
                    expr: Box::new(left),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: true,
                });
            } else if self.check(&Token::In) {
                self.advance();
                self.expect(&Token::LParen)?;
                let list = self.parse_expr_list()?;
                self.expect(&Token::RParen)?;
                return Ok(Expr::InList {
                    expr: Box::new(left),
                    list,
                    negated: true,
                });
            } else if self.check(&Token::Like) {
                self.advance();
                let pattern = self.parse_primary_expr()?;
                return Ok(Expr::Like {
                    expr: Box::new(left),
                    pattern: Box::new(pattern),
                    negated: true,
                });
            }
        }

        // IN
        if self.check(&Token::In) {
            self.advance();
            self.expect(&Token::LParen)?;
            let list = self.parse_expr_list()?;
            self.expect(&Token::RParen)?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: false,
            });
        }

        // LIKE
        if self.check(&Token::Like) {
            self.advance();
            let pattern = self.parse_primary_expr()?;
            return Ok(Expr::Like {
                expr: Box::new(left),
                pattern: Box::new(pattern),
                negated: false,
            });
        }

        // Comparison operators
        let op = match self.current() {
            Token::Eq => Some(BinaryOperator::Eq),
            Token::Neq => Some(BinaryOperator::Neq),
            Token::Lt => Some(BinaryOperator::Lt),
            Token::Gt => Some(BinaryOperator::Gt),
            Token::Lte => Some(BinaryOperator::Lte),
            Token::Gte => Some(BinaryOperator::Gte),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_additive_expr()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_additive_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_multiplicative_expr()?;

        loop {
            let op = match self.current() {
                Token::Plus => Some(BinaryOperator::Add),
                Token::Minus => Some(BinaryOperator::Sub),
                Token::Concat => Some(BinaryOperator::Concat),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_multiplicative_expr()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_multiplicative_expr(&mut self) -> Result<Expr> {
        let mut left = self.parse_unary_expr()?;

        loop {
            let op = match self.current() {
                Token::Asterisk => Some(BinaryOperator::Mul),
                Token::Slash => Some(BinaryOperator::Div),
                Token::Percent => Some(BinaryOperator::Mod),
                _ => None,
            };

            if let Some(op) = op {
                self.advance();
                let right = self.parse_unary_expr()?;
                left = Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                };
            } else {
                break;
            }
        }

        Ok(left)
    }

    fn parse_unary_expr(&mut self) -> Result<Expr> {
        match self.current() {
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            Token::Plus => {
                self.advance();
                let expr = self.parse_unary_expr()?;
                Ok(Expr::UnaryOp {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            _ => self.parse_primary_expr(),
        }
    }

    fn parse_primary_expr(&mut self) -> Result<Expr> {
        match self.current().clone() {
            // Literals
            Token::IntegerLiteral(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Integer(n)))
            }
            Token::FloatLiteral(n) => {
                self.advance();
                Ok(Expr::Literal(Literal::Float(n)))
            }
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expr::Literal(Literal::String(s)))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(true)))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Literal(Literal::Boolean(false)))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Literal(Literal::Null))
            }

            // Parenthesized expression or subquery
            Token::LParen => {
                self.advance();
                if self.check(&Token::Select) {
                    let subquery = self.parse_select()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::Subquery(Box::new(subquery)))
                } else {
                    let expr = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    Ok(Expr::Nested(Box::new(expr)))
                }
            }

            // EXISTS
            Token::Exists => {
                self.advance();
                self.expect(&Token::LParen)?;
                let subquery = self.parse_select()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Exists(Box::new(subquery)))
            }

            // CASE
            Token::Case => self.parse_case_expr(),

            // Aggregate functions
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                self.parse_function()
            }

            // Identifier (column or function)
            Token::Identifier(name) => {
                self.advance();

                // Check for function call
                if self.check(&Token::LParen) {
                    self.position -= 1; // Go back
                    self.parse_function()
                }
                // Check for qualified column (table.column)
                else if self.check(&Token::Dot) {
                    self.advance();
                    let column = self.expect_identifier()?;
                    Ok(Expr::Column(ColumnRef {
                        table: Some(name),
                        column,
                    }))
                } else {
                    Ok(Expr::Column(ColumnRef {
                        table: None,
                        column: name,
                    }))
                }
            }

            _ => Err(Error::UnexpectedToken {
                expected: "expression".to_string(),
                found: format!("{}", self.current()),
            }),
        }
    }

    fn parse_function(&mut self) -> Result<Expr> {
        let name = match self.current().clone() {
            Token::Count => {
                self.advance();
                "COUNT".to_string()
            }
            Token::Sum => {
                self.advance();
                "SUM".to_string()
            }
            Token::Avg => {
                self.advance();
                "AVG".to_string()
            }
            Token::Min => {
                self.advance();
                "MIN".to_string()
            }
            Token::Max => {
                self.advance();
                "MAX".to_string()
            }
            Token::Identifier(n) => {
                self.advance();
                n
            }
            _ => {
                return Err(Error::UnexpectedToken {
                    expected: "function name".to_string(),
                    found: format!("{}", self.current()),
                })
            }
        };

        self.expect(&Token::LParen)?;

        let distinct = if self.check(&Token::Distinct) {
            self.advance();
            true
        } else {
            false
        };

        let args = if self.check(&Token::Asterisk) {
            self.advance();
            vec![Expr::Column(ColumnRef {
                table: None,
                column: "*".to_string(),
            })]
        } else if self.check(&Token::RParen) {
            vec![]
        } else {
            self.parse_expr_list()?
        };

        self.expect(&Token::RParen)?;

        Ok(Expr::Function {
            name,
            args,
            distinct,
        })
    }

    fn parse_case_expr(&mut self) -> Result<Expr> {
        self.expect(&Token::Case)?;

        // Check for simple CASE (CASE expr WHEN ...)
        let operand = if !self.check(&Token::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.check(&Token::When) {
            self.advance();
            let when_expr = self.parse_expr()?;
            self.expect(&Token::Then)?;
            let then_expr = self.parse_expr()?;
            when_clauses.push((when_expr, then_expr));
        }

        let else_clause = if self.check(&Token::Else) {
            self.advance();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.expect(&Token::End)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    // ========== Helper functions ==========

    fn parse_expr_list(&mut self) -> Result<Vec<Expr>> {
        let mut exprs = Vec::new();

        loop {
            exprs.push(self.parse_expr()?);

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(exprs)
    }

    fn parse_identifier_list(&mut self) -> Result<Vec<String>> {
        let mut identifiers = Vec::new();

        loop {
            identifiers.push(self.expect_identifier()?);

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance();
        }

        Ok(identifiers)
    }

    fn current(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or(&Token::Eof)
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position + 1)
    }

    fn advance(&mut self) {
        if self.position < self.tokens.len() {
            self.position += 1;
        }
    }

    fn is_at_end(&self) -> bool {
        matches!(self.current(), Token::Eof)
    }

    fn check(&self, token: &Token) -> bool {
        std::mem::discriminant(self.current()) == std::mem::discriminant(token)
    }

    fn expect(&mut self, token: &Token) -> Result<()> {
        if self.check(token) {
            self.advance();
            Ok(())
        } else {
            Err(Error::UnexpectedToken {
                expected: format!("{}", token),
                found: format!("{}", self.current()),
            })
        }
    }

    fn expect_identifier(&mut self) -> Result<String> {
        match self.current().clone() {
            Token::Identifier(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(Error::UnexpectedToken {
                expected: "identifier".to_string(),
                found: format!("{}", self.current()),
            }),
        }
    }

    fn expect_integer(&mut self) -> Result<i64> {
        match self.current().clone() {
            Token::IntegerLiteral(n) => {
                self.advance();
                Ok(n)
            }
            _ => Err(Error::UnexpectedToken {
                expected: "integer".to_string(),
                found: format!("{}", self.current()),
            }),
        }
    }

    // ========== Transaction Statements ==========

    fn parse_begin(&mut self) -> Result<Statement> {
        self.expect(&Token::Begin)?;
        if self.check(&Token::Transaction) {
            self.advance();
        }
        Ok(Statement::BeginTransaction)
    }

    fn parse_commit(&mut self) -> Result<Statement> {
        self.expect(&Token::Commit)?;
        if self.check(&Token::Transaction) {
            self.advance();
        }
        Ok(Statement::Commit)
    }

    fn parse_rollback(&mut self) -> Result<Statement> {
        self.expect(&Token::Rollback)?;
        if self.check(&Token::Transaction) {
            self.advance();
        }
        Ok(Statement::Rollback)
    }

    fn parse_analyze(&mut self) -> Result<Statement> {
        self.expect(&Token::Analyze)?;
        let table_name = self.expect_identifier()?;
        Ok(Statement::Analyze(table_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let mut parser = Parser::new("SELECT * FROM users").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 1);
                assert!(matches!(s.columns[0], SelectItem::Wildcard));
                assert!(s.from.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_select_with_where() {
        let mut parser = Parser::new("SELECT id, name FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                assert_eq!(s.columns.len(), 2);
                assert!(s.where_clause.is_some());
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_parse_create_table() {
        let mut parser = Parser::new(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INTEGER DEFAULT 0
            )",
        )
        .unwrap();

        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::CreateTable(ct) => {
                assert_eq!(ct.table_name, "users");
                assert_eq!(ct.columns.len(), 4);
                assert!(ct.columns[0].primary_key);
                assert!(ct.columns[1].not_null);
                assert!(ct.columns[2].unique);
                assert!(ct.columns[3].default.is_some());
            }
            _ => panic!("Expected CREATE TABLE statement"),
        }
    }

    #[test]
    fn test_parse_insert() {
        let mut parser =
            Parser::new("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')").unwrap();

        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Insert(i) => {
                assert_eq!(i.table_name, "users");
                assert_eq!(i.columns.as_ref().unwrap().len(), 2);
                assert_eq!(i.values.len(), 2);
            }
            _ => panic!("Expected INSERT statement"),
        }
    }

    #[test]
    fn test_parse_update() {
        let mut parser =
            Parser::new("UPDATE users SET name = 'Charlie', age = 30 WHERE id = 1").unwrap();

        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Update(u) => {
                assert_eq!(u.table_name, "users");
                assert_eq!(u.assignments.len(), 2);
                assert!(u.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE statement"),
        }
    }

    #[test]
    fn test_parse_delete() {
        let mut parser = Parser::new("DELETE FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Delete(d) => {
                assert_eq!(d.table_name, "users");
                assert!(d.where_clause.is_some());
            }
            _ => panic!("Expected DELETE statement"),
        }
    }

    #[test]
    fn test_parse_join() {
        let mut parser = Parser::new(
            "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id",
        )
        .unwrap();

        let stmt = parser.parse().unwrap();

        match stmt {
            Statement::Select(s) => {
                let from = s.from.unwrap();
                assert_eq!(from.table.name, "users");
                assert_eq!(from.table.alias, Some("u".to_string()));
                assert_eq!(from.joins.len(), 1);
                assert!(matches!(from.joins[0].join_type, JoinType::Left));
            }
            _ => panic!("Expected SELECT statement"),
        }
    }
}
