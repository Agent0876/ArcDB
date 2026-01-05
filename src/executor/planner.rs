//! Query Planner for ArcDB
//!
//! This module converts parsed SQL AST into executable plans.

use crate::catalog::Catalog;
use crate::sql::ast::*;

/// Logical plan node
#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// Scan a table
    Scan {
        table_name: String,
        projection: Option<Vec<String>>,
    },
    /// Index Scan
    IndexScan {
        table_name: String,
        index_name: String,
        columns: Vec<String>, // Index columns
        op: BinaryOperator,
        value: Expr,
    },
    /// Filter rows
    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },
    /// Project columns
    Project {
        input: Box<LogicalPlan>,
        expressions: Vec<SelectItem>,
    },
    /// Join two inputs (Nested Loop)
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
    },
    /// Hash Join for equality conditions (efficient O(n+m))
    HashJoin {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_key: Expr,
        right_key: Expr,
    },
    /// Sort rows
    Sort {
        input: Box<LogicalPlan>,
        order_by: Vec<OrderByItem>,
    },
    /// Limit rows
    Limit {
        input: Box<LogicalPlan>,
        limit: Option<Expr>,
        offset: Option<Expr>,
    },
    /// Aggregate
    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
    },
    /// Insert into table
    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
    },
    /// Update table
    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        predicate: Option<Expr>,
    },
    /// Delete from table
    Delete {
        table_name: String,
        predicate: Option<Expr>,
    },
    /// Create table
    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
    },
    /// Drop table
    DropTable { table_name: String, if_exists: bool },
    /// Begin transaction
    BeginTransaction,
    /// Commmit transaction
    Commit,
    /// Rollback transaction
    Rollback,
    /// Create index
    CreateIndex {
        index_name: String,
        table_name: String,
        columns: Vec<String>,
        unique: bool,
        if_not_exists: bool,
    },
    /// Analyze table for statistics
    Analyze { table_name: String },
}

/// Query planner
pub struct Planner<'a> {
    catalog: &'a Catalog,
}

impl<'a> Planner<'a> {
    /// Create a new planner
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    /// Plan a statement
    pub fn plan(&self, stmt: Statement) -> LogicalPlan {
        match stmt {
            Statement::Select(select) => self.plan_select(select),
            Statement::Insert(insert) => self.plan_insert(insert),
            Statement::Update(update) => self.plan_update(update),
            Statement::Delete(delete) => self.plan_delete(delete),
            Statement::CreateTable(create) => self.plan_create_table(create),
            Statement::DropTable(drop) => self.plan_drop_table(drop),
            Statement::CreateIndex(create) => self.plan_create_index(create),
            Statement::BeginTransaction => LogicalPlan::BeginTransaction,
            Statement::Commit => LogicalPlan::Commit,
            Statement::Rollback => LogicalPlan::Rollback,
            Statement::Analyze(table_name) => LogicalPlan::Analyze { table_name },
        }
    }

    fn plan_select(&self, select: SelectStatement) -> LogicalPlan {
        // Start with table scan(s)
        let mut plan = if let Some(from) = select.from {
            // Base table scan
            let mut left = LogicalPlan::Scan {
                table_name: from.table.name.clone(),
                projection: None,
            };

            // Apply Joins
            // Apply Joins
            for join in from.joins {
                let right = LogicalPlan::Scan {
                    table_name: join.table.name.clone(),
                    projection: None,
                };

                // Check for equality condition to use HashJoin
                let mut is_hash_join = false;
                let mut left_key_expr = None;
                let mut right_key_expr = None;

                if let Some(Expr::BinaryOp {
                    left: l,
                    op,
                    right: r,
                }) = &join.condition
                {
                    if matches!(op, BinaryOperator::Eq) {
                        // Optimistically assume left expr is for left table and right expr is for right table
                        // In a real planner, we should validate this against schemas
                        is_hash_join = true;
                        left_key_expr = Some(*l.clone());
                        right_key_expr = Some(*r.clone());
                    }
                }

                if is_hash_join {
                    left = LogicalPlan::HashJoin {
                        left: Box::new(left),
                        right: Box::new(right),
                        left_key: left_key_expr.unwrap(),
                        right_key: right_key_expr.unwrap(),
                    };
                } else {
                    left = LogicalPlan::Join {
                        left: Box::new(left),
                        right: Box::new(right),
                        join_type: join.join_type,
                        condition: join.condition,
                    };
                }
            }
            left
        } else {
            // SELECT without FROM (e.g., SELECT 1 + 1)
            LogicalPlan::Scan {
                table_name: String::new(),
                projection: None,
            }
        };

        // Apply WHERE filter
        if let Some(predicate) = select.where_clause {
            // Optimization: If simple equality on indexed column, use IndexScan
            let mut optimized = false;
            if let LogicalPlan::Scan { table_name, .. } = &plan {
                if !table_name.is_empty() {
                    if let Expr::BinaryOp { left, op, right } = &predicate {
                        if matches!(op, BinaryOperator::Eq) {
                            // Check if left is column and right is value
                            if let Expr::Column(col_ref) = &**left {
                                if let Expr::Literal(_) = &**right {
                                    let col_name = &col_ref.column;
                                    // Check indexes
                                    let indexes = self.catalog.get_table_indexes(table_name);
                                    for idx in indexes {
                                        if idx.columns.len() == 1 && &idx.columns[0] == col_name {
                                            // Found index!
                                            plan = LogicalPlan::IndexScan {
                                                table_name: table_name.clone(),
                                                index_name: idx.name.clone(),
                                                columns: idx.columns.clone(),
                                                op: op.clone(),
                                                value: *right.clone(),
                                            };
                                            optimized = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if !optimized {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate,
                };
            }
        }

        // Apply GROUP BY
        if !select.group_by.is_empty() {
            // Extract aggregate expressions from select list
            let aggregates = self.extract_aggregates(&select.columns);
            plan = LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_by: select.group_by,
                aggregates,
            };

            // Apply HAVING
            if let Some(having) = select.having {
                plan = LogicalPlan::Filter {
                    input: Box::new(plan),
                    predicate: having,
                };
            }
        }

        // Apply projection
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            expressions: select.columns,
        };

        // Apply ORDER BY
        if !select.order_by.is_empty() {
            plan = LogicalPlan::Sort {
                input: Box::new(plan),
                order_by: select.order_by,
            };
        }

        // Apply LIMIT/OFFSET
        if select.limit.is_some() || select.offset.is_some() {
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                limit: select.limit,
                offset: select.offset,
            };
        }

        plan
    }

    fn plan_insert(&self, insert: InsertStatement) -> LogicalPlan {
        LogicalPlan::Insert {
            table_name: insert.table_name,
            columns: insert.columns,
            values: insert.values,
        }
    }

    fn plan_update(&self, update: UpdateStatement) -> LogicalPlan {
        LogicalPlan::Update {
            table_name: update.table_name,
            assignments: update.assignments,
            predicate: update.where_clause,
        }
    }

    fn plan_delete(&self, delete: DeleteStatement) -> LogicalPlan {
        LogicalPlan::Delete {
            table_name: delete.table_name,
            predicate: delete.where_clause,
        }
    }

    fn plan_create_table(&self, create: CreateTableStatement) -> LogicalPlan {
        LogicalPlan::CreateTable {
            table_name: create.table_name,
            columns: create.columns,
            if_not_exists: create.if_not_exists,
        }
    }

    fn plan_drop_table(&self, drop: DropTableStatement) -> LogicalPlan {
        LogicalPlan::DropTable {
            table_name: drop.table_name,
            if_exists: drop.if_exists,
        }
    }

    fn plan_create_index(&self, create: CreateIndexStatement) -> LogicalPlan {
        LogicalPlan::CreateIndex {
            index_name: create.index_name,
            table_name: create.table_name,
            columns: create.columns,
            unique: create.unique,
            if_not_exists: create.if_not_exists,
        }
    }

    fn extract_aggregates(&self, columns: &[SelectItem]) -> Vec<Expr> {
        let mut aggregates = Vec::new();
        for item in columns {
            if let SelectItem::Expr { expr, .. } = item {
                self.find_aggregates(expr, &mut aggregates);
            }
        }
        aggregates
    }

    fn find_aggregates(&self, expr: &Expr, result: &mut Vec<Expr>) {
        match expr {
            Expr::Function { name, .. } => {
                let name_upper = name.to_uppercase();
                if matches!(name_upper.as_str(), "COUNT" | "SUM" | "AVG" | "MIN" | "MAX") {
                    result.push(expr.clone());
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.find_aggregates(left, result);
                self.find_aggregates(right, result);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::Parser;

    #[test]
    fn test_plan_simple_select() {
        let catalog = Catalog::new();
        let planner = Planner::new(&catalog);

        let mut parser = Parser::new("SELECT * FROM users WHERE id = 1").unwrap();
        let stmt = parser.parse().unwrap();

        let plan = planner.plan(stmt);

        // Should have: Scan -> Filter -> Project
        match plan {
            LogicalPlan::Project { input, .. } => match *input {
                LogicalPlan::Filter { input, .. } => {
                    assert!(matches!(*input, LogicalPlan::Scan { .. }));
                }
                _ => panic!("Expected Filter"),
            },
            _ => panic!("Expected Project"),
        }
    }
}
