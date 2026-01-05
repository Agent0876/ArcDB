use crate::executor::LogicalPlan;
use crate::sql::ast::{BinaryOperator, Expr};
use crate::storage::Table;
use std::collections::HashMap;

/// Heuristic-based query optimizer
pub struct HeuristicOptimizer<'a> {
    /// Available tables for index lookup
    tables: &'a HashMap<String, Table>,
}

impl<'a> HeuristicOptimizer<'a> {
    /// Create a new optimizer
    pub fn new(tables: &'a HashMap<String, Table>) -> Self {
        Self { tables }
    }

    /// Optimize a logical plan
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, predicate } => {
                let optimized_input = self.optimize(*input);

                // Try to optimize Filter(Scan) into IndexScan
                if let LogicalPlan::Scan {
                    ref table_name,
                    ref projection,
                } = optimized_input
                {
                    if let Some(index_scan) =
                        self.try_optimize_index_scan(table_name, projection, &predicate)
                    {
                        return index_scan;
                    }
                }

                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    predicate,
                }
            }
            LogicalPlan::Project { input, expressions } => LogicalPlan::Project {
                input: Box::new(self.optimize(*input)),
                expressions,
            },
            LogicalPlan::Join {
                left,
                right,
                join_type,
                condition,
            } => LogicalPlan::Join {
                left: Box::new(self.optimize(*left)),
                right: Box::new(self.optimize(*right)),
                join_type,
                condition,
            },
            LogicalPlan::Sort { input, order_by } => LogicalPlan::Sort {
                input: Box::new(self.optimize(*input)),
                order_by,
            },
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => LogicalPlan::Limit {
                input: Box::new(self.optimize(*input)),
                limit,
                offset,
            },
            LogicalPlan::Aggregate {
                input,
                group_by,
                aggregates,
            } => LogicalPlan::Aggregate {
                input: Box::new(self.optimize(*input)),
                group_by,
                aggregates,
            },
            // Other plans are returned as-is
            _ => plan,
        }
    }

    /// Try to transform Filter(Scan) into IndexScan
    fn try_optimize_index_scan(
        &self,
        table_name: &str,
        _projection: &Option<Vec<String>>,
        predicate: &Expr,
    ) -> Option<LogicalPlan> {
        let table = self.tables.get(table_name)?;

        // Pattern: column op value
        if let Expr::BinaryOp { left, op, right } = predicate {
            if matches!(
                *op,
                BinaryOperator::Eq
                    | BinaryOperator::Gt
                    | BinaryOperator::Gte
                    | BinaryOperator::Lt
                    | BinaryOperator::Lte
            ) {
                // Check if left is column and right is literal (or vice versa)
                if let (Expr::Column(col_ref), Expr::Literal(lit)) = (&**left, &**right) {
                    if let Some(index_name) = table.get_index_for_column(&col_ref.column) {
                        return Some(LogicalPlan::IndexScan {
                            table_name: table_name.to_string(),
                            index_name,
                            columns: vec![col_ref.column.clone()],
                            op: *op,
                            value: Expr::Literal(lit.clone()),
                        });
                    }
                } else if let (Expr::Literal(lit), Expr::Column(col_ref)) = (&**left, &**right) {
                    // For range operators, we might need to flip the operator if literal is on the left
                    // But for Eq it's symmetric. For others, let's keep it simple for now or flip.
                    // For now, only Eq is supported on right-side Column.
                    if *op == BinaryOperator::Eq {
                        if let Some(index_name) = table.get_index_for_column(&col_ref.column) {
                            return Some(LogicalPlan::IndexScan {
                                table_name: table_name.to_string(),
                                index_name,
                                columns: vec![col_ref.column.clone()],
                                op: *op,
                                value: Expr::Literal(lit.clone()),
                            });
                        }
                    }
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Column, DataType, Schema, TableDef};
    use crate::sql::ast::{ColumnRef, Literal};
    use crate::storage::Table;
    use std::sync::Arc;

    fn create_test_table_with_index() -> Table {
        let mut schema = Schema::new();
        schema.add_column(Column::new("id", DataType::Integer, 0));
        schema.add_column(Column::new("name", DataType::Varchar(50), 1));

        let def = TableDef::new("test", schema, 1);
        let data_dir = std::path::PathBuf::from("data_test_opt");
        if !data_dir.exists() {
            std::fs::create_dir_all(&data_dir).ok();
        }
        let disk = Arc::new(crate::storage::DiskManager::new(data_dir));
        let bpm = Arc::new(std::sync::Mutex::new(
            crate::storage::BufferPoolManager::new(10, disk),
        ));
        let mut table = Table::new(Arc::new(def), bpm);

        // Create an index on 'id'
        table
            .create_index("id_idx".to_string(), vec!["id".to_string()])
            .unwrap();

        table
    }

    #[test]
    fn test_optimize_index_scan() {
        let table = create_test_table_with_index();
        let mut tables = HashMap::new();
        tables.insert("test".to_string(), table);

        let optimizer = HeuristicOptimizer::new(&tables);

        // Original plan: SELECT * FROM test WHERE id = 1
        let scan = LogicalPlan::Scan {
            table_name: "test".to_string(),
            projection: None,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(Literal::Integer(1))),
            },
        };

        let optimized = optimizer.optimize(filter);

        // Should be transformed to IndexScan
        if let LogicalPlan::IndexScan {
            table_name,
            index_name,
            columns,
            op,
            value: _,
        } = optimized
        {
            assert_eq!(table_name, "test");
            assert_eq!(index_name, "id_idx");
            assert_eq!(columns, vec!["id".to_string()]);
            assert_eq!(op, BinaryOperator::Eq);
        } else {
            panic!("Expected IndexScan, got {:?}", optimized);
        }
    }

    #[test]
    fn test_no_optimize_no_index() {
        let table = create_test_table_with_index();
        let mut tables = HashMap::new();
        tables.insert("test".to_string(), table);

        let optimizer = HeuristicOptimizer::new(&tables);

        // No index on 'name'
        let scan = LogicalPlan::Scan {
            table_name: "test".to_string(),
            projection: None,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef {
                    table: None,
                    column: "name".to_string(),
                })),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Literal(Literal::String("alice".to_string()))),
            },
        };

        let optimized = optimizer.optimize(filter.clone());

        // Should NOT be transformed
        match optimized {
            LogicalPlan::Filter { .. } => {}
            _ => panic!("Expected Filter, got {:?}", optimized),
        }
    }

    #[test]
    fn test_optimize_range_scan() {
        let table = create_test_table_with_index();
        let mut tables = HashMap::new();
        tables.insert("test".to_string(), table);

        let optimizer = HeuristicOptimizer::new(&tables);

        // Original plan: SELECT * FROM test WHERE id > 10
        let scan = LogicalPlan::Scan {
            table_name: "test".to_string(),
            projection: None,
        };
        let filter = LogicalPlan::Filter {
            input: Box::new(scan),
            predicate: Expr::BinaryOp {
                left: Box::new(Expr::Column(ColumnRef {
                    table: None,
                    column: "id".to_string(),
                })),
                op: BinaryOperator::Gt,
                right: Box::new(Expr::Literal(Literal::Integer(10))),
            },
        };

        let optimized = optimizer.optimize(filter);

        // Should be transformed to IndexScan
        if let LogicalPlan::IndexScan {
            table_name,
            index_name,
            columns,
            op,
            value: _,
        } = optimized
        {
            assert_eq!(table_name, "test");
            assert_eq!(index_name, "id_idx");
            assert_eq!(columns, vec!["id".to_string()]);
            assert_eq!(op, BinaryOperator::Gt);
        } else {
            panic!("Expected IndexScan, got {:?}", optimized);
        }
    }
}
