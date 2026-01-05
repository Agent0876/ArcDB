use arcdb::catalog::Catalog;
use arcdb::executor::{ExecutionEngine, LogicalPlan};
use std::sync::Arc;

#[test]
fn test_transaction_lifecycle() {
    let catalog = Arc::new(Catalog::new());
    let mut engine = ExecutionEngine::new(catalog).unwrap();

    // BEGIN
    let result = engine.execute(LogicalPlan::BeginTransaction).unwrap();
    assert!(result.message.unwrap().contains("started"));

    // We cannot access private fields `current_trans_id` and `transaction_manager` from integration test.
    // We can only verify behavior via public API (execute results).
    // So distinct from previous content, we remove field access.

    // The previous test code accessed private fields?
    // `engine.current_trans_id` is private if not pub.
    // `engine.transaction_manager` is private.

    // So I can only verify via side effects or return messages.
    // The message format is "Transaction {} started".

    // COMMIT
    let result = engine.execute(LogicalPlan::Commit).unwrap();
    assert!(result.message.unwrap().contains("committed"));

    // BEGIN & ROLLBACK
    engine.execute(LogicalPlan::BeginTransaction).unwrap();
    let result = engine.execute(LogicalPlan::Rollback).unwrap();
    assert!(result.message.unwrap().contains("rolled back"));
}
