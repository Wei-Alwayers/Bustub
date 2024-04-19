//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  try {
    if(exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED){
      if(!exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->GetTableOid()) && !exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->GetTableOid())){
        exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
      }
    }
  } catch (TransactionAbortException &e){
    fmt::print(e.GetInfo());
  }
  table_iterator_ptr_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  try {
    if(exec_ctx_->IsDelete()){
      exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->GetTableOid());
    }
    // unlock table
    if (table_iterator_ptr_->IsEnd()) {
      // READ_COMMITTED下如果table的锁是IS，需要释放
      if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid())){
        // 立即释放锁
        exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->GetTableOid());
      }
      return false;
    }
    // lock row
    if(exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid())){
        exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
    }
    else if(exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->GetTableOid()) || exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->GetTableOid())){
      exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
    }

    while (table_iterator_ptr_->GetTuple().first.is_deleted_) {
      // unlock useless row
      if( exec_ctx_->GetTransaction()->IsRowSharedLocked(plan_->GetTableOid(), table_iterator_ptr_->GetRID()) ||
          exec_ctx_->GetTransaction()->IsRowExclusiveLocked(plan_->GetTableOid(), table_iterator_ptr_->GetRID())){
        exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), table_iterator_ptr_->GetRID(), true);
      }

      table_iterator_ptr_->operator++();

      // unlock table
      if (table_iterator_ptr_->IsEnd()) {
        // READ_COMMITTED下如果table的锁是IS，需要释放
        if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid())){
          // 立即释放锁
          exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), plan_->GetTableOid());
        }
        return false;
      }

      // lock row
      if(exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid())){
        exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
      }
      else if(exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->GetTableOid()) || exec_ctx_->GetTransaction()->IsTableIntentionExclusiveLocked(plan_->GetTableOid())){
        exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
      }
    }
    *tuple = table_iterator_ptr_->GetTuple().second;
    *rid = table_iterator_ptr_->GetRID();
    // unlock row
    if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && exec_ctx_->GetTransaction()->IsTableIntentionSharedLocked(plan_->GetTableOid())){
      exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), table_iterator_ptr_->GetRID(), false);
    }
    table_iterator_ptr_->operator++();
  } catch (TransactionAbortException &e){
    fmt::print(e.GetInfo());
  }
  return true;
}

}  // namespace bustub
