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
  if(exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED){
    exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, plan_->GetTableOid());
  }
  table_iterator_ptr_ =
      std::make_unique<TableIterator>(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeEagerIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(exec_ctx_->IsDelete()){
    exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid());
  }
  if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED){
    // READ_UNCOMMITTED不需要任何锁
    if (table_iterator_ptr_->IsEnd()) {
      return false;
    }
    while (table_iterator_ptr_->GetTuple().first.is_deleted_) {
      table_iterator_ptr_->operator++();
      if (table_iterator_ptr_->IsEnd()) {
        return false;
      }
    }
    *tuple = table_iterator_ptr_->GetTuple().second;
    *rid = table_iterator_ptr_->GetRID();
    table_iterator_ptr_->operator++();
    return true;
  }
  // 其他隔离等级，需要加锁
  if (table_iterator_ptr_->IsEnd()) {
    return false;
  }
  if(exec_ctx_->IsDelete()){
    exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
  }
  else{
    exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
  }
  while (table_iterator_ptr_->GetTuple().first.is_deleted_) {
    exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), table_iterator_ptr_->GetRID(), true);
    table_iterator_ptr_->operator++();
    if (table_iterator_ptr_->IsEnd()) {
      return false;
    }
    if(exec_ctx_->IsDelete()){
      exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::EXCLUSIVE, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
    }
    else{
      exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), LockManager::LockMode::SHARED, plan_->GetTableOid(), table_iterator_ptr_->GetRID());
    }
  }
  *tuple = table_iterator_ptr_->GetTuple().second;
  *rid = table_iterator_ptr_->GetRID();
  if(exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED){
    // 立即释放锁
    exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), plan_->GetTableOid(), table_iterator_ptr_->GetRID(), true);
  }
  table_iterator_ptr_->operator++();
  return true;
}

}  // namespace bustub
