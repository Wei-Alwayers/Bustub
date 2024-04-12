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
  table_iterator_ptr_ = std::make_unique<TableIterator>(
      exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
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

}  // namespace bustub
