//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan),
      index_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get())),
      iterator_(dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->index_.get())->GetBeginIterator()),
      table_(exec_ctx->GetCatalog()->GetTable(exec_ctx->GetCatalog()->GetIndex(plan->GetIndexOid())->table_name_)->table_.get())
      {}


void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(iterator_.IsEnd()) {
    return false;
  }
  *rid = iterator_.operator*().second;
  while (table_->GetTuple(*rid).first.is_deleted_){
    iterator_.operator++();
    if(iterator_.IsEnd()){
      return false;
    }
    *rid = iterator_.operator*().second;
  }
  *tuple = table_->GetTuple(*rid).second;
  iterator_.operator++();
  return true;
}

}  // namespace bustub
