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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx), plan_(plan), iterator_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())->table_.get()->MakeIterator()){}

void SeqScanExecutor::Init() {}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(iterator_.IsEnd()){
    return false;
  }
  if(iterator_.GetTuple().first.is_deleted_){
    iterator_.operator++();
    return true;
  }
  *tuple = iterator_.GetTuple().second;
  *rid = iterator_.GetRID();
  iterator_.operator++();
  return true;
}

}  // namespace bustub
