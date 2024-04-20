//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/insert_executor.h"
#include <memory>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  is_inserted_ = false;
  try {
    if (!exec_ctx_->GetTransaction()->IsTableExclusiveLocked(plan_->TableOid())) {
      exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                             plan_->TableOid());
    }
  } catch (TransactionAbortException &e) {
    fmt::print(e.GetInfo());
  }
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_inserted_) {
    return false;
  }
  int size = 0;
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableHeap *table = catalog->GetTable(plan_->TableOid())->table_.get();
  std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(catalog->GetTable(plan_->TableOid())->name_);
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  RID child_rid;
  Tuple child_tuple;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    size++;
    RID insert_rid;
    try {
      insert_rid = table
                       ->InsertTuple(meta, child_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(),
                                     plan_->TableOid())
                       .value();
    } catch (TransactionAbortException &e) {
      fmt::print(e.GetInfo());
    }
    // 维护table write set
    exec_ctx_->GetTransaction()->AppendTableWriteRecord(TableWriteRecord{plan_->TableOid(), insert_rid, table});
    // 插入index
    Tuple index_tuple;
    for (IndexInfo *index_ptr : indexes) {
      index_tuple = child_tuple.KeyFromTuple(catalog->GetTable(plan_->TableOid())->schema_, index_ptr->key_schema_,
                                             index_ptr->index_->GetKeyAttrs());
      index_ptr->index_->InsertEntry(index_tuple, insert_rid, nullptr);
    }
  }
  std::vector<Value> res{};
  res.emplace_back(TypeId::INTEGER, size);
  *tuple = Tuple{res, &GetOutputSchema()};
  is_inserted_ = true;
  return true;
}

}  // namespace bustub
