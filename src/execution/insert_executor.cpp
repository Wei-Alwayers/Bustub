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
    RID insert_rid = table->InsertTuple(meta, child_tuple, nullptr, nullptr, plan_->TableOid()).value();
    // 插入index
    Tuple index_tuple;
    for (IndexInfo *index_ptr : indexes) {
      index_tuple = child_tuple.KeyFromTuple(catalog->GetTable(plan_->TableOid())->schema_, index_ptr->key_schema_,
                                             index_ptr->index_->GetKeyAttrs());
      index_ptr->index_->InsertEntry(index_tuple, insert_rid, nullptr);
    }
  }
  std::vector<Value> res{};
  res.push_back(Value(TypeId::INTEGER, size));
  *tuple = Tuple{res, &GetOutputSchema()};
  is_inserted_ = true;
  return true;
}

}  // namespace bustub
