//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  is_deleted_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_deleted_) {
    return false;
  }
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableHeap *table = catalog->GetTable(plan_->TableOid())->table_.get();
  std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(catalog->GetTable(plan_->TableOid())->name_);
  int size = 0;
  RID child_rid;
  Tuple child_tuple;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    size++;
    TupleMeta meta = table->GetTupleMeta(child_rid);
    meta.is_deleted_ = true;
    table->UpdateTupleMeta(meta, child_rid);
    // 删除index
    Tuple index_tuple;
    for (IndexInfo *index_ptr : indexes) {
      index_tuple = child_tuple.KeyFromTuple(catalog->GetTable(plan_->TableOid())->schema_, index_ptr->key_schema_,
                                             index_ptr->index_->GetKeyAttrs());
      index_ptr->index_->DeleteEntry(index_tuple, child_rid, nullptr);
    }
  }
  std::vector<Value> res{};
  res.emplace_back(TypeId::INTEGER, size);
  *tuple = Tuple{res, &GetOutputSchema()};
  is_deleted_ = true;
  return true;
}

}  // namespace bustub
