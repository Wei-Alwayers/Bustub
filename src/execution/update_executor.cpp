//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
  is_updated = false;
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_updated) {
    return false;
  }
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableHeap *table = table_info_->table_.get();
  std::vector<IndexInfo *> indexes = catalog->GetTableIndexes(table_info_->name_);
  int size = 0;
  RID child_rid;
  Tuple child_tuple;
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 删除旧数据
    TupleMeta old_meta = table->GetTupleMeta(child_rid);
    old_meta.is_deleted_ = true;
    table->UpdateTupleMeta(old_meta, child_rid);
    // 删除index
    Tuple index_tuple;
    for (IndexInfo *index_ptr : indexes) {
      index_tuple = child_tuple.KeyFromTuple(catalog->GetTable(plan_->TableOid())->schema_, index_ptr->key_schema_,
                                             index_ptr->index_->GetKeyAttrs());
      index_ptr->index_->DeleteEntry(index_tuple, child_rid, nullptr);
    }
    // 插入新数据
    std::vector<Value> values;
    const Schema *schema = &exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->schema_;
    int cols_size = plan_->target_expressions_.size();
    for (int i = 0; i < cols_size; i++) {
      values.push_back(plan_->target_expressions_[i]->Evaluate(&child_tuple, *schema));
    }
    Tuple insert_tuple = Tuple(values, schema);
    RID insert_rid = table->InsertTuple(meta, insert_tuple, nullptr, nullptr, plan_->TableOid()).value();
    // 插入index
    for (IndexInfo *index_ptr : indexes) {
      index_tuple = insert_tuple.KeyFromTuple(catalog->GetTable(plan_->TableOid())->schema_, index_ptr->key_schema_,
                                              index_ptr->index_->GetKeyAttrs());
      index_ptr->index_->InsertEntry(index_tuple, insert_rid, nullptr);
    }
    // 记录增加
    size++;
  }
  std::vector<Value> res{};
  res.push_back(Value(TypeId::INTEGER, size));
  *tuple = Tuple{res, &GetOutputSchema()};
  is_updated = true;
  return true;
}

}  // namespace bustub
