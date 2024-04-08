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
    : AbstractExecutor(exec_ctx) , plan_(plan), child_executor_(std::move(child_executor)){}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(is_updated){
    return false;
  }
  TableHeap *table = table_info_->table_.get();
  int size = 0;
  RID child_rid;
  Tuple child_tuple;
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    size++;
    // 删除旧数据
    TupleMeta old_meta = table->GetTupleMeta(child_rid);
    old_meta.is_deleted_ = true;
    table->UpdateTupleMeta(old_meta, child_rid);
    // 插入新数据
    table->InsertTuple(meta, child_tuple, nullptr, nullptr, plan_->TableOid());
  }
  std::vector<Value> res{};
  res.push_back(Value(TypeId::INTEGER, size));
  *tuple = Tuple{res, &GetOutputSchema()};
  is_updated = true;
  return true;
}

}  // namespace bustub
