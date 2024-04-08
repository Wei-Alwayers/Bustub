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
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void DeleteExecutor::Init() {}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(is_deleted){
    return false;
  }
  TableHeap *table = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  int size = 0;
  RID child_rid;
  Tuple child_tuple;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    size++;
    TupleMeta meta = table->GetTupleMeta(child_rid);
    meta.is_deleted_ = true;
    table->UpdateTupleMeta(meta, child_rid);
  }
  std::vector<Value> res{};
  res.push_back(Value(TypeId::INTEGER, size));
  *tuple = Tuple{res, &GetOutputSchema()};
  is_deleted = true;
  return true;
}

}  // namespace bustub
