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

#include <memory>

#include "execution/executors/filter_executor.h"
#include "execution/executors/insert_executor.h"
#include "execution/executors/seq_scan_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/values_plan.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void InsertExecutor::Init() {}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if(is_inserted_){
    return false;
  }
  int size = 0;
  TableHeap *table = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  TupleMeta meta = {INVALID_TXN_ID, INVALID_TXN_ID, false};
  auto values_child_node = std::dynamic_pointer_cast<const ValuesPlanNode>(plan_->GetChildPlan());
  if(values_child_node){
    // 孩子节点属于values node类型
    size = values_child_node->GetValues().size();
    for(int i = 0; i < size; i++){
      std::vector<Value> values;
      int values_size = values_child_node->GetValues()[i].size();
      for(int j = 0; j <  values_size; j++){
        values.push_back(values_child_node->GetValues()[i][j]->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->schema_));
      }
      const Schema *schema = &exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->schema_;
      Tuple insert_tuple = Tuple(values, schema);
      table->InsertTuple(meta, insert_tuple, nullptr, nullptr, plan_->TableOid());
    }
  } else {
    // 孩子节点属于seq scan类型
    RID child_rid;
    Tuple child_tuple;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
      size++;
      table->InsertTuple(meta, child_tuple, nullptr, nullptr, plan_->TableOid());
    }
  }
  std::vector<Value> res{};
  res.push_back(Value(TypeId::INTEGER, size));
  *tuple = Tuple{res, &GetOutputSchema()};
  is_inserted_ = true;
  return true;
}

}  // namespace bustub
