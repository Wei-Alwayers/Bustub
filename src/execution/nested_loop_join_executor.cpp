//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), right_executor_(std::move(right_executor)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  RID rid{};
  Tuple tuple{};
  left_executor_->Init();
  while (left_executor_->Next(&tuple, &rid)) {
    left_set_.push_back(tuple);
  }
  right_executor_->Init();
  while (right_executor_->Next(&tuple, &rid)){
    right_set_.push_back(tuple);
  }

  int left_size = left_set_.size();
  int right_size = right_set_.size();
  for(int i = 0; i < left_size; i++) {
    bool is_find = false;
    for (int j = 0; j < right_size; ++j) {
      auto is_joinable = plan_->predicate_->EvaluateJoin(&left_set_[i], left_executor_->GetOutputSchema(),
                                                         &right_set_[j], right_executor_->GetOutputSchema());
      if (is_joinable.CompareEquals(Value(TypeId::BOOLEAN, 1)) == CmpBool::CmpTrue) {
        // 把两个tuple合并
        Tuple joined_tuple;
        std::vector<Value> joined_values;
        for(uint32_t k = 0; k < left_executor_->GetOutputSchema().GetColumnCount(); k++) {
          joined_values.push_back(left_set_[i].GetValue(&left_executor_->GetOutputSchema(), k));
        }
        for(uint32_t k = 0; k < right_executor_->GetOutputSchema().GetColumnCount(); k++) {
          joined_values.push_back(right_set_[j].GetValue(&right_executor_->GetOutputSchema(), k));
        }
        joined_tuple = Tuple(joined_values, &GetOutputSchema());
        result_set_.push_back(joined_tuple);
        is_find = true;
      }
    }
    if(plan_->GetJoinType() == JoinType::LEFT && !is_find){
      Tuple joined_tuple;
      std::vector<Value> joined_values;
      for(uint32_t k = 0; k < left_executor_->GetOutputSchema().GetColumnCount(); k++) {
        joined_values.push_back(left_set_[i].GetValue(&left_executor_->GetOutputSchema(), k));
      }
      for(uint32_t k = 0; k < right_executor_->GetOutputSchema().GetColumnCount(); k++) {
        // 获取null value
        joined_values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(k).GetType()));
      }
      joined_tuple = Tuple(joined_values, &GetOutputSchema());
      result_set_.push_back(joined_tuple);
    }
  }

}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(cursor >= result_set_.size()){
    return false;
  }
  *tuple = result_set_[cursor];
  cursor++;
  return true;
}

}  // namespace bustub
