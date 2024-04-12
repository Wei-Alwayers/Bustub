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
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!result_set_.empty()) {
    *tuple = result_set_.front();
    result_set_.pop();
    return true;
  }
  RID left_rid{};
  Tuple left_tuple{};
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    RID right_rid;
    Tuple right_tuple;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      auto is_joinable = plan_->predicate_->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                         right_executor_->GetOutputSchema());
      if (is_joinable.CompareEquals(Value(TypeId::BOOLEAN, 1)) == CmpBool::CmpTrue) {
        // 把两个tuple合并
        Tuple joined_tuple;
        std::vector<Value> joined_values;
        for (uint32_t k = 0; k < left_executor_->GetOutputSchema().GetColumnCount(); k++) {
          joined_values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), k));
        }
        for (uint32_t k = 0; k < right_executor_->GetOutputSchema().GetColumnCount(); k++) {
          joined_values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), k));
        }
        joined_tuple = Tuple(joined_values, &GetOutputSchema());
        result_set_.push(joined_tuple);
      }
    }
    if (result_set_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      Tuple joined_tuple;
      std::vector<Value> joined_values;
      for (uint32_t k = 0; k < left_executor_->GetOutputSchema().GetColumnCount(); k++) {
        joined_values.push_back(left_tuple.GetValue(&left_executor_->GetOutputSchema(), k));
      }
      for (uint32_t k = 0; k < right_executor_->GetOutputSchema().GetColumnCount(); k++) {
        // 获取null value
        joined_values.push_back(
            ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(k).GetType()));
      }
      joined_tuple = Tuple(joined_values, &GetOutputSchema());
      result_set_.push(joined_tuple);
    }

    if (!result_set_.empty()) {
      *tuple = result_set_.front();
      result_set_.pop();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
