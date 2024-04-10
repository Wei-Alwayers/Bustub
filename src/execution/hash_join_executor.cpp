//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_child)), right_child_(std::move(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  RID left_rid;
  Tuple left_tuple;
  left_child_->Init();
  while (left_child_->Next(&left_tuple, &left_rid)){
    HashJoinKey hash_join_key;
    int key_size = plan_->LeftJoinKeyExpressions().size();
    for(int i = 0; i < key_size; i++){
      Value value = plan_->LeftJoinKeyExpressions()[i]->Evaluate(&left_tuple, left_child_->GetOutputSchema());
      hash_join_key.keys_.push_back(value);
    }
    hash_table_.insert(std::make_pair(hash_join_key, left_tuple));
    is_joined[hash_join_key] = false;
  }
  RID right_rid;
  Tuple right_tuple;
  right_child_->Init();
  while (right_child_->Next(&right_tuple, &right_rid)){
    HashJoinKey hash_join_key;
    int key_size = plan_->RightJoinKeyExpressions().size();
    for(int i = 0; i < key_size; i++){
      Value value = plan_->RightJoinKeyExpressions()[i]->Evaluate(&right_tuple, right_child_->GetOutputSchema());
      hash_join_key.keys_.push_back(value);
    }
    if(hash_table_.find(hash_join_key) != hash_table_.end()){
      // 找到对应的tuple, 把两个tuple合并
      Tuple joined_tuple;
      std::vector<Value> joined_values;
      for(uint32_t k = 0; k < left_child_->GetOutputSchema().GetColumnCount(); k++) {
        joined_values.push_back(hash_table_[hash_join_key].GetValue(&left_child_->GetOutputSchema(), k));
      }
      for(uint32_t k = 0; k < right_child_->GetOutputSchema().GetColumnCount(); k++) {
        joined_values.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), k));
      }
      joined_tuple = Tuple(joined_values, &GetOutputSchema());
      hash_table_[hash_join_key] = joined_tuple;
      is_joined[hash_join_key] = true;
    }
  }
  iterator_ = hash_table_.begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {

  while (iterator_ != hash_table_.end()){
    if(is_joined[iterator_->first]){
      *tuple = iterator_->second;
      iterator_++;
      return true;
    }
    if(plan_->GetJoinType() == JoinType::LEFT){
      std::vector<Value> joined_values;
      for(uint32_t k = 0; k < left_child_->GetOutputSchema().GetColumnCount(); k++) {
        joined_values.push_back(iterator_->second.GetValue(&left_child_->GetOutputSchema(), k));
      }
      for(uint32_t k = 0; k < right_child_->GetOutputSchema().GetColumnCount(); k++) {
        // 获取null value
        joined_values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(k).GetType()));
      }
      *tuple = Tuple(joined_values, &GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
