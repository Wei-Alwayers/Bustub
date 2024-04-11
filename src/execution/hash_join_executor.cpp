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
  // 创建left hash table
  while (left_child_->Next(&left_tuple, &left_rid)){
    HashJoinKey hash_join_key;
    int key_size = plan_->LeftJoinKeyExpressions().size();
    for(int i = 0; i < key_size; i++){
      Value value = plan_->LeftJoinKeyExpressions()[i]->Evaluate(&left_tuple, left_child_->GetOutputSchema());
      hash_join_key.keys_.push_back(value);
    }
    std::vector<Tuple> tuples;
    if(left_hash_table_.find(hash_join_key) != left_hash_table_.end()){
      tuples = left_hash_table_[hash_join_key];
    }
    tuples.push_back(left_tuple);
    left_hash_table_[hash_join_key] = tuples;
    is_joined[hash_join_key] = false;
  }
  RID right_rid;
  Tuple right_tuple;
  right_child_->Init();
  // 查找可以join的tuple
  while (right_child_->Next(&right_tuple, &right_rid)){
    HashJoinKey hash_join_key;
    int key_size = plan_->RightJoinKeyExpressions().size();
    for(int i = 0; i < key_size; i++){
      Value value = plan_->RightJoinKeyExpressions()[i]->Evaluate(&right_tuple, right_child_->GetOutputSchema());
      hash_join_key.keys_.push_back(value);
    }
    if(left_hash_table_.find(hash_join_key) != left_hash_table_.end()){
      is_joined[hash_join_key] = true;
      for(unsigned i = 0; i < left_hash_table_[hash_join_key].size(); i++){
        // 找到对应的tuple, 把两个tuple合并
        Tuple joined_tuple;
        std::vector<Tuple> tuples = left_hash_table_[hash_join_key];
        std::vector<Value> joined_values;
        for(uint32_t k = 0; k < left_child_->GetOutputSchema().GetColumnCount(); k++) {
          joined_values.push_back(tuples[i].GetValue(&left_child_->GetOutputSchema(), k));
        }
        for(uint32_t k = 0; k < right_child_->GetOutputSchema().GetColumnCount(); k++) {
          joined_values.push_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), k));
        }
        joined_tuple = Tuple(joined_values, &GetOutputSchema());
        result_set_.push_back(joined_tuple);
      }
    }
  }
  // 处理left join
  if(plan_->GetJoinType() == JoinType::LEFT){
    for(auto &it : is_joined){
      if(it.second == false){
        // 该tuple没有被join
        HashJoinKey hash_join_key = it.first;
        for(unsigned i = 0; i < left_hash_table_[hash_join_key].size(); i++){
          Tuple joined_tuple;
          std::vector<Tuple> tuples = left_hash_table_[hash_join_key];
          std::vector<Value> joined_values;
          for(uint32_t k = 0; k < left_child_->GetOutputSchema().GetColumnCount(); k++) {
            joined_values.push_back(tuples[i].GetValue(&left_child_->GetOutputSchema(), k));
          }
          for(uint32_t k = 0; k < right_child_->GetOutputSchema().GetColumnCount(); k++) {
            // 获取null value
            joined_values.push_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(k).GetType()));
          }
          joined_tuple = Tuple(joined_values, &GetOutputSchema());
          result_set_.push_back(joined_tuple);
        }
      }
    }
  }
  // 初始化cursor
  cursor = 0;
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(cursor >= result_set_.size()){
    return false;
  }
  *tuple = result_set_[cursor];
  cursor++;
  return true;
}

}  // namespace bustub
