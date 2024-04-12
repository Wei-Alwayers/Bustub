//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void LimitExecutor::Init() {
  child_executor_->Init();
  cursor = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(cursor >= plan_->GetLimit()){
    return false;
  }
  bool is_find = child_executor_->Next(tuple, rid);
  cursor++;
  if(!is_find){
    return false;
  }
  return true;
}

}  // namespace bustub
