#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)){}

void SortExecutor::Init() {
  result_set_.clear();
  RID rid;
  Tuple tuple;
  child_executor_->Init();
  while(child_executor_->Next(&tuple, &rid)){
    result_set_.push_back(tuple);
  }
  std::sort(result_set_.begin(), result_set_.end(), CustomComparator(plan_->order_bys_, plan_->OutputSchema()));
  cursor = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(cursor >= result_set_.size()){
    return false;
  }
  *tuple = result_set_[cursor];
  cursor++;
  return true;
}

}  // namespace bustub
