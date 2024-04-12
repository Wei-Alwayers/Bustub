#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),
      heap_(std::priority_queue<Tuple, std::vector<Tuple>, HeapCustomComparator>(HeapCustomComparator(plan->GetOrderBy(), plan->OutputSchema())))
      {}

void TopNExecutor::Init() {
  while (!heap_.empty()) {
    heap_.pop();
  }
  result_set_.clear();
  child_executor_->Init();
  RID rid;
  Tuple tuple;
  while(child_executor_->Next(&tuple, &rid)){
    heap_.push(tuple);
    if(heap_.size() > plan_->GetN()){
      heap_.pop();
    }
  }
  num_in_heap_ = heap_.size();
  for(size_t i = 0; i < num_in_heap_; i++){
    std::cout << heap_.top().ToString(&plan_->OutputSchema()) << std::endl;
    result_set_.push_back(heap_.top());
    heap_.pop();
  }
  std::reverse(result_set_.begin(), result_set_.end());
  cursor_ = 0;
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(cursor_ >= num_in_heap_){
    return false;
  }
  *tuple = result_set_[cursor_];
  cursor_++;
  return true;
}

auto TopNExecutor::GetNumInHeap() -> size_t {return result_set_.size();};

}  // namespace bustub
