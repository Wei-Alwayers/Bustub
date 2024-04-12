//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  /** Sets new child executor (for testing only) */
  void SetChildExecutor(std::unique_ptr<AbstractExecutor> &&child_executor) {
    child_executor_ = std::move(child_executor);
  }

  /** @return The size of top_entries_ container, which will be called on each child_executor->Next(). */
  auto GetNumInHeap() -> size_t;

  // 函数对象，用于动态生成比较器
  struct HeapCustomComparator {
    std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
    Schema schema_;

    // 构造函数，接收排序列的索引
    HeapCustomComparator(const std::vector<std::pair<OrderByType, AbstractExpressionRef>>& order_bys, const Schema schema) : order_bys_(order_bys) , schema_(schema){}

    // 重载 () 运算符，实现比较器功能
    bool operator()(const Tuple& lhs, const Tuple& rhs) const {
      for (std::pair<OrderByType, AbstractExpressionRef> order_by : order_bys_) {
        Value left_value = order_by.second->Evaluate(&lhs, schema_);
        Value right_value = order_by.second->Evaluate(&rhs, schema_);
        if(order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT){
          // 升序排列
          if(left_value.CompareLessThan(right_value) == CmpBool::CmpTrue){
            return true;
          }
          if(left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue){
            return false;
          }
        }
        if(order_by.first == OrderByType::DESC){
          // 降序排列
          if(left_value.CompareLessThan(right_value) == CmpBool::CmpTrue){
            return false;
          }
          if(left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue){
            return true;
          }
        }
      }
      return false; // 默认情况下，元素相等，不需要交换顺序
    }
  };

 private:
  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;

  std::priority_queue<Tuple, std::vector<Tuple>, HeapCustomComparator> heap_;
  std::vector<Tuple> result_set_;
  size_t num_in_heap_;
  unsigned long cursor_;
};
}  // namespace bustub
