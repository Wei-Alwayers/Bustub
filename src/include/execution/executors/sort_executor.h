//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
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
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;

  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> result_set_;
  size_t cursor_;
};

// 函数对象，用于动态生成比较器
struct CustomComparator {
  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;
  Schema schema_;

  // 构造函数，接收排序列的索引
  CustomComparator(std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys, Schema schema)
      : order_bys_(std::move(order_bys)), schema_(std::move(schema)) {}

  // 重载 () 运算符，实现比较器功能
  auto operator()(const Tuple &lhs, const Tuple &rhs) const -> bool {
    for (const std::pair<OrderByType, AbstractExpressionRef> &order_by : order_bys_) {
      Value left_value = order_by.second->Evaluate(&lhs, schema_);
      Value right_value = order_by.second->Evaluate(&rhs, schema_);
      if (order_by.first == OrderByType::ASC || order_by.first == OrderByType::DEFAULT) {
        // 升序排列
        if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
          return true;
        }
        if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
          return false;
        }
      }
      if (order_by.first == OrderByType::DESC) {
        // 降序排列
        if (left_value.CompareLessThan(right_value) == CmpBool::CmpTrue) {
          return false;
        }
        if (left_value.CompareGreaterThan(right_value) == CmpBool::CmpTrue) {
          return true;
        }
      }
    }
    return false;  // 默认情况下，元素相等，不需要交换顺序
  }
};

}  // namespace bustub
