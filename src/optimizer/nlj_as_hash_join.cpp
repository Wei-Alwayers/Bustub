#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  // Note for 2023 Spring: You should at least support join keys of the form:
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    if (const auto expression = std::dynamic_pointer_cast<const ComparisonExpression>(nlj_plan.predicate_)) {
      if (expression->comp_type_ == ComparisonType::Equal) {
        // 1. <column expr> = <column expr>
        std::vector<AbstractExpressionRef> left_key_expressions;
        std::vector<AbstractExpressionRef> right_key_expressions;
        const auto left_expression = std::dynamic_pointer_cast<const ColumnValueExpression>(expression->GetChildAt(0));
        ColumnValueExpression left_col_expression =
            ColumnValueExpression(0, left_expression->GetColIdx(), left_expression->GetReturnType());
        const auto right_expression = std::dynamic_pointer_cast<const ColumnValueExpression>(expression->GetChildAt(1));
        ColumnValueExpression right_col_expression =
            ColumnValueExpression(0, right_expression->GetColIdx(), right_expression->GetReturnType());
        if (left_expression->GetTupleIdx() == 0) {
          left_key_expressions.push_back(std::make_shared<ColumnValueExpression>(left_col_expression));
          right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(right_col_expression));
        } else {
          right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(left_col_expression));
          left_key_expressions.push_back(std::make_shared<ColumnValueExpression>(right_col_expression));
        }
        HashJoinPlanNode hash_join_node =
            HashJoinPlanNode(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                             left_key_expressions, right_key_expressions, nlj_plan.join_type_);
        AbstractPlanNodeRef new_plan = std::make_shared<HashJoinPlanNode>(hash_join_node);
        return new_plan;
      }
    }
    if (const auto expression = std::dynamic_pointer_cast<const LogicExpression>(nlj_plan.predicate_)) {
      if (expression->logic_type_ == LogicType::And && expression->children_.size() == 2) {
        if (const auto left_expression =
                std::dynamic_pointer_cast<const ComparisonExpression>(expression->GetChildAt(0))) {
          if (const auto right_expression =
                  std::dynamic_pointer_cast<const ComparisonExpression>(expression->GetChildAt(1))) {
            // 2. <column expr> = <column expr> AND <column expr> = <column expr>
            std::vector<AbstractExpressionRef> left_key_expressions;
            std::vector<AbstractExpressionRef> right_key_expressions;
            // TODO(hmwei): 变量名太不直观
            const auto first_left_expression =
                std::dynamic_pointer_cast<const ColumnValueExpression>(left_expression->GetChildAt(0));
            ColumnValueExpression first_left_col_expression =
                ColumnValueExpression(0, first_left_expression->GetColIdx(), first_left_expression->GetReturnType());
            const auto second_left_expression =
                std::dynamic_pointer_cast<const ColumnValueExpression>(right_expression->GetChildAt(0));
            ColumnValueExpression second_left_col_expression =
                ColumnValueExpression(0, second_left_expression->GetColIdx(), second_left_expression->GetReturnType());
            const auto first_right_expression =
                std::dynamic_pointer_cast<const ColumnValueExpression>(left_expression->GetChildAt(1));
            ColumnValueExpression first_right_col_expression =
                ColumnValueExpression(0, first_right_expression->GetColIdx(), first_right_expression->GetReturnType());
            const auto second_right_expression =
                std::dynamic_pointer_cast<const ColumnValueExpression>(right_expression->GetChildAt(1));
            ColumnValueExpression second_right_col_expression = ColumnValueExpression(
                0, second_right_expression->GetColIdx(), second_right_expression->GetReturnType());

            if (first_left_expression->GetTupleIdx() == 0) {
              left_key_expressions.push_back(std::make_shared<ColumnValueExpression>(first_left_col_expression));
              right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(first_right_col_expression));
            } else {
              left_key_expressions.push_back(std::make_shared<ColumnValueExpression>(first_right_col_expression));
              right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(first_left_col_expression));
            }
            if (second_left_expression->GetTupleIdx() == 0) {
              left_key_expressions.push_back(std::make_shared<ColumnValueExpression>(second_left_col_expression));
              right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(second_right_col_expression));
            } else {
              left_key_expressions.push_back(std::make_shared<ColumnValueExpression>(second_right_col_expression));
              right_key_expressions.push_back(std::make_shared<ColumnValueExpression>(second_left_col_expression));
            }
            HashJoinPlanNode hash_join_node =
                HashJoinPlanNode(nlj_plan.output_schema_, nlj_plan.GetLeftPlan(), nlj_plan.GetRightPlan(),
                                 left_key_expressions, right_key_expressions, nlj_plan.join_type_);
            AbstractPlanNodeRef new_plan = std::make_shared<HashJoinPlanNode>(hash_join_node);
            return new_plan;
          }
        }
      }
    }
  }
  return optimized_plan;
}

}  // namespace bustub
