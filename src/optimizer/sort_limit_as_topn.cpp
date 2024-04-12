#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if(optimized_plan->GetType() == PlanType::Limit){
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    const auto &child_plan = limit_plan.children_[0];
    if(child_plan->GetType() == PlanType::Sort){
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      TopNPlanNode top_n_plan = TopNPlanNode(limit_plan.output_schema_, sort_plan.GetChildPlan(), sort_plan.GetOrderBy(), limit_plan.GetLimit());
      AbstractPlanNodeRef new_plan = std::make_shared<TopNPlanNode>(top_n_plan);
      return new_plan;
    }
  }
  return optimized_plan;
}

}  // namespace bustub
