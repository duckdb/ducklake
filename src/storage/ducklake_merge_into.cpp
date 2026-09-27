#include "storage/ducklake_catalog.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"

namespace duckdb {

PhysicalOperator &DuckLakeCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalMergeInto &op, PhysicalOperator &plan) {
	// DuckLake writes a deletion file per data file, so it can apply at most one UPDATE/DELETE to a given row
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
			}
		}
	}
	if (update_delete_count > 1) {
		throw NotImplementedException("MERGE INTO with DuckLake only supports a single UPDATE/DELETE action currently");
	}
	return Catalog::PlanMergeInto(context, planner, op, plan);
}

} // namespace duckdb
