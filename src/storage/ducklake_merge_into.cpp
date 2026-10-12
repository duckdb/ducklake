#include "storage/ducklake_catalog.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "storage/ducklake_insert.hpp"

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
	auto &merge_into = Catalog::PlanMergeInto(context, planner, op, plan).Cast<PhysicalMergeInto>();
	for (auto &action : merge_into.actions) {
		if (action->action_type == MergeActionType::MERGE_INSERT) {
			action->op->Cast<DuckLakeInsert>().reads_table = true;
		}
	}
	return merge_into;
}

} // namespace duckdb
